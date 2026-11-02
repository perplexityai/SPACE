//! Per-client concurrency limiter for control-plane-touching API operations.
//!
//! Caps how many sandbox *create* and *reconnect* operations a single client
//! (identified by the `X-Client-ID` header) may have in flight at once. One
//! misbehaving client — a cron reconnect storm, a runaway create loop — must
//! not saturate the Kubernetes control-plane API and starve every other client
//! of connect/create success. Born from the incidents where sandbox connect
//! success rate collapsed under such load.
//!
//! # Cluster-wide, in Redis
//!
//! The in-flight count for each client lives in a shared Redis store (the same
//! `SPACE_REDIS_URL` the cluster activation limiter uses), **not** in per-replica
//! memory, so the cap is enforced across every `space-api` replica: a client's
//! storm fanned across pods by the load balancer is still capped as one. Each
//! client bucket is a ZSET of in-flight slots (member = per-request id, score =
//! an absolute deadline); a crashed or timed-out holder's slot is reclaimed once
//! its deadline (the `max_hold` TTL) passes, so a lost release never leaks a slot
//! forever. See [`redis_limiter`].
//!
//! # Composition with the cluster activation limiter
//!
//! This per-client gate is the *outer* gate: a handler reserves a slot here
//! before it calls into the controller, which performs the cluster-wide
//! activation admission (see [`crate::cluster_admission`], a Redis token
//! bucket). The two never charge each other:
//!
//! * A per-client rejection returns from the handler before the controller
//!   runs, so the cluster token bucket is never consulted.
//! * A cluster-admission rejection propagates up through the controller; the
//!   handler returns and the per-client [`ClientConcurrencyGuard`] drops,
//!   releasing the slot (`ZREM`). A request throttled by the global limiter
//!   therefore leaves no per-client slot consumed.
//!
//! The ordering is deliberate: a concurrency slot is refundable (drop the guard
//! → `ZREM`), whereas a consumed rate-limiter token is not. Making the
//! refundable gate the outer one keeps both budgets independent.
//!
//! # Buckets and the allowlist
//!
//! Only **recognized** client ids get their own per-client bucket. The
//! allowlist is `known_clients` (env-configured; defaults to empty) plus any id
//! present in the Eppo `overrides` map, less any entry that's reserved or isn't
//! a valid Kubernetes label value (see [`check_client_id`]). Every other
//! request — no `X-Client-ID`, **or** an unrecognized id — shares one
//! [`ClientKey::Unspecified`] bucket with a deliberately tighter cap. This
//! matters: without the allowlist a client rotating a fresh id per request would
//! mint a new full-cap bucket each time and never throttle; funnelling all
//! unrecognized ids into one shared bucket closes that bypass.
//!
//! # Caps: env defaults, Eppo runtime overrides
//!
//! The caps come from env/Helm (`max_in_flight_per_client`,
//! `max_in_flight_unspecified`) and can be retuned at runtime — without a
//! redeploy — by the JSON flag [`SPACE_API_CLIENT_CONCURRENCY_QUOTAS_FLAG`],
//! which carries an optional `unspecified` cap and a per-`X-Client-ID`
//! `overrides` map (whose keys also extend the allowlist). Any field the flag
//! omits, and any override that does not resolve or does not parse, falls back
//! to the env cap. `SPACE_API_CLIENT_CONCURRENCY_MODE` is the env master
//! over the override too, so the flag can only retune caps, never enable a
//! limiter the env var turned off.
//!
//! # Guardrail, not a correctness gate
//!
//! The limiter runs under the env-master-first policy
//! ([`pplx_space_feature_flags::policy`]):
//! `SPACE_API_CLIENT_CONCURRENCY_MODE=off` is the hard off, and
//! [`SPACE_API_CLIENT_CONCURRENCY_LIMITER_FLAG`] is the runtime toggle that can
//! disable it without a redeploy. A deployment that installs no flag provider
//! runs the limiter its environment asked for; a provider that is installed and
//! cannot answer leaves the flag at its declared fallback, which is on — so
//! neither an OSS deployment nor an unreachable Eppo silently drops the
//! guardrail.
//!
//! It fails open by construction and must never be a create-path SPOF: when
//! off, when a bucket's cap resolves to zero, when there is no usable Redis
//! store (`misconfigured`), or on any Redis error (`store_unavailable`), it
//! admits with an untracked no-op guard. It ships in [`Mode::Observe`] so
//! operators can size the caps from real traffic before flipping to
//! [`Mode::Enforce`]; observe mode never rejects, it only meters would-be
//! rejections.

use std::borrow::Cow;
use std::collections::{BTreeSet, HashSet};
use std::future::Future;
use std::sync::{Arc, LazyLock, RwLock};
use std::time::Duration;

use pplx_space_common_core::types::LabelValue;
use pplx_space_feature_flags::{
    FeatureFlagClient, SPACE_API_CLIENT_CONCURRENCY_LIMITER_FLAG,
    SPACE_API_CLIENT_CONCURRENCY_QUOTAS_FLAG,
};
use pplx_space_metrics::sandbox::api::ClientIdTag;
use pplx_space_metrics::sandbox::api::client_limit::{self, Decision, Operation, Reason};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

mod redis_limiter;
#[cfg(test)]
mod tests;

use redis_limiter::RedisConcurrencyStore;

const MODE_ENV: &str = "SPACE_API_CLIENT_CONCURRENCY_MODE";
const MAX_PER_CLIENT_ENV: &str = "SPACE_API_CLIENT_CONCURRENCY_MAX_PER_CLIENT";
const KNOWN_CLIENTS_ENV: &str = "SPACE_API_CLIENT_CONCURRENCY_KNOWN_CLIENTS";
/// The shared counter store, injected from the same `api.redis` secret contract
/// as the cluster activation limiter (rendered once for the process).
const REDIS_URL_ENV: &str = "SPACE_REDIS_URL";

/// Both Eppo flags are single cluster-wide config, so every replica evaluates
/// them against one constant subject — not a per-sandbox or per-org rollout.
const FLAG_SUBJECT: &str = "space-cluster";

/// Redis key prefix for the per-client concurrency ZSETs. Namespaced (and
/// versioned) so it never collides with other SPACE keys in a shared Redis and
/// a future encoding change can bump `v1`. Identified clients live under a
/// `:client:` sub-namespace and the anonymous bucket under a sibling
/// `:unspecified` leaf, so no client-supplied `X-Client-ID` value (not even the
/// literal `unspecified`) can collide with the anonymous bucket.
const KEY_PREFIX: &str = "space:api:client_concurrency:v1";

const DEFAULT_MODE: Mode = Mode::Observe;
/// Per-client cap for an *allowlisted* client. Kept well below what would let one
/// client's concurrent fan-out dominate the ~6 rps / 60-burst cluster activation
/// budget or the control-plane connection pool. Observe-mode data should size it.
const DEFAULT_MAX_PER_CLIENT: usize = 32;
/// The shared bucket holds every request that is **not** an allowlisted client —
/// both no-`X-Client-ID` traffic and unrecognized ids. One tight cap for all of
/// it, so an unknown/rotating id can't carve out its own allowance: a swarm of
/// random ids all contend on this single bucket. Deliberately tighter than the
/// per-client cap.
const DEFAULT_MAX_UNSPECIFIED: usize = 8;
/// How long a reserved slot survives without an explicit release before it is
/// reclaimed as abandoned. It must exceed the 150-second node-RPC ceiling so a
/// live request keeps its slot for its entire lifetime.
const DEFAULT_MAX_HOLD: Duration = Duration::from_secs(180);
/// Extra TTL on the ZSET key beyond `max_hold` so an idle client's key
/// eventually disappears once every slot has aged out.
const KEY_TTL_SLACK: Duration = Duration::from_secs(60);

/// `Retry-After` hint returned on an enforced rejection. Short by design: the
/// cap is a concurrency ceiling, so a slot frees the instant any in-flight
/// operation for the client completes — a one-second backoff is plenty.
const RETRY_AFTER: Duration = Duration::from_secs(1);

/// Whether the limiter rejects over-cap requests or only meters them.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Mode {
    /// Disable the limiter entirely.
    Off,
    /// Admit every request; emit a `would_reject` metric for over-cap ones.
    Observe,
    /// Reject over-cap requests with HTTP 429.
    Enforce,
}

impl Mode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Observe => "observe",
            Self::Enforce => "enforce",
        }
    }
}

impl std::fmt::Display for Mode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, thiserror::Error)]
#[error("unknown client concurrency mode: {0}")]
pub struct ParseModeError(String);

impl std::str::FromStr for Mode {
    type Err = ParseModeError;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "off" => Ok(Self::Off),
            "observe" => Ok(Self::Observe),
            "enforce" => Ok(Self::Enforce),
            other => Err(ParseModeError(other.to_string())),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientConcurrencyConfig {
    pub mode: Mode,
    /// Maximum concurrent create+reconnect operations for one *allowlisted*
    /// client (see `known_clients`) that has no explicit Eppo `overrides` entry.
    /// A zero cap disables the limit for allowlisted clients (fail-open).
    pub max_in_flight_per_client: usize,
    /// Maximum concurrent operations for the single shared bucket of everything
    /// that is not an allowlisted client — both no-`X-Client-ID` traffic and
    /// unrecognized ids. Deliberately tighter than the per-client cap. A zero cap
    /// disables the limit for that bucket (fail-open).
    pub max_in_flight_unspecified: usize,
    /// Recognized `X-Client-ID` values that get their own per-client bucket. Any
    /// id **not** in this set (and not added via the Eppo `overrides` map) shares
    /// the `unspecified` bucket, so a per-request-random id can't carve out its
    /// own allowance. So does an entry that's reserved or isn't a valid
    /// Kubernetes label value; startup warns about each.
    pub known_clients: BTreeSet<String>,
    /// How long a reserved slot survives without an explicit release before it
    /// is reclaimed as abandoned (crash / timeout safety).
    pub max_hold: Duration,
    /// The shared ZSET store URL (`redis://` / `rediss://`). `None` disables the
    /// store (fail-open `misconfigured`). Carried in config so `with_config` is a
    /// complete, testable interface; the process-env read lives only in
    /// [`Self::from_env`].
    pub redis_url: Option<String>,
}

impl Default for ClientConcurrencyConfig {
    fn default() -> Self {
        Self {
            mode: DEFAULT_MODE,
            max_in_flight_per_client: DEFAULT_MAX_PER_CLIENT,
            max_in_flight_unspecified: DEFAULT_MAX_UNSPECIFIED,
            known_clients: BTreeSet::new(),
            max_hold: DEFAULT_MAX_HOLD,
            redis_url: None,
        }
    }
}

impl ClientConcurrencyConfig {
    pub fn from_env() -> Self {
        let defaults = Self::default();
        Self {
            mode: std::env::var(MODE_ENV)
                .ok()
                .and_then(|raw| raw.parse().ok())
                .unwrap_or(defaults.mode),
            max_in_flight_per_client: parse_usize_env(
                MAX_PER_CLIENT_ENV,
                defaults.max_in_flight_per_client,
            ),
            max_in_flight_unspecified: defaults.max_in_flight_unspecified,
            known_clients: parse_known_clients_env(KNOWN_CLIENTS_ENV),
            max_hold: defaults.max_hold,
            redis_url: std::env::var(REDIS_URL_ENV)
                .ok()
                .map(|url| url.trim().to_string())
                .filter(|url| !url.is_empty()),
        }
    }

    /// The per-bucket cap for `key`, preferring an Eppo runtime `override` and
    /// falling back to the env-configured cap. Zero means "no limit for this
    /// bucket".
    fn cap_for(&self, key: &ClientKey, overrides: Option<&QuotaOverrides>) -> usize {
        match key {
            ClientKey::Client(id) => overrides
                .and_then(|o| o.overrides.get(id.as_ref()).copied())
                .unwrap_or(self.max_in_flight_per_client),
            ClientKey::Unspecified => overrides
                .and_then(|o| o.unspecified)
                .unwrap_or(self.max_in_flight_unspecified),
        }
    }
}

/// Runtime cap overrides carried by the Eppo JSON quota flag. Every field is
/// optional: an absent field falls back to the env-configured cap. Unknown JSON
/// keys are ignored (lenient) so the flag shape can evolve without breaking the
/// limiter.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Deserialize)]
#[serde(default)]
struct QuotaOverrides {
    /// Cap for the shared no-/unrecognized-`X-Client-ID` bucket.
    unspecified: Option<usize>,
    /// Per-`X-Client-ID` caps for explicitly configured clients.
    /// Presence of a key here **also allowlists** that id at runtime, so ops can
    /// add a recognized client without a redeploy, unless the key is reserved
    /// or isn't a valid Kubernetes label value.
    #[serde(deserialize_with = "deserialize_overrides")]
    overrides: std::collections::HashMap<String, usize>,
}

/// Accept the override map either as an object or as its JSON text.
///
/// flagd 0.2.2's in-process resolvers stringify any nested object inside a
/// flag's variant, so a deployment serving this flag through flagd delivers
/// `overrides` as the *text* `{"acme":32}`. Without decoding it here the whole
/// assignment fails to parse and every cap silently falls back to its
/// env-configured value — the flag would look configured and do nothing.
///
/// Confined to this field on purpose: this is the one place that knows
/// `overrides` must be a map of caps, so decoding a string found here is a
/// typed decision rather than a guess. A string that does not decode to that
/// map is still an error, exactly as a malformed object would be.
///
/// The shape is dispatched on explicitly rather than through an untagged
/// enum. An untagged enum tries each variant and, when none fits, reports
/// only that none fit — so a cap written as `{"acme": "lots"}` would tell an
/// operator "data did not match any variant" instead of naming the bad value.
/// The failure is logged (see [`ClientConcurrencyConfig::quota_overrides`])
/// and a whole unparsed assignment reverts every cap to its env value, so
/// that message is the only thing pointing at which entry to fix.
///
/// Pinned upstream by `pplx-space-feature-flags`'s `tests/flagd_end_to_end.rs`;
/// when flagd stops stringifying, that test fails and this can go.
fn deserialize_overrides<'de, D>(
    deserializer: D,
) -> Result<std::collections::HashMap<String, usize>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let decoded = match <serde_json::Value as serde::Deserialize>::deserialize(deserializer)? {
        // Only ever flagd's stringified map; its own parse error names what
        // is wrong inside the text.
        serde_json::Value::String(encoded) => serde_json::from_str(&encoded),
        // Anything else is decoded as the map it claims to be, so serde's
        // error describes the offending entry rather than the dispatch.
        value => serde_json::from_value(value),
    };
    decoded.map_err(serde::de::Error::custom)
}

/// Parse env var `name` as a `usize`, falling back to `default` when it is
/// unset or unparseable. An explicit `0` is honored — it means "disable this
/// bucket's cap" (fail-open), a legitimate operator choice — so it is not
/// coerced to the default.
fn parse_usize_env(name: &str, default: usize) -> usize {
    match std::env::var(name) {
        Ok(raw) => raw.trim().parse::<usize>().unwrap_or(default),
        Err(_) => default,
    }
}

/// Parse env var `name` as a comma-separated allowlist, empty when unset.
fn parse_known_clients_env(name: &str) -> BTreeSet<String> {
    std::env::var(name)
        .map(|raw| parse_known_clients(&raw))
        .unwrap_or_default()
}

/// Split a comma-separated allowlist value: trim each token and drop empties. An
/// empty or all-blank value yields an empty set (allowlist shrunk to nothing).
/// Split from the env read so the parsing is testable without env mutation.
fn parse_known_clients(raw: &str) -> BTreeSet<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|id| !id.is_empty())
        .map(String::from)
        .collect()
}

/// Allowlisted client IDs, interned for the life of the process so a resolved
/// ID can ride in the `Copy` metric identity as a `&'static str`.
///
/// Interning is a metrics rule, applied when a [`ClientKey`] becomes its tag
/// ([`ClientKey::client_id_tag`]); bucketing never depends on it. Only key-set
/// entries are interned: operator configuration, not caller input. The static
/// allowlist is interned once, in [`KnownClients::new`], and is fixed for the
/// process. Override keys can change at runtime and are never freed, so at most
/// [`MAX_RUNTIME_CLIENT_IDS`] of them are interned; past that, an override-only
/// client keeps its own bucket but reports [`ClientIdTag::OVERFLOW`]. Which ones
/// fit is first-come per process, so replicas can name different override-only
/// clients once the cap fills.
static INTERNED_CLIENT_IDS: LazyLock<RwLock<InternedClientIds>> = LazyLock::new(RwLock::default);

/// The most override-only client IDs a process interns over its lifetime.
/// Generous for an operator-curated override map, small enough that the leak
/// stays a fixed number of operator-chosen strings however often the map
/// changes.
const MAX_RUNTIME_CLIENT_IDS: usize = 64;

#[derive(Default)]
struct InternedClientIds {
    ids: HashSet<&'static str>,
    /// How many of `ids` were interned at runtime, against
    /// [`MAX_RUNTIME_CLIENT_IDS`].
    runtime: usize,
}

/// The process-lifetime copy of a static allowlist entry.
fn intern_static_client_id(id: &str) -> &'static str {
    intern_client_id(id, false).unwrap_or_else(|| unreachable!("only runtime interning is capped"))
}

/// The process-lifetime copy of an override-only `id`, or `None` once
/// [`MAX_RUNTIME_CLIENT_IDS`] runtime IDs are interned.
fn intern_override_client_id(id: &str) -> Option<&'static str> {
    intern_client_id(id, true)
}

fn intern_client_id(id: &str, runtime: bool) -> Option<&'static str> {
    // A poisoned lock only means another thread panicked mid-insert; the set
    // itself is still a valid set of leaked strings.
    {
        let interned = INTERNED_CLIENT_IDS
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(existing) = interned.ids.get(id) {
            return Some(existing);
        }
        // Once the cap is full, a new runtime id is refused here, so requests
        // past it never contend on the write lock.
        if runtime && interned.runtime >= MAX_RUNTIME_CLIENT_IDS {
            return None;
        }
    }
    let mut interned = INTERNED_CLIENT_IDS
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    // Re-check under the write lock: another request may have interned it.
    if let Some(existing) = interned.ids.get(id) {
        return Some(existing);
    }
    if runtime {
        if interned.runtime >= MAX_RUNTIME_CLIENT_IDS {
            return None;
        }
        interned.runtime += 1;
    }
    let leaked: &'static str = Box::leak(id.into());
    interned.ids.insert(leaked);
    Some(leaked)
}

/// Reserved for callers that don't come through the ECW surface, so no ECW
/// caller or key-set entry can claim it. Nothing resolves to it yet.
const NATIVE_CLIENT_KEY: &str = "native";

/// Keys no single ECW client may hold: `unknown` for the shared bucket,
/// `overflow` past the runtime interning cap, and [`NATIVE_CLIENT_KEY`]. An
/// allowlist entry or override key spelled like any of them is ignored, so a
/// tag never names an allowlisted client and a group of callers with one
/// value.
///
/// Matched case-insensitively: Datadog lowercases tag values, so an `Unknown`
/// entry would read as `client_id:unknown` after ingestion.
const RESERVED_CLIENT_IDS: [&str; 3] = [
    ClientIdTag::UNKNOWN.as_str(),
    ClientIdTag::OVERFLOW.as_str(),
    NATIVE_CLIENT_KEY,
];

fn is_reserved_client_id(id: &str) -> bool {
    RESERVED_CLIENT_IDS
        .iter()
        .any(|reserved| id.eq_ignore_ascii_case(reserved))
}

/// Why a key-set entry can't name a client of its own.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RejectedClientId {
    Reserved,
    /// Not a valid Kubernetes label value: the grammar client ids are held to,
    /// so a key can also be written as a label. It also keeps DogStatsD
    /// separators out of metric tags.
    NotALabelValue,
}

/// Whether a key-set entry may name a client: a valid Kubernetes label value
/// that isn't reserved.
fn check_client_id(id: &str) -> Result<(), RejectedClientId> {
    if is_reserved_client_id(id) {
        return Err(RejectedClientId::Reserved);
    }
    LabelValue::parse(id).map_err(|_| RejectedClientId::NotALabelValue)?;
    Ok(())
}

/// The static allowlist, interned once at construction so resolving one of its
/// IDs on the request path is a lock-free set lookup.
#[derive(Debug, Default)]
struct KnownClients(BTreeSet<&'static str>);

impl KnownClients {
    /// Intern `ids`, dropping with a warning every entry that can't name a
    /// client ([`check_client_id`]): reserved ids and ones that aren't label
    /// values, whose callers share the unspecified bucket. Ids that differ only
    /// in case keep separate buckets but share one `client_id` series after
    /// Datadog lowercases tag values, so they're warned about too.
    fn new(ids: &BTreeSet<String>) -> Self {
        let mut folded = BTreeSet::new();
        let mut known = BTreeSet::new();
        for id in ids {
            match check_client_id(id) {
                Ok(()) => {}
                Err(RejectedClientId::Reserved) => {
                    tracing::warn!(
                        client_id = %id,
                        "SPACE_API_CLIENT_CONCURRENCY_KNOWN_CLIENTS lists a reserved client id; it shares the unspecified bucket and reports as client_id:unknown"
                    );
                    continue;
                }
                Err(RejectedClientId::NotALabelValue) => {
                    tracing::warn!(
                        client_id = %id,
                        "SPACE_API_CLIENT_CONCURRENCY_KNOWN_CLIENTS lists a client id that isn't a valid Kubernetes label value; it's ignored, so that caller shares the unspecified bucket and reports as client_id:unknown"
                    );
                    continue;
                }
            }
            if !folded.insert(id.to_ascii_lowercase()) {
                tracing::warn!(
                    client_id = %id,
                    "SPACE_API_CLIENT_CONCURRENCY_KNOWN_CLIENTS lists client ids that differ only in case; they keep separate buckets but share one client_id metric series"
                );
            }
            known.insert(intern_static_client_id(id));
        }
        Self(known)
    }

    fn get(&self, id: &str) -> Option<&'static str> {
        self.0.get(id).copied()
    }
}

/// Override keys already warned about by [`warn_ignored_override_once`].
/// Capped like the interned set, since override keys can change at runtime.
static WARNED_OVERRIDE_KEYS: LazyLock<RwLock<BTreeSet<String>>> = LazyLock::new(Default::default);

/// Warn once per process about an override key [`check_client_id`] rejected.
/// Override keys aren't known until a request reads the flag, so this runs on
/// the first request that matches one; later matches, and new keys once
/// [`MAX_RUNTIME_CLIENT_IDS`] have been warned about, stay quiet. Those take
/// only the read lock, so a storm from an ignored key doesn't serialize on it.
fn warn_ignored_override_once(id: &str, rejected: RejectedClientId) {
    let done =
        |warned: &BTreeSet<String>| warned.contains(id) || warned.len() >= MAX_RUNTIME_CLIENT_IDS;
    if done(
        &WARNED_OVERRIDE_KEYS
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
    ) {
        return;
    }
    let mut warned = WARNED_OVERRIDE_KEYS
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    // Re-check under the write lock: another request may have warned first.
    if done(&warned) {
        return;
    }
    warned.insert(id.to_owned());
    let reason = match rejected {
        RejectedClientId::Reserved => "is a reserved client id",
        RejectedClientId::NotALabelValue => "isn't a valid Kubernetes label value",
    };
    tracing::warn!(
        client_id = %id,
        flag = SPACE_API_CLIENT_CONCURRENCY_QUOTAS_FLAG.key(),
        "client-concurrency quota override key {reason}; it's ignored, so that caller shares the unspecified bucket and reports as client_id:unknown"
    );
}

/// Identifies the bucket a request is tracked against. Allowlisted clients get
/// their own [`ClientKey::Client`] bucket; everything else (no `X-Client-ID`, or
/// an unrecognized id) shares the single [`ClientKey::Unspecified`] bucket.
///
/// A `Client` id borrows the copy [`KnownClients`] interned for a static entry,
/// so the common case doesn't allocate, and owns an override-only id. Its
/// metric tag derives from it ([`Self::client_id_tag`]).
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum ClientKey {
    Client(Cow<'static, str>),
    Unspecified,
}

impl ClientKey {
    /// Trim the header value and treat blank/absent as "no client id".
    fn normalize(client_id: Option<&str>) -> Option<&str> {
        client_id.map(str::trim).filter(|id| !id.is_empty())
    }

    /// Resolve a raw header value to its key: an id the key set names gets
    /// its own `Client` bucket; a blank, absent, or unrecognized id shares the
    /// `Unspecified` bucket — so a per-request-random id can't mint itself a
    /// fresh per-client allowance.
    ///
    /// The key set is the static `known` set plus the Eppo `overrides` keys
    /// (an override also implies "known"), less every entry
    /// [`check_client_id`] rejects. `known` was checked at construction; an
    /// override key is checked here, only once a header has matched it.
    fn classify(
        client_id: Option<&str>,
        known: &KnownClients,
        overrides: Option<&QuotaOverrides>,
    ) -> Self {
        let Some(id) = Self::normalize(client_id) else {
            return Self::Unspecified;
        };
        if let Some(known) = known.get(id) {
            return Self::Client(Cow::Borrowed(known));
        }
        if !overrides.is_some_and(|o| o.overrides.contains_key(id)) {
            return Self::Unspecified;
        }
        match check_client_id(id) {
            Ok(()) => Self::Client(Cow::Owned(id.to_owned())),
            Err(rejected) => {
                warn_ignored_override_once(id, rejected);
                Self::Unspecified
            }
        }
    }

    /// The bounded `client_id` metric tag for this key: the interned id,
    /// `overflow` for an override-only id past the interning cap, or `unknown`
    /// for the shared bucket. Interning happens here, not in [`Self::classify`],
    /// because it bounds metric memory, not buckets.
    fn client_id_tag(&self) -> ClientIdTag {
        match self {
            Self::Client(Cow::Borrowed(id)) => ClientIdTag::allowlisted(id),
            Self::Client(Cow::Owned(id)) => intern_override_client_id(id)
                .map_or(ClientIdTag::OVERFLOW, ClientIdTag::allowlisted),
            Self::Unspecified => ClientIdTag::UNKNOWN,
        }
    }

    /// The namespaced Redis ZSET key for this bucket. Allowlisted clients are
    /// keyed under `:client:<id>` and the shared bucket under `:unspecified`,
    /// so the two namespaces are disjoint no matter what `<id>` is.
    fn redis_key(&self) -> String {
        match self {
            Self::Client(id) => format!("{KEY_PREFIX}:client:{id}"),
            Self::Unspecified => format!("{KEY_PREFIX}:unspecified"),
        }
    }
}

/// The limiter's verdict for one admission attempt.
pub enum ClientConcurrencyDecision {
    /// Proceed. Hold the guard for the operation's full duration; dropping it
    /// releases the slot.
    Admitted(ClientConcurrencyGuard),
    /// The client is at its concurrent-operation cap. Return HTTP 429.
    Rejected {
        retry_after: Duration,
        client_id: ClientIdTag,
    },
}

impl ClientConcurrencyDecision {
    /// The bounded client the limiter attributed this request to — the same
    /// value its decision metric carried — so a handler can put it on its own
    /// metrics without resolving the allowlist again.
    pub fn client_id(&self) -> ClientIdTag {
        match self {
            Self::Admitted(guard) => guard.client_id,
            Self::Rejected { client_id, .. } => *client_id,
        }
    }
}

#[derive(Clone)]
pub struct ClientConcurrencyLimiter {
    inner: Arc<Inner>,
}

struct Inner {
    config: ClientConcurrencyConfig,
    /// `config.known_clients` without the entries [`check_client_id`] rejects,
    /// interned.
    known: KnownClients,
    feature_flags: FeatureFlagClient,
    /// The shared ZSET store. `None` when no usable `SPACE_REDIS_URL` is
    /// configured — the limiter then fails open (`misconfigured`).
    store: Option<RedisConcurrencyStore>,
}

impl ClientConcurrencyLimiter {
    pub fn from_env(feature_flags: FeatureFlagClient) -> Self {
        Self::with_config(ClientConcurrencyConfig::from_env(), feature_flags)
    }

    pub fn with_config(config: ClientConcurrencyConfig, feature_flags: FeatureFlagClient) -> Self {
        let known = KnownClients::new(&config.known_clients);
        if config.mode == Mode::Enforce && known.0.is_empty() {
            tracing::warn!(
                max_in_flight_unspecified = config.max_in_flight_unspecified,
                "client concurrency enforcement has an empty allowlist; clients without Eppo overrides share the unspecified bucket; configure SPACE_API_CLIENT_CONCURRENCY_KNOWN_CLIENTS before enforcing per-client limits"
            );
        }
        let store = build_store(config.redis_url.as_deref());
        Self {
            inner: Arc::new(Inner {
                config,
                known,
                feature_flags,
                store,
            }),
        }
    }

    pub fn config(&self) -> &ClientConcurrencyConfig {
        &self.inner.config
    }

    /// Whether the limiter should run, under the env-master-first policy:
    /// `SPACE_API_CLIENT_CONCURRENCY_MODE=off` is the hard off, and the flag can
    /// only disable a limiter the env var enabled. A deployment with no flag
    /// provider installed keeps the limiter its environment asked for; one
    /// whose provider is installed and cannot answer falls back to the flag's
    /// declaration, which is on.
    fn limiter_active(&self) -> bool {
        self.inner.feature_flags.gate_enabled(
            self.inner.config.mode != Mode::Off,
            SPACE_API_CLIENT_CONCURRENCY_LIMITER_FLAG,
            FLAG_SUBJECT,
        )
    }

    /// The runtime cap overrides from the Eppo quota flag, or `None` when the
    /// env master is off, the flag is unset, no provider is installed, or the
    /// value doesn't parse (all fall back to the env-configured caps).
    ///
    /// The env master is applied here as well as at [`Self::limiter_active`],
    /// even though today only an active limiter asks for overrides. Reading an
    /// override without its master is the shape that lets a provider widen
    /// something the environment turned off, and the policy is cheaper to hold
    /// at every read than to re-derive from the call order.
    fn quota_overrides(&self) -> Option<QuotaOverrides> {
        // `resolved()` collapses "master off" into "nothing resolved", which is
        // sound here because both answers land on the env-configured caps: the
        // overrides only ever retune a limiter that `limiter_active` already
        // gated on the same master.
        let value = self
            .inner
            .feature_flags
            .json_override(
                self.inner.config.mode != Mode::Off,
                SPACE_API_CLIENT_CONCURRENCY_QUOTAS_FLAG,
                FLAG_SUBJECT,
            )
            .resolved()?;
        match serde_json::from_value::<QuotaOverrides>(value) {
            Ok(overrides) => Some(overrides),
            Err(error) => {
                tracing::warn!(
                    %error,
                    flag = SPACE_API_CLIENT_CONCURRENCY_QUOTAS_FLAG.key(),
                    "invalid client-concurrency quota override JSON; falling back to env caps"
                );
                None
            }
        }
    }

    /// This limiter's liveness-heartbeat loop, for the caller to spawn.
    ///
    /// A flowing heartbeat means this process is running and its `counter` path
    /// reaches Datadog; it says nothing about whether a request was ever gated.
    /// Same shape and same guarantees as
    /// [`crate::cluster_admission::ClusterSandboxAdmission::liveness_heartbeat`].
    pub fn liveness_heartbeat(
        &self,
        shutdown_token: CancellationToken,
    ) -> impl Future<Output = ()> + Send + 'static {
        tracing::info!(
            mode = %self.inner.config.mode,
            interval_secs = crate::limiter_liveness::HEARTBEAT_INTERVAL.as_secs(),
            "per-client concurrency limiter liveness heartbeat starting"
        );
        crate::limiter_liveness::run_heartbeat(
            crate::limiter_liveness::HEARTBEAT_INTERVAL,
            shutdown_token,
            client_limit::heartbeat,
        )
    }

    /// Attempt to reserve a cluster-wide concurrency slot for `operation` on
    /// behalf of the client identified by `client_id` (the `X-Client-ID` header
    /// value, if any).
    pub async fn try_acquire(
        &self,
        operation: Operation,
        client_id: Option<&str>,
    ) -> ClientConcurrencyDecision {
        self.try_acquire_for(operation, client_id, self.inner.config.max_hold)
            .await
    }

    /// Reserve a slot whose stale-owner lease lasts at least `minimum_hold`.
    /// Long-polling handlers use this so a live request cannot outlast its
    /// cluster-wide concurrency record.
    pub async fn try_acquire_for(
        &self,
        operation: Operation,
        client_id: Option<&str>,
        minimum_hold: Duration,
    ) -> ClientConcurrencyDecision {
        let config = &self.inner.config;

        // Env-master-first: the env kill-switch is the hard off, and the flag
        // can only disable a limiter it enabled. Checked before touching
        // Redis. The metric scope and client id are derived against the static
        // allowlist only, since the override map isn't consulted here.
        if !self.limiter_active() {
            let key = ClientKey::classify(client_id, &self.inner.known, None);
            return admit_untracked(operation, Reason::Disabled, key.client_id_tag());
        }

        // Fetch the Eppo overrides once: they both extend the allowlist and
        // supply per-client caps.
        let overrides = self.quota_overrides();
        let key = ClientKey::classify(client_id, &self.inner.known, overrides.as_ref());
        let client_tag = key.client_id_tag();
        // Cap resolves from the Eppo override map, falling back to env. A zero
        // cap disables this bucket.
        let cap = config.cap_for(&key, overrides.as_ref());
        if cap == 0 {
            return admit_untracked(operation, Reason::Disabled, client_tag);
        }

        // No usable store: admit, but flag it as the pageable `misconfigured`
        // steady-state signal — the guardrail is silently absent.
        let Some(store) = self.inner.store.as_ref() else {
            return admit_untracked(operation, Reason::Misconfigured, client_tag);
        };

        let redis_key = key.redis_key();
        let member = Uuid::now_v7().to_string();
        // Observe mode admits over-cap requests, so it forces the slot in to keep
        // the cluster-wide count faithful; enforce lets the store reject.
        let force = config.mode == Mode::Observe;
        let max_hold = config.max_hold.max(minimum_hold);
        let key_ttl = max_hold + KEY_TTL_SLACK;

        match store
            .acquire(&redis_key, cap, max_hold, key_ttl, &member, force)
            .await
        {
            Ok(outcome) if !outcome.added => {
                // Only the enforce-over-cap path leaves the slot unadded.
                client_limit::decision(
                    operation,
                    Decision::Rejected,
                    Reason::ConcurrencyFull,
                    client_tag,
                );
                ClientConcurrencyDecision::Rejected {
                    retry_after: RETRY_AFTER,
                    client_id: client_tag,
                }
            }
            Ok(outcome) => {
                let (decision, reason) = if outcome.over_cap {
                    (Decision::WouldReject, Reason::ConcurrencyFull)
                } else {
                    (Decision::Admitted, Reason::None)
                };
                client_limit::decision(operation, decision, reason, client_tag);
                client_limit::in_flight(outcome.count as usize);
                ClientConcurrencyDecision::Admitted(ClientConcurrencyGuard {
                    slot: Some(Slot {
                        store: store.clone(),
                        redis_key,
                        member,
                    }),
                    client_id: client_tag,
                })
            }
            Err(error) => {
                // Fail open on any Redis error: the limiter must never be a
                // create-path SPOF. Alert on a sustained `store_unavailable` rate.
                tracing::warn!(
                    %error,
                    operation = %operation,
                    "client concurrency store unavailable; admitting (fail open)"
                );
                admit_untracked(operation, Reason::StoreUnavailable, client_tag)
            }
        }
    }
}

/// Build the shared store from the configured URL, or `None` (fail-open
/// `misconfigured`) when the URL is absent or the pool can't be built.
fn build_store(url: Option<&str>) -> Option<RedisConcurrencyStore> {
    let url = url.map(str::trim).filter(|url| !url.is_empty())?;
    match RedisConcurrencyStore::from_url(url) {
        Ok(store) => Some(store),
        Err(error) => {
            tracing::error!(
                %error,
                "failed to build Redis pool for client concurrency limiter; failing open"
            );
            None
        }
    }
}

/// Admit without reserving a slot (disabled / misconfigured / store-error). The
/// returned guard is a no-op that releases nothing.
fn admit_untracked(
    operation: Operation,
    reason: Reason,
    client_id: ClientIdTag,
) -> ClientConcurrencyDecision {
    client_limit::decision(operation, Decision::Admitted, reason, client_id);
    ClientConcurrencyDecision::Admitted(ClientConcurrencyGuard::noop(client_id))
}

struct Slot {
    store: RedisConcurrencyStore,
    redis_key: String,
    member: String,
}

/// RAII guard for one reserved concurrency slot. Dropping it releases the slot
/// (`ZREM`); a no-op guard (fail-open path) releases nothing. Because `Drop`
/// can't `await`, release is a detached `ZREM` — a failure just leaves the slot
/// to expire via its `max_hold` deadline.
#[must_use = "the slot is released as soon as the guard is dropped; hold it for the operation's duration"]
pub struct ClientConcurrencyGuard {
    slot: Option<Slot>,
    client_id: ClientIdTag,
}

impl ClientConcurrencyGuard {
    fn noop(client_id: ClientIdTag) -> Self {
        Self {
            slot: None,
            client_id,
        }
    }
}

impl Drop for ClientConcurrencyGuard {
    fn drop(&mut self) {
        let Some(slot) = self.slot.take() else {
            return;
        };
        // Release off the request path. If we're not inside a runtime (should
        // not happen from a handler) the slot simply expires via its deadline.
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                if let Err(error) = slot.store.release(&slot.redis_key, &slot.member).await {
                    tracing::debug!(
                        %error,
                        key = %slot.redis_key,
                        "client concurrency slot release failed; slot will expire via hold TTL"
                    );
                }
            });
        }
    }
}
