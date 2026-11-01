//! Per-client live cap: the most sandboxes with a live runtime one client key
//! may hold at once.
//!
//! A cap check counts the client's live sandboxes from two places
//! (RFC "Per-client admission independent of the substrate", Decision 5):
//!
//! - the substrate, through [`Substrate::live_sandboxes_for_client`]: on
//!   Kubernetes, the sandbox and restore pods the reflector shows labeled with
//!   the client's id. Direct can't count and answers unknown, so the cap
//!   admits there.
//! - the *pending set*, a short-lived Redis record of recent admissions whose
//!   pods no reflector has seen yet (`live_pending.lua`). Without it, a client
//!   could be admitted many times over the cap inside one watch delay.
//!
//! An admitted activation holds a [`LiveReservation`] until its pod exists or
//! it ends. The substrate counts only the visible sandboxes the pending set
//! doesn't already hold, so nothing counts twice.
//!
//! # Keys and caps
//!
//! A cap applies to a *key*, never a raw id: a listed id is its own key, and
//! every other ECW id shares `unknown`. `native` has no cap. The key set is the
//! env allowlist (`SPACE_API_CLIENT_CONCURRENCY_KNOWN_CLIENTS`) plus the ids
//! the quotas flag names. Caps come from the env defaults
//! `SPACE_API_CLIENT_LIVE_CAP_PER_CLIENT` (listed keys) and
//! `SPACE_API_CLIENT_LIVE_CAP_UNKNOWN`; unset or zero means no cap. The quotas
//! flag's `live` map overrides them per key, but only where the matching env
//! default is set: the flag tunes and assigns caps the environment enabled.
//! Listing an id in the flag moves it from `unknown` to its own key under the
//! per-client default, so it changes which cap applies, never whether a kind
//! of cap exists. A `0` entry is ignored.
//!
//! When the flag stops resolving, its last valid value stands for
//! `KEY_SET_STALENESS`, then the env allowlist alone lists ids. The RFC's
//! degraded mode (`SPACE_API_CLIENT_QUOTAS_FLAG_REQUIRED`, counting each
//! unlisted id on its own while degraded) belongs with the shared key
//! resolver and isn't implemented here.
//!
//! # Guardrail, not accounting
//!
//! Every failure fails open with its own metric reason: no usable Redis, a
//! Redis error (or a check whose every pass found its read too old, which
//! reports the same `store_unavailable`), an unsupported substrate,
//! attribution switched off, or a cold reflector. A component with no cap
//! configured emits nothing.
//! A stale reflector doesn't fail open: the check counts the pending set
//! alone, so a frozen index can't refuse and a burst still can't pass the cap
//! within one pending window. A cap that never fires can be told apart from
//! one that can't see.

use std::collections::{BTreeSet, HashMap};
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Duration;

use pplx_space_common_core::types::{ClientId, SandboxId};
use pplx_space_feature_flags::{FeatureFlagClient, SPACE_API_CLIENT_CONCURRENCY_QUOTAS_FLAG};
use pplx_space_metrics::sandbox::api::client_live_cap::{self, Check, Decision, Reason};

use crate::controllers::substrate::{
    CountedIds, LiveSandboxCount, LiveSandboxCountUnknown, Substrate,
};

mod pending_set;
mod reservation;
#[cfg(test)]
mod tests;

use pending_set::{Member, PendingRead, PendingSet, PendingTiming, ReserveVerdict};
pub use reservation::LiveReservation;

const PER_CLIENT_ENV: &str = "SPACE_API_CLIENT_LIVE_CAP_PER_CLIENT";
const UNKNOWN_ENV: &str = "SPACE_API_CLIENT_LIVE_CAP_UNKNOWN";
const REDIS_URL_ENV: &str = "SPACE_REDIS_URL";
const FLAG_SUBJECT: &str = "space-cluster";

/// How long a pending member counts after its last renewal. Once its pod
/// exists it only has to cover one watch delay.
const LIVE_PENDING_TTL: Duration = Duration::from_secs(30);
/// How long after its expiry a member is kept before pruning may delete it,
/// so pruning never removes a member between one check's read and its
/// reserve. A forget, or the last owner's release, still deletes a member
/// at once.
const LIVE_PENDING_GRACE: Duration = Duration::from_secs(60);
/// How long the last valid quotas-flag value stands in for a flag that
/// doesn't resolve, before the key set falls back to the env allowlist (RFC
/// Decision 2). A short provider outage changes no client's key.
const KEY_SET_STALENESS: Duration = Duration::from_secs(10 * 60);
/// Passes of read, count and reserve before a refusal that keeps finding new
/// members stands.
const MAX_CHECK_PASSES: usize = 3;
/// The earliest second a refused client's retry could succeed: a cap lifts
/// when the client releases a sandbox, which this can't predict, so retries
/// spread over the cluster limiter's jitter window above this floor.
const RETRY_AFTER_EARLIEST: Duration = Duration::from_secs(5);

/// The live cap's configuration. The default caps nothing.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ClientLiveCapConfig {
    /// The cap for a listed key without a flag override. `None`: listed keys
    /// aren't capped, and the flag can't cap them either.
    pub per_client: Option<NonZeroU64>,
    /// The cap of the shared `unknown` key. `None`: not capped.
    pub unknown: Option<NonZeroU64>,
    /// The env allowlist, part of the key set.
    pub known_clients: BTreeSet<String>,
    /// Whether the concurrency limiter's mode isn't `off`, so the ids its
    /// `overrides` map names join the key set.
    pub concurrency_overrides_active: bool,
    pub redis_url: Option<String>,
}

impl ClientLiveCapConfig {
    pub fn from_env() -> Self {
        let concurrency = crate::client_concurrency::ClientConcurrencyConfig::from_env();
        Self {
            per_client: parse_cap_env(PER_CLIENT_ENV),
            unknown: parse_cap_env(UNKNOWN_ENV),
            known_clients: concurrency.known_clients,
            concurrency_overrides_active: concurrency.mode != crate::client_concurrency::Mode::Off,
            redis_url: std::env::var(REDIS_URL_ENV)
                .ok()
                .map(|url| url.trim().to_string())
                .filter(|url| !url.is_empty()),
        }
    }

    fn any_cap(&self) -> bool {
        self.per_client.is_some() || self.unknown.is_some()
    }
}

fn parse_cap_env(name: &str) -> Option<NonZeroU64> {
    let raw = std::env::var(name).ok()?;
    parse_cap(name, &raw)
}

fn parse_cap(name: &str, raw: &str) -> Option<NonZeroU64> {
    match raw.trim().parse::<u64>() {
        Ok(value) => NonZeroU64::new(value),
        Err(error) => {
            tracing::warn!(var = name, %error, "invalid live cap; leaving it unset");
            None
        }
    }
}

/// The quotas flag's fields the live cap reads. Each decodes on its own, so a
/// malformed one drops only itself, and each accepts an object delivered as
/// text (flagd stringifies nested objects).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct QuotasFlag {
    /// Per-key live caps, `unknown` included.
    live: HashMap<String, u64>,
    /// The ids the concurrency limiter's `overrides` map names.
    override_ids: BTreeSet<String>,
}

impl QuotasFlag {
    fn parse(value: &serde_json::Value) -> Self {
        Self {
            // Decoded entry by entry, so one malformed entry drops only
            // itself rather than every override.
            live: decode_field::<HashMap<String, serde_json::Value>>(value, "live")
                .unwrap_or_default()
                .into_iter()
                .filter_map(|(id, cap)| match cap.as_u64() {
                    // A flag tunes a cap and never lifts one: lifting a cap
                    // is the environment's call.
                    Some(0) => {
                        tracing::warn!(client = %id, "ignoring a zero live cap in the client quotas flag");
                        None
                    }
                    Some(cap) => Some((id, cap)),
                    None => {
                        tracing::warn!(client = %id, value = %cap, "ignoring an invalid live cap in the client quotas flag");
                        None
                    }
                })
                .collect(),
            override_ids: decode_field::<HashMap<String, serde_json::Value>>(value, "overrides")
                .map(|overrides| overrides.into_keys().collect())
                .unwrap_or_default(),
        }
    }
}

fn decode_field<T: serde::de::DeserializeOwned>(
    value: &serde_json::Value,
    field: &str,
) -> Option<T> {
    let raw = value.get(field)?;
    let decoded = match raw {
        serde_json::Value::String(text) => serde_json::from_str(text),
        other => serde_json::from_value(other.clone()),
    };
    match decoded {
        Ok(decoded) => Some(decoded),
        Err(error) => {
            tracing::warn!(
                %error,
                field,
                flag = SPACE_API_CLIENT_CONCURRENCY_QUOTAS_FLAG.key(),
                "invalid client quotas flag field; ignoring it"
            );
            None
        }
    }
}

/// One cap check, resolved from one key-set snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
struct CapCheck {
    /// The resolved key, for metrics.
    key: String,
    ids: CountedIds,
    cap: NonZeroU64,
}

/// Turn `id` into the check its key needs, or `None` when its key has no
/// cap. `flag` is the quotas flag's value, if it resolved.
fn resolve(
    config: &ClientLiveCapConfig,
    flag: Option<&QuotasFlag>,
    id: &ClientId,
) -> Option<CapCheck> {
    if *id == ClientId::native() {
        return None;
    }
    let empty = QuotasFlag::default();
    let flag = flag.unwrap_or(&empty);
    let mut key_set: BTreeSet<ClientId> = config
        .known_clients
        .iter()
        .map(String::as_str)
        // An id the flag names, in `live` or `overrides`, is listed only while
        // listed keys are capped: an id listed with no per-client cap would
        // leave `unknown`'s cap for no cap, so a flag push would turn its cap
        // off.
        .chain(
            flag.live
                .keys()
                .map(String::as_str)
                .filter(|_| config.per_client.is_some()),
        )
        .chain(
            flag.override_ids
                .iter()
                .map(String::as_str)
                .filter(|_| config.concurrency_overrides_active && config.per_client.is_some()),
        )
        .filter(|candidate| {
            !ClientId::RESERVED
                .iter()
                .any(|reserved| reserved.eq_ignore_ascii_case(candidate))
        })
        .filter_map(|candidate| ClientId::parse(candidate).ok())
        .collect();
    // A flag tunes only a cap the environment turned on. Zero overrides were
    // dropped at parse, so an override never lifts a cap.
    let flag_cap = |key: &str, env: Option<NonZeroU64>| match flag.live.get(key) {
        Some(&value) => env.and(NonZeroU64::new(value)),
        None => env,
    };
    if key_set.contains(id) {
        return Some(CapCheck {
            key: id.as_str().to_string(),
            ids: CountedIds::Exactly(vec![id.clone()]),
            cap: flag_cap(id.as_str(), config.per_client)?,
        });
    }
    let cap = flag_cap(ClientId::UNKNOWN, config.unknown)?;
    key_set.remove(&ClientId::unknown());
    Some(CapCheck {
        key: ClientId::UNKNOWN.to_string(),
        ids: CountedIds::AllExcept(key_set.into_iter().collect()),
        cap,
    })
}

/// A client at its live cap.
#[derive(Debug, Clone, thiserror::Error)]
#[error("{message}")]
pub struct LiveCapRefusal {
    pub message: String,
    pub retry_after: Duration,
}

impl LiveCapRefusal {
    fn new(key: &str, cap: NonZeroU64) -> Self {
        Self {
            message: format!(
                "Client live sandbox cap reached: client key {key:?} is at its cap of {cap} \
                 sandboxes with a live runtime. Free one by suspending or deleting it (on \
                 the ECW API, pausing or killing it), then retry."
            ),
            retry_after: crate::cluster_admission::jittered_retry_after(RETRY_AFTER_EARLIEST),
        }
    }
}

#[derive(Clone)]
pub struct ClientLiveCap {
    inner: Arc<Inner>,
}

struct Inner {
    substrate: Substrate,
    store: Option<PendingSet>,
    config: ClientLiveCapConfig,
    feature_flags: FeatureFlagClient,
    timing: PendingTiming,
    last_flag: FlagRetention,
}

/// The last valid quotas-flag value and when it resolved.
#[derive(Default)]
struct FlagRetention {
    last: std::sync::Mutex<Option<(std::time::Instant, QuotasFlag)>>,
}

impl FlagRetention {
    /// The flag to resolve keys with at `now`: `fresh` when it resolved,
    /// otherwise the last valid value while it's younger than
    /// [`KEY_SET_STALENESS`], otherwise none (the env allowlist alone).
    fn get(&self, fresh: Option<QuotasFlag>, now: std::time::Instant) -> Option<QuotasFlag> {
        let mut last = self
            .last
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match fresh {
            Some(flag) => {
                *last = Some((now, flag.clone()));
                Some(flag)
            }
            None => last
                .as_ref()
                .filter(|(at, _)| now.saturating_duration_since(*at) <= KEY_SET_STALENESS)
                .map(|(_, flag)| flag.clone()),
        }
    }
}

fn emit(check: Check, decision: Decision, reason: Reason, key: &str) {
    client_live_cap::decision(check, decision, reason, key);
}

fn unknown_reason(reason: LiveSandboxCountUnknown) -> Reason {
    match reason {
        LiveSandboxCountUnknown::Unsupported => Reason::Unsupported,
        LiveSandboxCountUnknown::PodCacheCold => Reason::PodCacheCold,
        LiveSandboxCountUnknown::PodCacheStale => Reason::PodCacheStale,
    }
}

impl ClientLiveCap {
    pub fn new(substrate: Substrate, feature_flags: FeatureFlagClient) -> Self {
        Self::with_config(substrate, ClientLiveCapConfig::from_env(), feature_flags)
    }

    pub fn with_config(
        substrate: Substrate,
        config: ClientLiveCapConfig,
        feature_flags: FeatureFlagClient,
    ) -> Self {
        let store = match config.redis_url.as_deref() {
            Some(url) if config.any_cap() => match PendingSet::from_url(url) {
                Ok(store) => Some(store),
                Err(error) => {
                    tracing::error!(%error, "failed to build the live cap's Redis pool; failing open");
                    None
                }
            },
            _ => None,
        };
        Self {
            inner: Arc::new(Inner {
                substrate,
                store,
                config,
                feature_flags,
                timing: PendingTiming {
                    ttl: LIVE_PENDING_TTL,
                    grace: LIVE_PENDING_GRACE,
                },
                last_flag: FlagRetention::default(),
            }),
        }
    }

    /// This cap with its pending set under its own prefix, and `timing`.
    #[cfg(test)]
    fn isolated(mut self, name: &str, timing: PendingTiming) -> Self {
        let inner = Arc::get_mut(&mut self.inner).expect("isolated before the cap is shared");
        inner.store = inner.store.take().map(|store| store.isolated(name));
        inner.timing = timing;
        self
    }

    fn check_for(&self, client: &ClientId) -> Option<CapCheck> {
        let config = &self.inner.config;
        if !config.any_cap() {
            return None;
        }
        let flag = self
            .inner
            .feature_flags
            .json_override(
                config.any_cap(),
                SPACE_API_CLIENT_CONCURRENCY_QUOTAS_FLAG,
                FLAG_SUBJECT,
            )
            .resolved()
            .map(|value| QuotasFlag::parse(&value));
        let flag = self.inner.last_flag.get(flag, std::time::Instant::now());
        resolve(config, flag.as_ref(), client)
    }

    /// Whether the substrate can count this check: it labels pods with their
    /// activating client, and answers a per-client count at all. Without
    /// either there's nothing to count but the pending set's last half
    /// minute, which would cap a rate rather than live sandboxes, so the
    /// check admits before any Redis round trip. The probe counts no ids, so
    /// on Kubernetes it's an O(1) cache read that only tells whether the
    /// substrate can count at all.
    fn counts_clients(&self, check: Check, cap: &CapCheck) -> bool {
        let substrate = &self.inner.substrate;
        let reason = if !substrate.activation_client_attribution().is_enabled() {
            Reason::AttributionDisabled
        } else if matches!(
            substrate.live_sandboxes_for_client(
                &CountedIds::Exactly(Vec::new()),
                None,
                &std::collections::HashSet::new()
            ),
            LiveSandboxCount::Unknown(LiveSandboxCountUnknown::Unsupported)
        ) {
            Reason::Unsupported
        } else {
            return true;
        };
        emit(check, Decision::Admitted, reason, &cap.key);
        false
    }

    /// Reserve a live slot for `client` activating `sandbox`, before any token
    /// is spent. `Ok(None)` admits without a reservation: no cap applies, or
    /// the check failed open. `Ok(Some(_))` admits; hold the reservation until
    /// the activation ends.
    pub async fn reserve(
        &self,
        client: &ClientId,
        sandbox: SandboxId,
    ) -> Result<Option<LiveReservation>, LiveCapRefusal> {
        let Some(check) = self.check_for(client) else {
            return Ok(None);
        };
        if !self.counts_clients(Check::Reserve, &check) {
            return Ok(None);
        }
        let Some(store) = self.inner.store.as_ref() else {
            emit(
                Check::Reserve,
                Decision::Admitted,
                Reason::Misconfigured,
                &check.key,
            );
            return Ok(None);
        };
        let owner = uuid::Uuid::now_v7().simple().to_string();
        let member = Member {
            client,
            sandbox,
            owner: &owner,
        };
        let mut contended = false;
        for _ in 0..MAX_CHECK_PASSES {
            let (read, visible, degraded) = match self.read_and_count(store, &check, sandbox).await
            {
                Ok(counted) => counted,
                Err(reason) => {
                    emit(Check::Reserve, Decision::Admitted, reason, &check.key);
                    return Ok(None);
                }
            };
            let verdict = store
                .reserve(
                    &check.ids,
                    &read,
                    &member,
                    check.cap.get(),
                    visible,
                    self.inner.timing,
                )
                .await;
            match verdict {
                Ok(verdict @ (ReserveVerdict::Admitted | ReserveVerdict::Renewed)) => {
                    let reason = match (verdict, degraded) {
                        (ReserveVerdict::Renewed, _) => Reason::AlreadyPending,
                        (_, Some(degraded)) => degraded,
                        _ => Reason::UnderCap,
                    };
                    emit(Check::Reserve, Decision::Admitted, reason, &check.key);
                    return Ok(Some(LiveReservation::hold(
                        store.clone(),
                        client.clone(),
                        sandbox,
                        owner,
                        self.inner.timing,
                    )));
                }
                Ok(ReserveVerdict::Refused) => {
                    emit(
                        Check::Reserve,
                        Decision::Rejected,
                        Reason::AtCap,
                        &check.key,
                    );
                    return Err(LiveCapRefusal::new(&check.key, check.cap));
                }
                Ok(ReserveVerdict::Retry) => {
                    contended = true;
                    continue;
                }
                // The read and the script ran too far apart to agree: redo
                // the check. Its own timing, not the client's count.
                Ok(ReserveVerdict::Stale) => continue,
                Err(error) => {
                    tracing::warn!(%error, "live cap pending set unavailable; admitting (fail open)");
                    emit(
                        Check::Reserve,
                        Decision::Admitted,
                        Reason::StoreUnavailable,
                        &check.key,
                    );
                    return Ok(None);
                }
            }
        }
        if !contended {
            // Every pass went stale: an internal timing failure, so fail
            // open like the other store failures.
            tracing::warn!("live cap checks kept going stale; admitting (fail open)");
            emit(
                Check::Reserve,
                Decision::Admitted,
                Reason::StoreUnavailable,
                &check.key,
            );
            return Ok(None);
        }
        emit(
            Check::Reserve,
            Decision::Rejected,
            Reason::Contended,
            &check.key,
        );
        Err(LiveCapRefusal::new(&check.key, check.cap))
    }

    /// A read-only check of the same count, for a path that's about to spend
    /// a deferred charge before it knows whether it creates a pod: refuse
    /// before anything is spent when `client` is already at its cap.
    pub async fn precheck(
        &self,
        client: &ClientId,
        sandbox: SandboxId,
    ) -> Result<(), LiveCapRefusal> {
        let Some(check) = self.check_for(client) else {
            return Ok(());
        };
        if !self.counts_clients(Check::Precheck, &check) {
            return Ok(());
        }
        let Some(store) = self.inner.store.as_ref() else {
            emit(
                Check::Precheck,
                Decision::Admitted,
                Reason::Misconfigured,
                &check.key,
            );
            return Ok(());
        };
        match self.read_and_count(store, &check, sandbox).await {
            Ok((read, visible, degraded)) => {
                let pending = read
                    .members
                    .iter()
                    .filter(|member| **member != sandbox)
                    .count();
                let total = visible.saturating_add(pending as u64);
                if total >= check.cap.get() {
                    emit(
                        Check::Precheck,
                        Decision::Rejected,
                        Reason::AtCap,
                        &check.key,
                    );
                    Err(LiveCapRefusal::new(&check.key, check.cap))
                } else {
                    emit(
                        Check::Precheck,
                        Decision::Admitted,
                        degraded.unwrap_or(Reason::UnderCap),
                        &check.key,
                    );
                    Ok(())
                }
            }
            Err(reason) => {
                emit(Check::Precheck, Decision::Admitted, reason, &check.key);
                Ok(())
            }
        }
    }

    /// Read the pending members and count the visible sandboxes not among
    /// them. `Err` carries the fail-open reason.
    ///
    /// A stale pod cache counts no visible sandboxes, and the third value
    /// says so: the check then bounds admissions by the pending set alone. A
    /// frozen index must not refuse, since it may still hold sandboxes the
    /// client has deleted, and the pending set still keeps the admissions of
    /// one pending window within the cap.
    async fn read_and_count(
        &self,
        store: &PendingSet,
        check: &CapCheck,
        sandbox: SandboxId,
    ) -> Result<(PendingRead, u64, Option<Reason>), Reason> {
        let read = store.read(&check.ids).await.map_err(|error| {
            tracing::warn!(%error, "live cap pending set unavailable; admitting (fail open)");
            Reason::StoreUnavailable
        })?;
        let (visible, degraded) = match self.inner.substrate.live_sandboxes_for_client(
            &check.ids,
            Some(sandbox),
            &read.members,
        ) {
            LiveSandboxCount::Known(visible) => (visible, None),
            LiveSandboxCount::Unknown(LiveSandboxCountUnknown::PodCacheStale) => {
                (0, Some(Reason::PodCacheStale))
            }
            LiveSandboxCount::Unknown(reason) => return Err(unknown_reason(reason)),
        };
        Ok((read, visible, degraded))
    }

    /// Stop counting `sandbox`'s pending members for every client, once its
    /// pods are deleted. Best-effort: a failure leaves them to lapse.
    pub async fn forget(&self, sandbox: SandboxId) {
        let Some(store) = self.inner.store.as_ref() else {
            return;
        };
        if let Err(error) = store.forget(sandbox).await {
            tracing::warn!(%error, %sandbox, "failed to forget a sandbox's live-cap pending members");
        }
    }
}
