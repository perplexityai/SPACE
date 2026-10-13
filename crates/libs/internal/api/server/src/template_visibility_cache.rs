//! Last-known-visibility cache for the release-resolution RPCs.
//!
//! [`super::metadata::template_visibility`] reads `templates.visibility` as
//! its own statement on every release resolution, and degrades every failure
//! to `UNSPECIFIED`.
//!
//! Prefetch uses this field to skip candidates not known to be generic. It is
//! advisory: final prefetch resolution also requires generic-only selection in
//! the same SQL snapshot as the release and storage locations, independent of
//! this cache. A cached Generic value cannot authorize private artifacts after
//! a visibility transition.
//!
//! ## Not the same thing as `visibility_pool`
//!
//! `ClusterApiService::visibility_pool` sidelines the lookup onto its own small
//! pool so it cannot starve release resolution. That protects *release
//! resolution from this lookup*. This cache protects *the lookup's consumers
//! from a failed lookup*. Neither substitutes for the other, and the sideline
//! makes the cache matter more, not less. That pool is 3 connections with a
//! 500ms `statement_timeout` of its own (`VISIBILITY_POOL_MAX_CONNECTIONS` and
//! `VISIBILITY_POOL_STATEMENT_TIMEOUT` in `space-internal-api-svc`, the binary
//! the `space-cluster-api` chart deploys), against the
//! role's 10s — so contention on `templates` now cancels this lookup in half a
//! second, where the role's bound left twenty times as much room to wait the
//! lock out and still answer. That is
//! the trade the sideline deliberately buys: it converts a latency risk to
//! release resolution into a higher `UNSPECIFIED` rate here. Every one of those
//! is an answer a visibility-gated consumer must fail closed on, which is what
//! this cache absorbs.
//!
//! ## Three invariants, in the order they matter
//!
//! 1. **Only successful, concrete reads are cached.** The stored type is the
//!    domain [`TemplateVisibility`], which has no `Unspecified` variant by
//!    design — so an unknown answer is not merely "not cached", it is
//!    *unrepresentable* here. A failure can never overwrite the last known truth
//!    *with a visibility*, because it has nothing to write. It can still
//!    **erase** it: the read-the-row-and-it's-unusable arms invalidate, which is
//!    invariant 2's job and is the fail-closed direction.
//!
//! 2. **Staleness is served only when the row could not be reached, never
//!    when it was read and its answer was unusable.** Note the scope: this
//!    governs the *query* path. A fresh hit answers without consulting the row
//!    at all, so a template deleted (or hand-edited to an invalid value) inside
//!    the freshness window is not noticed until the entry expires — up to
//!    [`CacheConfig::ttl`] late. That is inherent to caching rather than a hole
//!    in the rule, but it does mean the "a deleted template reports
//!    UNSPECIFIED" contract on `ListLatestTemplateReleasesResponse` is now
//!    eventually-consistent, and the proto says so. A *transient* `Err(sqlx)` —
//!    pool exhaustion, a cancelled statement, a lock wait behind
//!    `ALTER TABLE templates` — means "we don't know what the row says", and the
//!    last known value is the best available answer. Not every `Err` qualifies:
//!    a dropped column (`42703`) or table (`42P01`) is a *standing* condition,
//!    so it refuses the fallback and erases the entry instead. Which side an
//!    error lands on is decided by `may_serve_stale` / `should_invalidate` in
//!    [`super::metadata`], not by `Err` versus `Ok`. A missing row (`Ok(None)`) or an
//!    unparseable value is different in kind: the row *was* read, and its
//!    answer changed to something this cache must not paper over. A deleted
//!    template must report `UNSPECIFIED` (the `ListLatestTemplateReleases`
//!    response-level field documents exactly that), and an unparseable value
//!    means the column's CHECK was bypassed — someone is editing that row by
//!    hand, which is the last moment to start trusting a cached `generic`.
//!    Both **invalidate** instead. See [`TemplateVisibilityCache::invalidate`].
//!
//! 3. **Staleness is bounded by a hard ceiling.** Past it, the cache stops
//!    answering and the lookup degrades to `UNSPECIFIED` as before. That is
//!    what keeps a *sustained* outage fail-closed while a blip is absorbed —
//!    the property a static allowlist carve-out would have given up
//!    permanently.
//!
//! ## Isolation cost, stated plainly
//!
//! Two exposures, and the first is unconditional — say it first, because it is
//! the one a reviewer signing off on isolation risk needs.
//!
//! **On a healthy database**, a narrowing flip (`generic` → `org_specific`) is
//! invisible for up to [`CacheConfig::ttl`] per replica: a read taken an instant
//! before the flip is served for the rest of its freshness window. No ordering
//! mechanism inside the cache can shorten this — the read was legitimately
//! correct when taken — so the TTL *is* the flip-visibility bound, not a
//! detail of it.
//!
//! **While the read is failing**, the bound extends to
//! [`CacheConfig::stale_ceiling`]. `from_env` clamps the ceiling up to at least
//! the TTL, so the worst case is `max(ttl, stale_ceiling)` and never something
//! larger hidden behind an independently-tuned knob.
//!
//! Two things bound both cases today. Prefetch only ever asks about
//! the names hardcoded in its `PREFETCH_TAG_TUPLES` const, so the exposure is
//! that fixed handful of platform templates and nothing else. And the admin
//! flip endpoint's narrowing residue gate refuses the flip outright while any
//! node still caches the rootfs (`409`, counting `node_template_inventory`) —
//! which is the standing state of a template that prefetch is warming, since every
//! node holding the build publishes an inventory row for it.
//!
//! The deferred visibility-flip work inherits one requirement from this file:
//! a narrowing flip must wait out the ceiling — or invalidate explicitly —
//! before it can declare residue-free. That path already has to quiesce
//! prefetch, so this is one more input to a gate it must build anyway.
//!
//! ## Configuration (env vars, read once at construction)
//!
//! - `SPACE_INTERNAL_API_VISIBILITY_CACHE_TTL_SECS` — how long a read stays
//!   fresh enough to answer without querying. **Default 0: disabled.**
//! - `SPACE_INTERNAL_API_VISIBILITY_CACHE_STALE_CEILING_SECS` — how long a read
//!   stays usable as a fallback *after* a failed query. **Default 0: disabled.**
//! - The entry cap is fixed at 1024. It only matters once a window is open.
//!
//! **Unset means off on every startup path**, including direct invocations
//! outside Helm. Enabling either window requires explicit configuration.
//! The documented and tested 30s / 600s values are examples, not defaults.
//!
//! Either window at `0` disables that half independently. An in-band zero is
//! safe for these two because forgetting the check degrades to the *stricter*
//! behavior (query every time; never serve stale), not to a plausible-but-wrong
//! one. `ttl = 0` with a live ceiling is a coherent middle setting: no staleness
//! on the happy path, resilience only on the sad one. The reverse —  a live TTL
//! with `ceiling = 0` — does **not** stop cached answers, because the freshness
//! window is checked before the query; stopping them entirely takes both.
//!
//! Both windows off means no reads answered and no writes either: both
//! [`TemplateVisibilityCache::store`] and
//! [`TemplateVisibilityCache::invalidate`] short-circuit before taking the map's
//! lock, so it never enters a path that had no lock before the cache existed.
//!
//! ## Two things this deliberately does not do
//!
//! **No single-flight.** Concurrent callers that all find the entry expired
//! all issue the query. That is exactly today's behavior, at a small fraction
//! of today's rate, so coalescing would add a mechanism to fix something the
//! cache already made smaller.
//!
//! **No process-global state.** The cache hangs off `ClusterApiService`, so
//! each service instance owns one. A `static` would be marginally less
//! plumbing and would leak one test's reads into the next in a shared-process
//! test binary — the tests here and in `super::tests` depend on a fresh cache
//! per fixture. The map itself is behind an `Arc`, because tonic clones the
//! service and a per-clone map would scatter the cache across connections
//! instead of sharing one.

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use pplx_space_common_core::types::TemplateVisibility;
use time::OffsetDateTime;

/// Both windows default to **disabled**. Unset means off, everywhere.
///
/// The fail-safe belongs in the binary: omitting an environment key in any
/// launcher must disable the cache. Chart-only defaults cannot guarantee that.
/// The documented and tested 30s / 600s values are examples, not defaults.
const DEFAULT_TTL: Duration = Duration::ZERO;
const DEFAULT_STALE_CEILING: Duration = Duration::ZERO;
/// Entry cap when unset. Unlike the windows this has no "off" meaning — it only
/// matters once a window is open — so it keeps a real value.
const DEFAULT_MAX_ENTRIES: NonZeroUsize = NonZeroUsize::new(1024).expect("1024 is non-zero");

/// Longest `templates.name` this will use as a cache key.
///
/// Turns `max_entries` into a bounded footprint rather than a count with an
/// unbounded per-entry cost.
///
/// Sized against the **name validator**, not against the prefetch set: template
/// names are rejected above 255 bytes
/// (`common/core/src/types/template/identity.rs`), and only a successful read
/// reaches [`TemplateVisibilityCache::store`], so every cacheable key came
/// through that validator and fits with one byte to spare. That one byte is the
/// whole headroom — if the validator's limit ever rises, this must rise with it,
/// or templates with longer names silently re-query on every resolution while
/// `outcome:refreshed` sits at 100% and looks healthy.
const MAX_KEY_LEN: usize = 256;

/// Plausibility ceiling for either duration knob.
///
/// Not a tuning limit — a typo guard. Both knobs bound how long a stale
/// visibility may be served, and `with_ceiling_at_least_ttl` propagates the TTL
/// onto the ceiling, so a single fat-fingered `600000` (meaning 600, or meaning
/// milliseconds) would otherwise serve a pre-flip `generic` for 6.9 days per
/// replica — exactly the permanent carve-out invariant 3 exists to prevent. An
/// hour is far above any value these knobs have a reason to hold and far below
/// any duration worth arguing about.
const MAX_WINDOW: Duration = Duration::from_secs(3600);

const ENV_TTL_SECS: &str = "SPACE_INTERNAL_API_VISIBILITY_CACHE_TTL_SECS";
const ENV_STALE_CEILING_SECS: &str = "SPACE_INTERNAL_API_VISIBILITY_CACHE_STALE_CEILING_SECS";

/// Freshness and capacity bounds for [`TemplateVisibilityCache`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CacheConfig {
    /// Age below which a stored read answers without querying. `ZERO`
    /// disables the fresh path: every call queries.
    pub(crate) ttl: Duration,
    /// Age below which a stored read answers *after* a failed query. `ZERO`
    /// disables that fallback, so a failed query degrades immediately.
    ///
    /// It does **not** stop cached answers on its own: `get_fresh` runs before
    /// the query and consults [`Self::ttl`], so with a live TTL an entry keeps
    /// answering without issuing a statement at all. To force live reads, set
    /// both windows to zero — which is what the chart comments tell an operator
    /// to do.
    pub(crate) stale_ceiling: Duration,
    /// Entry cap. When it binds, [`evict_oldest`] drops the
    /// least-recently-*observed* entry.
    ///
    /// Not "a safety valve that won't bind", which an earlier revision claimed on
    /// the premise that `templates` is operator-managed and small. It is not:
    /// `RegisterTemplate` inserts a row per user template build, and this cache
    /// is keyed on every template a caller *resolves*, not on the handful of
    /// hardcoded prefetch names. An org with thousands of templates doing steady
    /// sandbox creates will pin the map at the cap, and because an expired entry
    /// is never reclaimed (only out-of-window), every subsequent new key pays a
    /// full linear scan under the write guard. That is microseconds at 1024 and
    /// why the cap is fixed, but it is a working limit, not a
    /// valve.
    ///
    /// Together with [`MAX_KEY_LEN`] it makes the footprint bounded rather than
    /// open-ended: only a successful read reaches
    /// [`TemplateVisibilityCache::store`], so every key names a row that exists,
    /// and an over-long name is refused a slot outright. `max_entries ×
    /// MAX_KEY_LEN` is the dominant *term*, not the total — each slot also carries
    /// two timestamps, an enum and the map's bucket overhead, so the real figure
    /// at defaults is a few hundred KB rather than the 256 KB the product
    /// suggests.
    pub(crate) max_entries: NonZeroUsize,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            ttl: DEFAULT_TTL,
            stale_ceiling: DEFAULT_STALE_CEILING,
            max_entries: DEFAULT_MAX_ENTRIES,
        }
    }
}

impl CacheConfig {
    /// Read the three knobs from the environment, then clamp `stale_ceiling` up
    /// to at least `ttl`.
    ///
    /// Any unset or unparseable entry falls back to its default. A malformed
    /// value is not fatal: this is a load-and-resilience optimization, and
    /// refusing to start over a typo in it would be a worse outcome than running
    /// with the default.
    ///
    /// Without the clamp the two knobs are independent and the real bound on how
    /// old an answer can be is `max(ttl, stale_ceiling)`, because both windows
    /// funnel through the same comparison — so raising the TTL to shed database
    /// load would silently extend the isolation window past the ceiling that is
    /// documented as bounding it, on a perfectly healthy database. Clamping
    /// rather than rejecting keeps a mid-incident config edit from failing
    /// startup, and it moves in the safe direction: it can only make the
    /// ceiling agree with a bound the operator has already accepted by setting
    /// the TTL.
    pub(crate) fn from_env() -> Self {
        Self::parse_env().with_ceiling_at_least_ttl()
    }

    /// Raise a *non-zero* `stale_ceiling` to at least `ttl`. Pure so the
    /// invariant has its own test; see [`Self::from_env`] for why it exists.
    ///
    /// Zero is exempt, and that exemption is not a special case bolted on — zero
    /// means "disabled", which is a different kind of value from "600 seconds".
    /// Clamping it would silently turn the documented per-window kill switch into
    /// a 30-second fallback: an operator who set the ceiling to `0` would get a
    /// stale-on-failure window back without asking for one. (Stopping cached
    /// answers *entirely* takes both windows — see [`Self::stale_ceiling`].)
    fn with_ceiling_at_least_ttl(self) -> Self {
        if self.stale_ceiling.is_zero() || self.stale_ceiling >= self.ttl {
            return self;
        }
        tracing::warn!(
            ttl_secs = self.ttl.as_secs(),
            stale_ceiling_secs = self.stale_ceiling.as_secs(),
            "visibility-cache staleness ceiling is below the TTL; raising it to the TTL \
             (the TTL already permits answers that old)"
        );
        Self {
            stale_ceiling: self.ttl,
            ..self
        }
    }

    fn parse_env() -> Self {
        Self {
            ttl: parse_duration_secs(ENV_TTL_SECS, std::env::var(ENV_TTL_SECS).ok().as_deref())
                .unwrap_or(DEFAULT_TTL),
            stale_ceiling: parse_duration_secs(
                ENV_STALE_CEILING_SECS,
                std::env::var(ENV_STALE_CEILING_SECS).ok().as_deref(),
            )
            .unwrap_or(DEFAULT_STALE_CEILING),
            max_entries: DEFAULT_MAX_ENTRIES,
        }
    }
}

/// `None` for absent or unparseable input, so the caller applies its default.
///
/// Unset and invalid are distinguished, and invalid warns, because silently
/// defaulting a rejected value leaves no trace that the edit did not take: an
/// operator raising the ceiling mid-incident types `6OO` rather than `600` and
/// gets the default with nothing in the process saying so. The *rejected* value
/// is deliberately not echoed — see the `Err` arm — so the warn carries the
/// variable name, the parse error and the value's length. A value that parsed
/// and was merely capped is echoed, because there it is provably digits.
///
/// Note which direction that fails in, since an earlier revision's version of
/// this note had it backwards. With [`DEFAULT_STALE_CEILING`] and
/// [`DEFAULT_TTL`] both `Duration::ZERO`, a typo in a *disabling* edit still
/// disables — the kill switch cannot be defeated by mistyping it, which is the
/// point of the zero defaults. The trap that survives is not this function's:
/// mistyping `staleCeilingSeconds` while `ttlSeconds` stays non-zero leaves the
/// *freshness* window answering from cache, which the values files call out.
///
/// Pure apart from the log, taking the raw string rather than reading the
/// variable, so the parsing has tests that don't mutate process environment —
/// `cargo test` shares one process across tests, where an env write races every
/// other test in the binary.
fn parse_duration_secs(name: &str, raw: Option<&str>) -> Option<Duration> {
    let raw = raw?;
    match raw.trim().parse::<u64>() {
        Ok(secs) => {
            let parsed = Duration::from_secs(secs);
            if parsed > MAX_WINDOW {
                tracing::warn!(
                    variable = name,
                    value = raw,
                    max_secs = MAX_WINDOW.as_secs(),
                    "visibility-cache window exceeds the plausibility ceiling; capping it \
                     (a value this large is a typo, and it would extend the isolation window)"
                );
                return Some(MAX_WINDOW);
            }
            Some(parsed)
        }
        Err(error) => {
            // Length, not the value. Reaching this arm means `raw` did *not*
            // parse, so it is arbitrary text from the pod environment — a
            // mis-pasted secret would otherwise be echoed verbatim into the
            // container-log pipeline and on to Datadog, since this is a `warn`.
            // The capping arm above keeps `value = raw` deliberately: it is only
            // reached after a successful `u64` parse, so the value there is
            // provably digits and cannot carry a secret. Do not "make these
            // consistent" in either direction.
            tracing::warn!(
                variable = name,
                value_len = raw.len(),
                error = %error,
                "ignoring unparseable visibility-cache duration; using the default"
            );
            None
        }
    }
}

/// One cached read: what the row said, when we asked, and when the *server*
/// says it answered.
///
/// The two instants are not redundant and are not interchangeable.
/// `asked_at` is a local `Instant` and measures **age** — how stale this entry
/// is allowed to get. `observed_at` is Postgres's own `clock_timestamp()` and
/// establishes **order** — which of two concurrent reads saw the row later.
/// One value cannot do both jobs: a local instant taken before the pool acquire
/// can precede another caller's by seconds and still have snapshotted the row
/// *after* it, so ordering on it inverts exactly under the contention this cache
/// exists for.
///
/// Be precise about how strong `observed_at` is, because it is weaker than a
/// snapshot id. Under READ COMMITTED the statement's snapshot is taken at portal
/// start, while `clock_timestamp()` is evaluated during execution, so the two can
/// straddle a commit: a reader whose snapshot predates a flip can stamp after it
/// and win. **The ordering is therefore correct only to within one statement's
/// execution time**, not absolutely. See [`TemplateVisibilityCache::store`] for
/// why that residual is accepted rather than closed.
#[derive(Clone, Copy, Debug)]
struct Entry {
    visibility: TemplateVisibility,
    asked_at: Instant,
    observed_at: OffsetDateTime,
}

/// Per-template last-known `templates.visibility`, with a fresh window and a
/// bounded stale window. Cheap to clone — clones share one map.
#[derive(Clone, Debug)]
pub(crate) struct TemplateVisibilityCache {
    entries: Arc<RwLock<HashMap<String, Entry>>>,
    config: CacheConfig,
}

impl TemplateVisibilityCache {
    pub(crate) fn new(config: CacheConfig) -> Self {
        Self {
            entries: Arc::new(RwLock::new(HashMap::new())),
            config,
        }
    }

    pub(crate) fn from_env() -> Self {
        Self::new(CacheConfig::from_env())
    }

    /// The value to answer with *without querying*, if one is fresh enough.
    ///
    /// Pass the instant the lookup *started*. [`Self::get_stale`] deliberately
    /// takes a freshly-read one instead: the ceiling bounds how stale an answer
    /// may be when it is given, and a stalled query can add its whole statement
    /// timeout to the age. So the two are not meant to agree, and threading one
    /// instant into both would quietly widen the ceiling by the query's
    /// duration.
    pub(crate) fn get_fresh(&self, template: &str, now: Instant) -> Option<TemplateVisibility> {
        self.get_within(template, self.config.ttl, now)
    }

    /// The value to fall back on after a query that could not reach the row.
    /// Only ever called on `Err`; see invariant 2 in the module docs.
    pub(crate) fn get_stale(&self, template: &str, now: Instant) -> Option<TemplateVisibility> {
        self.get_within(template, self.config.stale_ceiling, now)
    }

    /// Record a successful, concrete read, unless a later observation already
    /// landed.
    ///
    /// The ordering check is not defensive padding — without it this is a lost
    /// update in the unsafe direction. Refreshes are deliberately not
    /// single-flighted, so two callers can be in flight on the same expired key
    /// at once. If the earlier observation saw `generic`, a narrowing flip then
    /// committed, and the later one saw and stored `org_specific`, an
    /// unconditional insert would let the earlier caller finish last and put
    /// `generic` back — serving a value the database had already contradicted,
    /// for a full window, on the axis where being wrong costs isolation rather
    /// than availability.
    ///
    /// It orders on `observed_at` — Postgres's `clock_timestamp()`, taken while
    /// the statement ran — and **not** on any local clock. A local instant is
    /// captured before the pool acquire, so under contention (this lookup runs
    /// on a 3-connection pool) a caller that started earlier can snapshot the
    /// row *later* than one that started after it. Ordering on request-start
    /// therefore inverts precisely when it matters, and would reject the fresher
    /// observation. Two instants, two jobs: see [`Entry`].
    ///
    /// **A mitigation, not a guarantee, and the residual is bounded by the TTL
    /// rather than by this comparison.** The server clock is evaluated during
    /// statement execution while the snapshot is taken at portal start, so a
    /// reader that snapshotted before a flip can still stamp after it and win.
    /// Two sound alternatives were considered and rejected. Single-flighting
    /// refreshes per key closes it completely, but serializes every caller for
    /// that template behind one query — on the path that also serves
    /// sandbox-create pulls, trading a bounded staleness risk for an unbounded
    /// latency one. Ordering on the row's `xmin` would order by data version
    /// rather than by clock, but `xid` is 32-bit and wraparound-safe comparison
    /// is more machinery than a sub-TTL improvement earns. Note also what does
    /// *not* help: projecting `clock_timestamp()` after the subselect would make
    /// a blocked reader's stamp *later*, i.e. more likely to win with stale data,
    /// and target-list evaluation order is unspecified anyway.
    ///
    /// Why the residual is tolerable: a read that was legitimately correct an
    /// instant before a flip is cached for its full freshness window regardless,
    /// so the TTL is the flip-visibility bound and no ordering rule can shorten
    /// it. This comparison only decides which pre-flip value wins inside a window
    /// that exists either way.
    ///
    /// A rejected observation is dropped, not acted on — even when it *disagrees*
    /// with the resident one. An earlier revision removed the entry on
    /// disagreement, reasoning that two reads which differ cannot be ordered by a
    /// clock we may not trust. That was wrong, and wrong in the dangerous
    /// direction: the predicate fires on the ordinary forward-clock straggler race
    /// this ordering exists to handle, where the resident value is the *correct*
    /// one. A pre-flip reader landing after a post-flip reader would delete the
    /// post-flip `org_specific`, and a third, even older reader would then find no
    /// entry, hit the insert arm with no ordering state, and install the pre-flip
    /// `generic` as fresh. The arm meant to stop a contradicted value being served
    /// was the thing that let it be served.
    ///
    /// One failure mode this ordering has that a local clock would not, and which
    /// neither the TTL nor "one statement's execution" bounds: if the database's
    /// clock steps **backwards** — an NTP correction, or a failover to a standby
    /// whose clock trails — every subsequent observation stamps earlier than the
    /// resident entry and is discarded, for as long as the step lasts. The cache
    /// goes inert and every request queries, which is the safe direction (it is
    /// the pre-cache behavior). What it is not is *visible*: `outcome:refreshed`
    /// fires on reaching the database, not on the store landing, so a cache that
    /// is silently rejecting every write looks identical to one absorbing load.
    /// If the `fresh` share ever collapses to zero while `refreshed` holds
    /// steady, this is the thing to suspect.
    pub(crate) fn store(
        &self,
        template: &str,
        visibility: TemplateVisibility,
        asked_at: Instant,
        observed_at: OffsetDateTime,
    ) {
        // With both windows disabled the cache can never answer, so writing to
        // it is pure cost — and the kill switch is documented as reproducing the
        // pre-cache behavior *exactly*, which has to include not taking this
        // lock on a path that never took one before.
        if self.config.ttl.is_zero() && self.config.stale_ceiling.is_zero() {
            return;
        }
        // Keys are `templates.name` values, which only reach here from a
        // successful read — so they name a row that exists and are bounded by
        // whatever the admin surface accepted. The length guard is belt and
        // braces that turns `max_entries` into a real memory bound
        // (`max_entries × MAX_KEY_LEN`) rather than a count with an unbounded
        // per-entry cost. A name too long to cache simply is not cached; it
        // still resolves, one query per request.
        if template.len() > MAX_KEY_LEN {
            return;
        }
        let entry = Entry {
            visibility,
            asked_at,
            observed_at,
        };
        let Ok(mut entries) = self.entries.write() else {
            // A poisoned lock means a prior holder panicked while mutating the
            // map. Every read path treats a poisoned lock as a miss, so the
            // cache is already inert; dropping this write keeps it inert
            // rather than reviving it with a half-known map.
            return;
        };
        match entries.get(template) {
            // Strictly-later observations only. Equal stamps are treated as
            // interchangeable, which is very nearly true and not quite:
            // `clock_timestamp()` has microsecond resolution, so two reads can
            // share a stamp and still straddle a commit inside it, in which case
            // the later writer wins and may be the pre-flip one. Bounded by the
            // same TTL as the rest of the residual below.
            Some(existing) if existing.observed_at > observed_at => {}
            Some(_) => {
                // `get_mut`, not `insert`: the key is known present, so
                // reallocating it would cost a `String` per refresh under the
                // write guard — and with `ttl = 0` (the documented
                // resilience-only setting) this runs on every release
                // resolution.
                if let Some(slot) = entries.get_mut(template) {
                    *slot = entry;
                }
            }
            None => {
                if entries.len() >= self.config.max_entries.get() {
                    evict_oldest(&mut entries);
                }
                entries.insert(template.to_string(), entry);
            }
        }
    }

    /// Forget any cached read for `template`.
    ///
    /// Called when the row *was* read and its answer is unusable — no row, an
    /// unparseable value, or a durable failure establishing that the column is
    /// not readable. All of them mean the cached value can no longer be assumed
    /// to describe the row, so the cache must stop answering for it, and a later
    /// transient failure must not be able to fall back to it either.
    ///
    /// A **removal, not a dated tombstone.** A tombstone would additionally
    /// order a straggler — a read that snapshotted before the delete and lands
    /// after it — and that is a real if narrow hole this deliberately leaves
    /// open: such a straggler can re-insert a value the row genuinely held less
    /// than one statement-execution earlier (bounded by the sideline pool's
    /// 500ms `statement_timeout`), and it then answers for one TTL. Reaching it
    /// needs a concurrent template delete or a CHECK-bypassing write, and the
    /// TTL bounds it either way.
    ///
    /// [`evict_oldest`] opens the same hole a second way, and this is the place it
    /// is written down: if a key is evicted while a read for it is in flight, that
    /// read lands on the `None` arm with no ordering state to compare against and
    /// inserts unconditionally — so a superseded value can return with a fresh
    /// `asked_at`. Reaching it needs the map at capacity *and* a concurrent flip
    /// for the evicted key; the bound is again the TTL. The tombstone that closed it cost more than it
    /// bought: it kept caller-supplied keys resident (an unbounded-memory vector
    /// on an unauthenticated-name path), and its ordering rules against
    /// eviction, capacity, and same-instant observations were a standing source
    /// of defects. Removal is the deliberate trade.
    pub(crate) fn invalidate(&self, template: &str) {
        // Same short-circuit as `store`, and for the same documented reason: with
        // both windows disabled the map is never read, so touching its lock would
        // put a write on the release-resolution path that had no lock at all
        // before the cache existed. Missing here is how the "kill switch takes no
        // lock" claim was false — `Ok(None)` and the unparseable-value arm both
        // reach this on every request.
        if self.config.ttl.is_zero() && self.config.stale_ceiling.is_zero() {
            return;
        }
        // Check under the *read* guard first. `Ok(None)` reaches this on every
        // request naming a template with no `templates` row, which is
        // caller-driven and routine — so a
        // node looping unknown names would otherwise take the exclusive guard at
        // full request rate and serialize every concurrent `get_fresh` in the
        // process, on the path that also serves sandbox-create pulls.
        // `HashMap::remove` needs the write guard even when the key is absent.
        //
        // This widens the straggler window documented above, and is a second,
        // independent way into it: the observation point moves from the `remove`
        // to this `contains_key`, so a pre-delete reader now only has to land
        // after the *check* rather than after the removal to reinstate the entry.
        // Accepted knowingly — the window stays bounded by one statement's
        // execution and by the TTL, while the cost avoided is unbounded in
        // request rate.
        if self
            .entries
            .read()
            .is_ok_and(|entries| !entries.contains_key(template))
        {
            return;
        }
        if let Ok(mut entries) = self.entries.write() {
            entries.remove(template);
        }
    }

    /// Number of cached reads currently held.
    ///
    /// Exists so a test can assert the *absence of a write*. Both getters
    /// short-circuit on a zero window before touching the map, so no assertion
    /// built on them can distinguish "did not store" from "stored but cannot be
    /// read back" — which left the disabled-cache guard untested and, per
    /// `test-economy.md`, a test name that its body could not falsify.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.entries
            .read()
            .map(|entries| entries.len())
            .unwrap_or(0)
    }

    fn get_within(
        &self,
        template: &str,
        limit: Duration,
        now: Instant,
    ) -> Option<TemplateVisibility> {
        if limit.is_zero() {
            return None;
        }
        let entries = self.entries.read().ok()?;
        let entry = entries.get(template)?;
        entry_is_usable(entry.asked_at, limit, now).then_some(entry.visibility)
    }
}

/// Whether a read taken at `read_at` is still usable at `now` under `limit`.
///
/// Split out as a pure function so the boundary condition has its own focused
/// test: an entry exactly *at* the limit is expired. "Within 30s" must not
/// silently mean "within 30s inclusive", because this same comparison is what
/// bounds the isolation window in invariant 3.
///
/// A zero `limit` needs no special case here even though it means "window
/// disabled": no age is less than zero, so the comparison already answers
/// false. `get_within` still short-circuits on zero before calling this, to
/// skip taking the lock at all on the kill-switch path — that early return is
/// an optimization, not the correctness guard, so the two cannot diverge.
///
/// `saturating_duration_since` rather than `-` for explicitness, not for
/// safety: on this toolchain `Instant`'s `Sub` already delegates to the
/// saturating `duration_since`, so `now - asked_at` returns zero rather than
/// panicking when `now` precedes `asked_at`. Both spellings behave identically;
/// the named one says out loud that a backwards interval is expected here — a
/// caller can pass a `now` captured before an await — and reads as "just
/// refreshed", which for a sub-tick inversion is the right answer.
fn entry_is_usable(read_at: Instant, limit: Duration, now: Instant) -> bool {
    now.saturating_duration_since(read_at) < limit
}

/// Drop the least-recently-observed entry to make room for a new one.
///
/// Least-recently-*observed*, not least-recently-used: hits don't touch the
/// timestamps, deliberately, because a hit that bumped them would extend the
/// very window invariant 3 is there to bound. Keyed on `observed_at` rather than
/// `asked_at` so eviction order matches the order the database was actually
/// read in, which is the same key the write guard orders on.
///
/// The linear scan runs whenever the map is at capacity, and
/// [`CacheConfig::max_entries`] explains why that is not a "won't bind in
/// practice" path: an expired entry is never reclaimed, so a tenant with more
/// live template names than the cap pins the map there and every subsequent new
/// key pays a full scan under the write guard. Sizing `max_entries` up makes
/// each scan more expensive, not less frequent.
fn evict_oldest(entries: &mut HashMap<String, Entry>) {
    let oldest = entries
        .iter()
        .min_by_key(|(_, entry)| entry.observed_at)
        .map(|(template, _)| template.clone());
    if let Some(template) = oldest {
        entries.remove(&template);
    }
}

#[cfg(test)]
mod tests;
