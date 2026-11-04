//! Background prefetch loop for template-manager.
//!
//! On a fixed interval, this loop runs two intent-driven passes that
//! both result in artifact digests being warmed on disk and pinned
//! from eviction (the GC pass is a follow-up; see below):
//!
//! 1. **Tag pass.** For each `(template_name, tag)` tuple in
//!    `SPACE_TEMPLATE_PREFETCH_TAGS`:
//!    1. Resolve the tuple to its current artifact digest via
//!       `ClusterApi.FindTemplateRelease`.
//!    2. Record the resolved `(name, tag, content_hash)` in
//!       `template_pinned_tags`. The pin reflects "this is what `tag`
//!       currently resolves to" regardless of whether the local pull
//!       then succeeds.
//!    3. *Queue* — not dispatch — a pull for any digest not already on
//!       disk. The queue drains after the latest-N pass has had its say
//!       on every template, so a verdict from either pass governs both;
//!       see "Visibility gate". Dispatch is
//!       [`crate::volume::VolumePuller::pull`] with
//!       [`crate::volume::PullTrigger::PrefetchTag`], bounded by a
//!       shared [`tokio::sync::Semaphore`] so prefetch can't starve
//!       on-demand pulls.
//!    4. After iterating all tuples, prune `template_pinned_tags` to
//!       the configured set *minus* every tuple of a template
//!       confirmed `org_specific` this tick, so both a removed tuple
//!       and an org-scoped one drop their pin. See "Visibility gate".
//!
//! 2. **Latest-N release pass.** For each unique template name
//!    extracted from `SPACE_TEMPLATE_PREFETCH_TAGS`, on every tick — the
//!    pass doubles as the template-visibility resolver, so `latest_n > 0`
//!    gates only the warming, never the resolve. See "Visibility gate"
//!    below.
//!    1. Call `ClusterApi.ListLatestTemplateReleases(name, fc,
//!       max(latest_n, 1))` — the `max` keeps the response carrying a
//!       template-level visibility even when nothing will be warmed.
//!       Empty result is normal (template registered but no releases
//!       yet).
//!    2. For each returned release, record the pin in
//!       `template_pinned_releases` keyed on `(name, content_hash)`.
//!       The pin is content-addressable: a digest pinned by both a
//!       tag and by being one of the latest N is dedup'd by the GC
//!       UNION view, not by erasing one of the two records.
//!    3. Dispatch pulls (with
//!       [`crate::volume::PullTrigger::PrefetchLatestRelease`]) for
//!       any digest not already on disk and not already dispatched
//!       this tick by the tag pass (intra-tick dedup `HashSet`).
//!    4. After iterating the per-template entries, prune
//!       `template_pinned_releases` to the resolved set, scoped to the
//!       templates that successfully resolved this tick — a transient
//!       cluster-API failure for one template must not flush its
//!       existing pins. One deliberate exception to that scoping: a
//!       template *either* pass confirmed `org_specific` is added to the
//!       prune scope. That covers a tag-pass verdict for a template this
//!       pass never resolved (or never ran for), and it is also the only
//!       route for a verdict this pass reached itself — its
//!       `OrgSpecific` arm `continue`s before the prune-eligible push.
//!       Data isolation can't depend on which RPC happened to succeed.
//!       See "Visibility gate".
//!
//! ## Why both passes
//!
//! Tags are an intent layer (`prod`, `latest`, …) that doesn't change
//! often. Latest-N is a content-addressable safety net: when an older
//! release is still serving traffic (because a sandbox was checkpointed
//! against it, or a deployment is mid-roll), keeping the recent N
//! digests warm avoids a cold pull on resume. Releases per template
//! grow append-only with cheap content-addressable digests, so the
//! incremental cost is well-bounded by N.
//!
//! ## Visibility gate (org-scoped templates)
//!
//! Prefetch runs identically on every node, so it must never warm an
//! `org_specific` template — that would materialize an org's private
//! rootfs onto shared and other-org nodes, breaking the org-scoped
//! templates data-isolation guarantee independent of pod placement.
//! Both passes consult the `TemplateVisibility` the cluster reports
//! alongside each resolution:
//!
//! - **`Generic`** — warm as usual.
//! - **`OrgSpecific`** — skip the pull *and drop the pins* (every tag
//!   tuple of the template leaves the prune keep-set; the template's
//!   release pins prune to empty), which makes any residue *eligible*
//!   for LRU eviction. Note the weaker verb: unpinning does not
//!   evict. [`crate::eviction`] is enabled by default (100 GiB budget) —
//!   and being enabled, it only reclaims once the node is *over budget*,
//!   and never within the grace period (1800 s default). So the residue
//!   becomes evictable, on the GC's schedule rather than this tick's.
//!   (With `SPACE_TEMPLATE_CACHE_BUDGET_BYTES=0` the loop returns
//!   without reading the DB or touching disk, so the residue is never
//!   evicted at all.) Treat this as defense in
//!   depth, not as the residue gate; the RFC assigns eviction-on-flip
//!   to the flip path that will own it.
//!   Either pass's verdict triggers both drops: the tag pass may see no
//!   visibility at all when the tag no longer resolves (only the
//!   latest-N verdict can clear its stale tag pins — carried on the
//!   *response level* so it survives a template whose releases are all
//!   gone), and the latest-N pass may be disabled or failing (only the
//!   tag-pass verdict can flush the release pins). Outcome:
//!   `skipped_org_specific`.
//!
//!   Bound on that response-level signal: it covers "template row
//!   exists, zero releases", **not** "template deleted". Once the last
//!   release is gone the `templates` row itself can be dropped, and
//!   from then on the cluster reports `Unspecified` rather than
//!   `OrgSpecific` — which *preserves* pins per the rule below. So a
//!   fully-deleted org-specific template's stale tag pins are not
//!   dropped by this mechanism; only a template that still has a row
//!   gets its residue unpinned. Reaching that state needs the template
//!   to be in `SPACE_TEMPLATE_PREFETCH_TAGS` and deleted cluster-side, at
//!   which point the configured entry is itself stale.
//! - **`Unspecified`** — the cluster couldn't determine visibility.
//!   Not known to be generic →
//!   no new pulls, but existing pins are *preserved*, mirroring the
//!   transient-failure rule: an unknown answer must not flush GC
//!   protection for everything cached on this node. (One carve-out:
//!   during the normal latest-N pass (`latest_n > 0`) a *successful*
//!   resolve with zero entries prunes the template's release pins to
//!   `{}` — zero registered releases means nothing should be
//!   release-pinned, whatever the visibility. When `latest_n == 0` the
//!   pass instead `continue`s on its visibility-only limit=1 read and
//!   prunes nothing, so existing release pins survive.) Outcome:
//!   `visibility_unknown` — except on that same zero-entry path, which
//!   falls through to the ordinary empty-list handling and emits **no**
//!   candidate metric at all (the release loop iterates zero times). So
//!   `visibility_unknown` counts templates whose visibility was unknown
//!   *and* that had at least one release; it is not a complete census
//!   of the suppressed set. Note it is keyed on having releases, not on
//!   warming being possible: the `releases.is_empty()` check precedes
//!   the `latest_n == 0` bail, so with latest-N warming disabled the
//!   counter still fires once per such template per tick even though
//!   nothing would have been warmed either way.
//!
//!   Operational note — this is the failure mode to watch, and it is
//!   wider than schema skew. `Unspecified` is what the cluster reports
//!   for *any* failure of the `templates.visibility` read (see
//!   `template_visibility` in the internal-api server: a missing
//!   column, a missing row, an unparseable value, and a plain query
//!   error or server-side timeout all degrade to it), and that read is
//!   an extra Postgres round-trip on every release resolution. So a
//!   saturated control-plane pool — not just a rolling deploy that
//!   lands new template-mgr before cluster-api has the field — can make
//!   prefetch candidates skip on every node at once, generic
//!   templates included: no pull is dispatched and prefetch
//!   warming stops fleet-wide for as long as the read keeps failing.
//!   (Under saturation the *release* lookup can fail first instead,
//!   which surfaces as `resolve_error` rather than
//!   `visibility_unknown`; the two outcomes split the same underlying
//!   outage by whichever query lost.) On-demand pulls are exempt so
//!   creates still succeed, but
//!   they succeed *cold*, which adds control-plane and S3 load in
//!   exactly the situation that caused the failure. The state does
//!   self-heal once the resolve returns a concrete visibility. One
//!   knock-on of dispatching every pull at the drain: no pull overlaps
//!   this pass's RPCs. A release pull decided for the first template
//!   waits for every remaining template's resolve, and a queued tag pull
//!   waits for the whole pass — so when those RPCs are slow, as in the
//!   saturation case above, the queued pulls sit idle for that long
//!   instead of already running, and `template.prefetch.tick_duration_ms`
//!   rises by the lost overlap. Negligible against a multi-GB S3 GET in
//!   the normal case, and the trade buys the dispatch gate. The
//!   fail-closed direction is deliberate: a misread can only degrade to
//!   `Unspecified`, never fabricate `OrgSpecific`, so isolation still
//!   requires a true `org_specific` read.
//!
//!   Diagnosing it starts on the *producer* side, not here:
//!   `template.visibility_lookup.degraded` carries a `reason` tag that
//!   distinguishes which of those causes fired, and it is the only
//!   signal that separates "schema predates the column" from "the query
//!   is failing". This side's `visibility_unknown` tells you prefetch
//!   stopped warming; that counter tells you why.
//!
//!   (The `TemplateNotGeneric` failure below is mostly not what fires
//!   here — the tick gate skips before dispatch. It is reachable only
//!   when the by-hash re-resolve disagrees with the tick's resolve:
//!   either a genuine flip after a `Generic` decision, or a visibility
//!   read that succeeded for the tick and failed for the re-resolve.)
//!
//!   That gate blocks the pull but deliberately does **not** feed its
//!   verdict back into the tick's `org_specific_templates`, so pins
//!   written earlier in the same tick survive it. Propagating it would
//!   not close the window: the tag pass upserts the pin from its own
//!   read on every tick (see `upsert_tag` below), so a cleanup at the
//!   end of one tick is re-asserted by the next while the two reads
//!   still disagree — the pin would flap rather than clear. What does
//!   clear it is the first tick whose *own* resolve reads
//!   `OrgSpecific`, which drops every tuple of the template through
//!   `mark_template_org_specific`. So the exposure is a pin (and any
//!   build it keeps GC-protected) outliving the flip for as long as the
//!   tick-side read stays stale, not indefinitely. No bytes land
//!   either way — the by-hash gate still refuses the pull.
//!
//! The gate governs *dispatch*, not just pinning. Each pass decides on
//! the visibility its own resolve returned, but a verdict belongs to the
//! tick rather than the pass that found it — so the tag pass queues its
//! pulls and the queue is filtered against the accumulated verdicts once
//! the latest-N pass has finished. Note what that does and does not
//! promise: the filter is positive-verdict-only, keyed on confirmed
//! `OrgSpecific` membership rather than on "confirmed generic". A queued
//! pull whose template the latest-N pass reported `Unspecified`, or
//! failed to resolve at all, still dispatches — the tag pass's own
//! `Generic` read stands. Note this is not what `latest_n = 0` does:
//! that disables latest-N *warming*, and its early exit sits after the
//! visibility gate, so an `OrgSpecific` verdict still suppresses a
//! queued tag pull there. Dispatching inline would leave the
//! by-hash gate in [`crate::volume`] as the only defense against a
//! template that flipped between the two passes, and that gate reads
//! `Generic` too whenever it beats the flip or hits a stale replica —
//! which puts a private rootfs on a shared node, with only the
//! (budget- and grace-gated) evictor to clean it up. Once
//! [`dispatch_pull`] spawns, nothing recalls the task: the verdict has
//! to land while the pull is still a queued decision.
//!
//! ### No switch opens the gate
//!
//! The gate is unconditional. An earlier opt-in switch,
//! `SPACE_TEMPLATE_PREFETCH_VISIBILITY_ENFORCEMENT`, defaulted off and
//! then treated every template as generic; it is retired, and startup
//! only warns when it is still set. The node-scoped template catalog in
//! the internal API keeps an unrelated node from resolving another
//! organization's release when Kubernetes service-account authentication
//! is configured, but a static-only deployment reads the catalog
//! unscoped, so this gate is the boundary there and defense in depth
//! everywhere else.
//!
//! The price is the `Unspecified` path above: a failing cluster-side
//! `templates.visibility` read stops prefetch warming on every node at
//! once, while on-demand pulls continue cold. The internal-api visibility
//! cache (`SPACE_INTERNAL_API_VISIBILITY_CACHE_*`) carries the read
//! through such a failure; enable it wherever prefetch is configured.
//!
//! On-demand pulls are deliberately not gated here: they serve an
//! already-authorized sandbox create (the API's create-time authz gate
//! owns that decision), including org-specific templates on their own
//! org's dedicated nodes.
//!
//! ## Deduplication
//!
//! Within a single tick the loop maintains a `HashSet<(name, hash)>`
//! of pulls *decided* on this tick — membership means claimed, not yet
//! running, since every dispatch happens at the drain. Both passes
//! insert; the latest-N pass also reads it before claiming a digest,
//! which avoids double-spending semaphore
//! permits and keeps the `trigger:` metric tag attributing each
//! digest's bytes to the first caller that wanted it. Across passes
//! the in-flight map inside `VolumePuller::pull` deduplicates
//! anyway, so this is purely an attribution / metric-cardinality
//! concern. A digest pinned by both a tag and a latest-N slot is
//! recorded in both pin tables and dedup'd by the GC view.
//!
//! ## Concurrency with on-demand pulls
//!
//! Prefetch reuses [`crate::volume::VolumePuller`] verbatim — it does
//! not bypass the in-flight map. If prefetch and an on-demand request
//! race on the same `(name, content_hash)`, only one materialization
//! runs; the loser short-circuits as `coalesced` after the winner
//! finishes. The `trigger:` metric tag identifies which caller actually
//! paid for the work.
//!
//! ## Configured tag tuples
//!
//! `SPACE_TEMPLATE_PREFETCH_TAGS` supplies the tag tuples at startup. Removed
//! tuples lose their tag pins on the next tick. Each unique template also opts
//! into latest-N warming.
//!
//! ## Configuration (env vars, daemon-scope)
//!
//! - `SPACE_TEMPLATE_PREFETCH_TAGS` — JSON array of `name:tag` references. Defaults
//!   to `seed:latest` and `code-runtime:latest`; `[]` disables all tag pins.
//! - `SPACE_TEMPLATE_PREFETCH_ENABLED` — set to `"0"` or `"false"` to
//!   disable. Default: enabled.
//! - `SPACE_TEMPLATE_PREFETCH_VISIBILITY_ENFORCEMENT` — retired and ignored;
//!   prefetch always requires confirmed generic visibility.
//! - `SPACE_TEMPLATE_PREFETCH_LATEST_N` — test hook only (ignored by
//!   release builds): number of latest releases to warm per template
//!   (default: 5; `0` disables latest-N *warming* and keeps only tag
//!   prefetch — the per-template visibility resolve still runs so an
//!   org-specific template's stale pins are dropped regardless).
//!
//! ## Metrics
//!
//! Prefetch-specific:
//! - `template.prefetch.tick` — counter, tagged `result:ok|error`.
//!   One per tick, written *after* the tick returns, so its absence
//!   covers a tick that hangs as well as a loop that exits.
//!   Monitor absence per cluster across both outcomes. A `result` filter
//!   requires the metrics backend to retain that tag for queries.
//! - `template.prefetch.tick_duration_ms` — timing per tick.
//! - `template.prefetch.tag.candidates` — counter, tagged
//!   `template:`, `tag:`,
//!   `outcome:dispatched|already_cached|unresolved|resolve_error|pin_error|skipped_org_specific|visibility_unknown`.
//!   Increments per `(name, tag)` decision in the tag pass.
//! - `template.prefetch.release.candidates` — counter, tagged
//!   `template:`, `hash:`,
//!   `outcome:dispatched|already_cached|coalesced_with_tag|resolve_error|pin_error|skipped_org_specific|visibility_unknown`.
//!   Increments per `(name, content_hash)` decision in the latest-N
//!   pass; the two visibility outcomes are emitted once per template
//!   (with an empty `hash:`) since the gate is template-level.
//!
//! Pull-path metrics carry
//! `trigger:on_demand|prefetch_tag|prefetch_latest_release` — see
//! [`crate::volume::PullTrigger`].

use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;
use std::time::Duration;

use pplx_space_common_core::types::{TemplateName, TemplateReference, TemplateTag};
use pplx_space_node_api_client::proto::TemplateVisibility;
use tokio::sync::Semaphore;
use tracing::{debug, info, warn};

use crate::pinned_digests::PinnedDigestsStore;
use crate::volume::{LatestReleasesOutcome, PullTrigger, VolumePuller};

const INTERVAL_SECS: u64 = 60;
const CONCURRENCY: usize = 2;
/// Default count for the per-template latest-N release warmer. Each
/// pinned digest occupies an expanded rootfs on `/space/cache` until
/// LRU eviction lands.
const DEFAULT_LATEST_N: u32 = 5;
/// Test-hook name overriding the latest-N count (see
/// `pplx_space_common_core::env::test_hook`); release builds never read it.
const LATEST_N_TEST_HOOK: &str = "SPACE_TEMPLATE_PREFETCH_LATEST_N";

/// Single jitter knob, applied as a fraction of the configured
/// interval in two places:
///
/// - **Per-tick sleep**: each sleep is
///   `interval * uniform(1 - JITTER_FRACTION, 1 + JITTER_FRACTION)`,
///   i.e. `interval * uniform(0.8, 1.2)` with the default 0.2 — a
///   span of `JITTER_FRACTION * interval * 2`.
/// - **First-tick boot stagger**: a one-shot sleep uniform over
///   `[0, JITTER_FRACTION * interval * 2)`. Same span as the per-tick
///   jitter; reuses the same knob so there's no boot-specific
///   constant.
///
/// Both uses keep simultaneous fleet boots/ticks from hitting the
/// cluster API and S3 in lockstep.
const JITTER_FRACTION: f64 = 0.2;

const PREFETCH_TAGS_ENV: &str = "SPACE_TEMPLATE_PREFETCH_TAGS";
const DEFAULT_PREFETCH_TAGS: &str = r#"["seed:latest","code-runtime:latest"]"#;

#[derive(Debug, thiserror::Error)]
enum PrefetchTagsParseError {
    #[error(transparent)]
    InvalidReferences(#[from] serde_json::Error),
    #[error("prefetch references must use tag selectors, not digests")]
    ExpectedTag,
}

fn parse_prefetch_tags(
    raw: &str,
) -> Result<Vec<(TemplateName, TemplateTag)>, PrefetchTagsParseError> {
    let references: Vec<TemplateReference> = serde_json::from_str(raw)?;
    references
        .into_iter()
        .map(|reference| {
            let tag = reference.tag().ok_or(PrefetchTagsParseError::ExpectedTag)?;
            Ok((reference.name().clone(), tag.clone()))
        })
        .collect()
}

#[derive(Clone, Debug)]
pub struct PrefetchConfig {
    pub enabled: bool,
    pub interval: Duration,
    pub concurrency: usize,
    /// Number of latest releases to *warm* per template.
    ///
    /// Zero disables the warming but not the pass, which doubles as the
    /// template-visibility resolver: it still issues one
    /// `ListLatestTemplateReleases` per template per tick (`limit = 1`)
    /// to learn each verdict. See the "Visibility gate" module docs.
    pub latest_n: u32,
}

impl PrefetchConfig {
    /// Read config from `SPACE_TEMPLATE_PREFETCH_*` env vars, falling
    /// back to defaults for any unset/malformed entry. The tick interval
    /// (60 s) and download concurrency (2) are fixed.
    ///
    /// Prefetch defaults to **enabled**. To turn it off, set
    /// `SPACE_TEMPLATE_PREFETCH_ENABLED=0` (or `false`) explicitly.
    pub fn from_env() -> Self {
        // Fail-open: prefetch is best-effort and won't disrupt on-demand
        // traffic if it errors, so an unrecognized value leaves it enabled.
        let enabled =
            pplx_space_common_core::env::env_flag_var("SPACE_TEMPLATE_PREFETCH_ENABLED", true);
        // Latest-N: parsed permissively. `0` is a meaningful value
        // (disable the pass), so unset → DEFAULT_LATEST_N, but an
        // explicit `0` is respected. The cluster-side handler caps at
        // 50; clamp here defensively so a misconfigured operator
        // doesn't spam the cluster API with a validation error every
        // tick.
        let latest_n = match pplx_space_common_core::env::test_hook_var(LATEST_N_TEST_HOOK) {
            Some(s) => s.parse::<u32>().unwrap_or(DEFAULT_LATEST_N).min(50),
            None => DEFAULT_LATEST_N,
        };
        Self {
            enabled,
            interval: Duration::from_secs(INTERVAL_SECS),
            concurrency: CONCURRENCY,
            latest_n,
        }
    }
}

/// Run the prefetch loop forever. Returns only if `cfg.enabled` is
/// false (logs and returns immediately) — there is no shutdown signal
/// today; the task is dropped when the tokio runtime shuts down. A
/// cooperative shutdown via `CancellationToken` is intentionally
/// deferred to a follow-up PR.
pub async fn run_loop(pins: PinnedDigestsStore, volume_puller: VolumePuller, cfg: PrefetchConfig) {
    if std::env::var_os(crate::volume::RETIRED_ENV_VISIBILITY_ENFORCEMENT).is_some() {
        warn!(
            env = crate::volume::RETIRED_ENV_VISIBILITY_ENFORCEMENT,
            "retired setting is ignored: prefetch always refuses templates not confirmed generic",
        );
    }

    if !cfg.enabled {
        info!("template prefetch disabled (SPACE_TEMPLATE_PREFETCH_ENABLED=0)");
        return;
    }

    let tag_tuples = match parse_prefetch_tags(
        &std::env::var(PREFETCH_TAGS_ENV).unwrap_or_else(|_| DEFAULT_PREFETCH_TAGS.to_string()),
    ) {
        Ok(tags) => tags,
        Err(error) => {
            warn!(%error, "invalid SPACE_TEMPLATE_PREFETCH_TAGS; prefetch disabled");
            return;
        }
    };

    info!(
        interval_secs = cfg.interval.as_secs(),
        concurrency = cfg.concurrency,
        tag_tuples = tag_tuples.len(),
        latest_n = cfg.latest_n,
        "template prefetch loop starting",
    );

    let semaphore = Arc::new(Semaphore::new(cfg.concurrency));
    // Per-process random first-tick stagger uniform over
    // [0, JITTER_FRACTION * interval * 2) — the same window span as
    // the per-tick jitter, applied as a one-shot pre-first-tick
    // sleep. With the default 60s interval and 0.2 fraction this is
    // [0, 24s), so the fleet de-synchronizes within roughly the
    // same time scale as the steady-state cadence.
    let initial_stagger = Duration::from_secs_f64(
        rand::random::<f64>() * JITTER_FRACTION * cfg.interval.as_secs_f64() * 2.0,
    );
    info!(
        stagger_ms = initial_stagger.as_millis() as u64,
        "template prefetch: sleeping random stagger before first tick",
    );
    tokio::time::sleep(initial_stagger).await;

    loop {
        let started = std::time::Instant::now();
        let result = tick(
            pins.clone(),
            volume_puller.clone(),
            &semaphore,
            cfg.latest_n,
            &tag_tuples,
        )
        .await;
        let result_label = match &result {
            Ok(()) => "ok",
            Err(_) => "error",
        };
        pplx_space_metrics::template::prefetch_tick(result_label, started);
        if let Err(err) = result {
            warn!(error = %err, "template prefetch tick failed");
        }
        tokio::time::sleep(jittered(cfg.interval)).await;
    }
}

/// Apply ±[`JITTER_FRACTION`] uniform jitter to `interval` so workers
/// don't all tick at the same instant. Returns a duration in
/// `[interval * (1 - f), interval * (1 + f))`.
fn jittered(interval: Duration) -> Duration {
    let factor = 1.0 + JITTER_FRACTION * (rand::random::<f64>() * 2.0 - 1.0);
    Duration::from_secs_f64(interval.as_secs_f64() * factor)
}

/// Unique template names from `SPACE_TEMPLATE_PREFETCH_TAGS`, in stable
/// alphabetical order so log lines and prune sets compare cleanly
/// across ticks.
fn unique_prefetch_templates(tag_tuples: &[(TemplateName, TemplateTag)]) -> Vec<String> {
    tag_tuples
        .iter()
        .map(|(name, _)| (*name).to_string())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

/// Decision the latest-N release pass makes for one resolved
/// `(template, content_hash)` pair. Extracted as a pure function
/// (see [`classify_release_dispatch`]) so the dedup invariant can be
/// unit-tested without standing up cluster-API or volume-puller
/// fakes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReleaseDispatchDecision {
    /// The tag pass earlier in this same tick already scheduled a
    /// dispatch for `(template, content_hash)`. The latest-N pass
    /// skips it and emits the `coalesced_with_tag` outcome — only
    /// one pull is scheduled even though both passes resolved to the
    /// same digest.
    CoalescedWithTag,
    /// A `ready` `template_builds` row already exists for this
    /// `(template, content_hash)`. The pin was written but no pull
    /// is needed.
    AlreadyCached,
    /// The digest is not yet on disk and was not dispatched by the
    /// tag pass — schedule a new pull.
    Dispatch,
}

/// Classify a `(template, content_hash)` pair from the latest-N pass.
/// Pure function — does not touch the DB or the cluster API; just
/// composes the two predicates (already dispatched this tick / locally
/// cached) into the decision the loop acts on. Keep it pure so the
/// dedup invariant has its own focused test.
///
/// The dedup ordering matters: a tag-claimed digest takes precedence
/// over the local-cache check, so `coalesced_with_tag` wins when both
/// predicates hold. They can — an *on-demand* pull can materialize the
/// digest between the tag pass's `local_build_exists` check and this
/// one, leaving it both tag-claimed and locally cached. The tag pass
/// itself is not a possible cause: it dispatches at the drain, after
/// this pass has run.
pub(crate) fn classify_release_dispatch(
    already_dispatched: &HashSet<(String, String)>,
    template: &str,
    content_hash: &str,
    locally_cached: bool,
) -> ReleaseDispatchDecision {
    if already_dispatched.contains(&(template.to_string(), content_hash.to_string())) {
        ReleaseDispatchDecision::CoalescedWithTag
    } else if locally_cached {
        ReleaseDispatchDecision::AlreadyCached
    } else {
        ReleaseDispatchDecision::Dispatch
    }
}

async fn tick(
    pins: PinnedDigestsStore,
    volume_puller: VolumePuller,
    semaphore: &Arc<Semaphore>,
    latest_n: u32,
    tag_tuples: &[(TemplateName, TemplateTag)],
) -> Result<(), String> {
    if tag_tuples.is_empty() {
        // Fall through so the prune call below still clears any stale rows
        // left over from a previous tick that had tuples.
        debug!("prefetch tick: no configured tag tuples");
    } else {
        debug!(
            tuples = tag_tuples.len(),
            latest_n, "prefetch tick: resolving tag pins and latest releases",
        );
    }

    let mut spawned: Vec<tokio::task::JoinHandle<()>> = Vec::new();
    // `(template_name, content_hash)` pairs the tag pass decided to
    // pull this tick — queued, not necessarily dispatched, since a
    // verdict can still suppress one at the drain. Used by the latest-N
    // pass to avoid double-dispatching a digest the tag pass already
    // claimed. See module docs ("Deduplication").
    let mut dispatched_this_tick: HashSet<(String, String)> = HashSet::new();
    // Org-specific verdicts collected this tick — from EITHER pass; a
    // verdict drops BOTH pin kinds for the template (see module docs,
    // "Visibility gate"). Cross-feeding matters in both directions: a
    // tag that stopped resolving never shows the tag pass a visibility
    // (only the latest-N verdict can clear its stale tag pin), and a
    // tag-pass verdict must flush release pins even when the latest-N
    // pass is disabled (`latest_n = 0`) or its RPC fails.
    // Tag-pass pulls decided this tick but deliberately not dispatched
    // until the latest-N pass has had its say on every template — see
    // the queueing site in pass 1.
    let mut pending_tag_pulls: Vec<PendingTagPull> = Vec::new();
    // Latest-N pulls, queued for the same drain purely so tag pulls can
    // take the semaphore first; these need no verdict filtering, since
    // the latest-N pass gates each template before reaching its
    // releases.
    let mut pending_release_pulls: Vec<(String, String)> = Vec::new();
    let mut org_specific_tag_tuples: HashSet<(String, String)> = HashSet::new();
    let mut org_specific_templates: BTreeSet<String> = BTreeSet::new();
    let db = pins.conn();

    // ── Pass 1: tag pins ───────────────────────────────────────────────
    for (name, tag) in tag_tuples {
        let (name, tag) = (name.as_str(), tag.as_str());
        let outcome = crate::volume::resolve_tag(name, tag).await;
        match outcome {
            crate::volume::TagResolveOutcome::Unresolved => {
                record_tag_candidate(name, tag, "unresolved");
                continue;
            }
            crate::volume::TagResolveOutcome::Error => {
                record_tag_candidate(name, tag, "resolve_error");
                continue;
            }
            crate::volume::TagResolveOutcome::Resolved { hash, visibility } => {
                // Harden against a verdict already accumulated for this
                // template, exactly as the latest-N pass does — the two
                // passes must not protect only one direction. A template
                // with more than one configured tuple is resolved once per
                // tuple, so a second tuple reading `Generic` after a first
                // read `OrgSpecific` (mid-tick flip, or a stale replica
                // answering the second RPC) would otherwise pin and
                // dispatch a template this tick already confirmed
                // private. The final by-hash gate would have to catch it,
                // and only if that resolve disagreed too.
                let reported_visibility = visibility;
                let visibility =
                    hardened_visibility(org_specific_templates.contains(name), visibility);
                match visibility {
                    TemplateVisibility::Generic => {}
                    TemplateVisibility::OrgSpecific => {
                        // `debug!` — meaning absent at deployed log
                        // levels, not merely cheap: the service filter
                        // defaults to `info` (see
                        // `pplx_space_observability`) and nothing sets
                        // `RUST_LOG` for this DaemonSet. The record for
                        // this event is the counter below, watched by a
                        // monitor on `outcome:skipped_org_specific` —
                        // over the clusters that monitor scopes, which
                        // leaves any others with neither it nor this
                        // line. Restoring `info!` requires that counter
                        // to stay watched, and costs what the level
                        // saves: while a template stays
                        // org-specific this arm runs once per tag tuple
                        // per tick on every node — ~1.4k lines/node/day
                        // per marked tuple at the 60s
                        // `INTERVAL_SECS`, scaling with how many
                        // configured tuples are marked — for an event the
                        // counter already reports.
                        //
                        // The latest-N pass keeps its `info!` for the
                        // case the counter cannot carry: a template whose
                        // configured tag was deleted cluster-side resolves
                        // `Unresolved` here and never reaches this arm, so
                        // the tag metric records that tuple as `unresolved`
                        // and never carries the org-specific outcome for
                        // it.
                        //
                        // `reported` separates a genuine private verdict
                        // from a hardened disagreeing read, for whoever
                        // raises the level to read this arm.
                        debug!(
                            template = %name,
                            tag = %tag,
                            reported = ?reported_visibility,
                            "prefetch: template is org-specific; skipping and dropping its pins",
                        );
                        record_tag_candidate(name, tag, "skipped_org_specific");
                        // Visibility is template-level: drop every tag
                        // tuple of this template (not just the one that
                        // happened to resolve) and queue its release
                        // pins for the flush below.
                        mark_template_org_specific(
                            tag_tuples,
                            &mut org_specific_tag_tuples,
                            &mut org_specific_templates,
                            name,
                        );
                        continue;
                    }
                    TemplateVisibility::Unspecified => {
                        // `debug!` because this arm is reachable in bulk:
                        // a failing cluster-side `templates.visibility`
                        // read returns `Unspecified` for every template
                        // while the release resolution still succeeds, so
                        // `info!` would put one line per template per tick
                        // on every node during the control-plane outage
                        // that caused it. The counter below carries the
                        // effect and the server counts the cause as
                        // `template.visibility_lookup.degraded`. The
                        // `unresolved` idle state takes the same view and
                        // goes further, relying on its counter alone.
                        debug!(
                            template = %name,
                            tag = %tag,
                            "prefetch: template visibility unknown; not warming \
                             (existing pin, if any, is preserved)",
                        );
                        record_tag_candidate(name, tag, "visibility_unknown");
                        continue;
                    }
                }
                // Record the pin first, before dispatch. The pin
                // reflects "this is what `tag` currently resolves to",
                // independent of whether the local pull then succeeds.
                //
                // A failure here is logged and counted as `pin_error`
                // but does NOT abort the tick — bubbling out would
                // skip every subsequent tuple, skip the prune block
                // below, and detach already-spawned pulls from the
                // join loop. Better to lose one tuple's tick than to
                // poison the rest of the loop.
                if let Err(err) = spawn_blocking_store_op(pins.clone(), {
                    let name = name.to_string();
                    let tag = tag.to_string();
                    let hash = hash.clone();
                    move |s| s.upsert_tag(&name, &tag, &hash)
                })
                .await
                {
                    warn!(
                        template = %name,
                        tag = %tag,
                        error = %err,
                        "prefetch: tag pin upsert failed; skipping dispatch for this tuple",
                    );
                    record_tag_candidate(name, tag, "pin_error");
                    continue;
                }

                if crate::volume::local_build_exists(&db, name, &hash) {
                    record_tag_candidate(name, tag, "already_cached");
                    continue;
                }
                // Queue rather than dispatch. This pass only has the
                // visibility its own tag resolve returned; the latest-N
                // pass below can still report `OrgSpecific` for the same
                // template, and once a pull is spawned nothing can recall
                // it — `mark_template_org_specific` edits the prune sets,
                // it does not cancel tasks. Dispatching here would leave
                // the by-hash gate in `pull_inner` as the only defense,
                // and that gate reads `Generic` too whenever it wins the
                // race against the flip or hits a stale replica, which
                // materializes a private rootfs on a shared node. So no
                // bytes move until the tick's whole verdict is in; see
                // "Visibility gate" in the module docs.
                //
                // The dedup insert belongs at decision time, not
                // dispatch time: the latest-N pass consults this set to
                // attribute a shared digest to `coalesced_with_tag`, and
                // it runs before the drain.
                dispatched_this_tick.insert((name.to_string(), hash.clone()));
                pending_tag_pulls.push(PendingTagPull { name, tag, hash });
            }
        }
    }

    // ── Pass 2: latest-N release pins ──────────────────────────────────
    //
    // Templates whose ListLatestTemplateReleases succeeded this tick
    // are the only ones whose `template_pinned_releases` rows we
    // touch (prune-scope). A cluster-API failure for one template
    // leaves its existing pins alone so a transient outage doesn't
    // flush GC protection for everything cached on this node.
    let mut prunable_templates: Vec<String> = Vec::new();
    let mut release_pins_keep: Vec<(String, String)> = Vec::new();
    // Runs on every tick, whatever `latest_n` says.
    //
    // `latest_n == 0` disables the *warming*, but the pass is also the
    // template-visibility resolver: if a formerly-generic template flips
    // to org-specific after its configured tag is deleted, the tag pass
    // sees only `Unresolved` and this is the sole path that can learn the
    // verdict and unpin the now-private rootfs. So it queries at least one
    // release, which keeps the template-level visibility on the response
    // even when nothing will be warmed.
    {
        let query_limit = latest_n.max(1);
        for template_name in unique_prefetch_templates(tag_tuples) {
            let (visibility, releases) =
                match crate::volume::list_latest_releases(&template_name, query_limit).await {
                    LatestReleasesOutcome::Resolved {
                        visibility,
                        releases,
                    } => (visibility, releases),
                    LatestReleasesOutcome::Error => {
                        record_release_candidate(&template_name, "", "resolve_error");
                        continue;
                    }
                };

            // A verdict already accumulated this tick (the tag pass) wins
            // over a stale or skewed generic latest-N read: without this,
            // a mid-tick visibility flip could let the release loop below
            // re-pin and even dispatch pulls for a template the tag pass
            // confirmed org-specific — the final flush would drop the
            // pins, but the rootfs would still have materialized.
            //
            // Whether the tag pass already marked this template
            // org-specific. Two consumers, both wanting exactly that
            // question: the hardening below, and the log level of the
            // `OrgSpecific` arm further down. The level is deliberately
            // silent when this is true — the tag pass's own arm is
            // `debug!` because a monitor watches its counter, so the
            // marked case is already reported and an `info!` here would
            // only duplicate it. So a marking the tag pass already
            // reported leaves no info-level line at all, by design, and
            // the counter is the record — a marking this pass reaches
            // alone still logs, which is what the `info!` below is for.
            // Keep that in view before reusing this flag for a third
            // purpose that needs marking to be loud.
            let tag_pass_marked_org_specific = org_specific_templates.contains(&template_name);
            let reported_visibility = visibility;
            let visibility = hardened_visibility(tag_pass_marked_org_specific, visibility);

            // Visibility gate (template-level; see module docs). The
            // verdict applies regardless of whether any releases came
            // back — the response-level visibility field is exactly what
            // keeps an org-specific verdict alive for a template whose
            // releases are all gone, so its stale pins still drop.
            match visibility {
                TemplateVisibility::Generic => {}
                TemplateVisibility::OrgSpecific => {
                    // No `info!` at all in the case this event usually
                    // takes: when the tag pass has already marked the
                    // template — which it does whenever its own resolve
                    // carried the visibility — the branch below is
                    // `debug!`, and the tag pass's arm is `debug!` too, so
                    // the counter is the whole record. That is the state
                    // the monitor on `outcome:skipped_org_specific` is
                    // calibrated on.
                    //
                    // The `info!` covers the cases where the tag pass
                    // never marked the template: the tag lookup came back
                    // `NotFound` — a deleted tag, or a template unknown
                    // cluster-side (`Unresolved`) — or `resolve_tag`
                    // errored, or it read `Generic` or `Unspecified` where
                    // this pass reads `OrgSpecific`. At most one such line
                    // per template per tick.
                    //
                    // Whether the tag metric sees any of them turns on one
                    // thing: did the tuple reach the pull queue? Only a
                    // queued tuple is suppressed at the drain, which is
                    // the one path that records `skipped_org_specific`
                    // against a verdict this pass reached alone. Every
                    // other arm `continue`s first — `unresolved`,
                    // `resolve_error`, `visibility_unknown`, `pin_error`
                    // and `already_cached` — so for those the tag metric
                    // carries whatever they recorded and never the
                    // org-specific outcome.
                    //
                    // `already_cached` is the one to hold onto, because it
                    // is the steady state on a node that is already
                    // warmed: a stale `Generic` read on such a node
                    // records `already_cached` and stops. The queued case
                    // is the exception, not the rule.
                    //
                    // None of that leaves the event unrecorded, though —
                    // this arm also emits `skipped_org_specific` on the
                    // release counter below, and that `outcome` is
                    // queryable. So the metric trail for these cases is
                    // the release pass's, not the tag pass's, and the
                    // `info!` is the part that names the template, which
                    // the release counter cannot. A deleted tag
                    // is the case this pass exists to cover and it does
                    // not self-heal, which makes this arm the
                    // steady-state emitter rather than the tag pass's;
                    // note `resolve_tag`'s error outcome covers durable
                    // codes too (`Unimplemented`, `PermissionDenied`),
                    // not just blips.
                    //
                    // `tracing` needs a static level, so the two calls
                    // are spelled out rather than parameterized.
                    if tag_pass_marked_org_specific {
                        debug!(
                            template = %template_name,
                            reported = ?reported_visibility,
                            "prefetch: template is org-specific; skipping latest-N \
                             warming and dropping its release and tag pins",
                        );
                    } else {
                        info!(
                            template = %template_name,
                            reported = ?reported_visibility,
                            "prefetch: template is org-specific; skipping latest-N \
                             warming and dropping its release and tag pins",
                        );
                    }
                    record_release_candidate(&template_name, "", "skipped_org_specific");
                    // Drops BOTH pin kinds: the tag pass may not have
                    // learned the visibility this tick (an org-specific
                    // template whose configured tag was deleted resolves
                    // `Unresolved` there), and a pin left from the
                    // template's generic days would keep its rootfs
                    // GC-protected on this node.
                    mark_template_org_specific(
                        tag_tuples,
                        &mut org_specific_tag_tuples,
                        &mut org_specific_templates,
                        &template_name,
                    );
                    continue;
                }
                TemplateVisibility::Unspecified => {
                    // With zero entries this is also the old-server /
                    // old-schema shape; fall through to the
                    // long-standing empty-list behavior below (release
                    // pins prune to {} — the cluster reports zero
                    // registered releases, so no digest should be
                    // release-pinned). Tag pins stay preserved either
                    // way: unknown is not a verdict.
                    if !releases.is_empty() {
                        // `debug!` for the same bulk-reachability reason
                        // as the tag pass's `Unspecified` arm, gated more
                        // narrowly: the `!releases.is_empty()` check above
                        // means a template with zero releases produces no
                        // line here at all.
                        debug!(
                            template = %template_name,
                            "prefetch: template visibility unknown; skipping latest-N \
                             warming (existing release pins are preserved)",
                        );
                        record_release_candidate(&template_name, "", "visibility_unknown");
                        continue;
                    }
                }
            }

            // Warming + release-pin management is latest-N's job; skip it
            // when disabled. The org-specific pin-drop above has already
            // run, so a `latest_n == 0` deploy still enforces isolation —
            // it just doesn't manage the latest-N warm set (and must not
            // prune existing release pins on the strength of a
            // visibility-only limit=1 read).
            if latest_n == 0 {
                continue;
            }

            // Successful resolve (even empty) → this template is
            // prune-eligible. An empty list narrows the pin set to
            // {} for this template, which is the right behavior:
            // if the cluster has zero registered releases, no digest
            // should be release-pinned.
            prunable_templates.push(template_name.clone());

            for release in releases {
                // Push the keep-entry BEFORE the upsert call. The
                // prune-scope guard above protects against cluster-API
                // failures by keeping the failing template out of
                // `prunable_templates`; here we protect against a
                // narrower failure mode: a transient *per-row*
                // SQLite-upsert error must not flush the digest's
                // pre-existing pin. The digest is still in the
                // latest-N window cluster-side, and the metadata
                // refresh (release_id, updated_at) can retry next
                // tick — the keep-set's job is to tell the prune what
                // the cluster says belongs in the window, not to
                // record which local upserts happened to succeed
                // this round.
                release_pins_keep.push((template_name.clone(), release.content_hash.clone()));

                if let Err(err) = spawn_blocking_store_op(pins.clone(), {
                    let template_name = template_name.clone();
                    let content_hash = release.content_hash.clone();
                    let release_id = release.release_id.clone();
                    let release_created_at = release.created_at.clone();
                    move |s| {
                        s.upsert_release(
                            &template_name,
                            &content_hash,
                            &release_id,
                            &release_created_at,
                        )
                    }
                })
                .await
                {
                    warn!(
                        template = %template_name,
                        hash = %release.content_hash,
                        error = %err,
                        "prefetch: release pin upsert failed; skipping dispatch for this release",
                    );
                    record_release_candidate(&template_name, &release.content_hash, "pin_error");
                    continue;
                }

                let locally_cached =
                    crate::volume::local_build_exists(&db, &template_name, &release.content_hash);
                let decision = classify_release_dispatch(
                    &dispatched_this_tick,
                    &template_name,
                    &release.content_hash,
                    locally_cached,
                );
                match decision {
                    ReleaseDispatchDecision::CoalescedWithTag => {
                        record_release_candidate(
                            &template_name,
                            &release.content_hash,
                            "coalesced_with_tag",
                        );
                    }
                    ReleaseDispatchDecision::AlreadyCached => {
                        record_release_candidate(
                            &template_name,
                            &release.content_hash,
                            "already_cached",
                        );
                    }
                    ReleaseDispatchDecision::Dispatch => {
                        record_release_candidate(
                            &template_name,
                            &release.content_hash,
                            "dispatched",
                        );
                        dispatched_this_tick
                            .insert((template_name.clone(), release.content_hash.clone()));
                        // Queued rather than dispatched so the single
                        // drain below can bias toward tag pulls. Spawning
                        // here would put a cold node's ~20 older-release
                        // pulls entirely ahead of the 5 tag digests that
                        // on-demand creates actually resolve to; queueing
                        // reduces that to whatever the scheduler's LIFO
                        // slot reorders. See the drain for why that is a
                        // bias and not a guarantee.
                        pending_release_pulls.push((template_name.clone(), release.content_hash));
                    }
                }
            }
        }
    }

    // Single dispatch point for the whole tick, tag pulls first.
    //
    // Isolation is the load-bearing reason it lives here rather than at
    // the two decision sites: a template the latest-N pass confirmed
    // org-specific loses its queued tag pull while that pull is still a
    // decision, instead of racing the by-hash gate.
    //
    // Draining tag pulls first also biases a cold node toward the
    // digests on-demand creates resolve to, ahead of older releases —
    // but only as a bias, not a guarantee. `dispatch_pull` takes its
    // semaphore permit inside the spawned task, and while the semaphore
    // is fair it is fair in *request* order, which is not spawn order:
    // the multi-thread scheduler parks the newest task in its LIFO slot
    // and polls that first, so the last release pull spawned here can
    // request a permit before any tag pull. With
    // the prefetch concurrency of 2 that
    // costs one of the two initial permits. Making it a guarantee would
    // mean acquiring the permit in this loop and moving it into the
    // task, which is cheap here (the tick joins every handle before
    // returning anyway) but turns this loop into the pacing loop and
    // needs its own handling for a closed semaphore. Not done, because
    // warming order is an optimization and nothing about the visibility
    // gate depends on it.
    let (dispatchable, suppressed) =
        partition_pending_tag_pulls(pending_tag_pulls, &org_specific_templates);
    for pending in suppressed {
        record_tag_candidate(pending.name, pending.tag, "skipped_org_specific");
    }
    for pending in dispatchable {
        record_tag_candidate(pending.name, pending.tag, "dispatched");
        // Dispatch by the resolved digest, not the tag. The pin written
        // in pass 1 and the artifact we're about to pull are then
        // guaranteed to match — passing the tag would let it flip
        // between that resolve and pull's re-resolve, leaving the pin
        // pointing at one digest while we materialize another.
        //
        // Because we pass the digest (not the tag),
        // `VolumePuller::pull` routes to `FindTemplateReleaseByHash`,
        // which matches `template_releases.hash` directly and tolerates
        // a bare (non-`sha256:`-prefixed) digest.
        spawned.push(dispatch_pull(
            Arc::clone(&db),
            volume_puller.clone(),
            Arc::clone(semaphore),
            pending.name.to_string(),
            pending.hash,
            PullTrigger::PrefetchTag,
        ));
    }
    for (name, hash) in pending_release_pulls {
        spawned.push(dispatch_pull(
            Arc::clone(&db),
            volume_puller.clone(),
            Arc::clone(semaphore),
            name,
            hash,
            PullTrigger::PrefetchLatestRelease,
        ));
    }

    // Wait for this tick's prefetch fan-out to finish before scheduling
    // the next sleep. This keeps the interval honest: "every N seconds
    // *between* ticks", not "tick rate". Join errors are not fatal —
    // a panic in one task shouldn't kill the loop.
    //
    // The join MUST run before we return — bubbling out of `tick` (e.g.
    // via `?` on the prune below) before this point would detach the
    // spawned pulls and let the next tick's sleep overlap with their
    // S3 GETs.
    for handle in spawned {
        if let Err(err) = handle.await
            && !err.is_cancelled()
        {
            warn!(error = %err, "prefetch task panicked");
        }
    }

    // Prune `template_pinned_tags` rows whose (name, tag) is no longer
    // in the const list — or whose template resolved as org-specific
    // this tick (the pin must drop so GC can evict the rootfs; see
    // module docs "Visibility gate"). Done unconditionally so removing
    // a tuple actually drops the pin on the next tick. Runs after the
    // join so a prune failure can't bubble out and detach any
    // in-flight pulls.
    //
    // Both prunes are attempted even if the first fails, and their
    // errors are combined at the end. They are independent cleanups of
    // two different tables, and an org-specific verdict needs BOTH to
    // land for the rootfs to lose its GC protection — so `?`-ing the tag
    // prune here would let a transient SQLite error on one table hold
    // the other table's isolation cleanup for a whole tick.
    let keep = tag_prune_keep_set(tag_tuples, &org_specific_tag_tuples);
    let tag_prune = spawn_blocking_store_op(pins.clone(), move |s| s.prune_tags_outside(&keep))
        .await
        .map_err(|e| format!("prune pinned tags: {e}"));

    // Fold the tick's org-specific verdicts into the release-prune
    // inputs: a tag-pass verdict must flush release pins even when the
    // latest-N pass never produced one for the template (`latest_n = 0`,
    // RPC failure, or a mid-tick skew where the latest pass still saw
    // generic and collected keep entries).
    flush_org_specific_release_pins(
        &org_specific_templates,
        &mut prunable_templates,
        &mut release_pins_keep,
    );

    // Prune `template_pinned_releases` rows for the templates that
    // successfully resolved this tick. Templates whose
    // ListLatestTemplateReleases call failed are intentionally NOT in
    // `prunable_templates` — preserve their last-known pin set.
    let release_prune = if prunable_templates.is_empty() {
        Ok(())
    } else {
        spawn_blocking_store_op(pins.clone(), move |s| {
            s.prune_releases_outside(&prunable_templates, &release_pins_keep)
        })
        .await
        .map_err(|e| format!("prune pinned releases: {e}"))
    };

    combine_prune_errors(tag_prune, release_prune)
}

/// Combine the tick's two pin-prune outcomes into one result, keeping
/// both messages when both failed.
///
/// Neither prune short-circuits the other, because they clean different
/// tables and [`crate::eviction`] reads their *union* — a digest stays
/// protected while either table still pins it. Two consequences of
/// decoupling them, both lost if one `?`s ahead of the other: an
/// org-specific template's release-only digests (older latest-N entries
/// no tag pin covers) become evictable in the same tick the verdict
/// lands, and release-pin hygiene for *every* template stops depending
/// on tag-prune health — a persistently failing tag prune used to mean
/// the release prune never ran at all, so stale latest-N pins
/// accumulated without bound. Pure function so the both-failed case has
/// its own focused test.
fn combine_prune_errors(
    tag_prune: Result<(), String>,
    release_prune: Result<(), String>,
) -> Result<(), String> {
    match (tag_prune, release_prune) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(err), Ok(())) | (Ok(()), Err(err)) => Err(err),
        (Err(tag), Err(release)) => Err(format!("{tag}; {release}")),
    }
}

/// A queued pull borrows its name and tag from this tick's configuration.
struct PendingTagPull<'a> {
    name: &'a str,
    tag: &'a str,
    hash: String,
}

/// Split the tick's queued tag pulls into the ones still safe to
/// dispatch and the ones an org-specific verdict suppressed.
///
/// This is the whole point of queueing rather than dispatching inline:
/// the tag pass decides on the visibility its own resolve returned, but
/// the verdict that governs is the tick's. A template the latest-N pass
/// later confirmed `org_specific` must lose its queued pull here, while
/// it is still a decision — once `dispatch_pull` spawns, nothing recalls
/// it, and the by-hash gate in `pull_inner` reads `Generic` too whenever
/// it beats the flip or hits a stale replica. Pure function so that
/// invariant has its own focused test.
fn partition_pending_tag_pulls<'a>(
    pending: Vec<PendingTagPull<'a>>,
    org_specific_templates: &BTreeSet<String>,
) -> (Vec<PendingTagPull<'a>>, Vec<PendingTagPull<'a>>) {
    pending
        .into_iter()
        .partition(|p| !org_specific_templates.contains(p.name))
}

/// Tag-pin prune keep-set for this tick: every tuple in
/// `SPACE_TEMPLATE_PREFETCH_TAGS` except those belonging to a template
/// confirmed `org_specific` this tick. Excluding them makes the prune
/// actively drop their pins — the data-isolation cleanup — while tuples
/// that merely failed to resolve (or whose visibility is unknown) stay
/// in the keep-set so their existing pins survive a transient outage.
/// Pure function so the invariant has its own focused test.
fn tag_prune_keep_set(
    tag_tuples: &[(TemplateName, TemplateTag)],
    org_specific: &HashSet<(String, String)>,
) -> Vec<(String, String)> {
    tag_tuples
        .iter()
        .map(|(n, t)| ((*n).to_string(), (*t).to_string()))
        .filter(|tuple| !org_specific.contains(tuple))
        .collect()
}

/// Record a confirmed org-specific verdict for `template`, from either
/// pass. Visibility is a template-level attribute, so one verdict drops
/// BOTH pin kinds: every configured tag tuple of the template leaves the
/// tag prune keep-set (not just the tuple that happened to surface the
/// verdict — a tag that stopped resolving shows the tag pass no
/// visibility at all), and the template is queued for the release-pin
/// flush ([`flush_org_specific_release_pins`]). Pure function so the
/// invariant has its own focused test.
fn mark_template_org_specific(
    tag_tuples: &[(TemplateName, TemplateTag)],
    org_specific_tag_tuples: &mut HashSet<(String, String)>,
    org_specific_templates: &mut BTreeSet<String>,
    template: &str,
) {
    org_specific_templates.insert(template.to_string());
    for (name, tag) in tag_tuples {
        let (name, tag) = (name.as_str(), tag.as_str());
        if name == template {
            org_specific_tag_tuples.insert((name.to_string(), tag.to_string()));
        }
    }
}

/// Visibility the latest-N pass should act on for one template: an
/// org-specific verdict already accumulated this tick (by the tag pass)
/// overrides whatever the latest-N response reported. Verdicts only
/// harden within a tick — a skewed or stale `Generic` read must never
/// soften one, or the release loop would warm a template another pass
/// just confirmed private. Pure function so the invariant has its own
/// focused test.
fn hardened_visibility(
    already_org_specific: bool,
    reported: TemplateVisibility,
) -> TemplateVisibility {
    if already_org_specific {
        TemplateVisibility::OrgSpecific
    } else {
        reported
    }
}

/// Fold the tick's org-specific verdicts into the release-prune inputs:
/// every confirmed template becomes prune-eligible and loses any keep
/// entries collected for it. This is the *only* route by which an
/// org-specific verdict reaches the release prune, whichever pass found
/// it: a tag-pass verdict may belong to a template the latest-N pass
/// never resolved (disabled via `latest_n = 0`, or its RPC failed), and
/// a latest-N verdict `continue`s before the prune-eligible push. Data
/// isolation can't depend on which RPC happened to succeed. The
/// keep-entry removal covers the same case for keep entries. Pure
/// function so the invariant has its own focused test.
///
/// The `prunable_templates` membership check is currently unreachable:
/// [`hardened_visibility`] runs before the pass's
/// `prunable_templates.push`, so a template either pass marked always
/// takes the `OrgSpecific` `continue` first, and
/// `unique_prefetch_templates` yields distinct names — the two
/// collections are disjoint by construction. It is kept as cheap
/// defense for a future third writer of `prunable_templates`, not
/// because a mid-tick skew can reach it today.
fn flush_org_specific_release_pins(
    org_specific_templates: &BTreeSet<String>,
    prunable_templates: &mut Vec<String>,
    release_pins_keep: &mut Vec<(String, String)>,
) {
    for template in org_specific_templates {
        if !prunable_templates.iter().any(|t| t == template) {
            prunable_templates.push(template.clone());
        }
    }
    release_pins_keep.retain(|(name, _)| !org_specific_templates.contains(name));
}

/// Run `op` against the [`PinnedDigestsStore`] on a blocking thread.
/// Centralizes the `spawn_blocking` + `rusqlite::Error → String`
/// conversion so the call sites in [`tick`] aren't dominated by
/// boilerplate. The closure runs on a blocking worker because every
/// store method takes the SQLite mutex synchronously.
async fn spawn_blocking_store_op<F>(pins: PinnedDigestsStore, op: F) -> Result<(), String>
where
    F: FnOnce(&PinnedDigestsStore) -> rusqlite::Result<()> + Send + 'static,
{
    tokio::task::spawn_blocking(move || op(&pins))
        .await
        .map_err(|e| format!("spawn_blocking panic: {e}"))?
        .map_err(|e| format!("{e}"))
}

/// Spawn a bounded prefetch pull. The semaphore is held for the full
/// duration of the pull so concurrency is bounded by *active*
/// downloads, not just dispatch rate.
///
/// Pulls are dispatched by the resolved `content_hash` (digest) so
/// the artifact we materialize matches the digest the prefetch tick
/// pinned — no race window where a tag flip mid-tick could leave the
/// pin pointing at one digest and the disk holding another. The
/// in-flight map inside `VolumePuller` is keyed on the resolved
/// digest, so concurrent on-demand and prefetch requests for the
/// same `(name, content_hash)` still coalesce onto one materialization.
///
/// `VolumePuller::pull` routes the cluster lookup to
/// `ClusterApi.FindTemplateReleaseByHash` because `req_hash` is set
/// here, sending the digest as-is against `template_releases.hash`
/// and bypassing any tag-based path entirely.
fn dispatch_pull(
    db: Arc<std::sync::Mutex<rusqlite::Connection>>,
    puller: VolumePuller,
    semaphore: Arc<Semaphore>,
    name: String,
    content_hash: String,
    trigger: PullTrigger,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let _permit = match semaphore.acquire_owned().await {
            Ok(p) => p,
            Err(_) => return, // semaphore closed; daemon shutting down
        };
        let outcome = puller.pull(db, &name, &content_hash, "", trigger).await;
        match outcome {
            crate::volume::PullOutcome::Materialized => {
                info!(
                    template = %name,
                    hash = %content_hash,
                    trigger = trigger.as_tag(),
                    "prefetch materialized",
                );
            }
            crate::volume::PullOutcome::ClassifiedFailure(
                failure @ (crate::volume::PullFailure::ClusterMetadataMissing
                | crate::volume::PullFailure::StorageObjectMissing),
            ) => {
                debug!(
                    template = %name,
                    hash = %content_hash,
                    trigger = trigger.as_tag(),
                    reason = failure.metric_reason(),
                    error = %failure,
                    "prefetch could not materialize build",
                );
            }
            // Own arm because the generic message below promises a retry
            // this failure may not get: for a real `org_specific` flip
            // the next tick's gate skips before dispatch, so nothing
            // retries. (The other way to reach here — the tick's resolve
            // reading `Generic` while the by-hash re-resolve's visibility
            // read fails — *does* recover on a later tick, but only while
            // that disagreement persists: once the read fails for the
            // tick too, the gate skips before dispatch and this arm stops
            // firing.) Report what happened rather than promise an
            // outcome that depends on which of the two reads is failing.
            crate::volume::PullOutcome::ClassifiedFailure(
                failure @ crate::volume::PullFailure::TemplateNotGeneric,
            ) => {
                info!(
                    template = %name,
                    hash = %content_hash,
                    trigger = trigger.as_tag(),
                    reason = failure.metric_reason(),
                    error = %failure,
                    "prefetch pull refused by the visibility gate; not warming this digest",
                );
            }
            crate::volume::PullOutcome::ClassifiedFailure(failure) => {
                warn!(
                    template = %name,
                    hash = %content_hash,
                    trigger = trigger.as_tag(),
                    reason = failure.metric_reason(),
                    error = %failure,
                    "prefetch failed to materialize build; next prefetch cycle will retry",
                );
            }
            crate::volume::PullOutcome::Retryable(status) => {
                info!(
                    template = %name,
                    hash = %content_hash,
                    trigger = trigger.as_tag(),
                    error = %status,
                    "prefetch rejected; next prefetch cycle will retry",
                );
            }
            crate::volume::PullOutcome::DownstreamFailure { failure, status } => {
                warn!(
                    template = %name,
                    hash = %content_hash,
                    trigger = trigger.as_tag(),
                    reason = failure.metric_reason(),
                    error = %status,
                    "prefetch downstream request failed",
                );
            }
            crate::volume::PullOutcome::HashMismatch(status) => {
                warn!(
                    template = %name,
                    hash = %content_hash,
                    trigger = trigger.as_tag(),
                    error = %status,
                    "prefetch failed",
                );
            }
        }
    })
}

fn record_tag_candidate(template: &str, tag: &str, outcome: &'static str) {
    pplx_space_metrics::template::prefetch_tag_candidate(template, tag, outcome);
}

fn record_release_candidate(template: &str, content_hash: &str, outcome: &'static str) {
    pplx_space_metrics::template::prefetch_release_candidate(template, content_hash, outcome);
}

#[cfg(test)]
mod tests;
