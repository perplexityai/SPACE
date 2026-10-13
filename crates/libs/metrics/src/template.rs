use std::time::Instant;

use crate::emit::{counter, elapsed_ms, gauge, tag, timing_ms};

/// Terminal result of one build-manager operation; the `result` tag on
/// `template.build.latency_ms` for `op:build`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TemplateBuildResult {
    Success,
    Failed,
    PublishFailed,
}

impl TemplateBuildResult {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Failed => "failed",
            Self::PublishFailed => "publish_failed",
        }
    }
}

impl std::fmt::Display for TemplateBuildResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Result of one named build step; the `result` tag on
/// `template.build.step.latency_ms`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TemplateBuildStepResult {
    Success,
    Failed,
}

impl TemplateBuildStepResult {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Failed => "failed",
        }
    }

    pub fn from_ok(ok: bool) -> Self {
        if ok { Self::Success } else { Self::Failed }
    }
}

impl std::fmt::Display for TemplateBuildStepResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Outcome of the build-manager SIGTERM drain; the `result` tag on
/// `template.build.latency_ms` for `op:drain`. `Timeout` means the
/// process exited with builds still running and is alertable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TemplateBuildDrainResult {
    Completed,
    Timeout,
}

impl TemplateBuildDrainResult {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Timeout => "timeout",
        }
    }
}

impl std::fmt::Display for TemplateBuildDrainResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

metric_vocabulary!(
    /// Why a build or one of its steps failed: the `error_kind` tag on the
    /// failed `template.build.latency_ms` and `template.build.step.latency_ms`
    /// series, absent on successes. Build-manager's logs and traces are kept
    /// out of the log pipelines because they carry recipe and image content,
    /// so this is the cause that reaches a dashboard. It is classified from
    /// the typed error, never from its message, and carries no user content.
    TemplateBuildErrorKind {
        /// Node disk admission refused the build's rootfs, or the image pull
        /// outgrew the builder's storage budget.
        StorageAdmission => "storage_admission",
        /// The registry rejected the image reference, or pulling it failed.
        ImagePull => "image_pull",
        /// Turning the pulled image into the build rootfs failed.
        Unpack => "unpack",
        /// The recipe was invalid, or one of its steps failed in the guest.
        Recipe => "recipe",
        /// A bounded wait expired: guest SSH readiness, a bounded guest
        /// operation, or the compressed rootfs a publish waits for.
        Timeout => "timeout",
        /// Registering or publishing the built template failed.
        Publish => "publish",
        /// Any other failure: an internal RPC, the builder host, or a panic.
        Other => "other",
    }
);

/// End-to-end latency and count for one terminal build-manager job
/// (recipe execution plus publish/register when enabled). Count
/// operations from this metric; there is no separate completion
/// counter. A failed build adds its [`TemplateBuildErrorKind`].
pub fn build_completed(
    template: &str,
    result: TemplateBuildResult,
    error_kind: Option<TemplateBuildErrorKind>,
    started_at: Instant,
) {
    let mut tags = vec![
        tag("op", "build"),
        tag("template", template),
        tag("result", result),
    ];
    tags.extend(error_kind.map(|kind| tag("error_kind", kind)));
    timing_ms("template.build.latency_ms", elapsed_ms(started_at), &tags);
}

/// Latency for one named step within a build. `step` is the bounded
/// build-manager `BuildPhase` vocabulary (`resolve_base`,
/// `start_build_vm`, `recipe_step`, `compact_rootfs`, `publish`, …);
/// individual recipe step labels stay on traces/logs, not metric tags.
/// A failed step adds its [`TemplateBuildErrorKind`].
pub fn build_step_completed(
    template: &str,
    step: &str,
    result: TemplateBuildStepResult,
    error_kind: Option<TemplateBuildErrorKind>,
    started_at: Instant,
) {
    let mut tags = vec![
        tag("op", "build"),
        tag("template", template),
        tag("step", step),
        tag("result", result),
    ];
    tags.extend(error_kind.map(|kind| tag("error_kind", kind)));
    timing_ms(
        "template.build.step.latency_ms",
        elapsed_ms(started_at),
        &tags,
    );
}

/// Duration and outcome of the build-manager SIGTERM drain, emitted on
/// `template.build.latency_ms` as `op:drain` (no `template` tag — the
/// drain covers every running build on the pod).
pub fn build_drain_completed(result: TemplateBuildDrainResult, started_at: Instant) {
    let tags = [tag("op", "drain"), tag("result", result)];
    timing_ms("template.build.latency_ms", elapsed_ms(started_at), &tags);
}

pub fn build_routing_fallback_missing_identity() {
    counter(
        "template.build.routing_fallback",
        1,
        &[tag("reason", "missing_builder_identity")],
    );
}

pub fn pull_duration(tags: &[String], result: &str, started_at: Instant) {
    let mut tags = tags.to_vec();
    tags.push(tag("result", result));
    timing_ms("template.pull.duration_ms", elapsed_ms(started_at), &tags);
}

pub fn pull_coalesced(tags: &[String]) {
    counter("template.pull.coalesced", 1, tags);
}

pub fn pull_s3_phase(tags: &[String], phase: &str, result: &str, started_at: Instant) {
    let mut tags = tags.to_vec();
    tags.push(tag("phase", phase));
    tags.push(tag("result", result));
    timing_ms("template.pull.s3.phase_ms", elapsed_ms(started_at), &tags);
}

pub fn pull_find_release(tags: &[String], result: &str, started_at: Instant) {
    let mut tags = tags.to_vec();
    tags.push(tag("result", result));
    timing_ms(
        "template.pull.find_release_ms",
        elapsed_ms(started_at),
        &tags,
    );
}

pub fn pull_s3_bytes(tags: &[String], region: Option<&str>, bytes: u64) {
    let mut tags = tags.to_vec();
    if let Some(region) = region {
        tags.push(tag("region", region));
    }
    counter("template.pull.s3.bytes", bytes, &tags);
}

pub fn get_cache_hit(tags: &[String]) {
    counter("template.get.cache_hits", 1, tags);
}

pub fn get_cache_miss(tags: &[String]) {
    counter("template.get.cache_misses", 1, tags);
}

pub fn rootfs_expansion_error(template: &str, trigger: &str, reason: &str) {
    counter(
        "template.rootfs.expansion.error",
        1,
        &[
            tag("template", template),
            tag("trigger", trigger),
            tag("reason", reason),
        ],
    );
}

pub fn rootfs_expansion(template: &str, trigger: &str, size_bytes: u64) {
    gauge(
        "template.rootfs.expansion.size_bytes",
        size_bytes as f64,
        &[tag("template", template), tag("trigger", trigger)],
    );
}

/// Why a template-visibility lookup could not produce a definite
/// answer; the `reason` tag on `template.visibility_lookup.degraded`.
///
/// The lookup that emits this is best-effort — it degrades to
/// `UNSPECIFIED` rather than failing release resolution — so a
/// `tracing::warn!` alone would leave the degradation unalertable. Each
/// variant is a distinct operational story, which is why they are
/// separate tag values rather than one `failed`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VisibilityLookupDegradation {
    /// No `templates.visibility` column (SQLSTATE 42703). Expected only
    /// while a control plane predating the visibility migration is
    /// still serving; sustained non-zero afterwards means schema drift.
    ColumnMissing,
    /// Any other query failure: pool exhaustion, connection reset,
    /// permission denied, or the app role's server-side
    /// `statement_timeout` firing — which is also where a lock wait
    /// behind an `ALTER TABLE templates` ends up, since a lock wait
    /// blocks rather than failing fast.
    QueryFailed,
    /// The stored value is not a recognized visibility spelling. The
    /// column's CHECK constraint should make this unreachable, so a
    /// non-zero rate means the constraint is missing or was bypassed.
    UnrecognizedValue,
    /// No `templates` row for the requested name.
    TemplateMissing,
}

impl VisibilityLookupDegradation {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ColumnMissing => "column_missing",
            Self::QueryFailed => "query_failed",
            Self::UnrecognizedValue => "unrecognized_value",
            Self::TemplateMissing => "template_missing",
        }
    }
}

impl std::fmt::Display for VisibilityLookupDegradation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One per release-resolution response whose template visibility fell
/// back to `UNSPECIFIED`.
///
/// Alertability differs per reason, so don't alert on the bare metric:
/// `template_missing` is caller-driven and never alertable;
/// `column_missing` is expected while the visibility migration is still
/// rolling out and alertable once it has completed everywhere;
/// `query_failed` and `unrecognized_value` are always alertable.
pub fn visibility_lookup_degraded(reason: VisibilityLookupDegradation) {
    counter(
        "template.visibility_lookup.degraded",
        1,
        &[tag("reason", reason.as_str())],
    );
}

/// How one template-visibility lookup was answered; the `outcome` tag on
/// `template.visibility_lookup.cache`.
///
/// Deliberately a separate counter from
/// `template.visibility_lookup.degraded` rather than more `reason` values on
/// it. That one means exactly "the response carried UNSPECIFIED", which is
/// what a prefetch-suppression monitor fires on; folding an absorbed blip into
/// it would make the monitor fire on a lookup that answered correctly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VisibilityLookupCacheOutcome {
    /// Answered from a cached read still inside the freshness window. No
    /// query ran. The steady-state majority.
    Fresh,
    /// Queried and got a concrete answer.
    ///
    /// Not "and stored it": the store is dropped when both windows are disabled,
    /// when the name is too long to key, when a strictly-later observation is
    /// already resident, and when the map's write lock is poisoned. This counts
    /// reaching the database and getting a value that parsed, which is what the
    /// load check wants.
    ///
    /// So `refreshed` alone never accounts for the traffic, kill switch or not.
    /// The other outcomes are not only the failure ones: a lookup finding no row
    /// reports `absent`, a failed query reports `miss`, and — the one an earlier
    /// wording of this note mis-partitioned — a lookup that *does* reach a row
    /// whose value fails to parse also reports `miss`, not `refreshed`. Reaching
    /// a row is necessary for `refreshed` and not sufficient.
    Refreshed,
    /// The query could not reach the row, and a cached read inside the
    /// staleness ceiling answered instead. **This is the database-health
    /// signal:** a non-zero rate means the lookup is failing and only the
    /// cache is keeping visibility-gated consumers from failing closed.
    ServedStale,
    /// The row was read and there is none. Caller-driven and **never
    /// alertable**: the `ListLatestTemplateReleases` contract lets a caller name
    /// a template that does not exist.
    ///
    /// Split out from `Miss` deliberately, and for one reason precisely: merged,
    /// a node polling an unknown name holds `miss` permanently non-zero, which
    /// gives `miss` a routine floor unrelated to degraded enforcement and makes
    /// `miss`-based alerting unusable. (It does *not* affect the `refreshed`
    /// share, whose denominator is `fresh + refreshed` — an earlier version of
    /// this comment claimed it did.)
    Absent,
    /// Nothing usable for any other reason — the query failed with no cached read
    /// inside the ceiling, the answer would not parse, or a durable failure
    /// invalidated. The response carries UNSPECIFIED and
    /// `template.visibility_lookup.degraded` fires alongside this with the
    /// specific reason.
    Miss,
}

impl VisibilityLookupCacheOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Fresh => "fresh",
            Self::Refreshed => "refreshed",
            Self::ServedStale => "served_stale",
            Self::Absent => "absent",
            Self::Miss => "miss",
        }
    }
}

impl std::fmt::Display for VisibilityLookupCacheOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One per template-visibility lookup, tagged by how it was answered.
///
/// Exactly one of these is emitted per lookup, so the sum is the lookup rate
/// and `outcome:refreshed` is the share that reached the database — which is
/// how you check the cache is actually absorbing load. Alert on
/// `outcome:served_stale`, not on `outcome:miss` alone: `served_stale` is the
/// early warning (the read is broken but consumers are still being answered),
/// and `miss` is the state that has already degraded enforcement.
pub fn visibility_lookup_cache(outcome: VisibilityLookupCacheOutcome) {
    counter(
        "template.visibility_lookup.cache",
        1,
        &[tag("outcome", outcome.as_str())],
    );
}

/// One per template-manager prefetch tick. Emits both the
/// `template.prefetch.tick` counter and the
/// `template.prefetch.tick_duration_ms` timing under a single
/// `result:ok|error` tag so dashboards can stay in sync.
///
/// Detect a stuck loop from absence of the counter, including error outcomes.
/// Configure backend tag retention before filtering on `result`. The counter
/// is written after the tick returns, covering both hangs and exited loops.
pub fn prefetch_tick(result: &str, started_at: Instant) {
    let tags = [tag("result", result)];
    counter("template.prefetch.tick", 1, &tags);
    timing_ms(
        "template.prefetch.tick_duration_ms",
        elapsed_ms(started_at),
        &tags,
    );
}

/// One per `(template, tag)` decision in the prefetch tag-pin phase.
/// `outcome` distinguishes the resolved states:
/// `dispatched`, `already_cached`, `unresolved` (tag doesn't exist on
/// the template — normal), `resolve_error` (transport / cluster
/// error), `pin_error` (cluster resolve succeeded but the local
/// SQLite pin upsert failed — operator-actionable),
/// `skipped_org_specific` (the template is `org_specific`, so
/// prefetch refused to warm it and dropped its tag pin — the
/// data-isolation gate acting), and `visibility_unknown` (the cluster
/// could not report visibility, so prefetch skipped warming but
/// preserved any existing pin).
pub fn prefetch_tag_candidate(template: &str, tag_value: &str, outcome: &str) {
    counter(
        "template.prefetch.tag.candidates",
        1,
        &[
            tag("template", template),
            tag("tag", tag_value),
            tag("outcome", outcome),
        ],
    );
}

/// One per `(template, content_hash)` decision in the prefetch
/// latest-N release phase. `outcome` mirrors
/// `template.prefetch.tag.candidates`:
/// `dispatched`, `already_cached`, `coalesced_with_tag` (a tag pass
/// in the same tick already dispatched this digest), `resolve_error`
/// (cluster-side error from ListLatestTemplateReleases), `pin_error`
/// (cluster lookup succeeded but the SQLite release-pin upsert
/// failed — operator-actionable), plus the two template-level
/// visibility outcomes `skipped_org_specific` and
/// `visibility_unknown`. Those two are emitted once per template with
/// an empty `hash` tag, since the gate is template-level rather
/// than per-digest. `resolve_error` is emitted the same way, so an
/// empty `hash` tag does not by itself mean a visibility outcome.
/// `visibility_unknown` is not emitted when the template also
/// resolved zero releases, so it does not count every template whose
/// visibility was unknown.
pub fn prefetch_release_candidate(template: &str, content_hash: &str, outcome: &str) {
    counter(
        "template.prefetch.release.candidates",
        1,
        &[
            tag("template", template),
            tag("hash", content_hash),
            tag("outcome", outcome),
        ],
    );
}

metric_vocabulary!(
    /// Why a template-manager GC tick failed: the `reason` tag on its
    /// `result:error` series. Every one skips the whole tick, evicting
    /// nothing.
    TemplateGcTickFailure {
        /// sandbox-manager could not say which templates running sandboxes
        /// use, so nothing is provably idle.
        ActiveSetUnavailable => "active_set_unavailable",
        /// The control plane could not say which templates warm pools need.
        WarmPoolUnavailable => "warm_pool_unavailable",
        /// Reading the eviction inputs from the template database failed.
        PlanSnapshot => "plan_snapshot",
        /// Composing the eviction plan from those inputs failed.
        Plan => "plan",
        /// The blocking task that reads or plans was lost.
        Task => "task",
    }
);

/// One per tick of the template-manager LRU eviction loop. Emits the
/// `template.gc.tick` counter and the `template.gc.tick_duration_ms`
/// timing under a single `result:ok|error` tag so dashboards can stay
/// in sync; an error adds its [`TemplateGcTickFailure`] as `reason`.
/// Monitor absence across both outcomes to detect a stuck loop; any
/// tag filters also need backend retention configuration. The
/// `template.gc.bytes_before` gauge reports the last tick's disk footprint;
/// `template.gc.bytes_freed` counts bytes released per tick, so
/// budget headroom can be plotted alongside actual eviction work.
pub fn gc_tick(
    result: Result<(), TemplateGcTickFailure>,
    started_at: Instant,
    bytes_before: u64,
    bytes_freed: u64,
    candidates: u64,
) {
    let tags = match result {
        Ok(()) => vec![tag("result", "ok")],
        Err(reason) => vec![tag("result", "error"), tag("reason", reason)],
    };
    counter("template.gc.tick", 1, &tags);
    timing_ms(
        "template.gc.tick_duration_ms",
        elapsed_ms(started_at),
        &tags,
    );
    gauge("template.gc.bytes_before", bytes_before as f64, &[]);
    counter("template.gc.bytes_freed", bytes_freed, &[]);
    counter("template.gc.candidates", candidates, &[]);
}

/// One per build deleted by the eviction loop. Tagged with the
/// template name so dashboards can rank "what gets evicted most"; the
/// content hash is intentionally NOT a tag (high-cardinality —
/// digests change on every build), but is logged.
pub fn gc_deleted(template: &str) {
    counter("template.gc.deleted", 1, &[tag("template", template)]);
}

/// One per eviction candidate that was skipped at execute time. The
/// `reason` enumerates the four [`crate::template::gc_skip_reason`]
/// labels we surface today:
/// `pinned_at_execute|active_at_execute|row_missing|error`. `error`
/// is the disk-or-DB failure path; the candidate stays on disk for
/// the next tick to retry.
pub fn gc_skipped(reason: &str) {
    counter("template.gc.skipped", 1, &[tag("reason", reason)]);
}

/// One per stuck-`state='deleting'` row resolved by the start-of-tick
/// reaper. `resolution:reset_to_ready` is the "disk intact, retry on
/// next plan" path; `resolution:drop_row` is the "disk gone,
/// resolve_template stops 404'ing" path. Non-zero counts at steady
/// state indicate that some prior tick's Phase 2 errored (transient
/// nodemgr unreachable, slow EBS delete, etc.) — useful as a leading
/// indicator of nodemgr health rather than as an error signal in
/// itself.
pub fn gc_reaper_resolution(resolution: &str) {
    counter("template.gc.reaper", 1, &[tag("resolution", resolution)]);
}

pub fn digest_pins_pruned(count: u64) {
    if count == 0 {
        return;
    }
    counter("template.digest_pins.pruned", count, &[]);
}

/// The requested content hash uses the legacy `version` tag on DogStatsD and
/// `version.requested` on OTLP, separate from the emitting process version.
pub fn fetch_error(name: &str, req_hash: &str, req_tag: &str, reason: &str) {
    let tags = vec![
        tag("template", name),
        tag(
            "tag",
            if req_tag.is_empty() {
                "<unset>"
            } else {
                req_tag
            },
        ),
        tag(
            "version",
            if req_hash.is_empty() {
                "<unset>"
            } else {
                req_hash
            },
        ),
        tag("reason", reason),
    ];
    counter("template.fetch.error", 1, &tags);
}

/// Build the base tag set carried on every pull-path metric. `trigger`
/// distinguishes who issued the pull so dashboards can split S3
/// download bandwidth, latency, and coalesce counts between caller-
/// driven and prefetcher-driven work — see `PullTrigger` in the
/// template-manager. Pass a low-cardinality value: today
/// `"on_demand"` or `"prefetch_tag"`.
pub fn request_tags(name: &str, req_hash: &str, req_tag: &str, trigger: &str) -> Vec<String> {
    vec![
        tag("template", name),
        tag("tag", request_tag_label(req_hash, req_tag)),
        tag("trigger", trigger),
    ]
}

fn request_tag_label<'a>(req_hash: &str, req_tag: &'a str) -> &'a str {
    if !req_hash.is_empty() {
        "by_hash"
    } else if !req_tag.is_empty() {
        req_tag
    } else {
        "latest"
    }
}
