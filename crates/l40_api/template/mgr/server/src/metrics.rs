//! Template-manager metrics: template pulls and the get cache, rootfs
//! expansion, prefetch and garbage collection.

use std::time::Instant;

use pplx_space_metrics_emit::{counter, elapsed_ms, gauge, metric_vocabulary, tag, timing_ms};

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
/// `reason` is one of the five labels the execute-time checks in
/// `eviction.rs` record:
/// `pinned_at_execute|active_at_execute|warm_pool_at_execute|row_missing|error`.
/// `error` is the disk-or-DB failure path; the candidate stays on disk for
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
