//! One warm-pool reconcile pass: its root span and the per-pass summary of
//! the per-config stages it ran.

use std::time::Duration;

/// A per-config stage that [`super::Reconciler::reconcile_config_stage`] times,
/// in the order a config runs them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ConfigStage {
    DeleteMismatchedOpenSlots,
    ObserveReadySlots,
    ObserveWarmingSlots,
    EvictSlotsOnCordonedNodes,
    DeleteExcessOpenSlots,
    SlotCounts,
}

impl ConfigStage {
    /// The `stage` value of the stage's log lines and of
    /// `warm_pool.reconcile.stage.duration_ms`.
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::DeleteMismatchedOpenSlots => "delete_mismatched_open_slots",
            Self::ObserveReadySlots => "observe_ready_slots",
            Self::ObserveWarmingSlots => "observe_warming_slots",
            Self::EvictSlotsOnCordonedNodes => "evict_slots_on_cordoned_nodes",
            Self::DeleteExcessOpenSlots => "delete_excess_open_slots",
            Self::SlotCounts => "slot_counts",
        }
    }
}

/// One stage across every config a pass reconciled.
#[derive(Debug, Default, Clone, Copy)]
pub(super) struct StageTally {
    configs: usize,
    failed_configs: usize,
    longest: Duration,
}

impl StageTally {
    /// `ok` when the stage succeeded for every config that ran it, `err` when
    /// it failed for any; absent when no config ran it.
    pub(super) fn result(&self) -> Option<&'static str> {
        match (self.configs, self.failed_configs) {
            (0, _) => None,
            (_, 0) => Some("ok"),
            _ => Some("err"),
        }
    }

    /// The longest the stage took for one config. Configs reconcile
    /// concurrently, so a sum would not be time the pass spent.
    pub(super) fn duration_ms(&self) -> Option<u64> {
        (self.configs > 0).then(|| u64::try_from(self.longest.as_millis()).unwrap_or(u64::MAX))
    }

    pub(super) fn failed_configs(&self) -> Option<usize> {
        (self.configs > 0).then_some(self.failed_configs)
    }
}

/// What one pass reconciled, collected while it runs and logged once when it
/// ends.
#[derive(Debug, Default)]
pub(super) struct PassReport {
    delete_mismatched_open_slots: StageTally,
    observe_ready_slots: StageTally,
    observe_warming_slots: StageTally,
    evict_slots_on_cordoned_nodes: StageTally,
    delete_excess_open_slots: StageTally,
    slot_counts: StageTally,
    /// Reconcilable configs the pass reconciled.
    pub(super) configs: usize,
    /// Of those, the configs whose reconcile failed or skipped a required
    /// operation, as `warm_pool.reconcile.config_state` reports them.
    pub(super) failed_configs: usize,
}

impl PassReport {
    pub(super) fn record_stage(&mut self, stage: ConfigStage, duration: Duration, ok: bool) {
        let tally = self.tally_mut(stage);
        tally.configs += 1;
        if !ok {
            tally.failed_configs += 1;
        }
        tally.longest = tally.longest.max(duration);
    }

    pub(super) fn stage(&self, stage: ConfigStage) -> StageTally {
        match stage {
            ConfigStage::DeleteMismatchedOpenSlots => self.delete_mismatched_open_slots,
            ConfigStage::ObserveReadySlots => self.observe_ready_slots,
            ConfigStage::ObserveWarmingSlots => self.observe_warming_slots,
            ConfigStage::EvictSlotsOnCordonedNodes => self.evict_slots_on_cordoned_nodes,
            ConfigStage::DeleteExcessOpenSlots => self.delete_excess_open_slots,
            ConfigStage::SlotCounts => self.slot_counts,
        }
    }

    fn tally_mut(&mut self, stage: ConfigStage) -> &mut StageTally {
        match stage {
            ConfigStage::DeleteMismatchedOpenSlots => &mut self.delete_mismatched_open_slots,
            ConfigStage::ObserveReadySlots => &mut self.observe_ready_slots,
            ConfigStage::ObserveWarmingSlots => &mut self.observe_warming_slots,
            ConfigStage::EvictSlotsOnCordonedNodes => &mut self.evict_slots_on_cordoned_nodes,
            ConfigStage::DeleteExcessOpenSlots => &mut self.delete_excess_open_slots,
            ConfigStage::SlotCounts => &mut self.slot_counts,
        }
    }
}

/// A count as a span attribute. tracing-opentelemetry records an unsigned
/// value as its debug string, so counts are recorded signed to stay numeric.
fn span_count(count: usize) -> i64 {
    i64::try_from(count).unwrap_or(i64::MAX)
}

/// The root span of one warm-pool reconcile pass, from the reconciler's
/// interval timer.
///
/// No request causes a pass, so it links to nothing and the configured
/// sampler decides it, as for any root without an origin. It parents
/// everything the pass does: its Postgres queries, Kubernetes calls and
/// sandbox-manager probes, and the lines it logs.
pub(super) fn span() -> tracing::Span {
    tracing::info_span!(
        parent: None,
        "warm_pool.pass",
        otel.kind = "internal",
        "operation.name" = "warm_pool.pass",
        configs = tracing::field::Empty,
        failed_configs = tracing::field::Empty,
        otel.status_code = tracing::field::Empty,
        otel.status_description = tracing::field::Empty,
    )
}

/// Record a finished pass on its root: an error when the pass failed or any
/// config failed, as `node_ctl.pass` does for a failed ref.
pub(super) fn record_outcome(span: &tracing::Span, report: &PassReport, pass_ok: bool) {
    span.record("configs", span_count(report.configs));
    span.record("failed_configs", span_count(report.failed_configs));
    if !pass_ok {
        span.record("otel.status_code", "ERROR");
        span.record(
            "otel.status_description",
            "the pass failed before it reconciled the pool configs",
        );
    } else if report.failed_configs > 0 {
        span.record("otel.status_code", "ERROR");
        span.record(
            "otel.status_description",
            format!(
                "{} of {} pool configs did not reconcile",
                report.failed_configs, report.configs
            )
            .as_str(),
        );
    }
}

/// The pass summary line: the given fields and message, preceded by each
/// stage's `<stage>_result`, `<stage>_duration_ms` and
/// `<stage>_failed_configs`, which are absent when no config ran the stage.
macro_rules! pass_summary {
    ($level:expr, $report:expr, $($fields:tt)*) => {{
        use $crate::warm_pool::pass::ConfigStage;
        let report: &$crate::warm_pool::pass::PassReport = $report;
        let delete_mismatched = report.stage(ConfigStage::DeleteMismatchedOpenSlots);
        let observe_ready = report.stage(ConfigStage::ObserveReadySlots);
        let observe_warming = report.stage(ConfigStage::ObserveWarmingSlots);
        let evict_cordoned = report.stage(ConfigStage::EvictSlotsOnCordonedNodes);
        let delete_excess = report.stage(ConfigStage::DeleteExcessOpenSlots);
        let slot_counts = report.stage(ConfigStage::SlotCounts);
        tracing::event!(
            $level,
            reconciled_configs = report.configs,
            failed_configs = report.failed_configs,
            delete_mismatched_open_slots_result = delete_mismatched.result(),
            delete_mismatched_open_slots_duration_ms = delete_mismatched.duration_ms(),
            delete_mismatched_open_slots_failed_configs = delete_mismatched.failed_configs(),
            observe_ready_slots_result = observe_ready.result(),
            observe_ready_slots_duration_ms = observe_ready.duration_ms(),
            observe_ready_slots_failed_configs = observe_ready.failed_configs(),
            observe_warming_slots_result = observe_warming.result(),
            observe_warming_slots_duration_ms = observe_warming.duration_ms(),
            observe_warming_slots_failed_configs = observe_warming.failed_configs(),
            evict_slots_on_cordoned_nodes_result = evict_cordoned.result(),
            evict_slots_on_cordoned_nodes_duration_ms = evict_cordoned.duration_ms(),
            evict_slots_on_cordoned_nodes_failed_configs = evict_cordoned.failed_configs(),
            delete_excess_open_slots_result = delete_excess.result(),
            delete_excess_open_slots_duration_ms = delete_excess.duration_ms(),
            delete_excess_open_slots_failed_configs = delete_excess.failed_configs(),
            slot_counts_result = slot_counts.result(),
            slot_counts_duration_ms = slot_counts.duration_ms(),
            slot_counts_failed_configs = slot_counts.failed_configs(),
            $($fields)*
        )
    }};
}
pub(super) use pass_summary;
