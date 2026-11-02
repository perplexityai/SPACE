//! Whether the control plane superseded an execution generation: the predicate
//! `BeginArtifactBackup`'s generation fence refuses a capture on, and the one
//! the orphaned-restore-pod GC reaps on. A reap starts the pod's teardown
//! suspend, so on a sandbox row that still exists (a soft-deleted tombstone
//! counts as missing) the GC reaps only a pod whose generation this predicate
//! supersedes, as of the GC's own read. The fence then refuses that pod's
//! capture if it checks it: it checks only an artifact's first registration
//! that carries an execution generation, and admits any other registration
//! unchecked. Both decide
//! with the same function ([`execution_generation_superseded`]). Each query
//! spells out the four [`ExecutionSupersessionRow`] columns at its own site,
//! so a change to how a column is read must be made at both.
//!
//! The fence runs in internal-api and the GC in the cluster controller, so a
//! change to this predicate reaches them at different times. When it makes
//! the predicate hold in more cases, deploy internal-api first: until then
//! the GC would reap pods whose capture the older fence still admits. When it
//! makes it hold in fewer, deploy the cluster controller first.

/// One `sandbox_k8s_executions` row as [`execution_generation_superseded`]
/// reads it, selected as `generation, active, state = 'replaced',
/// pod_materialized_at IS NOT NULL OR NULLIF(BTRIM(pod_uid), '') IS NOT NULL`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionSupersessionRow {
    pub generation: i64,
    pub active: bool,
    /// `state = 'replaced'`.
    pub replaced: bool,
    /// The execution created a pod: `pod_materialized_at` is set, or a
    /// non-blank `pod_uid` on a row that predates that record.
    ///
    /// `pod_materialized_at` is the set-once record that the execution
    /// created a pod, which the substrate's execution store writes in the
    /// same statements that record or clear a `pod_uid`; `pod_uid` alone is
    /// current state, which re-materialization, failed-recovery pod absence
    /// and a cross-node re-drive's reset clear after the pod may have run. The
    /// record is NULL on rows that predate it until the backfill stamps them,
    /// so a non-blank `pod_uid` counts too, and a row whose `pod_uid` was
    /// cleared before it was stamped reads as never having run, as it did
    /// before the record existed.
    ///
    /// The store records a pod's UID when it creates the pod, before the
    /// scheduler places it, so a created pod counts even if it never started:
    /// a source-pinned restore pod ruled Unschedulable leaves its execution
    /// stamped after the cross-node re-drive deletes it and resets the row.
    /// That's deliberate. Counting only pods with a recorded IP or state would
    /// miss a pod that ran before either was recorded, and admitting a capture
    /// over a pod that ran registers stale bytes as the latest snapshot.
    pub materialized: bool,
}

impl ExecutionSupersessionRow {
    pub const fn new(generation: i64, active: bool, replaced: bool, materialized: bool) -> Self {
        Self {
            generation,
            active,
            replaced,
            materialized,
        }
    }
}

/// Whether `generation` of a sandbox is superseded, given that sandbox's
/// execution rows at and above it (rows below it are ignored): a newer
/// execution is active or has materialized a pod, or this generation's own
/// execution was retired as `replaced` (an abandoned restore claim, a
/// started-recovery replacement). Either way no restore can adopt the runtime
/// of that generation.
///
/// A newer claim abandoned before it created a pod never ran, so it takes
/// nothing from this execution. An execution retired for any other reason
/// with nothing newer isn't superseded: its runtime may hold the sandbox's
/// latest state. The predicate is the generation, which retirement works in;
/// it doesn't tell apart two attempts of the same still-active generation.
pub fn execution_generation_superseded<'a>(
    executions: impl IntoIterator<Item = &'a ExecutionSupersessionRow>,
    generation: i64,
) -> bool {
    executions
        .into_iter()
        .any(|execution| match execution.generation.cmp(&generation) {
            std::cmp::Ordering::Greater => execution.active || execution.materialized,
            std::cmp::Ordering::Equal => !execution.active && execution.replaced,
            std::cmp::Ordering::Less => false,
        })
}

#[cfg(test)]
mod tests;
