//! `cleanup_*` sandbox-event emission for the orphaned-restore-pod GC. Maps a
//! per-orphan outcome to the matching [`SandboxEventType`] and populates the
//! shared event details, so a sandbox's timeline records that its stranded
//! restore pod was reaped.

use std::time::Duration;

use pplx_space_common_core::types::SandboxId;
use pplx_space_events::{
    SandboxEvent, SandboxEventDetails, SandboxEventSource, SandboxEventTrigger, SandboxEventType,
};

use super::classify::RestorePodSnapshot;

/// What a live reap records on the `space.sandbox` stream. `Attempting` is
/// emitted immediately before the DELETE — so a controller
/// killed mid-delete, or a timeout, still leaves evidence the cleanup was
/// attempted — and every other variant closes it. An accepted DELETE closes
/// nothing yet: the pod is only terminating, so `Deleted` is emitted once the
/// pod is observed gone, and `TeardownStuck` if it lingers. Only live
/// deletions emit these — a dry-run "would-reap" is not a real state
/// transition, so it stays out of the stream (metric + log only).
pub(crate) enum CleanupOutcome {
    Attempting,
    /// The pod this loop deleted is confirmed gone (absent from the pod cache).
    Deleted,
    /// The pod was already gone at delete time (404) — the cleanup goal is met.
    AlreadyGone,
    /// The UID + resourceVersion delete precondition failed (409): the pod at
    /// this name/version is no longer the one we verified — a newer pod
    /// replaced it, or it changed under us. Kubernetes returns 409 for a
    /// resourceVersion-only change too, so the same pod may still exist; the
    /// request closes as `cleanup_failed` (not a durable success) so it doesn't
    /// dangle, and the reap retries next tick with a fresh resourceVersion.
    StaleIdentity,
    /// The fenced reap exceeded the tick's wall-clock budget; its outcome is
    /// unknown and the next tick re-attempts.
    TimedOut,
    /// The DELETE was accepted but the pod has stayed terminating past the
    /// stuck-teardown threshold; its VM may still be running. A later
    /// `cleanup_completed` follows if the pod does eventually disappear.
    TeardownStuck,
    Failed(String),
}

pub(crate) fn emit_cleanup_event(
    sandbox_id: SandboxId,
    pod: &RestorePodSnapshot,
    observed_age: Duration,
    outcome: CleanupOutcome,
) {
    // The event type is fully determined by the outcome, so derive it here.
    let event_type = match &outcome {
        CleanupOutcome::Attempting => SandboxEventType::CleanupRequested,
        CleanupOutcome::Deleted | CleanupOutcome::AlreadyGone => SandboxEventType::CleanupCompleted,
        CleanupOutcome::StaleIdentity
        | CleanupOutcome::TimedOut
        | CleanupOutcome::TeardownStuck
        | CleanupOutcome::Failed(_) => SandboxEventType::CleanupFailed,
    };
    let mut details = SandboxEventDetails::default()
        .reason("orphaned_restore_pod")
        .operation("orphaned_restore_pod_gc")
        .trigger(SandboxEventTrigger::System)
        .age_secs(observed_age.as_secs().to_string())
        .pod_name(pod.pod_name.clone());
    if let Some(pod_uid) = &pod.pod_uid {
        details = details.pod_uid(pod_uid.as_str());
    }
    // A bound pod (has `spec.nodeName`) had already started an unrouted VM on
    // that node; an unbound (still-`Unschedulable`) pod is the pre-bind orphan
    // reaped before it could start one. The presence of the node name records
    // which case this was.
    if let Some(node_name) = &pod.node_name {
        details = details.k8s_node_name(node_name);
    }
    if let Some(generation) = pod.generation {
        details = details.execution_generation(generation.to_string());
    }
    details = match outcome {
        CleanupOutcome::Attempting => details.result("attempting"),
        CleanupOutcome::Deleted => details.result("deleted"),
        CleanupOutcome::AlreadyGone => details.result("already_gone"),
        CleanupOutcome::StaleIdentity => details.result("stale_identity"),
        CleanupOutcome::TimedOut => details.result("timed_out"),
        CleanupOutcome::TeardownStuck => details.result("teardown_stuck"),
        CleanupOutcome::Failed(error) => details.result("delete_failed").error(error),
    };
    SandboxEvent::new(
        sandbox_id,
        event_type,
        SandboxEventSource::ClusterController,
    )
    .emit_with(details);
}
