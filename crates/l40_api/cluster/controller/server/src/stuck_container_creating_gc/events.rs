//! `cleanup_*` sandbox-event emission for the GC. Maps a per-candidate outcome
//! to the matching [`SandboxEventType`] and populates the shared event details.

use pplx_space_common_core::types::SandboxId;
use pplx_space_events::{
    SandboxEvent, SandboxEventDetails, SandboxEventSource, SandboxEventTrigger, SandboxEventType,
};

use super::classify::PodSnapshot;

/// What a `cleanup_*` sandbox event should record as its result. The
/// non-`Attempting` variants are all terminal, so every emitted
/// `cleanup_requested` is eventually closed by one of them. Only live deletions
/// emit these events — a dry-run "would-delete" is not a real state transition,
/// so it stays out of the `space.sandbox` stream (metric + log only).
pub(crate) enum CleanupOutcome {
    Attempting,
    Deleted,
    /// The pod was already gone at delete time (404) — the cleanup goal is met.
    AlreadyGone,
    /// A delete precondition failed (409): the object changed under us, so we
    /// correctly did not delete it.
    SupersededIdentity,
    /// The DELETE exceeded the tick's wall-clock budget; its outcome is unknown
    /// and the next tick re-attempts.
    TimedOut,
    Failed(String),
}

pub(crate) fn emit_cleanup_event(
    sandbox_id: SandboxId,
    pod: &PodSnapshot,
    node_name: &str,
    age_secs: i64,
    outcome: CleanupOutcome,
) {
    // The event type is fully determined by the outcome, so derive it here
    // rather than threading a correlated second argument through every call.
    let event_type = match &outcome {
        CleanupOutcome::Attempting => SandboxEventType::CleanupRequested,
        CleanupOutcome::Deleted
        | CleanupOutcome::AlreadyGone
        | CleanupOutcome::SupersededIdentity => SandboxEventType::CleanupCompleted,
        CleanupOutcome::TimedOut | CleanupOutcome::Failed(_) => SandboxEventType::CleanupFailed,
    };
    let mut details = SandboxEventDetails::default()
        .reason("stuck_container_creating")
        .operation("stuck_container_creating_gc")
        .trigger(SandboxEventTrigger::System)
        .k8s_node_name(node_name)
        .age_secs(age_secs.to_string())
        .pod_name(pod.pod_name.clone());
    if let Some(pod_uid) = &pod.pod_uid {
        details = details.pod_uid(pod_uid.as_str());
    }
    details = match outcome {
        CleanupOutcome::Attempting => details.result("attempting"),
        CleanupOutcome::Deleted => details.result("deleted"),
        CleanupOutcome::AlreadyGone => details.result("already_gone"),
        CleanupOutcome::SupersededIdentity => details.result("stale_identity"),
        CleanupOutcome::TimedOut => details.result("timed_out"),
        CleanupOutcome::Failed(error) => details.result("delete_failed").error(error),
    };
    SandboxEvent::new(
        sandbox_id,
        event_type,
        SandboxEventSource::ClusterController,
    )
    .emit_with(details);
}
