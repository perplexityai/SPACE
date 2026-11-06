//! Admission of the lifecycle requests users make.

use pplx_space_common_core::types::SandboxId;
use pplx_space_public_api::SandboxLifecycleAction;

use super::SandboxAction;

/// A lifecycle request a user made, named for what they asked. A wake that their command or
/// access implies counts too, even when it is queued as the system's wake, with no actor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UserLifecycleRequest {
    /// A verb `SandboxController::action` runs, including a wake that the
    /// user's own activity implied.
    Action(SandboxAction),
    /// Deleting the sandbox.
    Delete,
    /// Rolling the sandbox back to one of its snapshots.
    Rollback,
}

impl From<SandboxLifecycleAction> for UserLifecycleRequest {
    fn from(action: SandboxLifecycleAction) -> Self {
        match action {
            SandboxLifecycleAction::Delete => Self::Delete,
            SandboxLifecycleAction::Rollback => Self::Rollback,
            SandboxLifecycleAction::Start => Self::Action(SandboxAction::Start),
            SandboxLifecycleAction::Stop => Self::Action(SandboxAction::Stop),
            SandboxLifecycleAction::Pause => Self::Action(SandboxAction::Pause),
            SandboxLifecycleAction::Resume => Self::Action(SandboxAction::Resume),
            SandboxLifecycleAction::Suspend => Self::Action(SandboxAction::Suspend),
            SandboxLifecycleAction::Restore => Self::Action(SandboxAction::Restore),
            SandboxLifecycleAction::Wake => Self::Action(SandboxAction::Wake),
            SandboxLifecycleAction::Kill => Self::Action(SandboxAction::Kill),
            SandboxLifecycleAction::Checkpoint => Self::Action(SandboxAction::Checkpoint),
            SandboxLifecycleAction::EnableSnapshots => Self::Action(SandboxAction::EnableSnapshots),
            SandboxLifecycleAction::DisableSnapshots => {
                Self::Action(SandboxAction::DisableSnapshots)
            }
        }
    }
}

/// What [`admit_user_lifecycle`] decided for a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UserLifecycleAdmission {
    /// The request proceeds.
    Admitted,
}

/// Admission for the lifecycle requests users make. Every request is admitted.
///
/// A route calls this just before it records a lifecycle operation: the native route only for a
/// request it has not seen, after its idempotency replay check, and the admin, sessions and ECW
/// routes before the store call that resolves replays and joins. `SandboxController::action` calls
/// it on entry, and the operation executor before it runs a delete or a rollback, so a recorded
/// request can be admitted more than once: by its route, and on each executor attempt that reaches
/// one of these calls. The ECW pause, resume and connect handlers, and ingress access to a paused
/// sandbox, call `action` directly and record no operation.
#[expect(
    unused_variables,
    reason = "every request is admitted alike, so nothing reads the sandbox or the request"
)]
pub(crate) fn admit_user_lifecycle(
    sandbox_id: SandboxId,
    request: UserLifecycleRequest,
) -> UserLifecycleAdmission {
    UserLifecycleAdmission::Admitted
}
