//! Admission of the lifecycle requests users make.
//!
//! The cancelled-wake redrive (`controllers::sandbox::wake_completion`) finishes a wake that
//! `SandboxController::action` already admitted, so it does not ask again.

use pplx_space_common_core::types::SandboxId;
use pplx_space_public_api::SandboxLifecycleAction;

use super::SandboxAction;

#[cfg(test)]
pub(crate) mod recorded;

/// A lifecycle request a user made, named for what they asked. A wake that their command or
/// access implies counts too, even when it is queued as the system's wake, with no actor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UserLifecycleRequest {
    /// A verb `SandboxController::action` runs, including a wake that the
    /// user's own activity implied. `SandboxAction::ImplicitWake` also names
    /// the wake that reaching the runtime for a data-plane request, ingress
    /// traffic or an open port implies when it runs without `action`.
    Action(SandboxAction),
    /// Deleting the sandbox.
    Delete,
    /// Rolling the sandbox back to one of its snapshots.
    Rollback,
    /// Creating a sandbox, admitted for the ID drawn for it.
    Create,
    /// Forking the sandbox, admitted for the source sandbox.
    Fork,
    /// The wake a control request needs when it cannot reach the sandbox's
    /// runtime, named by that request's verb (`SandboxAction::Update`).
    ControlWake(SandboxAction),
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
/// Its callers, by family:
///
/// - Recorded lifecycle operations. A route calls this just before it records one: the native
///   route only for a request it has not seen, after its idempotency replay check, and the admin,
///   sessions and ECW routes before the store call that resolves replays and joins.
/// - Their execution. `SandboxController::action` calls it on entry, and the operation executor
///   before it runs a delete or a rollback, so a recorded request can be admitted more than once:
///   by its route, and on each executor attempt that reaches one of these calls. The ECW pause,
///   resume and connect handlers, and ingress access to a paused sandbox, call `action` directly
///   and record no operation.
/// - Creates, as [`UserLifecycleRequest::Create`]. The native and admin routes call it just before
///   the store call that records the create. On the admin route, and on the native route when it
///   does not try a warm-pool claim or its claim attempt fails, that store call also resolves
///   replays and key conflicts, and on the native route the live-sandbox quota, so a replay, a
///   reused key or a create the quota check refuses is admitted too, for an ID it then discards.
///   When the native route tries a claim, it resolves replays, key conflicts and the quota first,
///   and admits a new create for the slot's reserved ID if it claims one, or for a fresh ID if none
///   is free. The ECW create records no operation: `SandboxController::create` calls it for the ID
///   it draws, before anything is written. An ECW create that then adopts a warm-pool slot answers
///   with the slot's reserved ID, so its admission names an ID it discards.
/// - ECW fork, as [`UserLifecycleRequest::Fork`] for the source sandbox, after the route's replay
///   checks and just before the store call that records the fork.
/// - Wakes outside `action`. `connect_spaced_runtime_client` calls it on entry for a connection
///   whose request names an activity, as `SandboxAction::ImplicitWake` of that activity:
///   data-plane calls through the API, native file access grants, their mid-stream reconnects,
///   and ingress access to a sandbox that is suspended, restoring, or started with no reachable
///   runtime. The open-port path calls it the same way before it reaches for the runtime of a
///   sandbox whose port is not yet applied on its node, and `control_runtime_client` before its
///   fallback wake, as [`UserLifecycleRequest::ControlWake`] of the caller's verb. On Kubernetes,
///   the substrate's `runtime_client`, which a connection that names no activity (`wait_ready`)
///   and `control_runtime_client`'s first attempt both call, can still wake a sandbox whose
///   suspend completed during the call, or recreate the pod of a non-dormant sandbox that lost
///   it; neither is admitted.
#[cfg_attr(
    not(test),
    expect(
        unused_variables,
        reason = "every request is admitted alike, so nothing reads the sandbox or the request"
    )
)]
pub(crate) fn admit_user_lifecycle(
    sandbox_id: SandboxId,
    request: UserLifecycleRequest,
) -> UserLifecycleAdmission {
    #[cfg(test)]
    recorded::record(sandbox_id, request);
    UserLifecycleAdmission::Admitted
}

/// Whether a user's lifecycle call `action` supersedes a drain restore of
/// the sandbox, the same rule an operation's kind follows at admission
/// ([`crate::public_operations::supersedes_drain_restore`]): it changes the
/// lifecycle of a `suspended` sandbox, the only one a drain restore waits
/// for, or asks for one.
pub(crate) fn action_supersedes_drain_restore(action: SandboxAction) -> bool {
    match action {
        SandboxAction::Start
        | SandboxAction::Wake
        | SandboxAction::ImplicitWake(_)
        | SandboxAction::Restore
        | SandboxAction::Suspend
        | SandboxAction::Stop
        | SandboxAction::Kill => true,
        // Both need a resident VM, which a suspended sandbox doesn't have:
        // the call gets the answer it would get without a drain, and the
        // restore brings back the VM it can act on.
        SandboxAction::Resume | SandboxAction::Pause => false,
        // A checkpoint answers unavailable on a suspended sandbox without
        // waking it; the snapshot settings are metadata.
        SandboxAction::Checkpoint
        | SandboxAction::EnableSnapshots
        | SandboxAction::DisableSnapshots => false,
        // `action` refuses these; a rollback and an update run through their
        // own operations, whose kinds are judged at admission.
        SandboxAction::Stats
        | SandboxAction::OpenPort
        | SandboxAction::Rollback
        | SandboxAction::Spaced
        | SandboxAction::Update => false,
    }
}
