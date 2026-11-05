//! Post-action stamps: best-effort writes of a lifecycle action's
//! user-facing result onto `sandboxes`, made by the api's lifecycle
//! dispatcher after the substrate call succeeds (for example `paused` after
//! a pause, or `started`/`on_node` after a start). They are not repairs made
//! while answering a read.
//!
//! Each stamp writes only over a row whose lifecycle, compared case- and
//! whitespace-insensitively, is admitted by [`ActionStamp::admits`]: the
//! lifecycles the row can hold while that action is in flight with no other
//! intent owning it. The api's action dispatcher checks no lifecycle at entry
//! for start, resume, pause, stop, kill or suspend (the native operation
//! executor skips an action whose target the row already holds, and a
//! Kubernetes start re-reads the row and refuses what it cannot start). None
//! of them writes a lifecycle of its own before its substrate call, except
//! Kubernetes paths that join or drive a restore first: a start of a
//! `suspended` row claims `restoring` through the wake, and the runtime
//! client a stop, kill or resume uses joins a `restoring` row's restore and
//! dials only once the row reads `started`. A stop or kill on a draining
//! node uses the non-recovering client instead, which joins no restore and
//! refuses a `restoring` row with a 503. So only a Kubernetes start can
//! find a `restoring` of its own (see the `restoring` bullet).
//! A Kubernetes suspend also claims
//! `suspending` before its node call, but it writes no post-action stamp. So
//! the set is every lifecycle an action may begin from that no other intent
//! owns, plus the results a concurrent completion of the same action leaves
//! (a node's `started` receipt, a Kubernetes restore owner's `started`).
//! Every other value is refused, except where a bullet below says otherwise:
//!
//! * `failed`, `quarantined`, `deleted` and `cleanup_required`: the safety
//!   contract with the transient-lifecycle sweeper. If the sweeper failed the
//!   row (and tore its execution down) while the substrate call was in
//!   flight, the stamp must not resurrect it as live.
//! * `deleting`: the api's delete owns the row. It finalizes the delete or,
//!   if it fails, puts back the lifecycle it read when it began
//!   ([`crate::delete::restore_lifecycle_from_deleting`]). That value
//!   predates this action's result, so after a failed delete the row can
//!   describe the sandbox as it was before the action, for example `stopped`
//!   while the sandbox a refused start booted runs.
//! * `suspending`: a Kubernetes suspend or drain-evacuation claim owns the
//!   row ([`crate::suspend::claim`], [`crate::drain`]). Its finalize writes
//!   `suspended` under its execution fence, and the transient-lifecycle
//!   sweeper, when it is enabled, ages out a claim that never finishes. A
//!   drain whose evacuation suspend fails puts back the lifecycle its claim
//!   read ([`crate::drain::revert_evacuation_suspend_claim`]), which, as for
//!   a failed delete, predates this action's result.
//! * `suspended`: a suspend that completed while the action was in flight,
//!   by the api's own stamp, a Kubernetes finalize, or a node's suspend
//!   projection. The row then describes the suspend; a later wake or restore
//!   brings it back. Two stamps write over it anyway:
//!   - a suspend's own, since a node's projection of the same suspend can
//!     land first;
//!   - a start on a substrate whose start of a `suspended` row claims no
//!     `restoring` (Direct, [`StartSubstrate::LeavesSuspended`]). Its node
//!     cold-boots a `suspended` sandbox as part of the start, with no memory
//!     restore, and the row stays `suspended` until this stamp. A Direct
//!     suspend writes no `suspending` claim, so the stamp cannot tell that
//!     `suspended` from one a concurrent suspend left. Writing over it is
//!     wrong only if that suspend finished after the start's node call and
//!     wrote `suspended` before this stamp, which only the start's
//!     route-receipt write separates. The row then reads `started` over a
//!     suspended sandbox, and the node's `suspended` report does not project
//!     it back: the projection needs a suspend snapshot registered at or
//!     after `lifecycle_changed_at`, and this stamp moves
//!     `lifecycle_changed_at` past that suspend's snapshot. Refusing instead
//!     would leave a sandbox the start booted reading `suspended` whenever
//!     the suspend finished between the start's entry read and its node
//!     call, a longer window, and nothing on Direct corrects that before the
//!     next wake.
//! * `stopping`: no control-plane writer sets it today, so only a row an
//!   older writer left holds it. A node that reports the sandbox stopped
//!   converges it to `stopped` ([`crate::ingest::converge_published_stopped`]),
//!   and the transient-lifecycle sweeper, when it is enabled, ages it out.
//! * `stopped`, for a resume, a pause or a suspend: a stop or kill that
//!   completed while the action was in flight, by the api's own stamp or a
//!   node's report that the sandbox stopped. The row then describes the stop.
//!   None of those actions begins from it: the node's sandbox manager resumes
//!   only a `paused` sandbox, pauses only a `started` one, and suspends only
//!   a `started` or `paused` one (or answers a retry of a suspend it already
//!   finished), and a Kubernetes runtime client recovers no pod for a
//!   `stopped` row. Two stamps write over it anyway:
//!   - a start's, since a start begins from `stopped`. It cannot tell that
//!     `stopped` from one a stop or kill that completed while the start was
//!     in flight left, so it also writes `started` over that stop; the wake
//!     path's completion CAS makes the opposite choice;
//!   - a stop's own, since a concurrent stop or kill, or the node's report of
//!     the same stop, can land first.
//! * `restoring`, for a resume, a pause or a suspend: a restore owner's
//!   claim: a Direct restore lease ([`crate::direct::claim_restore_lease`]),
//!   a Kubernetes user restore ([`crate::restore_lease::claim_user_restore`]),
//!   an evacuation restore claim
//!   ([`crate::restore_lease::claim_evacuation_restore`]) or a create that
//!   restores a snapshot, which inserts its row `restoring`
//!   ([`crate::create::insert_unplaced`]). Its completion
//!   ([`crate::direct::complete_restore_lease`],
//!   [`crate::restore_lease::complete_owned_user_restore`],
//!   [`crate::drain::finalize_evacuation_restore`],
//!   [`crate::create::finalize_created`]) matches a row still `restoring`
//!   (a create's also matches one already `started` at the placement it
//!   reports), so a stamp over it can make that completion miss. None
//!   of those actions leaves a `restoring` of its own. A pause dials through
//!   the non-recovering runtime client, which on Kubernetes refuses a
//!   `restoring` row before it dials. A Kubernetes resume's recovering
//!   runtime client dials only once a joined restore has committed
//!   `started`. On Direct each of them only dials the assigned node. A
//!   Kubernetes suspend writes no stamp. Two stamps write over it anyway, a
//!   start's and a stop's. On Kubernetes a start of a `suspended` row, or
//!   of a `restoring` row with no evacuation marker, restores through the
//!   wake, which claims `restoring`, so its stamp may find its own claim. A
//!   start of a `restoring` row that carries a suspended evacuation's marker
//!   instead takes that restore over without a claim of its own, so the
//!   `restoring` its stamp may find is the evacuation's (a stopped
//!   re-placement's marker refuses the start). No stop leaves a `restoring`
//!   of its own, and neither does a start that claimed no restore (every
//!   Direct start, that takeover, and a Kubernetes start of a row that was
//!   neither `suspended` nor `restoring`). So a stop, or such a start, that
//!   races another owner's restore claim can still write over it and make
//!   that owner's completion miss.
//! * Any value outside the stored vocabulary.
//!
//! A refused stamp matches zero rows. That is the normal outcome when another
//! writer won, not an error: each verb returns whether it wrote, and the api's
//! callers log a refusal without failing the action. Nothing then writes
//! the action's result: the row keeps the owner's state, and if that owner
//! is a delete or a drain evacuation that fails, its compensation puts back
//! the lifecycle it read before this action finished (the `deleting` and
//! `suspending` bullets).

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{SandboxId, SandboxLifecycle};

/// How a start's substrate handles a `suspended` row, as far as its stamp
/// is concerned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartSubstrate {
    /// A substrate whose start of a `suspended` row claims `restoring` first
    /// and whose restore owner normally commits `started` before this stamp
    /// (Kubernetes, through the wake), so a `suspended` the stamp finds is a
    /// newer suspend's.
    ClaimsRestoring,
    /// A substrate whose start of a `suspended` row claims no `restoring`:
    /// its node cold-boots the sandbox and the row stays `suspended` until
    /// this stamp (Direct). See the module docs' `suspended` bullet.
    LeavesSuspended,
}

/// A lifecycle action's post-action stamp.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActionStamp {
    /// `started` after a start.
    Start(StartSubstrate),
    /// `started` after a resume.
    Resume,
    /// `paused` after a pause.
    Pause,
    /// `stopped` after a stop or a kill.
    Stop,
    /// `suspended` after a suspend on a substrate whose suspend does not
    /// commit the lifecycle itself (Direct).
    Suspend,
}

impl ActionStamp {
    /// The lifecycle this stamp writes.
    pub const fn lifecycle(self) -> SandboxLifecycle {
        match self {
            Self::Start(_) | Self::Resume => SandboxLifecycle::Started,
            Self::Pause => SandboxLifecycle::Paused,
            Self::Stop => SandboxLifecycle::Stopped,
            Self::Suspend => SandboxLifecycle::Suspended,
        }
    }

    /// Whether this stamp may replace `lifecycle`: whether the row can hold
    /// it while this stamp's action is in flight with no other intent owning
    /// it. Every lifecycle and every stamp is decided here, so a new variant
    /// of either fails to compile until it is. See the module docs for why
    /// each refused value is refused.
    pub const fn admits(self, lifecycle: SandboxLifecycle) -> bool {
        match lifecycle {
            SandboxLifecycle::Created
            | SandboxLifecycle::Creating
            | SandboxLifecycle::Starting
            | SandboxLifecycle::Started
            | SandboxLifecycle::Pausing
            | SandboxLifecycle::Paused
            | SandboxLifecycle::Resuming
            | SandboxLifecycle::Checkpointing => true,
            SandboxLifecycle::Restoring => match self {
                Self::Start(_) | Self::Stop => true,
                Self::Resume | Self::Pause | Self::Suspend => false,
            },
            SandboxLifecycle::Suspended => match self {
                Self::Start(StartSubstrate::LeavesSuspended) | Self::Suspend => true,
                Self::Start(StartSubstrate::ClaimsRestoring)
                | Self::Resume
                | Self::Pause
                | Self::Stop => false,
            },
            SandboxLifecycle::Stopped => match self {
                Self::Start(_) | Self::Stop => true,
                Self::Resume | Self::Pause | Self::Suspend => false,
            },
            SandboxLifecycle::Suspending
            | SandboxLifecycle::Stopping
            | SandboxLifecycle::Deleting
            | SandboxLifecycle::Quarantined
            | SandboxLifecycle::Failed => false,
        }
    }

    /// The stored values of every lifecycle [`Self::admits`] accepts, to bind
    /// as a guard's `TEXT[]`. Values outside [`SandboxLifecycle`] (the
    /// tombstones and anything unparseable) are never in it.
    pub fn admitted_values(self) -> Vec<&'static str> {
        SandboxLifecycle::all()
            .iter()
            .filter(|lifecycle| self.admits(**lifecycle))
            .map(|lifecycle| lifecycle.as_str())
            .collect()
    }
}

/// **Operation step.** Stamp `stamp`'s lifecycle onto the row, ending a
/// lifecycle action, unless the row has left the action's in-flight
/// lifecycles ([`ActionStamp::admits`]). Returns whether the row was
/// written; `false` means the row is gone or another writer owns it.
pub async fn stamp_action_lifecycle(
    executor: impl sqlx::PgExecutor<'_>,
    id: SandboxId,
    stamp: ActionStamp,
) -> Result<bool, sqlx::Error> {
    let result = sqlx::query(sql!(
        "UPDATE sandboxes
            SET lifecycle = $1
          WHERE id = $2
            AND LOWER(TRIM(lifecycle)) = ANY($3::TEXT[])"
    ))
    .bind(stamp.lifecycle().as_str())
    .bind(id)
    .bind(stamp.admitted_values())
    .execute(executor)
    .await?;
    Ok(result.rows_affected() > 0)
}

/// **Operation step.** Stamp a successful start, `started` and `on_node`,
/// onto the row, dropping a `disk` artifact pin and keeping a `full` one.
/// Same guard as [`stamp_action_lifecycle`] with [`ActionStamp::Start`]; it
/// does not touch the node assignment. Returns whether the row was written.
/// The api's `maybe_mark_started_after_start` documents why the `disk` pin
/// goes.
pub async fn stamp_started_after_start(
    executor: impl sqlx::PgExecutor<'_>,
    id: SandboxId,
    substrate: StartSubstrate,
) -> Result<bool, sqlx::Error> {
    let result = sqlx::query(sql!(
        "UPDATE sandboxes
            SET lifecycle = 'started',
                placement = 'on_node',
                current_local_artifact_id = CASE
                    WHEN current_local_artifact_kind = 'disk' THEN NULL
                    ELSE current_local_artifact_id
                END,
                current_local_artifact_kind = CASE
                    WHEN current_local_artifact_kind = 'disk' THEN NULL
                    ELSE current_local_artifact_kind
                END,
                current_local_artifact_origin = CASE
                    WHEN current_local_artifact_kind = 'disk' THEN NULL
                    ELSE current_local_artifact_origin
                END
          WHERE id = $1
            AND LOWER(TRIM(lifecycle)) = ANY($2::TEXT[])"
    ))
    .bind(id)
    .bind(ActionStamp::Start(substrate).admitted_values())
    .execute(executor)
    .await?;
    Ok(result.rows_affected() > 0)
}

#[cfg(test)]
mod tests;
