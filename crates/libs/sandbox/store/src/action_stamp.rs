//! Post-action stamps: best-effort writes of a lifecycle action's
//! user-facing result onto `sandboxes`, made by the api's lifecycle
//! dispatcher after the substrate call succeeds (for example `paused` after
//! a pause, or `started`/`on_node` after a start). They are not repairs made
//! while answering a read.
//!
//! Each stamp writes only over a row whose lifecycle, compared case- and
//! whitespace-insensitively, is in its [`ActionStamp::stamps_from`] set: the
//! lifecycles the row can hold while that action is in flight with no other
//! intent owning it. The api checks no lifecycle at entry for start, resume,
//! pause, stop, kill or suspend, and none of them writes one of its own
//! before its substrate call (a Kubernetes start of a `suspended` row, which
//! claims `restoring` through the wake, is the exception), so the row
//! normally still holds the lifecycle the action began from. The set is every
//! lifecycle an action may begin from, plus the results a concurrent
//! completion of the same action leaves (a node's `started` receipt, a
//! Kubernetes restore owner's `started`). Every other value is refused:
//!
//! * `failed`, `quarantined`, `deleted` and `cleanup_required`: the safety
//!   contract with the transient-lifecycle sweeper. If the sweeper failed the
//!   row (and tore its execution down) while the substrate call was in
//!   flight, the stamp must not resurrect it as live.
//! * `deleting`: the api's delete owns the row. It finalizes the delete or,
//!   if it fails, puts back the lifecycle it read
//!   ([`crate::delete::restore_lifecycle_from_deleting`]).
//! * `suspending`: a Kubernetes suspend or drain-evacuation claim owns the
//!   row ([`crate::suspend::claim`], [`crate::drain`]). Its finalize writes
//!   `suspended` under its execution fence, and the transient-lifecycle
//!   sweeper ages out a claim that never finishes.
//! * `suspended`: a suspend that completed while the action was in flight,
//!   by the api's own stamp, a Kubernetes finalize, or a node's suspend
//!   projection. The row then describes the suspend; a later wake or restore
//!   brings it back. Two stamps keep it: a suspend's own (a node's
//!   projection of the same suspend can land first), and a start that began
//!   on a `suspended` row its substrate restores in place
//!   ([`StartOrigin::SuspendedInPlace`]).
//! * `stopping`: no control-plane writer sets it today, so only a row an
//!   older writer left holds it. A node that reports the sandbox stopped
//!   converges it to `stopped` ([`crate::ingest::converge_published_stopped`]),
//!   and the transient-lifecycle sweeper ages it out.
//! * Any value outside the stored vocabulary.
//!
//! A refused stamp matches zero rows. That is the normal outcome when another
//! writer won, not an error: each verb returns whether it wrote, and the api's
//! callers log a refusal without failing the action.

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{SandboxId, SandboxLifecycle};

/// Every lifecycle a start, resume, pause, stop or kill may find the row in
/// while its substrate call is in flight and no other intent owns the row:
/// each lifecycle the action may begin from, and each result a concurrent
/// completion of it leaves. It omits the lifecycles the module docs list as
/// another writer's.
pub const ACTION_IN_FLIGHT: &[SandboxLifecycle] = &[
    SandboxLifecycle::Created,
    SandboxLifecycle::Creating,
    SandboxLifecycle::Starting,
    SandboxLifecycle::Started,
    SandboxLifecycle::Restoring,
    SandboxLifecycle::Pausing,
    SandboxLifecycle::Paused,
    SandboxLifecycle::Resuming,
    SandboxLifecycle::Stopped,
    SandboxLifecycle::Checkpointing,
];

/// [`ACTION_IN_FLIGHT`] and `suspended`: what a suspend, or a start that
/// restores a `suspended` row in place, may find the row in. For them
/// `suspended` is the action's own state, not another intent's.
pub const SUSPENDED_ACTION_IN_FLIGHT: &[SandboxLifecycle] = &[
    SandboxLifecycle::Created,
    SandboxLifecycle::Creating,
    SandboxLifecycle::Starting,
    SandboxLifecycle::Started,
    SandboxLifecycle::Restoring,
    SandboxLifecycle::Pausing,
    SandboxLifecycle::Paused,
    SandboxLifecycle::Resuming,
    SandboxLifecycle::Suspended,
    SandboxLifecycle::Stopped,
    SandboxLifecycle::Checkpointing,
];

/// Where a start began, as far as its stamp is concerned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartOrigin {
    /// Every start but [`Self::SuspendedInPlace`].
    Other,
    /// A start that read the row `suspended` on a substrate that restores it
    /// on its node and leaves the row `suspended` until this stamp (Direct:
    /// its node restores a suspended sandbox as part of a start). A
    /// Kubernetes start of a `suspended` row claims `restoring` and its owner
    /// writes `started` before this stamp, so it is [`Self::Other`] and a
    /// `suspended` it finds is a newer suspend's.
    SuspendedInPlace,
}

/// A lifecycle action's post-action stamp.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActionStamp {
    /// `started` after a start.
    Start(StartOrigin),
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

    /// The lifecycles this stamp may replace. See the module docs for why
    /// each other value is refused.
    pub const fn stamps_from(self) -> &'static [SandboxLifecycle] {
        match self {
            Self::Start(StartOrigin::Other) | Self::Resume | Self::Pause | Self::Stop => {
                ACTION_IN_FLIGHT
            }
            Self::Start(StartOrigin::SuspendedInPlace) | Self::Suspend => {
                SUSPENDED_ACTION_IN_FLIGHT
            }
        }
    }

    fn stamps_from_values(self) -> Vec<&'static str> {
        self.stamps_from()
            .iter()
            .map(|lifecycle| lifecycle.as_str())
            .collect()
    }
}

/// **Operation step.** Stamp `stamp`'s lifecycle onto the row, ending a
/// lifecycle action, unless the row has left the action's in-flight
/// lifecycles ([`ActionStamp::stamps_from`]). Returns whether the row was
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
    .bind(stamp.stamps_from_values())
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
    origin: StartOrigin,
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
    .bind(ActionStamp::Start(origin).stamps_from_values())
    .execute(executor)
    .await?;
    Ok(result.rows_affected() > 0)
}

#[cfg(test)]
mod tests;
