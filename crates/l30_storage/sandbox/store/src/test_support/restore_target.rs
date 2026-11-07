//! The parity check every reader of the node drain's marker
//! (`sandboxes.evacuation_restore_target`) owes the restore episode target
//! (`sandboxes.restore_episode_target`): each reader that asks whether a
//! restore is the system's, or which lifecycle it lands in, gives the same
//! answer whichever of the two columns holds the value.

use std::fmt::Debug;

use pplx_space_common_core::sql;
use pplx_space_common_core::types::SandboxId;

use crate::sweeper::EvacuationRestoreTarget;

/// Every value both columns admit.
pub const RESTORE_TARGETS: [EvacuationRestoreTarget; 3] = [
    EvacuationRestoreTarget::Started,
    EvacuationRestoreTarget::Paused,
    EvacuationRestoreTarget::Stopped,
];

/// Which column holds a test row's restore target.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TargetColumn {
    /// `evacuation_restore_target`, the v1 drain's marker.
    Marker,
    /// `restore_episode_target`.
    Episode,
}

/// A restore target and the column holding it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HeldTarget {
    pub column: TargetColumn,
    pub target: EvacuationRestoreTarget,
}

impl HeldTarget {
    pub const fn episode(target: EvacuationRestoreTarget) -> Self {
        Self {
            column: TargetColumn::Episode,
            target,
        }
    }

    pub const fn marker(target: EvacuationRestoreTarget) -> Self {
        Self {
            column: TargetColumn::Marker,
            target,
        }
    }
}

/// Write `held` onto the row, and NULL into the other column, or NULL into
/// both for `None`. It leaves the lifecycle alone, so the marker-clearing
/// trigger keeps a marker it writes.
pub async fn hold_restore_target(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    held: Option<HeldTarget>,
) -> Result<(), sqlx::Error> {
    let value = |column| {
        held.filter(|held| held.column == column)
            .map(|held| held.target.as_str())
    };
    let written = sqlx::query(sql!(
        "UPDATE sandboxes
            SET evacuation_restore_target = $2,
                restore_episode_target = $3
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(value(TargetColumn::Marker))
    .bind(value(TargetColumn::Episode))
    .execute(executor)
    .await?;
    assert_eq!(
        written.rows_affected(),
        1,
        "no row {sandbox_id} to hold {held:?}"
    );
    Ok(())
}

/// The row's `restore_episode_target`, raw. `None` for a missing row too.
pub async fn read_restore_episode_target(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
) -> Result<Option<String>, sqlx::Error> {
    Ok(sqlx::query_scalar::<_, Option<String>>(sql!(
        "SELECT restore_episode_target FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_optional(executor)
    .await?
    .flatten())
}

/// Run `site` once per fixture: with each of [`RESTORE_TARGETS`] held by the
/// marker and by the episode target, and once with neither. `site` builds its
/// own rows each time and returns what it observed of the statement under
/// test.
///
/// # Panics
///
/// When a target gives a different outcome held by the episode target than by
/// the marker, or when no target's outcome differs from the control's, which
/// would make the comparison pass without the site reading either column.
pub async fn assert_episode_target_reads_as_marker<O, E>(
    site: &str,
    mut run: impl AsyncFnMut(Option<HeldTarget>) -> Result<O, E>,
) -> Result<(), E>
where
    O: Debug + PartialEq,
{
    let control = run(None).await?;
    let mut read = false;
    for target in RESTORE_TARGETS {
        let marked = run(Some(HeldTarget::marker(target))).await?;
        let episode = run(Some(HeldTarget::episode(target))).await?;
        assert_eq!(
            episode, marked,
            "{site}: a `{target}` episode target reads differently from the marker"
        );
        read |= marked != control;
    }
    assert!(
        read,
        "{site}: no restore target changes the outcome {control:?}, so this \
         proves nothing about the site"
    );
    Ok(())
}
