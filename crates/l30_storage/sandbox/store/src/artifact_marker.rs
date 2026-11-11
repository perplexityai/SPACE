//! Writes of the row's current local artifact marker
//! (`current_local_artifact_id`, `_kind` and `_origin`): which immutable
//! local artifact a node published for the sandbox, and the operation that
//! published it.
//!
//! The caller owns the transaction and the checks around the write: it
//! takes the sandbox row lock (`SELECT ... FOR UPDATE`) first and refuses an
//! artifact already claimed for snapshot deletion before calling here.

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{ArtifactKind, ArtifactOrigin, LocalArtifactId, SandboxId};

/// **Observed projection.** Set the current local artifact marker to the
/// artifact a node reported publishing, with `origin` as the operation that
/// published it, or clear all three columns when `artifact` is `None`.
///
/// No guard beyond the id: no lifecycle predicate of its own. It relies on
/// the caller's row lock and deleting-snapshot check in the same
/// transaction.
pub async fn set_current_local_artifact(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    artifact: Option<(LocalArtifactId, ArtifactKind)>,
    origin: ArtifactOrigin,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET current_local_artifact_id = $2,
                current_local_artifact_kind = $3,
                current_local_artifact_origin = CASE WHEN $2 IS NULL THEN NULL ELSE $4 END
          WHERE id = $1"
    ))
    .bind(sandbox_id.as_uuid())
    .bind(artifact.map(|(id, _)| id.as_uuid()))
    .bind(artifact.map(|(_, kind)| kind.as_str()))
    .bind(origin.as_str())
    .execute(executor)
    .await?;
    Ok(())
}
