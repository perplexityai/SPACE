//! The local artifact a sandbox publishes when it suspends, and the guard that refuses to delete
//! the current one.

use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublishedLocalArtifact {
    pub id: LocalArtifactId,
    pub kind: ArtifactKind,
}

impl PublishedLocalArtifact {
    pub(crate) fn from_published(
        published: Option<pplx_space_sbx_mgr_client::proto::PublishedArtifact>,
        operation: &str,
        expected_kind: ArtifactKind,
    ) -> Result<Self, SubstrateError> {
        let published = published.ok_or_else(|| SubstrateError::Failed {
            message: format!("blocking {operation} returned no local_artifact_id"),
        })?;
        let id = published
            .local_artifact_id
            .parse()
            .map_err(|err| SubstrateError::Failed {
                message: format!("blocking {operation} returned invalid local_artifact_id: {err}"),
            })?;
        let kind =
            match pplx_space_sbx_mgr_client::proto::ArtifactKind::try_from(published.artifact_kind)
            {
                Ok(pplx_space_sbx_mgr_client::proto::ArtifactKind::Full) => ArtifactKind::Full,
                Ok(pplx_space_sbx_mgr_client::proto::ArtifactKind::Disk) => ArtifactKind::Disk,
                _ => {
                    return Err(SubstrateError::Failed {
                        message: format!("blocking {operation} returned unsupported artifact_kind"),
                    });
                }
            };
        if kind != expected_kind {
            return Err(SubstrateError::Failed {
                message: format!(
                    "blocking {operation} returned {kind} artifact, expected {expected_kind}"
                ),
            });
        }
        Ok(Self { id, kind })
    }

    pub(crate) fn from_suspend_response(
        response: pplx_space_sbx_mgr_client::proto::SuspendResponse,
    ) -> Result<Self, SubstrateError> {
        Self::from_published(response.published_artifact, "suspend", ArtifactKind::Full)
    }

    pub(crate) fn from_committed_suspend_response(
        response: pplx_space_sbx_mgr_client::proto::SuspendResponse,
        sandbox_id: SandboxId,
        substrate: &'static str,
    ) -> Option<Self> {
        match Self::from_suspend_response(response) {
            Ok(published_artifact) => Some(published_artifact),
            Err(err) => {
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    substrate,
                    error = %err,
                    "sandbox suspend completed but returned unusable artifact metadata"
                );
                None
            }
        }
    }
}

/// Call only after locking the sandbox row in the same transaction. Retention
/// takes that lock before changing a snapshot from `ready` to `deleting`, so a
/// later statement observes the committed winner and cannot pin a claimed
/// artifact for restore.
pub(crate) async fn reject_deleting_current_local_artifact(
    tx: &mut Transaction<'_, Postgres>,
    sandbox_id: SandboxId,
    artifact: PublishedLocalArtifact,
) -> Result<(), SubstrateError> {
    let deleting_snapshot = deleting_snapshot_matches_artifact("$1", "$2", "$3");
    let query = format!("SELECT {deleting_snapshot}");
    let deleting = sqlx::query_scalar::<_, bool>(sqlx::AssertSqlSafe(sql!(query.as_str())))
        .bind(sandbox_id.as_uuid())
        .bind(artifact.id.as_uuid())
        .bind(artifact.kind.as_str())
        .fetch_one(&mut **tx)
        .await
        .db_context("failed to check current artifact retention claim")?;
    if deleting {
        return Err(SubstrateError::Unavailable {
            message: format!(
                "local artifact {} is already claimed for snapshot deletion",
                artifact.id
            ),
        });
    }
    Ok(())
}
