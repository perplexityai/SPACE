//! Writer × stored-lifecycle matrix for the two recoveries execution cleanup
//! runs inline after it retires a row's execution: the markerless `restoring`
//! revert (`recover_markerless_restoring_after_execution_cleanup_in`) and the
//! `started` revert onto a current suspend snapshot
//! (`recover_started_after_suspend_cleanup_in`).
//!
//! Each case builds the shape its recovery takes, retires the execution and
//! clears pod tracking in the cleanup transaction, then stores one lifecycle
//! on the row with its age unchanged before the recovery runs, so the
//! lifecycle is the only variable. The expected table is written out rather
//! than derived from the statements.
//!
//! Both recoveries write `suspended`, so a refusal of a row already
//! `suspended` is not visible in the lifecycle. Every case therefore also
//! compares the other columns the recoveries write (placement and its clock,
//! the node columns, the artifact marker, the restore lease and
//! `command_ready_pending`) with their values just before the recovery ran,
//! and the `started` recovery's own report of whether it moved the row.

use pplx_space_common_core::types::{LocalArtifactId, NodeId, SandboxId};
use pplx_space_placement::test_support::stored_lifecycle_matrix;

use super::*;

/// Columns are the markerless `restoring` recovery and the `started`
/// recovery. `A`: the recovery moves the row to `suspended`; `.`: it refuses
/// and leaves every column it writes as it found them. Both compare the
/// lifecycle exactly.
const EXPECTED: &[&str] = &[
    //              restoring  started
    "created            .         .",
    "creating           .         .",
    "starting           .         .",
    "started            .         A",
    "restoring          A         .",
    "pausing            .         .",
    "paused             .         .",
    "resuming           .         .",
    "suspending         .         .",
    "suspended          .         .",
    "stopping           .         .",
    "stopped            .         .",
    "deleting           .         .",
    "checkpointing      .         .",
    "quarantined        .         .",
    "failed             .         .",
    "deleted            .         .",
    "cleanup_required   .         .",
    "bogus              .         .",
    "_Suspended_        .         .",
];

#[derive(Debug, Clone, Copy)]
enum Recovery {
    MarkerlessRestoring,
    Started,
}

impl Recovery {
    const ALL: [Self; 2] = [Self::MarkerlessRestoring, Self::Started];

    /// Seed the row this recovery takes: a markerless `restoring` row with a
    /// ready full snapshot, or a `started` row whose suspend artifact marker
    /// names a ready snapshot taken after its last lifecycle change.
    async fn seed(self, pool: &PgPool, sandbox_id: SandboxId) -> TestResult {
        match self {
            Self::MarkerlessRestoring => {
                insert_sandbox(pool, sandbox_id, "restoring").await?;
                insert_ready_full_snapshot(
                    pool,
                    sandbox_id,
                    "s3://space-test/matrix-markerless-cleanup?region=us-east-1",
                )
                .await?;
            }
            Self::Started => {
                insert_sandbox(pool, sandbox_id, "started").await?;
                let artifact_id = LocalArtifactId::new();
                sqlx::query(sql!(
                    "UPDATE sandboxes
                     SET current_local_artifact_id = $2,
                         current_local_artifact_kind = 'full',
                         current_local_artifact_origin = 'suspend'
                     WHERE id = $1"
                ))
                .bind(sandbox_id)
                .bind(artifact_id)
                .execute(pool)
                .await?;
                insert_ready_full_snapshot_relative_to_lifecycle(
                    pool,
                    sandbox_id,
                    "s3://space-test/matrix-started-cleanup?region=us-east-1",
                    1,
                    Some(artifact_id),
                )
                .await?;
            }
        }
        Ok(())
    }

    /// Run the recovery; the `started` recovery reports whether it moved the
    /// row, the markerless one reports nothing.
    async fn recover(
        self,
        fixture: &PostgresStore,
        tx: &mut Transaction<'_, Postgres>,
        sandbox_id: SandboxId,
    ) -> TestResult<Option<bool>> {
        Ok(match self {
            Self::MarkerlessRestoring => {
                fixture
                    .store
                    .recover_markerless_restoring_after_execution_cleanup_in(tx, sandbox_id)
                    .await?;
                None
            }
            Self::Started => Some(
                fixture
                    .store
                    .recover_started_after_suspend_cleanup_in(tx, sandbox_id)
                    .await?,
            ),
        })
    }
}

/// The `sandboxes` columns either recovery writes.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
struct RecoveredColumns {
    lifecycle: String,
    placement: String,
    placement_changed_at: Option<OffsetDateTime>,
    active_node_id: Option<NodeId>,
    target_node_id: Option<NodeId>,
    current_local_artifact_id: Option<LocalArtifactId>,
    current_local_artifact_kind: Option<String>,
    current_local_artifact_origin: Option<String>,
    restore_lease_token: Option<Uuid>,
    restore_lease_expires_at: Option<OffsetDateTime>,
    command_ready_pending: bool,
}

async fn recovered_columns<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    sandbox_id: SandboxId,
) -> TestResult<RecoveredColumns> {
    Ok(sqlx::query_as(sql!(
        "SELECT lifecycle, placement, placement_changed_at, active_node_id, target_node_id,
                current_local_artifact_id, current_local_artifact_kind,
                current_local_artifact_origin, restore_lease_token,
                restore_lease_expires_at, command_ready_pending
           FROM sandboxes
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(executor)
    .await?)
}

#[tokio::test(flavor = "current_thread")]
async fn cleanup_recoveries_revert_only_their_stored_lifecycles() -> TestResult {
    let fixture = postgres_store().await?;
    let pool = &fixture.pool;

    let mut mismatches = Vec::new();
    for (stored, cells) in stored_lifecycle_matrix(Recovery::ALL, EXPECTED) {
        for (recovery, recovers) in cells {
            let recovers = recovers.applies();
            let sandbox_id = SandboxId::new();
            recovery.seed(pool, sandbox_id).await?;
            let execution = fixture
                .store
                .ensure_active_execution_row(sandbox_id)
                .await?;

            let mut cleanup = pool.begin().await?;
            assert!(
                K8sStore::lock_sandbox_for_execution_mutation_in(&mut cleanup, sandbox_id).await?
            );
            assert!(
                fixture
                    .store
                    .finish_active_execution_generation_in(
                        &mut cleanup,
                        sandbox_id,
                        execution.generation,
                        "forgotten",
                    )
                    .await?
            );
            fixture
                .store
                .clear_pod_tracking_in(&mut cleanup, sandbox_id, PlacementClock::Bump)
                .await?;
            // The trigger restamps `lifecycle_changed_at` on a lifecycle
            // change; put it back so the snapshot eligibility sees the age
            // the seeded shape had.
            let changed_at: String = sqlx::query_scalar(sql!(
                "SELECT lifecycle_changed_at::text FROM sandboxes WHERE id = $1"
            ))
            .bind(sandbox_id)
            .fetch_one(&mut *cleanup)
            .await?;
            sqlx::query(sql!("UPDATE sandboxes SET lifecycle = $2 WHERE id = $1"))
                .bind(sandbox_id)
                .bind(stored)
                .execute(&mut *cleanup)
                .await?;
            sqlx::query(sql!(
                "UPDATE sandboxes SET lifecycle_changed_at = $2::timestamptz WHERE id = $1"
            ))
            .bind(sandbox_id)
            .bind(changed_at)
            .execute(&mut *cleanup)
            .await?;
            let before = recovered_columns(&mut *cleanup, sandbox_id).await?;
            // Both recoveries clear the clock the cleanup just bumped, so a
            // refusal of a row already `suspended` shows only here.
            assert!(
                before.placement_changed_at.is_some(),
                "{recovery:?} from {stored:?}: the cleanup must leave a placement clock"
            );
            let reported = recovery.recover(&fixture, &mut cleanup, sandbox_id).await?;
            cleanup.commit().await?;

            let after = recovered_columns(pool, sandbox_id).await?;
            let row_matches = if recovers {
                after.lifecycle == "suspended" && after.placement_changed_at.is_none()
            } else {
                after == before
            };
            if reported.is_some_and(|moved| moved != recovers) || !row_matches {
                mismatches.push(format!(
                    "{recovery:?} from {stored:?}: reported {reported:?}, row {before:?} -> {after:?}"
                ));
            }
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
    Ok(())
}
