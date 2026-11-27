//! Postgres behavior of a fresh restore claim on a sandbox with a capture
//! stamped ahead of the database clock (`K8sStore::begin_restore_lifecycle`,
//! `K8sStore::begin_evacuation_restore_lifecycle`).
//!
//! A sibling module because the parent is already very large. Driven from
//! `k8s_store_postgres_behaviors` against the parent's fixture.

use pplx_space_sandbox_model::{SandboxId, SandboxLifecycle};

use super::*;

/// The columns a claim writes.
#[derive(Debug, PartialEq, Eq, sqlx::FromRow)]
struct ClaimedColumns {
    lifecycle: String,
    lifecycle_changed_at: Option<time::OffsetDateTime>,
    restore_lease_token: Option<Uuid>,
    evacuation_restore_target: Option<String>,
}

async fn claimed_columns(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<ClaimedColumns> {
    Ok(sqlx::query_as(sql!(
        "SELECT lifecycle, lifecycle_changed_at, restore_lease_token,
                evacuation_restore_target
           FROM sandboxes
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?)
}

/// The user claim and the evacuation claim both answer the retryable
/// `Unavailable` while a capture is stamped ahead of the clock, and leave the
/// row as it was. The same claim lands once the clock has passed the capture.
pub(super) async fn a_fresh_claim_is_retryable_while_a_capture_is_ahead_of_the_clock(
    fixture: &PostgresStore,
) -> TestResult {
    let pool = &fixture.pool;
    for evacuation in [false, true] {
        let sandbox_id = SandboxId::new();
        insert_sandbox(pool, sandbox_id, "suspended").await?;
        if evacuation {
            sqlx::query(sql!(
                "UPDATE sandboxes SET evacuation_restore_target = 'started' WHERE id = $1"
            ))
            .bind(sandbox_id)
            .execute(pool)
            .await?;
        }
        let snapshot_id = insert_ready_full_snapshot(
            pool,
            sandbox_id,
            "s3://space-test/snapshots/ahead-of-the-clock",
        )
        .await?;
        sqlx::query(sql!(
            "UPDATE snapshots
                SET \"timestamp\" = clock_timestamp() + INTERVAL '1 hour'
              WHERE id = $1"
        ))
        .bind(snapshot_id)
        .execute(pool)
        .await?;
        let before = claimed_columns(pool, sandbox_id).await?;
        let claim = || async {
            if evacuation {
                fixture
                    .store
                    .begin_evacuation_restore_lifecycle(sandbox_id, SandboxLifecycle::Started)
                    .await
            } else {
                fixture
                    .store
                    .begin_restore_lifecycle(sandbox_id, false)
                    .await
            }
        };

        let refused = claim()
            .await
            .expect_err("a claim must not land below a registered capture");
        assert!(
            matches!(refused, SubstrateError::Unavailable { .. }),
            "evacuation={evacuation}: {refused:?}"
        );
        assert_eq!(
            claimed_columns(pool, sandbox_id).await?,
            before,
            "evacuation={evacuation}"
        );

        sqlx::query(sql!(
            "UPDATE snapshots SET \"timestamp\" = clock_timestamp() WHERE id = $1"
        ))
        .bind(snapshot_id)
        .execute(pool)
        .await?;
        claim().await?;
        assert_eq!(
            claimed_columns(pool, sandbox_id).await?.lifecycle,
            "restoring",
            "evacuation={evacuation}"
        );
    }
    Ok(())
}
