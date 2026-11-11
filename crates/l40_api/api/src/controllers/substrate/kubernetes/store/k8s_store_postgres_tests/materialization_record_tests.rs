//! `sandbox_k8s_executions.pod_materialized_at`, the set-once record that an
//! execution materialized a pod, through the store writers that record or
//! clear a `pod_uid`. Once a writer has recorded a pod, no supported write
//! path can forget it.

use pplx_space_common_core::types::PodName;

use super::*;
use crate::controllers::substrate::{ObservedPod, RecordedExecution};

async fn materialized_at(
    pool: &PgPool,
    sandbox_id: SandboxId,
    generation: i64,
) -> TestResult<Option<OffsetDateTime>> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT pod_materialized_at
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND generation = $2"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .fetch_one(pool)
    .await?)
}

/// Put a `pod_uid` on the row the way a writer from before the record did:
/// without a stamp.
async fn record_unstamped_pod_uid(
    pool: &PgPool,
    sandbox_id: SandboxId,
    generation: i64,
    pod_uid: &str,
    state: &str,
) -> TestResult {
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET pod_uid = $3, state = $4, pod_materialized_at = NULL
          WHERE sandbox_id = $1 AND generation = $2"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .bind(pod_uid)
    .bind(state)
    .execute(pool)
    .await?;
    Ok(())
}

/// A write naming another generation stamps nothing. The first recorded pod
/// UID stamps the row, and a replacement UID or a cleared one leaves that
/// stamp as it was.
#[tokio::test]
async fn the_first_recorded_pod_stamps_the_execution_once() -> TestResult {
    let fixture = postgres_store().await?;
    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "started").await?;
    let generation = fixture
        .store
        .ensure_active_execution_claim(sandbox_id)
        .await?
        .row
        .generation;
    assert_eq!(
        materialized_at(&fixture.pool, sandbox_id, generation).await?,
        None
    );

    fixture
        .store
        .persist_pod_metadata(sandbox_id, generation + 1, Some("other-pod"), None)
        .await?;
    assert_eq!(
        materialized_at(&fixture.pool, sandbox_id, generation).await?,
        None,
        "a write for another generation stamps nothing"
    );

    fixture
        .store
        .persist_pod_metadata(sandbox_id, generation, Some("first-pod"), None)
        .await?;
    let stamped = materialized_at(&fixture.pool, sandbox_id, generation)
        .await?
        .expect("recording a pod UID stamps the execution");

    for (case, pod_uid, pod_ip) in [
        ("a replacement pod", Some("second-pod"), Some("10.90.0.9")),
        ("a cleared pod", None, None),
    ] {
        fixture
            .store
            .persist_pod_metadata(sandbox_id, generation, pod_uid, pod_ip)
            .await?;
        assert_eq!(
            materialized_at(&fixture.pool, sandbox_id, generation).await?,
            Some(stamped),
            "{case} keeps the first stamp"
        );
    }
    Ok(())
}

/// A row that recorded its pod before the stamp existed gains one from a
/// metadata write that changes nothing else, and that write leaves the row's
/// `state` and `updated_at` as they were. The UID fill that records a pod the
/// row never persisted stamps it too.
#[tokio::test]
async fn an_unchanged_write_stamps_a_row_from_before_the_record() -> TestResult {
    let fixture = postgres_store().await?;
    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "started").await?;
    let generation = fixture
        .store
        .ensure_active_execution_claim(sandbox_id)
        .await?
        .row
        .generation;
    record_unstamped_pod_uid(
        &fixture.pool,
        sandbox_id,
        generation,
        "legacy-pod",
        "created",
    )
    .await?;
    let state_and_updated_at = || async {
        sqlx::query_as::<_, (String, OffsetDateTime)>(sql!(
            "SELECT state, updated_at
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1 AND generation = $2"
        ))
        .bind(sandbox_id)
        .bind(generation)
        .fetch_one(&fixture.pool)
        .await
    };
    let before = state_and_updated_at().await?;

    fixture
        .store
        .persist_pod_metadata(sandbox_id, generation, Some("legacy-pod"), None)
        .await?;
    assert!(
        materialized_at(&fixture.pool, sandbox_id, generation)
            .await?
            .is_some(),
        "the same metadata written again fills the missing stamp"
    );
    assert_eq!(
        state_and_updated_at().await?,
        before,
        "filling the stamp changes nothing else"
    );

    let backfilled = SandboxId::new();
    let pod_name = K8sStore::execution_pod_name(backfilled, 1);
    insert_sandbox(&fixture.pool, backfilled, "failed").await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions (sandbox_id, generation, pod_name, pod_namespace)
         VALUES ($1, 1, $2, 'space')"
    ))
    .bind(backfilled)
    .bind(&pod_name)
    .execute(&fixture.pool)
    .await?;
    let identity = ExecutionIdentity {
        pod_name: PodName::parse(&pod_name)?,
        generation: Some(1),
        activation_attempt_id: None,
        recorded: RecordedExecution::Read {
            pod_uid: None,
            route_activation_id: None,
        },
        pod: ObservedPod::Unobserved,
    };
    assert!(
        fixture
            .store
            .backfill_missing_pod_uid(backfilled, &identity, "observed-pod")
            .await?
    );
    assert!(
        materialized_at(&fixture.pool, backfilled, 1)
            .await?
            .is_some(),
        "backfilling a pod UID stamps the execution"
    );
    Ok(())
}

/// Each writer that clears or replaces a `pod_uid` on an active row stamps
/// the row first, so a pod recorded before the stamp existed isn't forgotten
/// by the write that overwrites it. A clear its guards refuse stamps nothing.
#[tokio::test]
async fn every_clearing_writer_stamps_the_pod_it_clears() -> TestResult {
    let fixture = postgres_store().await?;

    // The metadata write replacing or clearing a pod UID.
    for (case, pod_uid) in [("replacing", Some("replacement-pod")), ("clearing", None)] {
        let rewritten = SandboxId::new();
        insert_sandbox(&fixture.pool, rewritten, "started").await?;
        let generation = fixture
            .store
            .ensure_active_execution_claim(rewritten)
            .await?
            .row
            .generation;
        record_unstamped_pod_uid(&fixture.pool, rewritten, generation, "old-pod", "created")
            .await?;
        fixture
            .store
            .persist_pod_metadata(rewritten, generation, pod_uid, None)
            .await?;
        assert!(
            materialized_at(&fixture.pool, rewritten, generation)
                .await?
                .is_some(),
            "{case} an unstamped pod UID stamps the pod it overwrites"
        );
    }

    // Pod re-materialization.
    let rematerialized = SandboxId::new();
    insert_sandbox(&fixture.pool, rematerialized, "started").await?;
    let generation = fixture
        .store
        .ensure_active_execution_claim(rematerialized)
        .await?
        .row
        .generation;
    record_unstamped_pod_uid(
        &fixture.pool,
        rematerialized,
        generation,
        "old-pod",
        "running",
    )
    .await?;
    assert!(
        fixture
            .store
            .claim_active_execution_pod_materialization(
                rematerialized,
                generation,
                SandboxLifecycle::Started,
            )
            .await?
    );
    assert!(
        materialized_at(&fixture.pool, rematerialized, generation)
            .await?
            .is_some(),
        "re-materialization stamps the pod it clears"
    );

    // The cross-node re-drive's reset, refused for a foreign lease and then
    // applied for its owner.
    let reset = SandboxId::new();
    insert_sandbox(&fixture.pool, reset, "restoring").await?;
    let generation = fixture
        .store
        .ensure_active_execution_claim(reset)
        .await?
        .row
        .generation;
    let owner = Uuid::now_v7();
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET restore_lease_token = $2,
                restore_lease_expires_at = NOW() + INTERVAL '120 seconds'
          WHERE id = $1"
    ))
    .bind(reset)
    .bind(owner)
    .execute(&fixture.pool)
    .await?;
    record_unstamped_pod_uid(&fixture.pool, reset, generation, "pinned-pod", "created").await?;
    assert!(
        !fixture
            .store
            .reset_active_execution_to_creating_if_generation(
                reset,
                generation,
                Some(Uuid::now_v7()),
                None,
            )
            .await?
    );
    assert_eq!(
        materialized_at(&fixture.pool, reset, generation).await?,
        None,
        "a reset its lease refuses stamps nothing"
    );
    assert!(
        fixture
            .store
            .reset_active_execution_to_creating_if_generation(reset, generation, Some(owner), None)
            .await?
    );
    assert!(
        materialized_at(&fixture.pool, reset, generation)
            .await?
            .is_some(),
        "the re-drive's reset stamps the pod it clears"
    );

    // Failed-recovery pod absence.
    let recovered = SandboxId::new();
    insert_sandbox(&fixture.pool, recovered, "started").await?;
    insert_ready_full_snapshot_relative_to_lifecycle(
        &fixture.pool,
        recovered,
        "s3://space-test/materialization-record?region=us-east-1",
        -1,
        None,
    )
    .await?;
    let generation = match fixture.store.claim_started_recovery(recovered).await? {
        StartedRecoveryClaim::CreatedExecution { generation } => generation,
        other => panic!("expected a new execution claim, got {other:?}"),
    };
    fixture
        .store
        .persist_pod_metadata(recovered, generation, Some("failed-pod"), None)
        .await?;
    fixture
        .store
        .complete_started_recovery(
            recovered,
            generation,
            None,
            StartedRecoveryCompletion::Failed,
        )
        .await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions SET pod_materialized_at = NULL
          WHERE sandbox_id = $1 AND generation = $2"
    ))
    .bind(recovered)
    .bind(generation)
    .execute(&fixture.pool)
    .await?;
    assert_eq!(
        fixture
            .store
            .converge_failed_recovery_after_pod_absence_if_generation(
                recovered,
                generation,
                Some("failed-pod"),
                None,
                None,
            )
            .await?,
        FailedRecoveryPodAbsenceOutcome::ConvergedSuspended
    );
    assert!(
        materialized_at(&fixture.pool, recovered, generation)
            .await?
            .is_some(),
        "failed-recovery pod absence stamps the pod it clears"
    );
    Ok(())
}

/// Rolling back to writers that predate the record leaves stamps alone: they
/// clear `pod_uid` without touching `pod_materialized_at`, so a pod a newer
/// writer recorded stays on record.
#[tokio::test]
async fn writers_from_before_the_record_keep_existing_stamps() -> TestResult {
    let fixture = postgres_store().await?;
    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "started").await?;
    let generation = fixture
        .store
        .ensure_active_execution_claim(sandbox_id)
        .await?
        .row
        .generation;
    fixture
        .store
        .persist_pod_metadata(sandbox_id, generation, Some("recorded-pod"), None)
        .await?;
    let stamped = materialized_at(&fixture.pool, sandbox_id, generation).await?;
    assert!(stamped.is_some());

    // The pre-record shape of the re-materialization clear.
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET pod_uid = NULL, pod_ip = NULL, state = 'creating', updated_at = NOW()
          WHERE sandbox_id = $1 AND generation = $2 AND active"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .execute(&fixture.pool)
    .await?;
    assert_eq!(
        materialized_at(&fixture.pool, sandbox_id, generation).await?,
        stamped
    );
    Ok(())
}
