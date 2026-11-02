//! `sandbox_k8s_executions.pod_create_intent_tracked` and
//! `pod_create_requested_at`: rows this store inserts are tracked, every write
//! that precedes a pod create stamps the request and hands back the
//! [`PodCreateIntent`] the create takes, and nothing else stamps. Node drain's
//! pod-less give-up reads a tracked, unstamped row as proof that no pod was
//! ever created for it.

use super::*;

#[derive(Debug, PartialEq, Eq, sqlx::FromRow)]
struct IntentRow {
    tracked: bool,
    requested_at: Option<OffsetDateTime>,
    updated_at: OffsetDateTime,
}

async fn intent(pool: &PgPool, sandbox_id: SandboxId, generation: i64) -> TestResult<IntentRow> {
    Ok(sqlx::query_as(sql!(
        "SELECT pod_create_intent_tracked AS tracked,
                pod_create_requested_at AS requested_at,
                updated_at
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND generation = $2"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .fetch_one(pool)
    .await?)
}

/// A claim is tracked and unstamped until something is about to create its
/// pod; the materialization claim stamps it, once, and names its generation.
#[tokio::test]
async fn the_materialization_claim_stamps_the_intent_once() -> TestResult {
    let fixture = postgres_store().await?;
    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "started").await?;
    let generation = fixture
        .store
        .ensure_active_execution_claim(sandbox_id)
        .await?
        .row
        .generation;
    let fresh = intent(&fixture.pool, sandbox_id, generation).await?;
    assert!(fresh.tracked, "a row this store inserts is tracked");
    assert_eq!(fresh.requested_at, None);

    // Another generation is claimed for nothing and stamps nothing.
    assert_eq!(
        fixture
            .store
            .claim_active_execution_pod_materialization(
                sandbox_id,
                generation + 1,
                SandboxLifecycle::Started,
            )
            .await?,
        None
    );
    assert_eq!(
        intent(&fixture.pool, sandbox_id, generation)
            .await?
            .requested_at,
        None
    );

    let token = fixture
        .store
        .claim_active_execution_pod_materialization(
            sandbox_id,
            generation,
            SandboxLifecycle::Started,
        )
        .await?
        .expect("the active generation claims materialization");
    assert_eq!(
        (token.sandbox_id(), token.generation()),
        (sandbox_id, generation)
    );
    let first = intent(&fixture.pool, sandbox_id, generation)
        .await?
        .requested_at
        .expect("the claim stamps the intent");

    assert!(
        fixture
            .store
            .claim_active_execution_pod_materialization(
                sandbox_id,
                generation,
                SandboxLifecycle::Started,
            )
            .await?
            .is_some()
    );
    assert_eq!(
        intent(&fixture.pool, sandbox_id, generation)
            .await?
            .requested_at,
        Some(first),
        "a re-materialization keeps the first stamp"
    );
    Ok(())
}

/// The cross-node re-drive's reset stamps the intent only when it applies.
#[tokio::test]
async fn the_redrive_reset_stamps_the_intent_only_for_its_owner() -> TestResult {
    let fixture = postgres_store().await?;
    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "restoring").await?;
    let generation = fixture
        .store
        .ensure_active_execution_claim(sandbox_id)
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
    .bind(sandbox_id)
    .bind(owner)
    .execute(&fixture.pool)
    .await?;

    assert_eq!(
        fixture
            .store
            .reset_active_execution_to_creating_if_generation(
                sandbox_id,
                generation,
                Some(Uuid::now_v7()),
                None,
            )
            .await?,
        None
    );
    assert_eq!(
        intent(&fixture.pool, sandbox_id, generation)
            .await?
            .requested_at,
        None,
        "a reset its lease refuses stamps nothing"
    );

    let token = fixture
        .store
        .reset_active_execution_to_creating_if_generation(sandbox_id, generation, Some(owner), None)
        .await?
        .expect("the owner's reset applies");
    assert_eq!(
        (token.sandbox_id(), token.generation()),
        (sandbox_id, generation)
    );
    assert!(
        intent(&fixture.pool, sandbox_id, generation)
            .await?
            .requested_at
            .is_some()
    );
    Ok(())
}

/// The cold create's own stamp: only the active generation, and without
/// moving `updated_at`, which the stale-create and give-up windows read.
#[tokio::test]
async fn the_cold_create_stamp_needs_the_active_generation() -> TestResult {
    let fixture = postgres_store().await?;
    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "creating").await?;
    let generation = fixture
        .store
        .ensure_active_execution_claim(sandbox_id)
        .await?
        .row
        .generation;
    let before = intent(&fixture.pool, sandbox_id, generation).await?;

    assert_eq!(
        fixture
            .store
            .record_pod_create_intent(sandbox_id, generation + 1)
            .await?,
        None
    );
    let token = fixture
        .store
        .record_pod_create_intent(sandbox_id, generation)
        .await?
        .expect("the active generation records its intent");
    assert_eq!(
        (token.sandbox_id(), token.generation()),
        (sandbox_id, generation)
    );
    let after = intent(&fixture.pool, sandbox_id, generation).await?;
    assert!(after.requested_at.is_some());
    assert_eq!(after.updated_at, before.updated_at);

    // A retired claim can't be stamped, so its pod is never created.
    let retired = SandboxId::new();
    insert_sandbox(&fixture.pool, retired, "creating").await?;
    let retired_claim = fixture.store.ensure_active_execution_claim(retired).await?;
    assert!(
        fixture
            .store
            .abandon_unmaterialized_create_claim(retired, &retired_claim)
            .await?
    );
    assert_eq!(
        fixture
            .store
            .record_pod_create_intent(retired, retired_claim.row.generation)
            .await?,
        None
    );
    assert_eq!(
        intent(&fixture.pool, retired, retired_claim.row.generation)
            .await?
            .requested_at,
        None
    );
    Ok(())
}

/// The livelock's shape: a started recovery's claim inserts its generation
/// tracked, and nothing stamps it until the materialization claim right
/// before the pod create. A recovery that stalls before then is provably
/// pod-less.
#[tokio::test]
async fn a_started_recovery_claim_is_tracked_and_unstamped() -> TestResult {
    let fixture = postgres_store().await?;
    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "started").await?;
    let generation = match fixture.store.claim_started_recovery(sandbox_id).await? {
        StartedRecoveryClaim::CreatedExecution { generation } => generation,
        other => panic!("expected a new execution claim, got {other:?}"),
    };
    let row = intent(&fixture.pool, sandbox_id, generation).await?;
    assert!(row.tracked);
    assert_eq!(row.requested_at, None);
    Ok(())
}

/// The generation-1 row reserved for a warm-pool slot's pod name is
/// untracked: that pod was created under the name with no stamp.
#[tokio::test]
async fn the_pool_reserved_generation_is_untracked() -> TestResult {
    let fixture = postgres_store().await?;
    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "creating").await?;
    fixture
        .store
        .retire_pool_reserved_generation(sandbox_id)
        .await?;
    let row = intent(&fixture.pool, sandbox_id, 1).await?;
    assert!(!row.tracked);
    assert_eq!(row.requested_at, None);
    Ok(())
}
