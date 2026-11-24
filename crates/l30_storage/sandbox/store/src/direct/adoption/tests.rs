//! The adoption reads against Postgres, with the Direct route identity and
//! provenance seeded: the probe's guard, and the lock verb's lock, scope and
//! no-wait refusal.

use pplx_space_accounts::{NodeId, OrgId, UserId};
use pplx_space_db_test_fixtures::{
    SEED_ORG_ID, SEED_USER_ID, TestDbResult, insert_suspended_sandbox, pg_test_db,
    pg_test_db_with_max_connections,
};
use pplx_space_sandbox_model::{SandboxId, SandboxLifecycle};
use pplx_space_sql_locator::sql;
use sqlx::PgPool;
use uuid::Uuid;

use super::{LiveAdoptionGenerations, lock_live_adoption_candidate, route_identity_trails_record};

/// A suspended Direct row with no active node, its route identity on
/// `node_id` at `route_generation` and its provenance at `last_generation`.
async fn unplaced(
    pool: &PgPool,
    node_id: NodeId,
    route_generation: i64,
    last_generation: i64,
) -> TestDbResult<SandboxId> {
    let sandbox_id = insert_suspended_sandbox(
        pool,
        OrgId::from_uuid(SEED_ORG_ID),
        UserId::from_uuid(SEED_USER_ID),
    )
    .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET placement = 'unplaced', active_node_id = NULL WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_direct_route_identities (
             sandbox_id, route_generation, current_node_id, substrate_kind,
             execution_generation, activation_attempt_id
         )
         VALUES ($1, $3, $2, 'space', $3, $4)"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .bind(route_generation)
    .bind(Uuid::now_v7())
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_direct_route_provenance (
             sandbox_id, last_route_generation, last_execution_generation
         )
         VALUES ($1, $2, $2)"
    ))
    .bind(sandbox_id)
    .bind(last_generation)
    .execute(pool)
    .await?;
    Ok(sandbox_id)
}

/// Moves the row to `restoring` under a lease expiring `expires_in_secs`
/// from now (negative for an expired one).
async fn lease(pool: &PgPool, sandbox_id: SandboxId, expires_in_secs: i64) -> TestDbResult<()> {
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET lifecycle = 'restoring',
                restore_lease_token = $2,
                restore_lease_expires_at = NOW() + make_interval(secs => $3)
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(Uuid::now_v7())
    .bind(expires_in_secs as f64)
    .execute(pool)
    .await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn route_identity_trails_record_only_behind_a_reserved_generation() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let node_id = NodeId::new();
    let sandbox_id = unplaced(pool, node_id, 2, 3).await?;
    let suspended = SandboxLifecycle::Suspended;

    assert!(route_identity_trails_record(pool, sandbox_id, node_id, 3, suspended).await?);
    for (node, generation, lifecycle, case) in [
        (
            node_id,
            2,
            suspended,
            "the route is at the record's generation",
        ),
        (
            node_id,
            4,
            suspended,
            "provenance never reserved the generation",
        ),
        (NodeId::new(), 3, suspended, "the route is on another node"),
        (
            node_id,
            3,
            SandboxLifecycle::Started,
            "the row left the lifecycle",
        ),
    ] {
        assert!(
            !route_identity_trails_record(pool, sandbox_id, node, generation, lifecycle).await?,
            "{case}"
        );
    }

    // A row holding a restore-lease token is adopted only at its receipt.
    lease(pool, sandbox_id, -60).await?;
    assert!(
        !route_identity_trails_record(pool, sandbox_id, node_id, 3, SandboxLifecycle::Restoring)
            .await?
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn lock_live_adoption_candidate_locks_only_the_sandboxes_row() -> TestDbResult<()> {
    let db = pg_test_db_with_max_connections(3).await?;
    let pool = &db.pool;
    let node_id = NodeId::new();
    let sandbox_id = unplaced(pool, node_id, 2, 3).await?;

    let mut transaction = pool.begin().await?;
    let generations = lock_live_adoption_candidate(
        &mut transaction,
        sandbox_id,
        SandboxLifecycle::Suspended,
        node_id,
        3,
    )
    .await?;
    assert_eq!(
        generations,
        Some(LiveAdoptionGenerations {
            execution_generation: 2,
            last_execution_generation: 3,
        })
    );

    // Until the transaction ends, the row is locked and the route rows are not.
    let mut other = pool.acquire().await?;
    let row_lock = sqlx::query(sql!(
        "SELECT 1 FROM sandboxes WHERE id = $1 FOR NO KEY UPDATE NOWAIT"
    ))
    .bind(sandbox_id)
    .execute(&mut *other)
    .await
    .expect_err("the candidate row is locked");
    assert_eq!(
        row_lock
            .as_database_error()
            .and_then(|error| error.code())
            .as_deref(),
        Some("55P03")
    );
    // The lock is exactly `FOR NO KEY UPDATE`: a route insert's foreign-key
    // check (`FOR KEY SHARE`) is not blocked, and `FOR SHARE` is.
    sqlx::query(sql!(
        "SELECT 1 FROM sandboxes WHERE id = $1 FOR KEY SHARE NOWAIT"
    ))
    .bind(sandbox_id)
    .execute(&mut *other)
    .await?;
    let share_lock = sqlx::query(sql!(
        "SELECT 1 FROM sandboxes WHERE id = $1 FOR SHARE NOWAIT"
    ))
    .bind(sandbox_id)
    .execute(&mut *other)
    .await
    .expect_err("the candidate row is locked against FOR SHARE");
    assert_eq!(
        share_lock
            .as_database_error()
            .and_then(|error| error.code())
            .as_deref(),
        Some("55P03")
    );
    for statement in [
        sql!(
            "SELECT 1 FROM sandbox_direct_route_identities WHERE sandbox_id = $1 FOR UPDATE NOWAIT"
        ),
        sql!(
            "SELECT 1 FROM sandbox_direct_route_provenance WHERE sandbox_id = $1 FOR UPDATE NOWAIT"
        ),
    ] {
        sqlx::query(statement)
            .bind(sandbox_id)
            .execute(&mut *other)
            .await?;
    }
    transaction.commit().await?;
    sqlx::query(sql!(
        "SELECT 1 FROM sandboxes WHERE id = $1 FOR NO KEY UPDATE NOWAIT"
    ))
    .bind(sandbox_id)
    .execute(&mut *other)
    .await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn lock_live_adoption_candidate_refuses_without_waiting() -> TestDbResult<()> {
    let db = pg_test_db_with_max_connections(3).await?;
    let pool = &db.pool;
    let node_id = NodeId::new();
    let leased = unplaced(pool, node_id, 3, 3).await?;
    lease(pool, leased, 60).await?;
    let behind = unplaced(pool, node_id, 4, 4).await?;

    // Another transaction holds both rows. A row that fails the guard in the
    // statement's snapshot returns no row instead of waiting for it.
    let mut holder = pool.begin().await?;
    sqlx::query(sql!(
        "SELECT 1 FROM sandboxes WHERE id = ANY($1) FOR UPDATE"
    ))
    .bind(vec![leased, behind])
    .execute(&mut *holder)
    .await?;

    let mut transaction = pool.begin().await?;
    sqlx::query(sql!("SET LOCAL lock_timeout = '5s'"))
        .execute(&mut *transaction)
        .await?;
    for (sandbox_id, lifecycle, case) in [
        (leased, SandboxLifecycle::Restoring, "a live restore lease"),
        (
            behind,
            SandboxLifecycle::Suspended,
            "a route newer than the record",
        ),
        (behind, SandboxLifecycle::Started, "another lifecycle"),
    ] {
        assert_eq!(
            lock_live_adoption_candidate(&mut transaction, sandbox_id, lifecycle, node_id, 3)
                .await?,
            None,
            "{case}"
        );
    }
    transaction.rollback().await?;
    holder.rollback().await?;
    Ok(())
}
