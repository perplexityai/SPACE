//! Each sweep's counts and the rows it leaves: other nodes' and other facets'
//! rows, rows outside the window, rows too young, rows whose node holds a
//! fresh `quarantine_recovery` observation, and a window with no candidate.

use pplx_space_accounts::NodeId;
use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, try_pg_test_db};
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use uuid::Uuid;

use super::{delete_orphaned, delete_stale_suspended};

const HOUR_SECS: i64 = 60 * 60;

/// The sandbox facet refs both sweeps delete on the observed row's node.
fn sandbox_refs(sandbox_id: SandboxId) -> [(&'static str, String); 3] {
    [
        ("network-policy", format!("sandbox:{sandbox_id}")),
        ("sandbox-inventory", sandbox_id.to_string()),
        ("sandbox-desired-state", sandbox_id.to_string()),
    ]
}

async fn insert_node(pool: &PgPool) -> TestResult<NodeId> {
    let node_id = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by)
         VALUES ($1, '127.0.0.1', 'worker', 'space')"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;
    Ok(node_id)
}

async fn insert_sandbox(
    pool: &PgPool,
    sandbox_id: SandboxId,
    lifecycle: &str,
    age_minutes: i32,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, managed_by, lifecycle,
             network_policy_generation, created_at, updated_at
         )
         VALUES (
             $1, $2::uuid, $3::uuid, 'space', $4,
             1, NOW() - make_interval(mins => $5::INTEGER),
             NOW() - make_interval(mins => $5::INTEGER)
         )"
    ))
    .bind(sandbox_id)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .bind(lifecycle)
    .bind(age_minutes)
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_observed(
    pool: &PgPool,
    node_id: NodeId,
    sandbox_id: SandboxId,
    age_minutes: i32,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO sandbox_observed_states (
             sandbox_id, node_id, status, observed_at, updated_at
         )
         VALUES (
             $1, $2, 'suspended',
             NOW() - make_interval(mins => $3::INTEGER),
             NOW() - make_interval(mins => $3::INTEGER)
         )"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .bind(age_minutes)
    .execute(pool)
    .await?;
    Ok(())
}

/// An observed-status row for `(node_id, facet, element)` with one
/// `ApplyFailed` condition, observed `age_minutes` ago with `payload`.
async fn insert_facet(
    pool: &PgPool,
    node_id: NodeId,
    facet: &str,
    element: &str,
    payload: Option<&str>,
    age_minutes: i32,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO node_facet_observed_status (
             node_id, facet, element, observed_payload, observed_at, updated_at
         )
         VALUES (
             $1, $2, $3, $4::JSONB,
             NOW() - make_interval(mins => $5::INTEGER),
             NOW() - make_interval(mins => $5::INTEGER)
         )"
    ))
    .bind(node_id)
    .bind(facet)
    .bind(element)
    .bind(payload)
    .bind(age_minutes)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_facet_conditions (
             node_id, facet, element, condition_type, condition_status,
             reason, message, last_transition_at, updated_at
         )
         VALUES ($1, $2, $3, 'ApplyFailed', 'True', 'ReconcileFailed', 'test', NOW(), NOW())"
    ))
    .bind(node_id)
    .bind(facet)
    .bind(element)
    .execute(pool)
    .await?;
    Ok(())
}

/// The three sandbox facet rows on `node_id`, each with a condition, aged
/// past every grace here.
async fn insert_sandbox_facets(
    pool: &PgPool,
    node_id: NodeId,
    sandbox_id: SandboxId,
) -> TestResult {
    for (facet, element) in sandbox_refs(sandbox_id) {
        insert_facet(pool, node_id, facet, &element, None, 120).await?;
    }
    Ok(())
}

async fn insert_quarantine_recovery(
    pool: &PgPool,
    node_id: NodeId,
    sandbox_id: SandboxId,
    age_minutes: i32,
) -> TestResult {
    insert_facet(
        pool,
        node_id,
        "sandbox-desired-state",
        &sandbox_id.to_string(),
        Some(r#"{"quarantine_recovery": {"schema_version": 1}}"#),
        age_minutes,
    )
    .await
}

async fn observed_exists(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<bool> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT EXISTS (SELECT 1 FROM sandbox_observed_states WHERE sandbox_id = $1)"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?)
}

/// `(status rows, condition rows)` for `(node_id, facet, element)`.
async fn facet_rows(
    pool: &PgPool,
    node_id: NodeId,
    facet: &str,
    element: &str,
) -> TestResult<(i64, i64)> {
    Ok(sqlx::query_as(sql!(
        "SELECT
             (SELECT COUNT(*) FROM node_facet_observed_status
               WHERE node_id = $1 AND facet = $2 AND element = $3),
             (SELECT COUNT(*) FROM node_facet_conditions
               WHERE node_id = $1 AND facet = $2 AND element = $3)"
    ))
    .bind(node_id)
    .bind(facet)
    .bind(element)
    .fetch_one(pool)
    .await?)
}

async fn sandbox_facets_left(
    pool: &PgPool,
    node_id: NodeId,
    sandbox_id: SandboxId,
) -> TestResult<(i64, i64)> {
    let mut left = (0, 0);
    for (facet, element) in sandbox_refs(sandbox_id) {
        let (status, conditions) = facet_rows(pool, node_id, facet, &element).await?;
        left.0 += status;
        left.1 += conditions;
    }
    Ok(left)
}

#[tokio::test(flavor = "current_thread")]
async fn delete_stale_suspended_counts_and_keeps_the_losing_rows() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = insert_node(pool).await?;
    let other_node = insert_node(pool).await?;

    // Created first, so the window that starts at its id leaves it out.
    let outside = SandboxId::new();
    let stale = SandboxId::new();
    let young = SandboxId::new();
    let started = SandboxId::new();
    let fresh_recovery = SandboxId::new();
    let expired_recovery = SandboxId::new();
    let above = SandboxId::new();
    for (sandbox_id, lifecycle, age_minutes) in [
        (outside, "suspended", 120),
        (stale, "suspended", 120),
        (young, "suspended", 5),
        (started, "started", 120),
        (fresh_recovery, "suspended", 120),
        (expired_recovery, "suspended", 120),
        (above, "suspended", 120),
    ] {
        insert_sandbox(pool, sandbox_id, lifecycle, age_minutes).await?;
        insert_observed(pool, node, sandbox_id, 120).await?;
    }
    for sandbox_id in [outside, stale, young, started, above] {
        insert_sandbox_facets(pool, node, sandbox_id).await?;
    }
    // Older than the stale age, inside the 3 h recovery grace: only the grace keeps it.
    insert_quarantine_recovery(pool, node, fresh_recovery, 120).await?;
    insert_quarantine_recovery(pool, node, expired_recovery, 240).await?;
    // Another node's and another facet's rows for the stale sandbox.
    insert_sandbox_facets(pool, other_node, stale).await?;
    insert_facet(
        pool,
        node,
        "template-inventory",
        &stale.to_string(),
        None,
        120,
    )
    .await?;

    // Inclusive: expired_recovery is deleted; above, created after it, is outside.
    let to = expired_recovery.as_uuid();
    let counts =
        delete_stale_suspended(pool, HOUR_SECS, 10, 3 * HOUR_SECS, outside.as_uuid(), to).await?;

    // `stale`'s three facet rows and `expired_recovery`'s one, each with its
    // condition.
    assert_eq!(counts, (2, 4, 4), "observed, status, conditions");
    assert!(!observed_exists(pool, stale).await?);
    assert_eq!(sandbox_facets_left(pool, node, stale).await?, (0, 0));
    assert!(!observed_exists(pool, expired_recovery).await?);
    assert_eq!(
        sandbox_facets_left(pool, node, expired_recovery).await?,
        (0, 0)
    );

    for (sandbox_id, why) in [
        (outside, "outside the window"),
        (young, "suspended too recently"),
        (started, "not suspended"),
        (above, "after the window"),
    ] {
        assert!(observed_exists(pool, sandbox_id).await?, "{why}");
        assert_eq!(
            sandbox_facets_left(pool, node, sandbox_id).await?,
            (3, 3),
            "{why}"
        );
    }
    assert!(
        observed_exists(pool, fresh_recovery).await?,
        "a fresh quarantine_recovery observation keeps the row"
    );
    assert_eq!(
        sandbox_facets_left(pool, node, fresh_recovery).await?,
        (1, 1)
    );
    assert_eq!(
        sandbox_facets_left(pool, other_node, stale).await?,
        (3, 3),
        "another node's rows survive"
    );
    assert_eq!(
        facet_rows(pool, node, "template-inventory", &stale.to_string()).await?,
        (1, 1),
        "another facet's row survives"
    );

    assert_eq!(
        delete_stale_suspended(pool, HOUR_SECS, 10, 3 * HOUR_SECS, outside.as_uuid(), to).await?,
        (0, 0, 0),
        "a window with no candidate deletes nothing"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn delete_stale_suspended_stops_at_its_limit() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = insert_node(pool).await?;
    let ids = [SandboxId::new(), SandboxId::new(), SandboxId::new()];
    for sandbox_id in ids {
        insert_sandbox(pool, sandbox_id, "suspended", 120).await?;
        insert_observed(pool, node, sandbox_id, 120).await?;
    }

    let (from, to) = (Uuid::nil(), Uuid::max());
    assert_eq!(
        delete_stale_suspended(pool, HOUR_SECS, 2, HOUR_SECS, from, to).await?,
        (2, 0, 0)
    );
    let mut left = 0;
    for sandbox_id in ids {
        left += i32::from(observed_exists(pool, sandbox_id).await?);
    }
    assert_eq!(left, 1);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn delete_orphaned_counts_and_keeps_the_losing_rows() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = insert_node(pool).await?;
    let other_node = insert_node(pool).await?;

    // Created first, so the window that starts at its id leaves it out.
    let outside = SandboxId::new();
    let orphan = SandboxId::new();
    let young = SandboxId::new();
    let has_row = SandboxId::new();
    let fresh_recovery = SandboxId::new();
    let last_orphan = SandboxId::new();
    let above = SandboxId::new();
    for (sandbox_id, age_minutes) in [
        (outside, 120),
        (orphan, 120),
        (young, 5),
        (has_row, 120),
        (fresh_recovery, 120),
        (last_orphan, 120),
        (above, 120),
    ] {
        insert_observed(pool, node, sandbox_id, age_minutes).await?;
    }
    insert_sandbox(pool, has_row, "suspended", 120).await?;
    for sandbox_id in [outside, orphan, young, has_row] {
        insert_sandbox_facets(pool, node, sandbox_id).await?;
    }
    insert_quarantine_recovery(pool, node, fresh_recovery, 0).await?;
    insert_sandbox_facets(pool, other_node, orphan).await?;
    insert_facet(
        pool,
        node,
        "template-inventory",
        &orphan.to_string(),
        None,
        120,
    )
    .await?;

    // Inclusive: last_orphan is deleted; above, created after it, is outside.
    let to = last_orphan.as_uuid();
    let counts = delete_orphaned(pool, HOUR_SECS, 10, outside.as_uuid(), to).await?;

    assert_eq!(counts, (2, 3, 3), "observed, status, conditions");
    assert!(!observed_exists(pool, orphan).await?);
    assert_eq!(sandbox_facets_left(pool, node, orphan).await?, (0, 0));
    assert!(!observed_exists(pool, last_orphan).await?);

    for (sandbox_id, why) in [
        (outside, "outside the window"),
        (young, "updated too recently"),
        (has_row, "its sandbox exists"),
    ] {
        assert!(observed_exists(pool, sandbox_id).await?, "{why}");
        assert_eq!(
            sandbox_facets_left(pool, node, sandbox_id).await?,
            (3, 3),
            "{why}"
        );
    }
    assert!(
        observed_exists(pool, fresh_recovery).await?,
        "a fresh quarantine_recovery observation keeps the row"
    );
    assert!(observed_exists(pool, above).await?, "after the window");
    assert_eq!(
        sandbox_facets_left(pool, other_node, orphan).await?,
        (3, 3),
        "another node's rows survive"
    );
    assert_eq!(
        facet_rows(pool, node, "template-inventory", &orphan.to_string()).await?,
        (1, 1),
        "another facet's row survives"
    );

    assert_eq!(
        delete_orphaned(pool, HOUR_SECS, 10, outside.as_uuid(), to).await?,
        (0, 0, 0),
        "a window with no candidate deletes nothing"
    );
    Ok(())
}
