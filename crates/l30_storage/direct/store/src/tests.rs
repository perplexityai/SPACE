//! Route-identity and provenance verbs against Postgres.

use pplx_space_accounts::{NodeId, OrgId, UserId};
use pplx_space_db_test_fixtures::{
    SEED_ORG_ID, SEED_USER_ID, TestDbResult, insert_suspended_sandbox, pg_test_db,
};
use pplx_space_node_model::NodeManagedBy;
use pplx_space_sandbox_model::{
    ExecutionGeneration, RouteGeneration, SandboxActivationAttemptId, SandboxId,
    SandboxRouteIdentity,
};
use pplx_space_sql_locator::sql;
use sqlx::PgPool;
use uuid::Uuid;

use crate::route_identities::{self, RouteIdentityRow};
use crate::route_provenance::{self, ReservedGenerations};

async fn sandbox(pool: &PgPool) -> TestDbResult<SandboxId> {
    insert_suspended_sandbox(
        pool,
        OrgId::from_uuid(SEED_ORG_ID),
        UserId::from_uuid(SEED_USER_ID),
    )
    .await
}

/// A Direct route identity; `attempt` orders activation attempts as UUIDs.
fn identity(
    sandbox_id: SandboxId,
    route_generation: u64,
    node_id: NodeId,
    execution_generation: i64,
    attempt: u128,
) -> SandboxRouteIdentity {
    SandboxRouteIdentity::try_new(
        sandbox_id,
        RouteGeneration::new(route_generation).expect("positive route generation"),
        node_id,
        NodeManagedBy::Space,
        ExecutionGeneration::new(execution_generation).expect("positive execution generation"),
        SandboxActivationAttemptId::from_uuid(Uuid::from_u128(attempt)),
        None,
    )
    .expect("a Direct identity has no pod UID")
}

fn row(identity: &SandboxRouteIdentity) -> RouteIdentityRow {
    RouteIdentityRow {
        route_generation: i64::try_from(identity.route_generation().get())
            .expect("test generation fits i64"),
        current_node_id: identity.current_node_id().as_uuid(),
        substrate_kind: identity.substrate_kind().as_str().to_string(),
        execution_generation: identity.execution_generation().get(),
        activation_attempt_id: identity.activation_attempt_id().as_uuid(),
        pod_uid: None,
    }
}

/// Runs [`route_identities::accept_receipt`] in a transaction of its own and
/// commits it.
async fn accept(pool: &PgPool, identity: &SandboxRouteIdentity) -> TestDbResult<bool> {
    let mut tx = pool.begin().await?;
    let accepted = route_identities::accept_receipt(&mut *tx, identity).await?;
    tx.commit().await?;
    Ok(accepted)
}

/// Runs [`route_provenance::retain_generations`] in a transaction of its own
/// and commits it.
async fn retain(
    pool: &PgPool,
    sandbox_id: SandboxId,
    route_generation: RouteGeneration,
    execution_generation: ExecutionGeneration,
) -> TestDbResult<()> {
    let mut tx = pool.begin().await?;
    route_provenance::retain_generations(
        &mut *tx,
        sandbox_id,
        route_generation,
        execution_generation,
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

async fn provenance(pool: &PgPool, sandbox_id: SandboxId) -> TestDbResult<Option<(i64, i64)>> {
    Ok(sqlx::query_as::<_, (i64, i64)>(sql!(
        "SELECT last_route_generation, last_execution_generation
         FROM sandbox_direct_route_provenance
         WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id.as_uuid())
    .fetch_optional(pool)
    .await?)
}

#[tokio::test(flavor = "current_thread")]
async fn route_identity_reads_back_and_deletes_without_its_provenance() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let sandbox_id = sandbox(pool).await?;
    let stored = identity(sandbox_id, 2, NodeId::new(), 3, 1);

    assert_eq!(route_identities::get(pool, sandbox_id).await?, None);
    assert!(accept(pool, &stored).await?);
    assert!(route_provenance::retain_route_identity_generations(pool, sandbox_id).await?);
    assert_eq!(
        route_identities::get(pool, sandbox_id).await?,
        Some(row(&stored))
    );

    route_identities::delete(pool, sandbox_id).await?;
    assert_eq!(route_identities::get(pool, sandbox_id).await?, None);
    assert_eq!(
        provenance(pool, sandbox_id).await?,
        Some((2, 3)),
        "clearing the route keeps the generations it used"
    );
    // A second delete finds nothing and is not an error.
    route_identities::delete(pool, sandbox_id).await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn store_unless_newer_refuses_a_lower_generation() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let sandbox_id = sandbox(pool).await?;
    let node_id = NodeId::new();
    let stored = identity(sandbox_id, 5, node_id, 5, 1);
    assert!(route_identities::store_unless_newer(pool, &stored).await?);

    // Losing cases: either generation lower than the stored one matches no row.
    let older_route = identity(sandbox_id, 4, NodeId::new(), 6, 2);
    assert!(!route_identities::store_unless_newer(pool, &older_route).await?);
    let older_execution = identity(sandbox_id, 6, NodeId::new(), 4, 2);
    assert!(!route_identities::store_unless_newer(pool, &older_execution).await?);
    assert_eq!(
        route_identities::get(pool, sandbox_id).await?,
        Some(row(&stored)),
        "a refused write leaves the stored route"
    );

    // Equal generations are not newer, so they replace it, on any node.
    let moved = identity(sandbox_id, 5, NodeId::new(), 5, 0);
    assert!(route_identities::store_unless_newer(pool, &moved).await?);
    assert_eq!(
        route_identities::get(pool, sandbox_id).await?,
        Some(row(&moved))
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn accept_receipt_refuses_a_stale_receipt_on_the_routed_node() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let sandbox_id = sandbox(pool).await?;
    let node_id = NodeId::new();
    let stored = identity(sandbox_id, 3, node_id, 3, 5);
    assert!(accept(pool, &stored).await?);
    sqlx::query(sql!(
        "UPDATE sandboxes SET active_node_id = $2 WHERE id = $1"
    ))
    .bind(sandbox_id.as_uuid())
    .bind(node_id.as_uuid())
    .execute(pool)
    .await?;

    // Losing cases on the stored route's node, while the row is active there.
    for (stale, case) in [
        (
            identity(sandbox_id, 2, node_id, 4, 6),
            "lower route generation",
        ),
        (identity(sandbox_id, 3, node_id, 2, 6), "older execution"),
        (
            identity(sandbox_id, 3, node_id, 3, 4),
            "same execution, earlier activation attempt",
        ),
    ] {
        assert!(
            !accept(pool, &stale).await?,
            "refuses a receipt with a {case}"
        );
        assert_eq!(
            route_identities::get(pool, sandbox_id).await?,
            Some(row(&stored)),
            "a refused receipt with a {case} leaves the stored route"
        );
    }

    let same_attempt = identity(sandbox_id, 3, node_id, 3, 5);
    assert!(accept(pool, &same_attempt).await?);
    let newer_execution = identity(sandbox_id, 4, node_id, 4, 1);
    assert!(accept(pool, &newer_execution).await?);
    assert_eq!(
        route_identities::get(pool, sandbox_id).await?,
        Some(row(&newer_execution))
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn accept_receipt_moves_the_route_only_to_the_sandboxes_active_node() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let sandbox_id = sandbox(pool).await?;
    let stored = identity(sandbox_id, 3, NodeId::new(), 3, 1);
    assert!(accept(pool, &stored).await?);

    // Losing case: another node, while the row names none, is refused even
    // with newer generations.
    let other_node = NodeId::new();
    let moved = identity(sandbox_id, 1, other_node, 1, 0);
    let newer_elsewhere = identity(sandbox_id, 9, other_node, 9, 9);
    assert!(!accept(pool, &newer_elsewhere).await?);
    assert_eq!(
        route_identities::get(pool, sandbox_id).await?,
        Some(row(&stored))
    );

    // Once the row is active there, the receipt moves the route whatever its
    // generations.
    sqlx::query(sql!(
        "UPDATE sandboxes SET active_node_id = $2 WHERE id = $1"
    ))
    .bind(sandbox_id.as_uuid())
    .bind(other_node.as_uuid())
    .execute(pool)
    .await?;
    assert!(accept(pool, &moved).await?);
    assert_eq!(
        route_identities::get(pool, sandbox_id).await?,
        Some(row(&moved))
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn provenance_initializes_once_and_advances_past_both_generations() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let sandbox_id = sandbox(pool).await?;

    assert_eq!(
        route_provenance::advance_generations(pool, sandbox_id).await?,
        None,
        "nothing to advance before the provenance exists"
    );
    assert!(route_provenance::initialize(pool, sandbox_id).await?);
    assert_eq!(provenance(pool, sandbox_id).await?, Some((1, 1)));

    // Losing case: a second initialization inserts nothing and keeps the row.
    sqlx::query(sql!(
        "UPDATE sandbox_direct_route_provenance
         SET last_route_generation = 4, last_execution_generation = 7
         WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id.as_uuid())
    .execute(pool)
    .await?;
    assert!(!route_provenance::initialize(pool, sandbox_id).await?);
    assert_eq!(provenance(pool, sandbox_id).await?, Some((4, 7)));

    assert_eq!(
        route_provenance::advance_generations(pool, sandbox_id).await?,
        Some(ReservedGenerations {
            last_route_generation: 8,
            last_execution_generation: 8,
        })
    );
    assert_eq!(provenance(pool, sandbox_id).await?, Some((8, 8)));

    sqlx::query(sql!(
        "UPDATE sandbox_direct_route_provenance
         SET last_route_generation = 9, last_execution_generation = 4
         WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id.as_uuid())
    .execute(pool)
    .await?;
    assert_eq!(
        route_provenance::advance_generations(pool, sandbox_id).await?,
        Some(ReservedGenerations {
            last_route_generation: 10,
            last_execution_generation: 10,
        })
    );
    assert_eq!(provenance(pool, sandbox_id).await?, Some((10, 10)));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn provenance_retains_only_higher_generations() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let sandbox_id = sandbox(pool).await?;
    let route = |value| RouteGeneration::new(value).expect("positive route generation");
    let execution = |value| ExecutionGeneration::new(value).expect("positive execution generation");

    retain(pool, sandbox_id, route(3), execution(5)).await?;
    assert_eq!(provenance(pool, sandbox_id).await?, Some((3, 5)));
    retain(pool, sandbox_id, route(4), execution(2)).await?;
    assert_eq!(
        provenance(pool, sandbox_id).await?,
        Some((4, 5)),
        "each column keeps the higher value"
    );

    assert!(
        !route_provenance::retain_route_identity_generations(pool, sandbox_id).await?,
        "no route identity, nothing retained"
    );
    assert_eq!(provenance(pool, sandbox_id).await?, Some((4, 5)));
    let routed = identity(sandbox_id, 6, NodeId::new(), 1, 1);
    assert!(accept(pool, &routed).await?);
    assert!(route_provenance::retain_route_identity_generations(pool, sandbox_id).await?);
    assert_eq!(provenance(pool, sandbox_id).await?, Some((6, 5)));
    retain(pool, sandbox_id, route(1), execution(9)).await?;
    assert_eq!(
        provenance(pool, sandbox_id).await?,
        Some((6, 9)),
        "a lower route generation keeps the stored one"
    );
    retain(pool, sandbox_id, route(8), execution(1)).await?;
    assert!(route_provenance::retain_route_identity_generations(pool, sandbox_id).await?);
    assert_eq!(
        provenance(pool, sandbox_id).await?,
        Some((8, 9)),
        "a route identity below the stored generations keeps them"
    );
    Ok(())
}
