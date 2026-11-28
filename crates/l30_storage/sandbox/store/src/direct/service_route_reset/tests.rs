//! The service route reset against Postgres, with the Direct route identity
//! seeded: the guard's losing cases change nothing.

use pplx_space_accounts::{NodeId, OrgId, UserId};
use pplx_space_db_test_fixtures::{
    SEED_ORG_ID, SEED_USER_ID, TestDbResult, insert_suspended_sandbox, pg_test_db,
};
use pplx_space_sandbox_model::{RouteGeneration, SandboxId};
use pplx_space_sql_locator::sql;
use sqlx::PgPool;
use uuid::Uuid;

use super::reset_applied_service_port;

/// `apply_state`, `last_applied_node_id`, `last_error`, `last_bridge_status`,
/// `last_observed_at IS NOT NULL`, as stored.
type Exposure = (String, Option<Uuid>, Option<String>, Option<String>, bool);

async fn exposure(pool: &PgPool, sandbox_id: SandboxId, port: i32) -> TestDbResult<Exposure> {
    Ok(sqlx::query_as::<_, Exposure>(sql!(
        "SELECT apply_state, last_applied_node_id, last_error, last_bridge_status,
                last_observed_at IS NOT NULL
           FROM sandbox_port_exposures
          WHERE sandbox_id = $1 AND protocol = 'tcp' AND guest_port = $2"
    ))
    .bind(sandbox_id)
    .bind(port)
    .fetch_one(pool)
    .await?)
}

async fn insert_exposure(
    pool: &PgPool,
    sandbox_id: SandboxId,
    port: i32,
    apply_state: &str,
    last_applied_node_id: NodeId,
) -> TestDbResult<()> {
    sqlx::query(sql!(
        "INSERT INTO sandbox_port_exposures (
             sandbox_id, protocol, guest_port, visibility, source, desired_state,
             apply_state, last_applied_node_id, last_bridge_status, last_observed_at
         )
         VALUES ($1, 'tcp', $2, 'public', 'api', 'open', $3, $4, 'direct', NOW())"
    ))
    .bind(sandbox_id)
    .bind(port)
    .bind(apply_state)
    .bind(last_applied_node_id)
    .execute(pool)
    .await?;
    Ok(())
}

fn generation(value: u64) -> RouteGeneration {
    RouteGeneration::new(value).expect("positive route generation")
}

/// Runs [`reset_applied_service_port`] in a transaction of its own and
/// commits it.
async fn reset(
    pool: &PgPool,
    sandbox_id: SandboxId,
    port: i32,
    route_generation: RouteGeneration,
    diagnostic: &str,
) -> TestDbResult<Option<NodeId>> {
    let mut tx = pool.begin().await?;
    let node_id =
        reset_applied_service_port(&mut *tx, sandbox_id, port, route_generation, diagnostic)
            .await?;
    tx.commit().await?;
    Ok(node_id)
}

#[tokio::test(flavor = "current_thread")]
async fn reset_applied_service_port_refuses_a_stale_route_and_changes_nothing() -> TestDbResult<()>
{
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let (node_id, other_node) = (NodeId::new(), NodeId::new());
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '127.0.0.1', 'worker', 'space', 'registered'),
                ($2, '127.0.0.2', 'worker', 'space', 'registered')"
    ))
    .bind(node_id)
    .bind(other_node)
    .execute(pool)
    .await?;
    let sandbox_id = insert_suspended_sandbox(
        pool,
        OrgId::from_uuid(SEED_ORG_ID),
        UserId::from_uuid(SEED_USER_ID),
    )
    .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET lifecycle = 'started', active_node_id = $2, target_node_id = $2
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .execute(pool)
    .await?;
    // A Direct sandbox has no execution row; its generation is its route
    // identity's.
    sqlx::query(sql!(
        "INSERT INTO sandbox_direct_route_identities (
             sandbox_id, route_generation, current_node_id, substrate_kind,
             execution_generation, activation_attempt_id
         )
         VALUES ($1, 3, $2, 'space', 3, $3)"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .bind(Uuid::now_v7())
    .execute(pool)
    .await?;
    insert_exposure(pool, sandbox_id, 8080, "applied", node_id).await?;
    insert_exposure(pool, sandbox_id, 8081, "pending", node_id).await?;
    insert_exposure(pool, sandbox_id, 8082, "applied", other_node).await?;
    let applied = (
        "applied".to_owned(),
        Some(node_id.as_uuid()),
        None,
        Some("direct".to_owned()),
        true,
    );
    let before_8081 = exposure(pool, sandbox_id, 8081).await?;
    let before_8082 = exposure(pool, sandbox_id, 8082).await?;

    // Losing cases: another generation, a port that isn't applied, and one
    // applied on another node match no row.
    for (port, route_generation) in [(8080, 4), (8081, 3), (8082, 3)] {
        assert_eq!(
            reset(
                pool,
                sandbox_id,
                port,
                generation(route_generation),
                "refused"
            )
            .await?,
            None,
            "port {port} at generation {route_generation}"
        );
    }
    assert_eq!(exposure(pool, sandbox_id, 8080).await?, applied);
    assert_eq!(exposure(pool, sandbox_id, 8081).await?, before_8081);
    assert_eq!(exposure(pool, sandbox_id, 8082).await?, before_8082);

    assert_eq!(
        reset(pool, sandbox_id, 8080, generation(3), "unreachable").await?,
        Some(node_id)
    );
    assert_eq!(
        exposure(pool, sandbox_id, 8080).await?,
        (
            "pending".to_owned(),
            None,
            Some("unreachable".to_owned()),
            None,
            false
        )
    );
    // Once pending, the same reset matches no row.
    assert_eq!(
        reset(pool, sandbox_id, 8080, generation(3), "again").await?,
        None
    );
    Ok(())
}
