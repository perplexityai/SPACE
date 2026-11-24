//! The observed-state publication writes against Postgres, with a Direct
//! route identity seeded: each accepts a sandbox placed on the publishing
//! node through its route identity, or one with no row, and skips the rest.

use std::collections::BTreeSet;

use pplx_space_accounts::{NodeId, OrgId, UserId};
use pplx_space_db_test_fixtures::{
    SEED_ORG_ID, SEED_USER_ID, TestDbResult, insert_suspended_sandbox, pg_test_db,
};
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;
use sqlx::PgPool;
use uuid::Uuid;

use super::{
    PublishedObservedState, write_degraded_observed_states, write_published_observed_states,
};

struct Placement {
    publisher: NodeId,
    /// Placed on `publisher` by its route identity, though its active node
    /// is another.
    routed: SandboxId,
    /// Placed on another node.
    elsewhere: SandboxId,
}

async fn placement(pool: &PgPool) -> TestDbResult<Placement> {
    let (publisher, other) = (NodeId::new(), NodeId::new());
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '127.0.0.1', 'worker', 'space', 'registered'),
                ($2, '127.0.0.2', 'worker', 'space', 'registered')"
    ))
    .bind(publisher)
    .bind(other)
    .execute(pool)
    .await?;
    let mut placed = Vec::new();
    for _ in 0..2 {
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
        .bind(other)
        .execute(pool)
        .await?;
        placed.push(sandbox_id);
    }
    sqlx::query(sql!(
        "INSERT INTO sandbox_direct_route_identities (
             sandbox_id, route_generation, current_node_id, substrate_kind,
             execution_generation, activation_attempt_id
         )
         VALUES ($1, 1, $2, 'space', 1, $3)"
    ))
    .bind(placed[0])
    .bind(publisher)
    .bind(Uuid::now_v7())
    .execute(pool)
    .await?;
    Ok(Placement {
        publisher,
        routed: placed[0],
        elsewhere: placed[1],
    })
}

/// `node_id`, `status`, `snapshots_enabled` of a stored observation.
async fn stored(
    pool: &PgPool,
    sandbox_id: SandboxId,
) -> TestDbResult<Option<(NodeId, Option<String>, bool)>> {
    Ok(sqlx::query_as::<_, (NodeId, Option<String>, bool)>(sql!(
        "SELECT node_id, status, snapshots_enabled
           FROM sandbox_observed_states
          WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .fetch_optional(pool)
    .await?)
}

#[tokio::test(flavor = "current_thread")]
async fn degraded_observations_are_fenced_on_placement() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let placement = placement(pool).await?;
    let rowless = SandboxId::new();

    let written = write_degraded_observed_states(
        pool,
        placement.publisher,
        serde_json::json!([
            {"sandbox_id": placement.routed.as_uuid(), "status": "started"},
            {"sandbox_id": placement.elsewhere.as_uuid(), "status": "started"},
            {"sandbox_id": rowless.as_uuid(), "status": null},
        ]),
    )
    .await?;
    assert_eq!(
        written.into_iter().collect::<BTreeSet<_>>(),
        BTreeSet::from([placement.routed, rowless])
    );
    assert_eq!(
        stored(pool, placement.routed).await?,
        Some((placement.publisher, Some("started".to_owned()), false))
    );
    assert_eq!(
        stored(pool, rowless).await?,
        Some((placement.publisher, None, false))
    );
    assert_eq!(stored(pool, placement.elsewhere).await?, None);

    // A NULL status keeps the stored one.
    write_degraded_observed_states(
        pool,
        placement.publisher,
        serde_json::json!([{"sandbox_id": placement.routed.as_uuid(), "status": null}]),
    )
    .await?;
    assert_eq!(
        stored(pool, placement.routed).await?,
        Some((placement.publisher, Some("started".to_owned()), false))
    );
    Ok(())
}

fn published(sandbox_id: SandboxId, status: &str) -> PublishedObservedState<'_> {
    PublishedObservedState {
        sandbox_id,
        status: Some(status),
        state_changed_at: None,
        template: Some("tmpl-store-test"),
        runtime: Some("fc"),
        spaced_version: None,
        runtime_version: None,
        guest_os: None,
        guest_arch: None,
        state_format_name: None,
        state_format_version: None,
        execution_metadata: None,
        pid: Some(7),
        namespace: None,
        uptime: None,
        sandbox_owner: None,
        sandbox_user: None,
        image_hash: None,
        snapshots_enabled: true,
        pool_template: None,
        failure_reason: None,
        observed_created_at: None,
        network_tx_bytes: Some(1111),
        network_rx_bytes: Some(2222),
        disk_usage_bytes: Some(4096),
        auto_lifecycle_authority: None,
    }
}

/// `sandbox_id`, `assigned_to_node`, `inserted`, `updated`, `first_seen`,
/// `status_changed` of each accepted row, sorted.
type Accepted = Vec<(SandboxId, bool, bool, bool, bool, bool)>;

async fn publish(
    pool: &PgPool,
    node_id: NodeId,
    rows: &[PublishedObservedState<'_>],
) -> TestDbResult<Accepted> {
    let mut accepted = write_published_observed_states(pool, node_id, rows)
        .await?
        .into_iter()
        .map(|row| {
            (
                row.sandbox_id,
                row.assigned_to_node,
                row.inserted,
                row.updated,
                row.first_seen,
                row.status_changed,
            )
        })
        .collect::<Vec<_>>();
    accepted.sort();
    Ok(accepted)
}

#[tokio::test(flavor = "current_thread")]
async fn published_observations_write_only_what_changed_on_the_placed_node() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let placement = placement(pool).await?;
    let rowless = SandboxId::new();
    let node_id = placement.publisher;
    let sorted = |mut rows: Accepted| {
        rows.sort();
        rows
    };

    let first = [
        published(placement.routed, "started"),
        published(placement.elsewhere, "started"),
        published(rowless, "started"),
    ];
    assert_eq!(
        publish(pool, node_id, &first).await?,
        sorted(vec![
            (placement.routed, true, true, false, true, true),
            (rowless, false, true, false, true, true),
        ]),
        "the sandbox placed elsewhere is not accepted"
    );
    assert_eq!(stored(pool, placement.elsewhere).await?, None);
    let network_bytes: (Option<i64>, Option<i64>) = sqlx::query_as(sql!(
        "SELECT network_tx_bytes, network_rx_bytes
           FROM sandbox_observed_states
          WHERE sandbox_id = $1"
    ))
    .bind(placement.routed)
    .fetch_one(pool)
    .await?;
    assert_eq!(network_bytes, (Some(1111), Some(2222)));

    // An identical publication writes nothing.
    assert_eq!(
        publish(pool, node_id, &first).await?,
        sorted(vec![
            (placement.routed, true, false, false, false, false),
            (rowless, false, false, false, false, false),
        ])
    );

    // A changed status updates the row. A NULL disk usage keeps the stored one.
    let changed = [PublishedObservedState {
        disk_usage_bytes: None,
        ..published(placement.routed, "paused")
    }];
    assert_eq!(
        publish(pool, node_id, &changed).await?,
        vec![(placement.routed, true, false, true, false, true)]
    );
    assert_eq!(
        stored(pool, placement.routed).await?,
        Some((node_id, Some("paused".to_owned()), true))
    );
    let disk_usage_bytes: Option<i64> = sqlx::query_scalar(sql!(
        "SELECT disk_usage_bytes FROM sandbox_observed_states WHERE sandbox_id = $1"
    ))
    .bind(placement.routed)
    .fetch_one(pool)
    .await?;
    assert_eq!(disk_usage_bytes, Some(4096));
    Ok(())
}
