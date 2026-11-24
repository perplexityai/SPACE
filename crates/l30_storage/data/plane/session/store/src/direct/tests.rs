use pplx_space_accounts::UserId;
use pplx_space_db_test_fixtures::{
    SEED_ORG_ID, SEED_USER_ID, insert_ready_worker_node, insert_suspended_sandbox, pg_test_db,
};
use pplx_space_test_env::TestResult;

use super::*;
use crate::{NewSession, insert_session};

async fn insert(
    pool: &sqlx::PgPool,
    session_id: &str,
    sandbox_id: SandboxId,
) -> Result<(), sqlx::Error> {
    let now = OffsetDateTime::now_utc();
    let scopes = vec!["terminal:connect".to_string()];
    let adapter_state = serde_json::json!({"rows": 24});
    insert_session(
        pool,
        &NewSession {
            session_id,
            org_id: OrgId::from_uuid(SEED_ORG_ID),
            actor_user_id: Some(UserId::from_uuid(SEED_USER_ID)),
            sandbox_id,
            kind: "terminal",
            state: "pending",
            scopes: &scopes,
            token_hash: "token-hash",
            token_expires_at: now + time::Duration::minutes(5),
            attach_deadline_at: now + time::Duration::minutes(1),
            target_port: None,
            target_protocol: None,
            target_path: None,
            adapter_state: &adapter_state,
            expires_at: now + time::Duration::hours(24),
        },
    )
    .await?;
    Ok(())
}

async fn route(pool: &sqlx::PgPool, session_id: &str) -> Result<SessionRoute, sqlx::Error> {
    Ok(live_session_route(pool, session_id, None)
        .await?
        .expect("live session route"))
}

/// Catches the route read returning another session's values, or reading an
/// expired session or one whose protocol session id does not match.
#[tokio::test(flavor = "current_thread")]
async fn a_live_session_route_reads_its_session_and_sandbox() -> TestResult {
    let db = pg_test_db().await?;
    let org_id = OrgId::from_uuid(SEED_ORG_ID);
    let sandbox_id =
        insert_suspended_sandbox(&db.pool, org_id, UserId::from_uuid(SEED_USER_ID)).await?;
    insert(&db.pool, "dps_route", sandbox_id).await?;
    insert(&db.pool, "dps_expired", sandbox_id).await?;
    sqlx::query(sql!(
        "UPDATE data_plane_sessions
         SET expires_at = NOW() - INTERVAL '1 minute'
         WHERE session_id = $1"
    ))
    .bind("dps_expired")
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE data_plane_sessions
         SET protocol_session_id = $2
         WHERE session_id = $1"
    ))
    .bind("dps_route")
    .bind("proto_route")
    .execute(&db.pool)
    .await?;

    let read = route(&db.pool, "dps_route").await?;
    assert_eq!(read.session_id, "dps_route");
    assert_eq!(read.protocol_session_id.as_deref(), Some("proto_route"));
    assert_eq!(read.org_id, org_id);
    assert_eq!(read.sandbox_id, sandbox_id);
    assert_eq!(read.kind, "terminal");
    assert_eq!(read.scopes, ["terminal:connect"]);
    assert_eq!(read.token_hash.as_deref(), Some("token-hash"));
    assert_eq!(read.revocation_status, "active");
    assert_eq!(read.org_revocation_epoch, 0);
    assert_eq!(read.sandbox_lifecycle, SandboxLifecycle::Suspended);
    assert_eq!(read.current_node_id, None);
    assert_eq!(read.sandbox_active_node_id, None);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&read.adapter_state_json)?,
        serde_json::json!({"rows": 24})
    );

    assert!(
        live_session_route(&db.pool, "dps_expired", None)
            .await?
            .is_none()
    );
    assert!(
        live_session_route(&db.pool, "dps_route", Some("proto_route"))
            .await?
            .is_some()
    );
    assert!(
        live_session_route(&db.pool, "dps_route", Some("other-protocol"))
            .await?
            .is_none()
    );
    Ok(())
}

/// Catches a repoint that leaves the route where it was, advances the route
/// generation when nothing changed, or misses one that did. The losing case:
/// repointing a session that is gone matches no row and changes no other
/// session's route.
#[tokio::test(flavor = "current_thread")]
async fn a_repoint_advances_the_route_generation_only_on_change() -> TestResult {
    let db = pg_test_db().await?;
    let org_id = OrgId::from_uuid(SEED_ORG_ID);
    let sandbox_id =
        insert_suspended_sandbox(&db.pool, org_id, UserId::from_uuid(SEED_USER_ID)).await?;
    let node_id = NodeId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    insert(&db.pool, "dps_route", sandbox_id).await?;
    let first = route(&db.pool, "dps_route").await?;

    let moved = repoint_session_route(&db.pool, "dps_route", node_id, "10.0.0.45:50051", "grpc")
        .await?
        .expect("a live session repoints");
    assert_eq!(moved.current_node_id, Some(node_id));
    assert_eq!(moved.backend_endpoint.as_deref(), Some("10.0.0.45:50051"));
    assert_eq!(moved.backend_transport.as_deref(), Some("grpc"));
    assert_ne!(moved.route_generation, first.route_generation);

    let same = repoint_session_route(&db.pool, "dps_route", node_id, "10.0.0.45:50051", "grpc")
        .await?
        .expect("a live session repoints");
    assert_eq!(same.route_generation, moved.route_generation);

    let changed = repoint_session_route(&db.pool, "dps_route", node_id, "10.0.0.45:50052", "grpc")
        .await?
        .expect("a live session repoints");
    assert_eq!(changed.backend_endpoint.as_deref(), Some("10.0.0.45:50052"));
    assert_ne!(changed.route_generation, moved.route_generation);

    assert!(
        repoint_session_route(&db.pool, "dps_missing", node_id, "10.0.0.46:50051", "grpc")
            .await?
            .is_none()
    );
    let after = route(&db.pool, "dps_route").await?;
    assert_eq!(
        (
            after.route_generation,
            after.current_node_id,
            after.backend_endpoint
        ),
        (
            changed.route_generation,
            changed.current_node_id,
            changed.backend_endpoint
        )
    );
    Ok(())
}
