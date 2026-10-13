//! Cluster admission over Direct's own inventory count
//! (`DirectSubstrate::eligible_start_node_count`), which counts a registered
//! worker with a stale heartbeat as eligible, matching direct placement's
//! last-known-worker fallback. Kubernetes counts schedulable nodes from its
//! node cache instead.

use pplx_space_common_core::types::NodeId;
use pplx_space_db_test_fixtures::try_pg_test_db;
use pplx_space_test_env::TestResult;

use crate::cluster_admission::tests::{direct_substrate, enabled_config};
use crate::cluster_admission::{
    ClusterActivationClass, ClusterSandboxAdmission, ClusterSandboxAdmissionConfig,
};
use crate::controllers::substrate::Substrate;

/// Stands in for `cluster_admission::tests::admission`, whose bucket parameter
/// is a type private to `cluster_admission`. The test passes no bucket, and
/// `with_config` builds none from a config without a Redis URL.
fn admission(
    substrate: Substrate,
    config: ClusterSandboxAdmissionConfig,
    _bucket: Option<std::convert::Infallible>,
) -> ClusterSandboxAdmission {
    ClusterSandboxAdmission::with_config(
        substrate,
        config,
        pplx_space_feature_flags::FeatureFlagClient::disabled(),
    )
}

#[tokio::test]
async fn stale_heartbeat_direct_worker_is_not_empty_cluster() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    // A registered worker whose status heartbeat is stale is still
    // placement-eligible via direct placement's last-known fallback, so the
    // empty-cluster guard must count it rather than reject as `empty_cluster`.
    let node_id = NodeId::new();
    sqlx::query(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '127.0.0.1', 'worker', 'space', 'registered')",
    )
    .bind(node_id)
    .execute(&db.pool)
    .await?;
    sqlx::query(
        "INSERT INTO node_direct (node_id, name, node_api_endpoint)
         VALUES ($1, 'stale-worker', '127.0.0.1:9802')",
    )
    .bind(node_id)
    .execute(&db.pool)
    .await?;
    sqlx::query(
        "INSERT INTO node_status_snapshots (
             node_id, observed_at, stale_after, reachable, role,
             scheduling_status, services, updated_at
         )
         VALUES ($1, NOW() - INTERVAL '1 hour', NOW() - INTERVAL '30 minutes', true, 'worker', 'ready', '[]'::jsonb, NOW() - INTERVAL '1 hour')",
    )
    .bind(node_id)
    .execute(&db.pool)
    .await?;

    // No bucket → the only way this returns Ok is if the guard counted the
    // stale worker (fail open); a false empty_cluster would return EmptyCluster.
    let admission = admission(direct_substrate(&db.pool), enabled_config(), None);
    admission
        .admit_activation(ClusterActivationClass::Cold)
        .await?;
    Ok(())
}
