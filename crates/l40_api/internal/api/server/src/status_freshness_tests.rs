//! A node's status window is derived from the poll interval it reports, so a
//! node that publishes only on its node-ctl tick never reads stale, and one
//! that stops publishing reads stale within the documented bound.

use std::time::Duration;

use pplx_space_common_core::sql;
use pplx_space_common_core::types::NodeId;
use pplx_space_common_svc::sql::sandbox_residency::node_heartbeat_fresh;
use pplx_space_db_test_fixtures::{TestDb, insert_ready_worker_node, try_pg_test_db};
use pplx_space_node_api_client::node_status_freshness::{
    DEFAULT_NODE_CTL_POLL_INTERVAL, DEFAULT_NODE_STATUS_FRESHNESS, NODE_STATUS_OUTBOUND_RECONCILER,
    node_status_freshness,
};
use pplx_space_test_env::TestResult;

use crate::ClusterApiService;
use crate::proto::{
    NodeCtlStatus, NodeRole as ProtoNodeRole, PublishNodeStateRequest, PublishedNodeStatus,
    ReconciliationStatus,
};
use crate::tests::{TEST_WORKER_IP, request_from_node};

struct Node {
    db: TestDb,
    service: ClusterApiService,
    node_id: NodeId,
}

async fn node() -> TestResult<Option<Node>> {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(None);
    };
    let node_id = NodeId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    let service = ClusterApiService::new(db.pool.clone());
    Ok(Some(Node {
        db,
        service,
        node_id,
    }))
}

impl Node {
    /// The node-status facet's publication, reporting `poll_interval` for its
    /// own reconciler the way node-ctl does on every tick.
    async fn publish_status(&self, poll_interval: Duration) -> Result<(), tonic::Status> {
        self.publish(Some(PublishedNodeStatus {
            node_ctl: Some(NodeCtlStatus {
                reachable: true,
                error: None,
                reconciliations: vec![ReconciliationStatus {
                    name: NODE_STATUS_OUTBOUND_RECONCILER.to_string(),
                    poll_interval_secs: poll_interval.as_secs(),
                    ..Default::default()
                }],
                sync_overrides: Vec::new(),
            }),
            ..Default::default()
        }))
        .await
    }

    async fn publish(&self, node_status: Option<PublishedNodeStatus>) -> Result<(), tonic::Status> {
        let request = request_from_node(
            PublishNodeStateRequest {
                node_id: self.node_id.to_string(),
                role: Some(ProtoNodeRole::Worker as i32),
                node_status,
                ..PublishNodeStateRequest::default()
            },
            TEST_WORKER_IP,
        );
        self.service.publish_node_state(request).await.map(|_| ())
    }

    /// `stale_after - observed_at` of the stored snapshot.
    async fn window(&self) -> TestResult<Duration> {
        let secs: f64 = sqlx::query_scalar(sql!(
            "SELECT EXTRACT(EPOCH FROM stale_after - observed_at)::DOUBLE PRECISION
               FROM node_status_snapshots
              WHERE node_id = $1"
        ))
        .bind(self.node_id)
        .fetch_one(&self.db.pool)
        .await?;
        Ok(Duration::from_secs_f64(secs))
    }

    /// Moves the last publication `elapsed` into the past, as if the node had
    /// been silent that long since.
    async fn go_quiet_for(&self, elapsed: Duration) -> TestResult {
        sqlx::query(sql!(
            "UPDATE node_status_snapshots
                SET observed_at = observed_at - make_interval(secs => $2::DOUBLE PRECISION),
                    stale_after = stale_after - make_interval(secs => $2::DOUBLE PRECISION)
              WHERE node_id = $1"
        ))
        .bind(self.node_id)
        .bind(elapsed.as_secs_f64())
        .execute(&self.db.pool)
        .await?;
        Ok(())
    }

    /// The admin API's `status.stale` and drainability's `heartbeat_fresh`,
    /// read together: `(stale, heartbeat_fresh)`.
    async fn freshness(&self) -> TestResult<(bool, bool)> {
        Ok(sqlx::query_as(sqlx::AssertSqlSafe(sql!(format!(
            "SELECT stale_after <= NOW(), {}
               FROM node_status_snapshots
              WHERE node_id = $1",
            node_heartbeat_fresh("$1")
        ))))
        .bind(self.node_id)
        .fetch_one(&self.db.pool)
        .await?)
    }
}

/// A quiet node publishes once per tick. Freshness only falls as the gap
/// grows, so a node still fresh two whole ticks after its last publication (one
/// lost) is fresh at every point in between.
#[tokio::test(flavor = "current_thread")]
async fn node_two_ticks_after_its_last_publication_still_reads_fresh() -> TestResult {
    let Some(node) = node().await? else {
        return Ok(());
    };
    let tick = DEFAULT_NODE_CTL_POLL_INTERVAL;
    node.publish_status(tick).await?;
    node.go_quiet_for(tick * 2).await?;
    assert_eq!(node.freshness().await?, (false, true));
    Ok(())
}

/// A node that stops publishing reads stale once the window has passed.
#[tokio::test(flavor = "current_thread")]
async fn node_reads_stale_once_the_window_has_passed() -> TestResult {
    let Some(node) = node().await? else {
        return Ok(());
    };
    let tick = DEFAULT_NODE_CTL_POLL_INTERVAL;
    node.publish_status(tick).await?;
    assert_eq!(node.window().await?, DEFAULT_NODE_STATUS_FRESHNESS);

    node.go_quiet_for(DEFAULT_NODE_STATUS_FRESHNESS).await?;
    assert_eq!(node.freshness().await?, (true, false));
    Ok(())
}

/// A publication without a usable interval keeps the window the node's last
/// reported interval set, and a node's first one gets the default.
#[tokio::test(flavor = "current_thread")]
async fn publication_without_an_interval_keeps_the_last_reported_window() -> TestResult {
    let Some(node) = node().await? else {
        return Ok(());
    };
    node.publish(None).await?;
    assert_eq!(node.window().await?, DEFAULT_NODE_STATUS_FRESHNESS);

    let tick = Duration::from_secs(120);
    node.publish_status(tick).await?;
    node.go_quiet_for(tick).await?;
    node.publish(None).await?;
    assert_eq!(node.window().await?, node_status_freshness(tick));
    Ok(())
}

/// A status publication that reports a shorter interval holds the node to it
/// at once, rather than to the later deadline its old interval set.
#[tokio::test(flavor = "current_thread")]
async fn status_publication_with_a_shorter_interval_replaces_the_deadline() -> TestResult {
    let Some(node) = node().await? else {
        return Ok(());
    };
    node.publish_status(Duration::from_secs(120)).await?;
    let tick = Duration::from_secs(20);
    node.publish_status(tick).await?;
    assert_eq!(node.window().await?, node_status_freshness(tick));
    Ok(())
}
