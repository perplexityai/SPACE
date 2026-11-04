use pplx_space_common_core::types::NodeId;

use super::*;
use pplx_space_cluster_trigger_queue::enqueue_triggers;
use pplx_space_db_test_fixtures::{TestDb, TestDbResult, pg_test_db};
use rstest::{fixture, rstest};

#[test]
fn retrying_client_connect_failures_keep_connect_error_class() {
    let error = TriggerError::Rpc {
        endpoint: "http://node-api:9800".to_string(),
        status: tonic::Status::unavailable("node-api connect failed: transport error"),
    };

    assert_eq!(error.class(), "connect");
}

#[test]
fn unavailable_rpc_failures_keep_rpc_error_class() {
    let error = TriggerError::Rpc {
        endpoint: "http://node-api:9800".to_string(),
        status: tonic::Status::unavailable("node-api returned unavailable"),
    };

    assert_eq!(error.class(), "rpc");
}

struct ReflectorBackedResolver {
    live_node_names: HashSet<String>,
    resolved_node_names: Mutex<Vec<String>>,
}

impl TriggerEndpointResolver for ReflectorBackedResolver {
    fn live_node_names(&self) -> Option<HashSet<String>> {
        Some(self.live_node_names.clone())
    }

    async fn try_resolve_node_api_endpoint(
        &self,
        target: TriggerNodeApiTarget<'_>,
    ) -> Result<String, TriggerResolveError> {
        self.resolved_node_names.lock().await.push(
            target
                .node_name
                .expect("k8s target must have a node name")
                .to_string(),
        );
        Ok("\n".to_string())
    }
}

struct GhostFanoutFixture {
    live_node_id: NodeId,
    ghost_node_id: NodeId,
    claim: StateReconciliationTriggerClaim,
    resolver: Arc<ReflectorBackedResolver>,
    db: TestDb,
}

/// Two enqueued-and-claimed Kubernetes worker triggers: `live-node` is present
/// in the resolver's reflector snapshot, `ghost-node` is absent from it.
#[fixture]
async fn ghost_fanout_fixture() -> TestDbResult<GhostFanoutFixture> {
    let db = pg_test_db().await?;
    let live_node_id = NodeId::new();
    let ghost_node_id = NodeId::new();
    for (node_id, node_name) in [(live_node_id, "live-node"), (ghost_node_id, "ghost-node")] {
        sqlx::query(pplx_space_common_core::sql!(
            "INSERT INTO nodes (id, role, managed_by, state)
             VALUES ($1, $2, $3, 'registered')"
        ))
        .bind(node_id)
        .bind(SpaceNodeRole::Worker)
        .bind(NodeManagedBy::Kubernetes)
        .execute(&db.pool)
        .await?;
        sqlx::query(pplx_space_common_core::sql!(
            "INSERT INTO node_k8s (node_id, k8s_node_name) VALUES ($1, $2)"
        ))
        .bind(node_id)
        .bind(node_name)
        .execute(&db.pool)
        .await?;
    }
    enqueue_triggers(
        &db.pool,
        [live_node_id, ghost_node_id],
        ReconcileReason::ManualNodeReconcile,
    )
    .await?;
    let claim = claim_triggers(&db.pool, 2).await?;
    let resolver = Arc::new(ReflectorBackedResolver {
        live_node_names: HashSet::from(["live-node".to_string()]),
        resolved_node_names: Mutex::new(Vec::new()),
    });
    Ok(GhostFanoutFixture {
        live_node_id,
        ghost_node_id,
        claim,
        resolver,
        db,
    })
}

#[rstest]
#[tokio::test(flavor = "current_thread")]
async fn fanout_resolves_live_nodes_once_and_dead_letters_ghosts_without_resolving(
    #[future] ghost_fanout_fixture: TestDbResult<GhostFanoutFixture>,
) -> TestDbResult<()> {
    let fixture = ghost_fanout_fixture.await?;
    let GhostFanoutFixture {
        live_node_id,
        ghost_node_id,
        claim,
        resolver,
        db,
    } = &fixture;

    let value_capture = pplx_space_metrics::test_capture::capture_values_guard();
    process_claim(
        &db.pool,
        resolver.clone(),
        Arc::new(TriggerNodeApiClientCache::default()),
        claim.clone(),
        2,
    )
    .await;
    let captured_values = value_capture.finish();

    let batch_sizes = captured_values
        .iter()
        .filter(|value| value.name == "reconcile.trigger.fanout.batch.claimed")
        .map(|value| value.value)
        .collect::<Vec<_>>();
    assert_eq!(batch_sizes, vec![2.0]);
    assert_eq!(
        captured_values
            .iter()
            .filter(|value| value.name == "reconcile.trigger.fanout.batch.duration_ms")
            .count(),
        1
    );
    assert_eq!(
        *resolver.resolved_node_names.lock().await,
        vec!["live-node".to_string()]
    );
    let states = sqlx::query_as::<_, (NodeId, bool, String)>(pplx_space_common_core::sql!(
        "SELECT node_id, dead_at IS NOT NULL, dead_reason
         FROM node_reconcile_triggers
         WHERE node_id = ANY($1)"
    ))
    .bind([*live_node_id, *ghost_node_id])
    .fetch_all(&db.pool)
    .await?
    .into_iter()
    .map(|(node_id, dead, reason)| (node_id, (dead, reason)))
    .collect::<BTreeMap<_, _>>();
    assert!(states[live_node_id].0);
    assert!(states[live_node_id].1.contains("invalid node-api endpoint"));
    assert!(states[ghost_node_id].0);
    assert!(
        states[ghost_node_id]
            .1
            .contains("live Kubernetes node cache")
    );
    Ok(())
}
