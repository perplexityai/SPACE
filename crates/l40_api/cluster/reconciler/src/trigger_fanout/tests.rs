use pplx_space_accounts::NodeId;

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
        sqlx::query(pplx_space_sql_locator::sql!(
            "INSERT INTO nodes (id, role, managed_by, state)
             VALUES ($1, $2, $3, 'registered')"
        ))
        .bind(node_id)
        .bind(SpaceNodeRole::Worker)
        .bind(NodeManagedBy::Kubernetes)
        .execute(&db.pool)
        .await?;
        sqlx::query(pplx_space_sql_locator::sql!(
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
    let claim = TriggerQueueStore::new(db.pool.clone())
        .claim_triggers(2)
        .await?;
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

    let value_capture = pplx_space_metrics_emit::test_capture::capture_values_guard();
    process_claim(
        &TriggerQueueStore::new(db.pool.clone()),
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
    let states = sqlx::query_as::<_, (NodeId, bool, String)>(pplx_space_sql_locator::sql!(
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

/// Each reason's wire number, pinned here because no proto compatibility
/// check runs in CI. A node decodes the number a controller of any version
/// sends, so none may move or be reused.
#[test]
fn every_reason_encodes_to_its_wire_number() {
    let wire_numbers = [
        (ReconcileReason::NodeRegistered, 1),
        (ReconcileReason::NodeUpdated, 2),
        (ReconcileReason::ManualNodeStatusRefresh, 3),
        (ReconcileReason::ManualNodeReconcile, 4),
        (ReconcileReason::ManualNodeNetworkPolicyReconcile, 5),
        (ReconcileReason::NodeTemplatePulled, 6),
        (ReconcileReason::SandboxRegistered, 7),
        (ReconcileReason::SandboxPortExposureUpdated, 8),
        (ReconcileReason::SandboxNetworkPolicyUpdated, 9),
        (ReconcileReason::SandboxDesiredStateUpdated, 10),
        (ReconcileReason::TemplateNetworkPolicyUpdated, 11),
        (ReconcileReason::TemplateDesiredStateUpdated, 12),
        (ReconcileReason::TemplateBaseSnapshotPolicyUpdated, 13),
        (ReconcileReason::TemplatePoolPolicyUpdated, 14),
        (ReconcileReason::TemplatePoolPolicyDeleted, 15),
        (ReconcileReason::NodeSandboxInventoryPublished, 16),
        (ReconcileReason::NodeTemplateInventoryPublished, 17),
        (ReconcileReason::FirewallRulesChanged, 18),
    ];
    assert_eq!(
        wire_numbers.map(|(reason, _)| reason),
        ReconcileReason::ALL,
        "a reason without a row"
    );
    let numbers = wire_numbers.map(|(_, number)| number);
    assert!(!numbers.contains(&0), "0 is unspecified");
    assert_eq!(
        BTreeSet::from(numbers).len(),
        numbers.len(),
        "a reused number"
    );
    for (reason, number) in wire_numbers {
        assert_eq!(
            i32::from(proto_reconcile_reason(reason)),
            number,
            "{reason}"
        );
    }
}

/// A claimed trigger holding the firewall rules reason, alone or beside
/// another, becomes a request naming the claim's typed reasons and its
/// diagnostic text, and nothing else: the request has no field that could
/// carry rules, a revision or a report.
#[tokio::test(flavor = "current_thread")]
async fn a_claimed_firewall_rules_trigger_requests_only_a_wakeup() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let firewall_rules_only = NodeId::new();
    let mixed = NodeId::new();
    for node_id in [firewall_rules_only, mixed] {
        sqlx::query(pplx_space_sql_locator::sql!(
            "INSERT INTO nodes (id, role, managed_by, state)
             VALUES ($1, $2, $3, 'registered')"
        ))
        .bind(node_id)
        .bind(SpaceNodeRole::Worker)
        .bind(NodeManagedBy::Kubernetes)
        .execute(&db.pool)
        .await?;
    }
    enqueue_triggers(&db.pool, [mixed], ReconcileReason::NodeUpdated).await?;
    enqueue_triggers(
        &db.pool,
        [firewall_rules_only, mixed],
        ReconcileReason::FirewallRulesChanged,
    )
    .await?;

    let requests = TriggerQueueStore::new(db.pool.clone())
        .claim_triggers(10)
        .await?
        .triggers
        .iter()
        .map(|trigger| {
            (
                trigger.node_id,
                trigger_request(&TriggerReasonPayload::from(trigger)),
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        requests,
        BTreeMap::from([
            (
                firewall_rules_only,
                TriggerStateReconciliationRequest {
                    reasons: vec![18],
                    diagnostic_reason: "firewall_rules_changed".to_string(),
                },
            ),
            (
                mixed,
                TriggerStateReconciliationRequest {
                    reasons: vec![18, 2],
                    diagnostic_reason: "firewall_rules_changed,node_updated".to_string(),
                },
            ),
        ])
    );
    Ok(())
}
