//! Per-scenario statement multisets from the real service on synthetic fixtures.

use std::collections::BTreeMap;

use anyhow::{Context, Result, ensure};
use clap::Parser;
use pplx_space_common_core::types::PolicyAuthority;
use pplx_space_db_test_fixtures::{TestDb, pg_test_db};
use pplx_space_net_common::proto::{
    EgressDefaultAction, NetworkPolicySource, NetworkPolicyV1, NetworkPolicyVersion,
};
use pplx_space_node_api_client::proto::{
    NodeRole, NodeSchedulingStatus, PublishNodeStateRequest, PublishedNodeStatus,
    PublishedTemplateState,
};
use pplx_space_publish_load::{Args, Node, service::LocalService, store};
use pplx_space_sbx_mgr_client::proto::{SnapshotDetail, SnapshotType};
use serde::Serialize;

#[derive(Debug, Serialize)]
struct Scenario {
    name: ScenarioKind,
    statements: BTreeMap<String, i64>,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "kebab-case")]
enum ScenarioKind {
    Empty,
    NodeStatus,
    Templates,
    RepeatedTemplates,
    AbsentTemplates,
    EmptyTemplateCensus,
    OrdinarySingle,
    FullInventory,
    NetworkPolicy,
    OmittedNetworkPolicy,
    SnapshotInventory,
    ClearSnapshotInventory,
    FailureEvidence,
    ClearFailureEvidence,
    Degraded,
    Absent,
    Stopped,
    RestoreCompleted,
    SuspendCompleted,
    ActivationRollback,
    PauseFailedRecovery,
    Quarantine,
    RepeatedQuarantine,
}

#[derive(Debug, Serialize)]
struct Report {
    schema: u32,
    seed: u64,
    scenarios: Vec<Scenario>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    ensure!(args.nodes == 5, "statement scenarios require --nodes 5");
    ensure!(args.sandboxes_per_node >= 2 && args.sandboxes_per_node < 1_000_000);
    ensure!(args.seed <= u64::from(u32::MAX));
    ensure!(!args.output.exists(), "output directory already exists");
    ensure!(std::env::var_os("SPACE_TEST_DEPS_OPTIONAL").is_none());
    pplx_space_rustls_provider::install_default_rustls_crypto_provider();
    store::verify_owned_cluster().await?;
    tokio::fs::create_dir_all(&args.output).await?;
    let db = pg_test_db().await.map_err(anyhow::Error::from_boxed)?;
    let result = run(&args, &db).await;
    db.pool.close().await;
    drop(db);
    result
}

fn request(node: &Node) -> PublishNodeStateRequest {
    PublishNodeStateRequest {
        node_id: node.id.clone(),
        role: Some(NodeRole::Worker.into()),
        ..Default::default()
    }
}

async fn send(service: &LocalService, request: PublishNodeStateRequest) -> Result<()> {
    let expected = request.sandboxes.len() + request.templates.len();
    let mut request = tonic::Request::new(request);
    request
        .metadata_mut()
        .insert("authorization", service.token.clone());
    let response = service
        .client()
        .publish_node_state(request)
        .await?
        .into_inner();
    ensure!(
        response.sandboxes_accepted + response.templates_accepted == expected as u64,
        "publication acceptance changed"
    );
    Ok(())
}

async fn capture(
    db: &TestDb,
    service: &LocalService,
    name: ScenarioKind,
    request: PublishNodeStateRequest,
) -> Result<Scenario> {
    let before = store::statement_calls(&db.pool).await?;
    send(service, request).await?;
    let after = store::statement_calls(&db.pool).await?;
    ensure!(
        before.keys().all(|query| after.contains_key(query)),
        "statement entries disappeared during capture"
    );
    let mut statements = BTreeMap::new();
    for (query, after) in after {
        let before = before.get(&query).copied().unwrap_or(0);
        ensure!(after >= before, "statement counters reset during capture");
        if after != before {
            statements.insert(query, after - before);
        }
    }
    ensure!(
        !statements.is_empty(),
        "scenario captured no service statements"
    );
    Ok(Scenario { name, statements })
}

async fn run(args: &Args, db: &TestDb) -> Result<()> {
    store::enable_statement_capture(&db.pool).await?;
    let nodes = store::seed(&db.pool, args).await?;
    let mut service = LocalService::start(args, &db.database_url).await?;
    let result = async {
        for node in &nodes {
            let mut full = request(node);
            full.sandbox_inventory_complete = true;
            full.sandboxes.clone_from(&node.sandboxes);
            send(&service, full).await?;
        }
        let mut scenarios = Vec::new();
        let node = nodes.first().context("ordinary node missing")?;
        scenarios.push(capture(db, &service, ScenarioKind::Empty, request(node)).await?);
        let mut status = request(node);
        status.node_status = Some(PublishedNodeStatus {
            scheduling_status: NodeSchedulingStatus::Ready.into(),
            ..Default::default()
        });
        scenarios.push(capture(db, &service, ScenarioKind::NodeStatus, status).await?);
        let mut templates = request(node);
        templates.template_inventory_complete = true;
        templates.templates.push(PublishedTemplateState {
            template: pplx_space_db_test_fixtures::publish_load::TEMPLATE_NAME.to_owned(),
            runtime: Some("fc".to_owned()),
            arch: Some("amd64".to_owned()),
            present: true,
            ..Default::default()
        });
        scenarios.push(capture(db, &service, ScenarioKind::Templates, templates.clone()).await?);
        let template_hashes = store::template_inventory_hashes(&db.pool, node.id.parse()?).await?;
        ensure!(
            template_hashes == [""],
            "template inventory was not accepted"
        );
        scenarios.push(
            capture(
                db,
                &service,
                ScenarioKind::RepeatedTemplates,
                templates.clone(),
            )
            .await?,
        );
        ensure!(
            store::template_inventory_hashes(&db.pool, node.id.parse()?).await? == template_hashes,
            "repeated template inventory changed release identity"
        );
        let mut absent_templates = templates.clone();
        absent_templates.template_inventory_complete = false;
        absent_templates
            .templates
            .first_mut()
            .context("template missing")?
            .present = false;
        scenarios.push(
            capture(
                db,
                &service,
                ScenarioKind::AbsentTemplates,
                absent_templates,
            )
            .await?,
        );
        ensure!(
            store::template_inventory_hashes(&db.pool, node.id.parse()?)
                .await?
                .is_empty(),
            "explicit absence left template inventory"
        );
        send(&service, templates).await?;
        ensure!(
            store::template_inventory_hashes(&db.pool, node.id.parse()?).await? == template_hashes,
            "complete-empty census setup did not restore inventory"
        );
        let mut empty_templates = request(node);
        empty_templates.template_inventory_complete = true;
        scenarios.push(
            capture(
                db,
                &service,
                ScenarioKind::EmptyTemplateCensus,
                empty_templates,
            )
            .await?,
        );
        ensure!(
            store::template_inventory_hashes(&db.pool, node.id.parse()?)
                .await?
                .is_empty(),
            "complete-empty census left template inventory"
        );
        let mut single = request(node);
        single.sandboxes.push(
            node.sandboxes
                .get(1)
                .context("ordinary sandbox missing")?
                .clone(),
        );
        scenarios.push(capture(db, &service, ScenarioKind::OrdinarySingle, single).await?);
        let mut full = request(node);
        full.sandbox_inventory_complete = true;
        full.sandboxes.clone_from(&node.sandboxes);
        scenarios.push(capture(db, &service, ScenarioKind::FullInventory, full).await?);
        for name in [
            ScenarioKind::NetworkPolicy,
            ScenarioKind::OmittedNetworkPolicy,
            ScenarioKind::SnapshotInventory,
            ScenarioKind::ClearSnapshotInventory,
            ScenarioKind::FailureEvidence,
            ScenarioKind::ClearFailureEvidence,
            ScenarioKind::Degraded,
            ScenarioKind::Absent,
        ] {
            let mut sandbox = node
                .sandboxes
                .get(1)
                .context("ordinary sandbox missing")?
                .clone();
            match name {
                ScenarioKind::NetworkPolicy => {
                    sandbox.observed_network_policy = Some(NetworkPolicyV1 {
                        version: NetworkPolicyVersion::V1.into(),
                        egress_default_action: EgressDefaultAction::Allow.into(),
                        ..Default::default()
                    });
                    sandbox.observed_network_policy_generation = 7;
                    sandbox.observed_network_policy_source = NetworkPolicySource::Override.into();
                    sandbox.observed_network_policy_authority =
                        PolicyAuthority::Cluster.as_proto_i32();
                }
                ScenarioKind::SnapshotInventory => {
                    sandbox.snapshots.push(SnapshotDetail {
                        id: "synthetic-inventory-snapshot".to_owned(),
                        r#type: SnapshotType::Disk.into(),
                        state: "ready".to_owned(),
                        size_bytes: Some(4096),
                        ..Default::default()
                    });
                }
                ScenarioKind::FailureEvidence => {
                    sandbox.status = Some("failed".to_owned());
                }
                ScenarioKind::Degraded => sandbox.state_degraded = true,
                ScenarioKind::Absent => sandbox.present = false,
                _ => {}
            }
            let sandbox_id = sandbox.sandbox_id.parse()?;
            let node_id = node.id.parse()?;
            if matches!(
                name,
                ScenarioKind::NetworkPolicy | ScenarioKind::OmittedNetworkPolicy
            ) {
                store::seed_policy_failure(&db.pool, node_id, sandbox_id).await?;
            }
            let mut partial = request(node);
            partial.sandboxes.push(sandbox);
            scenarios.push(capture(db, &service, name, partial).await?);
            match name {
                ScenarioKind::NetworkPolicy | ScenarioKind::OmittedNetworkPolicy => {
                    store::verify_policy_observation(
                        &db.pool,
                        node_id,
                        sandbox_id,
                        7,
                        matches!(name, ScenarioKind::OmittedNetworkPolicy),
                    )
                    .await?;
                }
                ScenarioKind::SnapshotInventory => ensure!(
                    store::snapshot_inventory(&db.pool, sandbox_id).await?
                        == [(
                            "synthetic-inventory-snapshot".to_owned(),
                            "disk".to_owned(),
                            "ready".to_owned(),
                            4096,
                        )],
                    "snapshot description did not persist"
                ),
                ScenarioKind::ClearSnapshotInventory => ensure!(
                    store::snapshot_inventory(&db.pool, sandbox_id)
                        .await?
                        .is_empty(),
                    "empty snapshot report did not clear its accepted scope"
                ),
                _ => {}
            }
        }
        for (node, name) in nodes.iter().zip([
            ScenarioKind::Stopped,
            ScenarioKind::RestoreCompleted,
            ScenarioKind::SuspendCompleted,
            ScenarioKind::ActivationRollback,
            ScenarioKind::PauseFailedRecovery,
        ]) {
            let mut rare = request(node);
            rare.sandboxes.push(node.rare.clone());
            scenarios.push(capture(db, &service, name, rare).await?);
        }
        store::verify_transitions(&db.pool, args).await?;
        let mut episode = None;
        for name in [ScenarioKind::Quarantine, ScenarioKind::RepeatedQuarantine] {
            let mut sandbox = node
                .sandboxes
                .get(1)
                .context("ordinary sandbox missing")?
                .clone();
            sandbox.status = Some("quarantined".to_owned());
            sandbox.failure_reason = Some("  synthetic quarantine evidence  ".to_owned());
            let sandbox_id = sandbox.sandbox_id.parse()?;
            let mut hold = request(node);
            hold.sandboxes.push(sandbox);
            scenarios.push(capture(db, &service, name, hold).await?);
            let observed = store::verify_quarantine(&db.pool, node.id.parse()?, sandbox_id).await?;
            if let Some(previous) = episode {
                ensure!(
                    previous == observed,
                    "repeat quarantine replaced the active episode"
                );
            }
            episode = Some(observed);
        }
        let report = Report {
            schema: 1,
            seed: args.seed,
            scenarios,
        };
        tokio::fs::write(
            args.output.join("statements.json"),
            serde_json::to_vec_pretty(&report)?,
        )
        .await?;
        Ok::<_, anyhow::Error>(())
    }
    .await;
    service.stop().await?;
    result
}
