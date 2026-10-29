//! Per-scenario statement multisets from the real service on synthetic fixtures.

use std::collections::BTreeMap;

use anyhow::{Context, Result, ensure};
use clap::Parser;
use pplx_space_common_core::types::{
    Arch, ArtifactComponentKind, ArtifactComponentMetadata, ExecutionMetadata, PolicyAuthority,
};
use pplx_space_db_test_fixtures::{TestDb, pg_test_db};
use pplx_space_net_common::proto::{
    EgressDefaultAction, NetworkPolicySource, NetworkPolicyV1, NetworkPolicyVersion,
};
use pplx_space_node_api_client::proto::{
    ExecutionInventoryObservation, ExecutionInventoryUnavailable, NodeRole, NodeSchedulingStatus,
    PublishNodeStateRequest, PublishedNodeStatus, PublishedTemplateState,
    execution_inventory_observation::Outcome,
};
use pplx_space_publish_load::{Args, Node, service::LocalService, store};
use pplx_space_sbx_mgr_client::proto::{SnapshotDetail, SnapshotLocation, SnapshotType};
use pplx_space_state_reconciliation::NetworkPolicyTarget;
use serde::Serialize;

mod statement_inventory;

#[derive(Debug, Serialize)]
struct Scenario {
    name: ScenarioKind,
    statements: BTreeMap<String, i64>,
    rows: BTreeMap<&'static str, Vec<serde_json::Value>>,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "kebab-case")]
enum ScenarioKind {
    Empty,
    NodeStatus,
    ExecutionInventory,
    UnavailableExecutionInventory,
    EmptyExecutionInventory,
    Templates,
    RepeatedTemplates,
    TemplateComponents,
    ClearedTemplateComponents,
    TemplatePolicy,
    OmittedTemplatePolicy,
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
    activity_epoch_seconds: u64,
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
    let rows = store::rows(&db.pool).await?.into_iter().collect();
    Ok(Scenario {
        name,
        statements,
        rows,
    })
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
        for (kind, outcome, expected_rows) in [
            (
                ScenarioKind::ExecutionInventory,
                Outcome::Complete(statement_inventory::installed()),
                1,
            ),
            (
                ScenarioKind::UnavailableExecutionInventory,
                Outcome::Unavailable(ExecutionInventoryUnavailable {}),
                1,
            ),
            (
                ScenarioKind::EmptyExecutionInventory,
                Outcome::Complete(Default::default()),
                0,
            ),
        ] {
            let mut publication = request(node);
            publication.execution_observation = Some(ExecutionInventoryObservation {
                outcome: Some(outcome),
            });
            let scenario = capture(db, &service, kind, publication).await?;
            for table in [
                "node_runtime_installations",
                "node_runtime_guests",
                "node_runtime_state_formats",
                "node_artifact_components",
            ] {
                let rows = scenario
                    .rows
                    .get(table)
                    .with_context(|| format!("missing {table}"))?;
                ensure!(rows.len() == expected_rows, "unexpected {table} row count");
            }
            scenarios.push(scenario);
        }
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
        let execution = ExecutionMetadata::initial_firecracker(
            Arch::X86_64,
            vec![ArtifactComponentMetadata {
                kind: ArtifactComponentKind::Kernel,
                role: None,
                version: Some("6.1.155".parse()?),
                digest: Some(format!("sha256:{}", "42".repeat(32)).parse()?),
                format: Some("vmlinux".parse()?),
            }],
        );
        let mut component_report = templates.clone();
        component_report
            .templates
            .first_mut()
            .context("template missing")?
            .execution = Some((&execution).into());
        for (kind, publication, expected_rows) in [
            (ScenarioKind::TemplateComponents, component_report, 1),
            (
                ScenarioKind::ClearedTemplateComponents,
                templates.clone(),
                0,
            ),
        ] {
            let scenario = capture(db, &service, kind, publication).await?;
            let rows = scenario
                .rows
                .get("node_template_components")
                .context("missing components")?;
            ensure!(
                rows.len() == expected_rows,
                "unexpected template component row count",
            );
            scenarios.push(scenario);
        }
        let mut template_policy = templates.clone();
        let policy_state = template_policy
            .templates
            .first_mut()
            .context("template missing")?;
        policy_state.network_policy = Some(NetworkPolicyV1 {
            version: NetworkPolicyVersion::V1.into(),
            egress_default_action: EgressDefaultAction::Allow.into(),
            ..Default::default()
        });
        policy_state.network_policy_generation = 11;
        policy_state.network_policy_authority = PolicyAuthority::Cluster.as_proto_i32();
        for (kind, publication, failure_present) in [
            (ScenarioKind::TemplatePolicy, template_policy, false),
            (ScenarioKind::OmittedTemplatePolicy, templates.clone(), true),
        ] {
            store::seed_policy_failure(
                &db.pool,
                node.id.parse()?,
                NetworkPolicyTarget::template(
                    pplx_space_db_test_fixtures::publish_load::TEMPLATE_NAME.parse()?,
                ),
            )
            .await?;
            scenarios.push(capture(db, &service, kind, publication).await?);
            store::verify_template_policy_observation(
                &db.pool,
                node.id.parse()?,
                11,
                failure_present,
            )
            .await?;
        }
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
                        id: "synthetic inventory/description".to_owned(),
                        r#type: SnapshotType::Disk.into(),
                        state: "legacy-state".to_owned(),
                        timestamp: Some("legacy-capture-time".to_owned()),
                        source_node_id: Some("legacy-origin".to_owned()),
                        label: Some("synthetic label".to_owned()),
                        retention_tier: Some("manual".to_owned()),
                        vcpu: 2,
                        memory_mib: 512,
                        size_bytes: Some(4096),
                        template_id: Some("captured-template".to_owned()),
                        content_hash: Some("captured-content".to_owned()),
                        locations: vec![
                            SnapshotLocation {
                                location_type: "node".to_owned(),
                                node_id: Some(node.id.clone()),
                                node_address: Some("snapshot-node.example.test".to_owned()),
                                state: "available".to_owned(),
                                size_bytes: 4096,
                                ..Default::default()
                            },
                            SnapshotLocation {
                                location_type: "future-location".to_owned(),
                                volume_id: Some("synthetic-volume".to_owned()),
                                state: "future-state".to_owned(),
                                size_bytes: u64::MAX,
                                ..Default::default()
                            },
                        ],
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
                store::seed_policy_failure(
                    &db.pool,
                    node_id,
                    NetworkPolicyTarget::sandbox(sandbox_id),
                )
                .await?;
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
                        == [store::SnapshotInventoryRow {
                            snapshot_id: "synthetic inventory/description".to_owned(),
                            snapshot_type: "disk".to_owned(),
                            state: "legacy-state".to_owned(),
                            timestamp: Some("legacy-capture-time".to_owned()),
                            source_node_id: Some("legacy-origin".to_owned()),
                            label: Some("synthetic label".to_owned()),
                            retention_tier: Some("manual".to_owned()),
                            vcpu: 2,
                            memory_mib: 512,
                            size_bytes: 4096,
                            template_id: Some("captured-template".to_owned()),
                            content_hash: Some("captured-content".to_owned()),
                            locations: serde_json::json!([
                                {
                                    "location_type": "node",
                                    "node_id": node.id,
                                    "node_address": "snapshot-node.example.test",
                                    "volume_id": null,
                                    "state": "available",
                                    "size_bytes": 4096,
                                },
                                {
                                    "location_type": "future-location",
                                    "node_id": null,
                                    "node_address": null,
                                    "volume_id": "synthetic-volume",
                                    "state": "future-state",
                                    "size_bytes": u64::MAX,
                                },
                            ]),
                        }],
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
            activity_epoch_seconds: 0,
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
