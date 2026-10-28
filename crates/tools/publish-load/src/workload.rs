use std::time::Duration;

use anyhow::{Context, Result, ensure};
use pplx_space_db_test_fixtures::publish_load::TEMPLATE_NAME;
use pplx_space_node_api_client::proto::{
    NodeRole, NodeSchedulingStatus, PublishNodeStateRequest, PublishedNodeStatus,
    PublishedSandboxState, PublishedTemplateState, cluster_api_client::ClusterApiClient,
};
use pplx_space_publish_load::Node;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tonic::metadata::{Ascii, MetadataValue};
use tonic::transport::Channel;

use crate::Args;
use crate::service::LocalService;

pub(crate) struct WorkloadOutcome {
    pub durations: Vec<f64>,
    pub activity_epoch_seconds: i64,
}

fn request(node: &Node, step: u32, seed: u64) -> Result<PublishNodeStateRequest> {
    let mut request = PublishNodeStateRequest {
        node_id: node.id.clone(),
        role: Some(NodeRole::Worker.into()),
        ..Default::default()
    };
    match step % 20 {
        0 => {
            request.sandbox_inventory_complete = true;
            request.sandboxes.clone_from(&node.sandboxes);
        }
        1..=11 => {
            // The same seed and schedule select exactly the same rows across
            // builds. Slot zero is reserved for the one-time rare transition.
            let offset = usize::from(!(node.retire_after_rare && step > 1));
            let regular_count = node
                .sandboxes
                .len()
                .checked_sub(offset)
                .filter(|count| *count > 0)
                .context("ordinary sandboxes missing")?;
            let index = offset + ((u64::from(step) * 73 + seed) % regular_count as u64) as usize;
            request.sandboxes.push(
                node.sandboxes
                    .get(index)
                    .context("single sandbox missing")?
                    .clone(),
            );
        }
        12..=17 => {
            request.node_status = Some(PublishedNodeStatus {
                scheduling_status: NodeSchedulingStatus::Ready.into(),
                ..Default::default()
            });
        }
        _ => {
            request.template_inventory_complete = true;
            request.templates = vec![PublishedTemplateState {
                template: TEMPLATE_NAME.into(),
                present: true,
                runtime: Some("fc".into()),
                arch: Some("amd64".into()),
                ..Default::default()
            }];
        }
    }
    Ok(request)
}

async fn send(
    client: &mut ClusterApiClient<Channel>,
    token: &MetadataValue<Ascii>,
    request: PublishNodeStateRequest,
) -> Result<f64> {
    let expected = request.sandboxes.len() + request.templates.len();
    let mut request = tonic::Request::new(request);
    request
        .metadata_mut()
        .insert("authorization", token.clone());
    let start = Instant::now();
    let response = client.publish_node_state(request).await?.into_inner();
    ensure!(
        response.sandboxes_accepted + response.templates_accepted == expected as u64,
        "publication count mismatch: expected {expected}, got {} sandboxes and {} templates",
        response.sandboxes_accepted,
        response.templates_accepted
    );
    Ok(start.elapsed().as_secs_f64() * 1000.0)
}

fn advance_running_activity(
    sandboxes: &mut [PublishedSandboxState],
    step: u32,
    activity: prost_types::Timestamp,
) {
    for sandbox in sandboxes {
        // A frozen transition cannot report activity newer than its durable
        // artifact. Only live runtimes advance counters and activity clocks.
        if sandbox.status.as_deref() != Some("started") {
            continue;
        }
        sandbox.uptime = Some(format!("{}s", i64::from(step) * 3 + 1));
        sandbox.network_tx_bytes = Some(u64::from(step) * 1024);
        sandbox.network_rx_bytes = Some(u64::from(step) * 2048);
        sandbox.disk_usage_bytes = Some(1_048_576 + u64::from(step) * 4096);
        sandbox.last_access_at = Some(activity);
        sandbox.last_command_at = Some(activity);
    }
}

pub(crate) async fn prime(service: &LocalService, nodes: &[Node]) -> Result<()> {
    let mut clients = JoinSet::new();
    for node in nodes {
        let mut client = service.client();
        let token = service.token.clone();
        let request = request(node, 0, 0)?;
        clients.spawn(async move { send(&mut client, &token, request).await });
    }
    while let Some(result) = clients.join_next().await {
        result??;
    }
    Ok(())
}

pub(crate) async fn run(
    args: &Args,
    service: &LocalService,
    nodes: Vec<Node>,
) -> Result<WorkloadOutcome> {
    // Quantize the node clock once. Per-request wall-clock jitter would make
    // activity writes on the one-minute freshness boundary nondeterministic.
    let activity_epoch_seconds = jiff::Timestamp::now().as_second();
    let start = Instant::now();
    let mut tasks = JoinSet::new();
    for (index, mut node) in nodes.into_iter().enumerate() {
        let args = args.clone();
        let mut client = service.client();
        let token = service.token.clone();
        tasks.spawn(async move {
            let mut durations = Vec::new();
            for step in 0..args.minutes * 20 {
                let seconds = (f64::from(step) * 3.0 + index as f64 * 3.0 / f64::from(args.nodes))
                    * args.time_scale;
                tokio::time::sleep_until(start + Duration::try_from_secs_f64(seconds)?).await;
                let activity = prost_types::Timestamp {
                    seconds: activity_epoch_seconds + i64::from(step) * 3,
                    nanos: 0,
                };
                advance_running_activity(&mut node.sandboxes, step, activity);
                let mut publish = request(&node, step, args.seed)?;
                if step == 1 {
                    // One rare claim per node, ~0.9% of single-row updates in
                    // the standard ten-minute run. Never replay its receipt.
                    let persistent = node.sandboxes.first_mut().context("rare sandbox missing")?;
                    *persistent = node.rare.clone();
                    publish.sandboxes = vec![node.rare.clone()];
                    persistent.transition_receipt = None;
                }
                durations.push(send(&mut client, &token, publish).await?);
                if step == 1 && node.retire_after_rare {
                    // Successful suspend/rollback releases central ownership.
                    // Later full inventories must stop claiming that sandbox.
                    let retired_id = &node.rare.sandbox_id;
                    node.sandboxes.retain(|row| &row.sandbox_id != retired_id);
                }
            }
            Ok::<_, anyhow::Error>(durations)
        });
    }
    let mut durations = Vec::new();
    while let Some(result) = tasks.join_next().await {
        durations.extend(result??);
    }
    // Include the idle tail: per-minute CPU and background work use the full
    // requested wall interval rather than stopping at the last request.
    tokio::time::sleep_until(
        start + Duration::try_from_secs_f64(f64::from(args.minutes) * 60.0 * args.time_scale)?,
    )
    .await;
    Ok(WorkloadOutcome {
        durations,
        activity_epoch_seconds,
    })
}

#[cfg(test)]
mod tests;
