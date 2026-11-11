//! The node state the admin read routes add to each node: cordon from the
//! substrate's Node cache, and the queued reconcile trigger and runtime
//! installations from Postgres.

use std::collections::HashMap;

use pplx_space_common_core::types::NodeId;
use pplx_space_common_core::types::admin::{
    AdminNodeReconcileTrigger, AdminNodeResponse, AdminNodeRuntimeInstallation,
};

use super::super::node_store::{AdminNodeStore, NodeRuntimeInstallationRow};
use super::super::support::{format_optional_time, format_time};
use crate::reconcile_trigger::QueuedTrigger;
use crate::routes::AppState;

/// Fill in the node state the read routes add to [`super::admin_node_response`]:
/// cordon from the substrate's Node cache, and the queued reconcile trigger
/// and runtime installations from Postgres, each looked up for the whole page
/// at once. Action responses skip it.
///
/// A failed Postgres read does not fail the route, which before these fields
/// depended only on the nodes query. Neither field is then set, and every
/// node carries the error as `read_state_error`, so a client can tell
/// "could not read" from "nothing queued".
pub(super) async fn attach_node_read_state(
    state: &AppState,
    store: &AdminNodeStore,
    responses: &mut [AdminNodeResponse],
) {
    let node_ids: Vec<NodeId> = responses.iter().map(|node| node.id).collect();
    let cordon_states = state.sandbox_controller.node_cordon_states();
    for node in responses.iter_mut() {
        node.cordoned = cordon_states
            .as_ref()
            .zip(node.k8s_node_name.as_deref())
            .and_then(|(states, name)| states.get(name).copied());
    }
    match read_page_state(state, store, &node_ids).await {
        Ok((mut triggers, mut installations)) => {
            for node in responses {
                node.reconcile_trigger = triggers.remove(&node.id);
                node.runtime_installations =
                    Some(installations.remove(&node.id).unwrap_or_default());
            }
        }
        Err(error) => {
            tracing::warn!(
                %error,
                nodes = node_ids.len(),
                "failed to read node reconcile triggers or runtime installations",
            );
            let error = error.to_string();
            for node in responses {
                node.read_state_error = Some(error.clone());
            }
        }
    }
}

type PageState = (
    HashMap<NodeId, AdminNodeReconcileTrigger>,
    HashMap<NodeId, Vec<AdminNodeRuntimeInstallation>>,
);

async fn read_page_state(
    state: &AppState,
    store: &AdminNodeStore,
    node_ids: &[NodeId],
) -> Result<PageState, sqlx::Error> {
    let triggers = crate::reconcile_trigger::queued_triggers(&state.pool, node_ids)
        .await?
        .into_iter()
        .map(|trigger| (trigger.node_id, admin_reconcile_trigger(trigger)))
        .collect();
    let installations =
        runtime_installations_by_node(store.runtime_installations_for_nodes(node_ids).await?);
    Ok((triggers, installations))
}

fn admin_reconcile_trigger(trigger: QueuedTrigger) -> AdminNodeReconcileTrigger {
    AdminNodeReconcileTrigger {
        reasons: trigger.reasons,
        requested_at: format_time(trigger.requested_at),
        next_attempt_at: format_time(trigger.next_attempt_at),
        attempts: trigger.attempts,
        claimed_at: format_optional_time(trigger.claimed_at),
        dead_at: format_optional_time(trigger.dead_at),
        dead_reason: trigger.dead_reason,
        last_error: trigger.last_error,
    }
}

/// Group guest rows into one installation each. Relies on the store's order:
/// a node's rows for one runtime version are adjacent.
fn runtime_installations_by_node(
    rows: Vec<NodeRuntimeInstallationRow>,
) -> HashMap<NodeId, Vec<AdminNodeRuntimeInstallation>> {
    let mut by_node: HashMap<NodeId, Vec<AdminNodeRuntimeInstallation>> = HashMap::new();
    for row in rows {
        let installations = by_node.entry(row.node_id).or_default();
        let same_installation = installations.last().is_some_and(|last| {
            last.runtime == row.runtime && last.runtime_version == row.runtime_version
        });
        if !same_installation {
            installations.push(AdminNodeRuntimeInstallation {
                runtime: row.runtime,
                runtime_version: row.runtime_version,
                guests: Vec::new(),
                observed_at: format_time(row.observed_at),
            });
        }
        if let (Some(os), Some(arch), Some(installation)) =
            (row.guest_os, row.guest_arch, installations.last_mut())
        {
            installation.guests.push(format!("{os}/{arch}"));
        }
    }
    by_node
}
