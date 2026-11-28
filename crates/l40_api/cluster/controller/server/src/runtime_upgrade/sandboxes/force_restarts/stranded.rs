use std::collections::{HashMap, HashSet};

use pplx_space_runtime_environment_policy_store::RuntimeEnvironmentState;
use pplx_space_runtime_upgrade_campaign_store::{self as store, Campaign};
use uuid::Uuid;

use super::{Inputs, MemoryRetirementSource, SandboxForceRestartDriver, SandboxUpgradeDispatcher};

impl<D: SandboxUpgradeDispatcher, M: MemoryRetirementSource> SandboxForceRestartDriver<D, M> {
    pub(super) async fn select_stranded_candidates(
        &self,
        campaign: &Campaign,
        scan_limit: u32,
    ) -> Result<Vec<Uuid>, String> {
        let inputs = Inputs::read(&self.pool).await?;
        let retirement = self.read_memory_retirement(campaign.from).await?;
        if inputs.policy.default_environment != Some(campaign.to)
            || inputs.policy.environments.get(&campaign.from)
                == Some(&RuntimeEnvironmentState::Allowed)
            || !retirement.retired
        {
            return Ok(Vec::new());
        }
        let mut selected = Vec::new();
        let mut seen = HashSet::new();
        let mut taken_on_node: HashMap<pplx_space_accounts::NodeId, u64> = HashMap::new();
        let mut excluded_nodes = Vec::new();
        let initial_cursor = self
            .stranded_cursors
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .get(&campaign.id)
            .copied()
            .flatten();
        let mut after = initial_cursor;
        let mut has_wrapped = after.is_none();
        let mut completed_ring = false;
        let mut gate_opened = false;
        let mut skipped_for_node_cap = false;
        let mut cursor_before_node_cap = None;
        let mut last_probed = None;
        if scan_limit > 0 {
            let page_limit = scan_limit as usize;
            let mut remaining_probe_budget = page_limit.saturating_mul(4);
            let mut goal = page_limit;
            while remaining_probe_budget > 0 {
                let request_limit = remaining_probe_budget.min(page_limit) as u32;
                let page = store::started_pending_target_sandboxes(
                    &self.pool,
                    campaign.id,
                    self.managed_by,
                    &excluded_nodes,
                    after,
                    request_limit,
                )
                .await
                .map_err(|error| {
                    format!("reading target-ready pending sandboxes failed: {error}")
                })?;
                if !page.gate_open {
                    break;
                }
                gate_opened = true;
                remaining_probe_budget = remaining_probe_budget.saturating_sub(page.probed);
                if let Some(first) = page.items.first() {
                    goal = page_limit
                        .min(usize::try_from(first.remaining_cluster_slots).unwrap_or(usize::MAX));
                    if goal == 0 {
                        gate_opened = false;
                        break;
                    }
                }
                for entry in page.items {
                    if has_wrapped && initial_cursor.is_some_and(|stop| entry.sandbox_id > stop) {
                        completed_ring = true;
                        break;
                    }
                    after = Some(entry.sandbox_id);
                    last_probed = after;
                    if let Some((node_id, remaining)) =
                        entry.node_id.zip(entry.remaining_node_slots)
                        && taken_on_node.get(&node_id).copied().unwrap_or(0) >= remaining
                    {
                        skipped_for_node_cap = true;
                        if !excluded_nodes.contains(&node_id) {
                            excluded_nodes.push(node_id);
                        }
                        continue;
                    }
                    let target_ready = entry.started_on_target
                        || entry.starting_on_target
                        || entry
                            .sandbox
                            .as_ref()
                            .and_then(|sandbox| sandbox.effective_execution())
                            .and_then(|execution| inputs.attributed(execution))
                            == Some(campaign.to);
                    if target_ready && seen.insert(entry.sandbox_id) {
                        if let Some((node_id, _remaining)) =
                            entry.node_id.zip(entry.remaining_node_slots)
                        {
                            *taken_on_node.entry(node_id).or_default() += 1;
                        }
                        if !skipped_for_node_cap {
                            cursor_before_node_cap = Some(entry.sandbox_id);
                        }
                        selected.push(entry.sandbox_id);
                        if selected.len() >= goal {
                            break;
                        }
                    }
                }
                if selected.len() >= goal || completed_ring {
                    break;
                }
                if let Some(window_end) = page.last_probed {
                    if has_wrapped && initial_cursor.is_some_and(|stop| window_end >= stop) {
                        completed_ring = true;
                        break;
                    }
                    after = Some(window_end);
                    last_probed = after;
                }
                if page.probed < request_limit as usize {
                    if !has_wrapped {
                        after = None;
                        has_wrapped = true;
                    } else {
                        completed_ring = true;
                        break;
                    }
                }
            }
            if gate_opened && goal > 0 {
                let saved_cursor = if !selected.is_empty() {
                    if skipped_for_node_cap {
                        cursor_before_node_cap.or_else(|| selected.first().copied())
                    } else {
                        selected.last().copied()
                    }
                } else if completed_ring {
                    None
                } else {
                    last_probed
                };
                self.stranded_cursors
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner())
                    .insert(campaign.id, saved_cursor);
            }
        }
        Ok(selected)
    }
}
