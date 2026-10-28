use tonic::Status;

use super::fields::u32_to_i32;
use crate::proto::{NodeCtlStatus, NodeSchedulingStatus, PublishedNodeStatus};

#[derive(Debug)]
pub(crate) struct ValidatedNodeStatus {
    scheduling_status: NodeSchedulingStatus,
    pub(crate) scheduling_reason: String,
    pub(crate) network_slots_ready: i32,
    pub(crate) network_pool_min_ready: i32,
    pub(crate) network_slots_degraded: i32,
    pub(crate) network_slots_quarantined: i32,
    pub(crate) network_slots_total: i32,
    pub(crate) network_slots_leased: i32,
    pub(crate) network_pool_max_slots: i32,
    pub(crate) node_ctl: Option<serde_json::Value>,
}

impl ValidatedNodeStatus {
    pub(crate) fn try_from_proto(status: PublishedNodeStatus) -> Result<Self, Status> {
        Ok(Self {
            // Older readers have always retained unknown scheduling values as
            // unspecified evidence, not rejected the surrounding publication.
            scheduling_status: NodeSchedulingStatus::try_from(status.scheduling_status)
                .unwrap_or_default(),
            scheduling_reason: status.scheduling_reason,
            network_slots_ready: u32_to_i32(status.network_slots_ready, "network_slots_ready")?,
            network_pool_min_ready: u32_to_i32(
                status.network_pool_min_ready,
                "network_pool_min_ready",
            )?,
            network_slots_degraded: u32_to_i32(
                status.network_slots_degraded,
                "network_slots_degraded",
            )?,
            network_slots_quarantined: u32_to_i32(
                status.network_slots_quarantined,
                "network_slots_quarantined",
            )?,
            network_slots_total: u32_to_i32(status.network_slots_total, "network_slots_total")?,
            network_slots_leased: u32_to_i32(status.network_slots_leased, "network_slots_leased")?,
            network_pool_max_slots: u32_to_i32(
                status.network_pool_max_slots,
                "network_pool_max_slots",
            )?,
            node_ctl: status.node_ctl.map(node_ctl_status_json),
        })
    }

    pub(crate) fn scheduling_status_label(&self) -> &'static str {
        match self.scheduling_status {
            NodeSchedulingStatus::Ready => "ready",
            NodeSchedulingStatus::Warming => "warming",
            NodeSchedulingStatus::Draining => "draining",
            NodeSchedulingStatus::Degraded => "degraded",
            NodeSchedulingStatus::Empty => "empty",
            NodeSchedulingStatus::Unavailable => "unavailable",
            NodeSchedulingStatus::Full => "full",
            NodeSchedulingStatus::Unspecified => "unknown",
        }
    }
}

fn node_ctl_status_json(status: NodeCtlStatus) -> serde_json::Value {
    serde_json::json!({
        "reachable": status.reachable,
        "error": status.error,
        "reconciliations": status
            .reconciliations
            .into_iter()
            .map(|status| {
                serde_json::json!({
                    "name": status.name,
                    "poll_interval_secs": status.poll_interval_secs,
                    "last_started_at_unix_secs": status.last_started_at_unix_secs,
                    "last_completed_at_unix_secs": status.last_completed_at_unix_secs,
                    "last_success_at_unix_secs": status.last_success_at_unix_secs,
                    "next_due_at_unix_secs": status.next_due_at_unix_secs,
                    "running": status.running,
                    "templates_seen": status.templates_seen,
                    "sandboxes_seen": status.sandboxes_seen,
                    "templates_applied": status.templates_applied,
                    "sandboxes_applied": status.sandboxes_applied,
                    "last_error": status.last_error,
                })
            })
            .collect::<Vec<_>>(),
    })
}

#[cfg(test)]
mod tests;
