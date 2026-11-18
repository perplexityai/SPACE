//! Gentle drains: the hold, and the turn to forced.

use super::*;

/// The `pending` action a gentle drain's node reports for a sandbox it
/// holds running.
pub(super) const HOLD_ACTION: &str = "hold";

/// Whether `report`, a stored `drain_status.report`, is the node's gentle
/// answer sealed under `epoch` that lists nothing but held sandboxes: every
/// `pending` entry's action is `hold`, and `blocked` and `unrecorded` are
/// empty. A report of any other shape, or none, isn't.
pub fn report_holds_only(report: Option<&serde_json::Value>, epoch: Option<u64>) -> bool {
    let Some(report) = report else {
        return false;
    };
    let entries = |key: &str| report.get(key).and_then(serde_json::Value::as_array);
    epoch.is_some()
        && report
            .get("sealed_epoch")
            .and_then(serde_json::Value::as_u64)
            == epoch
        && report.get("gentle").and_then(serde_json::Value::as_bool) == Some(true)
        && entries("blocked").is_some_and(Vec::is_empty)
        && entries("unrecorded").is_some_and(Vec::is_empty)
        && entries("pending").is_some_and(|pending| {
            pending.iter().all(|entry| {
                entry.get("action").and_then(serde_json::Value::as_str) == Some(HOLD_ACTION)
            })
        })
}

impl V2Node {
    /// A gentle drain that isn't due to turn forced, whose stored report
    /// under its epoch lists only held sandboxes: the node suspends and
    /// uploads nothing for it, so it needs no slot.
    pub(super) fn only_holding(&self) -> bool {
        self.gentle && !self.mode_forced && !self.deadline_due && self.report_holds_only
    }

    /// A gentle `draining` drain due to turn forced: the database clock has
    /// reached its deadline, or an operator escalated it.
    pub(super) fn escalation_due(&self) -> bool {
        self.gentle
            && self.phase.as_deref() == Some("draining")
            && (self.mode_forced || self.deadline_due)
    }
}

impl DrainCallError {
    /// The refusal of a sandbox-manager that predates gentle drains, or of
    /// one behind a node-api relay that drops `gentle_epoch`: it reads the
    /// gentle call's `epoch` 0 and refuses it, before it seals or suspends
    /// anything, with its epoch check's `InvalidArgument` "epoch must be
    /// nonzero ...". A node with gentle drains never answers that to a
    /// gentle call.
    pub(super) fn refused_by_an_older_node(&self) -> bool {
        self.code == Some(tonic::Code::InvalidArgument)
            && self.status_message.starts_with("epoch must be nonzero")
    }
}

/// Turn a gentle drain due to turn forced ([`V2Node::escalation_due`]).
/// Returns whether the drain still runs gently.
pub(super) async fn escalate<S: DrainV2Sink>(sink: &S, node: &V2Node, epoch: u64) -> bool {
    if !node.escalation_due() {
        return node.gentle;
    }
    match sink.escalate(node.id, epoch).await {
        Ok(true) => {
            tracing::info!(
                node_id = %node.id,
                epoch,
                deadline = node.deadline,
                by_operator = node.mode_forced,
                "node_drain v2: the gentle drain turned forced"
            );
            false
        }
        // The row moved on: the next tick reads it again, and a call this
        // tick stays gentle, which suspends no running sandbox.
        Ok(false) => true,
        Err(error) => {
            sink.record_error(
                node.id,
                &format!("turning the gentle drain forced failed: {error}"),
            )
            .await;
            true
        }
    }
}
