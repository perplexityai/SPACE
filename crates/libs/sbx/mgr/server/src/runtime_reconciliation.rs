use pplx_space_common_core::types::{
    SandboxId, SandboxQuarantineNextAction, SandboxQuarantineReasonCode, SandboxQuarantineSource,
};
use pplx_space_node_runtime_core::vm_state::VmState;

use crate::node_state_sync::mark_sandbox_dirty;
use crate::state::SandboxState;

#[derive(Debug)]
pub(crate) enum VmStateObservation {
    Observed(VmState),
    Unobservable(String),
}

impl VmStateObservation {
    pub(crate) fn state(&self) -> Option<VmState> {
        match self {
            Self::Observed(state) => Some(*state),
            Self::Unobservable(_) => None,
        }
    }

    pub(crate) fn unobservable_reason(&self) -> Option<&str> {
        match self {
            Self::Observed(_) => None,
            Self::Unobservable(reason) => Some(reason),
        }
    }
}

#[derive(Clone, Copy, Debug, thiserror::Error)]
pub(crate) enum ResidentObservationError {
    #[error("runtime state could not be observed")]
    Unobservable,
    #[error("runtime observations did not establish a stable resident state")]
    Unstable,
}

/// At most three bounded probes, 100 ms apart; only two consecutive resident
/// observations establish Running or Paused. NotStarted or Shutdown is affirmative evidence
/// that resident memory is unavailable and returns immediately: a later probe
/// failure must not erase that evidence. Interpretation of lifecycle intent
/// belongs to the caller. Peer-controlled error text is never recovery evidence.
pub(crate) async fn observe_stable_resident_state<F, Fut>(
    sandbox_id: SandboxId,
    mut probe: F,
) -> Result<VmState, ResidentObservationError>
where
    F: FnMut(SandboxId) -> Fut,
    Fut: std::future::Future<Output = VmStateObservation>,
{
    let mut previous = None;
    let mut error = ResidentObservationError::Unstable;
    for attempt in 0..3 {
        let current = match probe(sandbox_id).await {
            VmStateObservation::Observed(VmState::Running) => Some(VmState::Running),
            VmStateObservation::Observed(VmState::Paused) => Some(VmState::Paused),
            VmStateObservation::Observed(state @ (VmState::NotStarted | VmState::Shutdown)) => {
                return Ok(state);
            }
            VmStateObservation::Unobservable(_) => {
                error = ResidentObservationError::Unobservable;
                None
            }
        };
        if let Some(current) = current {
            if Some(current) == previous {
                return Ok(current);
            }
            error = ResidentObservationError::Unstable;
        }
        previous = current;
        if attempt < 2 {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }
    Err(error)
}

pub(crate) async fn observe_vm_state(
    sandbox_id: &SandboxId,
    probe_context: &'static str,
    node_manager: &crate::node_manager_transport::NodeManagerConnector,
) -> VmStateObservation {
    match crate::runtime_via_nodemgr::get_vm_state(*sandbox_id, node_manager).await {
        Ok(state) => VmStateObservation::Observed(state),
        Err(error) => {
            let reason = format!("runtime state probe failed: {error}");
            tracing::warn!(
                sandbox_id = %sandbox_id,
                probe_context,
                %error,
                "failed to observe runtime VM state"
            );
            VmStateObservation::Unobservable(reason)
        }
    }
}

pub(crate) fn mark_quarantined_best_effort(
    db: &crate::db::SharedConnection,
    sandbox_id: &str,
    expected_state: SandboxState,
    reason_code: SandboxQuarantineReasonCode,
    reason: &str,
) -> bool {
    let updated = {
        let Ok(db) = crate::node_state_sync::connection_or_signal_failure(db) else {
            return false;
        };
        match crate::db::mark_quarantined_from_state(
            &db,
            sandbox_id,
            expected_state,
            SandboxQuarantineSource::SbxManager,
            reason_code,
            reason,
            SandboxQuarantineNextAction::RequiresOperatorAttention,
        ) {
            Ok(true) => true,
            Ok(false) => {
                tracing::warn!(
                    sandbox_id,
                    %expected_state,
                    %reason_code,
                    reason,
                    "sandbox quarantine write was superseded"
                );
                false
            }
            Err(error) => {
                tracing::warn!(
                    sandbox_id,
                    %expected_state,
                    %reason_code,
                    reason,
                    %error,
                    "failed to persist sandbox quarantine"
                );
                false
            }
        }
    };
    if updated {
        mark_sandbox_dirty(db, sandbox_id);
    }
    updated
}
