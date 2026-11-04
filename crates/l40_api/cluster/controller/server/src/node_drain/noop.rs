//! Inert [`InstanceTerminator`] for environments without a real
//! provider termination story (kind/Tilt, CI, devbox, smoke tests).
//!
//! Returns `Ok(())` unconditionally. The reconciler then proceeds to
//! `DELETE FROM nodes WHERE id = $1 AND drain_phase = 'terminating'`
//! via `delete_terminating_node`, so the SPACE state machine
//! still completes — the underlying compute simply isn't destroyed.
//!
//! Use when you want to exercise the `terminating -> deleted`
//! transition end-to-end without affecting infrastructure. In kind,
//! the docker-worker container keeps running; the kube Node retains
//! whatever cordon/labels were applied by `node_drain`. Cleanup of
//! those side effects is the operator's (or test harness's)
//! responsibility — typically a `kubectl uncordon` after the test.

use pplx_space_common_core::types::NodeId;

use super::{InstanceTerminator, TerminateError};

#[derive(Debug, Default, Clone, Copy)]
pub struct NoopInstanceTerminator;

impl NoopInstanceTerminator {
    pub fn new() -> Self {
        Self
    }
}

impl InstanceTerminator for NoopInstanceTerminator {
    async fn terminate(&self, node_id: NodeId) -> Result<(), TerminateError> {
        tracing::info!(
            node_id = %node_id,
            "node_drain: NoopInstanceTerminator returning Ok without touching infrastructure"
        );
        Ok(())
    }
}

#[cfg(test)]
mod noop_tests;
