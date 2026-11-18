//! A start's statements for the v2 drain's stopped handoff: its read of the
//! row's handoff state, the handoff itself, and the landing of its
//! re-placement. The start claims a handed-off row with v1's evacuation claim
//! ([`K8sStore::begin_evacuation_restore_lifecycle`]).

use super::*;
use pplx_space_sandbox_store::drain_restore;

impl K8sStore {
    /// The row's handoff state, in one statement; `None` when it is gone.
    pub(in crate::controllers::substrate::kubernetes) async fn stopped_handoff_state(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<drain_restore::StoppedHandoffState>, SubstrateError> {
        drain_restore::stopped_handoff_state(&self.pool, sandbox_id)
            .await
            .db_context("failed to read a stopped sandbox's handoff state")
    }

    /// Hand stopped sandbox `sandbox_id` off from its home node `node_id`
    /// (`drain_restore::hand_off_stopped`), in a transaction of its own.
    pub(in crate::controllers::substrate::kubernetes) async fn hand_off_stopped(
        &self,
        sandbox_id: SandboxId,
        node_id: NodeId,
    ) -> Result<drain_restore::StoppedHandoff, SubstrateError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to begin a stopped sandbox's handoff")?;
        let done = drain_restore::hand_off_stopped(&mut tx, sandbox_id, node_id)
            .await
            .db_context("failed to hand off a stopped sandbox")?;
        tx.commit()
            .await
            .db_context("failed to commit a stopped sandbox's handoff")?;
        Ok(done)
    }

    /// Land the re-placement of handed-off stopped row `sandbox_id` that
    /// this start's claim drove as execution `execution_generation`
    /// (`drain_restore::finalize_stopped_handoff_restore`), in a transaction
    /// of its own. `false` when the row moved, or a later claim's execution
    /// replaced this one.
    pub(in crate::controllers::substrate::kubernetes) async fn land_handed_off_stopped(
        &self,
        sandbox_id: SandboxId,
        execution_generation: i64,
    ) -> Result<bool, SubstrateError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to begin a re-placed stopped sandbox's landing")?;
        let landed = drain_restore::finalize_stopped_handoff_restore(
            &mut tx,
            sandbox_id,
            execution_generation,
        )
        .await
        .db_context("failed to land a re-placed stopped sandbox")?;
        tx.commit()
            .await
            .db_context("failed to commit a re-placed stopped sandbox's landing")?;
        Ok(landed)
    }
}
