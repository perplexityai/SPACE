use pplx_space_common_core::types::{SandboxId, SandboxLifecycle, SandboxTransitionReceiptKind};

/// A healthy runtime claim that may retire a stored pause-failure quarantine.
#[derive(Clone, Copy, Debug, serde::Serialize)]
pub(crate) struct PauseFailedRecoveryClaim {
    pub(crate) sandbox_id: SandboxId,
    pub(crate) status: SandboxLifecycle,
}

/// A started report that may complete a restore if the stored row still permits it.
#[derive(Clone, Copy, Debug, serde::Serialize)]
#[serde(tag = "status", rename = "started")]
pub(crate) struct RestoreClaim {
    pub(crate) sandbox_id: SandboxId,
    pub(crate) transition_receipt_kind: Option<SandboxTransitionReceiptKind>,
}

/// A node reported a stopped runtime, not permission to change its database row.
#[derive(Clone, Copy, Debug, serde::Serialize)]
#[serde(tag = "status", rename = "stopped")]
pub(crate) struct StoppedClaim {
    pub(crate) sandbox_id: SandboxId,
}

/// A node reported a failed runtime; ownership and source lifecycle remain guards.
#[derive(Clone, Copy, Debug, serde::Serialize)]
#[serde(tag = "status", rename = "failed")]
pub(crate) struct FailedClaim {
    pub(crate) sandbox_id: SandboxId,
}
