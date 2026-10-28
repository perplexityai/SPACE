use pplx_space_common_core::types::SandboxId;

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
