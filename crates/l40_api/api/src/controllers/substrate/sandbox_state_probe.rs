//! Reads of the sandbox states a node reports, used to verify before destroying.

use super::*;

/// Per-sandbox liveness/salvage summary observed by one node-api
/// `WorkerApi/SandboxState` probe (which proxies the on-node sbx-mgr `State`
/// RPC). Verify-before-destroy reconcilers use this to decide whether a stale
/// control-plane row still has a live VM or salvageable node-local state
/// behind it before taking any destructive action. Live observations carry
/// the node-local lifecycle status when it parses, so repair loops can tell a
/// stale control-plane `failed` row from a node-local `failed` row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObservedSandboxState {
    /// The Firecracker process is alive on the node — the VM is running.
    VmAlive {
        node_status: Option<SandboxLifecycle>,
    },
    /// Known to the on-node sbx-mgr, VM not running, and a current-instance
    /// local artifact pointer is recorded: the node holds salvageable state
    /// that may not be backed up to durable storage yet.
    DormantWithArtifact {
        node_status: Option<SandboxLifecycle>,
    },
    /// Known to the on-node sbx-mgr, VM not running, and no current-instance
    /// local artifact pointer.
    DormantNoArtifact {
        node_status: Option<SandboxLifecycle>,
    },
    /// The on-node sbx-mgr has no record of the sandbox (`missing_ids`).
    Missing,
}

impl ObservedSandboxState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::VmAlive { .. } => "vm_alive",
            Self::DormantWithArtifact { .. } => "dormant_with_artifact",
            Self::DormantNoArtifact { .. } => "dormant_no_artifact",
            Self::Missing => "missing",
        }
    }

    pub const fn node_status(self) -> Option<SandboxLifecycle> {
        match self {
            Self::VmAlive { node_status }
            | Self::DormantWithArtifact { node_status }
            | Self::DormantNoArtifact { node_status } => node_status,
            Self::Missing => None,
        }
    }
}

impl std::fmt::Display for ObservedSandboxState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Batch-probe the liveness of `sandbox_ids` on one node via the node-api
/// `WorkerApi/SandboxState` RPC. Read-only: nothing on the node changes.
///
/// Ids the node answered for (present or explicitly missing) appear in the
/// returned map; an id absent from the map means the node's answer did not
/// cover it (a malformed id echo, or a skew where the node returned neither
/// presence nor absence) and callers must treat it as inconclusive rather
/// than missing. The latency contract is the channel timeout the caller
/// configured on `client` — this function adds no waiting of its own.
pub async fn sandbox_states_with_client(
    client: &mut RuntimeClient,
    endpoint_addr: &str,
    sandbox_ids: &[SandboxId],
) -> Result<BTreeMap<SandboxId, ObservedSandboxState>, SubstrateError> {
    let response = client
        .sandbox_state(pplx_space_sbx_mgr_client::proto::StateRequest {
            sandbox_ids: sandbox_ids.iter().map(SandboxId::to_string).collect(),
        })
        .await
        .map_err(|err| SubstrateError::Unavailable {
            message: format!("node-api sandbox-state probe at {endpoint_addr} failed: {err}"),
        })?
        .into_inner();

    Ok(observed_sandbox_states(response))
}

pub(super) fn observed_sandbox_states(
    response: pplx_space_sbx_mgr_client::proto::StateResponse,
) -> BTreeMap<SandboxId, ObservedSandboxState> {
    let mut states = BTreeMap::new();
    for info in response.sandboxes {
        let Ok(sandbox_id) = info.sandbox_id.parse::<SandboxId>() else {
            // Not one of ours (sbx-mgr ids are UUIDs); leave it out of the
            // map so the caller classifies the requested id as inconclusive.
            continue;
        };
        let node_status = match info.observed_lifecycle() {
            Ok(status) => status.label().parse::<SandboxLifecycle>().ok(),
            Err(error) => {
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    lifecycle = ?info.lifecycle,
                    error = %error,
                    "node-api sandbox-state probe returned unparseable sandbox status"
                );
                // Unknown vocabulary is not evidence of a dead sandbox. Omit
                // it so the requested-id probe remains inconclusive.
                continue;
            }
        };
        let state = if info.runtime_alive {
            ObservedSandboxState::VmAlive { node_status }
        } else if info.current_local_artifact_id.is_some() {
            ObservedSandboxState::DormantWithArtifact { node_status }
        } else {
            ObservedSandboxState::DormantNoArtifact { node_status }
        };
        states.insert(sandbox_id, state);
    }
    for missing in response.missing_ids {
        if let Ok(sandbox_id) = missing.parse::<SandboxId>() {
            states.insert(sandbox_id, ObservedSandboxState::Missing);
        }
    }
    states
}
