//! `GetNodeRuntimeDesiredState`: whole-node runtime intent. It carries the
//! cluster's execution environment policy behind a setting each cluster API
//! replica reads at startup, off by default, and no provisioning recipe yet.

use pplx_space_runtime_environment_policy_store as policy_store;
use tonic::{Request, Response, Status};

use crate::ClusterApiService;
use crate::proto::{
    GetNodeRuntimeDesiredStateRequest, GetNodeRuntimeDesiredStateResponse,
    RuntimeEnvironmentDesiredPolicy,
};
use crate::status::optional_db_role_from_proto;

/// Serves the cluster policy to nodes when truthy. Off by default, and while
/// it is off nodes keep their startup settings.
const RUNTIME_DESIRED_STATE_ENABLED_ENV: &str = "SPACE_INTERNAL_API_RUNTIME_DESIRED_STATE_ENABLED";

/// Whether `GetNodeRuntimeDesiredState` returns the cluster policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RuntimeDesiredState {
    Disabled,
    Enabled,
}

impl RuntimeDesiredState {
    pub(crate) fn from_env() -> Self {
        let raw = std::env::var(RUNTIME_DESIRED_STATE_ENABLED_ENV).ok();
        let state = if pplx_space_env_vars::env_flag(raw.as_deref(), false) {
            Self::Enabled
        } else {
            Self::Disabled
        };
        // Replicas decide independently, and an unrecognized value reads as
        // off, so record each replica's decision next to what it was given.
        tracing::info!(
            setting = RUNTIME_DESIRED_STATE_ENABLED_ENV,
            raw = raw.as_deref().unwrap_or("<unset>"),
            serves_execution_policy = state == Self::Enabled,
            "runtime desired state execution policy"
        );
        state
    }
}

impl ClusterApiService {
    pub(crate) async fn node_runtime_desired_state(
        &self,
        request: Request<GetNodeRuntimeDesiredStateRequest>,
    ) -> Result<Response<GetNodeRuntimeDesiredStateResponse>, Status> {
        let role_hint = optional_db_role_from_proto(request.get_ref().role)?;
        self.authenticated_origin_node_id(&request, &request.get_ref().node_id, role_hint)
            .await?;
        let execution_policy = match self.runtime_desired_state {
            RuntimeDesiredState::Disabled => None,
            RuntimeDesiredState::Enabled => {
                // Any read failure, an unknown stored state included, fails
                // the whole call rather than omitting the field: an absent
                // policy tells a node to fall back to its startup settings,
                // which could undo a block it has already applied.
                // The detail, which can quote a stored value, stays in the log.
                let policy = policy_store::get(&self.pool).await.map_err(|error| {
                    let source = std::error::Error::source(&error)
                        .map(ToString::to_string)
                        .unwrap_or_default();
                    tracing::error!(%error, %source, "read runtime environment policy");
                    Status::internal("read runtime environment policy failed")
                })?;
                Some(desired_policy(&policy))
            }
        };
        // No provisioning recipe store is configured. Absence must not become
        // an implicit E0 installation or permission to delete existing files.
        Ok(Response::new(GetNodeRuntimeDesiredStateResponse {
            recipe: None,
            execution_policy,
        }))
    }
}

fn desired_policy(
    policy: &policy_store::RuntimeEnvironmentPolicy,
) -> RuntimeEnvironmentDesiredPolicy {
    RuntimeEnvironmentDesiredPolicy {
        generation: policy.generation,
        default_environment_ref: policy
            .default_environment
            .map(|reference| reference.to_string()),
        executing_environment_refs: policy
            .executing()
            .map(|reference| reference.to_string())
            .collect(),
        legacy_unbound_allowed: policy.legacy_unbound_allowed,
    }
}
