//! `GetNodeRuntimeDesiredState`: whole-node runtime intent. It carries the
//! cluster's execution environment policy and a provisioning recipe, each
//! behind its own setting that every cluster API replica reads at startup,
//! off by default. The recipe is [`catalog_store::list_unretired`], served to
//! the nodes [`RuntimeProvisioningCohort`] admits.
//!
//! [`RuntimeProvisioningCohort`]: crate::runtime_provisioning_cohort::RuntimeProvisioningCohort

use pplx_space_common_core::types::NodeRole;
use pplx_space_runtime_environment_catalog_store as catalog_store;
use pplx_space_runtime_environment_policy_store as policy_store;
use tonic::{Request, Response, Status};

use crate::proto::{
    GetNodeRuntimeDesiredStateRequest, GetNodeRuntimeDesiredStateResponse,
    RuntimeEnvironmentDesiredPolicy, RuntimeProvisioningRecipe,
};
use crate::status::optional_db_role_from_proto;
use crate::{ClusterApiService, OriginNode};
use pplx_space_node_api_client::MAX_RECIPE_ENVIRONMENTS;

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
        let origin = self
            .authenticated_origin_node_id(&request, &request.get_ref().node_id, role_hint)
            .await?;
        let execution_policy = match self.runtime_desired_state {
            RuntimeDesiredState::Disabled => None,
            RuntimeDesiredState::Enabled => {
                // Any read failure, an unknown stored state included, fails
                // the whole call rather than omitting the field: an absent
                // policy tells a node to fall back to its startup settings,
                // which could undo a block it has already applied.
                let policy = policy_store::get(&self.pool).await.map_err(|error| {
                    log_store_read_failure(&error, "runtime environment policy");
                    Status::internal("read runtime environment policy failed")
                })?;
                Some(desired_policy(&policy))
            }
        };
        Ok(Response::new(GetNodeRuntimeDesiredStateResponse {
            recipe: self.provisioning_recipe(&origin).await,
            execution_policy,
        }))
    }

    /// `None` unless the node is in the cohort. Keyed on the registered role,
    /// not the request's hint: cluster nodes run no microVMs, so they're
    /// assigned no recipe; workers and builders both do, and get the same one.
    /// Absence never implies an E0 installation or permission to remove files.
    ///
    /// A catalog that can't be read, or that lists more than a recipe may,
    /// omits the recipe from this one response rather than failing the call,
    /// so it never withholds a policy read alongside it, such as a block. The
    /// read runs on `runtime_catalog_pool`, so a blocked one can't hold the
    /// connections the policy and authentication reads need. Omission costs
    /// only this response's installs: the node keeps what it has, and its
    /// pending installs, and asks again on the next tick.
    async fn provisioning_recipe(&self, origin: &OriginNode) -> Option<RuntimeProvisioningRecipe> {
        match origin.role {
            NodeRole::Cluster => return None,
            NodeRole::Worker | NodeRole::Builder => {}
        }
        if !self
            .runtime_provisioning_cohort
            .admits(&origin.display_id, origin.id)
        {
            return None;
        }
        let environments = match catalog_store::list_unretired(&self.runtime_catalog_pool).await {
            Ok(environments) => environments,
            Err(error) => {
                log_store_read_failure(&error, "runtime environment catalog");
                return None;
            }
        };
        if environments.len() > MAX_RECIPE_ENVIRONMENTS {
            // `required_environments` admits at most this many. Omitted rather
            // than trimmed, since trimming would pick which certified
            // environments to skip; a node would refuse the whole list anyway.
            tracing::error!(
                environments = environments.len(),
                limit = MAX_RECIPE_ENVIRONMENTS,
                "runtime environment catalog exceeds the recipe limit; omitting the recipe until environments are retired"
            );
            return None;
        }
        Some(RuntimeProvisioningRecipe {
            required_environments: environments
                .into_iter()
                .map(|reference| reference.to_string())
                .collect(),
        })
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

/// The detail can quote a stored value, so it goes to the log, never to the
/// node.
fn log_store_read_failure(error: &(dyn std::error::Error + 'static), what: &str) {
    let source = error.source().map(ToString::to_string).unwrap_or_default();
    tracing::error!(%error, %source, "read {what}");
}
