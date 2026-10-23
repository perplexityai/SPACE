//! Whole-node installation reconciliation. Recipes never select an environment
//! or remove bytes; execution eligibility and usable inventory remain separate.

use std::{collections::BTreeSet, future::Future, time::Duration};

use futures_util::{StreamExt, stream};
use pplx_space_common_core::types::{
    Arch, BootProfile, DigestParseError, EnvironmentReference, HostPlatform, RuntimeBinding,
};
use pplx_space_node_api_client::proto::{
    GetNodeRuntimeDesiredStateRequest, GetNodeRuntimeDesiredStateResponse, NodeRole,
    RuntimeProvisioningRecipe,
};
use pplx_space_node_runtime_core::catalog::{CatalogError, CompiledEnvironment};
use pplx_space_node_runtime_manager_client::{RuntimeManagerGrpcClient, proto as runtime};
use tonic::{Code, Status};

const RECONCILE_BUDGET: Duration = Duration::from_secs(30);
const MAX_ENVIRONMENTS: usize = 32;
const SUBMISSION_CONCURRENCY: usize = 4;

#[derive(Debug, thiserror::Error)]
pub(super) enum ProvisioningError {
    #[error("runtime provisioning exceeded its {RECONCILE_BUDGET:?} tick budget")]
    Timeout,
    #[error("fetch runtime recipe: {0}")]
    Fetch(#[source] Status),
    #[error("connect cluster API: {0}")]
    ClusterConnect(#[source] crate::ClusterApiConnectError),
    #[error("connect runtime-manager: {0}")]
    RuntimeConnect(#[from] pplx_space_node_runtime_manager_client::RuntimeManagerConnectError),
    #[error("unsupported provisioning host: {0}")]
    Host(&'static str),
    #[error("load compiled environment catalog: {0}")]
    Catalog(#[source] &'static CatalogError),
    #[error("recipe contains more than {MAX_ENVIRONMENTS} environments")]
    TooLarge,
    #[error("invalid environment reference: {0}")]
    Reference(#[from] DigestParseError),
    #[error("duplicate environment reference {0}")]
    Duplicate(EnvironmentReference),
    #[error("environment {0} is not compiled for this host")]
    Unknown(EnvironmentReference),
    #[error("prepare installation plan: {0}")]
    PlanTask(#[from] tokio::task::JoinError),
    #[error("list installed artifacts: {0}")]
    Inventory(#[source] Status),
    #[error("artifact inventory is incomplete; no installations submitted")]
    IncompleteInventory,
    #[error(
        "runtime provisioning pending: {missing} artifacts missing, {failed} submissions failed"
    )]
    Pending { missing: usize, failed: usize },
}

/// Bounded across cluster lookup, local discovery and submissions. Downloads
/// continue in rtmgr's durable worker after this tick or its caller is cancelled.
/// The next periodic tick reads fresh intent and inventory, including after a
/// restart or a lost reply. No recipe is cached or replayed on fetch failure.
pub(super) async fn refresh(
    node_id: &str,
    node_role: Option<NodeRole>,
) -> Result<(), ProvisioningError> {
    if node_role == Some(NodeRole::Cluster) {
        return Ok(());
    }
    tokio::time::timeout(RECONCILE_BUDGET, async {
        let mut cluster = crate::connect_cluster_api()
            .await
            .map_err(ProvisioningError::ClusterConnect)?;
        let response = cluster
            .get_node_runtime_desired_state(GetNodeRuntimeDesiredStateRequest {
                node_id: node_id.to_owned(),
                role: node_role.map(|role| role as i32),
            })
            .await
            .map(tonic::Response::into_inner);
        let Some(recipe) = recipe_from_response(response)? else {
            return Ok(());
        };
        // An explicit empty recipe is additive: it performs no local work.
        if recipe.required_environments.is_empty() {
            return Ok(());
        }
        let plan = tokio::task::spawn_blocking(move || {
            let architecture: Arch = std::env::consts::ARCH
                .parse()
                .map_err(|_| ProvisioningError::Host(std::env::consts::ARCH))?;
            let os = std::env::consts::OS
                .parse()
                .map_err(|_| ProvisioningError::Host(std::env::consts::OS))?;
            let catalog = CompiledEnvironment::cached_for_architecture(architecture)
                .map_err(ProvisioningError::Catalog)?;
            InstallationPlan::new(recipe, HostPlatform { os, architecture }, catalog)
        })
        .await??;
        let manager = Manager(pplx_space_node_runtime_manager_client::connect().await?);
        reconcile(&plan, &manager).await
    })
    .await
    .map_err(|_| ProvisioningError::Timeout)?
}

fn recipe_from_response(
    response: Result<GetNodeRuntimeDesiredStateResponse, Status>,
) -> Result<Option<RuntimeProvisioningRecipe>, ProvisioningError> {
    match response {
        Ok(response) => Ok(response.recipe),
        // An older cluster has no recipe authority. Do not infer E0 or an
        // empty applied recipe from this compatibility path.
        Err(error) if error.code() == Code::Unimplemented => Ok(None),
        Err(error) => Err(ProvisioningError::Fetch(error)),
    }
}

struct InstallationPlan {
    artifacts: Vec<runtime::ArtifactIdentity>,
}

impl InstallationPlan {
    fn new(
        recipe: RuntimeProvisioningRecipe,
        host: HostPlatform,
        catalog: &[CompiledEnvironment],
    ) -> Result<Self, ProvisioningError> {
        if recipe.required_environments.len() > MAX_ENVIRONMENTS {
            return Err(ProvisioningError::TooLarge);
        }
        let mut references = BTreeSet::new();
        let mut artifacts = Vec::new();
        for raw in recipe.required_environments {
            let reference = raw.parse()?;
            if !references.insert(reference) {
                return Err(ProvisioningError::Duplicate(reference));
            }
            let environment = catalog
                .iter()
                .find(|entry| entry.definition().reference() == reference)
                .filter(|entry| entry.definition().descriptor().spec().host == host)
                .ok_or(ProvisioningError::Unknown(reference))?;
            let spec = environment.definition().descriptor().spec();
            let RuntimeBinding::ManagedRelease(release) = &spec.runtime;
            let BootProfile::LinuxDirectV1 { kernel } = &spec.boot;
            let required = [
                runtime::artifact_identity::Artifact::Runtime(runtime::RuntimeArtifactIdentity {
                    kind: pplx_space_execution_common::proto::RuntimeKind::from(
                        release.runtime.kind,
                    ) as i32,
                    version: release.runtime.version.to_string(),
                    digest: release.digest.to_string(),
                }),
                runtime::artifact_identity::Artifact::Component(runtime::ComponentIdentity {
                    kind: pplx_space_execution_common::proto::ArtifactComponentKind::from(
                        kernel.kind,
                    ) as i32,
                    version: kernel.version.to_string(),
                    digest: kernel.digest.to_string(),
                }),
            ];
            for artifact in required {
                let identity = runtime::ArtifactIdentity {
                    artifact: Some(artifact),
                };
                if !artifacts.contains(&identity) {
                    artifacts.push(identity);
                }
            }
        }
        Ok(Self { artifacts })
    }
}

trait ArtifactManager: Sync {
    fn inventory(
        &self,
    ) -> impl Future<Output = Result<runtime::ListExecutionArtifactsResponse, Status>> + Send;
    fn install(
        &self,
        identity: runtime::ArtifactIdentity,
    ) -> impl Future<Output = Result<(), Status>> + Send;
}

struct Manager(RuntimeManagerGrpcClient);

impl ArtifactManager for Manager {
    async fn inventory(&self) -> Result<runtime::ListExecutionArtifactsResponse, Status> {
        self.0
            .clone()
            .list_execution_artifacts(runtime::ListExecutionArtifactsRequest::default())
            .await
            .map(tonic::Response::into_inner)
    }

    async fn install(&self, identity: runtime::ArtifactIdentity) -> Result<(), Status> {
        let operation = self
            .0
            .clone()
            .install_artifact(runtime::InstallArtifactRequest {
                identity: Some(identity.clone()),
            })
            .await?
            .into_inner()
            .operation
            .ok_or_else(|| Status::data_loss("install response has no operation"))?;
        if operation.identity.as_ref() != Some(&identity)
            || operation.action != runtime::ArtifactAction::Install as i32
            || operation.operation_id.is_empty()
        {
            return Err(Status::data_loss(
                "install acknowledgement does not match request",
            ));
        }
        match runtime::OperationState::try_from(operation.state) {
            Ok(
                runtime::OperationState::Queued
                | runtime::OperationState::Downloading
                | runtime::OperationState::Publishing
                | runtime::OperationState::Succeeded,
            ) => Ok(()),
            _ => Err(Status::failed_precondition(format!(
                "installation {} is not progressing: {}",
                operation.operation_id,
                operation
                    .error
                    .as_deref()
                    .unwrap_or("unexpected operation state"),
            ))),
        }
    }
}

async fn reconcile(
    plan: &InstallationPlan,
    manager: &impl ArtifactManager,
) -> Result<(), ProvisioningError> {
    if plan.artifacts.is_empty() {
        return Ok(());
    }
    let inventory = manager
        .inventory()
        .await
        .map_err(ProvisioningError::Inventory)?;
    if !inventory.complete || !inventory.issues.is_empty() {
        return Err(ProvisioningError::IncompleteInventory);
    }
    let missing: Vec<_> = plan
        .artifacts
        .iter()
        .filter(|identity| {
            !inventory
                .artifacts
                .iter()
                .any(|artifact| installed(identity, artifact))
        })
        .cloned()
        .collect();
    if missing.is_empty() {
        return Ok(());
    }
    let count = missing.len();
    let failed = stream::iter(missing).map(|identity| async move {
        match manager.install(identity.clone()).await {
            Ok(()) => 0,
            Err(error) => {
                tracing::warn!(?identity, %error, "runtime artifact installation submission failed");
                1
            }
        }
    }).buffer_unordered(SUBMISSION_CONCURRENCY).fold(0, |total, failed| async move { total + failed }).await;
    // An accepted operation is not proof of installed bytes. Completion is
    // observed on a later tick; active submissions deduplicate inside rtmgr.
    Err(ProvisioningError::Pending {
        missing: count,
        failed,
    })
}

fn installed(identity: &runtime::ArtifactIdentity, artifact: &runtime::ExecutionArtifact) -> bool {
    if artifact.management != runtime::ArtifactManagement::SpaceManaged as i32
        || artifact.status != runtime::ExecutionArtifactStatus::Valid as i32
    {
        return false;
    }
    match &identity.artifact {
        Some(runtime::artifact_identity::Artifact::Runtime(required)) => {
            artifact.kind == runtime::ExecutionArtifactKind::Runtime as i32
                && artifact.runtime_kind == required.kind
                && artifact.version.as_deref() == Some(required.version.as_str())
                && artifact.digest.as_deref() == Some(required.digest.as_str())
        }
        Some(runtime::artifact_identity::Artifact::Component(required)) => {
            artifact.kind == runtime::ExecutionArtifactKind::Component as i32
                && artifact.component_kind == required.kind
                && artifact.version.as_deref() == Some(required.version.as_str())
                && artifact.digest.as_deref() == Some(required.digest.as_str())
        }
        None => false,
    }
}

#[cfg(test)]
mod tests;
