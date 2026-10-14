//! Single source of truth for the per-sandbox setup a sandbox receives at
//! creation, shared by the cold-create and warm-pool-adoption paths.
//!
//! A cold `create_execution` and a warm-pool adoption (`prepare_pooled_execution`)
//! must set a sandbox up identically. The two paths apply that setup through
//! different mechanisms — cold bakes it into pod annotations consumed at boot;
//! adoption re-applies it via live gRPC to an already-booted pool pod — so they
//! cannot literally share one apply function across the process boundary.
//! Historically that meant setup steps were added to the cold path without being
//! mirrored into adoption (network policy, `snapshots/` permissions), each
//! surfacing later as a smoke failure once a pooled sandbox tripped over the
//! missing step.
//!
//! [`PerSandboxSetup`] resolves the setup *intent* once, from inputs both paths
//! already carry. Each path then consumes it by **exhaustive struct
//! destructure** (no `..`): the cold path in [`PerSandboxSetup::into_cold_pod_metadata`],
//! the adoption path in `KubernetesSubstrate::apply_adopted_per_sandbox_setup`.
//! Adding a field here is a compile error (E0027, "pattern does not mention
//! field") at every consumer until it is handled in each — so a new cold-path
//! setup step can no longer silently skip pooled sandboxes.
//!
//! Per-step process-boundary notes:
//! - Owner / process env / network policy / auto-lifecycle are applied adapter-side.
//! - [`NodeFilesystemSetup`] (the `snapshots/` group + setgid permissions) is a
//!   node-local effect the API process cannot apply directly. On a cold boot it
//!   is set by node-mgr `PrepareSandbox`; for adoption it is ensured eagerly by
//!   sbx-mgr's shared resume path (`resume_after_wake_lock`) before the VM
//!   resumes. The destructure still lists it so a future node-fs requirement is
//!   surfaced at both consumers.
//!
use pplx_space_common_core::types::{OrgId, UserId};

use pplx_space_common_core::kubernetes::SandboxRuntimePodMetadata;
use pplx_space_common_core::types::{
    AutoLifecyclePolicy, DiskIoLimits, EnvMap, PolicyAuthority, SandboxOwner,
};
use pplx_space_net_common::NetworkPolicyV1;

use super::SubstrateError;
use super::ensure_network_policy_annotation_size;
use crate::controllers::substrate::{
    ActorRef, CreateExecutionRequest, PooledExecutionClaimRequest, SandboxResources,
};

/// Cluster-API-created sandboxes always carry cluster authority for both their
/// network policy and their auto-lifecycle policy; user overrides are layered on
/// top by generation, not by changing authority here.
const SETUP_POLICY_AUTHORITY: PolicyAuthority = PolicyAuthority::Cluster;

/// Resolved network-policy setup intent. The effective policy is the value the
/// sandbox must enforce and is identical across cold and adoption; the `override`
/// records whether the user supplied an explicit policy (the wire *generation* is
/// derived per-path and is intentionally not stored here — see the module-level
/// note on cold vs. adoption generation handling).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct NetworkPolicySetup {
    /// Explicit per-sandbox override, if the request carried one.
    pub override_policy: Option<NetworkPolicyV1>,
    /// Effective policy = override or the template default.
    pub effective_policy: NetworkPolicyV1,
    pub authority: PolicyAuthority,
}

/// Node-local filesystem setup intent. A unit-like marker today (ensure the
/// artifact-dir permissions); modeled as a type so it participates in the
/// destructure-based compile guarantee and has a home for future node-fs data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum NodeFilesystemSetup {
    EnsureArtifactDirPermissions,
}

/// The resolved per-sandbox setup, identical for a cold create and a warm-pool
/// adoption built from equivalent inputs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PerSandboxSetup {
    pub sandbox_owner: SandboxOwner,
    pub user_id: UserId,
    pub org_id: OrgId,
    pub process_env: EnvMap,
    pub auto_lifecycle: AutoLifecyclePolicy,
    pub network_policy: NetworkPolicySetup,
    pub node_fs: NodeFilesystemSetup,
    /// Encryption must be installed before the sandbox is activated.
    pub volume_encryption: bool,
    pub disk_io_limits: DiskIoLimits,
}

impl PerSandboxSetup {
    /// Resolve the setup intent from the inputs both create paths carry. This is
    /// the single place the intent is decided.
    fn resolve(
        actor: &ActorRef,
        process_env: EnvMap,
        auto_lifecycle: &AutoLifecyclePolicy,
        network_policy_override: Option<&NetworkPolicyV1>,
        template_network_policy: &NetworkPolicyV1,
        volume_encryption: bool,
        disk_io_limits: DiskIoLimits,
    ) -> Self {
        let effective_policy = network_policy_override
            .cloned()
            .unwrap_or_else(|| template_network_policy.clone());
        Self {
            sandbox_owner: actor.sandbox_owner.clone(),
            user_id: actor.user_id,
            org_id: actor.org_id,
            process_env,
            auto_lifecycle: auto_lifecycle.clone(),
            network_policy: NetworkPolicySetup {
                override_policy: network_policy_override.cloned(),
                effective_policy,
                authority: SETUP_POLICY_AUTHORITY,
            },
            node_fs: NodeFilesystemSetup::EnsureArtifactDirPermissions,
            volume_encryption,
            disk_io_limits,
        }
    }

    /// Build the cold-create setup from a [`CreateExecutionRequest`].
    pub(super) fn from_create_request(request: &CreateExecutionRequest<'_>) -> Self {
        Self::resolve(
            request.actor,
            EnvMap::try_from(request.env.clone())
                .expect("request env should be validated before resolving sandbox setup"),
            request.auto_lifecycle,
            request.network_policy_override,
            request.template_network_policy,
            request.volume_encryption,
            request.disk_io_limits,
        )
    }

    /// Build the warm-pool-adoption setup from a [`PooledExecutionClaimRequest`].
    pub(super) fn from_claim_request(
        request: &PooledExecutionClaimRequest<'_>,
        claim: &crate::controllers::substrate::PooledExecutionClaim,
    ) -> Self {
        Self::resolve(
            request.actor,
            EnvMap::try_from(request.env.clone())
                .expect("request env should be validated before resolving sandbox setup"),
            request.auto_lifecycle,
            request.network_policy_override,
            request.template_network_policy,
            request.volume_encryption,
            claim.disk_io_limits,
        )
    }

    /// Cold consumer: serialize the setup into the owned strings a pod runtime
    /// metadata block borrows. Exhaustively destructures `self`, so a new setup
    /// field fails to compile here until handled.
    ///
    /// `network_policy_generation` is supplied by the caller because the cold and
    /// adoption paths derive it differently (see the module-level note); the
    /// effective policy and owner are shared.
    pub(super) fn into_cold_pod_metadata(
        self,
        network_policy_generation: u64,
    ) -> Result<ColdPodMetadata, SubstrateError> {
        let PerSandboxSetup {
            sandbox_owner,
            user_id,
            org_id,
            process_env: _,
            auto_lifecycle,
            network_policy,
            node_fs,
            volume_encryption,
            disk_io_limits,
        } = self;
        // Cold create passes process env through the pod/shim create path, not
        // through pod runtime metadata.
        let NetworkPolicySetup {
            // Cold encodes only the effective policy into the pod annotation; the
            // override-vs-inherit distinction is carried by the generation the
            // caller computed.
            override_policy: _,
            effective_policy,
            authority,
        } = network_policy;
        // Node-fs permissions are applied by node-mgr `PrepareSandbox` during the
        // cold boot this pod triggers; nothing to encode into the pod spec.
        let NodeFilesystemSetup::EnsureArtifactDirPermissions = node_fs;

        let auto_lifecycle_json =
            serde_json::to_string(&auto_lifecycle).map_err(|err| SubstrateError::Failed {
                message: format!("failed to serialize auto lifecycle policy: {err}"),
            })?;
        let network_policy_json =
            serde_json::to_string(&effective_policy).map_err(|err| SubstrateError::Failed {
                message: format!("failed to serialize network policy: {err}"),
            })?;
        ensure_network_policy_annotation_size(&network_policy_json)?;

        Ok(ColdPodMetadata {
            sandbox_owner,
            user_id,
            org_id,
            auto_lifecycle_json,
            network_policy_json,
            network_policy_generation,
            authority,
            volume_encryption,
            disk_io_limits,
        })
    }
}

/// Owned holder for the strings a [`SandboxRuntimePodMetadata`] borrows. Produced
/// by [`PerSandboxSetup::into_cold_pod_metadata`]; the caller keeps it alive while
/// building the pod request.
pub(super) struct ColdPodMetadata {
    sandbox_owner: SandboxOwner,
    user_id: UserId,
    org_id: OrgId,
    auto_lifecycle_json: String,
    network_policy_json: String,
    network_policy_generation: u64,
    authority: PolicyAuthority,
    volume_encryption: bool,
    disk_io_limits: DiskIoLimits,
}

impl ColdPodMetadata {
    pub(super) fn runtime_metadata<'a>(
        &'a self,
        resources: &SandboxResources,
    ) -> SandboxRuntimePodMetadata<'a> {
        SandboxRuntimePodMetadata {
            sandbox_owner: &self.sandbox_owner,
            user_id: self.user_id,
            org_id: self.org_id,
            auto_lifecycle_json: &self.auto_lifecycle_json,
            auto_lifecycle_authority: self.authority,
            network_policy_json: &self.network_policy_json,
            network_policy_generation: self.network_policy_generation,
            network_policy_authority: self.authority,
            vcpu: resources.vcpu,
            memory_mib: resources.memory_mib,
            disk_mib: resources.disk_mib,
            disk_io_limits: self.disk_io_limits,
            volume_encryption: self.volume_encryption,
        }
    }
}

#[cfg(test)]
mod tests;
