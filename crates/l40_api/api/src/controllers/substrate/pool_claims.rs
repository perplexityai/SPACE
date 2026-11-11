//! Warm-pool eligibility and the request, claim and refusal types of a pooled execution claim.

use super::*;

pub(crate) fn default_pool_eligible_for_request(
    boot_mode: CreateBootMode,
    backend: &str,
    wait_until_ready: bool,
    disk_mib: Option<u32>,
    file_changes_enabled: bool,
    implicit_wake_enabled: bool,
) -> bool {
    backend == "fc"
        && matches!(
            boot_mode,
            CreateBootMode::Fallthrough | CreateBootMode::Pool
        )
        && disk_mib.is_none()
        && !file_changes_enabled
        && (wait_until_ready || implicit_wake_enabled)
}

/// Combine a substrate's generic warm-pool eligibility with env handling. A cold
/// create bakes `req.env` into the sandbox at boot; if the substrate can't
/// reproduce that on a pooled sandbox at adoption (`substrate_supports_pool_env`
/// is false), an env-bearing create must take the cold path rather than silently
/// drop the requested variables.
pub(crate) fn pool_eligible_with_env(
    substrate_eligible: bool,
    substrate_supports_pool_env: bool,
    has_request_env: bool,
) -> bool {
    substrate_eligible && (substrate_supports_pool_env || !has_request_env)
}

pub struct PooledExecutionClaimRequest<'a> {
    pub metric_identity: SandboxApiMetricIdentity,
    pub actor: &'a ActorRef,
    pub template: &'a str,
    pub content_hash: Option<&'a str>,
    /// Pre-resolved template release id. When supplied, `claim_pooled_execution`
    /// skips the `template_releases` lookup the caller already performed.
    pub template_release_id: Option<Uuid>,
    pub runtime: &'a str,
    pub vcpu: Option<u32>,
    pub memory_mib: Option<u32>,
    pub env: &'a BTreeMap<String, String>,
    pub auto_lifecycle: &'a AutoLifecyclePolicy,
    pub network_policy_override: Option<&'a NetworkPolicyV1>,
    pub network_policy_generation: u64,
    pub template_network_policy: &'a NetworkPolicyV1,
    pub template_network_policy_generation: u64,
    /// Routes the claim to the matching encryptable pool partition. Encrypted
    /// creates bypass the pool until claim-time re-homing can stamp and apply
    /// the flag at adoption.
    pub volume_encryption: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PooledExecutionPoolShape {
    pub vcpu: u32,
    pub memory_mib: u32,
    pub disk_mib: Option<u32>,
    pub disk_io_limits: pplx_space_common_core::types::DiskIoLimits,
}

impl PooledExecutionPoolShape {
    pub const fn new(
        vcpu: u32,
        memory_mib: u32,
        disk_io_limits: pplx_space_common_core::types::DiskIoLimits,
    ) -> Self {
        Self {
            vcpu,
            memory_mib,
            disk_mib: None,
            disk_io_limits,
        }
    }

    pub const fn with_disk_mib(
        vcpu: u32,
        memory_mib: u32,
        disk_mib: Option<u32>,
        disk_io_limits: pplx_space_common_core::types::DiskIoLimits,
    ) -> Self {
        Self {
            vcpu,
            memory_mib,
            disk_mib,
            disk_io_limits,
        }
    }
}

pub struct PooledExecutionClaim {
    pub claim_id: Uuid,
    pub pool_slot_id: Option<Uuid>,
    pub sandbox_id: SandboxId,
    pub content_hash: String,
    pub pod_name: String,
    pub pod_uid: Option<PodUid>,
    pub execution_generation: u64,
    pub pod_ip: String,
    pub vcpu: Option<u32>,
    pub memory_mib: Option<u32>,
    pub disk_mib: Option<u32>,
    pub disk_io_limits: pplx_space_common_core::types::DiskIoLimits,
    pub active_node_id: Option<NodeId>,
    /// Authoritative route identity returned while claiming the execution.
    /// Substrates without route identity leave it unset.
    pub route_identity: Option<SandboxRouteIdentity>,
}

/// A warm-pool slot claimed for a create when the create was admitted, before
/// it was queued. The create's public sandbox ID is that slot's reserved ID,
/// so the executor must adopt exactly this slot (or build cold under the same
/// ID) and never claim a different one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdmittedPoolClaim {
    pub claim_id: Uuid,
    pub pool_slot_id: Uuid,
}

/// A ready warm-pool slot claimed while a create is being admitted, in the
/// transaction that records the create. `sandbox_id` is the slot's reserved
/// ID, which becomes the create's public ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdmissionPoolClaim {
    pub claim: AdmittedPoolClaim,
    pub sandbox_id: SandboxId,
}

/// What the substrate found for an [`AdmittedPoolClaim`].
pub enum AdmittedPoolClaimLookup {
    /// The admitted slot still carries this claim and passed the same pod
    /// and placement checks an ordinary claim makes. The caller must persist
    /// it with `persist_pooled_execution_claim` or compensate it with
    /// `rollback_pooled_execution_claim`.
    Adoptable {
        claim: Box<PooledExecutionClaim>,
        pool_shape: PooledExecutionPoolShape,
    },
    /// No unbound slot owns a pod under this sandbox ID any more (the slot
    /// was collected, or was bound and retired). A cold create under the same
    /// ID is safe once it skips the generation the slot's pod used.
    Released,
    /// A slot still owns a pod under this sandbox ID that this create will
    /// not adopt. It has been taken out of the claimable inventory, and the
    /// pool's own cleanup collects it; until then a cold create under this ID
    /// would leave that pod with nothing to collect it, so the caller waits.
    ReleasePending,
}

pub struct PooledExecutionClaimAttempt {
    pub claim: Option<PooledExecutionClaim>,
    /// Exact release-and-shape pool selected by this attempt. A miss can have
    /// no shape when no pool exists for the resolved release; callers must not
    /// manufacture one from optional create-request fields in that case.
    pub pool_shape: Option<PooledExecutionPoolShape>,
    /// Why this attempt missed, when the reason is one a retry cannot change.
    ///
    /// `None` for an ordinary miss — no slot, a stale slot, a pod that could
    /// not be read. Those are all "try again later", which is what the pool's
    /// own miss handling already assumes.
    ///
    /// The substrate does not decide what a refusal means for the caller,
    /// because that depends on the boot mode it does not see: a fall-through
    /// create builds cold and resolves the same boundary again, while a strict
    /// `boot_mode=pool` create has nowhere to fall through to and would
    /// otherwise report a permanent refusal as "no slot available yet".
    pub refusal: Option<PooledClaimRefusal>,
}

/// A pooled-claim miss that no retry can turn into a hit.
///
/// The variant carries the *class* of the answer, so the controller renders it
/// rather than deciding it — only this module knows which condition occurred.
/// The rule is the status the condition warrants, which for every condition the
/// cold path *also* fails on means matching what the cold path answers. One
/// producer has no cold-path counterpart at all; see below.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PooledClaimRefusal {
    /// The sandbox's node-group boundary admits no pooled pod. A conflict: the
    /// request is incompatible with this sandbox's placement.
    ///
    /// Two producers, and they reach the same status differently:
    ///
    /// * An **empty** candidate set (`NoCompatibleNodeGroup` — an org-specific
    ///   template whose org has no compatible dedicated group). The cold path
    ///   maps this to `Conflict`, so the two agree and the caller sees the same
    ///   409 either way.
    /// * A **dedicated-only** set. Here the cold path does not fail at all: it
    ///   renders the dedicated affinity and the create succeeds. There is no
    ///   cold-path status to match, and 409 is the answer specific to a strict
    ///   `boot_mode=pool` request — a pooled pod is built for no org and lands
    ///   on shared capacity, so no slot can ever satisfy this sandbox and no
    ///   retry changes that.
    PlacementIncompatible { message: String },
    /// The boundary could not be computed at all, for a reason an operator has
    /// to repair — a reaped `templates` row, or a predicate disagreement. The
    /// cold path maps both to `Failed`, so this renders as a 500: it is a
    /// control-plane fault, not something the caller expressed, and reporting
    /// it as a conflict would take the only signal of a missing row out of 5xx
    /// alerting. Terminal, so *not* a 503 either — a strict-pool client must
    /// not be told to retry what only an operator can fix.
    PlacementUnresolvable { message: String },
}

impl std::fmt::Debug for PooledExecutionClaimAttempt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PooledExecutionClaimAttempt")
            .field("claimed", &self.claim.is_some())
            .field("pool_shape", &self.pool_shape)
            .field("refusal", &self.refusal)
            .finish()
    }
}

impl PooledExecutionClaimAttempt {
    pub const fn claimed(
        claim: PooledExecutionClaim,
        pool_shape: PooledExecutionPoolShape,
    ) -> Self {
        Self {
            claim: Some(claim),
            pool_shape: Some(pool_shape),
            refusal: None,
        }
    }

    pub const fn missed(pool_shape: Option<PooledExecutionPoolShape>) -> Self {
        Self {
            claim: None,
            pool_shape,
            refusal: None,
        }
    }

    /// A miss the caller may need to surface differently. See
    /// [`PooledClaimRefusal`].
    pub const fn refused(
        pool_shape: Option<PooledExecutionPoolShape>,
        refusal: PooledClaimRefusal,
    ) -> Self {
        Self {
            claim: None,
            pool_shape,
            refusal: Some(refusal),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PooledPrepareFailureCleanup {
    FinalizeSandboxDbDelete,
    DeferredToReconciler,
}
