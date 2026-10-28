//! Kubernetes metadata naming helpers shared by SPACE control-plane and node
//! services.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::str::FromStr;

use k8s_openapi::api::core::v1::{
    Affinity, Container, EnvVar, NodeAffinity, NodeSelector, NodeSelectorRequirement,
    NodeSelectorTerm, Pod, PodSpec, PreferredSchedulingTerm, ResourceRequirements, Toleration,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::types::{
    MemorySize, NodePoolName, PolicyAuthority, ROOT_SANDBOX_OWNER, SandboxOwner, TemplateReference,
    VcpuCount,
};

#[path = "k8s/placement.rs"]
pub mod placement;
pub use placement::{
    DEDICATED_NODE_GROUP_PREFERENCE_WEIGHT, DEDICATED_NODE_TAINT_KEY,
    DEFAULT_SANDBOX_NODE_GROUP_PLACEMENT, NodeGroupPlacement, NodeGroupPlacementError,
    SANDBOX_NODE_GROUP_LABEL, SANDBOX_NODE_GROUP_PLACEMENT_ENV, node_group_label,
};

#[path = "k8s/node_selector.rs"]
pub mod node_selector;
pub use node_selector::SandboxNodeSelector;

pub const TEMPLATE_LABEL_DOMAIN: &str = "space.pplx.net";
pub const TEMPLATE_PRESENT_LABEL_VALUE: &str = "true";
pub const SANDBOX_MANAGED_BY_LABEL: &str = "space.pplx.net/managed-by";
/// Cold-create env forwarded by CRI to the infra OCI spec before manager Create.
/// Standard base64 of sorted UTF-8 `KEY=value\0` entries: EnvMap forbids NUL,
/// and this avoids JSON's sixfold expansion of allowed control characters.
/// A present empty value is authoritative; absence retains legacy shim behavior.
pub const SANDBOX_PROCESS_ENV_ANNOTATION: &str = "space.pplx.net/process-env-v1";

pub const SANDBOX_ID_ANNOTATION: &str = "space.pplx.net/sandbox-id";
pub const SANDBOX_EXECUTION_GENERATION_ANNOTATION: &str = "space.pplx.net/execution-generation";
pub const SANDBOX_ACTIVATION_ATTEMPT_ID_ANNOTATION: &str = "space.pplx.net/activation-attempt-id";
pub const SANDBOX_ROUTE_GENERATION_ANNOTATION: &str = "space.pplx.net/route-generation";
pub const SANDBOX_TEMPLATE_ANNOTATION: &str = "space.pplx.net/template";
pub const SANDBOX_CONTENT_HASH_ANNOTATION: &str = "space.pplx.net/content-hash";
pub const SANDBOX_CHECKPOINT_ANNOTATION: &str = "space.pplx.net/checkpoint";
pub const SANDBOX_VCPU_ANNOTATION: &str = "space.pplx.net/vcpu";
pub const SANDBOX_MEMORY_MIB_ANNOTATION: &str = "space.pplx.net/memory-mib";
pub const SANDBOX_SUSPEND_RESTORE_STORAGE_URI_ANNOTATION: &str =
    "space.pplx.net/suspend-restore-storage-uri";
pub const SANDBOX_SUSPEND_RESTORE_SOURCE_NODE_ANNOTATION: &str =
    "space.pplx.net/suspend-restore-source-node";
pub const SANDBOX_FORK_DISK_RESTORE_ANNOTATION: &str = "space.pplx.net/fork-disk-restore";
pub const SANDBOX_DISK_MIB_ANNOTATION: &str = "space.pplx.net/disk-mib";
pub const SANDBOX_DISK_IO_MAX_OPERATIONS_PER_SEC_ANNOTATION: &str =
    "space.pplx.net/disk-io-max-operations-per-sec";
pub const SANDBOX_DISK_IO_MAX_BANDWIDTH_MIB_PER_SEC_ANNOTATION: &str =
    "space.pplx.net/disk-io-max-bandwidth-mib-per-sec";
pub const SANDBOX_AUTO_LIFECYCLE_ANNOTATION: &str = "space.pplx.net/auto-lifecycle";
pub const SANDBOX_AUTO_LIFECYCLE_AUTHORITY_ANNOTATION: &str =
    "space.pplx.net/auto-lifecycle-authority";
pub const SANDBOX_NETWORK_POLICY_ANNOTATION: &str = "space.pplx.net/network-policy";
pub const SANDBOX_NETWORK_POLICY_GENERATION_ANNOTATION: &str =
    "space.pplx.net/network-policy-generation";
pub const SANDBOX_NETWORK_POLICY_AUTHORITY_ANNOTATION: &str =
    "space.pplx.net/network-policy-authority";
pub const SANDBOX_TRACEPARENT_ANNOTATION: &str = "space.pplx.net/traceparent";
pub const SANDBOX_OWNER_ANNOTATION: &str = "space.pplx.net/sandbox-owner";
pub const SANDBOX_USER_ANNOTATION: &str = "space.pplx.net/sandbox-user";
/// BYOK: the sandbox's immutable encrypted-volumes flag. Rendered only when
/// `true` ("true"); absent means plaintext, so pooled pods and every pod
/// created before this annotation existed read as plaintext. The shim
/// forwards it into `CreateRequest.volume_encryption`, and it must always be
/// rendered from the control-plane row (its SSOT) — a restore/recreate pod
/// that dropped it would trip the node's immutable-identity check on the
/// existing sandbox row.
pub const SANDBOX_VOLUME_ENCRYPTION_ANNOTATION: &str = "space.pplx.net/volume-encryption";
pub const SANDBOX_OWNER_ID_LABEL: &str = "space.pplx.net/user-id";
pub const SANDBOX_ORG_ID_LABEL: &str = "space.pplx.net/org-id";
pub const SANDBOX_POOL_SLOT_ID_KEY: &str = "space.pplx.net/pool-slot-id";
pub const SANDBOX_POOL_CONFIG_ID_KEY: &str = "space.pplx.net/pool-config-id";
pub const SANDBOX_POOL_ROLE_KEY: &str = "space.pplx.net/pool-role";
pub const SANDBOX_POOL_ROLE_WARM: &str = "warm";
/// Label and taint key identifying an isolation node pool a create request can
/// pin a sandbox to. The pool's nodes carry a `NoSchedule` taint on this key
/// and a label mapping it to the pool name; a placed pod gets a matching
/// `nodeSelector` entry plus a toleration. The value `space.pplx.net/smoke-pool`
/// is the discriminator the experiment ("smoke") pools already carry, so it is
/// kept verbatim; only the Rust name is a neutral placement term.
pub const SANDBOX_PLACEMENT_POOL_LABEL_KEY: &str = "space.pplx.net/smoke-pool";
pub const MAX_SANDBOX_NETWORK_POLICY_ANNOTATION_BYTES: usize = 128 * 1024;

/// Extended resource advertised on a worker Node's `status.capacity` and
/// requested by every sandbox pod. The node-local controller sources the
/// capacity from node-manager's `network_pool_max_slots`, so the scheduler
/// will not place more sandbox pods on a node than that node has network
/// slots. One sandbox pod consumes exactly one real node-manager slot, so the
/// request and limit are both `1` and the resource is never overcommitted.
pub const SANDBOX_NETWORK_SLOTS_RESOURCE: &str = "space.pplx.net/network-slots";

/// Node label carrying the node's [`HostCpu`](crate::types::HostCpu) identity,
/// published by the node-local controller from `/proc/cpuinfo`.
///
/// It's the same derivation the sandbox manager records as a suspend
/// snapshot's capture CPU, so a snapshot's recorded value can be compared with
/// the label of any node that might load it. A node whose CPU can't be
/// identified carries no label.
pub const HOST_CPU_LABEL: &str = "space.pplx.net/host-cpu";

/// Node label carrying the bucketed SSD-headroom signal for sandbox placement.
///
/// Coarse `/space/cache` headroom tier `0..=3`, keyed off disk *usage* (see
/// [`SsdHeadroomTier`]). The sandbox pod builder turns the tiers into weighted
/// preferred node-affinity terms so kube-scheduler avoids near-full nodes while
/// treating the whole low-utilization fleet as interchangeable. Nodes without
/// the label simply score 0 — a missing or stale tier degrades to unbiased
/// placement, never to unschedulable.
pub const SSD_HEADROOM_TIER_LABEL: &str = "space.pplx.net/ssd-headroom-tier";

/// Node label carrying the hard placement-block signal for sandbox scheduling.
///
/// Orthogonal to the soft [`SSD_HEADROOM_TIER_LABEL`] bias: `"true"` blocks new
/// sandbox placement on this node, `"false"` leaves it schedulable. The sandbox
/// pod builder turns it into a **required** node anti-affinity (`NotIn` the
/// blocked value). `NotIn` is satisfied by nodes that lack the label, so
/// unlabeled / older-node-ctl workers stay schedulable across a rollout.
pub const SSD_PLACEMENT_BLOCKED_LABEL: &str = "space.pplx.net/ssd-placement-blocked";

/// Value of [`SSD_PLACEMENT_BLOCKED_LABEL`] on a node that must receive no new
/// sandbox placements. Any other value (including `"false"` or the label's
/// absence) leaves the node schedulable.
pub const SSD_PLACEMENT_BLOCKED_VALUE: &str = "true";

/// Node label carrying whether the node-local SPACE service pods (node-ctl,
/// volume-manager, …) are currently `Ready` on this node.
///
/// A sandbox pod that lands on a node whose node-local daemons are mid-restart
/// does not fail — it *waits* for them, which is how a worker DaemonSet rolling
/// update (`maxUnavailable: 10%`) turns into a multi-second create-latency
/// spike across the fleet. The node cannot advertise this itself: node-ctl is
/// one of the daemons that goes away, so the writer is a cluster-singleton that
/// watches the DaemonSet pods and publishes the per-node verdict here.
///
/// The sandbox pod builder turns it into a **soft** weighted preferred
/// node-affinity term (`NotIn` the not-ready value), so new sandboxes drain off
/// a rolling node while it is being serviced but still land there when nothing
/// else has capacity — the current "slow but successful" worst case is
/// preserved. `NotIn` is satisfied by nodes that lack the label, so an
/// unlabeled node (writer disabled, node just joined, or a cluster that never
/// runs the writer) keeps the full weight and placement is unchanged.
pub const NODE_SERVICES_READY_LABEL: &str = "space.pplx.net/node-services-ready";

/// Value of [`NODE_SERVICES_READY_LABEL`] on a node whose node-local service
/// pods are all `Ready`.
pub const NODE_SERVICES_READY_VALUE: &str = "true";

/// Value of [`NODE_SERVICES_READY_LABEL`] on a node with at least one
/// not-`Ready` (or missing, or terminating) node-local service pod. Any other
/// value — including the label's absence — reads as ready, matching the
/// `NotIn` affinity term.
pub const NODE_SERVICES_NOT_READY_VALUE: &str = "false";

/// Node label binding a worker node to its SPACE node group. The value is the
/// validated `node_groups.name` slug (`NodeGroupName`).
///
/// This label is the scheduling source of truth for node-group membership;
/// the control plane mirrors it into `nodes.node_group_id` for reporting. It
/// deliberately lives under the `node-restriction.kubernetes.io/` prefix: the
/// NodeRestriction admission plugin forbids a kubelet from setting labels in
/// that prefix on its own Node object, so a compromised worker cannot
/// relabel itself into another org's dedicated group. Only the control plane
/// (EKS managed-node-group labels or an admin-credentialed patch) may set it.
///
/// Of those two control-plane paths, **this repo uses only the patch.** An MNG
/// label is one value for every node in its group, and membership is per-node
/// state the control plane has to be able to revise. Deployment configuration
/// must not assign this key. The default group's writer is
/// the cluster-controller's `node_group_labeler`; a later group's is an
/// admin-credentialed Node patch — an operator `kubectl label`, since no
/// endpoint on the admin surface writes Node labels — because that reconciler
/// stops writing labels once a second group exists.
pub const NODE_GROUP_LABEL: &str = "node-restriction.kubernetes.io/space-node-group";

const TEMPLATE_BUILD_LABEL_NAME_PREFIX: &str = "template-build";
const TEMPLATE_BUILD_LABEL_KEY_PREFIX: &str = "space.pplx.net/template-build-";
pub const TEMPLATE_WRITE_ANNOTATION_KEY: &str = "space.pplx.net/template-write";

/// Environment variable used by Kubernetes DaemonSets to expose the node name
/// via `spec.nodeName`.
pub const SPACE_K8S_NODE_NAME_ENV: &str = "SPACE_K8S_NODE_NAME";

pub const SANDBOX_POD_IMAGE_ENV: &str = "SPACE_SANDBOX_POD_IMAGE";
pub const SANDBOX_NODE_SELECTOR_KEY_ENV: &str = "SPACE_SANDBOX_NODE_SELECTOR_KEY";
pub const SANDBOX_NODE_SELECTOR_VALUE_ENV: &str = "SPACE_SANDBOX_NODE_SELECTOR_VALUE";
/// Additional required `nodeSelector` labels beyond the base
/// `SANDBOX_NODE_SELECTOR_KEY`/`VALUE` pair, as a comma-separated list of
/// `key=value` pairs. These tighten sandbox-pod placement to nodes where the
/// whole SPACE worker stack is operational: the worker DaemonSets select on
/// all four worker labels, so a pod that requires only the base `node-pool`
/// label can land on a partially-labeled node where sandbox-manager isn't
/// running and fail with `sandbox-manager.sock: connect: no such file or
/// directory`. Empty/unset leaves the base single-label selector unchanged.
pub const SANDBOX_NODE_SELECTOR_LABELS_ENV: &str = "SPACE_SANDBOX_NODE_SELECTOR_LABELS";
pub const SANDBOX_INTERNAL_SECRET_NAME_ENV: &str = "SPACE_INTERNAL_SECRET_NAME";

pub const DEFAULT_SANDBOX_POD_IMAGE: &str = "registry.k8s.io/pause:3.10";
pub const DEFAULT_SANDBOX_RUNTIME_CLASS: &str = "space";
pub const DEFAULT_SANDBOX_NODE_SELECTOR_KEY: &str = "node.kubernetes.io/space-enabled";
pub const DEFAULT_SANDBOX_NODE_SELECTOR_VALUE: &str = "true";
pub const DEFAULT_SANDBOX_PROXY_PORT: u16 = 9800;
pub const DEFAULT_SANDBOX_INTERNAL_SECRET_NAME: &str = "space-internal";
pub const DEFAULT_SANDBOX_INTERNAL_TOKEN_KEY: &str = "SPACE_INTERNAL_TOKEN";
pub const DEFAULT_SANDBOX_INTERNAL_CONTROL_TOKEN_KEY: &str = "SPACE_INTERNAL_CONTROL_TOKEN";
/// SSD-headroom placement bias: sandbox pods carry the
/// [`SSD_HEADROOM_TIER_LABEL`] preferred-affinity terms.
pub const DEFAULT_SANDBOX_SSD_HEADROOM_TIER_AFFINITY: bool = true;
/// Source-aware restore placement: when enabled, a portable (S3-backed)
/// restore that knows its source node renders a **required**
/// `nodeAffinity.matchFields: metadata.name` shortlist. Off by default;
/// `SPACE_SANDBOX_RESTORE_SOURCE_NODE_HARD_PIN` enables it.
pub const SANDBOX_RESTORE_SOURCE_NODE_HARD_PIN_ENV: &str =
    "SPACE_SANDBOX_RESTORE_SOURCE_NODE_HARD_PIN";
pub const SANDBOX_NODE_SERVICES_READY_AFFINITY_ENV: &str =
    "SPACE_SANDBOX_NODE_SERVICES_READY_AFFINITY";
pub const DEFAULT_SANDBOX_RESTORE_SOURCE_NODE_HARD_PIN: bool = false;
/// Node-local-service readiness placement bias (a weighted preferred
/// node-affinity term for nodes not labelled [`NODE_SERVICES_NOT_READY_VALUE`]).
/// Off by default; `SPACE_SANDBOX_NODE_SERVICES_READY_AFFINITY` enables it.
pub const DEFAULT_SANDBOX_NODE_SERVICES_READY_AFFINITY: bool = false;

/// Coarse SSD-headroom tier advertised on worker nodes via
/// [`SSD_HEADROOM_TIER_LABEL`], keyed off disk **usage**:
///
/// | tier | disk usage | meaning |
/// |------|-----------|---------|
/// | [`MAX`](Self::MAX) (`3`) | `< 80%` | healthy — place freely |
/// | `2` | `80–90%` | filling — start avoiding |
/// | `1` | `90–95%` | nearly full — strongly avoid |
/// | [`ZERO`](Self::ZERO) (`0`) | `>= 95%` | near the disk gate — avoid |
///
/// The healthy band is deliberately one wide bucket (`< 80%` used). Ranking
/// nodes finely by free space makes every nearly-empty node outrank the working
/// bulk, so on a disk-busy cluster all new sandboxes funnel onto the one or two
/// emptiest nodes; collapsing the whole low-utilization range into a single
/// tie-ing tier spreads them across the healthy fleet instead. Resolution is
/// spent only in the high-usage danger zone, where the disk-gated reclaim loops
/// (`crates/libs/sbx/mgr/server/src/disk_usage.rs`: snapshot reaper engages at
/// `0.50` usage, cold-sandbox eviction at `0.55`) are already firing and the
/// difference between a 90%- and a 96%-full node actually matters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct SsdHeadroomTier(u8);

impl SsdHeadroomTier {
    /// No advertised headroom — the conservative tier for near-full (`>= 95%`
    /// used), degenerate, or unmeasurable filesystems.
    pub const ZERO: SsdHeadroomTier = SsdHeadroomTier(0);
    /// Healthy: less than 80% of the cache filesystem is used.
    pub const MAX: SsdHeadroomTier = SsdHeadroomTier(3);

    /// Bucket a free fraction in `[0.0, 1.0]` into a headroom tier by the disk
    /// **usage** (`1 - free`) thresholds documented on [`Self`]. Out-of-range
    /// and non-finite inputs clamp to the conservative end ([`Self::ZERO`]),
    /// so a nonsense measurement never advertises headroom.
    pub fn from_free_fraction(free_fraction: f64) -> Self {
        if !free_fraction.is_finite() {
            return Self::ZERO;
        }
        // Boundaries in free-fraction terms (free = 1 - usage):
        //   free  > 0.20  <=>  usage  < 80%   -> healthy
        //   free in (0.10, 0.20]  <=>  usage in [80%, 90%)
        //   free in (0.05, 0.10]  <=>  usage in [90%, 95%)
        //   free <= 0.05  <=>  usage >= 95%   -> near full
        let free = free_fraction.clamp(0.0, 1.0);
        if free > 0.20 {
            Self::MAX
        } else if free > 0.10 {
            Self(2)
        } else if free > 0.05 {
            Self(1)
        } else {
            Self::ZERO
        }
    }

    pub fn as_u8(self) -> u8 {
        self.0
    }
}

impl std::fmt::Display for SsdHeadroomTier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub struct SandboxPodConfig<'a> {
    pub namespace: &'a str,
    pub sandbox_image: &'a str,
    pub runtime_class_name: &'a str,
    pub sandbox_node_selector_key: &'a str,
    pub sandbox_node_selector_value: &'a str,
    pub extra_node_selector_labels: &'a BTreeMap<String, String>,
    pub internal_secret_name: &'a str,
    pub internal_token_key: &'a str,
    pub internal_control_token_key: &'a str,
    pub ssd_headroom_tier_affinity: bool,
    /// When true, a portable restore that knows its source node renders a
    /// required `nodeAffinity.matchFields: metadata.name` pin instead of the
    /// legacy weight-100 preferred term. See
    /// [`DEFAULT_SANDBOX_RESTORE_SOURCE_NODE_HARD_PIN`].
    pub restore_source_node_hard_pin: bool,
    /// When true, the pod carries the weighted preferred node-affinity term
    /// that biases placement away from nodes whose node-local service pods are
    /// not `Ready`. See [`DEFAULT_SANDBOX_NODE_SERVICES_READY_AFFINITY`].
    pub node_services_ready_affinity: bool,
}

#[derive(Debug, Clone)]
pub struct SandboxPodConfigValues {
    /// Gate on resolving a node-group candidate set at all — the fourth of this
    /// bundle's default-off placement switches. Unlike the other three it does
    /// not reach [`SandboxPodConfig`], because it gates the *resolution* (a
    /// database read, performed before the build) rather than the rendering:
    /// the builder's input is the resolved
    /// [`NodeGroupPlacement`], and `None` there is what "gate off" renders as.
    /// See [`SANDBOX_NODE_GROUP_PLACEMENT_ENV`].
    pub node_group_placement: bool,
    pub sandbox_image: String,
    pub runtime_class_name: String,
    pub sandbox_node_selector_key: String,
    pub sandbox_node_selector_value: String,
    pub extra_node_selector_labels: BTreeMap<String, String>,
    pub internal_secret_name: String,
    pub internal_token_key: String,
    pub internal_control_token_key: String,
    pub ssd_headroom_tier_affinity: bool,
    pub restore_source_node_hard_pin: bool,
    pub node_services_ready_affinity: bool,
}

/// Failure parsing [`SANDBOX_NODE_SELECTOR_LABELS_ENV`] into a label map.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum NodeSelectorLabelsParseError {
    #[error("node selector label entry {entry:?} must be in key=value form")]
    MissingEquals { entry: String },
    #[error("node selector label entry {entry:?} has an empty key")]
    EmptyKey { entry: String },
}

/// Parse a comma-separated list of `key=value` node-selector labels.
///
/// Whitespace around entries, keys, and values is trimmed; empty entries
/// (e.g. a trailing comma) are skipped. Fails closed on a malformed entry
/// rather than dropping it silently — a missing required worker label is the
/// exact placement bug this guard exists to prevent.
pub fn parse_node_selector_labels(
    raw: &str,
) -> Result<BTreeMap<String, String>, NodeSelectorLabelsParseError> {
    let mut labels = BTreeMap::new();
    for entry in raw.split(',') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let (key, value) =
            entry
                .split_once('=')
                .ok_or_else(|| NodeSelectorLabelsParseError::MissingEquals {
                    entry: entry.to_string(),
                })?;
        let key = key.trim();
        if key.is_empty() {
            return Err(NodeSelectorLabelsParseError::EmptyKey {
                entry: entry.to_string(),
            });
        }
        labels.insert(key.to_string(), value.trim().to_string());
    }
    Ok(labels)
}

impl SandboxPodConfigValues {
    pub fn from_env() -> Result<Self, NodeSelectorLabelsParseError> {
        Ok(Self {
            node_group_placement: crate::env::env_flag_var(
                SANDBOX_NODE_GROUP_PLACEMENT_ENV,
                DEFAULT_SANDBOX_NODE_GROUP_PLACEMENT,
            ),
            sandbox_image: env_or(SANDBOX_POD_IMAGE_ENV, DEFAULT_SANDBOX_POD_IMAGE),
            runtime_class_name: DEFAULT_SANDBOX_RUNTIME_CLASS.to_string(),
            sandbox_node_selector_key: env_or(
                SANDBOX_NODE_SELECTOR_KEY_ENV,
                DEFAULT_SANDBOX_NODE_SELECTOR_KEY,
            ),
            sandbox_node_selector_value: env_or(
                SANDBOX_NODE_SELECTOR_VALUE_ENV,
                DEFAULT_SANDBOX_NODE_SELECTOR_VALUE,
            ),
            extra_node_selector_labels: parse_node_selector_labels(&env_or(
                SANDBOX_NODE_SELECTOR_LABELS_ENV,
                "",
            ))?,
            internal_secret_name: env_or(
                SANDBOX_INTERNAL_SECRET_NAME_ENV,
                DEFAULT_SANDBOX_INTERNAL_SECRET_NAME,
            ),
            internal_token_key: DEFAULT_SANDBOX_INTERNAL_TOKEN_KEY.to_string(),
            internal_control_token_key: DEFAULT_SANDBOX_INTERNAL_CONTROL_TOKEN_KEY.to_string(),
            ssd_headroom_tier_affinity: DEFAULT_SANDBOX_SSD_HEADROOM_TIER_AFFINITY,
            restore_source_node_hard_pin: crate::env::env_flag_var(
                SANDBOX_RESTORE_SOURCE_NODE_HARD_PIN_ENV,
                DEFAULT_SANDBOX_RESTORE_SOURCE_NODE_HARD_PIN,
            ),
            node_services_ready_affinity: crate::env::env_flag_var(
                SANDBOX_NODE_SERVICES_READY_AFFINITY_ENV,
                DEFAULT_SANDBOX_NODE_SERVICES_READY_AFFINITY,
            ),
        })
    }

    pub fn for_namespace<'a>(&'a self, namespace: &'a str) -> SandboxPodConfig<'a> {
        SandboxPodConfig {
            namespace,
            sandbox_image: &self.sandbox_image,
            runtime_class_name: &self.runtime_class_name,
            sandbox_node_selector_key: &self.sandbox_node_selector_key,
            sandbox_node_selector_value: &self.sandbox_node_selector_value,
            extra_node_selector_labels: &self.extra_node_selector_labels,
            internal_secret_name: &self.internal_secret_name,
            internal_token_key: &self.internal_token_key,
            internal_control_token_key: &self.internal_control_token_key,
            ssd_headroom_tier_affinity: self.ssd_headroom_tier_affinity,
            restore_source_node_hard_pin: self.restore_source_node_hard_pin,
            node_services_ready_affinity: self.node_services_ready_affinity,
        }
    }
}

impl Default for SandboxPodConfigValues {
    fn default() -> Self {
        Self {
            node_group_placement: DEFAULT_SANDBOX_NODE_GROUP_PLACEMENT,
            sandbox_image: DEFAULT_SANDBOX_POD_IMAGE.to_string(),
            runtime_class_name: DEFAULT_SANDBOX_RUNTIME_CLASS.to_string(),
            sandbox_node_selector_key: DEFAULT_SANDBOX_NODE_SELECTOR_KEY.to_string(),
            sandbox_node_selector_value: DEFAULT_SANDBOX_NODE_SELECTOR_VALUE.to_string(),
            extra_node_selector_labels: BTreeMap::new(),
            internal_secret_name: DEFAULT_SANDBOX_INTERNAL_SECRET_NAME.to_string(),
            internal_token_key: DEFAULT_SANDBOX_INTERNAL_TOKEN_KEY.to_string(),
            internal_control_token_key: DEFAULT_SANDBOX_INTERNAL_CONTROL_TOKEN_KEY.to_string(),
            ssd_headroom_tier_affinity: DEFAULT_SANDBOX_SSD_HEADROOM_TIER_AFFINITY,
            restore_source_node_hard_pin: DEFAULT_SANDBOX_RESTORE_SOURCE_NODE_HARD_PIN,
            node_services_ready_affinity: DEFAULT_SANDBOX_NODE_SERVICES_READY_AFFINITY,
        }
    }
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

#[derive(Debug, Clone, Copy)]
pub struct SandboxPoolPodMetadata {
    pub slot_id: Uuid,
    pub config_id: Uuid,
    pub vcpu: VcpuCount,
    pub memory: MemorySize,
    pub disk_mib: Option<u32>,
    pub disk_io_limits: crate::types::DiskIoLimits,
}

pub struct SandboxPodCommon<'a> {
    pub pod_name: &'a str,
    pub sandbox_id: crate::types::SandboxId,
    pub execution_generation: i64,
    pub activation_attempt_id: Option<crate::types::SandboxActivationAttemptId>,
    pub template: &'a str,
    pub content_hash: Option<&'a str>,
    pub env: &'a BTreeMap<String, String>,
    pub traceparent: Option<String>,
}

#[derive(Debug, Clone, Copy)]
pub struct SandboxRuntimePodMetadata<'a> {
    pub sandbox_owner: &'a SandboxOwner,
    pub user_id: crate::types::UserId,
    pub org_id: crate::types::OrgId,
    pub auto_lifecycle_json: &'a str,
    pub auto_lifecycle_authority: PolicyAuthority,
    pub network_policy_json: &'a str,
    pub network_policy_generation: u64,
    pub network_policy_authority: PolicyAuthority,
    pub vcpu: Option<u32>,
    pub memory_mib: Option<u32>,
    pub disk_mib: Option<u32>,
    pub disk_io_limits: crate::types::DiskIoLimits,
    /// BYOK: render the [`SANDBOX_VOLUME_ENCRYPTION_ANNOTATION`] so the shim
    /// creates the node sandbox with its immutable encrypted-volumes flag.
    pub volume_encryption: bool,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ForkDiskRestorePodContext<'a> {
    pub source_sandbox_id: &'a str,
    pub artifact_snapshot_id: &'a str,
    pub target_owner: &'a str,
}

pub enum SandboxPodRequest<'a> {
    OnDemand {
        common: SandboxPodCommon<'a>,
        runtime: SandboxRuntimePodMetadata<'a>,
        /// Create-time isolation node pool pin. When `Some`, the rendered pod
        /// gains a `nodeSelector` entry on [`SANDBOX_PLACEMENT_POOL_LABEL_KEY`]
        /// and a matching toleration, so it lands only on the pool's tainted
        /// nodes. This is a PodSpec-only pin applied at create; it is not
        /// persisted and does not survive to later reconcile/restore builds.
        node_pool: Option<&'a NodePoolName>,
        /// Compatible node-group set (plus dedicated-group targeting) for
        /// this sandbox. `None` renders no node-group scheduling constraints
        /// — required while worker nodes do not yet carry the boundary
        /// label; a required term over an absent label would make every
        /// sandbox unschedulable.
        node_group_placement: Option<&'a NodeGroupPlacement>,
    },
    Restore {
        /// Disk restarts use retained rootfs; full restores resume VM memory.
        /// The shim translates the disk marker into a cold Start or a retry
        /// attachment after observing the local sandbox state.
        artifact_kind: crate::types::ArtifactKind,
        common: SandboxPodCommon<'a>,
        runtime: SandboxRuntimePodMetadata<'a>,
        /// Node-group placement recomputed from the *current* template
        /// visibility and org at each pod build, never a snapshot persisted on
        /// the sandbox — so a scheduler-placed restore lands inside whatever
        /// the set is now. Note it does not *relocate* a pre-bound restore: for
        /// the local-only shape that sets `direct_node_name`, a boundary the
        /// bound node fails means kubelet rejects the pod and the restore fails
        /// (see the rendering site — there is no cross-node re-drive for a
        /// pre-bound pod). Template visibility is fixed at create in v1, so
        /// that case needs a deliberate migration, not a rebuild.
        ///
        /// Passing this together with `direct_node_name` obliges the caller to
        /// have checked the source node's live node-group label against the
        /// candidate set. `nodeName` bypasses kube-scheduler, so the only thing
        /// still evaluating the rendered boundary is kubelet admission — and a
        /// rejection there is terminal for the restore, not a cross-node
        /// re-drive. The caller's check is what keeps a legal restore working;
        /// the boundary is the backstop for when that check is wrong.
        node_group_placement: Option<&'a NodeGroupPlacement>,
        /// Exact S3 fallback selected by the API for a portable restore.
        suspend_restore_storage_uri: Option<&'a str>,
        /// Node holding the trusted local suspend bytes.
        suspend_restore_source_node: Option<&'a str>,
        fork_disk_restore: Option<Box<ForkDiskRestorePodContext<'a>>>,
        /// Direct source-node assignment while no portable fallback exists.
        direct_node_name: Option<&'a str>,
        /// Source-node affinity preference once S3 fallback is available.
        preferred_node_name: Option<&'a str>,
        /// Source-aware restore shortlist (source first, then fallback
        /// candidates), rendered under `restore_source_node_hard_pin` as ORed
        /// required `matchFields: metadata.name` terms — one per node, each
        /// carrying the shared hard exclusions. Bounding the candidate set to
        /// well under kube-scheduler's 100-node prefilter floor guarantees
        /// every listed node is *scored*, so the weight-100 preferred source
        /// term wins whenever the source is feasible and a busy source lands
        /// the pod on a fallback in the same scheduling pass instead of going
        /// `Unschedulable`. Empty (or single-entry) degrades to the bare
        /// source pin.
        restore_node_shortlist: Vec<String>,
        /// Extra `spec.tolerations` appended to the rendered pod. User-API
        /// restore passes `&[]`; the cluster-controller evacuation path
        /// passes operator-configured cohort tolerations so evacuated pods
        /// can land on tainted blue/green capacity.
        extra_tolerations: &'a [PodToleration],
        /// Optional additional **required** nodeAffinity term, expressed as
        /// a single `(label_key, label_value)` pair. When `Some`, an `In`
        /// match-expression on `label_key`/`label_value` is appended to
        /// `nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution`,
        /// pinning the pod to nodes carrying that label.
        ///
        /// User-API restore passes `None`; the cluster-controller
        /// evacuation path passes `Some` with the active target cohort's
        /// key+value so evacuated sandboxes land only on the target MNG.
        extra_required_affinity: Option<(&'a str, &'a str)>,
    },
    Pooled {
        common: SandboxPodCommon<'a>,
        pool: SandboxPoolPodMetadata,
        /// Node-group placement for the warm pod. Warm pools are
        /// generic-only (pooled pods carry no org identity), so this only
        /// ever names generic groups and never a dedicated target.
        node_group_placement: Option<&'a NodeGroupPlacement>,
    },
}

/// SPACE-internal representation of a Kubernetes pod toleration.
///
/// Field names match the upstream Kubernetes `Toleration` wire format
/// (camelCase JSON keys) so a toleration block from a Kubernetes manifest
/// can be dropped straight into Helm values or a JSON-encoded env var
/// without translation.
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PodToleration {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operator: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effect: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub toleration_seconds: Option<i64>,
}

impl PodToleration {
    /// `Equal`-operator `NoSchedule` toleration for a `key=value` node taint —
    /// the shape shared by the rollout-cohort and dedicated-node-group taints.
    pub fn equal_no_schedule(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: Some(key.into()),
            operator: Some("Equal".to_string()),
            value: Some(value.into()),
            effect: Some("NoSchedule".to_string()),
            toleration_seconds: None,
        }
    }

    pub fn to_k8s_toleration(&self) -> Toleration {
        Toleration {
            effect: self.effect.clone(),
            key: self.key.clone(),
            operator: self.operator.clone(),
            toleration_seconds: self.toleration_seconds,
            value: self.value.clone(),
        }
    }
}

/// Placement override applied to cluster-controller-built evacuation pods so
/// that, during a two-MNG blue/green node rollout, they land *only* on the
/// active target cohort.
///
/// The single `(cohort_key, cohort_value)` pair drives two pieces of the
/// rendered pod spec, derived together via [`Self::to_toleration`] and
/// [`Self::to_required_affinity`]:
///
/// * a `spec.toleration` with `operator = Equal`, `effect = NoSchedule`, so
///   the pod tolerates the rollout MNG's cohort taint, and
/// * a required `spec.affinity.nodeAffinity` term matching nodes that carry
///   the same key+value as a Kubernetes label.
///
/// The `rollout-mng` Terraform module sets both the taint and the matching
/// label on the target MNG. Empty (default) means no rollout in flight —
/// evacuation pods carry no cohort placement and behave like pre-rollout
/// restores.
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EvacuationPlacement {
    pub cohort_key: String,
    pub cohort_value: String,
}

impl EvacuationPlacement {
    /// Build a placement from raw strings sourced from env / Helm. Returns
    /// `None` when either field is empty after trimming — that's the
    /// "no rollout in flight" signal.
    pub fn from_parts(
        cohort_key: impl Into<String>,
        cohort_value: impl Into<String>,
    ) -> Option<Self> {
        let cohort_key = cohort_key.into().trim().to_string();
        let cohort_value = cohort_value.into().trim().to_string();
        if cohort_key.is_empty() || cohort_value.is_empty() {
            return None;
        }
        Some(Self {
            cohort_key,
            cohort_value,
        })
    }

    /// `spec.toleration` entry that lets an evacuation pod schedule onto a
    /// node carrying the matching cohort taint.
    pub fn to_toleration(&self) -> PodToleration {
        PodToleration::equal_no_schedule(self.cohort_key.clone(), self.cohort_value.clone())
    }

    /// `(label_key, label_value)` pair plumbed through to the pod builder as
    /// an extra required nodeAffinity term, so kube-scheduler pins the
    /// evacuation pod to nodes carrying the matching cohort label.
    pub fn to_required_affinity(&self) -> (&str, &str) {
        (&self.cohort_key, &self.cohort_value)
    }
}

pub fn sandbox_pod_name(sandbox_id: crate::types::SandboxId, execution_generation: i64) -> String {
    format!(
        "space-sbx-{}-g{execution_generation}",
        sandbox_id.as_uuid().simple()
    )
}

pub fn build_sandbox_pod(config: &SandboxPodConfig<'_>, request: SandboxPodRequest<'_>) -> Pod {
    let cold_create = matches!(&request, SandboxPodRequest::OnDemand { .. });
    let empty_tolerations: &[PodToleration] = &[];
    let (
        common,
        runtime,
        checkpoint,
        suspend_restore_storage_uri,
        suspend_restore_source_node,
        fork_disk_restore,
        direct_node_name,
        preferred_node_name,
        restore_node_shortlist,
        extra_tolerations,
        extra_required_affinity,
        pool,
        node_pool,
        prefer_template_cache,
        template_cache_preference_weight,
        node_group_placement,
    ) = match request {
        SandboxPodRequest::OnDemand {
            common,
            runtime,
            node_pool,
            node_group_placement,
        } => (
            common,
            Some(runtime),
            None,
            None,
            None,
            None,
            None,
            None,
            Vec::new(),
            empty_tolerations,
            None,
            None,
            node_pool,
            true,
            DEFAULT_TEMPLATE_CACHE_PREFERENCE_WEIGHT,
            node_group_placement,
        ),
        SandboxPodRequest::Restore {
            artifact_kind,
            common,
            runtime,
            node_group_placement,
            suspend_restore_storage_uri,
            suspend_restore_source_node,
            fork_disk_restore,
            direct_node_name,
            preferred_node_name,
            restore_node_shortlist,
            extra_tolerations,
            extra_required_affinity,
        } => {
            let checkpoint = fork_disk_restore.is_none().then_some(match artifact_kind {
                crate::types::ArtifactKind::Full => "suspend",
                crate::types::ArtifactKind::Disk => "stopped",
            });
            (
                common,
                Some(runtime),
                checkpoint,
                suspend_restore_storage_uri,
                suspend_restore_source_node,
                fork_disk_restore,
                direct_node_name,
                preferred_node_name,
                restore_node_shortlist,
                extra_tolerations,
                extra_required_affinity,
                None,
                None,
                true,
                RESTORE_TEMPLATE_CACHE_PREFERENCE_WEIGHT,
                node_group_placement,
            )
        }
        SandboxPodRequest::Pooled {
            common,
            pool,
            node_group_placement,
        } => (
            common,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Vec::new(),
            empty_tolerations,
            None,
            Some(pool),
            None,
            true,
            DEFAULT_TEMPLATE_CACHE_PREFERENCE_WEIGHT,
            node_group_placement,
        ),
    };
    let SandboxPodCommon {
        pod_name,
        sandbox_id,
        execution_generation,
        activation_attempt_id,
        template,
        content_hash,
        env,
        traceparent,
    } = common;

    let mut labels = BTreeMap::new();
    labels.insert(
        "app.kubernetes.io/name".to_string(),
        "space-sandbox".to_string(),
    );
    labels.insert(
        SANDBOX_MANAGED_BY_LABEL.to_string(),
        "space-api".to_string(),
    );
    labels.insert(SANDBOX_ID_ANNOTATION.to_string(), sandbox_id.to_string());
    labels.insert(
        SANDBOX_EXECUTION_GENERATION_ANNOTATION.to_string(),
        execution_generation.to_string(),
    );
    labels.insert(
        SANDBOX_ROUTE_GENERATION_ANNOTATION.to_string(),
        execution_generation.to_string(),
    );
    if let Some(runtime) = runtime {
        labels.insert(
            SANDBOX_OWNER_ID_LABEL.to_string(),
            runtime.user_id.to_string(),
        );
        labels.insert(SANDBOX_ORG_ID_LABEL.to_string(), runtime.org_id.to_string());
    }
    if let Some(pool) = pool {
        labels.insert(
            SANDBOX_POOL_CONFIG_ID_KEY.to_string(),
            pool.config_id.to_string(),
        );
        labels.insert(
            SANDBOX_POOL_ROLE_KEY.to_string(),
            SANDBOX_POOL_ROLE_WARM.to_string(),
        );
    }

    let mut node_selector = config.base_node_selector().into_labels();
    // Create-time isolation pool pin (OnDemand only): a plain nodeSelector entry
    // on the pool discriminator label. The matching toleration is added below.
    if let Some(node_pool) = node_pool {
        node_selector.insert(
            SANDBOX_PLACEMENT_POOL_LABEL_KEY.to_string(),
            node_pool.as_str().to_string(),
        );
    }

    let mut annotations = BTreeMap::new();
    annotations.insert(SANDBOX_ID_ANNOTATION.to_string(), sandbox_id.to_string());
    annotations.insert(
        SANDBOX_EXECUTION_GENERATION_ANNOTATION.to_string(),
        execution_generation.to_string(),
    );
    if let Some(activation_attempt_id) = activation_attempt_id {
        annotations.insert(
            SANDBOX_ACTIVATION_ATTEMPT_ID_ANNOTATION.to_string(),
            activation_attempt_id.to_string(),
        );
    }
    annotations.insert(
        SANDBOX_ROUTE_GENERATION_ANNOTATION.to_string(),
        execution_generation.to_string(),
    );
    annotations.insert(
        SANDBOX_TEMPLATE_ANNOTATION.to_string(),
        template.to_string(),
    );
    if let Some(checkpoint) = checkpoint {
        annotations.insert(
            SANDBOX_CHECKPOINT_ANNOTATION.to_string(),
            checkpoint.to_string(),
        );
    }
    if let Some(storage_uri) = suspend_restore_storage_uri {
        annotations.insert(
            SANDBOX_SUSPEND_RESTORE_STORAGE_URI_ANNOTATION.to_string(),
            storage_uri.to_string(),
        );
    }
    if let Some(source_node) = suspend_restore_source_node {
        annotations.insert(
            SANDBOX_SUSPEND_RESTORE_SOURCE_NODE_ANNOTATION.to_string(),
            source_node.to_string(),
        );
    }
    if let Some(fork_disk_restore) = fork_disk_restore {
        annotations.insert(
            SANDBOX_FORK_DISK_RESTORE_ANNOTATION.to_string(),
            serde_json::to_string(&fork_disk_restore)
                .expect("fork disk restore pod context serializes"),
        );
    }
    if let Some(runtime) = runtime {
        annotations.insert(
            SANDBOX_AUTO_LIFECYCLE_ANNOTATION.to_string(),
            runtime.auto_lifecycle_json.to_string(),
        );
        annotations.insert(
            SANDBOX_AUTO_LIFECYCLE_AUTHORITY_ANNOTATION.to_string(),
            runtime.auto_lifecycle_authority.to_string(),
        );
        annotations.insert(
            SANDBOX_NETWORK_POLICY_ANNOTATION.to_string(),
            runtime.network_policy_json.to_string(),
        );
        annotations.insert(
            SANDBOX_NETWORK_POLICY_GENERATION_ANNOTATION.to_string(),
            runtime.network_policy_generation.to_string(),
        );
        annotations.insert(
            SANDBOX_NETWORK_POLICY_AUTHORITY_ANNOTATION.to_string(),
            runtime.network_policy_authority.to_string(),
        );
        let sandbox_owner = runtime.sandbox_owner.as_str().to_string();
        annotations.insert(SANDBOX_OWNER_ANNOTATION.to_string(), sandbox_owner.clone());
        annotations.insert(SANDBOX_USER_ANNOTATION.to_string(), sandbox_owner);
        if let Some(vcpu) = runtime.vcpu {
            annotations.insert(SANDBOX_VCPU_ANNOTATION.to_string(), vcpu.to_string());
        }
        if let Some(memory_mib) = runtime.memory_mib {
            annotations.insert(
                SANDBOX_MEMORY_MIB_ANNOTATION.to_string(),
                memory_mib.to_string(),
            );
        }
        if let Some(disk_mib) = runtime.disk_mib {
            annotations.insert(
                SANDBOX_DISK_MIB_ANNOTATION.to_string(),
                disk_mib.to_string(),
            );
        }
        annotations.insert(
            SANDBOX_DISK_IO_MAX_OPERATIONS_PER_SEC_ANNOTATION.to_string(),
            runtime.disk_io_limits.max_operations_per_sec().to_string(),
        );
        annotations.insert(
            SANDBOX_DISK_IO_MAX_BANDWIDTH_MIB_PER_SEC_ANNOTATION.to_string(),
            runtime
                .disk_io_limits
                .max_bandwidth_mib_per_sec()
                .to_string(),
        );
        if runtime.volume_encryption {
            annotations.insert(
                SANDBOX_VOLUME_ENCRYPTION_ANNOTATION.to_string(),
                "true".to_string(),
            );
        }
    }
    if let Some(content_hash) = content_hash {
        annotations.insert(
            SANDBOX_CONTENT_HASH_ANNOTATION.to_string(),
            content_hash.to_string(),
        );
    }
    if let Some(traceparent) = traceparent {
        annotations.insert(SANDBOX_TRACEPARENT_ANNOTATION.to_string(), traceparent);
    }
    if let Some(pool) = pool {
        let sandbox_owner = ROOT_SANDBOX_OWNER.as_str().to_string();
        annotations.insert(SANDBOX_OWNER_ANNOTATION.to_string(), sandbox_owner.clone());
        annotations.insert(SANDBOX_USER_ANNOTATION.to_string(), sandbox_owner);
        annotations.insert(
            SANDBOX_AUTO_LIFECYCLE_ANNOTATION.to_string(),
            "{}".to_string(),
        );
        annotations.insert(
            SANDBOX_AUTO_LIFECYCLE_AUTHORITY_ANNOTATION.to_string(),
            PolicyAuthority::Cluster.to_string(),
        );
        annotations.insert(
            SANDBOX_POOL_SLOT_ID_KEY.to_string(),
            pool.slot_id.to_string(),
        );
        annotations.insert(
            SANDBOX_POOL_CONFIG_ID_KEY.to_string(),
            pool.config_id.to_string(),
        );
        annotations.insert(
            SANDBOX_VCPU_ANNOTATION.to_string(),
            pool.vcpu.get().get().to_string(),
        );
        annotations.insert(
            SANDBOX_MEMORY_MIB_ANNOTATION.to_string(),
            pool.memory.as_mib_floor().to_string(),
        );
        if let Some(disk_mib) = pool.disk_mib {
            annotations.insert(
                SANDBOX_DISK_MIB_ANNOTATION.to_string(),
                disk_mib.to_string(),
            );
        }
        annotations.insert(
            SANDBOX_DISK_IO_MAX_OPERATIONS_PER_SEC_ANNOTATION.to_string(),
            pool.disk_io_limits.max_operations_per_sec().to_string(),
        );
        annotations.insert(
            SANDBOX_DISK_IO_MAX_BANDWIDTH_MIB_PER_SEC_ANNOTATION.to_string(),
            pool.disk_io_limits.max_bandwidth_mib_per_sec().to_string(),
        );
    }

    // SPACE_INTERNAL_TOKEN / SPACE_INTERNAL_CONTROL_TOKEN are deliberately
    // NOT injected into the sandbox pod env. The container declared here is
    // `pause:3.10` (or a default), but `runtimeClassName: space` causes
    // containerd-shim-space-v2 to replace it with a Firecracker microVM whose
    // kernel + userspace do not inherit the K8s container env. No consumer of
    // these bearers runs inside the sandbox pod — verified empirically and via
    // grep across `crates/` and `shim/`. Filtering user-supplied env vars by
    // these names is kept as defense-in-depth so a workload cannot accidentally
    // (or intentionally) shadow them in the pod-spec metadata.
    let env_vars: Vec<EnvVar> = env
        .iter()
        .filter_map(|(name, value)| {
            if name == DEFAULT_SANDBOX_INTERNAL_TOKEN_KEY
                || name == DEFAULT_SANDBOX_INTERNAL_CONTROL_TOKEN_KEY
            {
                return None;
            }
            Some(EnvVar {
                name: name.clone(),
                value: Some(value.clone()),
                ..Default::default()
            })
        })
        .collect();

    if cold_create {
        use base64::Engine as _;
        // Reuse exactly the filtered plaintext env already destined for this pod.
        // Encrypted fields remain on their existing credential-manager path.
        let mut wire = Vec::new();
        for entry in &env_vars {
            wire.extend_from_slice(entry.name.as_bytes());
            wire.push(b'=');
            wire.extend_from_slice(entry.value.as_deref().expect("literal pod env").as_bytes());
            wire.push(0);
        }
        annotations.insert(
            SANDBOX_PROCESS_ENV_ANNOTATION.to_string(),
            base64::engine::general_purpose::STANDARD.encode(wire),
        );
    }

    // A placement belongs to exactly one org, and this pod has exactly one
    // trusted org identity (`runtime.org_id`, from the sandbox row). If they
    // disagree — or the pod has no org at all, which is every pooled pod — the
    // candidate set was computed for someone else and none of it can be trusted
    // for this pod: not the dedicated groups, and not the shared ones either,
    // since "shared" is only shared among orgs a caller was allowed to include.
    // So the check is on the placement's org rather than on whether it happens
    // to carry dedicated capacity: an all-shared candidate set derived for the
    // wrong org would otherwise render a perfectly satisfiable boundary and
    // schedule the sandbox onto a pool it was never cleared for.
    //
    // One arm is deliberately narrower than that: an *org-less* pod — every
    // pooled pod — carrying a set resolved for some org is judged on whether
    // that set targets dedicated capacity, because shared capacity is where a
    // warm pod belongs and it has no org of its own to disagree. See the arms
    // below, which spell out all four combinations; a placement may now carry
    // no org at all, which is the pooled shape resolved for the pooled subject.
    //
    // On a mismatch the rendered boundary becomes `In (C)` AND `NotIn (C)` (at
    // the rendering site below) — two conjuncts in one term, satisfiable by no
    // node — so no node admits the pod. Which way it fails depends on how it
    // was placed: a scheduler-placed pod stays Unschedulable, while a pod
    // pre-bound via `spec.nodeName` is rejected by kubelet at admission with
    // `reason: NodeAffinity`. Either way it never runs on capacity it was not
    // cleared for, which is the property being bought. Withholding the dedicated toleration alone would not be
    // fail-closed: the group label and the dedicated taint are two independent
    // control-plane writes, so a node whose label landed while its taint write
    // is still retrying would admit the pod, collapsing the
    // taint-excludes/affinity-attracts pair to a single control in exactly the
    // case we would be calling safe.
    let pod_org = runtime.map(|runtime| runtime.org_id);
    let placement_org = node_group_placement.map(NodeGroupPlacement::caller_org);
    // The candidate set is derived from an (org, template) pair, so both halves
    // must match the pod being rendered. Checking only the org would leave a pod
    // built with a *different* template than the placement was resolved for — a
    // generic template's set, which legally includes shared groups, reused for
    // an org-specific one — and the boundary would then admit shared capacity
    // for a rootfs that must never touch it.
    let template_mismatch =
        node_group_placement.is_some_and(|placement| placement.template().as_str() != template);
    // Both sides are now `Option<OrgId>` — the pod's org is absent for exactly
    // the pods a placement's is: pooled ones. So the common case is one
    // comparison, and only the org-less-pod-with-an-org-placement case needs a
    // rule of its own.
    let org_mismatch = template_mismatch
        || match (placement_org, pod_org) {
            // No placement at all: nothing to disagree with.
            (None, _) => false,
            // An org-less placement on an org-less pod agrees; on an
            // org-owned pod it does not — a set resolved without an org
            // identity was not resolved for that sandbox, even though (being
            // generic-only) it is narrower than what that org would have got.
            (Some(None), pod_org) => pod_org.is_some(),
            // A pod with no org of its own — every pooled pod — carrying a set
            // resolved *for* an org. Shared capacity is still fine for it (there
            // is no org to disagree with, and a warm pod belongs on the shared
            // pool), so the boundary applies as written. What it must never take
            // is *dedicated* capacity: a generic warm pod parked on reserved
            // hardware could later be claimed by an unrelated org.
            (Some(Some(_)), None) => {
                node_group_placement.is_some_and(NodeGroupPlacement::targets_dedicated_capacity)
            }
            (Some(Some(placement_org)), Some(pod_org)) => placement_org != pod_org,
        };
    if org_mismatch {
        tracing::error!(
            sandbox_id = %sandbox_id,
            ?placement_org,
            ?pod_org,
            placement_template = ?node_group_placement.map(NodeGroupPlacement::template),
            pod_template = template,
            template_mismatch,
            "node-group placement does not match the pod it is rendering; rendering an \
             unsatisfiable node-group boundary so no node admits the pod (Unschedulable if \
             scheduler-placed, rejected by kubelet with reason NodeAffinity if pre-bound) \
             rather than letting it onto capacity it was not cleared for"
        );
    }
    let dedicated_allowed = !org_mismatch
        && node_group_placement.is_some_and(NodeGroupPlacement::targets_dedicated_capacity);
    let mut toleration_list: Vec<Toleration> = extra_tolerations
        .iter()
        .map(PodToleration::to_k8s_toleration)
        .collect();
    // The pool taint toleration pairs with the nodeSelector entry above so a
    // placed pod can schedule onto the pool's `NoSchedule`-tainted nodes.
    // Operator `Exists` tolerates the taint regardless of its value.
    if node_pool.is_some() {
        toleration_list.push(Toleration {
            key: Some(SANDBOX_PLACEMENT_POOL_LABEL_KEY.to_string()),
            operator: Some("Exists".to_string()),
            effect: Some("NoSchedule".to_string()),
            ..Default::default()
        });
    }
    // Dedicated node-group taint toleration: emitted only when the placement
    // targets the caller's own dedicated groups AND that org matches the pod's
    // trusted runtime identity. Withholding it keeps a sandbox without a
    // dedicated target off reserved hardware only where the scheduler is doing
    // the placing — `NoSchedule` is scheduler-evaluated, so a pre-bound pod
    // ignores the taint entirely and the required boundary is its only fence.
    if dedicated_allowed
        && let Some(toleration) =
            node_group_placement.and_then(NodeGroupPlacement::dedicated_toleration)
    {
        toleration_list.push(toleration.to_k8s_toleration());
    }
    let tolerations: Option<Vec<Toleration>> =
        (!toleration_list.is_empty()).then_some(toleration_list);
    // A restore that knows its source node renders the source pin one of
    // three ways. Legacy (flag off): a weight-100 preferred affinity term
    // only. Source-aware with a shortlist: required ORed `matchFields:
    // metadata.name` terms for source + fallbacks, PLUS the weight-100
    // preferred source term — the scheduler filters within the shortlist and
    // the preference lands the pod on the source whenever it is feasible,
    // falling to a fallback in the same pass otherwise. Source-aware without
    // a shortlist (cold node cache): a bare required source pin, no
    // preference — the in-request watch handles the Unschedulable case.
    // `preferred_node_name` is not filtered for emptiness here, deliberately:
    // the match below reads it to decide whether to emit the required shortlist
    // at all, so dropping an empty value would widen a required constraint to
    // the whole fleet. Emptiness is instead checked where it matters, on the
    // dedicated-preference gate further down.
    let (required_source_node_names, preferred_source_node_name): (Vec<String>, Option<&str>) =
        if config.restore_source_node_hard_pin {
            match preferred_node_name {
                Some(source) if restore_node_shortlist.len() > 1 => {
                    (restore_node_shortlist, Some(source))
                }
                Some(source) => (vec![source.to_string()], None),
                None => (Vec::new(), None),
            }
        } else {
            (Vec::new(), preferred_node_name)
        };
    // Scoring terms are suppressed for every pinned shape. For `spec.nodeName`
    // (direct) and a bare required pin they are dead weight (one candidate).
    // For a shortlist they would *compete with the source preference inside
    // the NodeAffinity score*: preferred weights are capped at 100 by
    // Kubernetes validation, so a fallback matching template-cache (50) +
    // best ssd tier (50) would tie the source's weight-100 term and let the
    // resource-scoring plugins pick a fallback over a feasible source — the
    // exact off-source placement this feature removes. With them suppressed
    // the source is the only NodeAffinity preference (fallbacks score 0
    // there) and fallback choice falls to the scheduler's resource plugins;
    // the hard ssd-placement-block still excludes unsafe nodes.
    // A direct `spec.nodeName` assignment is preserved even when a node-group
    // placement is present. `nodeName` bypasses kube-scheduler, so the required
    // affinity below is evaluated only by kubelet admission (terminally — see
    // the rendering site) and the `NoSchedule` taint not at all. The caller is
    // therefore the enforcement point that keeps a legal restore working, not
    // this renderer. Callers that pass both a `direct_node_name` and a placement
    // MUST first resolve the source node's live
    // `node-restriction.kubernetes.io/space-node-group` label (the Node object,
    // not the mirrored `nodes.node_group_id` column, which lags a reassignment)
    // set. What to do then depends on the restore: with a portable snapshot,
    // pass `direct_node_name: None` and let kube-scheduler place the pod inside
    // the set. **Without one — the local-only case that sets this field in the
    // first place — dropping the pin is not enough**: the pod would schedule
    // onto a legal node that has neither the local suspend bytes nor a storage
    // URI, so the restore breaks after consuming a network slot. Suspend to S3
    // first, then restore under the boundary.
    //
    // Converting the pin here instead was tried and reverted: a local-only
    // restore has no portable snapshot, so unconditional scheduler filtering
    // subjects the resume to cordons and unrelated taints and strands a sandbox
    // with nowhere else to go — while buying no isolation, since the memory and
    // disk are already on that node. A source that has left the org's legal set
    // is a migration problem (evict with a portable snapshot, then re-place),
    // and this renderer has no cluster reads with which to tell the two cases
    // apart.
    let single_target_node = direct_node_name.is_some() || !required_source_node_names.is_empty();
    let preferred_template_label = if prefer_template_cache && !single_target_node {
        content_hash.map(|content_hash| template_build_presence_label_key(template, content_hash))
    } else {
        None
    };

    Pod {
        metadata: ObjectMeta {
            name: Some(pod_name.to_string()),
            namespace: Some(config.namespace.to_string()),
            labels: Some(labels),
            annotations: Some(annotations),
            ..Default::default()
        },
        spec: Some(PodSpec {
            automount_service_account_token: Some(false),
            runtime_class_name: Some(config.runtime_class_name.to_string()),
            restart_policy: Some("Never".to_string()),
            termination_grace_period_seconds: Some(10),
            node_selector: Some(node_selector),
            node_name: direct_node_name.map(str::to_string),
            affinity: sandbox_node_affinity(SandboxNodeAffinityInput {
                required_source_node_names: &required_source_node_names,
                preferred_source_node_name,
                preferred_template_label: preferred_template_label.as_deref(),
                preferred_template_weight: template_cache_preference_weight,
                extra_required_label: extra_required_affinity,
                // Rendered unconditionally, including for a pre-bound
                // (`spec.nodeName`) pod. kube-scheduler is skipped there, but
                // kubelet's own admission re-evaluates `spec.nodeSelector` and
                // `requiredDuringSchedulingIgnoredDuringExecution` against the
                // node it is bound to and rejects the pod (`reason:
                // NodeAffinity`) on a mismatch — the same path this pod's
                // worker `nodeSelector` already depends on. So the boundary is a
                // real backstop exactly where the scheduler cannot help. Note
                // the asymmetry with the dedicated toleration above:
                // `NoSchedule` is scheduler-only, so *that* is the conjunct a
                // pre-bound pod renders inertly.
                //
                // A kubelet rejection here is **terminal, not a re-drive**: the
                // restore path's cross-node recovery keys off
                // `SchedulingOutcome::Unschedulable`, and `scheduling_outcome_of`
                // short-circuits to `Scheduled` for any pod with `spec.nodeName`
                // populated, so the rejected pod fails instead of being replaced
                // cross-node. That is the intended trade for a local-only
                // restore, which has no portable snapshot to re-place from: a
                // sandbox whose source node left the legal set needs migration
                // (suspend to S3, then restore under the boundary), and failing
                // is correct where the alternative is running an org's rootfs on
                // hardware the boundary excludes. Note what the caller must
                // *not* do about it: dropping `direct_node_name` to let the
                // scheduler pick within the set is unavailable, because that
                // field is populated only when there is no storage URI, so any
                // other node has neither the local bytes nor a snapshot to
                // restore from. The caller pre-checks the source node's live
                // label and surfaces a migration-required error before any pod
                // exists, which keeps this backstop the last line rather than
                // the plan.
                required_node_group_expressions: node_group_placement
                    .map(|placement| {
                        let expression = placement.required_match_expression();
                        if org_mismatch {
                            // `In (C)` AND `NotIn (C)` on the same key: two
                            // conjuncts in one term, so no node can satisfy
                            // both, with no invented sentinel value.
                            // Unschedulable when the scheduler places the pod;
                            // a kubelet admission rejection when it is
                            // pre-bound.
                            let mut contradiction = expression.clone();
                            contradiction.operator = "NotIn".to_string();
                            vec![expression, contradiction]
                        } else {
                            vec![expression]
                        }
                    })
                    .unwrap_or_default(),
                // Scoring term, suppressed whenever the pod already has a
                // source-node preference of its own — a hard pin, a shortlist,
                // *or* the legacy soft preference, which is the default
                // (`DEFAULT_SANDBOX_RESTORE_SOURCE_NODE_HARD_PIN` is false).
                // Both cap at the Kubernetes maximum weight of 100, so an
                // unconditional dedicated term ties the source preference and
                // hands the choice to the resource plugins — an off-source
                // restore that pulls the snapshot from S3 for nothing. The
                // toleration above is unaffected: a dedicated source must still
                // be tolerable.
                preferred_dedicated_term: node_group_placement
                    .filter(|_| {
                        dedicated_allowed
                            && !single_target_node
                            // Emptiness is checked here, not upstream on
                            // `preferred_node_name`: filtering it before the
                            // hard-pin match below would drop the entire
                            // required shortlist along with it.
                            && !preferred_source_node_name
                                .is_some_and(|name| !name.is_empty())
                    })
                    .and_then(NodeGroupPlacement::preferred_dedicated_term),
                // Hard disk-safety exclusion. A required source pin still goes
                // through the scheduler, so the pin AND this NotIn term are ANDed:
                // if the source is placement-blocked the pod stays Unschedulable
                // and the create path re-drives it cross-node — dropping the term
                // would instead schedule the restore onto a disk-pressured source.
                // A direct `spec.nodeName` bypasses the scheduler, so this term
                // is skipped there. Note the divergence from the node-group
                // boundary above, which is *not* skipped: kubelet admission
                // evaluates required nodeAffinity, so this exclusion would in
                // fact still bind on a pre-bound pod. Leaving the existing
                // suppression alone is deliberate — flipping it would start
                // failing local-only restores whose source node is
                // disk-pressured, a live behavior change beyond this change's
                // scope. Worth revisiting with the disk-pressure owners.
                ssd_placement_block: config.ssd_headroom_tier_affinity
                    && direct_node_name.is_none(),
                // Soft headroom scoring. Dead weight when a single node is already
                // the only candidate (direct nodeName or a required source pin).
                ssd_headroom_tier_bias: config.ssd_headroom_tier_affinity && !single_target_node,
                // Soft node-local-service readiness bias, suppressed for the
                // same reason as the headroom scoring: with the candidate set
                // already pinned to one node (or to a required shortlist whose
                // scoring must stay owned by the source preference) the term
                // cannot change the outcome. The API drops not-ready nodes from
                // the restore shortlist instead, so the signal still applies
                // there — as a candidate filter rather than a score.
                node_services_ready_bias: config.node_services_ready_affinity
                    && !single_target_node,
            }),
            tolerations,
            containers: vec![Container {
                name: "sandbox".to_string(),
                image: Some(config.sandbox_image.to_string()),
                image_pull_policy: Some("IfNotPresent".to_string()),
                env: (!env_vars.is_empty()).then_some(env_vars),
                resources: Some(sandbox_network_slot_resources()),
                ..Default::default()
            }],
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Resource requirements pinning every sandbox pod to exactly one node-manager
/// network slot. Request and limit are equal (extended resources must be
/// integers and cannot be overcommitted), so kube-scheduler will only place a
/// sandbox pod on a node whose advertised `network-slots` capacity has an idle
/// slot left.
fn sandbox_network_slot_resources() -> ResourceRequirements {
    let one = BTreeMap::from([(
        SANDBOX_NETWORK_SLOTS_RESOURCE.to_string(),
        Quantity("1".to_string()),
    )]);
    ResourceRequirements {
        requests: Some(one.clone()),
        limits: Some(one),
        ..Default::default()
    }
}

/// Kubernetes `NodeSelectorTerm.matchFields` key naming the node's own name.
/// A required `metadata.name In [<node>]` term hits kube-scheduler's
/// NodeAffinity PreFilter NodeNames shortcut: exactly that node is evaluated
/// instead of the adaptive `percentageOfNodesToScore` sample.
pub const NODE_NAME_MATCH_FIELD: &str = "metadata.name";

/// `matchFields` requirement pinning a term to a single node by name.
fn node_name_in(node_name: &str) -> NodeSelectorRequirement {
    NodeSelectorRequirement {
        key: NODE_NAME_MATCH_FIELD.to_string(),
        operator: "In".to_string(),
        values: Some(vec![node_name.to_string()]),
    }
}

/// Every input to a sandbox pod's `spec.affinity.nodeAffinity`. Grouped into a
/// struct so each of the required/preferred terms is named at the call site
/// rather than being one of a run of positional booleans.
struct SandboxNodeAffinityInput<'a> {
    required_source_node_names: &'a [String],
    preferred_source_node_name: Option<&'a str>,
    preferred_template_label: Option<&'a str>,
    preferred_template_weight: i32,
    extra_required_label: Option<(&'a str, &'a str)>,
    required_node_group_expressions: Vec<NodeSelectorRequirement>,
    preferred_dedicated_term: Option<PreferredSchedulingTerm>,
    ssd_placement_block: bool,
    ssd_headroom_tier_bias: bool,
    node_services_ready_bias: bool,
}

fn sandbox_node_affinity(input: SandboxNodeAffinityInput<'_>) -> Option<Affinity> {
    let SandboxNodeAffinityInput {
        required_source_node_names,
        preferred_source_node_name,
        preferred_template_label,
        preferred_template_weight,
        extra_required_label,
        required_node_group_expressions,
        preferred_dedicated_term,
        ssd_placement_block,
        ssd_headroom_tier_bias,
        node_services_ready_bias,
    } = input;
    let required_source_node_names: Vec<&String> = required_source_node_names
        .iter()
        .filter(|value| !value.is_empty())
        .collect();
    let preferred_source_node_name = preferred_source_node_name.filter(|value| !value.is_empty());
    let preferred_template_label = preferred_template_label.filter(|value| !value.is_empty());
    let extra_required_label =
        extra_required_label.filter(|(key, value)| !key.is_empty() && !value.is_empty());
    if required_source_node_names.is_empty()
        && preferred_source_node_name.is_none()
        && preferred_template_label.is_none()
        && extra_required_label.is_none()
        && required_node_group_expressions.is_empty()
        && preferred_dedicated_term.is_none()
        && !ssd_placement_block
        && !ssd_headroom_tier_bias
        && !node_services_ready_bias
    {
        return None;
    }

    // Required node selectors are ANDed within a single term and ORed across
    // terms. The hard placement-block exclusion, the node-group boundary, and
    // any caller-supplied required label sit together so a pod must satisfy
    // all of them; every ORed shortlist term repeats the shared expressions,
    // so a drain-cohort pin or a source pin can never let a pod escape the
    // node-group boundary (or vice versa). `NotIn` is also satisfied by nodes
    // that lack the block label, so unlabeled / older-node-ctl workers stay
    // eligible. The hard exclusion is independent of the soft tier bias: a
    // source-pinned pod suppresses the scoring terms but must keep the
    // exclusion so a placement-blocked source leaves it Unschedulable for the
    // create-path re-drive instead of overloading that node.
    let mut match_expressions: Vec<NodeSelectorRequirement> = Vec::new();
    if let Some((label_key, label_value)) = extra_required_label {
        match_expressions.push(NodeSelectorRequirement {
            key: label_key.to_string(),
            operator: "In".to_string(),
            values: Some(vec![label_value.to_string()]),
        });
    }
    match_expressions.extend(required_node_group_expressions);
    if ssd_placement_block {
        match_expressions.push(NodeSelectorRequirement {
            key: SSD_PLACEMENT_BLOCKED_LABEL.to_string(),
            operator: "NotIn".to_string(),
            values: Some(vec![SSD_PLACEMENT_BLOCKED_VALUE.to_string()]),
        });
    }
    // Required source-node pin / shortlist. Kubernetes validation restricts a
    // `matchFields: metadata.name` requirement to a single value, so a
    // multi-node shortlist renders as one ORed term per node — each term
    // repeating the shared hard exclusions (terms OR, their contents AND).
    // kube-scheduler's NodeAffinity PreFilter unions the names across terms,
    // so only the listed nodes are evaluated regardless of the adaptive
    // `percentageOfNodesToScore` sample. A node that is full/blocked/gone
    // simply fails filtering; if every listed node fails, the pod is left
    // `Unschedulable`, which the API restore path observes and re-drives
    // cross-node in the same request.
    let node_selector_terms: Vec<NodeSelectorTerm> = if required_source_node_names.is_empty() {
        vec![NodeSelectorTerm {
            match_expressions: (!match_expressions.is_empty()).then_some(match_expressions.clone()),
            match_fields: None,
        }]
    } else {
        required_source_node_names
            .iter()
            .map(|node_name| NodeSelectorTerm {
                match_expressions: (!match_expressions.is_empty())
                    .then_some(match_expressions.clone()),
                match_fields: Some(vec![node_name_in(node_name)]),
            })
            .collect()
    };
    let required_during_scheduling_ignored_during_execution = node_selector_terms
        .iter()
        .any(|term| term.match_expressions.is_some() || term.match_fields.is_some())
        .then_some(NodeSelector {
            node_selector_terms,
        });

    let preferred_during_scheduling_ignored_during_execution = {
        let mut preferred = Vec::new();
        if let Some(node_name) = preferred_source_node_name {
            preferred.push(PreferredSchedulingTerm {
                weight: RESTORE_SOURCE_NODE_PREFERENCE_WEIGHT,
                preference: NodeSelectorTerm {
                    match_fields: Some(vec![node_name_in(node_name)]),
                    ..Default::default()
                },
            });
        }
        if let Some(label) = preferred_template_label {
            // The template-cache preference is conditioned on service readiness
            // when that bias is on (requirements within one term are ANDed), so
            // a cached-but-rolling node scores nothing here. Both weights cap at
            // the Kubernetes maximum of 100, so leaving them independent would
            // let cache-on-a-rolling-node *tie* readiness-on-an-uncached-node
            // and hand the choice to the remaining scoring plugins — the exact
            // daemon wait this bias exists to avoid. Gating instead of
            // re-weighting keeps readiness strictly dominant: the weakest
            // serviced node still scores the readiness term (100) while the
            // strongest rolling node can only reach the disk-tier weight (50).
            // The cost is narrow and intentional: while a node is rolling its
            // cache locality is ignored — and node-ctl, which drives template
            // pulls, is down there anyway.
            let mut match_expressions = vec![NodeSelectorRequirement {
                key: label.to_string(),
                operator: "In".to_string(),
                values: Some(vec![TEMPLATE_PRESENT_LABEL_VALUE.to_string()]),
            }];
            if node_services_ready_bias {
                match_expressions.push(node_services_ready_requirement());
            }
            preferred.push(PreferredSchedulingTerm {
                weight: preferred_template_weight,
                preference: NodeSelectorTerm {
                    match_expressions: Some(match_expressions),
                    ..Default::default()
                },
            });
        }
        if node_services_ready_bias {
            preferred.push(PreferredSchedulingTerm {
                weight: NODE_SERVICES_READY_PREFERENCE_WEIGHT,
                preference: NodeSelectorTerm {
                    match_expressions: Some(vec![node_services_ready_requirement()]),
                    ..Default::default()
                },
            });
        }
        if let Some(mut term) = preferred_dedicated_term {
            // Gate the dedicated preference on service readiness for exactly
            // the reason the template-cache preference above is gated: both cap
            // at the Kubernetes maximum weight of 100, so an ungated dedicated
            // term lets a *rolling* dedicated node tie a ready one and hand the
            // choice to the remaining scoring plugins — the daemon wait the
            // readiness bias exists to avoid. ANDing the requirement into the
            // term (requirements within one term are ANDed) makes a rolling
            // dedicated node score nothing here instead.
            if node_services_ready_bias {
                term.preference
                    .match_expressions
                    .get_or_insert_with(Vec::new)
                    .push(node_services_ready_requirement());
            }
            preferred.push(term);
        }
        if ssd_headroom_tier_bias {
            preferred.extend(ssd_headroom_tier_preferred_terms());
        }
        (!preferred.is_empty()).then_some(preferred)
    };

    Some(Affinity {
        node_affinity: Some(NodeAffinity {
            required_during_scheduling_ignored_during_execution,
            preferred_during_scheduling_ignored_during_execution,
        }),
        ..Default::default()
    })
}

/// "This node's node-local services are not known to be down" — satisfied by a
/// node labelled ready *and* by an unlabeled one. Shared by the standalone
/// readiness preference and the readiness-gated template-cache preference so
/// the two can never drift apart.
fn node_services_ready_requirement() -> NodeSelectorRequirement {
    NodeSelectorRequirement {
        key: NODE_SERVICES_READY_LABEL.to_string(),
        operator: "NotIn".to_string(),
        values: Some(vec![NODE_SERVICES_NOT_READY_VALUE.to_string()]),
    }
}

const RESTORE_SOURCE_NODE_PREFERENCE_WEIGHT: i32 = 100;
const DEFAULT_TEMPLATE_CACHE_PREFERENCE_WEIGHT: i32 = 100;
const RESTORE_TEMPLATE_CACHE_PREFERENCE_WEIGHT: i32 = 50;

/// Preferred-affinity weight of a node whose node-local service pods are
/// `Ready` ([`NODE_SERVICES_READY_LABEL`]).
///
/// The Kubernetes maximum, matching the strongest existing preference
/// (template-cache locality). A node mid-DaemonSet-restart costs a create the
/// full daemon-restart wait — seconds, versus the fraction of a second a
/// template-cache miss costs on a node that is otherwise healthy — and the
/// cache is not even usable while node-ctl (which drives template pulls) is
/// down. Weighting it below the cache term would let a cached-but-rolling node
/// keep winning, which is the placement this bias exists to stop.
const NODE_SERVICES_READY_PREFERENCE_WEIGHT: i32 = 100;

/// Preferred-affinity weight of a healthy node ([`SsdHeadroomTier::MAX`]).
/// Capped below the weight-100 source-node and default template-cache
/// preferences so disk headroom cannot outrank those strong locality signals.
/// Restore template-cache affinity intentionally shares this weight because it
/// is only a secondary hint below source-node locality.
const SSD_HEADROOM_HEALTHY_WEIGHT: i32 = 50;

/// Top of the `0..=10` headroom scale some workers carry during a mixed-version
/// rollout, when sandbox pod specs are rendered before every node-ctl DaemonSet
/// has caught up: an older node-ctl encodes headroom on a finer free-space
/// scale (`⌊free × 10⌋`) instead of the `0..=3` usage tiers. The healthy term
/// keeps matching those workers' healthy values (`MAX..=LEGACY_DECILE_MAX`) so a
/// near-empty node carrying an old `4`–`10` label keeps the healthy weight
/// instead of matching no term, dropping to zero, and inverting the bias toward
/// fuller nodes.
///
/// The range starts at `MAX` (`3`), not at every value that is healthy on the
/// finer scale, because `"2"` collides: on the finer scale `"2"` is
/// `free ∈ [0.20, 0.30)` (usage 70–80%, healthy), but the `0..=3` scale writes
/// `"2"` for a *filling* 80–90%-full node. A label value alone can't say which
/// scale produced it, so matching `"2"` in the healthy term would *permanently*
/// over-weight genuinely-filling nodes — worse than the alternative, where a
/// not-yet-relabeled `"2"` worker is transiently under-weighted (it scores the
/// tier-2 weight, still above every fuller node) for the one reconcile tick
/// until node-ctl relabels it to `"3"`. `"3"` is healthy on both scales, so it
/// is the safe lower bound.
///
/// Inert once every node carries the `0..=3` usage label; remove once no worker
/// can still be on the older node-ctl.
const SSD_HEADROOM_LEGACY_DECILE_MAX: u8 = 10;

/// Preferred-affinity weight for a node at the given headroom tier.
///
/// A convex ramp `healthy × (tier / max)²`: the single healthy tier scores the
/// full [`SSD_HEADROOM_HEALTHY_WEIGHT`], and the weight falls away steeply as a
/// node fills toward the disk gate, so among sub-healthy nodes the scheduler
/// still prefers the least-full one and the near-full tier (`0`) scores nothing
/// at all. "Flat across healthy, steep near the gate."
fn ssd_headroom_tier_weight(tier: u8) -> i32 {
    let max = i32::from(SsdHeadroomTier::MAX.as_u8());
    let tier = i32::from(tier);
    (SSD_HEADROOM_HEALTHY_WEIGHT * tier * tier) / (max * max)
}

/// One preferred scheduling term per headroom tier that carries a non-zero
/// weight (`1..=MAX`), each matching nodes whose [`SSD_HEADROOM_TIER_LABEL`]
/// equals that tier. A node matches exactly one term (its current tier); the
/// near-full tier `0` and unlabeled nodes match none and score 0. Because the
/// whole healthy range collapses into the single top tier, every healthy node
/// ties on the placement score and kube-scheduler spreads new sandboxes across
/// them via its own tie-break/topology rather than piling onto the emptiest.
///
/// The healthy term additionally matches the legacy decile values
/// `MAX..=`[`SSD_HEADROOM_LEGACY_DECILE_MAX`] for rollout safety — see that
/// constant for why the range starts at `MAX` (the `"2"` value collides between
/// schemes) and why the danger terms need no such compatibility.
fn ssd_headroom_tier_preferred_terms() -> impl Iterator<Item = PreferredSchedulingTerm> {
    (1..=SsdHeadroomTier::MAX.as_u8()).filter_map(|tier| {
        let weight = ssd_headroom_tier_weight(tier);
        if weight == 0 {
            return None;
        }
        let values: Vec<String> = if tier == SsdHeadroomTier::MAX.as_u8() {
            (tier..=SSD_HEADROOM_LEGACY_DECILE_MAX)
                .map(|value| value.to_string())
                .collect()
        } else {
            vec![tier.to_string()]
        };
        Some(PreferredSchedulingTerm {
            weight,
            preference: NodeSelectorTerm {
                match_expressions: Some(vec![NodeSelectorRequirement {
                    key: SSD_HEADROOM_TIER_LABEL.to_string(),
                    operator: "In".to_string(),
                    values: Some(values),
                }]),
                ..Default::default()
            },
        })
    })
}

fn digest_prefix(value: &str) -> String {
    let digest = Sha256::digest(value.as_bytes());
    let mut out = String::with_capacity(24);
    for byte in digest.iter().take(12) {
        write!(&mut out, "{byte:02x}").expect("writing to String cannot fail");
    }
    out
}

fn slug(value: &str, max_len: usize) -> String {
    let mut out = String::new();
    let mut last_dash = false;
    for ch in value.chars() {
        let next = if ch.is_ascii_alphanumeric() {
            ch.to_ascii_lowercase()
        } else {
            '-'
        };
        if next == '-' {
            if out.is_empty() || last_dash {
                continue;
            }
            last_dash = true;
        } else {
            last_dash = false;
        }
        out.push(next);
        if out.len() >= max_len {
            break;
        }
    }
    while out.ends_with('-') {
        out.pop();
    }
    if out.is_empty() { "x".to_string() } else { out }
}

/// Node label key indicating one exact ready local template build exists.
pub fn template_build_presence_label_key(template: &str, content_hash: &str) -> String {
    format!(
        "{TEMPLATE_LABEL_DOMAIN}/{TEMPLATE_BUILD_LABEL_NAME_PREFIX}-{}-{}",
        slug(template, 18),
        digest_prefix(&format!("{template}\0{content_hash}"))
    )
}

pub fn is_template_presence_metadata_key(key: &str) -> bool {
    key.starts_with(TEMPLATE_BUILD_LABEL_KEY_PREFIX)
}

pub fn is_template_presence_label_key(key: &str) -> bool {
    is_template_presence_metadata_key(key)
}

/// Node annotation key requesting that one template reference be materialized
/// on this node.
///
/// The annotation value is a full [`TemplateReference`], for example
/// `seed:latest`, `code-runtime:release-2026-05-03`, or `seed@sha256:...`.
pub fn template_write_annotation_key() -> &'static str {
    TEMPLATE_WRITE_ANNOTATION_KEY
}

pub fn template_write_annotation_key_for(reference: &TemplateReference) -> String {
    format!(
        "{TEMPLATE_WRITE_ANNOTATION_KEY}-{}",
        digest_prefix(&reference.to_string())
    )
}

pub fn is_template_write_annotation_key(key: &str) -> bool {
    // The `template-write-*` prefix is reserved for per-reference pull
    // requests. Do not add unrelated annotation keys under it: node-local
    // controllers will parse every matching annotation as a template write.
    key == TEMPLATE_WRITE_ANNOTATION_KEY
        || key
            .strip_prefix(TEMPLATE_WRITE_ANNOTATION_KEY)
            .is_some_and(|suffix| suffix.starts_with('-') && suffix.len() > 1)
}

pub fn parse_template_write_annotation(key: &str, value: &str) -> Option<TemplateReference> {
    is_template_write_annotation_key(key).then_some(())?;
    let value = value.trim();
    (value.contains(':') || value.contains('@')).then_some(())?;
    TemplateReference::from_str(value).ok()
}

// This file is itself loaded via `#[path = "k8s.rs"]` (as `mod kubernetes`
// in lib.rs), so rustc resolves its children in `src/` rather than under a
// `k8s/` directory. The explicit path keeps the test file where the
// sibling-file convention expects it without renaming the module.
#[cfg(test)]
#[path = "k8s/tests.rs"]
mod tests;
