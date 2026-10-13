use pplx_space_common_core::types::SandboxId;

use super::per_sandbox_setup::{NetworkPolicySetup, NodeFilesystemSetup, PerSandboxSetup};
use super::pool::pooled_route_activation_id;
use super::sandbox_pod_view::quantity_as_u64;
use super::store::{ActiveExecutionStatusRow, PlacementRetryDisposition, SuspendExecutionClaim};
use super::support::*;
use super::wait_ready::{CommandReadyPod, PodProxyWaitTarget};
use super::*;
use crate::controllers::db_contention_error;
use crate::controllers::substrate::DbContext;
use crate::controllers::substrate::PooledClaimRefusal;
use crate::controllers::substrate::{
    AdmissionPoolClaim, AdmittedPoolClaim, AdmittedPoolClaimLookup, CreateBootMode,
    PooledExecutionClaim, PooledExecutionClaimAttempt, PooledExecutionClaimRequest,
    PooledPrepareFailureCleanup, PublishedLocalArtifact, create_wait_should_preserve_node_work,
    enqueue_artifact_backup_with_client,
};
use crate::models::{ClusterPoolConfigRequest, ClusterPoolConfigResponse};
use crate::node_group_placement::{
    NodeGroupPlacementResolveError, PlacementSubject, ResolvePath as NodeGroupResolvePath,
    ResolvedPlacement,
};
use pplx_space_cluster_pool::{OpenPoolSlotDeleteLock, WarmPoolStore};
use pplx_space_common_core::{
    sql,
    types::{
        ArtifactKind, AutoLifecyclePolicy, BackupSource, ExecutionGeneration, NodeAddress, NodeId,
        NodeManagedBy as SpaceNodeManagedBy, PodUid, PolicyAuthority, SandboxLifecycle,
        SandboxTransitionTargetState,
    },
};
use pplx_space_events::{
    SandboxEvent, SandboxEventBoolField, SandboxEventDetails, SandboxEventSource, SandboxEventType,
};
use rand::RngExt;
use sqlx::{Postgres, Transaction};

mod node_eligibility;
mod pooled;
mod substrate_adapter;
mod suspend;
mod teardown;
pub(super) use node_eligibility::{
    NetworkSlotCapacityPreflight, network_slot_capacity_preflight, node_network_slot_available,
    node_not_placement_blocked, node_schedulable, node_services_not_ready,
};
#[cfg(test)]
use pooled::pooled_metadata_update_error_is_reclaimable;
pub(super) use pooled::pooled_route_identity_adoption_request;
use pooled::{pooled_wake_request, wait_for_pooled_split_receipt_capability};
#[cfg(test)]
use substrate_adapter::create_ready_timeout_error;
use suspend::{GenerationSuspendOutcome, await_generation_suspend};
#[cfg(test)]
use suspend::{await_suspend_continuation, poll_generation_suspend};
#[cfg(test)]
pub(super) use suspend::{
    generation_suspend_rpc_should_retry, published_generation_suspend_artifact,
    suspend_pod_deletion_confirmation,
};
use teardown::{FenceReadFreshness, PodFenceCheck};

const MAX_STALE_POOL_CLAIMS_PER_REQUEST: usize = 3;
const NODE_API_ENDPOINT_RESOLVE_TIMEOUT: Duration = Duration::from_secs(15);
const NODE_API_ENDPOINT_RESOLVE_RETRY_INTERVAL: Duration = Duration::from_millis(250);
pub(super) const WARM_POOL_SETUP_RPC_TIMEOUT: Duration = Duration::from_secs(5);
// Planned teardown waits for the exact snapshot upload to become remotely
// durable before retiring the pod. Full-codec artifacts can outlive the
// generic node-RPC budget, so this path needs its own outer deadline.
pub(super) const TEARDOWN_SUSPEND_DURABILITY_BUDGET: Duration = Duration::from_secs(300);
const MAX_LOGGED_SUSPEND_PENDING_REASON_BYTES: usize = 512;
const MAX_SUSPEND_PENDING_REASON_LOGS: u8 = 3;
const SUSPEND_PENDING_REASON_LOG_INTERVAL: Duration = Duration::from_secs(30);
const NETWORK_SLOT_CAPACITY_UNAVAILABLE_REASON: &str = "network_slot_capacity_unavailable";
const SUSPEND_TRIGGER_METADATA: &str = "x-space-suspend-trigger";

#[cfg(test)]
mod tests;
