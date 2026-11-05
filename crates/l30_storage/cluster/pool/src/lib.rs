#![cfg_attr(not(feature = "kubernetes"), allow(unused))]

#[cfg(feature = "kubernetes")]
mod error;
#[cfg(feature = "kubernetes")]
mod models;
#[cfg(feature = "kubernetes")]
mod owner;
#[cfg(feature = "kubernetes")]
mod rows;
#[cfg(feature = "kubernetes")]
mod state;
#[cfg(feature = "kubernetes")]
mod store;

#[cfg(all(test, feature = "kubernetes"))]
mod tests;

#[cfg(feature = "kubernetes")]
pub use error::{PoolError, Result};
#[cfg(feature = "kubernetes")]
pub use models::{
    AutoscaleRead, AutoscaleRun, ClaimedPoolSlot, ClaimedSlotBindSource, DeletedDisabledWarmPools,
    FailedAutoscaleCandidate, InvalidAutoscaleRow, InvalidPoolConfigRow, ManualPoolConfigSpec,
    NewPoolSlot, OpenPoolSlotDeleteLock, PoolAssignment, PoolConfig, PoolConfigRead,
    PoolConfigWithCounts, PoolSlot, PoolSlotCounts, ProtectedTemplateRelease,
    ReleasePoolConfigWithCounts, ReservedPoolSlot, TEMPLATE_USAGE_SANDBOX_ID_LIMIT,
    TemplateWarmPoolUsage, WarmPoolConfigRef, WarmPoolPolicyShape,
};
#[cfg(feature = "kubernetes")]
pub use owner::{PoolConfigOwner, PoolConfigOwnerParseError};
#[cfg(feature = "kubernetes")]
pub use state::PoolSlotState;
#[cfg(feature = "kubernetes")]
pub use store::{BackstopSweepOutcome, WarmPoolStore};

/// TRUE iff a warm-pool slot, in any state, reserves the sandbox ID
/// `sandbox_id`, a UUID SQL expression; never NULL. A whole boolean predicate
/// for a statement outside this crate that tells a pool slot's sandbox from
/// another a node reports with no sandbox row, as
/// [`WarmPoolStore::reserving_pool_configs`] does for a list of IDs.
/// `reserved_sandbox_id` is unique, so the probe is one index lookup.
#[cfg(feature = "kubernetes")]
pub fn reserved_by_pool_slot(sandbox_id: &str) -> String {
    format!(
        "EXISTS (\n\
             SELECT 1\n\
             FROM kubernetes_pool_slots reserving_slot\n\
             WHERE reserving_slot.reserved_sandbox_id = {sandbox_id}\n\
         )"
    )
}
