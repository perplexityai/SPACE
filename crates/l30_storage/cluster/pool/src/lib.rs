#![cfg_attr(not(feature = "kubernetes"), allow(unused))]

#[cfg(feature = "kubernetes")]
mod configs;
#[cfg(feature = "kubernetes")]
mod error;
#[cfg(feature = "kubernetes")]
mod models;
#[cfg(feature = "kubernetes")]
mod owner;
#[cfg(feature = "kubernetes")]
mod policies;
#[cfg(feature = "kubernetes")]
mod rows;
#[cfg(feature = "kubernetes")]
mod state;
#[cfg(feature = "kubernetes")]
mod store;

#[cfg(all(test, feature = "kubernetes"))]
mod tests;

#[cfg(feature = "kubernetes")]
pub use configs::pool_config_template_name;

#[cfg(feature = "kubernetes")]
pub use error::{PoolError, Result};
#[cfg(feature = "kubernetes")]
pub use models::{
    AutoscaleRead, AutoscaleRun, ClaimedPoolSlot, ClaimedSlotBindSource, DeletedDisabledWarmPools,
    DisabledPoolConfig, FailedAutoscaleCandidate, InvalidAutoscaleRow, InvalidPoolConfigRow,
    ManualPoolConfigSpec, NewPoolSlot, OpenPoolSlotDeleteLock, PoolAssignment, PoolConfig,
    PoolConfigRead, PoolConfigWithCounts, PoolReservation, PoolSlot, PoolSlotCounts,
    ProtectedTemplateRelease, ReleasePoolConfigWithCounts, ReservedPoolSlot,
    TEMPLATE_USAGE_SANDBOX_ID_LIMIT, TemplateWarmPoolUsage, WarmPoolConfigRef, WarmPoolPolicyShape,
};
#[cfg(feature = "kubernetes")]
pub use owner::{PoolConfigOwner, PoolConfigOwnerParseError};
#[cfg(feature = "kubernetes")]
pub use policies::{
    WarmPoolPolicyRecord, WarmPoolPolicyUpsert, fc_warm_pool_policies, upsert_fc_warm_pool_policy,
};
#[cfg(feature = "kubernetes")]
pub use state::PoolSlotState;
#[cfg(feature = "kubernetes")]
pub use store::{BackstopSweepOutcome, WarmPoolStore};
