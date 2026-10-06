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
    ClaimedPoolSlot, ClaimedSlotBindSource, ManualPoolConfigSpec, NewPoolSlot,
    OpenPoolSlotDeleteLock, PoolAssignment, PoolConfig, PoolConfigWithCounts, PoolSlot,
    PoolSlotCounts, ProtectedTemplateRelease, ReleasePoolConfigWithCounts, ReservedPoolSlot,
};
#[cfg(feature = "kubernetes")]
pub use owner::{PoolConfigOwner, PoolConfigOwnerParseError};
#[cfg(feature = "kubernetes")]
pub use state::PoolSlotState;
#[cfg(feature = "kubernetes")]
pub use store::{BackstopSweepOutcome, WarmPoolStore};
