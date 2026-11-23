//! Owning store for node-manager's node-local SQLite database: the network
//! slot pool (`network_slots`, `network_pool_meta`) and the execution fence of
//! each sandbox (`sandbox_executions`).
//!
//! Every verb runs on the caller's connection or transaction. Opening the
//! database, its busy timeout and PRAGMAs, its migrations and the startup
//! repair sequence stay in `pplx-space-node-mgr`, which holds the one
//! connection behind its pool lock. Where a slot-keyed write follows its read
//! inside the caller's transaction, the two come together:
//! [`slots::ready_slot_to_lease`] and [`slots::lease_to_release`] return the
//! row with the write bound to it, so the caller's checks run in between and
//! the write cannot reach another row or transaction.
//!
//! The store owns these tables, but node-manager's server still runs its
//! guarded writes and its predicate-built slot reads on them directly until
//! that code moves behind the store.

pub mod executions;
pub mod meta;
pub mod slots;

pub use slots::{LeaseKind, PoolValueParseError, SlotHealth, SlotState};

#[cfg(test)]
mod tests;
