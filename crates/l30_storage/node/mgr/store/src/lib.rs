//! Owning store for node-manager's node-local SQLite database: the network
//! slot pool (`network_slots`, `network_pool_meta`) and the execution fence of
//! each sandbox (`sandbox_executions`).
//!
//! Every verb runs on the caller's connection or transaction. [`database`]
//! migrates the database and opens it with its busy timeout and PRAGMAs; the
//! startup repair sequence stays in `pplx-space-node-mgr`, which holds the one
//! connection behind its pool lock. Where a slot-keyed write follows its read
//! inside the caller's transaction, the two come together:
//! [`slots::ready_slot_to_lease`] and [`slots::lease_to_release`] return the
//! row with the write bound to it, so the caller's checks run in between and
//! the write cannot reach another row or transaction.
//!
//! Outside its tests, node-manager's server runs no SQL of its own here.
//! Its guarded transitions, which re-check the row's state, owner,
//! generation or execution in their own `WHERE`, are verbs here, named for
//! the transition and grouped by the code they serve: [`slots`] for
//! allocation, release, PID publication, operator repair and startup
//! repair, [`slots::reset`] for the reset worker, [`slots::worker`] for pool
//! maintenance, and [`executions::retire_execution`] for the execution
//! fence. Each runs its statement with the guard whole, bound to the values
//! the caller passes, and returns what the guard matched; matching no row
//! is an outcome, not an error. The caller keeps its transaction, its pool
//! lock and the decision of what each outcome means.

pub mod database;
pub mod executions;
pub mod meta;
pub mod slots;

pub use slots::{LeaseKind, PoolValueParseError, SlotHealth, SlotState};

#[cfg(test)]
mod tests;
