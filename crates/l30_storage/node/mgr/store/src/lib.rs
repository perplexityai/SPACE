//! Owning store for node-manager's node-local SQLite database: the network
//! slot pool (`network_slots`, `network_pool_meta`) and the execution fence of
//! each sandbox (`sandbox_executions`).
//!
//! Every verb takes the store's [`Connection`], so it runs on the caller's
//! connection or inside the [`Transaction`] the caller opened on it, and
//! fails with the store's [`Error`]; [`slots::lease_to_release`],
//! [`slots::ready_slot_to_lease`] and [`slots::idle_slot_scan`] take the
//! [`Transaction`] itself. [`database`] migrates the database, opens it with
//! its busy timeout and PRAGMAs, and opens it read-only for node-manager's
//! startup check; the startup repair sequence stays in `pplx-space-node-mgr`,
//! which holds the one connection behind its pool lock. `AGENTS.md` lists the
//! rules for this crate's public surface. Where a slot-keyed write follows
//! its read inside the caller's transaction, the two come together:
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
mod handle;
pub mod meta;
pub mod slots;

pub use handle::{Connection, Error, Result, Transaction};
pub use slots::{LeaseKind, PoolValueParseError, SlotHealth, SlotState};

#[cfg(test)]
mod tests;
