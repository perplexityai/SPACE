//! Owning store for public operations state: the `public_operations` queue,
//! each operation's `public_operation_events`, and the
//! `public_idempotency_records` that replay a request's response.
//!
//! Verbs are free functions that run in the caller's transaction and under
//! the locks it holds. Most are executor-generic. This store also owns the
//! statements that need more care than a plain verb:
//!
//! - the transitions guarded on a claim token (and, for payload writes and
//!   claim renewals, its generation), a state the row must still be in, a row
//!   with no actor, a federated grant's newness, a key binding's fingerprint
//!   or expiry, or a record's recorded response, each with its guard whole in
//!   its SQL and bound to the values the caller decided on. Each returns
//!   whether it matched; matching no row is another writer winning, not an
//!   error, and the caller decides what follows;
//! - the reads that lock rows the caller relies on until its transaction
//!   ends, and the transaction-level advisory locks that serialize one
//!   sandbox's queue or one kind's active operation on it, one idempotency
//!   key's writers, a fork source's capture leader, or the dispatcher's keyed
//!   claims, all of which take the caller's connection rather than any
//!   executor. The caller derives each advisory key and takes the lock where
//!   its transaction needs it. The dispatcher's claim ([`claim_queue`]) takes
//!   the caller's connection too, since its rows stay locked for the rest of
//!   the caller's transaction. A `FOR UPDATE` that serves only its own
//!   statement, as in the finished-payload scrub, stays executor-generic;
//! - the statements that compare with the database clock, verbatim, so the
//!   decision stays on the database's `NOW()` in the executor the caller
//!   passes.
//!
//! The callers keep their transactions, the order of the statements in
//! them, the `lock_timeout` settings that bound a wait, and each outcome's
//! consequences.

pub mod claim_queue;
pub mod events;
pub mod idempotency;
pub mod operations;
pub mod quota;

#[cfg(test)]
mod tests;
