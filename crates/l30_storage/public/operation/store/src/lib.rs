//! Owning store for public operations state: the `public_operations` queue,
//! each operation's `public_operation_events`, and the
//! `public_idempotency_records` that replay a request's response.
//!
//! Verbs are executor-generic free functions, so each runs in the caller's
//! transaction and under the locks it holds. Writes guarded on a precondition
//! (a claim token or generation, a state the row must still be in, a
//! recorded response), `SELECT ... FOR UPDATE` reads, and statements whose
//! `WHERE` compares with the database clock still run at their callers in
//! `pplx-space-api` and the cluster controller; moving them belongs to the
//! change that touches them.

pub mod events;
pub mod idempotency;
pub mod operations;
pub mod quota;

#[cfg(test)]
mod tests;
