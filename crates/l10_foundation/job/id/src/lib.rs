//! Identity of a persisted background job.
//!
//! It is its own crate so that the job queue (`pplx-space-common-jobs`) can
//! name a job without depending on `pplx-space-accounts`. The account
//! identifiers in `pplx-space-accounts` are made with the same
//! `pplx_space_uuid_id::uuid_id!` macro and share its parse error.

pplx_space_uuid_id::uuid_id!(
    /// Identity of a persisted background job, retained across retries and recovery.
    JobId
);

// SQLite stores the UUID as text, as for the other identifiers in
// node-local schemas. Unit tests compile it without the feature, through the
// pplx-space-sql-codec dev-dependency, so the round trip always runs.
#[cfg(any(feature = "rusqlite", test))]
pplx_space_uuid_id::sqlite_uuid!(JobId);

#[cfg(test)]
mod tests;
