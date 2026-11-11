//! SQLite encodes domain UUIDs as text to preserve existing node-local schemas.

#[cfg(test)]
use super::NodeId;
use super::SandboxId;

pplx_space_job_id::sqlite_uuid!(SandboxId);

#[cfg(test)]
mod tests;
