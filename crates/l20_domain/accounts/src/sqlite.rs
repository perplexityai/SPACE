//! SQLite encodes domain UUIDs as text to preserve existing node-local schemas.

use super::NodeId;

pplx_space_job_id::sqlite_uuid!(NodeId);
