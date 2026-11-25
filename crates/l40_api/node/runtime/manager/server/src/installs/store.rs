//! The runtime-manager database: durable operation intent and outcomes
//! alongside the shared jobs table. `operation_results` reads and writes
//! belong to `pplx-space-node-runtime-manager-store`.
//! Artifact presence belongs to the node store, never to an ownership ledger.

use std::path::Path;

use pplx_space_node_runtime_manager_store::JobResultError;
use pplx_space_node_runtime_manager_store::database::{self, Connection, SqliteError};
pub(crate) use pplx_space_node_runtime_manager_store::{
    JobOutcome, JobPhase, JobResult, delete_orphaned_job_results, get_job_result, put_job_result,
};

pub(crate) const DATABASE_FILE: &str = "runtime-manager.db";

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("runtime-manager database")]
    Sqlite(#[from] database::Error),
    #[error("runtime-manager database migration")]
    Migrate(#[source] SqliteError),
    #[error("cannot open the runtime-manager database")]
    Open(#[source] SqliteError),
    #[error("runtime-manager database holds an unreadable {column} value '{value}'")]
    Corrupt { column: &'static str, value: String },
}

impl From<JobResultError> for StoreError {
    fn from(error: JobResultError) -> Self {
        match error {
            JobResultError::Sqlite(error) => Self::Sqlite(error),
            JobResultError::Corrupt { column, value } => Self::Corrupt { column, value },
        }
    }
}

/// Migrate and open the database in `state_dir`, with the shared jobs schema.
/// The database is restricted to 0600 inside the already private directory.
pub(crate) async fn open(state_dir: &Path) -> Result<Connection, StoreError> {
    let path = state_dir.join(DATABASE_FILE);
    database::migrate(&path)
        .await
        .map_err(StoreError::Migrate)?;
    database::open(&path).map_err(StoreError::Open)
}

#[cfg(test)]
mod tests;
