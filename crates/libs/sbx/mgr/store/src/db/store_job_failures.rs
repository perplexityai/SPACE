//! Jobs the store fails permanently itself, outside the jobs worker:
//! superseded, reconciled or revoked teardown suspends, and quarantined or
//! blocked cleanup. The worker logs only the failures it records, so the store
//! logs each of these once, after the write commits. A rolled-back write is
//! never logged; a committed one is logged even if the process then exits
//! without the worker's shutdown hook, which only counts stored failures.
//!
//! The writes take the `&Connection` a [`Transaction`](super::Transaction)
//! derefs to, which cannot reach the transaction, so failures written inside
//! one wait in a per-thread list keyed by connection until it commits or rolls
//! back. A `Transaction` is `!Send` and SQLite opens at most one on a
//! connection, so each key holds one transaction's failures.

use std::cell::RefCell;

use pplx_space_common_core::{sql, types::JobId};
use rusqlite::params;

use super::{Connection, OptionalExtension as _, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct StoreJobFailure {
    pub(super) job_id: JobId,
    pub(super) kind: String,
    pub(super) subject_id: Option<String>,
    pub(super) error: String,
}

thread_local! {
    static UNCOMMITTED: RefCell<Vec<(usize, StoreJobFailure)>> = const { RefCell::new(Vec::new()) };
}

/// [`pplx_space_common_jobs::db::fail_job_permanent`], logged once committed.
pub(super) fn fail_job_permanent(conn: &Connection, job_id: &JobId, error: &str) -> Result<()> {
    let failure = failure(conn, job_id, error)?;
    pplx_space_common_jobs::db::fail_job_permanent(conn.raw(), job_id, error, None)?;
    if let Some(failure) = failure {
        record(conn, failure);
    }
    Ok(())
}

/// [`pplx_space_common_jobs::db::fail_running_job_permanent`], logged once
/// committed.
pub(super) fn fail_running_job_permanent(
    conn: &Connection,
    job_id: &JobId,
    error: &str,
) -> Result<bool> {
    let failure = failure(conn, job_id, error)?;
    let failed =
        pplx_space_common_jobs::db::fail_running_job_permanent(conn.raw(), job_id, error, None)?;
    if failed && let Some(failure) = failure {
        record(conn, failure);
    }
    Ok(failed)
}

/// Reads the job's kind and subject, both fixed at insert, before the write
/// fails it: in autocommit mode a read failing after the write would report an
/// error for a failure already stored. `None` when the job does not exist.
fn failure(conn: &Connection, job_id: &JobId, error: &str) -> Result<Option<StoreJobFailure>> {
    conn.query_row(
        sql!("SELECT kind, subject_id FROM jobs WHERE job_id = ?1"),
        params![job_id],
        |row| {
            Ok(StoreJobFailure {
                job_id: *job_id,
                kind: row.get(0)?,
                subject_id: row.get(1)?,
                error: error.to_owned(),
            })
        },
    )
    .optional()
}

fn record(conn: &Connection, failure: StoreJobFailure) {
    if conn.is_autocommit() {
        log(&failure);
    } else {
        let key = connection_key(conn.raw());
        UNCOMMITTED.with_borrow_mut(|uncommitted| uncommitted.push((key, failure)));
    }
}

fn connection_key(conn: &rusqlite::Connection) -> usize {
    std::ptr::from_ref(conn) as usize
}

/// The failures written inside the transaction open on one connection. Those
/// not taken by [`Self::take`] when it commits are discarded when this drops.
pub(super) struct TransactionFailures(usize);

impl TransactionFailures {
    /// Called as the transaction begins. Discards failures left by writes in a
    /// transaction opened outside [`Transaction`](super::Transaction), which
    /// never reports them.
    pub(super) fn begin(conn: &rusqlite::Connection) -> Self {
        let failures = Self(connection_key(conn));
        failures.take();
        failures
    }

    /// Removes and returns this transaction's failures.
    pub(super) fn take(&self) -> Vec<StoreJobFailure> {
        UNCOMMITTED.with_borrow_mut(|uncommitted| {
            let (taken, kept) = std::mem::take(uncommitted)
                .into_iter()
                .partition::<Vec<_>, _>(|(key, _)| *key == self.0);
            *uncommitted = kept;
            taken.into_iter().map(|(_, failure)| failure).collect()
        })
    }
}

impl Drop for TransactionFailures {
    fn drop(&mut self) {
        self.take();
    }
}

/// Logs the failures a committed transaction wrote.
pub(super) fn log_committed(failures: &[StoreJobFailure]) {
    for failure in failures {
        log(failure);
    }
}

fn log(failure: &StoreJobFailure) {
    tracing::error!(
        job_id = %failure.job_id,
        kind = %failure.kind,
        subject_id = ?failure.subject_id,
        error = %failure.error,
        "job failed permanently — will not retry"
    );
}
