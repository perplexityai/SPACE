//! Scheduling references existing cleanup authority and commits with its claim.
use pplx_space_common_core::types::JobId;

use super::*;
use pplx_space_common_jobs::{JobPriority, JobStatus, db as jobs};

pub(crate) const KIND: &str = "sandbox_cleanup";

/// A terminal domain outcome is distinct from an attempt that did no work.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum CleanupOutcome {
    Applied,
    AlreadyApplied,
    Superseded,
    Quarantined(String),
    Blocked(String),
}

impl CleanupOutcome {
    pub(crate) fn job_status(&self) -> JobStatus {
        match self {
            Self::Applied | Self::AlreadyApplied => JobStatus::Completed,
            Self::Superseded => JobStatus::Cancelled,
            Self::Quarantined(_) | Self::Blocked(_) => JobStatus::Failed,
        }
    }
}

pub(crate) enum CleanupEligibility {
    Runnable,
    Finished(CleanupOutcome),
}

#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct CleanupFailure {
    pub code: i32,
    pub message: String,
}

/// Called inside the transaction which applies lifecycle completion. A failed
/// job write rolls back lifecycle finalization too; recovery may repeat the
/// idempotent host cleanup, but cannot mistake lost authority for success.
pub(crate) fn finish(
    conn: &Connection,
    id: &JobId,
    outcome: &CleanupOutcome,
) -> rusqlite::Result<bool> {
    match outcome {
        CleanupOutcome::Applied | CleanupOutcome::AlreadyApplied => jobs::complete_job(conn, id, 0),
        CleanupOutcome::Superseded => {
            jobs::request_job_cancellation(conn, id)?;
            jobs::mark_job_cancelled(conn, id)
        }
        CleanupOutcome::Quarantined(message) | CleanupOutcome::Blocked(message) => {
            let error = serde_json::to_string(&CleanupFailure {
                code: 9, // gRPC FailedPrecondition; decoded at the service boundary.
                message: message.clone(),
            })
            .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
            jobs::fail_running_job_permanent(conn, id, &error, None)
        }
    }
}

pub(crate) fn finish_in_transaction(
    conn: &Connection,
    id: Option<&JobId>,
    outcome: &CleanupOutcome,
) -> rusqlite::Result<()> {
    if let Some(id) = id
        && !finish(conn, id, outcome)?
    {
        return Err(rusqlite::Error::QueryReturnedNoRows);
    }
    Ok(())
}

/// Absence of authority does not prove completion. Only an execution-scoped
/// delete receipt can establish prior application by a different binary.
pub(crate) fn eligibility(
    conn: &Connection,
    work: &CleanupWork,
) -> rusqlite::Result<CleanupEligibility> {
    let id = work.sandbox_id.to_string();
    let state = current_state_label(conn, &id)?;
    if state.is_none()
        && work.completion == CleanupCompletion::DeleteRow
        && let Some(generation) = work.execution_generation
        && execution_delete_receipt_covers(conn, &id, generation)?
    {
        return Ok(CleanupEligibility::Finished(CleanupOutcome::AlreadyApplied));
    }
    let Some(state) = state else {
        return Ok(CleanupEligibility::Finished(CleanupOutcome::Superseded));
    };
    let Ok(state) = state.parse::<SandboxState>() else {
        return Ok(CleanupEligibility::Finished(CleanupOutcome::Blocked(
            "cleanup encountered an unknown lifecycle state; operator repair required".to_owned(),
        )));
    };
    if runtime_execution_generation(conn, &id)? != work.execution_generation {
        return Ok(CleanupEligibility::Finished(CleanupOutcome::Superseded));
    }
    if state == SandboxState::Quarantined {
        return Ok(CleanupEligibility::Finished(CleanupOutcome::Quarantined(
            "sandbox is quarantined; operator repair required".to_owned(),
        )));
    }
    if current(conn, work)? {
        Ok(CleanupEligibility::Runnable)
    } else {
        Ok(CleanupEligibility::Finished(CleanupOutcome::Superseded))
    }
}

/// Whether cleanup flushes the guest's root filesystem before it terminates
/// the VM. A stop kills the VM without shutting the guest down, so writes the
/// guest acknowledged but has not yet committed to its disk would be lost.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum GuestFlush {
    /// This request asks nothing; merged with pending work, that work's
    /// choice stands. Work persisted before this field existed reads as this,
    /// and does not flush.
    #[default]
    Unrequested,
    /// An ordinary stop of a running sandbox: attempt a bounded flush before
    /// every termination attempt.
    BeforeTermination,
    /// A forced stop: never flush, whatever an earlier request asked.
    Forbidden,
}

impl GuestFlush {
    /// What a stop's request becomes, decided under the cleanup fence from the
    /// state the stop supersedes. A VM known not to be running has nothing to
    /// flush. Any other state, transitional ones included, is no proof that a
    /// flush already covered every acknowledged write. That includes Paused:
    /// pause flushes before it freezes the VM, and a write in progress then
    /// can complete after that flush, so cleanup resumes a paused VM to
    /// flush it.
    pub(crate) fn for_superseded_state(self, state: Option<SandboxState>) -> Self {
        match (self, state) {
            (
                Self::BeforeTermination,
                Some(
                    SandboxState::Suspended
                    | SandboxState::Stopped
                    | SandboxState::Created
                    | SandboxState::Creating,
                ),
            ) => Self::Unrequested,
            (requested, _) => requested,
        }
    }

    /// A forced stop overrides an ordinary one, and either overrides a
    /// request that asked nothing.
    pub(crate) fn merge(self, other: Self) -> Self {
        match (self, other) {
            (Self::Forbidden, _) | (_, Self::Forbidden) => Self::Forbidden,
            (Self::BeforeTermination, _) | (_, Self::BeforeTermination) => Self::BeforeTermination,
            (Self::Unrequested, Self::Unrequested) => Self::Unrequested,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct CleanupWork {
    pub version: u8,
    pub sandbox_id: SandboxId,
    pub uuid8: SandboxRuntimeLabel,
    pub execution_generation: Option<ExecutionGeneration>,
    pub cleanup_id: Option<SandboxCleanupEpisodeId>,
    pub completion: CleanupCompletion,
    pub keep_filesystem: bool,
    pub immediate_kill: bool,
    #[serde(default)]
    pub guest_flush: GuestFlush,
    pub published_artifact: Option<(LocalArtifactId, String)>,
}

impl CleanupWork {
    pub fn priority(&self) -> JobPriority {
        JobPriority::new(i32::from(self.immediate_kill))
    }

    fn compatible(&self, other: &Self) -> bool {
        let mut candidate = other.clone();
        candidate.immediate_kill = self.immediate_kill;
        candidate.guest_flush = self.guest_flush;
        *self == candidate
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum CleanupJobError {
    #[error("cleanup scheduling database: {0}")]
    Database(#[from] rusqlite::Error),
    #[error("cleanup scheduling payload: {0}")]
    Payload(#[from] serde_json::Error),
    #[error("unsupported cleanup scheduling version {0}")]
    UnsupportedVersion(u8),
    #[error("cleanup scheduling record disappeared")]
    Missing,
}

pub(crate) fn decode(payload: &str) -> std::result::Result<CleanupWork, CleanupJobError> {
    let work: CleanupWork = serde_json::from_str(payload)?;
    if work.version != 1 {
        return Err(CleanupJobError::UnsupportedVersion(work.version));
    }
    Ok(work)
}

/// Caller holds the cleanup fence and a transaction. Reuse preserves retry
/// pacing; an authorized kill upgrade makes the obligation immediately due.
pub(crate) fn persist(
    conn: &Connection,
    mut work: CleanupWork,
) -> std::result::Result<JobId, CleanupJobError> {
    let subject = work.sandbox_id.to_string();
    if let Some(id) = jobs::find_active_job(conn, KIND, &subject)? {
        let existing = jobs::get_job(conn, &id)?.ok_or(CleanupJobError::Missing)?;
        let previous = decode(&existing.payload)?;
        // A recovery hint has no new publication evidence. Keep evidence already
        // attached to this same execution and completion obligation.
        if work.published_artifact.is_none()
            && work.execution_generation == previous.execution_generation
            && work.cleanup_id == previous.cleanup_id
            && work.completion == previous.completion
        {
            work.published_artifact
                .clone_from(&previous.published_artifact);
        }
        if previous.compatible(&work) && existing.status != JobStatus::Cancelling.as_str() {
            work.immediate_kill |= previous.immediate_kill;
            work.guest_flush = work.guest_flush.merge(previous.guest_flush);
            jobs::update_active_job_payload(
                conn,
                &id,
                &serde_json::to_string(&work)?,
                work.priority(),
            )?;
            return Ok(id);
        }
        // The domain fence excludes a mutating attempt. A dispatched attempt
        // still waiting for the fence rechecks status before touching resources.
        jobs::request_job_cancellation(conn, &id)?;
        jobs::mark_job_cancelled(conn, &id)?;
    }
    let id = JobId::new();
    // The persisting caller's context, which an attempt no caller waits for
    // links to. A job coalesced into an active one keeps that job's context.
    let traceparent = pplx_space_observability::trace_propagation::current_traceparent();
    jobs::insert_job(
        conn,
        jobs::InsertJob {
            job_id: &id,
            kind: KIND,
            payload: &serde_json::to_string(&work)?,
            subject_id: Some(&subject),
            dedup_key: Some(&subject),
            max_attempts: i32::MAX,
            traceparent: traceparent.as_deref(),
            priority: work.priority(),
        },
    )?;
    Ok(id)
}

pub(crate) fn current(conn: &Connection, work: &CleanupWork) -> rusqlite::Result<bool> {
    let id = work.sandbox_id.to_string();
    if runtime_execution_generation(conn, &id)? != work.execution_generation {
        return Ok(false);
    }
    if let Some(episode) = work.cleanup_id {
        return cleanup_episode_current(conn, &id, episode);
    }
    if work.completion == CleanupCompletion::DeleteRow
        && !crate::auto_lifecycle_delete_receipts::db::pending_receipt_matches_observed_execution(
            conn,
            work.sandbox_id,
        )?
    {
        return Ok(false);
    }
    cleanup_intent_current(conn, &id, work.completion)
}

/// Indexed retry readiness. Fenced admission coalesces each sandbox to one active
/// job, so exclusion costs at most the bounded number of active subjects.
pub(crate) fn due_job_ids(
    conn: &Connection,
    priority: JobPriority,
    active_subjects: &[String],
    limit: usize,
) -> rusqlite::Result<Vec<JobId>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let excluded = serde_json::to_string(active_subjects)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let limit = i64::try_from(limit)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let mut statement = conn.prepare(sql!(
        "SELECT job_id FROM jobs INDEXED BY idx_sandbox_cleanup_due
         WHERE kind = ?1 AND priority = ?2 AND status = 'pending'
           AND COALESCE(next_attempt_at, created_at) <= datetime('now')
           AND (subject_id IS NULL OR subject_id NOT IN (SELECT value FROM json_each(?3)))
         ORDER BY COALESCE(next_attempt_at, created_at), job_id LIMIT ?4"
    ))?;
    statement
        .query_map(params![KIND, priority.get(), excluded, limit], |row| {
            row.get(0)
        })?
        .collect()
}

#[cfg(test)]
mod tests;
