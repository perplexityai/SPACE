//! Teardown-suspend operations: job leases, status, and artifact binding.

use super::*;

pub const TEARDOWN_SUSPEND_JOB_KIND: &str = "teardown_suspend";

pub(crate) const PLANNED_TEARDOWN_SUSPEND_OPERATION: &str = "planned_teardown";

const TEARDOWN_SUSPEND_SUPERSEDED_REASON: &str = "superseded by a newer local execution lease";

#[derive(Clone, Copy, Debug, serde::Deserialize, serde::Serialize)]
struct TeardownSuspendJobPayload {
    execution_generation: ExecutionGeneration,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    published_artifact_id: Option<LocalArtifactId>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TeardownSuspendOperation {
    pub job_id: JobId,
    pub execution_generation: ExecutionGeneration,
    pub published_artifact_id: Option<LocalArtifactId>,
    pub status: TeardownSuspendOperationStatus,
}

pub type SupersededTeardownGeneration = Option<ExecutionGeneration>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TeardownSuspendOperationStatus {
    Pending,
    Running,
    Completed,
    Failed,
}

impl TeardownSuspendOperationStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }

    pub const fn is_active(self) -> bool {
        matches!(self, Self::Pending | Self::Running)
    }

    pub const fn can_support_release_proof(self) -> bool {
        matches!(self, Self::Pending | Self::Running | Self::Completed)
    }
}

impl fmt::Display for TeardownSuspendOperationStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("unknown teardown suspend job status {0:?}")]
pub struct TeardownSuspendOperationStatusParseError(String);

impl FromStr for TeardownSuspendOperationStatus {
    type Err = TeardownSuspendOperationStatusParseError;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "pending" => Ok(Self::Pending),
            "running" => Ok(Self::Running),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            other => Err(TeardownSuspendOperationStatusParseError(other.to_string())),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TeardownSuspendStatus {
    pub state: SandboxState,
    pub operation: Option<TeardownSuspendOperation>,
    pub artifact: Option<LocalArtifactRow>,
    pub backup_completed: bool,
}

impl TeardownSuspendStatus {
    pub fn is_ready(&self) -> bool {
        self.state == SandboxState::Suspended && self.artifact.is_some() && self.backup_completed
    }

    pub fn is_ready_for(&self, execution_generation: ExecutionGeneration) -> bool {
        self.is_ready()
            && self.operation.as_ref().is_some_and(|operation| {
                operation.execution_generation == execution_generation
                    && operation.status.can_support_release_proof()
            })
    }

    pub fn is_durable_for(&self, execution_generation: ExecutionGeneration) -> bool {
        self.is_ready()
            && self.operation.as_ref().is_some_and(|operation| {
                operation.execution_generation == execution_generation
                    && operation.status == TeardownSuspendOperationStatus::Completed
                    && self.artifact.as_ref().is_some_and(|artifact| {
                        operation.published_artifact_id == Some(artifact.local_artifact_id)
                    })
            })
    }
}

pub fn bind_suspended_teardown_operation(
    conn: &Connection,
    id: &str,
    execution_generation: ExecutionGeneration,
) -> Result<SupersededTeardownGeneration> {
    let tx = immediate_transaction(conn)?;
    let has_suspended_artifact = tx.query_row(
        "SELECT EXISTS (
             SELECT 1
             FROM sandboxes AS s
             JOIN local_artifacts AS a
               ON a.local_artifact_id = s.current_local_artifact_id
              AND a.sandbox_id = s.id
             WHERE s.id = ?1
               AND s.state = 'suspended'
               AND s.current_local_artifact_kind = 'full'
               AND s.current_local_artifact_origin = 'suspend'
               AND a.artifact_kind = 'full'
               AND a.artifact_origin = 'suspend'
         )",
        params![id],
        |row| row.get::<_, bool>(0),
    )?;
    if !has_suspended_artifact {
        return Err(rusqlite::Error::QueryReturnedNoRows);
    }
    let claim = ensure_teardown_suspend_operation(&tx, id, execution_generation)?;
    mark_teardown_suspend_operation_running(&tx, id, execution_generation)?;
    tx.commit()?;
    emit_teardown_suspend_lease_claim(id, execution_generation, claim);
    Ok(claim)
}

pub(super) fn ensure_teardown_suspend_operation(
    conn: &Connection,
    id: &str,
    execution_generation: ExecutionGeneration,
) -> Result<SupersededTeardownGeneration> {
    if runtime_execution_generation(conn, id)?.is_some_and(|current| current > execution_generation)
    {
        return Err(rusqlite::Error::InvalidQuery);
    }
    if latest_teardown_suspend_operation(conn, id)?
        .is_some_and(|latest| latest.execution_generation > execution_generation)
    {
        return Err(rusqlite::Error::InvalidQuery);
    }
    if latest_teardown_suspend_operation(conn, id)?.is_some_and(|latest| {
        latest.execution_generation == execution_generation
            && latest.status == TeardownSuspendOperationStatus::Failed
    }) && !teardown_suspend_retry_admissible(conn, id, execution_generation)?
    {
        return Err(rusqlite::Error::QueryReturnedNoRows);
    }
    let mut superseded_generation = None;
    if let Some(active) = active_teardown_suspend_operation(conn, id)? {
        if active.execution_generation > execution_generation {
            return Err(rusqlite::Error::InvalidQuery);
        }
        if active.execution_generation < execution_generation {
            superseded_generation = Some(active.execution_generation);
            pplx_space_common_jobs::db::fail_job_permanent(
                conn,
                &active.job_id,
                TEARDOWN_SUSPEND_SUPERSEDED_REASON,
                None,
            )?;
        }
    }
    if active_teardown_suspend_operation(conn, id)?.is_none() {
        let job_id = JobId::new();
        let payload = serde_json::to_string(&TeardownSuspendJobPayload {
            execution_generation,
            published_artifact_id: None,
        })
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
        let dedup_key = format!("{id}:{execution_generation}");
        pplx_space_common_jobs::db::insert_job(
            conn,
            pplx_space_common_jobs::db::InsertJob {
                job_id: &job_id,
                kind: TEARDOWN_SUSPEND_JOB_KIND,
                payload: &payload,
                subject_id: Some(id),
                dedup_key: Some(&dedup_key),
                max_attempts: 1,
                traceparent: None,
                priority: pplx_space_common_jobs::JobPriority::default(),
            },
        )?;
    }
    Ok(superseded_generation)
}

pub(super) fn emit_teardown_suspend_lease_claim(
    sandbox_id: &str,
    execution_generation: ExecutionGeneration,
    superseded_generation: SupersededTeardownGeneration,
) {
    let Some(previous_generation) = superseded_generation else {
        return;
    };
    emit_sandbox_event(
        sandbox_id,
        SandboxEventType::SuspendSuperseded,
        SandboxEventSource::SbxManager,
        SandboxEventDetails::default()
            .operation(PLANNED_TEARDOWN_SUSPEND_OPERATION)
            .trigger(SandboxEventTrigger::System)
            .result("superseded")
            .execution_generation(previous_generation.to_string())
            .reason(format!(
                "superseded by local execution lease generation {execution_generation}"
            )),
    );
}

pub fn claim_teardown_suspend_operation(
    conn: &Connection,
    id: &str,
    execution_generation: ExecutionGeneration,
) -> Result<SupersededTeardownGeneration> {
    let tx = immediate_transaction(conn)?;
    let claim = ensure_teardown_suspend_operation(&tx, id, execution_generation)?;
    tx.commit()?;
    emit_teardown_suspend_lease_claim(id, execution_generation, claim);
    Ok(claim)
}

pub fn begin_teardown_suspend_operation(
    conn: &Connection,
    id: &str,
    execution_generation: ExecutionGeneration,
) -> Result<SupersededTeardownGeneration> {
    let tx = immediate_transaction(conn)?;
    let claim = ensure_teardown_suspend_operation(&tx, id, execution_generation)?;
    mark_teardown_suspend_operation_running(&tx, id, execution_generation)?;
    tx.commit()?;
    emit_teardown_suspend_lease_claim(id, execution_generation, claim);
    Ok(claim)
}

pub(super) fn mark_teardown_suspend_operation_running(
    conn: &Connection,
    id: &str,
    execution_generation: ExecutionGeneration,
) -> Result<()> {
    let operation =
        active_teardown_suspend_operation(conn, id)?.ok_or(rusqlite::Error::QueryReturnedNoRows)?;
    if operation.execution_generation != execution_generation {
        return Err(rusqlite::Error::InvalidQuery);
    }
    if operation.status == TeardownSuspendOperationStatus::Pending {
        pplx_space_common_jobs::db::mark_job_running(conn, &operation.job_id)?;
    }
    Ok(())
}

pub fn stage_teardown_suspend_artifact(
    conn: &Connection,
    id: &str,
    execution_generation: ExecutionGeneration,
    local_artifact_id: LocalArtifactId,
    local_path: &str,
) -> Result<()> {
    let tx = immediate_transaction(conn)?;
    let operation =
        active_teardown_suspend_operation(&tx, id)?.ok_or(rusqlite::Error::QueryReturnedNoRows)?;
    if operation.execution_generation != execution_generation {
        return Err(rusqlite::Error::QueryReturnedNoRows);
    }
    upsert_suspend_local_artifact(&tx, id, local_artifact_id, local_path)?;
    let changed = tx.execute(
        "UPDATE sandboxes
         SET current_local_artifact_id = ?1,
             current_local_artifact_kind = 'full',
             current_local_artifact_origin = 'suspend',
             suspend_predecessor_artifact_id = NULL,
             updated_at = ?2
         WHERE id = ?3
           AND state = 'suspending'",
        params![local_artifact_id.to_string(), now()?, id],
    )?;
    if changed != 1 {
        return Err(rusqlite::Error::QueryReturnedNoRows);
    }
    tx.commit()?;
    Ok(())
}

pub(super) fn advance_teardown_suspend_lease(
    conn: &Connection,
    sandbox_id: &str,
    execution_generation: ExecutionGeneration,
) -> Result<SupersededTeardownGeneration> {
    if runtime_execution_generation(conn, sandbox_id)?
        .is_some_and(|current| current > execution_generation)
    {
        return Err(rusqlite::Error::InvalidQuery);
    }
    if latest_teardown_suspend_operation(conn, sandbox_id)?.is_some_and(|latest| {
        latest.execution_generation > execution_generation
            || (latest.execution_generation == execution_generation
                && latest.status == TeardownSuspendOperationStatus::Completed)
    }) {
        return Err(rusqlite::Error::InvalidQuery);
    }

    let mut superseded_generation = None;
    if let Some(active) = active_teardown_suspend_operation(conn, sandbox_id)? {
        if active.execution_generation == execution_generation {
            return Ok(superseded_generation);
        }
        superseded_generation = Some(active.execution_generation);
        pplx_space_common_jobs::db::fail_job_permanent(
            conn,
            &active.job_id,
            TEARDOWN_SUSPEND_SUPERSEDED_REASON,
            None,
        )?;
    }

    Ok(superseded_generation)
}

pub(super) fn active_teardown_suspend_operation(
    conn: &Connection,
    sandbox_id: &str,
) -> Result<Option<TeardownSuspendOperation>> {
    let mut statement = conn.prepare(
        "SELECT job_id, payload, status
         FROM jobs
         WHERE kind = ?1
           AND subject_id = ?2
           AND status IN ('pending', 'running')
         ORDER BY created_at DESC, job_id DESC
         LIMIT 2",
    )?;
    let rows = statement
        .query_map(params![TEARDOWN_SUSPEND_JOB_KIND, sandbox_id], |row| {
            Ok((
                row.get::<_, JobId>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?
        .collect::<Result<Vec<_>>>()?;
    if rows.len() > 1 {
        return Err(rusqlite::Error::FromSqlConversionFailure(
            0,
            Type::Text,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("multiple active teardown suspend jobs for sandbox {sandbox_id}"),
            )),
        ));
    }
    rows.into_iter()
        .next()
        .map(|(job_id, payload, status)| parse_teardown_suspend_operation(job_id, payload, status))
        .transpose()
}

pub(crate) fn latest_teardown_suspend_operation(
    conn: &Connection,
    sandbox_id: &str,
) -> Result<Option<TeardownSuspendOperation>> {
    conn.query_row(
        "SELECT job_id, payload, status
         FROM jobs
         WHERE kind = ?1
           AND subject_id = ?2
         ORDER BY created_at DESC, job_id DESC
         LIMIT 1",
        params![TEARDOWN_SUSPEND_JOB_KIND, sandbox_id],
        |row| parse_teardown_suspend_operation(row.get(0)?, row.get(1)?, row.get(2)?),
    )
    .optional()
}

fn parse_teardown_suspend_operation(
    job_id: JobId,
    payload: String,
    status: String,
) -> Result<TeardownSuspendOperation> {
    let payload: TeardownSuspendJobPayload = serde_json::from_str(&payload).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(1, Type::Text, Box::new(error))
    })?;
    let status = TeardownSuspendOperationStatus::from_str(&status).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(2, Type::Text, Box::new(error))
    })?;
    Ok(TeardownSuspendOperation {
        job_id,
        execution_generation: payload.execution_generation,
        published_artifact_id: payload.published_artifact_id,
        status,
    })
}

pub(crate) struct TeardownReleaseEvidence {
    pub runtime_generation: Option<ExecutionGeneration>,
    pub suspend: Option<TeardownSuspendStatus>,
    pub deleted: bool,
    pub activation_rolled_back: bool,
}

/// Release evidence must describe one SQLite snapshot: a historical rollback
/// cannot be combined with ownership from before a replacement activation.
pub(crate) fn teardown_release_evidence(
    conn: &Connection,
    sandbox_id: &str,
    execution_generation: ExecutionGeneration,
) -> Result<TeardownReleaseEvidence> {
    let tx = conn.unchecked_transaction()?;
    let runtime_generation = runtime_execution_generation(&tx, sandbox_id)?;
    let suspend = teardown_suspend_status(&tx, sandbox_id)?;
    let deleted = execution_delete_receipt_covers(&tx, sandbox_id, execution_generation)?;
    let activation_rolled_back = match &suspend {
        Some(status)
            if runtime_generation.is_none()
                && matches!(status.state, SandboxState::Created | SandboxState::Stopped)
                && status.operation.as_ref().is_none_or(|operation| {
                    operation.status == TeardownSuspendOperationStatus::Completed
                }) =>
        {
            completed_activation_rollback_covers(
                &tx,
                sandbox_id,
                status.state,
                execution_generation,
            )?
        }
        _ => false,
    };
    tx.commit()?;
    Ok(TeardownReleaseEvidence {
        runtime_generation,
        suspend,
        deleted,
        activation_rolled_back,
    })
}

fn completed_activation_rollback_covers(
    conn: &Connection,
    sandbox_id: &str,
    state: SandboxState,
    execution_generation: ExecutionGeneration,
) -> Result<bool> {
    let receipt = conn.query_row(
        sql!("SELECT transition_receipt_id, transition_receipt_kind,
                     transition_receipt_target_state, transition_receipt_execution_generation,
                     transition_receipt_reason_code, transition_receipt_activation_attempt_id
                FROM sandboxes s
               WHERE s.id = ?1
                 AND s.state IN ('created', 'stopped')
                 AND s.space_api_route_execution_generation IS NULL
                 AND s.uid IS NULL AND s.slot IS NULL AND s.guest_cid IS NULL
                 AND s.pid IS NULL AND s.spaced_host_pid IS NULL AND s.netgtw_pid IS NULL
                 AND s.vsock_uds_path IS NULL AND s.api_socket_path IS NULL
                 AND s.space_api_route_generation IS NULL
                 AND s.space_api_route_node_id IS NULL
                 AND s.space_api_route_pod_uid IS NULL
                 AND s.space_api_route_substrate_kind IS NULL
                 AND s.space_api_route_activation_attempt_id IS NULL
                 AND s.cleanup_completion IS NULL AND s.cleanup_keep_filesystem IS NULL
                 AND NOT EXISTS (SELECT 1 FROM sandbox_cleanup_episodes e WHERE e.sandbox_id = s.id)"),
        [sandbox_id],
        |row| Ok(RawSandboxStatePublicationMetadata {
            quarantine_reason_code: None,
            transition_receipt_id: row.get(0)?,
            transition_receipt_kind: row.get(1)?,
            transition_receipt_target_state: row.get(2)?,
            transition_receipt_execution_generation: row.get(3)?,
            transition_receipt_reason_code: row.get(4)?,
            transition_receipt_activation_attempt_id: row.get(5)?,
        }),
    ).optional()?;
    // Missing or malformed receipts supply no release authority. In particular,
    // absence of a runtime generation is also normal after a failed suspend.
    Ok(receipt.is_some_and(|receipt| {
        matches!(
            receipt.decode(state),
            Ok(SandboxStatePublicationMetadata::TransitionReceipt(receipt))
                if receipt.kind() == SandboxTransitionReceiptKind::ActivationRollback
                    && receipt.execution_generation() >= execution_generation
        )
    }))
}

pub fn teardown_suspend_status(
    conn: &Connection,
    sandbox_id: &str,
) -> Result<Option<TeardownSuspendStatus>> {
    let status = conn
        .query_row(
            "SELECT s.state,
                s.current_local_artifact_id,
                s.current_local_artifact_kind,
                s.current_local_artifact_origin,
                a.local_path,
                COALESCE(a.backup_completed_at IS NOT NULL, FALSE)
         FROM sandboxes AS s
         LEFT JOIN local_artifacts AS a
           ON a.local_artifact_id = s.current_local_artifact_id
          AND a.sandbox_id = s.id
         WHERE s.id = ?1",
            params![sandbox_id],
            |row| {
                let state_text = row.get::<_, String>(0)?;
                let state = state_text.parse::<SandboxState>().map_err(|_| {
                    rusqlite::Error::FromSqlConversionFailure(
                        0,
                        Type::Text,
                        Box::new(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("unknown sandbox state {state_text:?}"),
                        )),
                    )
                })?;
                let local_artifact_id = row
                    .get::<_, Option<String>>(1)?
                    .map(|value| {
                        LocalArtifactId::from_str(&value).map_err(|error| {
                            rusqlite::Error::FromSqlConversionFailure(
                                1,
                                Type::Text,
                                Box::new(error),
                            )
                        })
                    })
                    .transpose()?;
                let artifact_kind = row
                    .get::<_, Option<String>>(2)?
                    .map(|value| {
                        ArtifactKind::from_str(&value).map_err(|error| {
                            rusqlite::Error::FromSqlConversionFailure(
                                2,
                                Type::Text,
                                Box::new(error),
                            )
                        })
                    })
                    .transpose()?;
                let artifact_origin = row
                    .get::<_, Option<String>>(3)?
                    .map(|value| {
                        ArtifactOrigin::from_str(&value).map_err(|error| {
                            rusqlite::Error::FromSqlConversionFailure(
                                3,
                                Type::Text,
                                Box::new(error),
                            )
                        })
                    })
                    .transpose()?;
                let artifact_path = row.get::<_, Option<String>>(4)?;
                let backup_completed = row.get::<_, bool>(5)?;

                let artifact = match (
                    local_artifact_id,
                    artifact_kind,
                    artifact_origin,
                    artifact_path,
                ) {
                    (None, None, None, None) => None,
                    (
                        Some(local_artifact_id),
                        Some(ArtifactKind::Full),
                        Some(ArtifactOrigin::Suspend),
                        Some(local_path),
                    ) => Some(LocalArtifactRow {
                        local_artifact_id,
                        sandbox_id: sandbox_id.to_string(),
                        artifact_kind: ArtifactKind::Full,
                        artifact_origin: ArtifactOrigin::Suspend,
                        snapshot_name: None,
                        local_path,
                    }),
                    _ => {
                        return Err(rusqlite::Error::FromSqlConversionFailure(
                            1,
                            Type::Text,
                            Box::new(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "sandbox suspend artifact pin is incomplete or invalid",
                            )),
                        ));
                    }
                };

                Ok((state, artifact, backup_completed))
            },
        )
        .optional()?;
    let Some((state, artifact, backup_completed)) = status else {
        return Ok(None);
    };
    Ok(Some(TeardownSuspendStatus {
        state,
        operation: latest_teardown_suspend_operation(conn, sandbox_id)?,
        artifact,
        backup_completed,
    }))
}

pub fn complete_teardown_suspend(
    conn: &Connection,
    sandbox_id: &str,
    execution_generation: ExecutionGeneration,
    published_artifact_id: Option<LocalArtifactId>,
) -> Result<bool> {
    let tx = immediate_transaction(conn)?;
    let Some(operation) = active_teardown_suspend_operation(&tx, sandbox_id)? else {
        return Ok(false);
    };
    if operation.execution_generation != execution_generation {
        return Ok(false);
    }
    if let Some(published_artifact_id) = published_artifact_id {
        if !teardown_suspend_artifact_is_current_and_backed(&tx, sandbox_id, published_artifact_id)?
        {
            return Ok(false);
        }
        bind_teardown_suspend_artifact(
            &tx,
            &operation.job_id,
            execution_generation,
            published_artifact_id,
        )?;
    }
    mark_teardown_suspend_operation_running(&tx, sandbox_id, execution_generation)?;
    if !pplx_space_common_jobs::db::complete_job(&tx, &operation.job_id, 0)? {
        return Err(rusqlite::Error::QueryReturnedNoRows);
    }
    tx.commit()?;
    Ok(true)
}

pub fn bind_completed_teardown_suspend_artifact(
    conn: &Connection,
    sandbox_id: &str,
    execution_generation: ExecutionGeneration,
    published_artifact_id: LocalArtifactId,
) -> Result<bool> {
    let tx = immediate_transaction(conn)?;
    let Some(operation) = latest_teardown_suspend_operation(&tx, sandbox_id)? else {
        return Ok(false);
    };
    if operation.execution_generation != execution_generation
        || operation.status != TeardownSuspendOperationStatus::Completed
        || operation.published_artifact_id.is_some()
        || !teardown_suspend_artifact_is_current_and_backed(&tx, sandbox_id, published_artifact_id)?
    {
        return Ok(false);
    }
    bind_teardown_suspend_artifact(
        &tx,
        &operation.job_id,
        execution_generation,
        published_artifact_id,
    )?;
    tx.commit()?;
    Ok(true)
}

fn teardown_suspend_artifact_is_current_and_backed(
    conn: &Connection,
    sandbox_id: &str,
    published_artifact_id: LocalArtifactId,
) -> Result<bool> {
    conn.query_row(
        "SELECT EXISTS (
             SELECT 1
               FROM sandboxes AS s
               JOIN local_artifacts AS a
                 ON a.local_artifact_id = s.current_local_artifact_id
                AND a.sandbox_id = s.id
              WHERE s.id = ?1
                AND s.state = 'suspended'
                AND s.current_local_artifact_id = ?2
                AND s.current_local_artifact_kind = 'full'
                AND s.current_local_artifact_origin = 'suspend'
                AND a.backup_completed_at IS NOT NULL
         )",
        params![sandbox_id, published_artifact_id.to_string()],
        |row| row.get(0),
    )
}

fn bind_teardown_suspend_artifact(
    conn: &Connection,
    job_id: &JobId,
    execution_generation: ExecutionGeneration,
    published_artifact_id: LocalArtifactId,
) -> Result<()> {
    let payload = serde_json::to_string(&TeardownSuspendJobPayload {
        execution_generation,
        published_artifact_id: Some(published_artifact_id),
    })
    .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    if conn.execute(
        "UPDATE jobs SET payload = ?1 WHERE job_id = ?2",
        params![payload, job_id],
    )? != 1
    {
        return Err(rusqlite::Error::QueryReturnedNoRows);
    }
    Ok(())
}
