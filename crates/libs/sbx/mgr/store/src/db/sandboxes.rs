//! `sandboxes` row inserts, reads, listings, and non-lifecycle column writes.

use super::*;

/// Minimal lifecycle evidence that remains readable when optional metadata or
/// a newer lifecycle label cannot be decoded by this binary.
#[derive(Debug)]
pub struct RawSandboxLifecycle {
    pub state: String,
    pub state_changed_at: String,
}

db_private_outside_tests! {
    fn raw_sandbox_lifecycle(conn: &Connection, id: &str) -> Result<Option<RawSandboxLifecycle>> {
        conn.query_row(
            "SELECT state, state_changed_at FROM sandboxes WHERE id = ?1",
            [id],
            |row| {
                Ok(RawSandboxLifecycle {
                    state: row.get(0)?,
                    state_changed_at: timestamps::diagnostic_column(row, 1)?,
                })
            },
        )
        .optional()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SandboxLifecycleCount {
    pub state: SandboxState,
    pub count: u64,
}

pub struct RuntimeFields<'a> {
    pub uid: u32,
    pub slot: u32,
    pub guest_cid: u32,
    pub pid: u32,
    pub spaced_host_pid: Option<u32>,
    pub netgtw_pid: u32,
    pub vsock_uds_path: &'a str,
    pub api_socket_path: &'a str,
    pub space_api_route_identity: SpaceApiRouteIdentity,
    pub route_identity_provenance: crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance,
    pub spaced_endpoint_proof: Option<&'a SpacedEndpointProof>,
}

pub struct InsertSandboxParams<'a> {
    pub guest_image: pplx_space_common_core::types::guest_image::GuestImageContract,
    pub id: &'a str,
    pub uuid8: &'a SandboxRuntimeLabel,
    pub state: SandboxState,
    pub template: &'a str,
    pub template_hash: Option<&'a str>,
    pub user: &'a SandboxOwner,
    pub runtime: &'a str,
    pub snapshots_enabled: bool,
    pub rootfs_size_mb: Option<u32>,
    pub disk_io_limits: pplx_space_common_core::types::DiskIoLimits,
    pub pool_template: Option<&'a str>,
    pub volume_encryption: bool,
    pub process_env: &'a BTreeMap<String, String>,
    pub auto_lifecycle: &'a AutoLifecyclePolicy,
    pub network_policy_override: Option<&'a NetworkPolicy>,
    pub network_policy_generation: u64,
    pub network_policy_authority: PolicyAuthority,
    pub auto_lifecycle_authority: PolicyAuthority,
}

#[derive(Debug, thiserror::Error)]
pub enum RestoreAdoptOrphanInsertError {
    #[error(transparent)]
    Sqlite(#[from] Error),
    #[error("restore sandbox row state must be restoring, got {state}")]
    NonRestoringState { state: SandboxState },
    #[error("existing sandbox row for id {id} is not a recoverable synthetic orphan")]
    ExistingRowNotSyntheticOrphan { id: String },
}

#[derive(Debug, thiserror::Error)]
pub enum InsertRecoveredSuspendedSandboxError {
    #[error(transparent)]
    Db(#[from] Error),
    #[error("recovered suspended sandbox insert must use suspended state, got {state}")]
    NonSuspendedInsert { state: SandboxState },
    #[error("recovered suspended sandbox artifact pin updated {changed} rows instead of one")]
    ArtifactPinInvariant { changed: usize },
}

db_private_outside_tests! {
    /// Count every durable sandbox-manager row by its bounded lifecycle. This stays
    /// in SQL so the sampler's memory use is independent of the number of sandboxes
    /// on the node.
    fn sandbox_lifecycle_counts(conn: &Connection) -> Result<Vec<SandboxLifecycleCount>> {
        let mut statement = conn.prepare(
            "SELECT state, COUNT(*)
           FROM sandboxes
       GROUP BY state
       ORDER BY state",
        )?;
        let rows = statement.query_map([], |row| {
            let state_label: String = row.get(0)?;
            let state = state_label.parse::<SandboxState>().map_err(|_| {
                rusqlite::Error::FromSqlConversionFailure(
                    0,
                    Type::Text,
                    format!("unknown sandbox state '{state_label}'").into(),
                )
            })?;
            let count = row.get::<_, SqliteU64>(1)?.0;
            Ok(SandboxLifecycleCount { state, count })
        })?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }
}

db_private_outside_tests! {
    fn current_state_label(conn: &Connection, id: &str) -> Result<Option<String>> {
        conn.query_row(
            "SELECT state FROM sandboxes WHERE id = ?1",
            params![id],
            |row| row.get(0),
        )
        .optional()
    }
}

pub fn record_remote_restore_artifact_staging(conn: &Connection, sandbox_id: &str) -> Result<bool> {
    let changed = conn.execute(
        "UPDATE sandboxes
         SET restore_origin = 'remote',
             restore_phase = 'artifact_staging',
             updated_at = ?1
         WHERE id = ?2
           AND state = 'restoring'
           AND restore_origin = 'remote'
           AND restore_phase IN ('metadata_staging', 'artifact_staging')",
        params![now()?, sandbox_id],
    )?;
    Ok(changed == 1)
}

pub fn replace_retryable_remote_restore_source(
    conn: &Connection,
    sandbox_id: &str,
    expected_source: &str,
    selected_source: &str,
) -> Result<bool> {
    let changed = conn.execute(
        "UPDATE sandboxes
         SET restore_storage_uri = ?1,
             updated_at = ?2
         WHERE id = ?3
           AND state = 'restoring'
           AND restore_origin = 'remote'
           AND restore_phase IN ('metadata_staging', 'artifact_staging')
           AND restore_storage_uri = ?4
           AND uid IS NULL
           AND slot IS NULL
           AND guest_cid IS NULL
           AND pid IS NULL
           AND spaced_host_pid IS NULL
           AND netgtw_pid IS NULL
           AND vsock_uds_path IS NULL
           AND api_socket_path IS NULL
           AND current_local_artifact_id IS NULL
           AND cleanup_completion IS NULL
           AND cleanup_keep_filesystem IS NULL",
        params![selected_source, now()?, sandbox_id, expected_source],
    )?;
    Ok(changed == 1)
}

fn initial_auto_lifecycle_state(
    policy: &AutoLifecyclePolicy,
    state: SandboxState,
) -> AutoLifecycleRuntimeState {
    if state == SandboxState::Started {
        return crate::auto_lifecycle::initial_runtime_state(
            policy,
            crate::auto_lifecycle::now_secs(),
        );
    }
    crate::auto_lifecycle::initial_disarmed_runtime_state(policy)
}

pub fn insert_sandbox(conn: &Connection, params: InsertSandboxParams<'_>) -> Result<()> {
    let InsertSandboxParams {
        guest_image,
        id,
        uuid8,
        state,
        template,
        template_hash,
        user,
        runtime,
        snapshots_enabled,
        rootfs_size_mb,
        disk_io_limits,
        pool_template,
        volume_encryption,
        process_env,
        auto_lifecycle,
        network_policy_override,
        network_policy_generation,
        network_policy_authority,
        auto_lifecycle_authority,
    } = params;

    let guest_image_json = (!guest_image.legacy_transport())
        .then_some(&guest_image)
        .map(serde_json::to_string)
        .transpose()
        .map_err(Error::to_sql_conversion)?;
    let ts = now()?;
    let process_env_json = serde_json::to_string(process_env)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let auto_lifecycle_json = serde_json::to_string(auto_lifecycle)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let auto_lifecycle_state = initial_auto_lifecycle_state(auto_lifecycle, state);
    let auto_lifecycle_state_json = serde_json::to_string(&auto_lifecycle_state)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let network_policy_override_json = network_policy_override
        .map(serde_json::to_string)
        .transpose()
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    conn.execute(
        "INSERT INTO sandboxes (id, uuid8, state, template, template_hash, user, runtime, snapshots_enabled, rootfs_size_mb, pool_template, process_env_json, auto_lifecycle_json, auto_lifecycle_state_json, next_auto_event_at, network_policy_override_json, network_policy_generation, network_policy_authority, auto_lifecycle_authority, volume_encryption, created_at, updated_at, state_changed_at, disk_io_max_operations_per_sec, disk_io_max_bandwidth_mib_per_sec, node_execution_id, node_execution_retired, guest_image_contract_json)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?21, ?22, ?23, ?24, 1, ?25)",
        params![
            id,
            uuid8.as_str(),
            state.as_str(),
            template,
            template_hash,
            user.as_str(),
            runtime,
            snapshots_enabled,
            rootfs_size_mb,
            pool_template,
            process_env_json,
            auto_lifecycle_json,
            auto_lifecycle_state_json,
            auto_lifecycle_state.next_auto_event_at.map(SqliteU64),
            network_policy_override_json.as_deref(),
            u64_to_sql_i64(network_policy_generation),
            network_policy_authority.as_str(),
            auto_lifecycle_authority.as_str(),
            volume_encryption,
            ts,
            ts,
            u64_to_sql_i64(disk_io_limits.max_operations_per_sec()),
            u64_to_sql_i64(disk_io_limits.max_bandwidth_mib_per_sec()),
            // Staging can fail before the first activation. Give its cleanup
            // a durable request identity, but not an allocated predecessor.
            SandboxActivationAttemptId::new().to_string(),
            guest_image_json,
        ],
    )?;
    Ok(())
}

db_private_outside_tests! {
    fn node_execution_fence(
        conn: &Connection,
        sandbox_id: &str,
    ) -> Result<Option<pplx_space_node_mgr_protocol::execution_fence::ExecutionFence>> {
        use pplx_space_node_mgr_protocol::execution_fence::ExecutionFence;
        let (current, previous): (Option<String>, Option<String>) = conn.query_row(
            "SELECT node_execution_id, previous_node_execution_id FROM sandboxes WHERE id = ?1",
            params![sandbox_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        let parse = |raw: String| {
            raw.parse::<SandboxActivationAttemptId>().map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            })
        };
        match current {
            Some(current) => Ok(Some(ExecutionFence::new(
                parse(current)?,
                previous.map(parse).transpose()?,
            ))),
            None if previous.is_none() => Ok(None),
            None => Err(Error::invalid_query()),
        }
    }
}

db_private_outside_tests! {
    /// Bounded startup preflight; legacy NULL rows require the adoption baseline.
    fn unbound_node_executions(conn: &Connection) -> Result<Vec<String>> {
        let mut statement = conn
            .prepare("SELECT id FROM sandboxes WHERE node_execution_id IS NULL ORDER BY id LIMIT ?1")?;
        statement
            .query_map(
                [pplx_space_node_mgr_protocol::execution_fence::EXECUTION_PREFLIGHT_LIMIT as i64],
                |row| row.get(0),
            )?
            .collect::<rusqlite::Result<_>>()
            .map_err(Error::from)
    }
}

/// Import receiver authority only while inserting a previously absent orphan
/// row in this transaction. Never use inspection to re-authorize a rejected
/// cleanup of an existing row: the receiver may own a successor execution.
pub fn inherit_orphan_node_execution(
    tx: &Transaction<'_>,
    sandbox_id: &str,
    fence: pplx_space_node_mgr_protocol::execution_fence::ExecutionFence,
) -> Result<()> {
    let changed = tx.execute(
        "UPDATE sandboxes SET node_execution_id = ?2, previous_node_execution_id = ?3,
                              node_execution_retired = 0
         WHERE id = ?1 AND node_execution_retired = 1 AND previous_node_execution_id IS NULL",
        params![
            sandbox_id,
            fence.current().to_string(),
            fence.previous().map(|id| id.to_string())
        ],
    )?;
    if changed != 1 {
        return Err(Error::no_rows());
    }
    Ok(())
}

/// A cleanup reply may acknowledge the predecessor when allocation never
/// reached the node. Never overwrite an activation that superseded this request.
pub fn acknowledge_node_cleanup(
    conn: &Connection,
    sandbox_id: &str,
    expected: pplx_space_node_mgr_protocol::execution_fence::ExecutionFence,
    retained: Option<SandboxActivationAttemptId>,
) -> Result<bool> {
    if retained.is_some_and(|id| !expected.owns(id)) {
        return Err(Error::invalid_query());
    }
    Ok(conn.execute(
        "UPDATE sandboxes SET node_execution_id = ?4, previous_node_execution_id = NULL,
                              node_execution_retired = ?5
         WHERE id = ?1 AND node_execution_id = ?2 AND previous_node_execution_id IS ?3",
        params![
            sandbox_id,
            expected.current().to_string(),
            expected.previous().map(|id| id.to_string()),
            retained.unwrap_or(expected.current()).to_string(),
            retained.is_none(),
        ],
    )? == 1)
}

pub fn upsert_sandbox_execution_metadata(
    conn: &Connection,
    sandbox_id: &str,
    execution: &pplx_space_common_core::types::ExecutionMetadata,
) -> Result<()> {
    let tx = immediate_transaction(conn)?;
    upsert_sandbox_execution_metadata_in_transaction(&tx, sandbox_id, execution)?;
    tx.commit()?;
    Ok(())
}

pub(super) fn upsert_sandbox_execution_metadata_in_transaction(
    conn: &Connection,
    sandbox_id: &str,
    execution: &pplx_space_common_core::types::ExecutionMetadata,
) -> Result<()> {
    let runtime: String = conn.query_row(
        "SELECT runtime FROM sandboxes WHERE id = ?1",
        params![sandbox_id],
        |row| row.get(0),
    )?;
    let runtime_matches = runtime
        .parse::<RuntimeKind>()
        .is_ok_and(|runtime| runtime == execution.runtime().kind);
    if !runtime_matches {
        return Err(Error::to_sql_conversion(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "sandbox runtime column disagrees with execution metadata",
        )));
    }

    let execution_json = serde_json::to_string(execution)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    conn.execute(
        "UPDATE sandboxes
         SET execution_metadata_json = ?2,
             updated_at = ?3
         WHERE id = ?1",
        params![sandbox_id, execution_json, now()?],
    )?;
    Ok(())
}

pub fn insert_recovered_suspended_sandbox_in_transaction(
    conn: &Connection,
    params: InsertSandboxParams<'_>,
    execution: &ExecutionMetadata,
    local_artifact_id: LocalArtifactId,
    local_path: &str,
) -> std::result::Result<(), InsertRecoveredSuspendedSandboxError> {
    if params.state != SandboxState::Suspended {
        return Err(InsertRecoveredSuspendedSandboxError::NonSuspendedInsert {
            state: params.state,
        });
    }
    let sandbox_id = params.id.to_string();
    insert_sandbox(conn, params)?;
    upsert_sandbox_execution_metadata_in_transaction(conn, &sandbox_id, execution)?;
    insert_local_artifact(
        conn,
        &sandbox_id,
        local_artifact_id,
        ArtifactKind::Full,
        ArtifactOrigin::Suspend,
        None,
        local_path,
    )?;
    let changed = conn.execute(
        "UPDATE sandboxes
         SET current_local_artifact_id = ?1,
             current_local_artifact_kind = 'full',
             current_local_artifact_origin = 'suspend',
             updated_at = ?2
         WHERE id = ?3
           AND state = 'suspended'
           AND current_local_artifact_id IS NULL",
        params![local_artifact_id.to_string(), now()?, sandbox_id],
    )?;
    if changed != 1 {
        return Err(InsertRecoveredSuspendedSandboxError::ArtifactPinInvariant { changed });
    }
    Ok(())
}

/// The store tests' convenience for
/// `restore_adopt_orphan_or_insert_sandbox_in_transaction`: it opens and
/// commits a transaction of its own. Both production callers already hold a
/// transaction and call the `_in_transaction` form directly.
#[cfg(test)]
pub(super) fn restore_adopt_orphan_or_insert_sandbox(
    conn: &Connection,
    params: InsertSandboxParams<'_>,
    restore_operation: &RemoteRestoreOperation,
) -> std::result::Result<(), RestoreAdoptOrphanInsertError> {
    let tx = immediate_transaction(conn)?;
    restore_adopt_orphan_or_insert_sandbox_in_transaction(&tx, params, restore_operation)?;
    tx.commit()?;
    Ok(())
}

pub fn restore_adopt_orphan_or_insert_sandbox_in_transaction(
    conn: &Connection,
    params: InsertSandboxParams<'_>,
    restore_operation: &RemoteRestoreOperation,
) -> std::result::Result<(), RestoreAdoptOrphanInsertError> {
    let InsertSandboxParams {
        guest_image,
        id,
        uuid8,
        state,
        template,
        template_hash,
        user,
        runtime,
        snapshots_enabled,
        rootfs_size_mb,
        disk_io_limits,
        pool_template,
        volume_encryption,
        process_env,
        auto_lifecycle,
        network_policy_override,
        network_policy_generation,
        network_policy_authority,
        auto_lifecycle_authority,
    } = params;

    if state != SandboxState::Restoring {
        return Err(RestoreAdoptOrphanInsertError::NonRestoringState { state });
    }
    let guest_image_json = (!guest_image.legacy_transport())
        .then_some(&guest_image)
        .map(serde_json::to_string)
        .transpose()
        .map_err(Error::to_sql_conversion)?;
    let ts = now()?;
    let process_env_json = serde_json::to_string(process_env).map_err(Error::to_sql_conversion)?;
    let auto_lifecycle_json =
        serde_json::to_string(auto_lifecycle).map_err(Error::to_sql_conversion)?;
    let auto_lifecycle_state = initial_auto_lifecycle_state(auto_lifecycle, state);
    let auto_lifecycle_state_json =
        serde_json::to_string(&auto_lifecycle_state).map_err(Error::to_sql_conversion)?;
    let network_policy_override_json = network_policy_override
        .map(serde_json::to_string)
        .transpose()
        .map_err(Error::to_sql_conversion)?;
    let orphan_auto_lifecycle_json = serde_json::to_string(&AutoLifecyclePolicy::disabled())
        .map_err(Error::to_sql_conversion)?;

    let rows_affected = conn.execute(
        "INSERT INTO sandboxes (id, uuid8, state, template, template_hash, user, runtime, snapshots_enabled, rootfs_size_mb, pool_template, process_env_json, auto_lifecycle_json, auto_lifecycle_state_json, next_auto_event_at, network_policy_override_json, network_policy_generation, network_policy_authority, auto_lifecycle_authority, volume_encryption, created_at, updated_at, state_changed_at, restore_origin, restore_phase, restore_storage_uri, disk_io_max_operations_per_sec, disk_io_max_bandwidth_mib_per_sec, node_execution_id, node_execution_retired, guest_image_contract_json)
         VALUES (?1, ?2, 'restoring', ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?23, ?18, ?18, ?18, 'remote', ?24, ?25, ?26, ?27, ?28, 1, ?29)
         ON CONFLICT(id) DO UPDATE SET
             uuid8 = excluded.uuid8,
             state = excluded.state,
             state_changed_at = CASE
                 WHEN sandboxes.state IS NOT excluded.state THEN excluded.state_changed_at
                 ELSE sandboxes.state_changed_at
             END,
             template = excluded.template,
             template_hash = excluded.template_hash,
             user = excluded.user,
             runtime = excluded.runtime,
             uid = NULL,
             slot = NULL,
             guest_cid = NULL,
             pid = NULL,
             spaced_host_pid = NULL,
             netgtw_pid = NULL,
             vsock_uds_path = NULL,
             api_socket_path = NULL,
             space_api_route_generation = NULL,
             space_api_route_node_id = NULL,
             space_api_route_execution_generation = NULL,
             space_api_route_pod_uid = NULL,
             space_api_route_substrate_kind = NULL,
             space_api_route_activation_attempt_id = NULL,
             space_api_route_identity_provenance = NULL,
             spaced_endpoint_proof_json = NULL,
             snapshots_enabled = excluded.snapshots_enabled,
             rootfs_size_mb = excluded.rootfs_size_mb,
             disk_io_max_operations_per_sec = excluded.disk_io_max_operations_per_sec,
             disk_io_max_bandwidth_mib_per_sec = excluded.disk_io_max_bandwidth_mib_per_sec,
             pool_template = excluded.pool_template,
             volume_encryption = excluded.volume_encryption,
             guest_image_contract_json = excluded.guest_image_contract_json,
             guest_uptime_secs = NULL,
             process_env_json = excluded.process_env_json,
             auto_lifecycle_json = excluded.auto_lifecycle_json,
             auto_lifecycle_state_json = excluded.auto_lifecycle_state_json,
             next_auto_event_at = excluded.next_auto_event_at,
             auto_lifecycle_delete_block_reason = NULL,
             auto_lifecycle_delete_blocked_at = NULL,
             failure_reason = NULL,
             cleanup_completion = NULL,
             cleanup_keep_filesystem = NULL,
             network_tx_bytes = 0,
             network_rx_bytes = 0,
             applied_network_policy_generation = 0,
             applied_network_policy_source = '',
             auto_lifecycle_authority = excluded.auto_lifecycle_authority,
             restore_origin = excluded.restore_origin,
             restore_phase = excluded.restore_phase,
             restore_storage_uri = excluded.restore_storage_uri,
             created_at = excluded.created_at,
             updated_at = excluded.updated_at
         WHERE sandboxes.state IN ('quarantined', 'restoring')
           AND EXISTS (
               SELECT 1 FROM sandbox_quarantine_state qs
                WHERE qs.sandbox_id = sandboxes.id
                  AND qs.source = 'sbx_manager'
                  AND qs.reason_code = 'orphaned_sandbox_row_missing'
           )
           AND sandboxes.template = ?19
           AND sandboxes.template_hash IS NULL
           AND sandboxes.user = ?20
           AND sandboxes.runtime = ?21
           AND sandboxes.uid IS NULL
           AND sandboxes.slot IS NULL
           AND sandboxes.guest_cid IS NULL
           AND sandboxes.pid IS NULL
           AND sandboxes.spaced_host_pid IS NULL
           AND sandboxes.netgtw_pid IS NULL
           AND sandboxes.vsock_uds_path IS NULL
           AND sandboxes.api_socket_path IS NULL
           AND sandboxes.snapshots_enabled = 0
           AND sandboxes.guest_image_contract_json IS NULL
           AND sandboxes.pool_template IS NULL
           AND sandboxes.guest_uptime_secs IS NULL
           AND sandboxes.process_env_json = '{}'
           AND sandboxes.auto_lifecycle_json = ?22
           AND sandboxes.cleanup_completion IS NULL
           AND sandboxes.cleanup_keep_filesystem IS NULL
           AND sandboxes.applied_network_policy_generation = 0
           AND sandboxes.applied_network_policy_source = ''
           AND sandboxes.network_policy_authority = 'cluster'
           AND sandboxes.auto_lifecycle_authority = 'cluster'",
        params![
            id,
            uuid8.as_str(),
            template,
            template_hash,
            user.as_str(),
            runtime,
            snapshots_enabled,
            rootfs_size_mb,
            pool_template,
            process_env_json,
            auto_lifecycle_json,
            auto_lifecycle_state_json,
            auto_lifecycle_state.next_auto_event_at.map(SqliteU64),
            network_policy_override_json.as_deref(),
            u64_to_sql_i64(network_policy_generation),
            network_policy_authority.as_str(),
            auto_lifecycle_authority.as_str(),
            ts,
            ORPHANED_SANDBOX_TEMPLATE,
            ORPHANED_SANDBOX_OWNER.as_str(),
            RuntimeKind::Firecracker.alias(),
            orphan_auto_lifecycle_json,
            volume_encryption,
            restore_operation.phase().as_str(),
            restore_operation.source().as_wire(),
            u64_to_sql_i64(disk_io_limits.max_operations_per_sec()),
            u64_to_sql_i64(disk_io_limits.max_bandwidth_mib_per_sec()),
            // New restore staging needs cleanup authority before activation;
            // the conflict branch preserves the adopted row's authority.
            SandboxActivationAttemptId::new().to_string(),
            guest_image_json,
        ],
    )?;

    if rows_affected == 1 {
        return Ok(());
    }

    // The synthetic orphan reconciler has no dedicated marker column today, so
    // adoption is gated on its exact stable column tuple. Quarantined rollout
    // residue additionally requires the durable quarantine marker; a conflict
    // that misses these fences is a real sandbox row and must not be clobbered.
    Err(RestoreAdoptOrphanInsertError::ExistingRowNotSyntheticOrphan { id: id.to_string() })
}

pub fn update_snapshots_enabled(conn: &Connection, id: &str, enabled: bool) -> Result<()> {
    conn.execute(
        "UPDATE sandboxes SET snapshots_enabled = ?1, updated_at = ?2 WHERE id = ?3",
        params![enabled, now()?, id],
    )?;
    Ok(())
}

/// Check the observation against the current incarnation before replaying
/// guest setup. The caller holds the wake and cleanup fences across setup.
pub fn startup_resident_witness_current(conn: &Connection, observed: &SandboxRow) -> Result<bool> {
    let Some(current) = get_sandbox(conn, &observed.id)? else {
        return Ok(false);
    };
    Ok(inventory_deletion_witness_matches(&current, observed)
        && current.uuid8 == observed.uuid8
        && current.space_api_route_identity == observed.space_api_route_identity
        && current.process_env == observed.process_env
        && current.spaced_endpoint_proof == observed.spaced_endpoint_proof
        && current.cleanup_completion.is_none()
        && current.cleanup_keep_filesystem.is_none()
        // Presence alone blocks competing recovery, including ownership written
        // by a newer binary that this reader cannot decode.
        && !has_cleanup_episode(conn, &observed.id)?
        && !conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM sandbox_restore_transfer_scopes WHERE sandbox_id = ?1)",
            [&observed.id],
            |row| row.get::<_, bool>(0),
        )?)
}

db_private_outside_tests! {
    fn sandbox_is_retryable_interrupted_restore(row: &SandboxRow) -> bool {
        row.state == SandboxState::Restoring
            && row
                .restore_operation
                .as_ref()
                .is_some_and(|operation| operation.origin() == RestoreOrigin::Remote)
            && !sandbox_has_runtime_fields(row)
            && row.current_local_artifact_id.is_none()
            && row.cleanup_completion.is_none()
            && row.cleanup_keep_filesystem.is_none()
    }
}

db_private_outside_tests! {
    fn sandbox_state_changed_at(
        conn: &Connection,
        sandbox_ids: &[String],
    ) -> Result<HashMap<String, String>> {
        if sandbox_ids.is_empty() {
            return Ok(HashMap::new());
        }
        let placeholders = std::iter::repeat_n("?", sandbox_ids.len())
            .collect::<Vec<_>>()
            .join(", ");
        let query = format!(
            "SELECT id, state_changed_at
         FROM sandboxes
         WHERE id IN ({placeholders})"
        );
        let mut statement = conn.prepare(&query)?;
        let rows = statement.query_map(rusqlite::params_from_iter(sandbox_ids), |row| {
            Ok((row.get(0)?, timestamps::diagnostic_column(row, 1)?))
        })?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }
}

db_private_outside_tests! {
    /// Read one row's `state_changed_at` exactly as stored.
    ///
    /// Raw, unlike [`sandbox_state_changed_at`]'s diagnostic rendering, and for
    /// the same reason [`SandboxRow::updated_at`] is raw: the value doubles as a
    /// CAS witness, so it has to compare byte-for-byte against the column it came
    /// from. Time-based decisions decode it strictly through
    /// [`timestamps::EpochSeconds::parse`] instead of trusting the text.
    ///
    /// `None` means no such row. A non-text value is returned rendered rather
    /// than rejected, so a corrupt timestamp reaches the caller's explicit
    /// refusal path instead of looking like a missing sandbox.
    fn sandbox_state_changed_at_witness(conn: &Connection, id: &str) -> Result<Option<String>> {
        conn.query_row(
            "SELECT state_changed_at FROM sandboxes WHERE id = ?1",
            params![id],
            |row| {
                Ok(match row.get::<_, rusqlite::types::Value>(0)? {
                    rusqlite::types::Value::Text(raw) => raw,
                    rusqlite::types::Value::Integer(raw) => raw.to_string(),
                    rusqlite::types::Value::Real(raw) => raw.to_string(),
                    rusqlite::types::Value::Null | rusqlite::types::Value::Blob(_) => String::new(),
                })
            },
        )
        .optional()
    }
}

pub fn update_uuid8(conn: &Connection, id: &str, uuid8: &SandboxRuntimeLabel) -> Result<()> {
    conn.execute(
        "UPDATE sandboxes SET uuid8 = ?1, updated_at = ?2 WHERE id = ?3",
        params![uuid8.as_str(), now()?, id],
    )?;
    Ok(())
}

/// Publish route identity and invalidate evidence bound to a replaced execution.
///
/// Unlike [`update_runtime_fields_for_activation`] this touches no runtime process state and
/// does not advance the teardown suspend lease: it exists for activations that
/// republish the route identity of an already-running VM, where the runtime
/// fields and the execution generation are unchanged.
///
/// Forward-moving callers validate the route epoch before entering this
/// writer. Pool-adoption rollback is the exception: it restores a snapshot of
/// this same row taken before adoption. The pending-receipt predicate still
/// prevents that rollback from crossing a receipt for the restored or a newer
/// execution generation.
///
/// Route-only adoption cannot establish a successor execution. It is limited
/// to lifecycle-stable rows without cleanup ownership; a new runtime must use
/// the activation path that claims its lifecycle state and cleanup episode.
/// A new execution generation or activation attempt invalidates endpoint
/// evidence bound to the prior activation.
pub fn update_space_api_route_identity(
    conn: &Connection,
    sandbox_id: SandboxId,
    identity: &SpaceApiRouteIdentity,
    provenance: crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance,
) -> Result<()> {
    let tx = immediate_transaction(conn)?;
    let receipt_supersession =
        update_space_api_route_identity_in_transaction(&tx, sandbox_id, identity, provenance)?;
    tx.commit()?;
    emit_auto_lifecycle_delete_receipt_superseded_if_present(sandbox_id, receipt_supersession);
    Ok(())
}

pub fn update_space_api_route_identity_in_transaction(
    conn: &Connection,
    sandbox_id: SandboxId,
    identity: &SpaceApiRouteIdentity,
    provenance: crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance,
) -> Result<Option<auto_lifecycle_delete_receipts::PendingReceiptSupersession>> {
    let sandbox_id_string = sandbox_id.to_string();
    let previous_generation = runtime_execution_generation(conn, &sandbox_id_string)?;
    let updated = conn.execute(
        "UPDATE sandboxes SET space_api_route_generation = ?1, space_api_route_node_id = ?2,
         space_api_route_execution_generation = ?3, space_api_route_pod_uid = ?4,
         space_api_route_substrate_kind = ?5, space_api_route_activation_attempt_id = ?6,
         space_api_route_identity_provenance = ?7,
         spaced_endpoint_proof_json = CASE
             WHEN space_api_route_execution_generation IS ?3
              AND space_api_route_activation_attempt_id IS ?6
             THEN spaced_endpoint_proof_json
             ELSE NULL
         END,
         updated_at = ?8
         WHERE id = ?9
           AND state IN ('started', 'paused')
           AND cleanup_completion IS NULL
           AND cleanup_keep_filesystem IS NULL
           AND NOT EXISTS (
               SELECT 1
                FROM auto_lifecycle_delete_outbox
               WHERE sandbox_id = ?9
                 AND state = 'pending'
                  AND (?3 IS NULL OR execution_generation >= ?3)
           )",
        params![
            identity
                .route_generation
                .map(|value| SqliteU64(value.get())),
            identity.current_node_id.map(|value| value.to_string()),
            identity.execution_generation.map(|value| value.get()),
            identity.pod_uid.as_ref().map(PodUid::as_str),
            identity.substrate_kind.map(|value| value.to_string()),
            identity
                .activation_attempt_id
                .map(|value| value.to_string()),
            provenance.as_str(),
            now()?,
            sandbox_id_string
        ],
    )?;
    if updated != 1 {
        return Err(auto_lifecycle_delete_receipts::execution_write_miss_error(
            conn,
            sandbox_id,
            identity.execution_generation,
        ));
    }
    reset_access_floor_for_execution_change(
        conn,
        &sandbox_id_string,
        previous_generation,
        identity.execution_generation,
    )?;
    if let Some(evidence) =
        crate::auto_lifecycle_delete_receipts::RouteEvidence::from_identity(identity, provenance)
    {
        auto_lifecycle_delete_receipts::record_retained_route_evidence(conn, sandbox_id, evidence)?;
    }
    // Provenance controls whether route evidence may mint a receipt. Once this
    // transaction persists a newer execution generation, an older receipt no
    // longer describes the row, including when the new route is incomplete.
    let receipt_supersession =
        auto_lifecycle_delete_receipts::discard_pending_receipt_for_execution_advance(
            conn,
            sandbox_id,
            identity.execution_generation,
        )?;
    Ok(receipt_supersession)
}

pub fn update_runtime_fields_for_activation(
    conn: &Connection,
    sandbox_id: SandboxId,
    cleanup_id: SandboxCleanupEpisodeId,
    fields: RuntimeFields<'_>,
) -> Result<SupersededTeardownGeneration> {
    update_runtime_fields_with_activation_owner(conn, sandbox_id, Some(cleanup_id), fields)
}

pub(super) fn update_runtime_fields_with_activation_owner(
    conn: &Connection,
    sandbox_id: SandboxId,
    cleanup_id: Option<SandboxCleanupEpisodeId>,
    fields: RuntimeFields<'_>,
) -> Result<SupersededTeardownGeneration> {
    let id = sandbox_id.to_string();
    let RuntimeFields {
        uid,
        slot,
        guest_cid,
        pid,
        spaced_host_pid,
        netgtw_pid,
        vsock_uds_path,
        api_socket_path,
        route_identity_provenance,
        spaced_endpoint_proof,
        space_api_route_identity:
            SpaceApiRouteIdentity {
                route_generation: space_api_route_generation,
                current_node_id: space_api_route_node_id,
                substrate_kind: space_api_route_substrate_kind,
                execution_generation: space_api_route_execution_generation,
                pod_uid: space_api_route_pod_uid,
                activation_attempt_id: space_api_route_activation_attempt_id,
                ..
            },
    } = fields;
    let execution_generation = space_api_route_execution_generation;
    let spaced_endpoint_proof_json = spaced_endpoint_proof
        .map(serde_json::to_string)
        .transpose()
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;

    let tx = immediate_transaction(conn)?;
    let previous_generation = runtime_execution_generation(&tx, &id)?;
    let claim = execution_generation
        .map(|execution_generation| advance_teardown_suspend_lease(&tx, &id, execution_generation))
        .transpose()?
        .unwrap_or(None);
    let updated = tx.execute(
        "UPDATE sandboxes SET uid = ?1, slot = ?2, guest_cid = ?3, pid = ?4,
         spaced_host_pid = ?5, netgtw_pid = ?6, vsock_uds_path = ?7, api_socket_path = ?8,
         space_api_route_generation = ?9, space_api_route_node_id = ?10,
         space_api_route_execution_generation = ?11, space_api_route_pod_uid = ?12,
         space_api_route_substrate_kind = ?13,
         space_api_route_activation_attempt_id = ?14,
         space_api_route_identity_provenance = ?15,
         spaced_endpoint_proof_json = ?16,
         updated_at = ?17
         WHERE id = ?18
           AND NOT EXISTS (
               SELECT 1
                FROM auto_lifecycle_delete_outbox
               WHERE sandbox_id = ?18
                 AND state = 'pending'
                  AND (?11 IS NULL OR execution_generation >= ?11)
           )
           AND (
               (
                   ?19 IS NULL
                   AND state NOT IN ('deleting', 'cleanup_failed')
                   AND cleanup_completion IS NULL
                   AND cleanup_keep_filesystem IS NULL
                   AND NOT EXISTS (
                       SELECT 1 FROM sandbox_cleanup_episodes
                        WHERE sandbox_id = ?18
                   )
               )
               OR EXISTS (
                   SELECT 1
                     FROM sandbox_cleanup_episodes AS episode
                    WHERE episode.sandbox_id = ?18
                      AND episode.cleanup_id = ?19
                      AND episode.phase = 'armed'
                      AND episode.activation_state = sandboxes.state
                      AND episode.completion = sandboxes.cleanup_completion
                      AND episode.keep_filesystem = sandboxes.cleanup_keep_filesystem
               )
           )",
        params![
            uid,
            slot,
            guest_cid,
            pid,
            spaced_host_pid,
            netgtw_pid,
            vsock_uds_path,
            api_socket_path,
            space_api_route_generation.map(|value| SqliteU64(value.get())),
            space_api_route_node_id.map(|value| value.to_string()),
            space_api_route_execution_generation.map(|value| value.get()),
            space_api_route_pod_uid.as_ref().map(PodUid::as_str),
            space_api_route_substrate_kind.map(|value| value.to_string()),
            space_api_route_activation_attempt_id.map(|value| value.to_string()),
            route_identity_provenance.as_str(),
            spaced_endpoint_proof_json,
            now()?,
            id,
            cleanup_id.map(|value| value.to_string())
        ],
    )?;
    if updated != 1 {
        return Err(auto_lifecycle_delete_receipts::execution_write_miss_error(
            &tx,
            sandbox_id,
            space_api_route_execution_generation,
        ));
    }
    reset_access_floor_for_execution_change(&tx, &id, previous_generation, execution_generation)?;
    if let (Some(execution_generation), Some(activation_attempt_id), Some(substrate)) = (
        space_api_route_execution_generation,
        space_api_route_activation_attempt_id,
        space_api_route_substrate_kind,
    ) {
        auto_lifecycle_delete_receipts::record_retained_route_evidence(
            &tx,
            sandbox_id,
            crate::auto_lifecycle_delete_receipts::RouteEvidence {
                execution_generation,
                activation_attempt_id,
                substrate,
                provenance: route_identity_provenance,
            },
        )?;
    }
    let receipt_supersession =
        auto_lifecycle_delete_receipts::discard_pending_receipt_for_execution_advance(
            &tx,
            sandbox_id,
            space_api_route_execution_generation,
        )?;
    tx.commit()?;
    emit_auto_lifecycle_delete_receipt_superseded_if_present(sandbox_id, receipt_supersession);
    if let Some(execution_generation) = execution_generation {
        emit_teardown_suspend_lease_claim(&id, execution_generation, claim);
    }
    Ok(claim)
}

// Called inside the identity writer's transaction. Same-generation refresh and
// pause/resume preserve the floor; a new execution cannot inherit it.
fn reset_access_floor_for_execution_change(
    conn: &Connection,
    id: &str,
    previous: Option<ExecutionGeneration>,
    next: Option<ExecutionGeneration>,
) -> Result<()> {
    if previous == next {
        return Ok(());
    }
    let sandbox = get_sandbox(conn, id)?.ok_or(rusqlite::Error::QueryReturnedNoRows)?;
    let mut runtime = sandbox.auto_lifecycle_state;
    if runtime.access_extended_until.take().is_some() {
        crate::auto_lifecycle::recompute_runtime_state(&mut runtime, &sandbox.auto_lifecycle);
        update_auto_lifecycle_state(conn, id, &runtime)?;
    }
    Ok(())
}

db_private_outside_tests! {
    fn runtime_execution_generation(
        conn: &Connection,
        sandbox_id: &str,
    ) -> Result<Option<ExecutionGeneration>> {
        let generation = conn
            .query_row(
                "SELECT space_api_route_execution_generation
         FROM sandboxes
         WHERE id = ?1",
                params![sandbox_id],
                |row| row.get::<_, Option<i64>>(0),
            )
            .optional()?
            .flatten();
        generation
            .map(ExecutionGeneration::new)
            .transpose()
            .map_err(|error| Error::decode(0, Type::Integer, error))
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ActivationRouteProvenance {
    pub current_route_generation: Option<RouteGeneration>,
    pub current_execution_generation: Option<ExecutionGeneration>,
    pub current_substrate_kind: Option<NodeManagedBy>,
    pub retained_route_generation: Option<RouteGeneration>,
    pub retained_execution_generation: Option<ExecutionGeneration>,
    pub retained_substrate_kind: Option<NodeManagedBy>,
}

fn activation_route_provenance_from_row(
    row: &rusqlite::Row<'_>,
    offset: usize,
) -> rusqlite::Result<ActivationRouteProvenance> {
    let values = (
        row.get::<_, Option<SqliteU64>>(offset)?
            .map(|value| value.0),
        row.get::<_, Option<i64>>(offset + 1)?,
        row.get::<_, Option<String>>(offset + 2)?,
        row.get::<_, Option<SqliteU64>>(offset + 3)?
            .map(|value| value.0),
        row.get::<_, Option<i64>>(offset + 4)?,
        row.get::<_, Option<String>>(offset + 5)?,
        row.get::<_, Option<String>>(offset + 6)?,
    );
    let parse_route_generation = |value: Option<u64>, column| {
        value
            .map(RouteGeneration::new)
            .transpose()
            .map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(column, Type::Integer, Box::new(error))
            })
    };
    let parse_execution_generation = |value: Option<i64>, column| {
        value
            .map(ExecutionGeneration::new)
            .transpose()
            .map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(column, Type::Integer, Box::new(error))
            })
    };
    let parse_substrate = |value: Option<String>, column| {
        value
            .map(|value| value.parse::<NodeManagedBy>())
            .transpose()
            .map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(column, Type::Text, Box::new(error))
            })
    };
    let retained_route_substrate = parse_substrate(values.5, offset + 5)?;
    let retained_legacy_substrate = parse_substrate(values.6, offset + 6)?;
    let retained_substrate_kind = match (retained_route_substrate, retained_legacy_substrate) {
        (Some(NodeManagedBy::Kubernetes), _) | (_, Some(NodeManagedBy::Kubernetes)) => {
            Some(NodeManagedBy::Kubernetes)
        }
        (Some(NodeManagedBy::Space), _) | (_, Some(NodeManagedBy::Space)) => {
            Some(NodeManagedBy::Space)
        }
        (None, None) => None,
    };
    Ok(ActivationRouteProvenance {
        current_route_generation: parse_route_generation(values.0, offset)?,
        current_execution_generation: parse_execution_generation(values.1, offset + 1)?,
        current_substrate_kind: parse_substrate(values.2, offset + 2)?,
        retained_route_generation: parse_route_generation(values.3, offset + 3)?,
        retained_execution_generation: parse_execution_generation(values.4, offset + 4)?,
        retained_substrate_kind,
    })
}

db_private_outside_tests! {
    fn activation_route_provenance(
        conn: &Connection,
        sandbox_id: &str,
    ) -> Result<ActivationRouteProvenance> {
        let values = conn.query_row(
            "SELECT space_api_route_generation,
                space_api_route_execution_generation,
                space_api_route_substrate_kind,
                last_observed_route_generation,
                last_observed_execution_generation,
                last_observed_route_generation_substrate_kind,
                last_observed_route_substrate_kind
           FROM sandboxes
          WHERE id = ?1",
            params![sandbox_id],
            |row| activation_route_provenance_from_row(row, 0),
        )?;
        Ok(values)
    }
}

db_private_outside_tests! {
    fn activation_route_provenance_for_sandboxes(
        conn: &Connection,
        sandbox_ids: &[String],
    ) -> Result<HashMap<String, Result<ActivationRouteProvenance>>> {
        if sandbox_ids.is_empty() {
            return Ok(HashMap::new());
        }
        let placeholders = std::iter::repeat_n("?", sandbox_ids.len())
            .collect::<Vec<_>>()
            .join(", ");
        let query = format!(
            "SELECT id,
                space_api_route_generation,
                space_api_route_execution_generation,
                space_api_route_substrate_kind,
                last_observed_route_generation,
                last_observed_execution_generation,
                last_observed_route_generation_substrate_kind,
                last_observed_route_substrate_kind
           FROM sandboxes
          WHERE id IN ({placeholders})"
        );
        let mut statement = conn.prepare(&query)?;
        let rows = statement.query_map(rusqlite::params_from_iter(sandbox_ids), |row| {
            Ok((
                row.get::<_, String>(0)?,
                activation_route_provenance_from_row(row, 1).map_err(Error::from),
            ))
        })?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }
}

pub fn clear_runtime_fields(conn: &Connection, id: &str) -> Result<()> {
    conn.execute(
        "UPDATE sandboxes SET uid = NULL, slot = NULL, guest_cid = NULL, pid = NULL,
         spaced_host_pid = NULL, netgtw_pid = NULL, vsock_uds_path = NULL, api_socket_path = NULL,
         space_api_route_generation = NULL, space_api_route_node_id = NULL,
         space_api_route_execution_generation = NULL, space_api_route_pod_uid = NULL,
         space_api_route_substrate_kind = NULL, space_api_route_activation_attempt_id = NULL,
         space_api_route_identity_provenance = NULL,
         spaced_endpoint_proof_json = NULL,
         guest_uptime_secs = NULL, updated_at = ?1 WHERE id = ?2",
        params![now()?, id],
    )?;
    Ok(())
}

db_private_outside_tests! {
    fn sandbox_exists(conn: &Connection, id: &str) -> Result<bool> {
        conn.query_row(
            sql!("SELECT EXISTS(SELECT 1 FROM sandboxes WHERE id = ?1)"),
            [id],
            |row| row.get(0),
        )
    }
}

pub(super) const GET_SANDBOX_SQL: &str =
        "SELECT id, uuid8, state, template, template_hash, user, runtime, uid, slot, guest_cid, pid,
         spaced_host_pid, netgtw_pid, vsock_uds_path, api_socket_path, created_at, updated_at, snapshots_enabled, rootfs_size_mb, pool_template, guest_uptime_secs, process_env_json, auto_lifecycle_json, auto_lifecycle_state_json, next_auto_event_at, failure_reason, cleanup_completion, cleanup_keep_filesystem, network_policy_override_json, network_tx_bytes, network_rx_bytes, network_policy_generation, applied_network_policy_generation, applied_network_policy_source, network_policy_authority, auto_lifecycle_authority, current_local_artifact_id, current_local_artifact_kind, current_local_artifact_origin, execution_metadata_json, volume_encryption, space_api_route_generation, space_api_route_node_id, space_api_route_execution_generation, space_api_route_pod_uid, restore_origin, restore_phase, restore_storage_uri, space_api_route_substrate_kind, space_api_route_activation_attempt_id, spaced_endpoint_proof_json, disk_io_max_operations_per_sec, disk_io_max_bandwidth_mib_per_sec, node_execution_id, guest_image_contract_json
         FROM sandboxes WHERE id = ?1";

db_private_outside_tests! {
    fn get_sandbox(conn: &Connection, id: &str) -> Result<Option<SandboxRow>> {
        // Every guest file operation and process start looks its sandbox up
        // several times under the one database connection; parsing this wide
        // SELECT each time dominated that work.
        let mut stmt = conn.prepare_cached(GET_SANDBOX_SQL)?;
        let mut rows = stmt.query_map(params![id], row_to_sandbox)?;
        rows.next().transpose().map_err(Error::from)
    }
}

db_private_outside_tests! {
    /// Select at most two identities: two matches prove ambiguity without decoding
    /// unrelated or ambiguous sandbox metadata. Callers decode only a unique match.
    fn find_by_prefix(conn: &Connection, prefix: &str) -> Result<Vec<String>> {
        let mut stmt = conn.prepare("SELECT id FROM sandboxes ORDER BY id")?;
        let mut rows = stmt.query([])?;
        let mut matches = Vec::new();
        while let Some(row) = rows.next()? {
            let id = row.get::<_, String>(0)?;
            if pplx_space_common_core::ids::sandbox_id_matches(&id, prefix) {
                matches.push(id);
                if matches.len() == 2 {
                    break;
                }
            }
        }
        Ok(matches)
    }
}

const LIST_SANDBOXES_SQL: &str =
    "SELECT id, uuid8, state, template, template_hash, user, runtime, uid, slot, guest_cid, pid,
         spaced_host_pid, netgtw_pid, vsock_uds_path, api_socket_path, created_at, updated_at, snapshots_enabled, rootfs_size_mb, pool_template, guest_uptime_secs, process_env_json, auto_lifecycle_json, auto_lifecycle_state_json, next_auto_event_at, failure_reason, cleanup_completion, cleanup_keep_filesystem, network_policy_override_json, network_tx_bytes, network_rx_bytes, network_policy_generation, applied_network_policy_generation, applied_network_policy_source, network_policy_authority, auto_lifecycle_authority, current_local_artifact_id, current_local_artifact_kind, current_local_artifact_origin, execution_metadata_json, volume_encryption, space_api_route_generation, space_api_route_node_id, space_api_route_execution_generation, space_api_route_pod_uid, restore_origin, restore_phase, restore_storage_uri, space_api_route_substrate_kind, space_api_route_activation_attempt_id, spaced_endpoint_proof_json, disk_io_max_operations_per_sec, disk_io_max_bandwidth_mib_per_sec, node_execution_id, guest_image_contract_json
         FROM sandboxes ORDER BY created_at";

db_private_outside_tests! {
    fn list_sandboxes(conn: &Connection) -> Result<Vec<SandboxRow>> {
        let mut stmt = conn.prepare(LIST_SANDBOXES_SQL)?;
        let rows = stmt.query_map([], row_to_sandbox)?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }
}

db_private_outside_tests! {
    fn list_sandboxes_for_recovery(conn: &Connection) -> Result<Vec<SandboxRow>> {
        let mut stmt = conn.prepare(LIST_SANDBOXES_SQL)?;
        let mut rows = stmt.query([])?;
        let mut valid = Vec::new();
        while let Some(row) = rows.next()? {
            match row_to_sandbox(row) {
                Ok(row) => valid.push(row),
                Err(error) if is_row_decode_error(&error) => tracing::warn!(
                    sandbox_id = ?row.get_ref(0)?,
                    error = %describe_raw_error(&error),
                    "skipping undecodable sandbox row during local recovery"
                ),
                Err(error) => return Err(Error::from(error)),
            }
        }
        Ok(valid)
    }
}

/// A finite sweep: concurrent inserts are visited on the next sweep instead of
/// keeping this one open forever and starving previously deferred candidates.
/// New candidates wait for a later sweep to reach their rowid; admission pressure
/// and recovery failures can delay progress further. No recovery deadline is promised.
#[derive(Debug)]
pub struct PeriodicLifecycleRecoveryScan {
    next: i64,
    through: Option<i64>,
    runtime_exits: bool,
}

impl Default for PeriodicLifecycleRecoveryScan {
    fn default() -> Self {
        Self {
            next: i64::MIN,
            through: None,
            runtime_exits: false,
        }
    }
}

impl PeriodicLifecycleRecoveryScan {
    /// Keep running-process probes from consuming interrupted-operation capacity.
    pub fn runtime_exits() -> Self {
        Self {
            runtime_exits: true,
            ..Self::default()
        }
    }

    fn restart(&mut self) {
        self.next = i64::MIN;
        self.through = None;
    }
}

db_private_outside_tests! {
    /// Bounded discovery, not a complete inventory or authority to mutate a row.
    /// Examine at most 128 raw rowids. Interrupted operations return at most eight
    /// candidates; cheap process-exit probes can consume that full row window.
    /// Failed decodes do not fill candidate slots, so eligible
    /// malformed rows can cause up to 128 individual full-row decode attempts.
    /// Advance past malformed/ineligible rows as well as candidates, so a retained
    /// or busy first page cannot starve the rest. Rowid is only a scan position;
    /// each consumer must revalidate the execution witness under its own fences.
    fn periodic_lifecycle_recovery_batch(
        conn: &Connection,
        scan: &mut PeriodicLifecycleRecoveryScan,
    ) -> Result<Vec<SandboxRow>> {
        if scan.through.is_none() {
            scan.through = conn.query_row(sql!("SELECT MAX(rowid) FROM sandboxes"), [], |row| {
                row.get(0)
            })?;
        }
        let Some(through) = scan.through else {
            return Ok(Vec::new());
        };
        let mut statement = conn.prepare(sql!(
            "SELECT rowid, id, state, current_local_artifact_id FROM sandboxes
         WHERE rowid >= ?1 AND rowid <= ?2 ORDER BY rowid LIMIT 128"
        ))?;
        let mut rows = statement.query([scan.next, through])?;
        let mut candidates = Vec::new();
        for _ in 0..128 {
            let Some(row) = rows.next()? else {
                scan.restart();
                break;
            };
            let rowid: i64 = row.get(0)?;
            if rowid == through {
                scan.restart();
            } else {
                // The upper witness is greater, so this cannot overflow.
                scan.next = rowid + 1;
            }
            let (id, state) = match (row.get::<_, String>(1), row.get::<_, String>(2)) {
                (Ok(id), Ok(state)) => (id, state),
                (Err(error), _) | (_, Err(error)) => {
                    tracing::warn!(
                        rowid,
                        error = %describe_raw_error(&error),
                        "skipping undecodable sandbox key/state during periodic recovery"
                    );
                    continue;
                }
            };
            let eligible = match state.parse::<SandboxState>() {
                Ok(SandboxState::Started) => scan.runtime_exits,
                Ok(SandboxState::Paused | SandboxState::Resuming | SandboxState::Checkpointing) => !scan.runtime_exits,
                Ok(SandboxState::Suspending) => {
                    !scan.runtime_exits && matches!(row.get_ref(3)?, rusqlite::types::ValueRef::Null)
                }
                _ => false,
            };
            if !eligible {
                continue;
            }
            match get_sandbox(conn, &id) {
                Ok(Some(row)) => candidates.push(row),
                Ok(None) => {}
                Err(error) => tracing::warn!(
                    sandbox_id = %id,
                    error = %describe_error(&error),
                    "skipping undecodable sandbox row during periodic recovery"
                ),
            }
            if !scan.runtime_exits && candidates.len() == 8 {
                break;
            }
        }
        Ok(candidates)
    }
}

db_private_outside_tests! {
    /// Host-control net-gateway auth lookup: rows whose registered
    /// `(netgtw_pid, uid)` matches the caller's peer credentials.
    ///
    /// Restricted to states where a live gateway can legitimately exist
    /// (`SandboxState::can_authorize_net_gateway`): launch-failure and other
    /// terminal rows retain the `(uid, netgtw_pid)` of dead processes, and since
    /// uids are slot-derived and pids recycle, an unfiltered match would let a
    /// stale row shadow — or be served in place of — a later sandbox's gateway.
    /// The state list below must stay in sync with that predicate; the
    /// per-state lookup test cross-checks them.
    fn list_by_netgtw_pid_uid(
        conn: &Connection,
        netgtw_pid: u32,
        uid: u32,
    ) -> Result<Vec<SandboxRow>> {
        let mut stmt = conn.prepare(
            "SELECT id, uuid8, state, template, template_hash, user, runtime, uid, slot, guest_cid, pid,
         spaced_host_pid, netgtw_pid, vsock_uds_path, api_socket_path, created_at, updated_at, snapshots_enabled, rootfs_size_mb, pool_template, guest_uptime_secs, process_env_json, auto_lifecycle_json, auto_lifecycle_state_json, next_auto_event_at, failure_reason, cleanup_completion, cleanup_keep_filesystem, network_policy_override_json, network_tx_bytes, network_rx_bytes, network_policy_generation, applied_network_policy_generation, applied_network_policy_source, network_policy_authority, auto_lifecycle_authority, current_local_artifact_id, current_local_artifact_kind, current_local_artifact_origin, execution_metadata_json, volume_encryption, space_api_route_generation, space_api_route_node_id, space_api_route_execution_generation, space_api_route_pod_uid, restore_origin, restore_phase, restore_storage_uri, space_api_route_substrate_kind, space_api_route_activation_attempt_id, spaced_endpoint_proof_json, disk_io_max_operations_per_sec, disk_io_max_bandwidth_mib_per_sec, node_execution_id, guest_image_contract_json
         FROM sandboxes
         WHERE netgtw_pid = ?1 AND uid = ?2
           AND state IN ('starting', 'started', 'stopping', 'pausing', 'paused',
                         'resuming', 'checkpointing', 'suspending', 'restoring')
         LIMIT 2",
        )?;
        let rows = stmt.query_map(params![netgtw_pid, uid], row_to_sandbox)?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }
}

db_private_outside_tests! {
    /// Host-control spaced-host auth lookup: rows whose registered
    /// `(spaced_host_pid, uid)` matches the caller's peer credentials.
    fn list_by_spaced_host_pid_uid(
        conn: &Connection,
        spaced_host_pid: u32,
        uid: u32,
    ) -> Result<Vec<SandboxRow>> {
        let mut stmt = conn.prepare(
            "SELECT id
         FROM sandboxes
         WHERE spaced_host_pid = ?1 AND uid = ?2
         LIMIT 2",
        )?;
        let sandbox_ids = stmt
            .query_map(params![spaced_host_pid, uid], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(stmt);

        sandbox_ids
            .into_iter()
            .map(|sandbox_id| get_sandbox(conn, &sandbox_id)?.ok_or(Error::no_rows()))
            .collect()
    }
}

#[cfg(any(test, feature = "test-support"))]
pub fn delete_sandbox(conn: &Connection, id: &str) -> Result<()> {
    let tx = immediate_transaction(conn)?;
    delete_sandbox_child_rows(&tx, id)?;
    tx.execute("DELETE FROM sandboxes WHERE id = ?1", params![id])
        .map_err(auto_lifecycle_delete_receipts::map_pending_receipt_delete_error)?;
    tx.commit()?;
    Ok(())
}

fn inventory_deletion_witness_matches(current: &SandboxRow, observed: &SandboxRow) -> bool {
    current.id == observed.id
        && current.created_at == observed.created_at
        && current.updated_at == observed.updated_at
        && current.state == observed.state
        && current.restore_operation == observed.restore_operation
        && current.failure_reason == observed.failure_reason
        && current.uid == observed.uid
        && current.slot == observed.slot
        && current.guest_cid == observed.guest_cid
        && current.pid == observed.pid
        && current.spaced_host_pid == observed.spaced_host_pid
        && current.netgtw_pid == observed.netgtw_pid
        && current.vsock_uds_path == observed.vsock_uds_path
        && current.api_socket_path == observed.api_socket_path
        && current.cleanup_completion == observed.cleanup_completion
        && current.cleanup_keep_filesystem == observed.cleanup_keep_filesystem
        && current.current_local_artifact_id == observed.current_local_artifact_id
        && current.current_local_artifact_kind == observed.current_local_artifact_kind
        && current.current_local_artifact_origin == observed.current_local_artifact_origin
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InventoryOwnershipReason {
    DurableCleanupEpisode,
    RowCleanupIntent,
    MalformedCleanupIntent,
    LifecycleOwned,
    RestoreTransferScope,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InventoryStaleRowMutation {
    Removed,
    Missing,
    WitnessChanged,
    Deferred(InventoryOwnershipReason),
}

/// Delete a row selected by inventory only if its lifecycle ownership witness
/// is unchanged and no durable lifecycle owner exists. The immediate
/// transaction makes the ownership reads and manual child-row cascade
/// indivisible from concurrent lifecycle writers.
pub fn delete_sandbox_if_inventory_witness_matches(
    conn: &Connection,
    observed: &SandboxRow,
) -> Result<InventoryStaleRowMutation> {
    let tx = immediate_transaction(conn)?;
    let Some(current) = get_sandbox(&tx, &observed.id)? else {
        return Ok(InventoryStaleRowMutation::Missing);
    };
    if !inventory_deletion_witness_matches(&current, observed) {
        return Ok(InventoryStaleRowMutation::WitnessChanged);
    }
    if cleanup_episode(&tx, &observed.id)?.is_some() {
        return Ok(InventoryStaleRowMutation::Deferred(
            InventoryOwnershipReason::DurableCleanupEpisode,
        ));
    }
    match (current.cleanup_completion, current.cleanup_keep_filesystem) {
        (Some(_), Some(_)) => {
            return Ok(InventoryStaleRowMutation::Deferred(
                InventoryOwnershipReason::RowCleanupIntent,
            ));
        }
        (None, None) => {}
        _ => {
            return Ok(InventoryStaleRowMutation::Deferred(
                InventoryOwnershipReason::MalformedCleanupIntent,
            ));
        }
    }
    // A quarantine is held for operator or cluster disposition: missing local
    // evidence is not staleness. Only an explicit disposition (cluster-deleted
    // eviction, on-node Delete, or quarantine recovery) may release it;
    // eviction also refuses non-platform owners such as orphan rows.
    if matches!(
        current.state,
        SandboxState::Stopping | SandboxState::Deleting | SandboxState::Quarantined
    ) {
        return Ok(InventoryStaleRowMutation::Deferred(
            InventoryOwnershipReason::LifecycleOwned,
        ));
    }
    if restore_transfer_scope(&tx, &observed.id)?.is_some() {
        return Ok(InventoryStaleRowMutation::Deferred(
            InventoryOwnershipReason::RestoreTransferScope,
        ));
    }
    let execution_generation = current.space_api_route_identity.execution_generation;
    delete_sandbox_child_rows(&tx, &observed.id)?;
    let deleted = tx
        .execute("DELETE FROM sandboxes WHERE id = ?1", params![&observed.id])
        .map_err(auto_lifecycle_delete_receipts::map_pending_receipt_delete_error)?;
    if deleted != 1 {
        return Ok(InventoryStaleRowMutation::WitnessChanged);
    }
    record_execution_delete_receipt(&tx, &observed.id, execution_generation)?;
    tx.commit()?;
    Ok(InventoryStaleRowMutation::Removed)
}

db_private_outside_tests! {
    fn sandbox_has_runtime_fields(row: &SandboxRow) -> bool {
        row.uid.is_some()
            || row.slot.is_some()
            || row.guest_cid.is_some()
            || row.pid.is_some()
            || row.spaced_host_pid.is_some()
            || row.netgtw_pid.is_some()
            || row.vsock_uds_path.is_some()
            || row.api_socket_path.is_some()
    }
}

pub(super) fn sandbox_has_full_suspend_artifact_pin(row: &SandboxRow) -> bool {
    row.current_local_artifact_id.is_some()
        && row.current_local_artifact_kind == Some(ArtifactKind::Full)
        && row.current_local_artifact_origin == Some(ArtifactOrigin::Suspend)
}

pub(super) fn sandbox_has_registered_full_suspend_artifact(
    conn: &Connection,
    row: &SandboxRow,
) -> Result<bool> {
    let Some(local_artifact_id) = row.current_local_artifact_id else {
        return Ok(false);
    };
    if row.current_local_artifact_kind != Some(ArtifactKind::Full)
        || row.current_local_artifact_origin != Some(ArtifactOrigin::Suspend)
    {
        return Ok(false);
    }
    Ok(
        get_local_artifact(conn, local_artifact_id)?.is_some_and(|artifact| {
            artifact.sandbox_id == row.id
                && artifact.artifact_kind == ArtifactKind::Full
                && artifact.artifact_origin == ArtifactOrigin::Suspend
        }),
    )
}

/// Update the user field on a sandbox.
pub fn update_user(conn: &Connection, id: &str, user: &SandboxOwner) -> Result<()> {
    conn.execute(
        "UPDATE sandboxes SET user = ?1, updated_at = ?2 WHERE id = ?3",
        params![user.as_str(), now()?, id],
    )?;
    Ok(())
}

pub fn update_process_env(
    conn: &Connection,
    id: &str,
    process_env: &BTreeMap<String, String>,
) -> Result<()> {
    let process_env_json = serde_json::to_string(process_env)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    conn.execute(
        "UPDATE sandboxes SET process_env_json = ?1, updated_at = ?2 WHERE id = ?3",
        params![process_env_json, now()?, id],
    )?;
    Ok(())
}

pub fn set_sandbox_owner_if_current(
    conn: &Connection,
    id: &str,
    expected_current_owner: &SandboxOwner,
    owner: &SandboxOwner,
) -> Result<Option<SandboxRow>> {
    let tx = immediate_transaction(conn)?;
    let Some(previous) = get_sandbox(&tx, id)? else {
        tx.commit()?;
        return Ok(None);
    };
    if previous.user.as_str() == owner.as_str() {
        tx.commit()?;
        return Ok(Some(previous));
    }
    if previous.user.as_str() != expected_current_owner.as_str() {
        tx.commit()?;
        return Ok(None);
    }

    let changed = tx.execute(
        "UPDATE sandboxes
         SET user = ?1,
             updated_at = ?2
         WHERE id = ?3
           AND user = ?4",
        params![owner.as_str(), now()?, id, expected_current_owner.as_str()],
    )?;
    if changed != 1 {
        tx.commit()?;
        return Ok(None);
    }
    tx.commit()?;
    Ok(Some(previous))
}

pub fn update_guest_uptime(conn: &Connection, id: &str, secs: u64) -> Result<()> {
    conn.execute(
        "UPDATE sandboxes SET guest_uptime_secs = ?1 WHERE id = ?2",
        params![secs as i64, id],
    )?;
    Ok(())
}

/// Persist the net-gateway `(uid, pid)` registration that host-control auth
/// matches against. Launch paths rely on this being durable before the
/// gateway arm returns, so an UPDATE that matched no row (the sandbox was
/// deleted by concurrent cleanup mid-launch) is an error: silently returning
/// `Ok` would leave the just-launched gateway permanently unauthorized
/// without tripping the caller's launch-failure cleanup.
pub fn update_netgtw_pid(conn: &Connection, id: &str, uid: u32, netgtw_pid: u32) -> Result<()> {
    let changed = conn.execute(
        "UPDATE sandboxes SET uid = ?1, netgtw_pid = ?2, updated_at = ?3 WHERE id = ?4",
        params![uid, netgtw_pid, now()?, id],
    )?;
    if changed != 1 {
        return Err(Error::no_rows());
    }
    Ok(())
}

/// Commit a watchdog replacement only while the persisted endpoint authority
/// still matches the observation that initiated the restart. A concurrent
/// caller may already have committed the same live replacement; confirming
/// that PID is idempotent, while any different PID remains protected.
pub fn replace_netgtw_pid(
    conn: &Connection,
    id: &str,
    uid: u32,
    expected_netgtw_pid: Option<u32>,
    netgtw_pid: u32,
) -> Result<()> {
    let changed = conn.execute(
        "UPDATE sandboxes
         SET netgtw_pid = ?1, updated_at = ?2
         WHERE id = ?3
           AND uid = ?4
           AND (netgtw_pid IS ?5 OR netgtw_pid = ?1)",
        params![netgtw_pid, now()?, id, uid, expected_netgtw_pid],
    )?;
    if changed != 1 {
        return Err(Error::no_rows());
    }
    Ok(())
}

/// Persist the replacement spaced-host PID that endpoint authentication
/// matches against. The owner UID is already part of the sandbox runtime row
/// and is deliberately included in the guard so a stale watchdog observation
/// cannot transfer endpoint authority to a replacement after ownership moved.
/// A concurrent caller may confirm the same replacement PID idempotently.
pub fn update_spaced_host_pid(
    conn: &Connection,
    id: &str,
    uid: u32,
    expected_spaced_host_pid: Option<u32>,
    spaced_host_pid: u32,
) -> Result<()> {
    let changed = conn.execute(
        "UPDATE sandboxes
         SET spaced_host_pid = ?1, spaced_endpoint_proof_json = NULL, updated_at = ?2
         WHERE id = ?3
           AND uid = ?4
           AND (spaced_host_pid IS ?5 OR spaced_host_pid = ?1)",
        params![spaced_host_pid, now()?, id, uid, expected_spaced_host_pid],
    )?;
    if changed != 1 {
        return Err(Error::no_rows());
    }
    Ok(())
}

/// Memoize a proof for an execution that predates proof capture.
///
/// Every runtime identity dimension participates in the compare-and-set so a
/// bridge request racing activation replacement cannot attach its observation
/// to the replacement execution.
pub fn persist_spaced_endpoint_proof_if_missing(
    conn: &Connection,
    runtime: &crate::endpoint_proof::SpacedEndpointRuntimeIdentity,
    proof: &SpacedEndpointProof,
) -> Result<bool> {
    proof
        .validate_runtime(runtime)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let proof_json = serde_json::to_string(proof)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let changed = conn.execute(
        "UPDATE sandboxes
         SET spaced_endpoint_proof_json = ?1, updated_at = ?2
         WHERE id = ?3
           AND spaced_endpoint_proof_json IS NULL
           AND pid = ?4
           AND spaced_host_pid = ?5
           AND uid = ?6
           AND guest_cid = ?7
           AND vsock_uds_path = ?8
           AND space_api_route_execution_generation = ?9
           AND space_api_route_activation_attempt_id = ?10",
        params![
            proof_json,
            now()?,
            runtime.sandbox_id.to_string(),
            runtime.runtime_pid.get().get(),
            runtime.spaced_host_pid.get().get(),
            runtime.sandbox_uid.get(),
            runtime.guest_cid.get().get(),
            runtime.vsock_path.to_string_lossy(),
            runtime.execution_generation.get(),
            runtime.activation_attempt_id.to_string(),
        ],
    )?;
    Ok(changed == 1)
}

/// Persist a net-gateway stats observation, if it actually advances the
/// counters.
///
/// The counters only ever climb (`MAX`), so an observation that carries no
/// new bytes has nothing to persist — and must not be written anyway. The
/// watchdog re-probes every sandbox with a live net-gateway every 30 s for as
/// long as its runtime is resident, and an unconditional write makes each of
/// those probes bump `updated_at` on a row where nothing changed. That column
/// is the quiescence clock the cluster-deleted eviction reads, so an idle
/// sandbox's purely informational probe would hold the row inside the window
/// forever. Same shape as [`mark_network_policy_applied`]: write only when
/// the observation is news.
pub fn update_network_counters(
    conn: &Connection,
    id: &str,
    network_tx_bytes: u64,
    network_rx_bytes: u64,
) -> Result<()> {
    conn.execute(
        "UPDATE sandboxes
         SET network_tx_bytes = MAX(network_tx_bytes, ?1),
             network_rx_bytes = MAX(network_rx_bytes, ?2),
             updated_at = ?3
         WHERE id = ?4
           AND (?1 > network_tx_bytes OR ?2 > network_rx_bytes)",
        params![
            u64_to_sql_i64(network_tx_bytes),
            u64_to_sql_i64(network_rx_bytes),
            now()?,
            id
        ],
    )?;
    Ok(())
}
