//! Warm-pool sandbox membership, claims, and donor restoration.

use super::*;

// -- Pool helpers --
// Migration needed: ALTER TABLE sandboxes ADD COLUMN pool_template TEXT;

db_private_outside_tests! {
    /// Find one paused sandbox with matching pool membership and optional content hash.
    fn find_pool_sandbox(
        conn: &Connection,
        template: &str,
        content_hash: Option<&str>,
        required_user: Option<&SandboxOwner>,
    ) -> Result<Option<SandboxRow>> {
        Ok(find_pool_sandboxes(conn, template, content_hash, required_user, 1)?
            .into_iter()
            .next())
    }
}

db_private_outside_tests! {
    /// Up to `limit` paused sandboxes with matching pool membership and
    /// optional content hash, oldest first.
    fn find_pool_sandboxes(
        conn: &Connection,
        template: &str,
        content_hash: Option<&str>,
        required_user: Option<&SandboxOwner>,
        limit: u32,
    ) -> Result<Vec<SandboxRow>> {
        let mut stmt = conn.prepare(
            "SELECT id, uuid8, state, template, template_hash, user, runtime, uid, slot, guest_cid, pid,
         spaced_host_pid, netgtw_pid, vsock_uds_path, api_socket_path, created_at, updated_at, snapshots_enabled, rootfs_size_mb, pool_template, guest_uptime_secs, process_env_json, auto_lifecycle_json, auto_lifecycle_state_json, next_auto_event_at, failure_reason, cleanup_completion, cleanup_keep_filesystem, network_policy_override_json, network_tx_bytes, network_rx_bytes, network_policy_generation, applied_network_policy_generation, applied_network_policy_source, network_policy_authority, auto_lifecycle_authority, current_local_artifact_id, current_local_artifact_kind, current_local_artifact_origin, execution_metadata_json, volume_encryption, space_api_route_generation, space_api_route_node_id, space_api_route_execution_generation, space_api_route_pod_uid, restore_origin, restore_phase, restore_storage_uri, space_api_route_substrate_kind, space_api_route_activation_attempt_id, spaced_endpoint_proof_json, disk_io_max_operations_per_sec, disk_io_max_bandwidth_mib_per_sec, node_execution_id, guest_image_contract_json
         FROM sandboxes
         WHERE pool_template = ?1
           AND state = 'paused'
           AND (?2 IS NULL OR template_hash = ?2)
           AND (?3 IS NULL OR user = ?3)
         ORDER BY created_at, id
         LIMIT ?4",
        )?;
        let rows = stmt.query_map(
            params![
                template,
                content_hash,
                required_user.map(SandboxOwner::as_str),
                limit
            ],
            row_to_sandbox,
        )?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Error::from)
    }
}

pub fn claim_pool_sandbox(
    conn: &Connection,
    sandbox_id: &str,
    template: &str,
    content_hash: Option<&str>,
    required_user: Option<&SandboxOwner>,
    user: &SandboxOwner,
    process_env: &BTreeMap<String, String>,
) -> Result<Option<SandboxRow>> {
    let process_env_json = serde_json::to_string(process_env)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let tx = immediate_transaction(conn)?;
    let mut stmt = tx.prepare(
        "SELECT id, uuid8, state, template, template_hash, user, runtime, uid, slot, guest_cid, pid,
         spaced_host_pid, netgtw_pid, vsock_uds_path, api_socket_path, created_at, updated_at, snapshots_enabled, rootfs_size_mb, pool_template, guest_uptime_secs, process_env_json, auto_lifecycle_json, auto_lifecycle_state_json, next_auto_event_at, failure_reason, cleanup_completion, cleanup_keep_filesystem, network_policy_override_json, network_tx_bytes, network_rx_bytes, network_policy_generation, applied_network_policy_generation, applied_network_policy_source, network_policy_authority, auto_lifecycle_authority, current_local_artifact_id, current_local_artifact_kind, current_local_artifact_origin, execution_metadata_json, volume_encryption, space_api_route_generation, space_api_route_node_id, space_api_route_execution_generation, space_api_route_pod_uid, restore_origin, restore_phase, restore_storage_uri, space_api_route_substrate_kind, space_api_route_activation_attempt_id, spaced_endpoint_proof_json, disk_io_max_operations_per_sec, disk_io_max_bandwidth_mib_per_sec, node_execution_id, guest_image_contract_json
         FROM sandboxes
         WHERE id = ?4 AND pool_template = ?1
           AND state = 'paused'
           AND (?2 IS NULL OR template_hash = ?2)
           AND (?3 IS NULL OR user = ?3)
         ORDER BY created_at, id
         LIMIT 1",
    )?;
    let donor = stmt
        .query_row(
            params![
                template,
                content_hash,
                required_user.map(SandboxOwner::as_str),
                sandbox_id
            ],
            row_to_sandbox,
        )
        .optional()?;
    drop(stmt);

    let Some(donor) = donor else {
        tx.commit()?;
        return Ok(None);
    };

    let changed = tx.execute(
        "UPDATE sandboxes
         SET pool_template = NULL,
             user = ?1,
             process_env_json = ?2,
             updated_at = ?3
         WHERE id = ?4
           AND pool_template = ?5
           AND state = 'paused'
           AND (?6 IS NULL OR template_hash = ?6)
           AND (?7 IS NULL OR user = ?7)",
        params![
            user.as_str(),
            process_env_json,
            now()?,
            donor.id,
            template,
            content_hash,
            required_user.map(SandboxOwner::as_str)
        ],
    )?;
    if changed != 1 {
        tx.commit()?;
        return Ok(None);
    }
    tx.commit()?;
    Ok(Some(donor))
}

/// Set or clear the pool_template field on a sandbox.
pub fn set_pool_template(conn: &Connection, id: &str, pool_template: Option<&str>) -> Result<()> {
    conn.execute(
        "UPDATE sandboxes SET pool_template = ?1, updated_at = ?2 WHERE id = ?3",
        params![pool_template, now()?, id],
    )?;
    Ok(())
}

fn restore_pool_donor(
    conn: &Connection,
    sandbox_id: SandboxId,
    donor: &SandboxRow,
) -> Result<Option<auto_lifecycle_delete_receipts::PendingReceiptSupersession>> {
    let id = sandbox_id.to_string();
    set_pool_template(conn, &id, donor.pool_template.as_deref())?;
    update_user(conn, &id, &donor.user)?;
    update_process_env(conn, &id, &donor.process_env)?;
    update_auto_lifecycle(
        conn,
        &id,
        &donor.auto_lifecycle,
        donor.auto_lifecycle_authority,
    )?;
    update_auto_lifecycle_state(conn, &id, &donor.auto_lifecycle_state)?;
    set_network_policy_fields(
        conn,
        &id,
        donor.network_policy_override.as_ref(),
        donor.network_policy_generation,
        donor.network_policy_authority,
    )?;
    let receipt_supersession = update_space_api_route_identity_in_transaction(
        conn,
        sandbox_id,
        &donor.space_api_route_identity,
        crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::Unfenced,
    )?;
    Ok(receipt_supersession)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PoolDonorRestoreDisposition {
    Restored,
    NotAdopted,
    ReceiptOwned,
    LifecycleOwned,
}

pub fn restore_pool_donor_if_adopted(
    conn: &Connection,
    sandbox_id: SandboxId,
    donor: &SandboxRow,
) -> Result<PoolDonorRestoreDisposition> {
    let id = sandbox_id.to_string();
    let tx = immediate_transaction(conn)?;
    let current_pool_template = tx
        .query_row(
            "SELECT pool_template FROM sandboxes WHERE id = ?1",
            params![id],
            |row| row.get::<_, Option<String>>(0),
        )
        .optional()?;
    let Some(None) = current_pool_template else {
        tx.commit()?;
        return Ok(PoolDonorRestoreDisposition::NotAdopted);
    };
    let receipt_supersession = match restore_pool_donor(&tx, sandbox_id, donor) {
        Ok(receipt_supersession) => receipt_supersession,
        Err(error)
            if auto_lifecycle_delete_receipts::is_pending_receipt_execution_fence(&error) =>
        {
            tx.rollback()?;
            return Ok(PoolDonorRestoreDisposition::ReceiptOwned);
        }
        Err(error) if auto_lifecycle_delete_receipts::is_lifecycle_execution_fence(&error) => {
            tx.rollback()?;
            return Ok(PoolDonorRestoreDisposition::LifecycleOwned);
        }
        Err(error) => return Err(error),
    };
    tx.commit()?;
    emit_auto_lifecycle_delete_receipt_superseded_if_present(sandbox_id, receipt_supersession);
    Ok(PoolDonorRestoreDisposition::Restored)
}
