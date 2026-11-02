use super::*;
use crate::db::Connection;
use crate::db::{InsertSandboxParams, insert_sandbox, test_conn};

#[test]
fn projection_order_does_not_change_values_or_corrupt_column_diagnostics() {
    let conn = test_conn();
    let id = SandboxId::new().to_string();
    let runtime_label = SandboxRuntimeLabel::parse("abcdef12").unwrap();
    let owner = SandboxOwner::parse("row-owner").unwrap();
    let image = pplx_space_common_core::types::guest_image::GuestImageContract::new(
        pplx_space_common_core::types::guest_image::GuestDiskLayout::OpaqueRawDisk,
        pplx_space_common_core::types::guest_image::GuestReadiness::TcpService {
            port: std::num::NonZeroU16::new(3389).unwrap(),
        },
        pplx_space_common_core::types::guest_image::GuestNetwork::SlotDhcpV1,
        pplx_space_common_core::types::guest_image::GuestAgent::Absent,
    )
    .unwrap();
    let env = BTreeMap::from([("KEY".to_owned(), "value".to_owned())]);
    insert_sandbox(
        &conn,
        InsertSandboxParams {
            guest_image: image,
            id: &id,
            uuid8: &runtime_label,
            state: SandboxState::Stopped,
            template: "row-template",
            template_hash: Some("row-template-hash"),
            user: &owner,
            runtime: "fc",
            snapshots_enabled: true,
            rootfs_size_mb: Some(8192),
            disk_io_limits: Default::default(),
            pool_template: None,
            volume_encryption: false,
            process_env: &env,
            auto_lifecycle: &AutoLifecyclePolicy::disabled(),
            network_policy_override: None,
            network_policy_generation: 7,
            network_policy_authority: PolicyAuthority::Local,
            auto_lifecycle_authority: PolicyAuthority::Cluster,
        },
    )
    .unwrap();

    // The store may reorder an explicit projection without changing the row
    // contract. Keep unrelated integer fields distinct to catch misrouting.
    let mut statement = conn
        .prepare(
            "SELECT process_env_json, network_policy_generation, rootfs_size_mb,
                    state, uuid8, user, template, template_hash, runtime,
                    uid, slot, guest_cid, pid, spaced_host_pid, netgtw_pid,
                    vsock_uds_path, api_socket_path, created_at, updated_at,
                    snapshots_enabled, pool_template, guest_uptime_secs,
                    auto_lifecycle_json, auto_lifecycle_state_json, next_auto_event_at,
                    failure_reason, cleanup_completion, cleanup_keep_filesystem,
                    network_policy_override_json, network_tx_bytes, network_rx_bytes,
                    applied_network_policy_generation, applied_network_policy_source,
                    network_policy_authority, auto_lifecycle_authority,
                    current_local_artifact_id, current_local_artifact_kind,
                    current_local_artifact_origin, execution_metadata_json, volume_encryption,
                    space_api_route_generation, space_api_route_node_id,
                    space_api_route_execution_generation, space_api_route_pod_uid,
                    restore_origin, restore_phase, restore_storage_uri,
                    space_api_route_substrate_kind, space_api_route_activation_attempt_id,
                    spaced_endpoint_proof_json, disk_io_max_operations_per_sec,
                    disk_io_max_bandwidth_mib_per_sec, node_execution_id, guest_image_contract_json, id
               FROM sandboxes WHERE id = ?1",
        )
        .unwrap();
    let row = statement.query_row([&id], row_to_sandbox).unwrap();
    assert_eq!(row.guest_image, image);
    assert_eq!(row.id, id);
    assert_eq!(row.uuid8, runtime_label);
    assert_eq!(row.state, SandboxState::Stopped);
    assert_eq!(row.user, owner);
    assert_eq!(row.template, "row-template");
    assert_eq!(row.template_hash.as_deref(), Some("row-template-hash"));
    assert_eq!(row.process_env, env);
    assert_eq!(row.rootfs_size_mb, Some(8192));
    assert_eq!(row.network_policy_generation, 7);
    assert_eq!(row.network_policy_authority, PolicyAuthority::Local);
    assert_eq!(row.auto_lifecycle_authority, PolicyAuthority::Cluster);

    conn.execute(
        "UPDATE sandboxes SET process_env_json = 'malformed' WHERE id = ?1",
        [&id],
    )
    .unwrap();
    match statement.query_row([&id], row_to_sandbox) {
        Err(rusqlite::Error::FromSqlConversionFailure(index, Type::Text, _)) => {
            assert_eq!(statement.column_name(index).unwrap(), "process_env_json");
        }
        Err(error) => panic!("unexpected decode error: {error}"),
        Ok(_) => panic!("malformed process environment must fail closed"),
    }
}

/// Complete statements are the storage convention, so a projection that omits
/// a counter is a store bug, not a degraded row: these columns are
/// `INTEGER NOT NULL DEFAULT 0`, and decoding a fabricated zero for a
/// generation would silently rewrite a fencing comparison's input.
#[test]
fn absent_or_malformed_counters_fail_closed_instead_of_decoding_zero() {
    let conn = test_conn();
    let id = SandboxId::new().to_string();
    insert_sandbox(
        &conn,
        InsertSandboxParams {
            guest_image: Default::default(),
            id: &id,
            uuid8: &SandboxRuntimeLabel::parse("abcdef12").unwrap(),
            state: SandboxState::Stopped,
            template: "row-template",
            template_hash: None,
            user: &SandboxOwner::parse("row-owner").unwrap(),
            runtime: "fc",
            snapshots_enabled: false,
            rootfs_size_mb: None,
            disk_io_limits: Default::default(),
            pool_template: None,
            volume_encryption: false,
            process_env: &BTreeMap::new(),
            auto_lifecycle: &AutoLifecyclePolicy::disabled(),
            network_policy_override: None,
            network_policy_generation: 7,
            network_policy_authority: PolicyAuthority::Cluster,
            auto_lifecycle_authority: PolicyAuthority::Cluster,
        },
    )
    .unwrap();

    for column in [
        "network_policy_generation",
        "applied_network_policy_generation",
        "network_tx_bytes",
        "network_rx_bytes",
    ] {
        let sql = sandbox_projection(&conn, Some(column));
        match conn.prepare(&sql).unwrap().query_row([&id], row_to_sandbox) {
            Err(rusqlite::Error::InvalidColumnName(name)) => assert_eq!(name, column),
            Err(error) => panic!("unexpected {column} decode error: {error}"),
            Ok(_) => panic!("{column} missing from the projection must fail closed"),
        }

        // SQLite INTEGER affinity keeps unconvertible text as TEXT, so a
        // corrupt counter reaches the decoder rather than the schema.
        conn.execute(
            &format!("UPDATE sandboxes SET {column} = 'corrupt' WHERE id = ?1"),
            [&id],
        )
        .unwrap();
        let sql = sandbox_projection(&conn, None);
        match conn.prepare(&sql).unwrap().query_row([&id], row_to_sandbox) {
            Err(rusqlite::Error::InvalidColumnType(_, name, Type::Text)) => {
                assert_eq!(name, column);
            }
            Err(error) => panic!("unexpected {column} decode error: {error}"),
            Ok(_) => panic!("malformed {column} must fail closed"),
        }
        conn.execute(
            &format!("UPDATE sandboxes SET {column} = 0 WHERE id = ?1"),
            [&id],
        )
        .unwrap();
    }

    let sql = sandbox_projection(&conn, None);
    let row = conn
        .prepare(&sql)
        .unwrap()
        .query_row([&id], row_to_sandbox)
        .unwrap();
    assert_eq!(row.network_policy_generation, 0);
}

/// Test-only projection builder. Production queries keep their own complete
/// statements; this exists to drop exactly one column from an otherwise whole
/// projection.
fn sandbox_projection(conn: &Connection, omitted: Option<&str>) -> String {
    let mut statement = conn
        .prepare("SELECT name FROM pragma_table_info('sandboxes')")
        .unwrap();
    let all = statement
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    let columns = all
        .iter()
        .filter(|name| Some(name.as_str()) != omitted)
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(
        columns.len() + usize::from(omitted.is_some()),
        all.len(),
        "projection must actually omit the column under test"
    );
    format!("SELECT {} FROM sandboxes WHERE id = ?1", columns.join(", "))
}
