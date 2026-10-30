use super::*;

pub(super) async fn handle(
    command: Commands,
    client: &mut pplx_space_sbx_mgr_client::SandboxManagerGrpcClient,
) {
    match command {
        Commands::Pause {
            sandbox_id,
            blocking: _,
        } => {
            let short_id = sandbox_label(sandbox_id.as_str());
            client
                .pause(PauseRequest {
                    sandbox_id: sandbox_id.to_string(),
                })
                .await
                .unwrap_or_else(|e| exit_error(e));
            println!("Paused: {short_id}");
        }

        Commands::Resume {
            sandbox_id,
            blocking,
            ready_timeout_secs,
        } => {
            let short_id = sandbox_label(sandbox_id.as_str());
            client
                .resume(ResumeRequest {
                    activation_auto_lifecycle: None,
                    expected_execution_generation: None,
                    sandbox_id: sandbox_id.to_string(),
                    blocking,
                    ready_timeout_secs,
                })
                .await
                .unwrap_or_else(|e| exit_error(e));
            println!("Resumed: {short_id}");
        }

        Commands::Suspend {
            sandbox_id,
            blocking,
            format,
        } => {
            let short_id = sandbox_label(sandbox_id.as_str());
            client
                .suspend(SuspendRequest {
                    sandbox_id: sandbox_id.to_string(),
                    blocking,
                    format: format.to_proto() as i32,
                })
                .await
                .unwrap_or_else(|e| exit_error(e));
            println!("Suspended: {short_id}");
        }

        Commands::Wake {
            sandbox_id,
            blocking,
        } => {
            let short_id = sandbox_label(sandbox_id.as_str());
            client
                .wake(WakeRequest {
                    implicit_wake_source: 0,
                    sandbox_id: sandbox_id.to_string(),
                    blocking,
                    start_context: None,
                    target_state: 0,
                    runtime_upgrade: None,
                })
                .await
                .unwrap_or_else(|e| exit_error(e));
            println!("Woke: {short_id}");
        }

        Commands::Kill {
            sandbox_id,
            signal,
            blocking,
        } => {
            let short_id = sandbox_label(sandbox_id.as_str());
            client
                .kill(KillRequest {
                    sandbox_id: sandbox_id.to_string(),
                    signal,
                    blocking,
                })
                .await
                .unwrap_or_else(|e| exit_error(e));
            println!("Killed (signal {signal}): {short_id}");
        }

        Commands::Checkpoint {
            sandbox_id,
            name,
            blocking: _,
        } => {
            let short_id = sandbox_label(sandbox_id.as_str());
            let resp = client
                .create_snapshot(CreateSnapshotRequest {
                    sandbox_id: sandbox_id.to_string(),
                    r#type: SnapshotType::Full.into(),
                    label: name,
                    creation_identity: None,
                    source: None,
                })
                .await
                .unwrap_or_else(|e| exit_error(e))
                .into_inner();
            println!("Snapshot: {short_id} -> {}", resp.snapshot_name);
        }

        Commands::Restore {
            sandbox_id,
            blocking,
            ready_timeout_secs,
            suspend_restore_storage_uri,
        } => {
            let short_id = sandbox_label(sandbox_id.as_str());
            let resp = client
                .start(local_restore_request(
                    sandbox_id.to_string(),
                    blocking,
                    ready_timeout_secs,
                    suspend_restore_storage_uri,
                ))
                .await
                .unwrap_or_else(|e| exit_error(e))
                .into_inner();
            println!("Restored:  {short_id}");
            println!("PID:       {}", resp.pid);
            println!("Guest IP:  {}", resp.guest_ip);
            println!("Namespace: {}", resp.namespace);
        }

        Commands::EnableSnapshots { sandbox_id } => {
            let short_id = sandbox_label(sandbox_id.as_str());
            warn_if_inbound_reconciliation_active(FACET_SANDBOX_DESIRED_STATE).await;
            client
                .enable_snapshots(EnableSnapshotsRequest {
                    sandbox_id: sandbox_id.to_string(),
                })
                .await
                .unwrap_or_else(|e| exit_error(e));
            println!("Snapshots enabled: {short_id}");
        }

        Commands::DisableSnapshots { sandbox_id } => {
            let short_id = sandbox_label(sandbox_id.as_str());
            warn_if_inbound_reconciliation_active(FACET_SANDBOX_DESIRED_STATE).await;
            client
                .disable_snapshots(DisableSnapshotsRequest {
                    sandbox_id: sandbox_id.to_string(),
                })
                .await
                .unwrap_or_else(|e| exit_error(e));
            println!("Snapshots disabled: {short_id}");
        }

        Commands::Snapshots {
            sandbox_id,
            delete,
            delete_all,
        } => {
            if delete_all || delete.is_some() {
                let resp = client
                    .delete_snapshots(DeleteSnapshotsRequest {
                        sandbox_id: sandbox_id.to_string(),
                        snapshot_name: delete,
                        all: delete_all,
                    })
                    .await
                    .unwrap_or_else(|e| exit_error(e))
                    .into_inner();

                if resp.deleted.is_empty() {
                    println!("No snapshots deleted.");
                } else {
                    for name in &resp.deleted {
                        println!("Deleted: {name}");
                    }
                    println!("{} snapshot(s) deleted.", resp.deleted.len());
                }
                return;
            }

            let resp = client
                .list_snapshots(ListSnapshotsRequest {
                    sandbox_ids: vec![sandbox_id.to_string()],
                })
                .await
                .unwrap_or_else(|e| exit_error(e))
                .into_inner();

            if resp.snapshots.is_empty() {
                println!("No snapshots found.");
                return;
            }

            let mut table = Table::new();
            table.load_style(UTF8_FULL_CONDENSED);
            table.set_header(vec!["NAME", "TYPE", "STATE", "TIER", "TIMESTAMP", "SIZE"]);

            for s in &resp.snapshots {
                let type_str =
                    match pplx_space_sbx_mgr_client::proto::SnapshotType::try_from(s.r#type) {
                        Ok(SnapshotType::Disk) => "disk",
                        Ok(SnapshotType::Full) => "full",
                        _ => "?",
                    };
                table.add_row(vec![
                    s.id.clone(),
                    type_str.to_string(),
                    s.state.clone(),
                    s.retention_tier.clone().unwrap_or_default(),
                    format_snapshot_ts(s.timestamp.as_deref().unwrap_or("")),
                    s.size_bytes
                        .map(format_bytes)
                        .unwrap_or_else(|| "unknown".into()),
                ]);
            }

            println!("{table}");
            println!();
            println!("{} snapshot(s).", resp.snapshots.len());
        }
        _ => unreachable!("command routed to wrong dispatch group"),
    }
}

fn local_restore_request(
    sandbox_id: String,
    blocking: bool,
    ready_timeout_secs: Option<u32>,
    suspend_restore_storage_uri: Option<String>,
) -> StartRequest {
    pplx_space_sbx_mgr_client::direct_restore_start_request(
        sandbox_id,
        blocking,
        ready_timeout_secs,
        suspend_restore_storage_uri,
        None,
    )
}

#[cfg(test)]
mod tests;
