use super::*;

pub(super) async fn handle(
    command: Commands,
    client: &mut pplx_space_sbx_mgr_client::SandboxManagerGrpcClient,
) {
    match command {
        Commands::Doctor { sandbox_id } => {
            let sandbox = resolve_sandbox_state(client, &sandbox_id).await;
            let full_id = &sandbox.sandbox_id;
            let short_id = sandbox_label(full_id);
            let lifecycle = sandbox
                .observed_lifecycle()
                .unwrap_or_else(|error| exit_error(error));
            println!("Doctor: {short_id}  ({})", lifecycle.label());
            println!();

            if !matches!(
                lifecycle,
                pplx_space_sbx_mgr_client::proto::SandboxLifecycleState::Started
                    | pplx_space_sbx_mgr_client::proto::SandboxLifecycleState::Paused
            ) {
                println!("Sandbox is not running — nothing to diagnose.");
                return;
            }

            // Collect PIDs from the sandbox info
            let run_dir = pplx_space_common_core::host::linux::paths::sandbox_run_dir(full_id);
            let processes: Vec<(&str, Option<u32>)> = vec![
                ("firecracker", sandbox.pid),
                (
                    "spaced-host",
                    read_pid_symlink(&format!("{run_dir}/space-spaced-host-svc.pid")),
                ),
                (
                    "net-gateway",
                    read_pid_symlink(&format!("{run_dir}/space-netgtw-svc.pid")),
                ),
            ];

            let mut table = Table::new();
            table.load_style(UTF8_FULL_CONDENSED);
            table.set_header(vec![
                "PROCESS",
                "PID",
                "STATUS",
                "CPU%",
                "MEM (RSS)",
                "THREADS",
            ]);

            let mut all_healthy = true;

            for (name, pid) in &processes {
                match pid {
                    None => {
                        all_healthy = false;
                        table.add_row(vec![
                            name.to_string(),
                            "-".into(),
                            "\u{274c} not found".into(),
                            "-".into(),
                            "-".into(),
                            "-".into(),
                        ]);
                    }
                    Some(pid) => {
                        let info = read_proc_info(*pid);
                        let status_str = match info.state {
                            'Z' => {
                                all_healthy = false;
                                "\u{1f480} zombie".to_string()
                            }
                            'T' | 't' => "\u{23f8}\u{fe0f}  stopped".to_string(),
                            'S' | 'D' | 'R' | 'I' => "\u{2705} healthy".to_string(),
                            c => {
                                all_healthy = false;
                                format!("\u{2753} {c}")
                            }
                        };
                        table.add_row(vec![
                            name.to_string(),
                            pid.to_string(),
                            status_str,
                            format!("{:.1}", info.cpu_percent),
                            format_bytes(info.rss_bytes),
                            info.threads.to_string(),
                        ]);
                    }
                }
            }

            println!("{table}");
            println!();
            if all_healthy {
                println!("\u{2705} All processes healthy.");
            } else {
                println!("\u{274c} Issues detected — see table above.");
            }
        }

        Commands::Rollback {
            sandbox_id,
            snapshot,
            no_restart,
        } => {
            let short_id = sandbox_label(sandbox_id.as_str());
            let resp = client
                .rollback(RollbackRequest {
                    sandbox_id: sandbox_id.to_string(),
                    snapshot_name: snapshot,
                    auto_restart: !no_restart,
                    netns_path: None,
                    route_identity: None,
                })
                .await
                .unwrap_or_else(|e| exit_error(e))
                .into_inner();

            println!("Rolled back {short_id} to: {}", resp.rolled_back_to);
            if !no_restart {
                println!("Sandbox restarted.");
            }
        }

        Commands::Snapshot {
            sandbox_id,
            full,
            disk: _,
            blocking: _,
        } => {
            let short_id = sandbox_label(sandbox_id.as_str());
            let snap_type = if full {
                SnapshotType::Full
            } else {
                SnapshotType::Disk
            };
            let resp = client
                .create_snapshot(CreateSnapshotRequest {
                    sandbox_id: sandbox_id.to_string(),
                    r#type: snap_type.into(),
                    label: None,
                    creation_identity: None,
                    source: None,
                })
                .await
                .unwrap_or_else(|e| exit_error(e))
                .into_inner();
            let type_label = if full { "Full" } else { "Disk" };
            println!(
                "{type_label} snapshot: {short_id} -> {}",
                resp.snapshot_name
            );
        }

        Commands::Wait {
            sandbox_id,
            timeout,
        } => {
            let short_id = sandbox_label(sandbox_id.as_str());
            println!("Waiting for {short_id} to reach terminal state...");
            let resp = client
                .wait(WaitRequest {
                    sandbox_id: sandbox_id.to_string(),
                    timeout_secs: timeout,
                })
                .await
                .unwrap_or_else(|e| exit_error(e))
                .into_inner();

            println!(
                "Sandbox {} reached state: {}",
                sandbox_label(&resp.sandbox_id),
                resp.observed_lifecycle()
                    .unwrap_or_else(|error| exit_error(error))
                    .label()
            );
        }
        _ => unreachable!("command routed to wrong dispatch group"),
    }
}
