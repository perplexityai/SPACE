use super::*;
use pplx_space_common_core::types::MAX_ENV_VALUE_BYTES;
use pplx_space_node_api_client::proto::worker_api_internal_server::WorkerApiInternal;
use pplx_space_node_api_client::proto::worker_api_server::WorkerApi;
use pplx_space_sbx_mgr_client::operations as ops;

fn fresh_pair(
    node_id: &str,
) -> (
    tempfile::TempDir,
    SimWorkerApiService,
    SimWorkerApiInternalService,
    SimState,
) {
    let dir = tempfile::tempdir().unwrap();
    let state = SimState::new(
        dir.path().to_path_buf(),
        dir.path().to_path_buf(),
        node_id.into(),
    );
    (
        dir,
        SimWorkerApiService::new(state.clone()),
        SimWorkerApiInternalService::new(state.clone()),
        state,
    )
}

fn test_create_request(sandbox_id: &str) -> sbx::CreateRequest {
    sbx::CreateRequest {
        environment: None,
        template: "seed".into(),
        owner: "root".into(),
        runtime: "fc".into(),
        snapshots_enabled: false,
        volume_encryption: false,
        autostart: false,
        pool_template: None,
        boot_mode: 0,
        blocking: true,
        sandbox_id: Some(sandbox_id.into()),
        content_hash: Some("hash-seed".into()),
        env: Default::default(),
        auto_lifecycle: None,
        rootfs_size_mb: None,
        network_policy: None,
        network_policy_generation: 0,
        network_policy_authority: PolicyAuthority::Cluster.as_proto_i32(),
        auto_lifecycle_authority: PolicyAuthority::Cluster.as_proto_i32(),
        template_network_policy: None,
        template_network_policy_generation: 0,
        template_network_policy_authority: PolicyAuthority::Cluster.as_proto_i32(),
        route_identity: None,
        disk_io_max_operations_per_sec: None,
        disk_io_max_bandwidth_mib_per_sec: None,
    }
}

#[test]
fn simulated_identity_preserves_supplied_canonical_route_dimensions() {
    let requested = sbx::SpaceApiRouteIdentity {
        sandbox_id: Some(uuid::Uuid::now_v7().to_string()),
        route_generation: Some(7),
        current_node_id: Some("control-plane-node".to_string()),
        execution_generation: Some(7),
        pod_uid: Some("pod-uid".to_string()),
        activation_attempt_id: Some("activation-id".to_string()),
        substrate_kind: sbx::SandboxSubstrateKind::Kubernetes as i32,
    };

    assert_eq!(
        sim_direct_route_identity("sim-node", Some(requested.clone())),
        requested
    );
}

#[test]
fn simulated_kubernetes_identity_fills_a_canonical_node_id() {
    let requested = sbx::SpaceApiRouteIdentity {
        substrate_kind: sbx::SandboxSubstrateKind::Kubernetes as i32,
        ..Default::default()
    };

    let actual = sim_direct_route_identity("sim-node", Some(requested));
    assert_eq!(
        actual.current_node_id,
        Some(
            pplx_space_common_svc::types::k8s::space_k8s_node_id("sim-node", "worker").to_string()
        )
    );
}

#[tokio::test]
async fn create_then_state_then_start() {
    let (_dir, svc, _internal, _state) = fresh_pair("node-a");

    let create = svc
        .create_sandbox(Request::new(test_create_request("sbx-1")))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(create.sandbox_id, "sbx-1");

    let state_resp = svc
        .sandbox_state(Request::new(sbx::StateRequest {
            sandbox_ids: vec!["sbx-1".into(), "missing".into()],
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(state_resp.sandboxes.len(), 1);
    assert_eq!(
        state_resp.sandboxes[0].observed_lifecycle().unwrap(),
        sbx::SandboxLifecycleState::Creating
    );
    assert_eq!(state_resp.missing_ids, vec!["missing".to_string()]);

    svc.start_sandbox(Request::new(sbx::StartRequest {
        activation_auto_lifecycle: None,
        operation_activation_attempt_id: None,
        sandbox_id: "sbx-1".into(),
        vcpu: None,
        memory_mib: None,
        checkpoint: None,
        from_warm: false,
        blocking: true,
        netns_path: None,
        ready_timeout_secs: None,
        target_state: 0,
        suspend_restore_storage_uri: None,
        suspend_restore_source_node: None,
        route_identity: None,
        restore_artifact_as_new: None,
        expected_local_artifact_id: None,
    }))
    .await
    .unwrap();
}

#[tokio::test]
async fn stop_and_kill_reach_stopped() {
    let (_dir, svc, _internal, _state) = fresh_pair("node-a");

    for sandbox_id in ["sbx-stop", "sbx-kill"] {
        svc.create_sandbox(Request::new(test_create_request(sandbox_id)))
            .await
            .unwrap();
    }

    let stop = svc
        .stop_sandbox(Request::new(sbx::StopRequest {
            sandbox_id: "sbx-stop".into(),
            template_filter: None,
            blocking: true,
            immediate_kill: false,
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(stop.stopped_ids, vec!["sbx-stop".to_string()]);

    svc.kill_sandbox(Request::new(sbx::KillRequest {
        sandbox_id: "sbx-kill".into(),
        signal: 9,
        blocking: true,
    }))
    .await
    .unwrap()
    .into_inner();

    let state_resp = svc
        .sandbox_state(Request::new(sbx::StateRequest {
            sandbox_ids: vec!["sbx-stop".into(), "sbx-kill".into()],
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(state_resp.sandboxes.len(), 2);
    for sandbox in state_resp.sandboxes {
        assert_eq!(
            sandbox.observed_lifecycle().unwrap(),
            sbx::SandboxLifecycleState::Stopped
        );
    }
}

#[tokio::test]
async fn suspend_on_node_a_then_wake_on_node_b_restores() {
    let dir = tempfile::tempdir().unwrap();
    let state_a = SimState::new(
        dir.path().to_path_buf(),
        dir.path().to_path_buf(),
        "node-a".into(),
    );
    let state_b = SimState::new(
        dir.path().to_path_buf(),
        dir.path().to_path_buf(),
        "node-b".into(),
    );
    let svc_a = SimWorkerApiService::new(state_a.clone());
    let internal_b = SimWorkerApiInternalService::new(state_b.clone());

    let mut create = test_create_request("sbx-x");
    create.snapshots_enabled = true;
    create.content_hash = Some("hash-x".into());
    svc_a.create_sandbox(Request::new(create)).await.unwrap();
    svc_a
        .start_sandbox(Request::new(sbx::StartRequest {
            activation_auto_lifecycle: None,
            operation_activation_attempt_id: None,
            sandbox_id: "sbx-x".into(),
            vcpu: None,
            memory_mib: None,
            checkpoint: None,
            from_warm: false,
            blocking: true,
            netns_path: None,
            ready_timeout_secs: None,
            target_state: 0,
            suspend_restore_storage_uri: None,
            suspend_restore_source_node: None,
            route_identity: None,
            restore_artifact_as_new: None,
            expected_local_artifact_id: None,
        }))
        .await
        .unwrap();
    svc_a
        .suspend_sandbox(Request::new(sbx::SuspendRequest {
            sandbox_id: "sbx-x".into(),
            blocking: true,
            format: 0,
        }))
        .await
        .unwrap();

    // node-b has no in-memory record; WakeSandbox restores from the shared dir.
    let wake = internal_b
        .wake_sandbox(Request::new(sbx::WakeRequest {
            implicit_wake_source: 0,
            sandbox_id: "sbx-x".into(),
            blocking: true,
            start_context: None,
            target_state: 0,
            runtime_upgrade: None,
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(wake.sandbox_id, "sbx-x");
    assert_eq!(wake.transition, sbx::WakeTransition::Restore as i32);
    // node-b now owns the running record.
    assert_eq!(state_b.get("sbx-x").await.unwrap().status, status::RUNNING);
}

#[tokio::test]
async fn internal_adopt_pool_sandbox_claims_paused_member() {
    let (_dir, svc, internal, state) = fresh_pair("node-a");
    let mut create = test_create_request("pool-1");
    create.owner = "pool".into();
    create.pool_template = Some("seed".into());
    svc.create_sandbox(Request::new(create)).await.unwrap();
    state
        .transition("pool-1", &[status::CREATING], status::PAUSED)
        .await
        .unwrap();

    let mut env = std::collections::BTreeMap::new();
    env.insert("SANDBOX_TYPE".to_string(), "example-workload".to_string());

    let adopted = internal
        .adopt_pool_sandbox(Request::new(sbx::AdoptPoolSandboxRequest {
            template: "seed".into(),
            owner: "user:00000000-0000-0000-0000-000000000020".into(),
            env: env.clone().into_iter().collect(),
            auto_lifecycle: None,
            network_policy: None,
            network_policy_generation: 0,
            network_policy_authority: PolicyAuthority::Cluster.as_proto_i32(),
            auto_lifecycle_authority: PolicyAuthority::Cluster.as_proto_i32(),
            content_hash: Some("hash-seed".into()),
            template_network_policy: None,
            template_network_policy_generation: 0,
            template_network_policy_authority: PolicyAuthority::Cluster.as_proto_i32(),
            route_identity: None,
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(adopted.sandbox_id, "pool-1");
    let record = state.get("pool-1").await.unwrap();
    assert_eq!(record.pool_template, None);
    assert_eq!(record.user, "user:00000000-0000-0000-0000-000000000020");
    assert_eq!(record.status, status::PAUSED);
    assert_eq!(record.process_env, env);
}

#[tokio::test]
async fn update_sandbox_replaces_process_env() {
    let (_dir, svc, _internal, state) = fresh_pair("node-a");
    let mut create = test_create_request("pool-1");
    create.owner = "user:00000000-0000-0000-0000-000000000020".into();
    svc.create_sandbox(Request::new(create)).await.unwrap();
    state
        .transition("pool-1", &[status::CREATING], status::PAUSED)
        .await
        .unwrap();
    let mut env = std::collections::BTreeMap::new();
    env.insert("SANDBOX_TYPE".to_string(), "example-workload".to_string());
    env.insert("MULTILINE".to_string(), "line 1\nline 2".to_string());
    let mut requested_env = env.clone();
    requested_env.insert("BAD-NAME".to_string(), "drop".to_string());
    requested_env.insert("TOO_LONG".to_string(), "x".repeat(MAX_ENV_VALUE_BYTES + 1));

    svc.update_sandbox(Request::new(sbx::UpdateRequest {
        sandbox_id: "pool-1".into(),
        vcpu: None,
        memory_mib: None,
        pids_max: None,
        expected_current_owner: Some("user:00000000-0000-0000-0000-000000000020".into()),
        owner: Some("user:00000000-0000-0000-0000-000000000020".into()),
        process_env: requested_env.into_iter().collect(),
        replace_process_env: true,
    }))
    .await
    .unwrap();

    let record = state.get("pool-1").await.unwrap();
    assert_eq!(record.process_env, env);
}

#[tokio::test]
async fn spaced_mkdir_write_read_stat_rename_remove_round_trip() {
    use tokio_stream::StreamExt;
    let (_dir, svc, _internal, state) = fresh_pair("node-a");
    // create sandbox
    let mut create = test_create_request("sbx-fs");
    create.content_hash = None;
    svc.create_sandbox(Request::new(create)).await.unwrap();

    // mkdir -p /home/user/data
    svc.spaced_mkdir(Request::new(sbx::SpacedMkdirRequest {
        sandbox_id: "sbx-fs".into(),
        operation: Some(ops::MkdirRequest {
            path: "/home/user/data".into(),
            parents: true,
            exist_ok: false,
            user: None,
        }),
    }))
    .await
    .unwrap();

    // write /home/user/data/hello.txt = "hello, sandbox\n"
    let payload = b"hello, sandbox\n".to_vec();
    let messages: Vec<Result<sbx::SpacedWriteRequest, Status>> = vec![
        Ok(sbx::SpacedWriteRequest {
            sandbox_id: "sbx-fs".into(),
            operation: Some(ops::WriteRequest {
                message: Some(sbx::spaced_write_request::Message::Open(
                    sbx::SpacedWriteOpen {
                        path: "/home/user/data/hello.txt".into(),
                        append: false,
                        idempotency_key: None,
                        user: None,
                    },
                )),
            }),
        }),
        Ok(sbx::SpacedWriteRequest {
            sandbox_id: "sbx-fs".into(),
            operation: Some(ops::WriteRequest {
                message: Some(sbx::spaced_write_request::Message::Data(payload.clone())),
            }),
        }),
    ];
    let stream = tokio_stream::iter(messages);
    let written = super::spaced_write_impl(&state, stream)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(written.bytes_written, payload.len() as u64);

    // stat returns the right size
    let stat = svc
        .spaced_stat(Request::new(sbx::SpacedStatRequest {
            sandbox_id: "sbx-fs".into(),
            operation: Some(ops::StatRequest {
                path: "/home/user/data/hello.txt".into(),
                user: None,
            }),
        }))
        .await
        .unwrap()
        .into_inner();
    let info = stat.info.expect("stat info");
    assert_eq!(info.size_bytes, payload.len() as u64);
    assert_eq!(info.file_type, sbx::SpacedFileType::File as i32);

    // read back
    let read_resp = svc
        .spaced_read(Request::new(sbx::SpacedReadRequest {
            sandbox_id: "sbx-fs".into(),
            operation: Some(ops::ReadRequest {
                path: "/home/user/data/hello.txt".into(),
                offset: 0,
                length: None,
                user: None,
            }),
        }))
        .await
        .unwrap();
    let mut stream = read_resp.into_inner();
    let mut buf = Vec::new();
    while let Some(chunk) = stream.next().await {
        buf.extend(chunk.unwrap().data);
    }
    assert_eq!(buf, payload);

    // rename
    svc.spaced_rename(Request::new(sbx::SpacedRenameRequest {
        sandbox_id: "sbx-fs".into(),
        operation: Some(ops::RenameRequest {
            old_path: "/home/user/data/hello.txt".into(),
            new_path: "/home/user/data/hello-renamed.txt".into(),
            idempotency_key: None,
            user: None,
        }),
    }))
    .await
    .unwrap();
    let stat_err = svc
        .spaced_stat(Request::new(sbx::SpacedStatRequest {
            sandbox_id: "sbx-fs".into(),
            operation: Some(ops::StatRequest {
                path: "/home/user/data/hello.txt".into(),
                user: None,
            }),
        }))
        .await
        .unwrap_err();
    assert_eq!(stat_err.code(), tonic::Code::NotFound);

    // remove
    svc.spaced_remove(Request::new(sbx::SpacedRemoveRequest {
        sandbox_id: "sbx-fs".into(),
        operation: Some(ops::RemoveRequest {
            path: "/home/user/data".into(),
            recursive: true,
            missing_ok: false,
            user: None,
        }),
    }))
    .await
    .unwrap();
}

#[tokio::test]
async fn spaced_write_rejects_path_traversal() {
    let (_dir, svc, _internal, state) = fresh_pair("node-a");
    let mut create = test_create_request("sbx-evil");
    create.content_hash = None;
    svc.create_sandbox(Request::new(create)).await.unwrap();

    let messages: Vec<Result<sbx::SpacedWriteRequest, Status>> =
        vec![Ok(sbx::SpacedWriteRequest {
            sandbox_id: "sbx-evil".into(),
            operation: Some(ops::WriteRequest {
                message: Some(sbx::spaced_write_request::Message::Open(
                    sbx::SpacedWriteOpen {
                        path: "/../escape".into(),
                        append: false,
                        idempotency_key: None,
                        user: None,
                    },
                )),
            }),
        })];
    let stream = tokio_stream::iter(messages);
    let err = super::spaced_write_impl(&state, stream).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn generation_suspend_returns_exact_artifact_and_fences_older_requests() {
    let (_dir, svc, _internal, state) = fresh_pair("node-a");
    state
        .insert_new(crate::state::SandboxRecord::new_creating(
            "sbx-generation".to_string(),
            "seed".to_string(),
            "fc".to_string(),
            "root".to_string(),
            None,
            true,
        ))
        .await
        .unwrap();
    state
        .set_status("sbx-generation", status::RUNNING)
        .await
        .unwrap();

    let durable = svc
        .prepare_teardown_suspend(Request::new(sbx::PrepareTeardownSuspendRequest {
            sandbox_id: "sbx-generation".to_string(),
            execution_generation: 2,
            action: sbx::TeardownSuspendAction::BeginOrJoin as i32,
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(durable.outcome, sbx::TeardownSuspendOutcome::Durable as i32);
    let artifact = durable
        .published_artifact
        .expect("durable simulated suspend must identify its artifact");
    assert_eq!(artifact.artifact_kind, sbx::ArtifactKind::Full as i32);
    assert_eq!(artifact.origin, sbx::ArtifactOrigin::Suspend as i32);

    let observed = svc
        .prepare_teardown_suspend(Request::new(sbx::PrepareTeardownSuspendRequest {
            sandbox_id: "sbx-generation".to_string(),
            execution_generation: 2,
            action: sbx::TeardownSuspendAction::ObserveOnly as i32,
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        observed.outcome,
        sbx::TeardownSuspendOutcome::Durable as i32
    );
    assert_eq!(observed.published_artifact, Some(artifact));

    let superseded = svc
        .prepare_teardown_suspend(Request::new(sbx::PrepareTeardownSuspendRequest {
            sandbox_id: "sbx-generation".to_string(),
            execution_generation: 1,
            action: sbx::TeardownSuspendAction::ObserveOnly as i32,
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        superseded.outcome,
        sbx::TeardownSuspendOutcome::Superseded as i32
    );
}

fn start_request(sandbox_id: &str) -> sbx::StartRequest {
    sbx::StartRequest {
        activation_auto_lifecycle: None,
        operation_activation_attempt_id: None,
        sandbox_id: sandbox_id.into(),
        vcpu: None,
        memory_mib: None,
        checkpoint: None,
        from_warm: false,
        blocking: true,
        netns_path: None,
        ready_timeout_secs: None,
        target_state: 0,
        suspend_restore_storage_uri: None,
        suspend_restore_source_node: None,
        expected_local_artifact_id: None,
        route_identity: None,
        restore_artifact_as_new: None,
    }
}

async fn start_sleep(svc: &SimWorkerApiService, sandbox_id: &str) -> String {
    use tokio_stream::StreamExt;
    let mut stream = svc
        .spaced_start_process(Request::new(sbx::SpacedStartProcessRequest {
            sandbox_id: sandbox_id.into(),
            operation: Some(ops::StartRequest {
                command: vec!["sleep".into(), "30".into()],
                ..Default::default()
            }),
        }))
        .await
        .unwrap()
        .into_inner();
    let started = stream.next().await.unwrap().unwrap();
    assert!(matches!(
        started.event,
        Some(sbx::spaced_process_event::Event::Started(_))
    ));
    started.process_id
}

async fn running_process_ids(svc: &SimWorkerApiService, sandbox_id: &str) -> Vec<String> {
    svc.spaced_list_processes(Request::new(sbx::SpacedListProcessesRequest {
        sandbox_id: sandbox_id.into(),
        operation: Some(ops::ListProcessesRequest::default()),
    }))
    .await
    .unwrap()
    .into_inner()
    .processes
    .into_iter()
    .map(|process| process.process_id)
    .collect()
}

/// Stop and kill are execution teardown, and the fake processes must not
/// outlive it. Before the shared teardown helper, `sleep` → stop or kill →
/// start again → list still reported the old process as running, and its
/// stream stayed open, because only delete and cleanup cleared the table.
#[tokio::test]
async fn stop_and_kill_end_the_sandbox_processes_before_a_restart() {
    let (_dir, svc, _internal, _state) = fresh_pair("node-a");

    for (sandbox_id, teardown) in [("sbx-stop", "stop"), ("sbx-kill", "kill")] {
        svc.create_sandbox(Request::new(test_create_request(sandbox_id)))
            .await
            .unwrap();
        svc.start_sandbox(Request::new(start_request(sandbox_id)))
            .await
            .unwrap();
        let process_id = start_sleep(&svc, sandbox_id).await;
        assert_eq!(
            running_process_ids(&svc, sandbox_id).await,
            vec![process_id.clone()]
        );

        match teardown {
            "stop" => {
                svc.stop_sandbox(Request::new(sbx::StopRequest {
                    sandbox_id: sandbox_id.into(),
                    template_filter: None,
                    blocking: true,
                    immediate_kill: false,
                }))
                .await
                .unwrap();
            }
            _ => {
                svc.kill_sandbox(Request::new(sbx::KillRequest {
                    sandbox_id: sandbox_id.into(),
                    signal: 9,
                    blocking: true,
                }))
                .await
                .unwrap();
            }
        }

        svc.start_sandbox(Request::new(start_request(sandbox_id)))
            .await
            .unwrap();
        assert!(
            running_process_ids(&svc, sandbox_id).await.is_empty(),
            "{teardown}: the restarted sandbox must not list its previous processes"
        );
        let connect = svc
            .spaced_connect_process(Request::new(sbx::SpacedConnectProcessRequest {
                sandbox_id: sandbox_id.into(),
                operation: Some(ops::ConnectProcessRequest {
                    process_id: process_id.clone(),
                    timeout: None,
                    output_cursor: None,
                    resume_from_oldest_retained: false,
                }),
            }))
            .await;
        assert_eq!(
            connect.err().map(|status| status.code()),
            Some(tonic::Code::NotFound),
            "{teardown}: the old process is gone after teardown"
        );
    }
}

/// The spaced process RPCs validate their payload like the sandbox manager:
/// a missing `operation` is `InvalidArgument`, as are an empty `command` on
/// start and an empty `process_id` on connect, signal, and kill. Before this
/// the simulator defaulted the payload, so a start with no command registered
/// a fake process that exited successfully and a connect with no payload
/// looked up the empty process id.
#[tokio::test]
async fn process_rpcs_reject_missing_or_empty_payloads_like_production() {
    let (_dir, svc, _internal, _state) = fresh_pair("node-a");
    svc.create_sandbox(Request::new(test_create_request("sbx-v")))
        .await
        .unwrap();
    svc.start_sandbox(Request::new(start_request("sbx-v")))
        .await
        .unwrap();

    let invalid = |status: Option<tonic::Status>, what: &str| {
        assert_eq!(
            status.as_ref().map(tonic::Status::code),
            Some(tonic::Code::InvalidArgument),
            "{what}: {status:?}"
        );
    };

    invalid(
        svc.spaced_start_process(Request::new(sbx::SpacedStartProcessRequest {
            sandbox_id: "sbx-v".into(),
            operation: None,
        }))
        .await
        .err(),
        "start without a payload",
    );
    invalid(
        svc.spaced_start_process(Request::new(sbx::SpacedStartProcessRequest {
            sandbox_id: "sbx-v".into(),
            operation: Some(ops::StartRequest::default()),
        }))
        .await
        .err(),
        "start with an empty command",
    );
    invalid(
        svc.spaced_connect_process(Request::new(sbx::SpacedConnectProcessRequest {
            sandbox_id: "sbx-v".into(),
            operation: None,
        }))
        .await
        .err(),
        "connect without a payload",
    );
    invalid(
        svc.spaced_connect_process(Request::new(sbx::SpacedConnectProcessRequest {
            sandbox_id: "sbx-v".into(),
            operation: Some(ops::ConnectProcessRequest::default()),
        }))
        .await
        .err(),
        "connect with an empty process id",
    );
    invalid(
        svc.spaced_list_processes(Request::new(sbx::SpacedListProcessesRequest {
            sandbox_id: "sbx-v".into(),
            operation: None,
        }))
        .await
        .err(),
        "list without a payload",
    );
    invalid(
        svc.spaced_send_signal(Request::new(sbx::SpacedSendSignalRequest {
            sandbox_id: "sbx-v".into(),
            operation: Some(ops::SendSignalRequest::default()),
        }))
        .await
        .err(),
        "signal with an empty process id",
    );
    invalid(
        svc.spaced_send_signal(Request::new(sbx::SpacedSendSignalRequest {
            sandbox_id: "sbx-v".into(),
            operation: Some(ops::SendSignalRequest {
                process_id: "sim-proc-1000".into(),
                signal: 0,
            }),
        }))
        .await
        .err(),
        "signal that is not positive",
    );
    invalid(
        svc.spaced_kill_process(Request::new(sbx::SpacedKillProcessRequest {
            sandbox_id: "sbx-v".into(),
            operation: None,
        }))
        .await
        .err(),
        "kill without a payload",
    );

    // Nothing was registered by the rejected starts.
    assert!(running_process_ids(&svc, "sbx-v").await.is_empty());
}

/// A direct wake that hydrates a new execution carries the reserved identity
/// in `WakeStartContext.route_identity`. Start and restore prefer it and record
/// it, so the receipt is the reserved generation rather than the stored or
/// default one; authoritative resident wakes adopt it as well.
#[tokio::test]
async fn internal_wake_prefers_and_records_the_start_context_identity() {
    let (_dir, svc, internal, state) = fresh_pair("node-a");
    svc.create_sandbox(Request::new(test_create_request("sbx-w")))
        .await
        .unwrap();
    svc.start_sandbox(Request::new(start_request("sbx-w")))
        .await
        .unwrap();
    svc.stop_sandbox(Request::new(sbx::StopRequest {
        sandbox_id: "sbx-w".into(),
        template_filter: None,
        blocking: true,
        immediate_kill: false,
    }))
    .await
    .unwrap();
    assert_eq!(state.get("sbx-w").await.unwrap().route_generation, Some(1));

    let wake = internal
        .wake_sandbox(Request::new(sbx::WakeRequest {
            implicit_wake_source: 0,
            sandbox_id: "sbx-w".into(),
            blocking: true,
            start_context: Some(sbx::WakeStartContext {
                route_identity: Some(sbx::SpaceApiRouteIdentity {
                    route_generation: Some(7),
                    execution_generation: Some(7),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            target_state: 0,
            runtime_upgrade: None,
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(wake.transition, sbx::WakeTransition::Start as i32);
    let identity = wake
        .route_identity
        .expect("a start wake reports its identity");
    assert_eq!(identity.route_generation, Some(7));
    assert_eq!(identity.execution_generation, Some(7));
    let record = state.get("sbx-w").await.unwrap();
    assert_eq!(
        record.route_generation,
        Some(7),
        "the reserved identity is recorded"
    );
    assert_eq!(record.execution_generation, Some(7));

    // Resume adopts the authoritative identity without replacing the runtime.
    state
        .transition("sbx-w", &[status::RUNNING], status::PAUSED)
        .await
        .unwrap();
    let resume = internal
        .wake_sandbox(Request::new(sbx::WakeRequest {
            implicit_wake_source: 0,
            sandbox_id: "sbx-w".into(),
            blocking: true,
            start_context: Some(sbx::WakeStartContext {
                route_identity: Some(sbx::SpaceApiRouteIdentity {
                    route_generation: Some(9),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            target_state: 0,
            runtime_upgrade: None,
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resume.transition, sbx::WakeTransition::Resume as i32);
    assert_eq!(resume.route_identity.unwrap().route_generation, Some(9));
    assert_eq!(state.get("sbx-w").await.unwrap().route_generation, Some(9));
}

#[tokio::test]
async fn resident_kubernetes_wake_acknowledges_and_records_requested_identity() {
    for paused in [false, true] {
        let (_dir, svc, internal, state) = fresh_pair("node-a");
        svc.create_sandbox(Request::new(test_create_request("sbx-resident")))
            .await
            .unwrap();
        svc.start_sandbox(Request::new(start_request("sbx-resident")))
            .await
            .unwrap();
        if paused {
            state
                .transition("sbx-resident", &[status::RUNNING], status::PAUSED)
                .await
                .unwrap();
        }
        let requested = sbx::SpaceApiRouteIdentity {
            sandbox_id: Some("sbx-resident".into()),
            route_generation: Some(7),
            execution_generation: Some(7),
            activation_attempt_id: Some(uuid::Uuid::now_v7().to_string()),
            pod_uid: Some("resident-pod".into()),
            substrate_kind: sbx::SandboxSubstrateKind::Kubernetes as i32,
            current_node_id: None,
        };
        let wake = internal
            .wake_sandbox(Request::new(sbx::WakeRequest {
                implicit_wake_source: 0,
                sandbox_id: "sbx-resident".into(),
                blocking: true,
                start_context: Some(sbx::WakeStartContext {
                    route_identity: Some(requested.clone()),
                    ..Default::default()
                }),
                target_state: 0,
                runtime_upgrade: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            wake.transition,
            if paused {
                sbx::WakeTransition::Resume
            } else {
                sbx::WakeTransition::Noop
            } as i32
        );
        let mut expected = requested;
        expected.current_node_id = Some(
            pplx_space_common_svc::types::k8s::space_k8s_node_id("node-a", "worker").to_string(),
        );
        assert_eq!(wake.route_identity, Some(expected.clone()));
        assert_eq!(
            state
                .get("sbx-resident")
                .await
                .unwrap()
                .activation_attempt_id,
            expected.activation_attempt_id
        );
    }
}

/// The activation attempt is persisted with the generations, so a later wake
/// or resume receipt repeats the attempt the simulator reported at start
/// instead of minting a new one for an execution that did not change (receipt
/// ordering would otherwise advance the active direct-route identity).
#[tokio::test]
async fn wake_replays_the_recorded_activation_attempt() {
    let (_dir, svc, internal, state) = fresh_pair("node-a");
    svc.create_sandbox(Request::new(test_create_request("sbx-att")))
        .await
        .unwrap();
    let mut start = start_request("sbx-att");
    start.route_identity = Some(sbx::SpaceApiRouteIdentity {
        route_generation: Some(3),
        execution_generation: Some(3),
        activation_attempt_id: Some("attempt-3".into()),
        ..Default::default()
    });
    let started = svc
        .start_sandbox(Request::new(start))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        started
            .route_identity
            .unwrap()
            .activation_attempt_id
            .as_deref(),
        Some("attempt-3")
    );
    assert_eq!(
        state
            .get("sbx-att")
            .await
            .unwrap()
            .activation_attempt_id
            .as_deref(),
        Some("attempt-3")
    );

    svc.stop_sandbox(Request::new(sbx::StopRequest {
        sandbox_id: "sbx-att".into(),
        template_filter: None,
        blocking: true,
        immediate_kill: false,
    }))
    .await
    .unwrap();
    let wake = internal
        .wake_sandbox(Request::new(sbx::WakeRequest {
            implicit_wake_source: 0,
            sandbox_id: "sbx-att".into(),
            blocking: true,
            start_context: None,
            target_state: 0,
            runtime_upgrade: None,
        }))
        .await
        .unwrap()
        .into_inner();
    let identity = wake.route_identity.expect("start wake reports an identity");
    assert_eq!(identity.route_generation, Some(3));
    assert_eq!(identity.activation_attempt_id.as_deref(), Some("attempt-3"));
}

#[tokio::test]
async fn wake_policy_fences_only_resident_resume() {
    // Running retries and omitted policies are no-ops; only a policy-bearing
    // paused resident must match the observed optional generation.
    for (paused, resident, expected, policy, error) in [
        (true, Some(7), Some(7), true, None),
        (true, None, None, true, None),
        (
            true,
            Some(7),
            None,
            true,
            Some(tonic::Code::FailedPrecondition),
        ),
        (
            true,
            None,
            Some(7),
            true,
            Some(tonic::Code::FailedPrecondition),
        ),
        (
            true,
            Some(8),
            Some(7),
            true,
            Some(tonic::Code::FailedPrecondition),
        ),
        (false, Some(8), Some(7), true, None),
        (true, Some(7), None, false, None),
        (
            true,
            Some(7),
            Some(7),
            false,
            Some(tonic::Code::InvalidArgument),
        ),
    ] {
        let (_dir, _svc, internal, state) = fresh_pair("node-a");
        let mut record = crate::SandboxRecord::new_creating(
            "resident".into(),
            "seed".into(),
            "fc".into(),
            "root".into(),
            None,
            false,
        );
        record.status = if paused {
            status::PAUSED
        } else {
            status::RUNNING
        }
        .into();
        record.execution_generation = resident;
        record.route_generation = resident.map(|g| u64::try_from(g).unwrap());
        state.insert_new(record).await.unwrap();
        state
            .apply_access_deadline("resident", Some(123), None)
            .await
            .unwrap();
        let before = state.get("resident").await.unwrap();
        let result = internal
            .wake_sandbox(Request::new(sbx::WakeRequest {
                sandbox_id: "resident".into(),
                blocking: true,
                start_context: Some(sbx::WakeStartContext {
                    expected_execution_generation: expected,
                    activation_auto_lifecycle: policy.then(|| {
                        pplx_space_common_core::types::AutoLifecyclePolicy::disabled().into()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }))
            .await;
        assert_eq!(
            result.as_ref().err().map(Status::code),
            error,
            "paused={paused} resident={resident:?} expected={expected:?} policy={policy}"
        );
        let after = state.get("resident").await.unwrap();
        if error.is_some() {
            assert_eq!(after.status, before.status);
        } else {
            assert_eq!(after.status, status::RUNNING);
        }
        if error.is_some() || !paused || !policy {
            assert_eq!(after.access_idle_policy, before.access_idle_policy);
            assert_eq!(after.access_extended_until, before.access_extended_until);
        }
        assert_eq!(after.execution_generation, before.execution_generation);
    }
}
