use super::*;

#[tokio::test]
async fn canceled_attestation_keeps_its_worker_permit_until_blocking_work_finishes() {
    let limit = std::sync::Arc::new(tokio::sync::Semaphore::new(1));
    let (first_started_tx, first_started_rx) = tokio::sync::oneshot::channel();
    let (release_first_tx, release_first_rx) = std::sync::mpsc::channel();
    let first_limit = std::sync::Arc::clone(&limit);
    let first = tokio::spawn(async move {
        run_endpoint_attestation_with_limit(
            first_limit,
            EndpointAttestationPhase::Capture,
            move || {
                first_started_tx.send(()).unwrap();
                release_first_rx.recv().unwrap();
                Ok(())
            },
        )
        .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(1), first_started_rx)
        .await
        .expect("first blocking worker should start")
        .expect("first blocking worker should report startup");
    first.abort();
    assert!(
        first.await.unwrap_err().is_cancelled(),
        "first attestation caller should be canceled"
    );
    assert_eq!(
        limit.available_permits(),
        0,
        "a canceled caller must not release a permit while its blocking worker is still running"
    );

    let (second_started_tx, second_started_rx) = tokio::sync::oneshot::channel();
    let second_limit = std::sync::Arc::clone(&limit);
    let second = tokio::spawn(async move {
        run_endpoint_attestation_with_limit(
            second_limit,
            EndpointAttestationPhase::Capture,
            move || {
                second_started_tx.send(()).unwrap();
                Ok(())
            },
        )
        .await
    });

    release_first_tx.send(()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), second_started_rx)
        .await
        .expect("second blocking worker should start after the first finishes")
        .expect("second blocking worker should report startup");
    second.await.unwrap().unwrap();
}

#[tokio::test]
async fn saturated_attestation_returns_retryable_error_within_a_bound() {
    let limit = std::sync::Arc::new(tokio::sync::Semaphore::new(1));
    let permit = limit.clone().acquire_owned().await.unwrap();

    let result = tokio::time::timeout(
        std::time::Duration::from_millis(50),
        run_endpoint_attestation_with_limit_and_timeout(
            limit,
            std::time::Duration::from_millis(10),
            EndpointAttestationPhase::Capture,
            || Ok(()),
        ),
    )
    .await;
    drop(permit);

    let error = result
        .expect("saturated attestation admission must be bounded")
        .expect_err("saturated attestation admission must be rejected");
    assert_eq!(error.code(), tonic::Code::Unavailable);
}

#[test]
fn proc_stat_parser_handles_spaces_and_parentheses_in_command() {
    let stat =
        "42 (worker name) with paren)) S 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 98765 20";
    assert_eq!(parse_proc_stat_start_time(stat).unwrap(), 98765);
}

#[test]
fn descriptor_discovery_reads_the_runtime_network_namespace() {
    let proc_root = tempfile::tempdir().unwrap();
    let runtime_pid = 42;
    let runtime_net_dir = proc_root.path().join(runtime_pid.to_string()).join("net");
    let runtime_fd_dir = proc_root.path().join(runtime_pid.to_string()).join("fd");
    std::fs::create_dir_all(&runtime_net_dir).unwrap();
    std::fs::create_dir_all(&runtime_fd_dir).unwrap();
    std::fs::create_dir_all(proc_root.path().join("net")).unwrap();
    std::fs::write(
        proc_root.path().join("net/unix"),
        "Num RefCount Protocol Flags Type St Inode Path\n",
    )
    .unwrap();
    std::fs::write(
        runtime_net_dir.join("unix"),
        "Num RefCount Protocol Flags Type St Inode Path\n000: 00000002 00000000 00010000 0001 01 4242 /v.sock\n",
    )
    .unwrap();
    std::os::unix::fs::symlink("socket:[4242]", runtime_fd_dir.join("7")).unwrap();

    assert_eq!(
        find_owning_descriptor_in_proc_root(proc_root.path(), runtime_pid, Path::new("/v.sock"))
            .unwrap(),
        (7, 4242)
    );
}

#[test]
fn descriptor_discovery_rejects_ambiguous_path_ownership() {
    let proc_root = tempfile::tempdir().unwrap();
    let runtime_pid = 42;
    let runtime_net_dir = proc_root.path().join(runtime_pid.to_string()).join("net");
    let runtime_fd_dir = proc_root.path().join(runtime_pid.to_string()).join("fd");
    std::fs::create_dir_all(&runtime_net_dir).unwrap();
    std::fs::create_dir_all(&runtime_fd_dir).unwrap();
    std::fs::write(
        runtime_net_dir.join("unix"),
        concat!(
            "Num RefCount Protocol Flags Type St Inode Path\n",
            "000: 00000002 00000000 00010000 0001 01 4242 /v.sock\n",
            "001: 00000002 00000000 00010000 0001 01 4343 /v.sock\n",
        ),
    )
    .unwrap();
    std::os::unix::fs::symlink("socket:[4242]", runtime_fd_dir.join("7")).unwrap();

    assert!(matches!(
        find_owning_descriptor_in_proc_root(proc_root.path(), runtime_pid, Path::new("/v.sock")),
        Err(EndpointProofError::Mismatch(_))
    ));
}

#[test]
fn process_argument_matching_preserves_empty_values() {
    assert!(!process_arguments_include(
        b"space-spaced-host-svc\0--vsock-uds\0\0/v.sock\0",
        "--vsock-uds",
        "/v.sock",
    ));
}

#[test]
fn proof_capture_rejects_a_pre_peer_pinning_spaced_host() {
    let runtime = RegisteredEndpointRuntime {
        sandbox_id: "sandbox".to_string(),
        sandbox_uid: 20_001,
        guest_cid: 3,
        spaced_host_pid: 43,
        runtime_pid: 42,
        runtime_kind: pplx_space_common_core::types::RuntimeKind::Firecracker,
        vsock_path: PathBuf::from("/space/jail/firecracker/sandbox/root/v.sock"),
    };
    let arguments = format!(
        "space-spaced-host-svc\0--vsock-uds\0{}\0",
        runtime.vsock_path.display()
    );

    assert!(!spaced_host_arguments_match(arguments.as_bytes(), &runtime));
}

#[test]
fn validation_uses_the_recorded_runtime_endpoint() {
    use pplx_space_common_core::types::RuntimeKind;
    use pplx_space_node_runtime_core::control::{guest_endpoint_identity, guest_socket_path};
    for (kind, executable) in [
        (RuntimeKind::Firecracker, "firecracker"),
        (RuntimeKind::CloudHypervisor, "cloud-hypervisor"),
    ] {
        let (name, path) = guest_endpoint_identity(kind).unwrap();
        assert_eq!(name, executable);
        assert_eq!(path, Path::new("/v.sock"));
        let host_path = guest_socket_path(kind, "sandbox").unwrap();
        assert!(host_path.ends_with(format!("{executable}/sandbox/root/v.sock")));
    }
}

#[test]
fn validation_treats_missing_bound_host_state_as_identity_loss() {
    let status = endpoint_proof_status(
        EndpointProofError::Inspect {
            subject: "runtime descriptor".to_string(),
            source: std::io::Error::from(std::io::ErrorKind::NotFound),
        },
        EndpointAttestationPhase::Validate,
    );
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
}

#[test]
fn malformed_local_process_state_is_not_a_caller_argument_error() {
    let status = endpoint_proof_status(
        EndpointProofError::MalformedHostState("process stat is malformed".to_string()),
        EndpointAttestationPhase::Capture,
    );
    assert_eq!(status.code(), tonic::Code::Internal);
}

#[cfg(target_os = "linux")]
#[test]
fn captured_host_binding_rejects_process_descriptor_and_socket_replacement() {
    let temp = tempfile::tempdir().unwrap();
    let socket_path = temp.path().join("v.sock");
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let uid = std::fs::symlink_metadata(&socket_path).unwrap().uid();
    let pid = std::process::id();
    let executable_name = std::env::current_exe()
        .unwrap()
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let runtime = RegisteredEndpointRuntime {
        sandbox_id: SandboxId::new().to_string(),
        sandbox_uid: uid,
        guest_cid: 3,
        spaced_host_pid: pid,
        runtime_pid: pid,
        runtime_kind: pplx_space_common_core::types::RuntimeKind::Firecracker,
        vsock_path: socket_path.clone(),
    };
    let proof = capture_endpoint_proof_host_state(
        &runtime,
        &executable_name,
        &executable_name,
        &socket_path,
    )
    .unwrap();
    validate_endpoint_proof_host_state(
        &proof,
        &runtime,
        &executable_name,
        &executable_name,
        &socket_path,
    )
    .unwrap();

    let _client = std::os::unix::net::UnixStream::connect(&socket_path).unwrap();
    let (_accepted, _) = listener.accept().unwrap();
    validate_endpoint_proof_host_state(
        &proof,
        &runtime,
        &executable_name,
        &executable_name,
        &socket_path,
    )
    .unwrap();

    let mut replaced_process = proof.clone();
    replaced_process
        .spaced_host
        .as_mut()
        .unwrap()
        .start_time_ticks += 1;
    assert!(matches!(
        validate_endpoint_proof_host_state(
            &replaced_process,
            &runtime,
            &executable_name,
            &executable_name,
            &socket_path,
        ),
        Err(EndpointProofError::Mismatch(_))
    ));

    let mut wrong_descriptor = proof.clone();
    wrong_descriptor.vsock.as_mut().unwrap().runtime_fd = u32::MAX;
    assert!(matches!(
        validate_endpoint_proof_host_state(
            &wrong_descriptor,
            &runtime,
            &executable_name,
            &executable_name,
            &socket_path,
        ),
        Err(EndpointProofError::Mismatch(_))
    ));

    drop(listener);
    std::fs::remove_file(&socket_path).unwrap();
    let _replacement = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    assert!(matches!(
        validate_endpoint_proof_host_state(
            &proof,
            &runtime,
            &executable_name,
            &executable_name,
            &socket_path,
        ),
        Err(EndpointProofError::Mismatch(_))
    ));
}
