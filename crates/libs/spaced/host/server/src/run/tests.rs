use super::*;

/// The flag shape the local (non-Firecracker) node-manager passes: nothing
/// set, so every service default must survive untouched.
#[test]
fn empty_config_resolves_to_the_service_defaults() {
    let resolved = resolve_service_config(SpacedHostRunConfig::default())
        .expect("an empty config is always valid");
    let defaults = SpacedHostServiceConfig::default();

    assert_eq!(resolved.process_recovery_activation, None);
    assert_eq!(resolved.socket_path, defaults.socket_path);
    assert_eq!(
        resolved.host_control_socket_path,
        defaults.host_control_socket_path
    );
    assert_eq!(
        resolved.bridge_host_socket_path,
        defaults.bridge_host_socket_path
    );
    assert!(matches!(
        resolved.bridge_guest_connector,
        BridgeGuestConnector::Uds(_)
    ));
}

#[test]
fn socket_flags_override_only_the_paths_they_name() {
    let resolved = resolve_service_config(SpacedHostRunConfig {
        socket: Some(PathBuf::from("/tmp/spaced-host.sock")),
        host_control_socket: Some(PathBuf::from("/tmp/host-control.sock")),
        ..SpacedHostRunConfig::default()
    })
    .expect("socket overrides are always valid");

    assert_eq!(resolved.socket_path, PathBuf::from("/tmp/spaced-host.sock"));
    assert_eq!(
        resolved.host_control_socket_path,
        PathBuf::from("/tmp/host-control.sock")
    );
    assert_eq!(
        resolved.bridge_host_socket_path,
        SpacedHostServiceConfig::default().bridge_host_socket_path
    );
}

#[test]
fn uds_bridge_flags_replace_both_bridge_directions() {
    let resolved = resolve_service_config(SpacedHostRunConfig {
        bridge_guest_uds: Some(PathBuf::from("/tmp/bridge-guest.sock")),
        bridge_host_uds: Some(PathBuf::from("/tmp/bridge-host.sock")),
        ..SpacedHostRunConfig::default()
    })
    .expect("UDS-bridge mode needs no Firecracker peer");

    assert_eq!(
        resolved.bridge_host_socket_path,
        PathBuf::from("/tmp/bridge-host.sock")
    );
    match resolved.bridge_guest_connector {
        BridgeGuestConnector::Uds(path) => {
            assert_eq!(path, PathBuf::from("/tmp/bridge-guest.sock"));
        }
        other => panic!("expected a UDS connector, got {other:?}"),
    }
}

#[test]
fn vsock_uds_builds_the_firecracker_connector_and_wins_over_the_uds_flags() {
    let activation = pplx_space_common_core::types::ProcessRecoveryToken::new();
    let resolved = resolve_service_config(SpacedHostRunConfig {
        process_recovery_activation: Some(activation),
        vsock_uds: Some(PathBuf::from("/tmp/fc.vsock")),
        firecracker_pid: Some(4242),
        firecracker_uid: Some(1000),
        // Documented as ignored when `--vsock-uds` is set.
        bridge_guest_uds: Some(PathBuf::from("/tmp/ignored-guest.sock")),
        bridge_host_uds: Some(PathBuf::from("/tmp/ignored-host.sock")),
        ..SpacedHostRunConfig::default()
    })
    .expect("a complete Firecracker peer is valid");
    assert_eq!(resolved.process_recovery_activation, Some(activation));

    assert_eq!(
        resolved.bridge_host_socket_path,
        pplx_space_spaced_common::firecracker_vsock_port_path(
            std::path::Path::new("/tmp/fc.vsock"),
            pplx_space_spaced_common::BRIDGE_HOST_VSOCK_PORT,
        )
    );
    match resolved.bridge_guest_connector {
        BridgeGuestConnector::FirecrackerVsock {
            vsock_uds_path,
            port,
            expected_peer,
        } => {
            assert_eq!(vsock_uds_path, PathBuf::from("/tmp/fc.vsock"));
            assert_eq!(port, pplx_space_spaced_common::BRIDGE_GUEST_VSOCK_PORT);
            assert_eq!(expected_peer.pid, 4242);
            assert_eq!(expected_peer.uid, 1000);
        }
        other => panic!("expected a Firecracker vsock connector, got {other:?}"),
    }
}

#[test]
fn vsock_uds_without_a_firecracker_pid_is_rejected() {
    let error = resolve_service_config(SpacedHostRunConfig {
        vsock_uds: Some(PathBuf::from("/tmp/fc.vsock")),
        firecracker_uid: Some(1000),
        ..SpacedHostRunConfig::default()
    })
    .expect_err("a vsock with no peer PID cannot be attested");

    assert!(matches!(error, RunSpacedHostError::MissingFirecrackerPid));
    assert_eq!(
        error.to_string(),
        "--firecracker-pid is required with --vsock-uds"
    );
}

#[test]
fn vsock_uds_without_a_firecracker_uid_is_rejected() {
    let error = resolve_service_config(SpacedHostRunConfig {
        vsock_uds: Some(PathBuf::from("/tmp/fc.vsock")),
        firecracker_pid: Some(4242),
        ..SpacedHostRunConfig::default()
    })
    .expect_err("a vsock with no peer UID cannot be attested");

    assert!(matches!(error, RunSpacedHostError::MissingFirecrackerUid));
    assert_eq!(
        error.to_string(),
        "--firecracker-uid is required with --vsock-uds"
    );
}

#[test]
fn a_firecracker_pid_beyond_the_process_id_range_is_rejected() {
    let error = resolve_service_config(SpacedHostRunConfig {
        vsock_uds: Some(PathBuf::from("/tmp/fc.vsock")),
        firecracker_pid: Some(u32::MAX),
        firecracker_uid: Some(1000),
        ..SpacedHostRunConfig::default()
    })
    .expect_err("a PID that cannot be an i32 cannot be compared to a peer credential");

    assert!(matches!(
        error,
        RunSpacedHostError::FirecrackerPidOutOfRange
    ));
}

/// clap's `requires = "vsock_uds"` makes this unreachable from the command
/// line, but `SpacedHostRunConfig` is public now: an in-process caller can
/// name a Firecracker peer with no vsock to check it on. Rejecting it keeps
/// that caller from silently getting the unauthenticated UDS bridge instead.
#[test]
fn a_firecracker_peer_without_a_vsock_is_rejected_rather_than_ignored() {
    for config in [
        SpacedHostRunConfig {
            firecracker_pid: Some(4242),
            ..SpacedHostRunConfig::default()
        },
        SpacedHostRunConfig {
            firecracker_uid: Some(1000),
            ..SpacedHostRunConfig::default()
        },
        SpacedHostRunConfig {
            firecracker_pid: Some(4242),
            firecracker_uid: Some(1000),
            ..SpacedHostRunConfig::default()
        },
    ] {
        let error = resolve_service_config(config)
            .expect_err("a Firecracker peer with no vsock is incoherent");
        assert!(matches!(
            error,
            RunSpacedHostError::FirecrackerPeerWithoutVsockUds
        ));
    }
}

#[test]
fn boot_types_keep_their_flag_spelling_and_resume_verdict() {
    let expected = [
        (SpacedHostBootType::Fresh, "fresh", None),
        (SpacedHostBootType::Snapshot, "snapshot", None),
        (SpacedHostBootType::BaseSnapshot, "base-snapshot", None),
        (SpacedHostBootType::Restore, "restore", None),
        (
            SpacedHostBootType::Resume,
            "resume",
            Some(ResumeReason::SuspendResume),
        ),
    ];

    for (boot_type, spelling, reason) in expected {
        assert_eq!(boot_type.as_str(), spelling);
        assert_eq!(boot_type.resume_reason(), reason);
    }
    assert_eq!(SpacedHostBootType::default(), SpacedHostBootType::Fresh);
}

/// `LogContext`'s fields are private, so assert through its `Debug` shape:
/// what matters is that both the boot type and the sandbox reach it, since
/// that context is installed before `run` and labels every startup failure.
#[test]
fn log_context_carries_the_boot_type_and_the_sandbox_when_named() {
    let without_sandbox = SpacedHostRunConfig {
        boot_type: SpacedHostBootType::BaseSnapshot,
        ..SpacedHostRunConfig::default()
    }
    .log_context();
    let rendered = format!("{without_sandbox:?}");
    assert!(rendered.contains("base-snapshot"), "{rendered}");
    assert!(rendered.contains("sandbox_id: None"), "{rendered}");

    let with_sandbox = SpacedHostRunConfig {
        boot_type: SpacedHostBootType::Resume,
        sandbox_id: Some("sbx-1234".to_string()),
        ..SpacedHostRunConfig::default()
    }
    .log_context();
    let rendered = format!("{with_sandbox:?}");
    assert!(rendered.contains("resume"), "{rendered}");
    assert!(rendered.contains("sbx-1234"), "{rendered}");
}
