use super::*;
use pplx_space_sbx_mgr_client::proto::CreateResponse;

#[test]
fn parse_boot_mode_accepts_known_modes() {
    assert_eq!(parse_boot_mode("cold").unwrap(), BootMode::Cold);
    assert_eq!(
        parse_boot_mode("base-snapshot").unwrap(),
        BootMode::BaseSnapshot
    );
    assert_eq!(parse_boot_mode("pool").unwrap(), BootMode::Pool);
    parse_boot_mode("warm").unwrap_err();
}

#[test]
fn create_response_lines_include_runtime_fields_only_when_started() {
    let stopped = CreateResponse {
        sandbox_id: "019d84bd-280d-7331-99c6-b63377f3a3a2".to_string(),
        boot_mode_used: BootMode::Cold.into(),
        ..Default::default()
    };
    assert_eq!(
        create_response_lines(&stopped),
        vec!["Sandbox:   77f3a3a2", "Boot mode: cold"]
    );

    let started = CreateResponse {
        sandbox_id: "sbx_019d84bd-280d-7331-99c6-b63377f3a3a2".to_string(),
        pid: Some(42),
        guest_ip: Some("10.0.0.2".to_string()),
        namespace: Some("sns-test".to_string()),
        log_path: Some("/tmp/sandbox.log".to_string()),
        boot_mode_used: BootMode::Pool.into(),
        ..Default::default()
    };
    assert_eq!(
        create_response_lines(&started),
        vec![
            "Sandbox:   77f3a3a2",
            "PID:       42",
            "Guest IP:  10.0.0.2",
            "Namespace: sns-test",
            "Boot mode: pool",
            "Log:       tail -f /tmp/sandbox.log",
        ]
    );
}

#[test]
fn id_prefix_for_selected_scope_requires_id_or_all() {
    assert_eq!(
        id_prefix_for_selected_scope(None, true).unwrap(),
        "all".to_string()
    );
    assert_eq!(
        id_prefix_for_selected_scope(Some(SandboxSelector::parse("abc123").unwrap()), false)
            .unwrap(),
        "abc123".to_string()
    );
    id_prefix_for_selected_scope(None, false).unwrap_err();
}

#[test]
fn local_create_and_start_delegate_direct_identity_to_sandbox_manager() {
    let create = local_create_request(
        CreateCommandInput {
            environment: None,
            content_hash: None,
            runtime: Some("fc".to_string()),
            template: "seed".to_string(),
            user: None,
            snapshots: false,
            autostart: false,
            boot_mode: "cold".to_string(),
            blocking: false,
            env: Vec::new(),
            rootfs_size_mb: None,
            auto_lifecycle: AutoLifecycleArgs::default(),
        },
        BootMode::Cold,
        "root".to_string(),
        Default::default(),
        None,
    );
    let start = local_start_request(
        "sandbox-id".to_string(),
        None,
        Some(4096),
        false,
        true,
        Some(60),
    );

    assert_eq!(create.route_identity, None);
    assert_eq!(start.route_identity, None);
    assert_eq!(start.memory_mib, Some(4096));
}
