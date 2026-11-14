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

#[test]
fn a_discard_delete_needs_its_reason_and_one_full_uuid() {
    const ID: &str = "019a0000-0000-7000-8000-000000000001";
    let parse = |extra: &[&str]| {
        let mut args = vec!["space-sbxmgr", "delete", ID];
        args.extend_from_slice(extra);
        Cli::try_parse_from(args)
    };
    let parsed = parse(&["--discard-retained-filesystem", "--reason", "node lost"])
        .expect("a discard with a reason parses");
    let Commands::Delete {
        discard_retained_filesystem,
        reason,
        ..
    } = parsed.command
    else {
        panic!("not a delete");
    };
    assert!(discard_retained_filesystem);
    assert_eq!(reason.as_deref(), Some("node lost"));

    for (case, extra) in [
        ("no reason", &["--discard-retained-filesystem"][..]),
        ("a reason alone", &["--reason", "node lost"][..]),
        (
            "with --all",
            &["--discard-retained-filesystem", "--reason", "x", "--all"][..],
        ),
        (
            "with --template",
            &[
                "--discard-retained-filesystem",
                "--reason",
                "x",
                "--template",
                "t",
            ][..],
        ),
    ] {
        assert!(parse(extra).is_err(), "{case} must be refused");
    }

    assert_eq!(
        discard_target(Some(ID.parse().unwrap()))
            .unwrap()
            .to_string(),
        ID
    );
    discard_target(Some("019a0000".parse().unwrap())).unwrap_err();
    discard_target(None).unwrap_err();
}

#[test]
fn a_discard_is_sent_only_for_a_sandbox_held_as_its_only_copy() {
    use pplx_space_sbx_mgr_client::proto::SandboxLifecycleState;
    let sandbox = |lifecycle: SandboxLifecycleState, failure_reason: Option<&str>| SandboxInfo {
        sandbox_id: "019a0000-0000-7000-8000-000000000001".to_owned(),
        lifecycle: Some(lifecycle as i32),
        failure_reason: failure_reason.map(str::to_owned),
        ..Default::default()
    };
    let stranded = Some(pplx_space_sbx_mgr_client::SUSPEND_PUBLISH_FAILED_NO_ARTIFACT_RUNTIME_GONE);
    for held in [
        stranded,
        Some(pplx_space_sbx_mgr_client::SUSPEND_CAPTURE_UNRESOLVED),
    ] {
        discard_precheck(&sandbox(SandboxLifecycleState::Quarantined, held)).unwrap();
        discard_precheck(&sandbox(SandboxLifecycleState::Failed, held)).unwrap();
    }
    for (case, info) in [
        (
            "a stopped sandbox",
            sandbox(SandboxLifecycleState::Stopped, stranded),
        ),
        (
            "another quarantine",
            sandbox(SandboxLifecycleState::Quarantined, Some("pause_failed")),
        ),
        (
            "no failure reason",
            sandbox(SandboxLifecycleState::Quarantined, None),
        ),
    ] {
        assert!(discard_precheck(&info).is_err(), "{case}");
    }
}

/// An older sandbox-manager ignores the discard field and runs an ordinary
/// delete, which erases any row the retention policy doesn't hold. So the
/// pre-check is what keeps a discard sent to such a node safe: it passes only
/// a row that node's ordinary delete refuses, and a report it can't read as
/// one, such as a lifecycle it doesn't know or none at all, is refused before
/// anything is sent.
#[test]
fn a_discard_reaches_an_older_node_only_where_its_ordinary_delete_refuses() {
    use pplx_space_sbx_mgr_client::proto::SandboxLifecycleState;
    let stranded =
        Some(pplx_space_sbx_mgr_client::SUSPEND_PUBLISH_FAILED_NO_ARTIFACT_RUNTIME_GONE.to_owned());
    let report = |lifecycle: Option<i32>, failure_reason: Option<String>| SandboxInfo {
        sandbox_id: "019a0000-0000-7000-8000-000000000002".to_owned(),
        lifecycle,
        failure_reason,
        ..Default::default()
    };
    for (case, info) in [
        ("no lifecycle reported", report(None, stranded.clone())),
        (
            "a lifecycle this CLI doesn't know",
            report(Some(i32::MAX), stranded.clone()),
        ),
        (
            "a started sandbox",
            report(
                Some(SandboxLifecycleState::Started as i32),
                stranded.clone(),
            ),
        ),
        (
            "a quarantine without a retention reason",
            report(Some(SandboxLifecycleState::Quarantined as i32), None),
        ),
    ] {
        assert!(discard_precheck(&info).is_err(), "{case}");
    }
    for (lifecycle, reason) in [
        (SandboxLifecycleState::Quarantined, stranded.clone()),
        (
            SandboxLifecycleState::Failed,
            Some(pplx_space_sbx_mgr_client::SUSPEND_CAPTURE_UNRESOLVED.to_owned()),
        ),
    ] {
        let held = report(Some(lifecycle as i32), reason);
        discard_precheck(&held).unwrap();
        assert!(
            pplx_space_sbx_mgr_client::suspend_evidence_requires_retention(
                held.failure_reason.as_deref()
            ),
            "the older node's ordinary delete refuses this row too"
        );
    }
}
