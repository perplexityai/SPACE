use super::*;

#[test]
fn handoff_rejects_service_path_overrides() {
    let defaults = Cli::try_parse_from(["space-volmgr-svc"]).unwrap();
    assert!(defaults.prepare_volume_identity.is_none());
    assert!(!defaults.check_volume_credentials);
    assert!(defaults.expected_volume_identity.is_none());
    let error = Cli::try_parse_from(["space-volmgr-svc", "--expected-volume-identity", "legacy"])
        .err()
        .unwrap();
    assert_eq!(
        error.kind(),
        clap::error::ErrorKind::MissingRequiredArgument
    );
    let guarded = Cli::try_parse_from([
        "space-volmgr-svc",
        "--prepare-volume-identity",
        "legacy",
        "--expected-volume-identity",
        "legacy",
    ])
    .unwrap();
    assert_eq!(
        guarded.expected_volume_identity,
        Some(identity_handoff::Identity::Legacy)
    );
    let probe = Cli::try_parse_from(["space-volmgr-svc", "--check-volume-credentials"]).unwrap();
    assert!(probe.prepare_volume_identity.is_none());
    assert!(probe.check_volume_credentials);
    let normal = Cli::try_parse_from([
        "space-volmgr-svc",
        "--db",
        "/custom/state.db",
        "--socket",
        "/custom/service.sock",
        "--staging-dir",
        "/custom/staging",
    ])
    .unwrap();
    assert!(normal.prepare_volume_identity.is_none());
    assert!(!normal.check_volume_credentials);

    for identity in ["legacy", "separate"] {
        let handoff =
            Cli::try_parse_from(["space-volmgr-svc", "--prepare-volume-identity", identity])
                .unwrap();
        assert!(handoff.prepare_volume_identity.is_some());
        for option in ["--db", "--socket", "--staging-dir"] {
            let error = Cli::try_parse_from([
                "space-volmgr-svc",
                "--prepare-volume-identity",
                identity,
                option,
                "/custom/path",
            ])
            .err()
            .unwrap();
            assert_eq!(error.kind(), clap::error::ErrorKind::ArgumentConflict);
        }
        let error = Cli::try_parse_from([
            "space-volmgr-svc",
            "--prepare-volume-identity",
            identity,
            "--check-volume-credentials",
        ])
        .err()
        .unwrap();
        assert_eq!(error.kind(), clap::error::ErrorKind::ArgumentConflict);
    }
}

#[test]
fn unset_stream_push_budget_streams_and_zero_selects_file_uploads() {
    let unset = snapshot_upload_mode(Err(std::env::VarError::NotPresent)).unwrap();
    assert!(matches!(unset, SnapshotUploadMode::Streaming(_)));
    let zero = snapshot_upload_mode(Ok("0".to_owned())).unwrap();
    assert!(matches!(zero, SnapshotUploadMode::File));
    // Budget validation belongs to SnapshotUploadMode; this layer owns parsing.
    snapshot_upload_mode(Ok("1Gi".to_owned())).unwrap_err();
}
