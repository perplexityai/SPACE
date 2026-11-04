use super::*;

#[test]
fn normal_startup_and_credential_probe_remain_available() {
    let defaults = Cli::try_parse_from(["space-volmgr-svc"]).unwrap();
    assert!(!defaults.check_volume_credentials);
    let probe = Cli::try_parse_from(["space-volmgr-svc", "--check-volume-credentials"]).unwrap();
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
    assert!(!normal.check_volume_credentials);
}

#[test]
fn retired_handoff_options_are_rejected() {
    for option in ["--prepare-volume-identity", "--expected-volume-identity"] {
        for value in [None, Some("legacy"), Some("separate")] {
            let mut arguments = vec!["space-volmgr-svc", option];
            arguments.extend(value);
            let error = Cli::try_parse_from(arguments)
                .err()
                .expect("retired handoff options must not parse");
            assert_eq!(error.kind(), clap::error::ErrorKind::UnknownArgument);
        }
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
