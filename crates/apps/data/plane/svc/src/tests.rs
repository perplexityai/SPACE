use super::*;

fn config(args: &[&str]) -> DataPlaneConfig {
    Cli::try_parse_from(std::iter::once("space-data-plane-svc").chain(args.iter().copied()))
        .unwrap()
        .into()
}

/// The service flattens the library's upload-admission flags into its own
/// command line; the library's tests cover what each flag means.
#[test]
fn upload_admission_flags_reach_the_data_plane_config() {
    let admission = config(&["--upload-admission", "--max-concurrent-uploads-per-org=8"])
        .upload_admission
        .unwrap();
    assert_eq!(admission.per_org.max_concurrent_uploads.get(), 8);
}

#[test]
fn shutdown_grace_parses_from_flag_and_environment() {
    if !pplx_space_test_env::enter_test_process(&[(
        "SPACE_DATA_PLANE_SHUTDOWN_GRACE_SECS",
        Some(std::ffi::OsStr::new("45")),
    )]) {
        return;
    }
    assert_eq!(config(&[]).shutdown_grace, Some(Duration::from_secs(45)));
    // The flag wins over the environment.
    assert_eq!(
        config(&["--shutdown-grace-secs", "9"]).shutdown_grace,
        Some(Duration::from_secs(9))
    );
}

#[test]
fn shutdown_grace_is_unset_without_the_flag_or_environment() {
    if !pplx_space_test_env::enter_test_process(&[("SPACE_DATA_PLANE_SHUTDOWN_GRACE_SECS", None)]) {
        return;
    }
    assert_eq!(config(&[]).shutdown_grace, None);
}

#[test]
fn shutdown_grace_below_the_stream_open_budget_is_rejected() {
    let below = (MIN_SHUTDOWN_GRACE.as_secs() - 1).to_string();
    assert!(
        Cli::try_parse_from(["space-data-plane-svc", "--shutdown-grace-secs", &below]).is_err()
    );
    assert!(Cli::try_parse_from(["space-data-plane-svc", "--shutdown-grace-secs", "0"]).is_err());
    let minimum = MIN_SHUTDOWN_GRACE.as_secs().to_string();
    assert_eq!(
        config(&["--shutdown-grace-secs", &minimum]).shutdown_grace,
        Some(MIN_SHUTDOWN_GRACE)
    );
}

#[test]
fn revocation_check_interval_above_the_maximum_is_rejected() {
    let maximum = MAX_REVOCATION_CHECK_INTERVAL_MS.to_string();
    assert_eq!(
        config(&["--revocation-check-interval-ms", &maximum]).revocation_check_interval,
        Duration::from_millis(MAX_REVOCATION_CHECK_INTERVAL_MS)
    );
    let above = (MAX_REVOCATION_CHECK_INTERVAL_MS + 1).to_string();
    assert!(
        Cli::try_parse_from([
            "space-data-plane-svc",
            "--revocation-check-interval-ms",
            &above
        ])
        .is_err()
    );
}
