use super::*;

fn config(args: &[&str]) -> DataPlaneConfig {
    Cli::try_parse_from(std::iter::once("space-data-plane-svc").chain(args.iter().copied()))
        .unwrap()
        .into()
}

#[test]
fn upload_admission_is_off_unless_enabled() {
    // Clap reads these from the environment, so run without them set.
    if !pplx_space_test_env::enter_test_process(&[
        ("SPACE_DATA_PLANE_UPLOAD_ADMISSION", None),
        ("SPACE_DATA_PLANE_MAX_CONCURRENT_UPLOADS", None),
        ("SPACE_DATA_PLANE_MAX_IN_FLIGHT_UPLOAD_BYTES", None),
        ("SPACE_DATA_PLANE_UPLOAD_ADMISSION_WAIT_MS", None),
    ]) {
        return;
    }
    assert!(config(&[]).upload_admission.is_none());
}

/// The env var shares this parser, so the argv rows cover its spellings.
#[test]
fn upload_admission_accepts_boolean_spellings() {
    for value in ["true", "1", "yes", "on"] {
        let flag = format!("--upload-admission={value}");
        assert!(config(&[&flag]).upload_admission.is_some(), "{value}");
    }
    for value in ["false", "0", "no", "off"] {
        let flag = format!("--upload-admission={value}");
        assert!(config(&[&flag]).upload_admission.is_none(), "{value}");
    }
    assert!(config(&["--upload-admission"]).upload_admission.is_some());
    assert!(Cli::try_parse_from(["space-data-plane-svc", "--upload-admission=maybe"]).is_err());
}

#[test]
fn upload_admission_is_enabled_from_the_environment() {
    if !pplx_space_test_env::enter_test_process(&[(
        "SPACE_DATA_PLANE_UPLOAD_ADMISSION",
        Some(std::ffi::OsStr::new("1")),
    )]) {
        return;
    }
    assert!(config(&[]).upload_admission.is_some());
}

#[test]
fn upload_admission_limits_are_overridable_and_reject_zero() {
    let admission = config(&[
        "--upload-admission",
        "--max-concurrent-uploads=8",
        "--max-in-flight-upload-bytes=4096",
        "--upload-admission-wait-ms=500",
    ])
    .upload_admission
    .unwrap();
    assert_eq!(admission.max_concurrent_uploads.get(), 8);
    assert_eq!(admission.max_in_flight_bytes.get(), 4096);
    assert_eq!(admission.wait, Duration::from_millis(500));

    assert!(Cli::try_parse_from(["space-data-plane-svc", "--max-concurrent-uploads=0"]).is_err());
    assert!(
        Cli::try_parse_from(["space-data-plane-svc", "--max-in-flight-upload-bytes=0"]).is_err()
    );
}
