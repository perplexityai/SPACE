use std::time::Duration;

use clap::Parser;

use super::UploadAdmissionArgs;
use crate::upload_admission::UploadAdmissionConfig;

/// A command line with nothing but the upload-admission flags, as a service
/// flattens them into its own.
#[derive(Parser)]
struct Cli {
    #[command(flatten)]
    upload: UploadAdmissionArgs,
}

fn config(args: &[&str]) -> Option<UploadAdmissionConfig> {
    Cli::try_parse_from(std::iter::once("test").chain(args.iter().copied()))
        .unwrap()
        .upload
        .config()
}

#[test]
fn upload_admission_is_off_unless_enabled() {
    // Clap reads these from the environment, so run without them set.
    if !pplx_space_test_env::enter_test_process(&[
        ("SPACE_DATA_PLANE_UPLOAD_ADMISSION", None),
        ("SPACE_DATA_PLANE_MAX_CONCURRENT_UPLOADS", None),
        ("SPACE_DATA_PLANE_MAX_IN_FLIGHT_UPLOAD_BYTES", None),
        ("SPACE_DATA_PLANE_UPLOAD_ADMISSION_WAIT_MS", None),
        ("SPACE_DATA_PLANE_MAX_CONCURRENT_UPLOADS_PER_ORG", None),
        ("SPACE_DATA_PLANE_MAX_IN_FLIGHT_UPLOAD_BYTES_PER_ORG", None),
        ("SPACE_DATA_PLANE_MAX_CONCURRENT_UPLOADS_PER_SANDBOX", None),
        (
            "SPACE_DATA_PLANE_MAX_IN_FLIGHT_UPLOAD_BYTES_PER_SANDBOX",
            None,
        ),
        ("SPACE_DATA_PLANE_ORG_TRANSFER_BYTES_PER_SECOND", None),
        ("SPACE_DATA_PLANE_ORG_TRANSFER_BURST_BYTES", None),
        ("SPACE_DATA_PLANE_UPLOAD_MIN_BODY_BYTES_PER_SECOND", None),
        ("SPACE_DATA_PLANE_UPLOAD_MIN_BODY_RATE_WINDOW_MS", None),
        ("SPACE_DATA_PLANE_UPLOAD_HTTP2_MAX_DURATION_MS", None),
    ]) {
        return;
    }
    assert!(config(&[]).is_none());
}

/// The env var shares this parser, so the argv rows cover its spellings.
#[test]
fn upload_admission_accepts_boolean_spellings() {
    for value in ["true", "1", "yes", "on"] {
        let flag = format!("--upload-admission={value}");
        assert!(config(&[&flag]).is_some(), "{value}");
    }
    for value in ["false", "0", "no", "off"] {
        let flag = format!("--upload-admission={value}");
        assert!(config(&[&flag]).is_none(), "{value}");
    }
    assert!(config(&["--upload-admission"]).is_some());
    assert!(Cli::try_parse_from(["test", "--upload-admission=maybe"]).is_err());
}

#[test]
fn upload_admission_is_enabled_from_the_environment() {
    if !pplx_space_test_env::enter_test_process(&[(
        "SPACE_DATA_PLANE_UPLOAD_ADMISSION",
        Some(std::ffi::OsStr::new("1")),
    )]) {
        return;
    }
    assert!(config(&[]).is_some());
}

#[test]
fn upload_admission_limits_are_overridable_and_reject_zero() {
    let admission = config(&[
        "--upload-admission",
        "--max-concurrent-uploads=8",
        "--max-in-flight-upload-bytes=4096",
        "--upload-admission-wait-ms=500",
    ])
    .unwrap();
    assert_eq!(admission.max_concurrent_uploads.get(), 8);
    assert_eq!(admission.max_in_flight_bytes.get(), 4096);
    assert_eq!(admission.wait, Duration::from_millis(500));

    assert!(Cli::try_parse_from(["test", "--max-concurrent-uploads=0"]).is_err());
    assert!(Cli::try_parse_from(["test", "--max-in-flight-upload-bytes=0"]).is_err());
}

#[test]
fn upload_shares_and_body_governance_are_overridable() {
    let admission = config(&[
        "--upload-admission",
        "--max-concurrent-uploads-per-org=8",
        "--max-in-flight-upload-bytes-per-org=8192",
        "--max-concurrent-uploads-per-sandbox=2",
        "--max-in-flight-upload-bytes-per-sandbox=2048",
        "--org-transfer-bytes-per-second=1000",
        "--org-transfer-burst-bytes=5000",
        "--upload-min-body-bytes-per-second=10",
        "--upload-min-body-rate-window-ms=500",
        "--upload-http2-max-duration-ms=60000",
    ])
    .unwrap();
    assert_eq!(admission.per_org.max_concurrent_uploads.get(), 8);
    assert_eq!(admission.per_org.max_in_flight_bytes.get(), 8192);
    assert_eq!(admission.per_sandbox.max_concurrent_uploads.get(), 2);
    assert_eq!(admission.per_sandbox.max_in_flight_bytes.get(), 2048);
    let rate = admission.org_transfer_rate.unwrap();
    assert_eq!(
        (rate.bytes_per_second.get(), rate.burst_bytes.get()),
        (1000, 5000)
    );
    let floor = admission.min_body_rate.unwrap();
    assert_eq!(floor.bytes_per_second.get(), 10);
    assert_eq!(floor.window, Duration::from_millis(500));
    assert_eq!(
        admission.http2_upload_max_duration,
        Some(Duration::from_secs(60))
    );
}

#[test]
fn the_transfer_rate_and_body_floor_turn_off_but_never_go_to_zero() {
    let admission = config(&[
        "--upload-admission",
        "--org-transfer-bytes-per-second=off",
        "--upload-min-body-bytes-per-second=OFF",
        "--upload-http2-max-duration-ms=off",
    ])
    .unwrap();
    assert!(admission.org_transfer_rate.is_none());
    assert!(admission.min_body_rate.is_none());
    assert!(admission.http2_upload_max_duration.is_none());

    for flag in [
        "--max-concurrent-uploads-per-org=0",
        "--max-in-flight-upload-bytes-per-org=0",
        "--max-concurrent-uploads-per-sandbox=0",
        "--max-in-flight-upload-bytes-per-sandbox=0",
        "--org-transfer-bytes-per-second=0",
        "--org-transfer-burst-bytes=0",
        "--upload-min-body-bytes-per-second=0",
        "--upload-min-body-rate-window-ms=0",
        "--upload-http2-max-duration-ms=0",
    ] {
        assert!(Cli::try_parse_from(["test", flag]).is_err(), "{flag}");
    }
}
