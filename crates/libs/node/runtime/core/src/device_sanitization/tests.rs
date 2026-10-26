use super::*;
use serde_json::{Value, json};

const ATTEMPT: &str = "01900000-0000-7000-8000-000000000001";

fn completed() -> Value {
    json!({
        "attempt": ATTEMPT,
        "coverage_kind": "cuda_allocations",
        "status": "scrub_completed",
        "coverage": {
            "reported_total_bytes": 64,
            "free_bytes_before": 40,
            "overwritten_and_verified_bytes": 32,
            "uncovered_reported_bytes": 32,
            "allocation_count": 2,
            "verified_passes": 3
        }
    })
}

fn console(report: Value) -> Vec<u8> {
    format!("kernel startup\r\nSPACE_GPU_SANITIZER_REPORT_V1 {report}\r\nreboot: Power down\r\n")
        .into_bytes()
}

#[test]
fn valid_current_report_preserves_uncovered_memory() {
    let limits = CudaScrubLimits::new(24, 64).unwrap();
    let coverage = limits
        .verify_report(ATTEMPT.parse().unwrap(), &console(completed()))
        .unwrap();
    assert_eq!(coverage.overwritten_and_verified_bytes(), 32);
    assert_eq!(coverage.uncovered_reported_bytes(), 32);
}

#[test]
fn clean_poweroff_stale_or_invalid_reports_never_establish_coverage() {
    let limits = CudaScrubLimits::new(24, 64).unwrap();
    let cases: Vec<(&str, Vec<u8>)> = vec![
        ("missing", b"reboot: Power down\n".to_vec()),
        ("truncated", b"SPACE_GPU_SANITIZER_REPORT_V1 {\n".to_vec()),
        (
            "multiple",
            [console(completed()), console(completed())].concat(),
        ),
        (
            "bootstrap",
            [
                b"SPACE_GPU_SANITIZER_BOOT_FAILED exit=1\n".to_vec(),
                console(completed()),
            ]
            .concat(),
        ),
        ("console bound", vec![b' '; MAX_SANITIZER_CONSOLE_BYTES + 1]),
        (
            "report bound",
            [PREFIX.to_vec(), vec![b' '; MAX_REPORT_BYTES + 1]].concat(),
        ),
        (
            "failure",
            console(
                json!({"attempt": ATTEMPT, "coverage_kind": "cuda_allocations", "status": "failed", "error": "readback mismatch"}),
            ),
        ),
    ];
    for (name, data) in cases {
        limits
            .verify_report(ATTEMPT.parse().unwrap(), &data)
            .expect_err(name);
    }
    for (field, value) in [
        ("/attempt", json!("01900000-0000-7000-8000-000000000002")),
        ("/status", json!("qualified")),
        ("/coverage_kind", json!("full_framebuffer")),
        ("/coverage/reported_total_bytes", json!(65)),
        ("/coverage/free_bytes_before", json!(31)),
        ("/coverage/overwritten_and_verified_bytes", json!(23)),
        ("/coverage/uncovered_reported_bytes", json!(0)),
        ("/coverage/allocation_count", json!(0)),
        ("/coverage/allocation_count", json!(1025)),
        ("/coverage/verified_passes", json!(2)),
        ("/coverage/verified_passes", json!(4)),
    ] {
        let mut report = completed();
        *report.pointer_mut(field).unwrap() = value;
        limits
            .verify_report(ATTEMPT.parse().unwrap(), &console(report))
            .expect_err(field);
    }
    let mut report = completed();
    report["ready"] = json!(true);
    limits
        .verify_report(ATTEMPT.parse().unwrap(), &console(report))
        .expect_err("unknown field");
}
