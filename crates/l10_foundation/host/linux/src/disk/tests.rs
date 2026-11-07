use super::*;

#[test]
fn fraction_and_need_math() {
    let usage = DiskUsage {
        used_bytes: 600,
        total_bytes: 1_000,
    };
    assert!((usage.fraction() - 0.6).abs() < 1e-9);
    assert!((usage.free_fraction() - 0.4).abs() < 1e-9);
    // Target 0.4 of 1000 = 400; need = 600 - 400.
    assert_eq!(usage.bytes_over_target(0.4), 200);
    assert_eq!(usage.bytes_over_target(0.9), 0);
    let degenerate = DiskUsage {
        used_bytes: 1,
        total_bytes: 0,
    };
    assert!(degenerate.fraction().abs() < f64::EPSILON);
    assert!(
        degenerate.free_fraction().abs() < f64::EPSILON,
        "a degenerate fs must not advertise headroom"
    );
}

#[test]
fn over_reported_usage_clamps_free_fraction_to_zero() {
    let usage = DiskUsage {
        used_bytes: 1_500,
        total_bytes: 1_000,
    };
    assert!((usage.fraction() - 1.0).abs() < f64::EPSILON);
    assert!(usage.free_fraction().abs() < f64::EPSILON);
}

#[cfg(unix)]
#[test]
fn statvfs_reads_a_real_filesystem() {
    let status = statvfs_status("/tmp").expect("statvfs(/tmp) should succeed");
    assert!(status.total_bytes > 0);
    assert!(status.free_bytes <= status.total_bytes);
    assert!(status.available_bytes <= status.total_bytes);
}
