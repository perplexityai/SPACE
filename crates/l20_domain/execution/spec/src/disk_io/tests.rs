use super::*;

#[test]
fn constructor_accepts_supported_positive_limits() {
    let limits = DiskIoLimits::try_new(5_000, 200).unwrap();

    assert_eq!(limits.max_operations_per_sec(), 5_000);
    assert_eq!(limits.max_bandwidth_mib_per_sec(), 200);
}

#[test]
fn constructor_rejects_zero_and_unrepresentable_limits() {
    assert!(matches!(
        DiskIoLimits::try_new(0, 200),
        Err(DiskIoLimitsError::Zero {
            field: "max_operations_per_sec"
        })
    ));
    assert!(matches!(
        DiskIoLimits::try_new(5_000, 0),
        Err(DiskIoLimitsError::Zero {
            field: "max_bandwidth_mib_per_sec"
        })
    ));
    DiskIoLimits::try_new(MAX_OPERATIONS_PER_SEC, 200).unwrap();
    assert!(matches!(
        DiskIoLimits::try_new(MAX_OPERATIONS_PER_SEC + 1, 200),
        Err(DiskIoLimitsError::OutOfRange {
            field: "max_operations_per_sec",
            ..
        })
    ));
    DiskIoLimits::try_new(5_000, MAX_BANDWIDTH_MIB_PER_SEC).unwrap();
    assert!(matches!(
        DiskIoLimits::try_new(5_000, MAX_BANDWIDTH_MIB_PER_SEC + 1),
        Err(DiskIoLimitsError::OutOfRange {
            field: "max_bandwidth_mib_per_sec",
            ..
        })
    ));
}

#[test]
fn optional_wire_fields_are_paired() {
    assert_eq!(
        DiskIoLimits::try_from_optional(None, None).unwrap(),
        DiskIoLimits::default()
    );
    assert_eq!(
        DiskIoLimits::try_from_optional(Some(5_000), Some(200)).unwrap(),
        DiskIoLimits::try_new(5_000, 200).unwrap()
    );
    assert!(matches!(
        DiskIoLimits::try_from_optional(Some(5_000), None),
        Err(DiskIoLimitsError::PartialPair)
    ));
    assert!(matches!(
        DiskIoLimits::try_from_optional(None, Some(200)),
        Err(DiskIoLimitsError::PartialPair)
    ));
}

#[test]
fn a_drive_limiter_keeps_the_sustained_rates_and_holds_a_burst_of_each() {
    let burst = std::num::NonZeroU32::new(5).unwrap();
    let limiter = RateLimiter::for_disk(DiskIoLimits::try_new(1_000, 100).unwrap(), burst);
    assert_eq!(
        limiter.bandwidth,
        TokenBucket {
            size: 5 * 100 * 1024 * 1024,
            refill_time_ms: 5_000,
        },
        "the bucket holds five seconds and refills at the configured rate"
    );
    assert_eq!(
        limiter.operations,
        TokenBucket {
            size: 5_000,
            refill_time_ms: 5_000,
        },
        "the bucket holds five seconds and refills at the configured rate"
    );

    let largest = DiskIoLimits::try_new(MAX_OPERATIONS_PER_SEC, MAX_BANDWIDTH_MIB_PER_SEC).unwrap();
    let limiter = RateLimiter::for_disk(largest, std::num::NonZeroU32::MAX);
    assert_eq!(limiter.operations.size, u64::MAX);
    assert_eq!(limiter.bandwidth.size, u64::MAX);
}
