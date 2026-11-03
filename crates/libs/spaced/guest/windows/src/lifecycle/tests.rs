use super::*;

#[test]
fn host_timestamp_conversion_preserves_utc_and_rejects_out_of_range_values() {
    for (seconds, nanos, expected) in [
        (0, 123_456_789, Some((1970, 1, 1, 123))),
        (-11_644_473_600, 0, Some((1601, 1, 1, 0))),
        (253_402_300_799, 999_999_999, Some((9999, 12, 31, 999))),
        (-11_644_473_601, 0, None),
        (253_402_300_800, 0, None),
        (0, -1, None),
        (0, 1_000_000_000, None),
    ] {
        let result = system_time(prost_types::Timestamp { seconds, nanos });
        match expected {
            Some(expected) => {
                let time = result.unwrap();
                assert_eq!(
                    (time.wYear, time.wMonth, time.wDay, time.wMilliseconds),
                    expected
                );
            }
            None => assert!(matches!(result, Err(LifecycleError::Timestamp))),
        }
    }
}
