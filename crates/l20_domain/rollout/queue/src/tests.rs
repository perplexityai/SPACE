use std::time::Duration;

use crate::{MAX_LEASE, clamp_lease};

#[test]
fn a_lease_is_clamped_to_one_second_through_an_hour() {
    assert_eq!(MAX_LEASE, Duration::from_secs(3600));
    assert_eq!(clamp_lease(Duration::ZERO), Duration::from_secs(1));
    assert_eq!(
        clamp_lease(Duration::from_millis(999)),
        Duration::from_secs(1)
    );
    assert_eq!(
        clamp_lease(Duration::from_secs(90)),
        Duration::from_secs(90)
    );
    assert_eq!(clamp_lease(MAX_LEASE + Duration::from_nanos(1)), MAX_LEASE);
}
