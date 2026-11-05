use super::*;

#[cfg(target_os = "linux")]
#[test]
fn setup_cache_guard_rejects_host_local_and_wildcard_cids() {
    for cid in [0, 1, tokio_vsock::VMADDR_CID_HOST, u32::MAX] {
        assert!(matches!(
            require_guest_cid(cid),
            Err(MemoryReportingError::NotGuest)
        ));
    }
    for cid in [3, 32, 65535] {
        require_guest_cid(cid).unwrap();
    }
}

#[test]
fn reporting_policy_validates_and_applies_a_runtime_setting() {
    for value in ["-1", "10", "256", "", "auto", "0\n"] {
        assert!(value.parse::<ReportingOrder>().is_err(), "{value:?}");
    }
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("order");
    for order in 0..=9 {
        let value: ReportingOrder = order.to_string().parse().unwrap();
        apply_order(&path, value).unwrap();
        assert_eq!(
            std::fs::read_to_string(&path).unwrap().trim(),
            order.to_string()
        );
    }
    assert!(apply_order(&directory.path().join("missing/order"), ReportingOrder(0)).is_err());
}
