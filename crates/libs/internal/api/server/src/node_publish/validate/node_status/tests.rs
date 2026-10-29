use super::*;
use tonic::Code;

#[test]
fn node_status_overflows_keep_field_specific_errors() {
    type OverflowCase = (&'static str, fn(&mut PublishedNodeStatus));
    let cases: [OverflowCase; 7] = [
        ("network_slots_ready", |status| {
            status.network_slots_ready = u32::MAX
        }),
        ("network_pool_min_ready", |status| {
            status.network_pool_min_ready = u32::MAX
        }),
        ("network_slots_degraded", |status| {
            status.network_slots_degraded = u32::MAX
        }),
        ("network_slots_quarantined", |status| {
            status.network_slots_quarantined = u32::MAX
        }),
        ("network_slots_total", |status| {
            status.network_slots_total = u32::MAX
        }),
        ("network_slots_leased", |status| {
            status.network_slots_leased = u32::MAX
        }),
        ("network_pool_max_slots", |status| {
            status.network_pool_max_slots = u32::MAX
        }),
    ];
    for (field, invalidate) in cases {
        let mut status = PublishedNodeStatus::default();
        invalidate(&mut status);
        let error = ValidatedNodeStatus::try_from_proto(status).unwrap_err();
        assert_eq!(error.code(), Code::InvalidArgument);
        assert_eq!(
            error.message(),
            format!("{field} exceeds i32::MAX: number too large to fit in target type")
        );
    }
}

#[test]
fn scheduling_status_keeps_known_labels_and_forward_compatible_fallback() {
    let cases = [
        (NodeSchedulingStatus::Unspecified as i32, "unknown"),
        (NodeSchedulingStatus::Ready as i32, "ready"),
        (NodeSchedulingStatus::Warming as i32, "warming"),
        (NodeSchedulingStatus::Draining as i32, "draining"),
        (NodeSchedulingStatus::Degraded as i32, "degraded"),
        (NodeSchedulingStatus::Empty as i32, "empty"),
        (NodeSchedulingStatus::Unavailable as i32, "unavailable"),
        (NodeSchedulingStatus::Full as i32, "full"),
        (i32::MAX, "unknown"),
    ];
    for (scheduling_status, label) in cases {
        let status = ValidatedNodeStatus::try_from_proto(PublishedNodeStatus {
            scheduling_status,
            scheduling_reason: "synthetic observation".to_string(),
            ..PublishedNodeStatus::default()
        })
        .unwrap();
        assert_eq!(status.scheduling_status_label(), label);
        assert_eq!(status.scheduling_reason, "synthetic observation");
        assert!(status.node_ctl.is_none());
    }
}
