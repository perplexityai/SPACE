use super::*;

#[test]
fn policy_from_proto_rejects_unspecified_wake_source() {
    let policy = proto::AutoLifecyclePolicy {
        wake: Some(proto::AutoLifecycleWakePolicy {
            on: vec![proto::AutoLifecycleWakeSource::Unspecified as i32],
        }),
        condition_sets: Vec::new(),
        stages: Vec::new(),
    };

    let error = AutoLifecyclePolicy::try_from(&policy).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("wake source must not be unspecified")
    );
}

#[test]
fn policy_from_proto_rejects_unknown_wake_source() {
    let policy = proto::AutoLifecyclePolicy {
        wake: Some(proto::AutoLifecycleWakePolicy { on: vec![999] }),
        condition_sets: Vec::new(),
        stages: Vec::new(),
    };

    let error = AutoLifecyclePolicy::try_from(&policy).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("unknown auto lifecycle wake source value 999")
    );
}

#[test]
fn policy_from_proto_rejects_unknown_activity_source() {
    let policy = proto::AutoLifecyclePolicy {
        wake: None,
        condition_sets: vec![proto::AutoLifecycleConditionSet {
            id: proto::AutoLifecycleConditionSetId::GuestActivity as i32,
            kind: proto::AutoLifecycleConditionKind::GuestActivity as i32,
            activity_source: Some(999),
            max_guest_idle_extension: None,
        }],
        stages: Vec::new(),
    };

    let error = AutoLifecyclePolicy::try_from(&policy).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("unknown auto lifecycle activity source value 999")
    );
}

#[test]
fn status_proto_round_trip_preserves_activity_clocks() {
    let status = AutoLifecycleStatus {
        last_access_at: Some(100),
        last_command_at: Some(200),
        ..AutoLifecycleStatus::default()
    };

    let proto = proto::AutoLifecycleStatus::from(&status);
    assert_eq!(
        proto
            .last_access_at
            .as_ref()
            .map(|timestamp| timestamp.seconds),
        Some(100)
    );
    assert_eq!(
        proto
            .last_command_at
            .as_ref()
            .map(|timestamp| timestamp.seconds),
        Some(200)
    );

    let decoded = AutoLifecycleStatus::try_from(proto).unwrap();
    assert_eq!(decoded.last_access_at, Some(100));
    assert_eq!(decoded.last_command_at, Some(200));
}
