use super::{
    AutoLifecycleActivitySource, AutoLifecycleConditionKind, AutoLifecycleConditionSet,
    AutoLifecycleConditionSetId, AutoLifecycleIdleAction, AutoLifecyclePolicy, AutoLifecycleStage,
    AutoLifecycleStageChain, AutoLifecycleWakePolicy, AutoLifecycleWakeSource,
};
use std::collections::BTreeMap;

#[cfg(feature = "serde")]
use pplx_space_durations::parse_simple_duration_secs;

#[test]
fn disabled_policy_is_empty() {
    let policy = AutoLifecyclePolicy::disabled();
    assert!(policy.is_disabled());
}

#[test]
fn normalization_preserves_explicitly_disabled_wake_sources() {
    let policy = AutoLifecyclePolicy {
        condition_sets: BTreeMap::from([(
            AutoLifecycleConditionSetId::AccessIdle,
            AutoLifecycleConditionSet {
                kind: AutoLifecycleConditionKind::Inactivity,
                activity_source: Some(AutoLifecycleActivitySource::Access),
                max_guest_idle_extension: None,
            },
        )]),
        stages: vec![AutoLifecycleStage {
            condition_set: AutoLifecycleConditionSetId::AccessIdle,
            at: 300,
            action: AutoLifecycleIdleAction::Pause,
        }],
        ..Default::default()
    }
    .normalized();

    assert!(policy.wake.on.is_empty());
}

#[cfg(feature = "serde")]
#[test]
fn duration_parser_supports_suffixes() {
    assert_eq!(parse_simple_duration_secs("30s"), Ok(30));
    assert_eq!(parse_simple_duration_secs("5m"), Ok(300));
    assert_eq!(parse_simple_duration_secs("2h"), Ok(7200));
    assert_eq!(parse_simple_duration_secs("1d"), Ok(86_400));
    assert_eq!(parse_simple_duration_secs("1.5m"), Ok(90));
}

#[test]
fn stage_chain_parses_and_canonicalizes_duplicate_times() {
    let chain = "pause@5m,delete@5m,pause@10m"
        .parse::<AutoLifecycleStageChain>()
        .unwrap();

    assert_eq!(chain.to_string(), "delete@5m");
}

#[test]
fn stage_chain_drops_redundant_repeated_severity() {
    let chain = "pause@5m,pause@10m,suspend@15m,stop@20m,stop@25m"
        .parse::<AutoLifecycleStageChain>()
        .unwrap();

    assert_eq!(chain.to_string(), "pause@5m,suspend@15m,stop@20m");
}

#[cfg(feature = "serde")]
#[test]
fn stage_chain_round_trips_through_serde_string() {
    let chain = "pause@5m,suspend@15m,delete@1d"
        .parse::<AutoLifecycleStageChain>()
        .unwrap();
    let encoded = serde_json::to_string(&chain).unwrap();
    let decoded: AutoLifecycleStageChain = serde_json::from_str(&encoded).unwrap();

    assert_eq!(encoded, "\"pause@5m,suspend@15m,delete@1d\"");
    assert_eq!(decoded, chain);
}

#[test]
fn validation_rejects_non_monotonic_dormancy_severity() {
    let policy = AutoLifecyclePolicy {
        wake: AutoLifecycleWakePolicy {
            on: vec![AutoLifecycleWakeSource::Access],
        },
        condition_sets: BTreeMap::from([(
            AutoLifecycleConditionSetId::AccessIdle,
            AutoLifecycleConditionSet {
                kind: AutoLifecycleConditionKind::Inactivity,
                activity_source: Some(AutoLifecycleActivitySource::Access),
                max_guest_idle_extension: None,
            },
        )]),
        stages: vec![
            AutoLifecycleStage {
                condition_set: AutoLifecycleConditionSetId::AccessIdle,
                at: 300,
                action: AutoLifecycleIdleAction::Stop,
            },
            AutoLifecycleStage {
                condition_set: AutoLifecycleConditionSetId::AccessIdle,
                at: 600,
                action: AutoLifecycleIdleAction::Pause,
            },
        ],
    };

    assert!(
        policy
            .validate_basic()
            .unwrap_err()
            .contains("monotonic in dormancy severity")
    );
}

#[test]
fn validation_rejects_too_many_stages() {
    let mut stages = Vec::new();
    for index in 1..=101 {
        stages.push(AutoLifecycleStage {
            condition_set: AutoLifecycleConditionSetId::AccessIdle,
            at: index,
            action: AutoLifecycleIdleAction::Pause,
        });
    }

    let policy = AutoLifecyclePolicy {
        condition_sets: BTreeMap::from([(
            AutoLifecycleConditionSetId::AccessIdle,
            AutoLifecycleConditionSet {
                kind: AutoLifecycleConditionKind::Inactivity,
                activity_source: Some(AutoLifecycleActivitySource::Access),
                max_guest_idle_extension: None,
            },
        )]),
        stages,
        ..Default::default()
    };

    assert!(
        policy
            .validate_basic()
            .unwrap_err()
            .contains("at most 100 stages")
    );
}

#[test]
fn condition_set_ids_reject_unknown_values() {
    assert!(
        "custom_guest_activity"
            .parse::<AutoLifecycleConditionSetId>()
            .unwrap_err()
            .to_string()
            .contains("unsupported auto lifecycle condition set")
    );
}

#[test]
fn guest_activity_helpers_use_staged_guest_activity_conditions() {
    let policy = AutoLifecyclePolicy {
        condition_sets: BTreeMap::from([
            (
                AutoLifecycleConditionSetId::GuestActivity,
                AutoLifecycleConditionSet {
                    kind: AutoLifecycleConditionKind::GuestActivity,
                    activity_source: None,
                    max_guest_idle_extension: Some("30s".parse().unwrap()),
                },
            ),
            (
                AutoLifecycleConditionSetId::AccessIdle,
                AutoLifecycleConditionSet {
                    kind: AutoLifecycleConditionKind::Inactivity,
                    activity_source: Some(AutoLifecycleActivitySource::Access),
                    max_guest_idle_extension: Some("2m".parse().unwrap()),
                },
            ),
        ]),
        stages: vec![AutoLifecycleStage {
            condition_set: AutoLifecycleConditionSetId::GuestActivity,
            at: 30,
            action: AutoLifecycleIdleAction::Suspend,
        }],
        ..Default::default()
    };

    assert!(policy.has_guest_activity_stages());
    assert_eq!(
        policy.guest_activity_hold_cap(),
        Some("30s".parse().unwrap())
    );
}
