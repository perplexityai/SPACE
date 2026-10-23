use pplx_space_common_core::types::{
    AutoLifecycleConditionSet, AutoLifecycleConditionSetId, AutoLifecycleIdleAction,
    AutoLifecyclePolicy, AutoLifecycleStage, AutoLifecycleWakePolicy, AutoLifecycleWakeSource,
    SimpleDuration,
};

use super::*;

#[test]
fn fingerprint_normalizes_equivalent_policy_ordering() {
    let first = AutoLifecyclePolicy {
        wake: AutoLifecycleWakePolicy {
            on: vec![
                AutoLifecycleWakeSource::Command,
                AutoLifecycleWakeSource::Access,
            ],
        },
        ..AutoLifecyclePolicy::disabled()
    };
    let second = AutoLifecyclePolicy {
        wake: AutoLifecycleWakePolicy {
            on: vec![
                AutoLifecycleWakeSource::Access,
                AutoLifecycleWakeSource::Command,
            ],
        },
        ..AutoLifecyclePolicy::disabled()
    };

    let fingerprint = AutoLifecyclePolicyFingerprint::for_policy(&first);
    assert_eq!(
        fingerprint,
        AutoLifecyclePolicyFingerprint::for_policy(&second)
    );
    assert_eq!(
        fingerprint
            .to_string()
            .parse::<AutoLifecyclePolicyFingerprint>()
            .unwrap(),
        fingerprint
    );
    assert!(fingerprint.to_string().starts_with("v1:sha256:"));
    fingerprint
        .0
        .to_string()
        .parse::<AutoLifecyclePolicyFingerprint>()
        .unwrap_err();
}

/// Golden vector for the `v1` fingerprint.
///
/// Every other fingerprint test asserts relative equality, which stays green if
/// the projection or [`AutoLifecyclePolicy::normalize_in_place`] changes shape
/// on both sides at once. A published receipt carries the literal digest, so a
/// receiver that hashes the same policy differently silently stops matching
/// in-flight receipts. Pinning the bytes makes any such change fail here, which
/// is the signal to bump `AUTO_LIFECYCLE_POLICY_FINGERPRINT_VERSION` instead of
/// re-pinning this literal.
///
/// The literal must hold under **every** feature set, not just the one CI
/// happens to test with. The first version of this vector held only under
/// `--all-features`: `otel` transitively enabled serde_json's
/// `preserve_order`, which reorders JSON map keys, and the projection then
/// contained a map. The publisher and receiver of a receipt are different
/// binaries with different feature graphs, so a feature-dependent digest is a
/// receipt that never matches. If this test disagrees across feature sets, fix
/// the projection's encoding — don't re-pin per configuration.
#[test]
fn fingerprint_v1_golden_vector_pins_projection_and_normalization() {
    let policy = AutoLifecyclePolicy {
        // Unsorted and duplicated on purpose: normalization sorts and dedups
        // wake sources, so the pinned digest covers that too.
        wake: AutoLifecycleWakePolicy {
            on: vec![
                AutoLifecycleWakeSource::Command,
                AutoLifecycleWakeSource::Access,
                AutoLifecycleWakeSource::Command,
            ],
        },
        condition_sets: [
            (
                AutoLifecycleConditionSetId::AccessIdle,
                AutoLifecycleConditionSet::access_idle(),
            ),
            (
                AutoLifecycleConditionSetId::GuestActivity,
                AutoLifecycleConditionSet::guest_activity(Some(
                    SimpleDuration::new(std::time::Duration::from_secs(300)).unwrap(),
                )),
            ),
        ]
        .into(),
        // Also unsorted on purpose.
        stages: vec![
            AutoLifecycleStage {
                condition_set: AutoLifecycleConditionSetId::GuestActivity,
                at: 1800,
                action: AutoLifecycleIdleAction::Delete,
            },
            AutoLifecycleStage {
                condition_set: AutoLifecycleConditionSetId::AccessIdle,
                at: 900,
                action: AutoLifecycleIdleAction::Suspend,
            },
        ],
    };

    assert_eq!(
        AutoLifecyclePolicyFingerprint::for_policy(&policy).to_string(),
        "v1:sha256:c14d7330076cee1c038ced42732a04499d0854fcc0714cdbd28c771277df63c3",
    );
}

#[test]
fn fingerprint_changes_with_stage_timing_and_action() {
    let policy = AutoLifecyclePolicy {
        wake: AutoLifecycleWakePolicy::default(),
        condition_sets: [(
            AutoLifecycleConditionSetId::AccessIdle,
            AutoLifecycleConditionSet::access_idle(),
        )]
        .into(),
        stages: vec![AutoLifecycleStage {
            condition_set: AutoLifecycleConditionSetId::AccessIdle,
            at: 900,
            action: AutoLifecycleIdleAction::Delete,
        }],
    };
    let baseline = AutoLifecyclePolicyFingerprint::for_policy(&policy);
    let mut changed_timing = policy.clone();
    changed_timing.stages.first_mut().unwrap().at = 901;
    let mut changed_action = policy.clone();
    changed_action.stages.first_mut().unwrap().action = AutoLifecycleIdleAction::Stop;

    assert_ne!(
        baseline,
        AutoLifecyclePolicyFingerprint::for_policy(&changed_timing)
    );
    assert_ne!(
        baseline,
        AutoLifecyclePolicyFingerprint::for_policy(&changed_action)
    );
}

#[test]
fn fingerprint_accepts_duration_larger_than_json_u64_nanoseconds() {
    let duration =
        SimpleDuration::new(std::time::Duration::from_secs(u64::MAX / 1_000_000_000 + 1)).unwrap();
    let policy = AutoLifecyclePolicy {
        condition_sets: [(
            AutoLifecycleConditionSetId::GuestActivity,
            AutoLifecycleConditionSet::guest_activity(Some(duration)),
        )]
        .into(),
        ..AutoLifecyclePolicy::disabled()
    };

    let _ = AutoLifecyclePolicyFingerprint::for_policy(&policy);
}
