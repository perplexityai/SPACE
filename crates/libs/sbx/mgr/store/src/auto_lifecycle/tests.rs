use std::collections::BTreeMap;

use pplx_space_common_core::types::{
    AutoLifecycleConditionSet, AutoLifecycleConditionSetId, AutoLifecycleIdleAction,
    AutoLifecyclePolicy, AutoLifecycleRuntimeState, AutoLifecycleStage,
};

use super::{
    ACCESS_IDLE_CONDITION_SET, COMMAND_IDLE_CONDITION_SET, GUEST_ACTIVITY_CONDITION_SET,
    MAX_AUTO_SUSPEND_ERROR_STREAK, apply_observed_access_touch, arm_inactivity_conditions,
    clear_error, compute_next_auto_event, disarm_guest_activity_on_external_access, due_stage,
    extend_guest_activity_idle_epoch, initial_disarmed_runtime_state, initial_runtime_state,
    mark_error, mark_error_and_disarm_conditions, mark_suspend_error, notify_guest_activity_state,
    policy_from_proto, recompute_runtime_state, test_guest_activity_hold_cap, touch_access,
    touch_command_and_access, validate_v1_policy,
};
use pplx_space_sbx_mgr_client::proto;

fn sample_policy() -> AutoLifecyclePolicy {
    let mut policy = AutoLifecyclePolicy {
        wake: Default::default(),
        condition_sets: BTreeMap::from([(
            AutoLifecycleConditionSetId::AccessIdle,
            AutoLifecycleConditionSet::access_idle(),
        )]),
        stages: vec![
            AutoLifecycleStage {
                condition_set: AutoLifecycleConditionSetId::AccessIdle,
                at: 300,
                action: AutoLifecycleIdleAction::Pause,
            },
            AutoLifecycleStage {
                condition_set: AutoLifecycleConditionSetId::AccessIdle,
                at: 900,
                action: AutoLifecycleIdleAction::Suspend,
            },
        ],
    };
    policy.normalize_in_place();
    policy
}

#[test]
fn v1_validation_accepts_access_idle_policy() {
    validate_v1_policy(&sample_policy()).expect("sample policy must validate");
}

#[test]
fn policy_from_proto_returns_normalized_policy() {
    let policy = proto::AutoLifecyclePolicy {
        wake: Some(proto::AutoLifecycleWakePolicy {
            on: vec![
                proto::AutoLifecycleWakeSource::Command as i32,
                proto::AutoLifecycleWakeSource::Access as i32,
                proto::AutoLifecycleWakeSource::Command as i32,
            ],
        }),
        condition_sets: vec![proto::AutoLifecycleConditionSet {
            id: proto::AutoLifecycleConditionSetId::AccessIdle as i32,
            kind: proto::AutoLifecycleConditionKind::Inactivity as i32,
            activity_source: Some(proto::AutoLifecycleActivitySource::Access as i32),
            max_guest_idle_extension: None,
        }],
        stages: vec![
            proto::AutoLifecycleStage {
                condition_set: proto::AutoLifecycleConditionSetId::AccessIdle as i32,
                at: Some(prost_types::Duration {
                    seconds: 900,
                    nanos: 0,
                }),
                action: proto::AutoLifecycleIdleAction::Suspend as i32,
            },
            proto::AutoLifecycleStage {
                condition_set: proto::AutoLifecycleConditionSetId::AccessIdle as i32,
                at: Some(prost_types::Duration {
                    seconds: 300,
                    nanos: 0,
                }),
                action: proto::AutoLifecycleIdleAction::Pause as i32,
            },
        ],
    };

    let policy = policy_from_proto(&policy).unwrap();

    assert_eq!(
        policy.wake.on,
        vec![
            pplx_space_common_core::types::AutoLifecycleWakeSource::Access,
            pplx_space_common_core::types::AutoLifecycleWakeSource::Command,
        ]
    );
    assert_eq!(
        policy
            .stages
            .iter()
            .map(|stage| stage.at)
            .collect::<Vec<_>>(),
        vec![300, 900]
    );
}

#[test]
fn v1_validation_accepts_command_idle_policy() {
    let mut policy = AutoLifecyclePolicy {
        wake: Default::default(),
        condition_sets: BTreeMap::from([(
            AutoLifecycleConditionSetId::CommandIdle,
            AutoLifecycleConditionSet::command_idle(),
        )]),
        stages: vec![AutoLifecycleStage {
            condition_set: AutoLifecycleConditionSetId::CommandIdle,
            at: 300,
            action: AutoLifecycleIdleAction::Pause,
        }],
    };
    policy.normalize_in_place();
    validate_v1_policy(&policy).expect("policy must validate");
}

#[test]
fn v1_validation_accepts_guest_activity_policy() {
    let mut policy = AutoLifecyclePolicy {
        wake: Default::default(),
        condition_sets: BTreeMap::from([(
            AutoLifecycleConditionSetId::GuestActivity,
            AutoLifecycleConditionSet::guest_activity(Some(test_guest_activity_hold_cap())),
        )]),
        stages: vec![AutoLifecycleStage {
            condition_set: AutoLifecycleConditionSetId::GuestActivity,
            at: 300,
            action: AutoLifecycleIdleAction::Suspend,
        }],
    };
    policy.normalize_in_place();
    validate_v1_policy(&policy).expect("policy must validate");
}

#[test]
fn v1_validation_rejects_guest_activity_stages_without_hold_cap() {
    let mut policy = AutoLifecyclePolicy {
        wake: Default::default(),
        condition_sets: BTreeMap::from([(
            AutoLifecycleConditionSetId::GuestActivity,
            AutoLifecycleConditionSet::guest_activity(None),
        )]),
        stages: vec![AutoLifecycleStage {
            condition_set: AutoLifecycleConditionSetId::GuestActivity,
            at: 300,
            action: AutoLifecycleIdleAction::Suspend,
        }],
    };
    policy.normalize_in_place();

    let error = validate_v1_policy(&policy).expect_err("missing cap rejected");
    assert_eq!(
        error,
        "guest_activity with lifecycle stages must specify max_guest_idle_extension"
    );
}

#[test]
fn stage_progress_is_monotonic() {
    let policy = sample_policy();
    let mut state = initial_runtime_state(&policy, 100);
    assert_eq!(compute_next_auto_event(&policy, &state), Some(400));
    assert_eq!(
        due_stage(&policy, &state, 401),
        Some((
            0,
            AutoLifecycleConditionSetId::AccessIdle,
            AutoLifecycleIdleAction::Pause,
            400,
        ))
    );
    touch_access(&mut state, &policy, 500);
    assert_eq!(state.last_access_at, Some(500));
    assert_eq!(compute_next_auto_event(&policy, &state), Some(800));
}

#[test]
fn inactivity_starts_only_after_activity_capable_transition() {
    let policy = sample_policy();
    let mut state = initial_disarmed_runtime_state(&policy);

    assert_eq!(compute_next_auto_event(&policy, &state), None);
    assert!(due_stage(&policy, &state, 10_000).is_none());

    arm_inactivity_conditions(&mut state, &policy, 10_000);
    assert_eq!(compute_next_auto_event(&policy, &state), Some(10_300));
    assert!(due_stage(&policy, &state, 10_299).is_none());
    assert!(due_stage(&policy, &state, 10_300).is_some());

    arm_inactivity_conditions(&mut state, &policy, 20_000);
    assert_eq!(compute_next_auto_event(&policy, &state), Some(10_300));
}

#[test]
fn side_effecting_failure_is_suppressed_until_new_activity() {
    let policy = sample_policy();
    let mut state = initial_runtime_state(&policy, 100);

    mark_error_and_disarm_conditions(&mut state, &policy, "backup fence unavailable");

    assert!(due_stage(&policy, &state, 10_000).is_none());
    assert_eq!(state.next_auto_event_at, None);
    assert_eq!(
        state.last_auto_error.as_deref(),
        Some("backup fence unavailable")
    );

    touch_access(&mut state, &policy, 20_000);
    recompute_runtime_state(&mut state, &policy);
    assert_eq!(state.next_auto_event_at, Some(20_300));
    assert!(due_stage(&policy, &state, 20_301).is_some());
}

#[test]
fn observed_access_touch_uses_max_epoch_for_replay_safety() {
    let policy = sample_policy();
    let mut state = initial_runtime_state(&policy, 500);
    let condition = state
        .conditions
        .get_mut(&AutoLifecycleConditionSetId::AccessIdle)
        .expect("access condition");
    condition.last_fired_stage_index = Some(0);

    assert!(apply_observed_access_touch(&mut state, &policy, 400));
    let condition = state
        .conditions
        .get(&AutoLifecycleConditionSetId::AccessIdle)
        .expect("access condition");
    assert_eq!(condition.epoch_at, Some(500));
    assert_eq!(condition.last_fired_stage_index, Some(0));
    assert_eq!(state.last_access_at, Some(400));

    assert!(!apply_observed_access_touch(&mut state, &policy, 300));
    assert_eq!(state.last_access_at, Some(400));

    assert!(apply_observed_access_touch(&mut state, &policy, 600));
    let condition = state
        .conditions
        .get(&AutoLifecycleConditionSetId::AccessIdle)
        .expect("access condition");
    assert_eq!(condition.epoch_at, Some(600));
    assert_eq!(condition.last_touched_at, Some(600));
    assert_eq!(condition.last_fired_stage_index, None);
    assert_eq!(state.next_auto_event_at, Some(900));
}

#[test]
fn command_touch_rearms_access_and_command_conditions() {
    let mut policy = AutoLifecyclePolicy {
        wake: Default::default(),
        condition_sets: BTreeMap::from([
            (
                AutoLifecycleConditionSetId::AccessIdle,
                AutoLifecycleConditionSet::access_idle(),
            ),
            (
                AutoLifecycleConditionSetId::CommandIdle,
                AutoLifecycleConditionSet::command_idle(),
            ),
        ]),
        stages: vec![
            AutoLifecycleStage {
                condition_set: AutoLifecycleConditionSetId::AccessIdle,
                at: 300,
                action: AutoLifecycleIdleAction::Pause,
            },
            AutoLifecycleStage {
                condition_set: AutoLifecycleConditionSetId::CommandIdle,
                at: 120,
                action: AutoLifecycleIdleAction::Pause,
            },
        ],
    };
    policy.normalize_in_place();

    let mut state = initial_runtime_state(&policy, 100);
    touch_command_and_access(&mut state, &policy, 250);

    assert_eq!(
        state
            .conditions
            .get(&ACCESS_IDLE_CONDITION_SET)
            .and_then(|condition| condition.epoch_at),
        Some(250)
    );
    assert_eq!(
        state
            .conditions
            .get(&COMMAND_IDLE_CONDITION_SET)
            .and_then(|condition| condition.epoch_at),
        Some(250)
    );
    assert_eq!(state.last_access_at, Some(250));
    assert_eq!(state.last_command_at, Some(250));
}

#[test]
fn access_touch_records_activity_without_policy_conditions() {
    let policy = AutoLifecyclePolicy::disabled();
    let mut state = AutoLifecycleRuntimeState::default();

    touch_access(&mut state, &policy, 500);

    assert_eq!(state.last_access_at, Some(500));
    assert_eq!(state.last_command_at, None);
    assert!(state.conditions.is_empty());
    assert_eq!(state.next_auto_event_at, None);
}

#[test]
fn command_touch_records_activity_without_policy_conditions() {
    let policy = AutoLifecyclePolicy::disabled();
    let mut state = AutoLifecycleRuntimeState::default();

    touch_command_and_access(&mut state, &policy, 700);

    assert_eq!(state.last_access_at, Some(700));
    assert_eq!(state.last_command_at, Some(700));
    assert!(state.conditions.is_empty());
    assert_eq!(state.next_auto_event_at, None);
}

#[test]
fn due_stage_prefers_most_dormant_due_action_across_conditions() {
    let mut policy = AutoLifecyclePolicy {
        wake: Default::default(),
        condition_sets: BTreeMap::from([
            (
                AutoLifecycleConditionSetId::AccessIdle,
                AutoLifecycleConditionSet::access_idle(),
            ),
            (
                AutoLifecycleConditionSetId::CommandIdle,
                AutoLifecycleConditionSet::command_idle(),
            ),
        ]),
        stages: vec![
            AutoLifecycleStage {
                condition_set: AutoLifecycleConditionSetId::AccessIdle,
                at: 300,
                action: AutoLifecycleIdleAction::Pause,
            },
            AutoLifecycleStage {
                condition_set: AutoLifecycleConditionSetId::CommandIdle,
                at: 120,
                action: AutoLifecycleIdleAction::Stop,
            },
        ],
    };
    policy.normalize_in_place();

    let state = initial_runtime_state(&policy, 100);
    assert_eq!(
        due_stage(&policy, &state, 500),
        Some((
            1,
            AutoLifecycleConditionSetId::CommandIdle,
            AutoLifecycleIdleAction::Stop,
            220,
        ))
    );
}

#[test]
fn guest_activity_arms_and_disarms_guest_activity_condition() {
    let mut policy = AutoLifecyclePolicy {
        wake: Default::default(),
        condition_sets: BTreeMap::from([(
            AutoLifecycleConditionSetId::GuestActivity,
            AutoLifecycleConditionSet::guest_activity(Some(test_guest_activity_hold_cap())),
        )]),
        stages: vec![AutoLifecycleStage {
            condition_set: AutoLifecycleConditionSetId::GuestActivity,
            at: 300,
            action: AutoLifecycleIdleAction::Suspend,
        }],
    };
    policy.normalize_in_place();

    let mut state = initial_runtime_state(&policy, 100);
    assert_eq!(
        state
            .conditions
            .get(&GUEST_ACTIVITY_CONDITION_SET)
            .map(|condition| (condition.armed, condition.epoch_at)),
        Some((false, None))
    );

    assert!(notify_guest_activity_state(&mut state, &policy, true, 250,));
    assert_eq!(
        state
            .conditions
            .get(&GUEST_ACTIVITY_CONDITION_SET)
            .map(|condition| (condition.armed, condition.epoch_at)),
        Some((true, Some(250)))
    );

    assert!(notify_guest_activity_state(&mut state, &policy, false, 400,));
    assert_eq!(
        state
            .conditions
            .get(&GUEST_ACTIVITY_CONDITION_SET)
            .map(|condition| (condition.armed, condition.epoch_at)),
        Some((false, None))
    );
}

#[test]
fn external_access_disarms_guest_activity_epoch() {
    let mut policy = AutoLifecyclePolicy {
        wake: Default::default(),
        condition_sets: BTreeMap::from([(
            AutoLifecycleConditionSetId::GuestActivity,
            AutoLifecycleConditionSet::guest_activity(Some(test_guest_activity_hold_cap())),
        )]),
        stages: vec![AutoLifecycleStage {
            condition_set: AutoLifecycleConditionSetId::GuestActivity,
            at: 300,
            action: AutoLifecycleIdleAction::Suspend,
        }],
    };
    policy.normalize_in_place();

    let mut state = initial_runtime_state(&policy, 100);
    assert!(notify_guest_activity_state(&mut state, &policy, true, 250));
    disarm_guest_activity_on_external_access(&mut state, &policy, 400);

    let condition = state
        .conditions
        .get(&GUEST_ACTIVITY_CONDITION_SET)
        .expect("guest condition");
    assert!(!condition.armed);
    assert_eq!(condition.epoch_at, None);
    assert_eq!(due_stage(&policy, &state, 600), None);
}

#[test]
fn guest_active_for_extends_idle_epoch() {
    let mut policy = AutoLifecyclePolicy {
        wake: Default::default(),
        condition_sets: BTreeMap::from([(
            AutoLifecycleConditionSetId::GuestActivity,
            AutoLifecycleConditionSet::guest_activity(Some(test_guest_activity_hold_cap())),
        )]),
        stages: vec![AutoLifecycleStage {
            condition_set: AutoLifecycleConditionSetId::GuestActivity,
            at: 300,
            action: AutoLifecycleIdleAction::Suspend,
        }],
    };
    policy.normalize_in_place();

    let mut state = initial_runtime_state(&policy, 100);
    assert!(extend_guest_activity_idle_epoch(
        &mut state, &policy, 250, 120
    ));
    recompute_runtime_state(&mut state, &policy);

    let condition = state
        .conditions
        .get(&GUEST_ACTIVITY_CONDITION_SET)
        .expect("guest condition");
    assert!(condition.armed);
    assert_eq!(condition.epoch_at, Some(370));
    assert_eq!(state.open_activity_leases, 0);
    assert_eq!(state.next_auto_event_at, Some(670));
}

#[tokio::test]
async fn staging_lock_does_not_block_when_wake_lock_is_held_same_task() {
    use super::AutoLifecycleCoordinator;

    let coord = AutoLifecycleCoordinator::default();
    let sandbox_id = "019e31ec-5633-7111-b27c-1b6ac5cb170a";

    let wake = coord.wake_lock(sandbox_id);
    let _guard = wake.lock().await;

    // A same-task acquisition while wake_lock is held would hang if
    // the two locks shared a single map.
    let staging = coord.staging_lock(sandbox_id);
    let acquired = tokio::time::timeout(std::time::Duration::from_secs(1), staging.lock())
        .await
        .expect("staging_lock and wake_lock must be independent");
    drop(acquired);
}

#[tokio::test]
async fn staging_lock_returns_same_mutex_for_same_sandbox() {
    use super::AutoLifecycleCoordinator;

    let coord = AutoLifecycleCoordinator::default();
    let sandbox_id = "019e31ec-5633-7111-b27c-1b6ac5cb170a";

    let lock_a = coord.staging_lock(sandbox_id);
    let lock_b = coord.staging_lock(sandbox_id);
    assert!(std::sync::Arc::ptr_eq(&lock_a, &lock_b));
}

#[tokio::test]
async fn staging_lock_isolates_distinct_sandboxes() {
    use super::AutoLifecycleCoordinator;

    let coord = AutoLifecycleCoordinator::default();

    let lock_a = coord.staging_lock("019e31ec-5633-7111-b27c-1b6ac5cb170a");
    let lock_b = coord.staging_lock("019e31ed-7a9a-7b3c-91d2-9b3a4c5d6e7f");
    assert!(!std::sync::Arc::ptr_eq(&lock_a, &lock_b));

    let _guard = lock_a.lock().await;
    let _guard = tokio::time::timeout(std::time::Duration::from_secs(1), lock_b.lock())
        .await
        .expect("staging_lock for a different sandbox must not block");
}

#[tokio::test]
async fn staging_lock_serializes_concurrent_acquirers() {
    use super::AutoLifecycleCoordinator;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let coord = AutoLifecycleCoordinator::default();
    let sandbox_id = "019e31ec-5633-7111-b27c-1b6ac5cb170a";

    let in_section = Arc::new(AtomicUsize::new(0));
    let max_observed = Arc::new(AtomicUsize::new(0));

    let mut tasks = Vec::new();
    for _ in 0..8 {
        let coord = coord.clone();
        let in_section = in_section.clone();
        let max_observed = max_observed.clone();
        tasks.push(tokio::spawn(async move {
            let lock = coord.staging_lock(sandbox_id);
            let _guard = lock.lock().await;
            let now = in_section.fetch_add(1, Ordering::SeqCst) + 1;
            max_observed.fetch_max(now, Ordering::SeqCst);
            // Hold long enough that an unsynchronized peer would overlap.
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            in_section.fetch_sub(1, Ordering::SeqCst);
        }));
    }
    for t in tasks {
        t.await.expect("staging task panicked");
    }
    assert_eq!(max_observed.load(Ordering::SeqCst), 1);
}

#[test]
fn last_wake_completed_records_per_sandbox_and_clears_on_remove() {
    use super::AutoLifecycleCoordinator;

    let coord = AutoLifecycleCoordinator::default();
    let sandbox_id = "019e31ec-5633-7111-b27c-1b6ac5cb170a";
    let other_sandbox_id = "019e31ec-5633-7111-b27c-1b6ac5cb170b";

    assert!(
        coord.since_last_wake_completed(sandbox_id).is_none(),
        "no wake recorded yet"
    );

    coord.record_wake_completed(sandbox_id);
    assert!(
        coord.since_last_wake_completed(sandbox_id).is_some(),
        "recorded wake must be observable"
    );
    assert!(
        coord.since_last_wake_completed(other_sandbox_id).is_none(),
        "wake record must be keyed per sandbox"
    );

    coord.remove_sandbox(sandbox_id);
    assert!(
        coord.since_last_wake_completed(sandbox_id).is_none(),
        "remove_sandbox must drop the wake-completion record"
    );
}

#[tokio::test]
async fn retired_sandbox_keeps_existing_lock_owners_and_waiters_serialized() {
    use super::AutoLifecycleCoordinator;

    let coord = AutoLifecycleCoordinator::default();
    let sandbox_id = "019e31ec-5633-7111-b27c-1b6ac5cb170a";

    for family in 0..3 {
        let acquire = || match family {
            0 => coord.wake_lock(sandbox_id),
            1 => coord.artifact_lock(sandbox_id),
            _ => coord.staging_lock(sandbox_id),
        };
        let before = acquire();
        let weak = std::sync::Arc::downgrade(&before);
        let owner = before.clone().lock_owned().await;
        let mut waiter = Box::pin(before.clone().lock_owned());
        assert!(futures_util::poll!(&mut waiter).is_pending());
        coord.remove_sandbox(sandbox_id);
        let after = acquire();
        assert!(std::sync::Arc::ptr_eq(&before, &after));
        after.try_lock().unwrap_err();
        drop(owner);
        let waiter_guard = waiter.await;
        after.try_lock().unwrap_err();
        drop((before, after));
        assert!(
            weak.upgrade().is_some(),
            "the guard must retain lock identity"
        );
        drop(waiter_guard);
        assert!(
            weak.upgrade().is_none(),
            "idle retired locks must be reclaimed"
        );
    }
}

#[test]
fn deadline_range_is_validated_and_computation_fits_sqlite() {
    for (delay, accepted) in [
        (i64::MAX as u64, true),
        (i64::MAX as u64 + 1, false),
        (u64::MAX, false),
    ] {
        let mut policy = sample_policy();
        policy.stages.truncate(1);
        policy.stages[0].at = delay;
        assert_eq!(validate_v1_policy(&policy).is_ok(), accepted);
        let runtime = initial_runtime_state(&policy, 100);
        assert!(runtime.next_auto_event_at.unwrap() <= i64::MAX as u64);
    }
}

/// Fix for the encrypted-suspend capture loop: failed auto-suspends back off
/// exponentially and park (disarm) after the budget instead of retrying every
/// reconcile tick forever; clearing the error (activity) resets the budget.
#[test]
fn suspend_failures_back_off_and_park_after_the_budget() {
    let policy = sample_policy();
    let mut state = initial_runtime_state(&policy, 1_000);
    for attempt in 1..MAX_AUTO_SUSPEND_ERROR_STREAK {
        assert!(!mark_suspend_error(
            &mut state,
            &policy,
            2_000,
            "capture failed"
        ));
        assert_eq!(state.auto_error_streak, attempt);
        // Backoff doubles (10, 20, 40, ...) and caps at 600s.
        let expected = (10u64 << (attempt - 1).min(6)).min(600);
        assert!(
            state.next_auto_event_at >= Some(2_000 + expected),
            "attempt {attempt}"
        );
        assert!(state.conditions.values().any(|c| c.armed));
    }
    // Budget spent parks the sandbox, but ARMED at a ~6h next-attempt so a
    // fixed cause self-heals on the slow retry without tenant activity —
    // signalled once, on entry.
    assert!(mark_suspend_error(
        &mut state,
        &policy,
        2_000,
        "capture failed"
    ));
    assert!(
        state.conditions.values().any(|c| c.armed),
        "parking must not disarm — a fixed fleet re-attempts on its own"
    );
    assert_eq!(
        state.next_auto_event_at,
        Some(2_000 + 6 * 3_600),
        "next attempt is pushed to the decay horizon, not the 5s loop"
    );
    // The ~6h retry (streak already past the budget) re-parks without
    // re-signalling, and stays armed.
    assert!(!mark_suspend_error(
        &mut state,
        &policy,
        2_000 + 6 * 3_600,
        "capture failed"
    ));
    assert!(state.conditions.values().any(|c| c.armed));
    assert_eq!(state.next_auto_event_at, Some(2_000 + 2 * 6 * 3_600));

    // Fresh activity clears the error; the next failure starts a new budget.
    clear_error(&mut state, &policy);
    assert!(!mark_suspend_error(
        &mut state,
        &policy,
        3_000,
        "capture failed"
    ));
    assert_eq!(state.auto_error_streak, 1);

    // A failure landing hours past the scheduled retry is a new episode, not
    // the same loop: the stale streak resets instead of accumulating.
    assert!(!mark_suspend_error(
        &mut state,
        &policy,
        3_100,
        "capture failed"
    ));
    assert_eq!(state.auto_error_streak, 2);
    assert!(!mark_suspend_error(
        &mut state,
        &policy,
        3_100 + 7 * 3_600,
        "capture failed"
    ));
    assert_eq!(state.auto_error_streak, 1);

    // A non-suspend failure interposed after activity must not resurrect a
    // pre-activity streak: activity itself resets it, at the source.
    state.auto_error_streak = MAX_AUTO_SUSPEND_ERROR_STREAK - 1;
    touch_access(&mut state, &policy, 30_000);
    assert_eq!(state.auto_error_streak, 0, "activity resets the budget");
    mark_error(&mut state, &policy, "pause failed");
    assert!(!mark_suspend_error(
        &mut state,
        &policy,
        30_010,
        "capture failed"
    ));
    assert_eq!(state.auto_error_streak, 1);
}

#[test]
fn connect_floor_survives_activity_without_changing_policy_or_other_clocks() {
    let mut policy = sample_policy();
    policy.stages.truncate(1);
    let mut state = initial_runtime_state(&policy, 100);
    assert!(!super::extend_access_deadline(&mut state, &policy, 400).unwrap());
    assert_eq!(state.access_extended_until, None);
    assert!(super::extend_access_deadline(&mut state, &policy, 1000).unwrap());
    for observed in [101, 300, 200] {
        apply_observed_access_touch(&mut state, &policy, observed);
        assert_eq!(compute_next_auto_event(&policy, &state), Some(1000));
        assert!(due_stage(&policy, &state, 999).is_none());
    }
    assert_eq!(policy.stages[0].at, 300);
    assert_eq!(
        state.conditions[&ACCESS_IDLE_CONDITION_SET].epoch_at,
        Some(300)
    );
    assert!(!super::extend_access_deadline(&mut state, &policy, 900).unwrap());
    let persisted = serde_json::to_string(&state).unwrap();
    let recovered = serde_json::from_str(&persisted).unwrap();
    assert_eq!(due_stage(&policy, &recovered, 1000).unwrap().3, 1000);

    let mut reset = initial_runtime_state(&policy, 400);
    assert_eq!(compute_next_auto_event(&policy, &reset), Some(700));
    // An unresolved connect can linearize after reset using its fixed minimum.
    super::extend_access_deadline(&mut reset, &policy, 1000).unwrap();
    assert_eq!(compute_next_auto_event(&policy, &reset), Some(1000));
}
