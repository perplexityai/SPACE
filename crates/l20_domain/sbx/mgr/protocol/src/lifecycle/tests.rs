use super::*;

#[test]
fn lifecycle_observation_requires_a_known_present_enum() {
    let mut known = 0;
    for raw in 1..=18 {
        let Ok(state) = SandboxLifecycleState::try_from(raw) else {
            // The retired cleanup lifecycles are unknown values now.
            assert!(matches!(raw, 15 | 16), "{raw}");
            assert_eq!(
                decode_lifecycle(Some(raw)),
                Err(LifecycleObservationError::UnknownValue(raw))
            );
            continue;
        };
        known += 1;
        assert_eq!(decode_lifecycle(Some(raw)).unwrap(), state);
        assert_eq!(
            SandboxLifecycleState::from_label(state.label()).unwrap(),
            state
        );
        let info = SandboxInfo {
            lifecycle: Some(raw),
            ..Default::default()
        };
        let list = SandboxListElement {
            lifecycle: Some(raw),
            ..Default::default()
        };
        let wait = WaitResponse {
            sandbox_id: String::new(),
            lifecycle: Some(raw),
            cleanup: None,
        };
        let unowned = ObservedStatus::Unowned(state);
        assert_eq!(info.observed_status().unwrap(), unowned);
        assert_eq!(list.observed_status().unwrap(), unowned);
        assert_eq!(wait.observed_status().unwrap(), unowned);
        assert!(unowned.is_unowned(state));
        assert_eq!(unowned.label(), state.label());
    }
    assert_eq!(known, 16);
    for raw in [0, -1, 19, i32::MAX] {
        assert_eq!(
            decode_lifecycle(Some(raw)),
            Err(LifecycleObservationError::UnknownValue(raw))
        );
    }
    assert_eq!(
        decode_lifecycle(None),
        Err(LifecycleObservationError::Missing)
    );
    for label in ["cleanup_required", "cleanup_failed"] {
        assert_eq!(
            SandboxLifecycleState::from_label(label),
            Err(LifecycleObservationError::UnknownLabel(label.to_owned()))
        );
    }
}

#[test]
fn an_owning_cleanup_is_observed_beside_its_lifecycle_and_passes_no_lifecycle_check() {
    let cleanup = |state: SandboxCleanupState| SandboxCleanup {
        state: state as i32,
        ..SandboxCleanup::default()
    };
    for (owner, observed, label) in [
        (
            SandboxCleanupState::Required,
            ObservedCleanup::Required,
            "cleanup_required",
        ),
        (
            SandboxCleanupState::Failed,
            ObservedCleanup::Failed,
            "cleanup_failed",
        ),
    ] {
        for teardown in [Teardown::Stopping, Teardown::Deleting, Teardown::Suspending] {
            let lifecycle = teardown.lifecycle();
            let info = SandboxInfo {
                lifecycle: Some(lifecycle as i32),
                cleanup: Some(cleanup(owner)),
                ..Default::default()
            };
            let list = SandboxListElement {
                lifecycle: Some(lifecycle as i32),
                cleanup: Some(cleanup(owner)),
                ..Default::default()
            };
            let wait = WaitResponse {
                sandbox_id: String::new(),
                lifecycle: Some(lifecycle as i32),
                cleanup: Some(cleanup(owner)),
            };
            let status = ObservedStatus::Owned {
                teardown,
                cleanup: observed,
            };
            assert_eq!(info.observed_status().unwrap(), status, "{lifecycle:?}");
            assert_eq!(list.observed_status().unwrap(), status, "{lifecycle:?}");
            assert_eq!(wait.observed_status().unwrap(), status, "{lifecycle:?}");
            assert!(!status.is_unowned(lifecycle));
            assert_eq!(status.lifecycle(), lifecycle);
            assert_eq!(status.unowned(), None);
            assert_eq!(status.label(), label);
        }
    }
    // An armed activation's rollback target leaves the lifecycle as reported.
    let armed = SandboxInfo {
        lifecycle: Some(SandboxLifecycleState::Starting as i32),
        cleanup: Some(cleanup(SandboxCleanupState::Unspecified)),
        ..Default::default()
    };
    assert_eq!(
        armed.observed_status().unwrap(),
        ObservedStatus::Unowned(SandboxLifecycleState::Starting)
    );
    let unknown = SandboxInfo {
        lifecycle: Some(SandboxLifecycleState::Stopping as i32),
        cleanup: Some(SandboxCleanup {
            state: i32::MAX,
            ..SandboxCleanup::default()
        }),
        ..Default::default()
    };
    assert_eq!(
        unknown.observed_status(),
        Err(LifecycleObservationError::UnknownCleanupState(i32::MAX))
    );
    // A cleanup over a lifecycle no cleanup can be over names no status.
    let misplaced = SandboxInfo {
        lifecycle: Some(SandboxLifecycleState::Started as i32),
        cleanup: Some(cleanup(SandboxCleanupState::Failed)),
        ..Default::default()
    };
    assert_eq!(
        misplaced.observed_status(),
        Err(LifecycleObservationError::CleanupOutsideTeardown(
            SandboxLifecycleState::Started
        ))
    );
}
