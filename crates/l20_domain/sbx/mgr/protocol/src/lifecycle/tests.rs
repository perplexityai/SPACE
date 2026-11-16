use super::*;

#[test]
fn lifecycle_observation_requires_a_known_present_enum() {
    for raw in 1..=18 {
        let state = SandboxLifecycleState::try_from(raw).unwrap();
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
        assert_eq!(info.observed_lifecycle().unwrap(), state);
        assert_eq!(list.observed_lifecycle().unwrap(), state);
        assert_eq!(wait.observed_lifecycle().unwrap(), state);
    }
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
}

#[test]
fn an_owning_cleanup_decodes_to_its_combined_status_whatever_lifecycle_is_reported() {
    let cleanup = |state: SandboxCleanupState| SandboxCleanup {
        state: state as i32,
        ..SandboxCleanup::default()
    };
    for (owner, status) in [
        (
            SandboxCleanupState::Required,
            SandboxLifecycleState::CleanupRequired,
        ),
        (
            SandboxCleanupState::Failed,
            SandboxLifecycleState::CleanupFailed,
        ),
    ] {
        for lifecycle in [
            status,
            SandboxLifecycleState::Stopping,
            SandboxLifecycleState::Deleting,
            SandboxLifecycleState::Suspending,
        ] {
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
            assert_eq!(info.observed_lifecycle().unwrap(), status, "{lifecycle:?}");
            assert_eq!(list.observed_lifecycle().unwrap(), status, "{lifecycle:?}");
            assert_eq!(wait.observed_lifecycle().unwrap(), status, "{lifecycle:?}");
        }
    }
    // An armed activation's rollback target leaves the lifecycle as reported.
    let armed = SandboxInfo {
        lifecycle: Some(SandboxLifecycleState::Starting as i32),
        cleanup: Some(cleanup(SandboxCleanupState::Unspecified)),
        ..Default::default()
    };
    assert_eq!(
        armed.observed_lifecycle().unwrap(),
        SandboxLifecycleState::Starting
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
        unknown.observed_lifecycle(),
        Err(LifecycleObservationError::UnknownCleanupState(i32::MAX))
    );
}
