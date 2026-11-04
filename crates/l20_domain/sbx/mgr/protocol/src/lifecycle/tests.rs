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
