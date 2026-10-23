use std::sync::Mutex;

use pplx_space_common_core::types::SandboxId;

use super::*;

static SEEN: Mutex<Vec<(SandboxId, SandboxEventType, Option<String>)>> = Mutex::new(Vec::new());

struct Recorder;

impl SandboxEventObserver for Recorder {
    fn observe(&self, event: &SandboxEvent, details: &SandboxEventDetails) {
        if details.action.as_deref() == Some("panic") {
            panic!("observer panic");
        }
        SEEN.lock().unwrap().push((
            event.sandbox_id(),
            event.event_type(),
            details.action.clone(),
        ));
    }
}

// The only test that installs the process-wide observer.
#[test]
fn installed_observer_sees_each_emitted_event() {
    assert!(install_sandbox_event_observer(Box::new(Recorder)).is_ok());
    assert!(install_sandbox_event_observer(Box::new(Recorder)).is_err());

    let sandbox_id = SandboxId::new();
    let event = SandboxEvent::new(
        sandbox_id,
        SandboxEventType::LifecycleConverged,
        SandboxEventSource::SbxManager,
    );
    event.emit_with(SandboxEventDetails::default().action("panic"));
    event.emit_with(SandboxEventDetails::default().action("wake"));

    let seen: Vec<_> = SEEN
        .lock()
        .unwrap()
        .iter()
        .filter(|(id, _, _)| *id == sandbox_id)
        .cloned()
        .collect();
    assert_eq!(
        seen,
        [(
            sandbox_id,
            SandboxEventType::LifecycleConverged,
            Some("wake".to_owned())
        )]
    );
}
