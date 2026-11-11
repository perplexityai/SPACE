//! Optional in-process observer of emitted sandbox events.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::OnceLock;

use crate::{SandboxEvent, SandboxEventDetails};

/// Receives every sandbox event after it is logged.
///
/// `observe` runs on the emitting thread, inside the caller's code path, so it
/// must return quickly and must not block. A panic in it is caught and the
/// event is still emitted.
pub trait SandboxEventObserver: Send + Sync {
    fn observe(&self, event: &SandboxEvent, details: &SandboxEventDetails);
}

static OBSERVER: OnceLock<Box<dyn SandboxEventObserver>> = OnceLock::new();

/// Installs the process's observer. Only the first call takes effect; a later
/// call returns its observer unchanged in `Err`.
pub fn install_sandbox_event_observer(
    observer: Box<dyn SandboxEventObserver>,
) -> Result<(), Box<dyn SandboxEventObserver>> {
    OBSERVER.set(observer)
}

pub(crate) fn notify(event: &SandboxEvent, details: &SandboxEventDetails) {
    if let Some(observer) = OBSERVER.get() {
        let _ = catch_unwind(AssertUnwindSafe(|| observer.observe(event, details)));
    }
}
