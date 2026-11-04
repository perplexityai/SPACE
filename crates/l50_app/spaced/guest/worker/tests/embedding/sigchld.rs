//! A caller's preinstalled notification handler coexists with Tokio's handler.
use std::sync::atomic::{AtomicUsize, Ordering};

use nix::sys::signal::{SaFlags, SigAction, SigHandler, SigSet, Signal, sigaction};

static NOTIFICATIONS: AtomicUsize = AtomicUsize::new(0);

extern "C" fn notify(_signal: nix::libc::c_int) {
    // Only a lock-free atomic operation: no allocation, locks or child reaping
    // in this process-wide signal handler.
    NOTIFICATIONS.fetch_add(1, Ordering::Relaxed);
}

pub(super) fn notifications() -> usize {
    NOTIFICATIONS.load(Ordering::Relaxed)
}

pub(super) struct CallerHandler(SigAction);

impl CallerHandler {
    pub(super) fn install() -> Self {
        let action = SigAction::new(
            SigHandler::Handler(notify),
            SaFlags::SA_RESTART,
            SigSet::empty(),
        );
        // SAFETY: single-threaded test entrypoint, before Tokio registers its
        // handler. The callback above is async-signal-safe.
        Self(unsafe { sigaction(Signal::SIGCHLD, &action) }.expect("caller SIGCHLD handler"))
    }
}

impl Drop for CallerHandler {
    fn drop(&mut self) {
        // SAFETY: restore the valid disposition saved before installing our handler.
        // The runtime is already dropped, including when the test unwinds.
        unsafe { sigaction(Signal::SIGCHLD, &self.0) }.expect("restore caller SIGCHLD handler");
    }
}
