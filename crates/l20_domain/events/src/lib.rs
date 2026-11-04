#![recursion_limit = "256"]

mod common;
mod details;
mod event_type;
mod observer;
mod phase;
mod source;

pub use common::{
    SANDBOX_EVENT_SCHEMA_VERSION, SANDBOX_EVENT_STREAM, SandboxEvent, emit_sandbox_event,
};
pub use details::{
    CreateShapeSource, SandboxEventBoolField, SandboxEventDetails, SandboxEventTrigger,
    SandboxRestoreProvenance,
};
pub use event_type::{SANDBOX_EVENT_TYPES, SandboxEventType};
pub use observer::{SandboxEventObserver, install_sandbox_event_observer};
pub use phase::SandboxEventPhase;
pub use source::{SANDBOX_EVENT_SOURCES, SandboxEventSource};

#[cfg(test)]
mod observer_tests;
#[cfg(test)]
mod tests;
