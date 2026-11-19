//! Guest server backends selected explicitly by operating system.
//! Linux and macOS share Unix mechanisms; Windows owns its native SCM, Job
//! Object and VirtIO transport implementation. Capabilities remain backend-specific.

pub mod platform;
// Preserve the target backend's existing exports for embedding callers.
pub use platform::current::*;
