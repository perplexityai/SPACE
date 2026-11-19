#[cfg(unix)]
pub(crate) mod unix;

#[cfg(target_os = "linux")]
pub mod linux;
#[cfg(target_os = "macos")]
pub mod macos;
#[cfg(windows)]
pub mod windows;

#[cfg(target_os = "linux")]
pub use linux as current;
#[cfg(target_os = "macos")]
pub use macos as current;
#[cfg(windows)]
pub use windows as current;
