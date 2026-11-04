#[cfg(target_os = "linux")]
mod linux;
#[cfg(not(target_os = "linux"))]
mod other;

#[cfg(target_os = "linux")]
pub(crate) use linux::*;
#[cfg(not(target_os = "linux"))]
pub(crate) use other::*;
