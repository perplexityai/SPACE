#[cfg(target_os = "linux")]
mod linux;

#[cfg(target_os = "linux")]
pub(crate) use linux::{WorkerClient, WorkerPool};

#[cfg(target_os = "linux")]
pub(crate) use linux::{FilesystemWorkerError, run_from_environment};
