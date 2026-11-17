//! Single-threaded Linux process containment. The supervisor tracks the
//! unprivileged runner; it owns and reaps the PID-namespace init process.
//! Killing the runner kills that process through PDEATHSIG, and cgroup cleanup
//! remains the authority for all descendants.
use std::path::PathBuf;

use pplx_space_execution_spec::RuntimeKind;

#[cfg(all(target_os = "linux", feature = "experimental-wasm"))]
mod wasi_guest;

/// The default preserves the exec contract of serialized VM launch requests.
/// The guest entrypoint is the supervisor's already-loaded, trusted service
/// code, not an executable selected from the tenant's filesystem.
#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
pub(crate) enum Entrypoint {
    #[default]
    Runtime,
    #[cfg(all(target_os = "linux", feature = "experimental-wasm"))]
    WasiGuest(Box<wasi_guest::Config>),
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct Config {
    // Older supervisors serialize Cloud Hypervisor launch specifications without
    // a runtime discriminator. Preserve that meaning across rolling upgrades.
    #[serde(default = "legacy_runtime")]
    pub runtime: RuntimeKind,
    #[serde(default)]
    pub entrypoint: Entrypoint,
    pub root: PathBuf,
    pub registration_directory: PathBuf,
    pub network_namespace: PathBuf,
    pub executable: PathBuf,
    pub executable_source: pplx_space_node_runtime_core::authenticated_source::SourcePins,
    pub uid: u32,
    pub gid: u32,
    pub cgroup: PathBuf,
    pub output: PathBuf,
    pub arguments: Vec<String>,
}

fn legacy_runtime() -> RuntimeKind {
    RuntimeKind::CloudHypervisor
}

#[cfg(test)]
mod tests;

#[cfg(not(target_os = "linux"))]
pub(crate) fn run(
    _config: Config,
    mut stream: std::os::unix::net::UnixStream,
) -> Result<(), String> {
    let message = "standalone containment requires Linux";
    crate::sidecar_launcher::write_error(&mut stream, message).map_err(|e| e.to_string())?;
    Err(message.into())
}

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
pub(crate) use linux::run;
