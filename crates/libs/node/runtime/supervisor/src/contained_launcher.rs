//! Single-threaded Linux process containment. The supervisor tracks the
//! unprivileged runner; it owns and reaps the PID-namespace init process.
//! Killing the runner kills that process through PDEATHSIG, and cgroup cleanup
//! remains the authority for all descendants.
use std::path::PathBuf;

#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct Config {
    pub root: PathBuf,
    pub network_namespace: PathBuf,
    pub executable: PathBuf,
    pub executable_digest: pplx_space_node_runtime_core::verity::VerityDigest,
    pub uid: u32,
    pub gid: u32,
    pub cgroup: PathBuf,
    pub output: PathBuf,
    pub arguments: Vec<String>,
}

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
