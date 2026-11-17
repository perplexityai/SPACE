//! A process adapter, not a second command transport. The ordinary Process RPC
//! owns admission, identities, output, signals, timeouts and replay.
//!
//! An operator supplies this profile only inside a per-sandbox contained agent.
//! The lifecycle owner authenticates and read-only mounts the engine, module and
//! library tree before serving requests. These paths are not tenant input. This
//! adapter is not itself a filesystem or host-process containment boundary.

use std::path::{Component, Path, PathBuf};

use pplx_space_common_grpc::{GuestCapability, guest_capability_unsupported_status};
use tokio::process::Command;

/// A deliberately bounded Python/WASI profile. No native fallback and no shell.
#[derive(Clone, Debug)]
pub struct WasiProcessProfile {
    engine: PathBuf,
    module: PathBuf,
    workspace: PathBuf,
    python_library: PathBuf,
    memory_bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum WasiProcessProfileError {
    #[error("WASI profile paths must be absolute canonical locators")]
    Path,
    #[error("WASI immutable tools must be outside the writable workspace")]
    ToolsInWorkspace,
    #[error("WASI memory limit must be between 16 MiB and 4 GiB")]
    Memory,
}

impl WasiProcessProfile {
    /// Locators are already authenticated by the launch owner. Construction does
    /// not confer integrity, read-only mounting or platform qualification.
    pub fn new(
        engine: PathBuf,
        module: PathBuf,
        workspace: PathBuf,
        python_library: PathBuf,
        memory_bytes: u64,
    ) -> Result<Self, WasiProcessProfileError> {
        for path in [&engine, &module, &workspace, &python_library] {
            if !path.is_absolute()
                || path == Path::new("/")
                || path.to_str().is_none_or(|text| text.contains("::"))
                || path
                    .components()
                    .any(|c| matches!(c, Component::ParentDir | Component::CurDir))
            {
                return Err(WasiProcessProfileError::Path);
            }
        }
        if engine.starts_with(&workspace)
            || module.starts_with(&workspace)
            || python_library.starts_with(&workspace)
            || workspace.starts_with(&python_library)
        {
            return Err(WasiProcessProfileError::ToolsInWorkspace);
        }
        if !(16 * 1024 * 1024..=4 * 1024 * 1024 * 1024).contains(&memory_bytes) {
            return Err(WasiProcessProfileError::Memory);
        }
        Ok(Self {
            engine,
            module,
            workspace,
            python_library,
            memory_bytes,
        })
    }

    pub(crate) fn command(
        &self,
        argv: &[String],
        cwd: &Path,
        env: impl Iterator<Item = (String, String)>,
        pty: bool,
    ) -> Result<Command, tonic::Status> {
        if !matches!(
            argv.first().map(String::as_str),
            Some("python" | "python3" | "/usr/bin/python3")
        ) {
            return Err(guest_capability_unsupported_status(
                GuestCapability::PlatformOperations,
                "WASI profile supports Python commands only; native fallback is forbidden",
            ));
        }
        if pty {
            return Err(guest_capability_unsupported_status(
                GuestCapability::InteractiveProcessControl,
                "WASI profile does not implement a PTY",
            ));
        }
        cwd.strip_prefix(&self.workspace).map_err(|_| {
            tonic::Status::invalid_argument("WASI cwd must be inside the sandbox workspace")
        })?;
        let guest_cwd = cwd
            .to_str()
            .ok_or_else(|| tonic::Status::invalid_argument("WASI cwd must be UTF-8"))?;
        let mut command = Command::new(&self.engine);
        // This environment belongs to the native engine, not to guest Python.
        // In particular, guest LD_PRELOAD/PATH cannot affect host execution.
        command.env_clear();
        command
            .arg("run")
            // The contained agent has a read-only root and no trusted writable
            // compilation cache. Never consult a host user's cache directory.
            .arg("-C")
            .arg("cache=n")
            .arg("-S")
            .arg("inherit-env=n,inherit-network=n,allow-ip-name-lookup=n,tcp=n,udp=n,http=n")
            .arg("-S")
            .arg(format!("cwd={guest_cwd}"))
            .arg("-W")
            .arg(format!("max-memory-size={}", self.memory_bytes))
            // Preserve the contained agent's path namespace: the process and
            // filesystem APIs must address the same file, without an alias map.
            .arg("--dir")
            .arg(&self.workspace)
            .arg("--dir")
            .arg(&self.python_library)
            .arg("--env")
            .arg(format!("PYTHONHOME={}", self.python_library.display()))
            .arg("--env")
            .arg(format!("PWD={guest_cwd}"));
        for (key, value) in env {
            command.arg("--env").arg(format!("{key}={value}"));
        }
        command.arg(&self.module).args(argv.iter().skip(1));
        Ok(command)
    }
}

#[cfg(test)]
mod tests;
