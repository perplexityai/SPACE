#[cfg(target_os = "linux")]
mod linux;
#[cfg(not(target_os = "linux"))]
mod other;

#[cfg(target_os = "linux")]
pub use linux::*;
#[cfg(not(target_os = "linux"))]
pub use other::*;

/// The default limit on the jailer run in [`launch_via_jailer`], which ends
/// once the jailer has built the chroot, cgroups and namespaces and daemonized
/// Firecracker. That work copies the Firecracker binary and writes cgroup
/// files, so it slows down with the rest of a loaded node, as the pidfile and
/// API-socket waits after it do. Those waits allow 5 s each; a tighter limit
/// on the jailer alone would fail launches that every later stage would have
/// let finish.
pub const DEFAULT_JAILER_START_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// A lifetime witness without signaling authority. The namespace runner must
/// observe itself across fork, where the signalable handle's self-PID guard
/// deliberately does not apply.
pub struct ProcessObserver(ProcessHandle);
impl ProcessObserver {
    pub fn current() -> Result<Self, ProcessHandleError> {
        pin_current_process().map(Self)
    }
    pub fn pid(&self) -> u32 {
        self.0.pid()
    }
    pub fn has_exited(&self) -> std::io::Result<bool> {
        self.0.has_exited()
    }
}
impl From<ProcessHandle> for ProcessObserver {
    fn from(handle: ProcessHandle) -> Self {
        Self(handle)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcessOwnership {
    Owned,
    Gone,
    Foreign,
}

impl std::fmt::Debug for SidecarLauncherConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SidecarLauncherConfig")
            .field("program", &self.program)
            .field("uid", &self.uid)
            .field("gid", &self.gid)
            .field("cgroup_path", &self.cgroup_path)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SandboxSignalError {
    #[error("inspect sandbox process ownership: {0}")]
    Inspect(#[from] std::io::Error),
    #[error(transparent)]
    Signal(#[from] ProcessHandleError),
}

impl ProcessHandle {
    /// Signal only the pinned process if it still belongs to this sandbox.
    /// Gone and foreign processes are skipped; unreadable ownership fails closed.
    pub fn signal_if_owned(
        &self,
        sandbox_id: &str,
        signal: i32,
    ) -> Result<bool, SandboxSignalError> {
        match self.sandbox_ownership(sandbox_id)? {
            ProcessOwnership::Owned => Ok(self.signal(signal)?),
            ProcessOwnership::Gone | ProcessOwnership::Foreign => Ok(false),
        }
    }

    /// Inspect all UID/GID values. None means the pinned process exited;
    /// unreadable evidence on a live process remains an error.
    pub fn credentials_match(&self, uid: u32, gid: u32) -> std::io::Result<Option<bool>> {
        let result = std::fs::read_to_string(format!("/proc/{}/status", self.pid()))
            .map(|status| credentials_match(&status, uid, gid));
        self.inspect_while_running(result)
    }

    pub fn program_matches(&self, program: &str) -> std::io::Result<Option<bool>> {
        self.inspect_while_running(process_matches_program(self.pid(), program))
    }

    /// Match the unified VM cgroup exactly, not a sandbox ID in argv or a
    /// sibling sidecar cgroup. Discard evidence if this process exits meanwhile.
    pub fn cgroup_matches(&self, expected: &std::path::Path) -> std::io::Result<Option<bool>> {
        let expected = expected
            .strip_prefix("/sys/fs/cgroup")
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?;
        let result = std::fs::read_to_string(format!("/proc/{}/cgroup", self.pid())).map(|text| {
            text.lines().any(|line| {
                line.strip_prefix("0::").is_some_and(|path| {
                    std::path::Path::new(path).strip_prefix("/").ok() == Some(expected)
                })
            })
        });
        self.inspect_while_running(result)
    }

    /// Discard inspection results if the pinned process has exited.
    pub(crate) fn inspect_while_running<T>(
        &self,
        result: std::io::Result<T>,
    ) -> std::io::Result<Option<T>> {
        if self.has_exited()? {
            Ok(None)
        } else {
            result.map(Some)
        }
    }

    /// Check metadata against a pinned process, distinguishing disappearance
    /// from foreign ownership and unreadable evidence. Recheck the pidfd after
    /// reading /proc: its numeric PID may already name a replacement process.
    pub fn sandbox_ownership(&self, sandbox_id: &str) -> std::io::Result<ProcessOwnership> {
        if self.has_exited()? {
            return Ok(ProcessOwnership::Gone);
        }
        let cgroup = std::fs::read_to_string(format!("/proc/{}/cgroup", self.pid()));
        let ownership = if cgroup.as_ref().is_ok_and(|text| text.contains(sandbox_id)) {
            Ok(ProcessOwnership::Owned)
        } else {
            let cmdline = std::fs::read(format!("/proc/{}/cmdline", self.pid()));
            if cmdline.as_ref().is_ok_and(|text| {
                text.split(|byte| *byte == 0)
                    .any(|part| part == sandbox_id.as_bytes())
            }) {
                Ok(ProcessOwnership::Owned)
            } else {
                cgroup.and(cmdline).map(|_| ProcessOwnership::Foreign)
            }
        };
        if self.has_exited()? {
            Ok(ProcessOwnership::Gone)
        } else {
            ownership
        }
    }
}

fn credentials_match(status: &str, uid: u32, gid: u32) -> bool {
    [("Uid:", uid), ("Gid:", gid)]
        .into_iter()
        .all(|(field, expected)| {
            status
                .lines()
                .find_map(|line| line.strip_prefix(field))
                .is_some_and(|values| {
                    let mut values = values.split_whitespace();
                    (0..4).all(|_| {
                        values.next().and_then(|value| value.parse::<u32>().ok()) == Some(expected)
                    }) && values.next().is_none()
                })
        })
}

/// Identify a deployed sidecar binary, including content-addressed installs.
pub fn process_matches_program(pid: u32, program: &str) -> std::io::Result<bool> {
    use pplx_space_common_core::host::linux::paths;
    let Some(binary) = std::path::Path::new(program)
        .file_name()
        .and_then(|name| name.to_str())
    else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "program has no filename",
        ));
    };
    std::fs::read_link(format!("/proc/{pid}/exe")).map(|exe| {
        paths::strip_exe_deleted_marker(&exe)
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| {
                name == binary || paths::is_content_addressed_binary_file_name(name, binary)
            })
    })
}

#[cfg(test)]
mod tests;
