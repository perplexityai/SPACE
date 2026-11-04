use std::io::{Read, Write};
use std::os::fd::{AsFd, AsRawFd, FromRawFd, RawFd};
use std::os::unix::net::UnixStream as StdUnixStream;
use std::os::unix::process::CommandExt;
use std::process::Command;

use pplx_space_node_runtime_core::launch::SidecarLauncherConfig;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::sync::OwnedSemaphorePermit;

// Tonic bounds the protobuf request at roughly 4 MiB. JSON escaping can expand
// an otherwise valid request, so keep the private transport bounded while
// allowing the full public request shape to cross it.
const MAX_LAUNCH_SPEC_BYTES: usize = 32 * 1024 * 1024;
const MAX_LAUNCHER_ERROR_BYTES: usize = 64 * 1024;
pub(crate) const RUNTIME_PID_TAG: u8 = 3;
pub(crate) const PREPARED_TAG: u8 = b'P';
const ERROR_TAG: u8 = b'E';
/// Hidden CLI option name used by the supervisor's self-exec launcher protocol.
pub const SIDECAR_LAUNCHER_FD_ARG_NAME: &str = "sidecar-launcher-fd";

pub(crate) struct SpawnedSidecarLauncher {
    pub(crate) pid: u32,
    stream: UnixStream,
    launch_spec: Vec<u8>,
}

pub(crate) struct PreparedSidecarLauncher {
    launch_spec: Vec<u8>,
    launcher_slot: OwnedSemaphorePermit,
}

impl SpawnedSidecarLauncher {
    /// The namespace runner reports the VMM's host PID, which is the UDS peer
    /// identity. The runner itself remains tracked separately for child reaping.
    pub(crate) async fn wait_for_contained_exec(mut self) -> Result<u32, String> {
        write_frame_async(&mut self.stream, &self.launch_spec).await?;
        self.stream
            .shutdown()
            .await
            .map_err(|e| format!("finish contained launch request: {e}"))?;
        read_contained_outcome(&mut self.stream).await
    }

    pub(crate) async fn wait_for_exec(mut self) -> Result<(), String> {
        write_frame_async(&mut self.stream, &self.launch_spec).await?;
        self.stream
            .shutdown()
            .await
            .map_err(|error| format!("finish sidecar launcher request: {error}"))?;
        read_launcher_outcome(&mut self.stream).await
    }
}

async fn read_contained_outcome(stream: &mut UnixStream) -> Result<u32, String> {
    let tag = stream
        .read_u8()
        .await
        .map_err(|e| format!("read runtime PID tag: {e}"))?;
    if tag == ERROR_TAG {
        return Err(read_error_async(stream).await?);
    }
    if tag != RUNTIME_PID_TAG {
        return Err("missing contained runtime PID".into());
    }
    let pid = stream
        .read_u32()
        .await
        .map_err(|e| format!("read runtime PID: {e}"))?;
    if pid <= 1 {
        return Err("invalid contained runtime PID".into());
    }
    read_launcher_outcome(stream).await?;
    Ok(pid)
}

/// Serialize and bound a sidecar launch before creating any process or cgroup
/// state. Cancellation is therefore harmless throughout this async phase.
pub(crate) async fn prepare(
    config: SidecarLauncherConfig,
    launcher_slot: OwnedSemaphorePermit,
) -> Result<PreparedSidecarLauncher, String> {
    prepare_spec(LaunchSpec::Sidecar(config), launcher_slot).await
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", content = "config")]
enum LaunchSpec {
    Contained {
        contained: crate::contained_launcher::Config,
    },
    Sidecar(SidecarLauncherConfig),
}

pub(crate) async fn prepare_contained(
    config: crate::contained_launcher::Config,
    launcher_slot: OwnedSemaphorePermit,
) -> Result<PreparedSidecarLauncher, String> {
    prepare_spec(LaunchSpec::Contained { contained: config }, launcher_slot).await
}

async fn prepare_spec(
    config: LaunchSpec,
    launcher_slot: OwnedSemaphorePermit,
) -> Result<PreparedSidecarLauncher, String> {
    // Move the permit into the blocking task. Dropping the caller detaches
    // spawn_blocking work, so retaining the permit outside this closure would
    // let repeated cancellations bypass the actual serialization bound.
    let (launch_spec, launcher_slot) = tokio::task::spawn_blocking(move || {
        let launch_spec = serde_json::to_vec(&config)
            .map_err(|error| format!("serialize sidecar launch specification: {error}"))?;
        if launch_spec.len() > MAX_LAUNCH_SPEC_BYTES {
            return Err(format!(
                "sidecar launch specification exceeds {MAX_LAUNCH_SPEC_BYTES} bytes"
            ));
        }
        Ok((launch_spec, launcher_slot))
    })
    .await
    .map_err(|error| format!("sidecar launch serialization task failed: {error}"))??;

    Ok(PreparedSidecarLauncher {
        launch_spec,
        launcher_slot,
    })
}

impl PreparedSidecarLauncher {
    /// Start a fresh copy of runtime-supervisor in its single-threaded launcher
    /// mode. This function has no await points: once the caller creates the
    /// sidecar cgroup, it either returns a trackable child or a synchronous
    /// error that lets the caller remove newly created state.
    ///
    /// The only application-supplied work between the process library's fork
    /// and its first exec is one async-signal-safe `fcntl(2)` call that passes
    /// the private status socket to the new image.
    pub(crate) fn spawn(self) -> Result<SpawnedSidecarLauncher, String> {
        let PreparedSidecarLauncher {
            launch_spec,
            launcher_slot,
        } = self;
        let (parent_stream, child_stream) = StdUnixStream::pair()
            .map_err(|error| format!("create sidecar launcher socketpair: {error}"))?;
        set_cloexec(&parent_stream, true)
            .map_err(|error| format!("protect parent sidecar launcher socket: {error}"))?;
        set_cloexec(&child_stream, true)
            .map_err(|error| format!("protect child sidecar launcher socket: {error}"))?;
        parent_stream
            .set_nonblocking(true)
            .map_err(|error| format!("configure sidecar launcher socket: {error}"))?;
        let stream = UnixStream::from_std(parent_stream)
            .map_err(|error| format!("adopt sidecar launcher socket: {error}"))?;
        let launcher_fd = child_stream.as_raw_fd();
        let mut command = launcher_command(sidecar_launcher_program()?);
        command
            .arg(format!("--{SIDECAR_LAUNCHER_FD_ARG_NAME}"))
            .arg(launcher_fd.to_string());
        // SAFETY: the closure calls only fcntl(2), an async-signal-safe syscall,
        // and returns its errno without allocation. Clearing CLOEXEC is required
        // for the first exec into launcher mode; launcher mode restores it before
        // the final exec into the sidecar.
        unsafe {
            command.pre_exec(move || set_cloexec(&child_stream, false));
        }
        let child = command
            .spawn()
            .map_err(|error| format!("exec sidecar launcher: {error}"))?;
        drop(command);
        drop(launcher_slot);
        Ok(SpawnedSidecarLauncher {
            pid: child.id(),
            stream,
            launch_spec,
        })
    }
}

fn launcher_command(program: impl AsRef<std::ffi::OsStr>) -> Command {
    let mut command = Command::new(program);
    // The launch specification carries the sidecar's explicit environment.
    // Never pass supervisor credentials or loader settings across this exec.
    command
        .env_clear()
        .env("PATH", "/usr/sbin:/usr/bin:/sbin:/bin");
    command
}

#[cfg(target_os = "linux")]
fn sidecar_launcher_program() -> Result<&'static str, String> {
    // Using procfs pins launcher mode to the running supervisor image during a
    // rolling binary replacement; resolving the installed path could mix two
    // protocol versions in one launch.
    Ok("/proc/self/exe")
}

#[cfg(not(target_os = "linux"))]
fn sidecar_launcher_program() -> Result<std::path::PathBuf, String> {
    std::env::current_exe().map_err(|error| format!("resolve runtime-supervisor binary: {error}"))
}

pub fn run(raw_fd: RawFd) -> Result<(), String> {
    if raw_fd < 0 {
        return Err(format!("invalid sidecar launcher fd {raw_fd}"));
    }
    // SAFETY: F_GETFD only inspects the integer descriptor and reports EBADF
    // for an invalid launcher argument; it borrows no Rust memory.
    if unsafe { libc::fcntl(raw_fd, libc::F_GETFD) } == -1 {
        return Err(format!(
            "protect sidecar launcher status fd: {}",
            std::io::Error::last_os_error()
        ));
    }
    // SAFETY: launcher mode receives this descriptor exactly once from the
    // parent process and takes sole ownership for the remainder of this mode.
    let mut stream = unsafe { StdUnixStream::from_raw_fd(raw_fd) };
    set_cloexec(&stream, true)
        .map_err(|error| format!("protect sidecar launcher status fd: {error}"))?;

    let request = read_frame(&mut stream).and_then(|bytes| {
        serde_json::from_slice::<LaunchSpec>(&bytes)
            .map_err(|error| format!("decode launcher specification: {error}"))
    });
    let result = match request {
        Ok(LaunchSpec::Contained { contained }) => {
            return crate::contained_launcher::run(contained, stream);
        }
        Ok(LaunchSpec::Sidecar(config)) => run_inner(config, &mut stream),
        Err(error) => Err(error),
    };
    match result {
        Ok(()) => Ok(()),
        Err(error) => match write_error(&mut stream, &error) {
            Ok(()) => Err(error),
            Err(report_error) => Err(format!(
                "{error}; additionally failed to report launcher error: {report_error}"
            )),
        },
    }
}

fn run_inner(config: SidecarLauncherConfig, stream: &mut StdUnixStream) -> Result<(), String> {
    let prepared = pplx_space_node_runtime_core::launch::prepare_sidecar_launch(config)?;
    stream
        .write_all(&[PREPARED_TAG])
        .map_err(|error| format!("report prepared sidecar launch: {error}"))?;
    prepared.exec()
}

async fn read_launcher_outcome(stream: &mut UnixStream) -> Result<(), String> {
    match stream
        .read_u8()
        .await
        .map_err(|error| format!("read sidecar launcher outcome: {error}"))?
    {
        PREPARED_TAG => match stream.read_u8().await {
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => Ok(()),
            Ok(ERROR_TAG) => Err(read_error_async(stream).await?),
            Ok(tag) => Err(format!(
                "sidecar launcher sent unexpected post-prepare tag {tag}"
            )),
            Err(error) => Err(format!("read sidecar exec outcome: {error}")),
        },
        ERROR_TAG => Err(read_error_async(stream).await?),
        tag => Err(format!(
            "sidecar launcher sent unexpected outcome tag {tag}"
        )),
    }
}

fn read_frame(stream: &mut StdUnixStream) -> Result<Vec<u8>, String> {
    let mut length = [0u8; 4];
    stream
        .read_exact(&mut length)
        .map_err(|error| format!("read sidecar launch specification length: {error}"))?;
    let length = usize::try_from(u32::from_be_bytes(length))
        .map_err(|error| format!("convert sidecar launch specification length: {error}"))?;
    if length > MAX_LAUNCH_SPEC_BYTES {
        return Err(format!(
            "sidecar launch specification exceeds {MAX_LAUNCH_SPEC_BYTES} bytes"
        ));
    }
    let mut body = vec![0u8; length];
    stream
        .read_exact(&mut body)
        .map_err(|error| format!("read sidecar launch specification: {error}"))?;
    Ok(body)
}

async fn write_frame_async(stream: &mut UnixStream, body: &[u8]) -> Result<(), String> {
    let length = u32::try_from(body.len())
        .map_err(|error| format!("encode sidecar launch specification length: {error}"))?;
    stream
        .write_all(&length.to_be_bytes())
        .await
        .map_err(|error| format!("write sidecar launch specification length: {error}"))?;
    stream
        .write_all(body)
        .await
        .map_err(|error| format!("write sidecar launch specification: {error}"))
}

pub(crate) fn write_error(stream: &mut StdUnixStream, error: &str) -> Result<(), std::io::Error> {
    let mut end = error.len().min(MAX_LAUNCHER_ERROR_BYTES);
    while !error.is_char_boundary(end) {
        end -= 1;
    }
    let body = error
        .get(..end)
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "truncate sidecar launcher error at UTF-8 boundary",
            )
        })?
        .as_bytes();
    let length = u32::try_from(body.len()).map_err(|error| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("encode sidecar launcher error length: {error}"),
        )
    })?;
    stream.write_all(&[ERROR_TAG])?;
    stream.write_all(&length.to_be_bytes())?;
    stream.write_all(body)
}

async fn read_error_async(stream: &mut UnixStream) -> Result<String, String> {
    let length = stream
        .read_u32()
        .await
        .map_err(|error| format!("read sidecar launcher error length: {error}"))?;
    let length = usize::try_from(length)
        .map_err(|error| format!("convert sidecar launcher error length: {error}"))?;
    if length > MAX_LAUNCHER_ERROR_BYTES {
        return Err(format!(
            "sidecar launcher error exceeds {MAX_LAUNCHER_ERROR_BYTES} bytes"
        ));
    }
    let mut body = vec![0u8; length];
    stream
        .read_exact(&mut body)
        .await
        .map_err(|error| format!("read sidecar launcher error: {error}"))?;
    String::from_utf8(body).map_err(|error| format!("sidecar launcher error is not UTF-8: {error}"))
}

fn set_cloexec(fd: impl AsFd, enabled: bool) -> std::io::Result<()> {
    use nix::fcntl::{FcntlArg, FdFlag, fcntl};
    let mut flags = FdFlag::from_bits_retain(fcntl(fd.as_fd(), FcntlArg::F_GETFD)?);
    flags.set(FdFlag::FD_CLOEXEC, enabled);
    fcntl(fd, FcntlArg::F_SETFD(flags))?;
    Ok(())
}

#[cfg(test)]
mod tests;
