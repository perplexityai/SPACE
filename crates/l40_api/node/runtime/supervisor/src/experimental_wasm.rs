//! Experimental non-KVM runtime owned by the node supervisor.
//!
//! This deliberately has no public listener or production scheduling registration.
//! A trusted local caller provides configuration; untrusted commands only reach
//! Wasmtime/WASI. Container cgroups must supply the memory limit. Snapshots, networking,
//! arbitrary OCI images and production tenancy are absent.
//! Additional preopened directories must be mounted read-only by deployment.
//! The configured module is trusted; guest requests only supply its arguments.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::process::{CommandExt, ExitStatusExt};
use std::path::{Component, Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use nix::sys::signal::{Signal, killpg};
use nix::unistd::Pid;
use pplx_space_common_core::types::SandboxLifecycle;
use pplx_space_execution_spec::{RuntimeCapabilities, StartupPolicy};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

const MAX_BYTES: u64 = 1_048_576;
// Leave room for base64 and response metadata inside the 1 MiB wire budget.
const MAX_BINARY_BYTES: u64 = 512_000;

#[derive(Debug, Error)]
pub enum WasmRequestError {
    #[error("Wasm runtime I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid Wasm runtime request: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid base64 file payload: {0}")]
    Base64(#[from] base64::DecodeError),
    #[error("Wasm request rejected: {0}")]
    Invalid(&'static str),
    #[error("Wasm runtime is busy or could not be locked: {0}")]
    Lock(#[from] std::fs::TryLockError),
    #[error("Wasm output collector panicked")]
    CapturePanic,
}

/// Host-controlled configuration, never supplied by guest commands.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WasmConfig {
    pub root: PathBuf,
    pub tools: PathBuf,
    pub module: PathBuf,
    #[serde(default)]
    pub preopens: BTreeMap<String, PathBuf>,
}

#[derive(Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
enum Request {
    Capabilities {},
    Create {
        #[serde(default)]
        sandbox_id: Option<Uuid>,
        #[serde(default)]
        required_capabilities: RuntimeCapabilities,
        startup_policy: Option<StartupPolicy>,
    },
    Inspect {
        sandbox_id: Uuid,
    },
    Stop {
        sandbox_id: Uuid,
    },
    Start {
        sandbox_id: Uuid,
    },
    Execute {
        sandbox_id: Uuid,
        #[serde(default)]
        execution_id: Option<Uuid>,
        argv: Vec<String>,
        timeout_secs: u64,
        #[serde(default)]
        env: BTreeMap<String, String>,
    },
    Cancel {
        sandbox_id: Uuid,
        execution_id: Uuid,
    },
    Upload {
        sandbox_id: Uuid,
        path: String,
        content: String,
    },
    Download {
        sandbox_id: Uuid,
        path: String,
    },
    UploadBinary {
        sandbox_id: Uuid,
        path: String,
        content_base64: String,
    },
    DownloadBinary {
        sandbox_id: Uuid,
        path: String,
    },
    Delete {
        sandbox_id: Uuid,
    },
}

#[derive(Serialize)]
struct Response {
    protocol_version: u32,
    runtime: &'static str,
    sandbox_id: Uuid,
    #[serde(skip_serializing_if = "Option::is_none")]
    execution: Option<Execution>,
    #[serde(skip_serializing_if = "Option::is_none")]
    content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    content_base64: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    capabilities: Option<RuntimeCapabilities>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_lifecycle"
    )]
    state: Option<SandboxLifecycle>,
}

fn serialize_lifecycle<S: serde::Serializer>(
    state: &Option<SandboxLifecycle>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    state.map(SandboxLifecycle::as_str).serialize(serializer)
}

#[derive(Serialize)]
struct Execution {
    execution_id: Uuid,
    exit_code: i32,
    timed_out: bool,
    cancelled: bool,
    stdout: String,
    stderr: String,
    elapsed_ms: u128,
    supervisor_pid: u32,
    runtime_pid: u32,
}

/// Per-sandbox single-flight local protocol entrypoint. Synchronous by design: this mode is
/// a dedicated supervisor process, never a Tokio worker or stateless API handler.
pub fn run_request(
    config: WasmConfig,
    input: impl Read,
    mut output: impl Write,
) -> Result<(), WasmRequestError> {
    if !config.root.is_absolute()
        || !config.tools.is_absolute()
        || !config.module.is_absolute()
        || config.preopens.iter().any(|(guest, host)| {
            !host.is_absolute()
                || !guest.starts_with('/')
                || guest == "/"
                || guest.starts_with("/workspace")
                || guest.starts_with("/tmp")
                || guest.contains("..")
        })
    {
        return Err(WasmRequestError::Invalid("host paths must be absolute"));
    }
    fs::create_dir_all(&config.root)?;
    let root = fs::canonicalize(&config.root)?;
    let tools = fs::canonicalize(&config.tools)?;
    // Version-one callers took this lock exclusively. A shared lock preserves
    // safe exclusion from an older in-flight runner without serializing v2
    // sandboxes against one another.
    let compatibility_lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(root.join(".lock"))?;
    compatibility_lock.try_lock_shared()?;
    let mut bytes = Vec::new();
    input.take(MAX_BYTES + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_BYTES {
        return Err(WasmRequestError::Invalid("request exceeds 1 MiB"));
    }
    let mut request: Request = serde_json::from_slice(&bytes)?;
    if let Request::Create {
        sandbox_id,
        required_capabilities,
        startup_policy,
    } = &request
    {
        let available = RuntimeCapabilities::default();
        if !available.contains(*required_capabilities)
            || startup_policy
                .is_some_and(|policy| !available.contains(policy.required_capabilities()))
        {
            return Err(WasmRequestError::Invalid(
                "required platform capabilities are not qualified",
            ));
        }
        if sandbox_id.is_some_and(|id| id.get_version_num() != 7) {
            return Err(WasmRequestError::Invalid("sandbox id must be UUIDv7"));
        }
    }
    // Locks live outside deletable sandboxes: deleting and recreating an ID
    // must not give two callers different lock inodes for the same resource.
    let id = match &mut request {
        Request::Capabilities {} => None,
        Request::Create { sandbox_id, .. } => Some(*sandbox_id.get_or_insert_with(Uuid::now_v7)),
        Request::Inspect { sandbox_id }
        | Request::Stop { sandbox_id }
        | Request::Start { sandbox_id }
        | Request::Execute { sandbox_id, .. }
        | Request::Cancel { sandbox_id, .. }
        | Request::Upload { sandbox_id, .. }
        | Request::Download { sandbox_id, .. }
        | Request::UploadBinary { sandbox_id, .. }
        | Request::DownloadBinary { sandbox_id, .. }
        | Request::Delete { sandbox_id } => Some(*sandbox_id),
    };
    let locks = root.join(".locks");
    fs::create_dir_all(&locks)?;
    let _lock = if !matches!(&request, Request::Cancel { .. }) {
        id.map(|id| {
            let file = OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .open(locks.join(id.to_string()))?;
            file.try_lock()?;
            Ok::<_, WasmRequestError>(file)
        })
        .transpose()?
    } else {
        None
    };
    let mut response = Response {
        protocol_version: 2,
        runtime: "wasmtime-wasi",
        sandbox_id: Uuid::nil(),
        execution: None,
        content: None,
        content_base64: None,
        capabilities: None,
        state: None,
    };
    match request {
        Request::Capabilities {} => {
            // Reuse the platform vocabulary without claiming unqualified
            // gateway, managed-volume, snapshot or warm-pool integrations.
            response.capabilities = Some(RuntimeCapabilities::default());
        }
        Request::Create { sandbox_id, .. } => {
            let id = sandbox_id.unwrap_or_else(Uuid::now_v7);
            let dir = root.join(id.to_string());
            if !dir.try_exists()? {
                // Publish a complete node-local sandbox or nothing. The sandbox
                // lock serializes retries; abandoned staging is never runnable.
                let pending = tempfile::tempdir_in(&root)?;
                fs::create_dir(pending.path().join("workspace"))?;
                fs::create_dir(pending.path().join("tmp"))?;
                fs::rename(pending.path(), &dir)?;
            }
            sandbox_dir(&root, id)?;
            response.sandbox_id = id;
            response.state = Some(sandbox_state(&dir)?);
        }
        Request::Inspect { sandbox_id } => {
            response.state = Some(sandbox_state(&sandbox_dir(&root, sandbox_id)?)?);
            response.sandbox_id = sandbox_id;
        }
        Request::Stop { sandbox_id } => {
            let dir = sandbox_dir(&root, sandbox_id)?;
            // Execute holds the same lock until child cleanup. Stop is a
            // quiescent admission transition, not concurrent cancellation.
            OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .open(dir.join(".stopped"))?;
            response.state = Some(SandboxLifecycle::Stopped);
            response.sandbox_id = sandbox_id;
        }
        Request::Start { sandbox_id } => {
            let dir = sandbox_dir(&root, sandbox_id)?;
            match fs::remove_file(dir.join(".stopped")) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
            response.state = Some(SandboxLifecycle::Started);
            response.sandbox_id = sandbox_id;
        }
        Request::Execute {
            sandbox_id,
            execution_id,
            argv,
            timeout_secs,
            env,
        } => {
            let dir = sandbox_dir(&root, sandbox_id)?;
            if matches!(sandbox_state(&dir)?, SandboxLifecycle::Stopped) {
                return Err(WasmRequestError::Invalid("sandbox is stopped"));
            }
            if !(1..=120).contains(&timeout_secs)
                || argv.len() > 256
                || argv.iter().map(String::len).sum::<usize>() > 65_536
                || argv.iter().any(|argument| argument.contains('\0'))
            {
                return Err(WasmRequestError::Invalid("invalid arguments or timeout"));
            }
            if env.len() > 32
                || env.iter().any(|(key, value)| {
                    key.is_empty()
                        || !key.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_')
                        || key.len() > 128
                        || value.len() > 4096
                        || value.contains('\0')
                })
            {
                return Err(WasmRequestError::Invalid("invalid guest environment"));
            }
            response.sandbox_id = sandbox_id;
            let execution_id = execution_id.unwrap_or_else(Uuid::now_v7);
            if execution_id.get_version_num() != 7 {
                return Err(WasmRequestError::Invalid("execution id must be UUIDv7"));
            }
            response.execution = Some(execute(
                &config,
                &tools,
                &dir,
                &argv,
                env,
                ExecutionControl {
                    execution_id,
                    timeout_secs,
                },
            )?);
        }
        Request::Cancel {
            sandbox_id,
            execution_id,
        } => {
            if execution_id.get_version_num() != 7 {
                return Err(WasmRequestError::Invalid("execution id must be UUIDv7"));
            }
            let dir = sandbox_dir(&root, sandbox_id)?;
            // An ID-scoped marker also covers cancellation racing process
            // startup. No PID is accepted or signalled by this request.
            OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .open(dir.join(format!(".cancel-{execution_id}")))?;
            response.sandbox_id = sandbox_id;
        }
        Request::Upload {
            sandbox_id,
            path,
            content,
        } => {
            let dir = sandbox_dir(&root, sandbox_id)?;
            upload_bytes(&dir, &path, content.as_bytes())?;
            response.sandbox_id = sandbox_id;
        }
        Request::UploadBinary {
            sandbox_id,
            path,
            content_base64,
        } => {
            let dir = sandbox_dir(&root, sandbox_id)?;
            let bytes = BASE64.decode(content_base64)?;
            if bytes.len() as u64 > MAX_BINARY_BYTES {
                return Err(WasmRequestError::Invalid("binary upload exceeds 500 KiB"));
            }
            upload_bytes(&dir, &path, &bytes)?;
            response.sandbox_id = sandbox_id;
        }
        Request::DownloadBinary { sandbox_id, path } => {
            let dir = sandbox_dir(&root, sandbox_id)?;
            let mut bytes = Vec::new();
            File::open(guest_path(&dir, &path)?)?
                .take(MAX_BINARY_BYTES + 1)
                .read_to_end(&mut bytes)?;
            if bytes.len() as u64 > MAX_BINARY_BYTES {
                return Err(WasmRequestError::Invalid("binary download exceeds 500 KiB"));
            }
            response.content_base64 = Some(BASE64.encode(bytes));
            response.sandbox_id = sandbox_id;
        }
        Request::Download { sandbox_id, path } => {
            let dir = sandbox_dir(&root, sandbox_id)?;
            let mut content = String::new();
            File::open(guest_path(&dir, &path)?)?
                .take(MAX_BYTES + 1)
                .read_to_string(&mut content)?;
            if content.len() as u64 > MAX_BYTES {
                return Err(WasmRequestError::Invalid("download exceeds 1 MiB"));
            }
            response.content = Some(content);
            response.sandbox_id = sandbox_id;
        }
        Request::Delete { sandbox_id } => {
            match sandbox_dir(&root, sandbox_id) {
                Ok(dir) => fs::remove_dir_all(dir)?,
                Err(WasmRequestError::Io(error))
                    if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
            response.sandbox_id = sandbox_id;
        }
    }
    serde_json::to_writer(&mut output, &response)?;
    output.write_all(b"\n")?;
    Ok(())
}

fn upload_bytes(dir: &Path, path: &str, bytes: &[u8]) -> Result<(), WasmRequestError> {
    let host_path = guest_path(dir, path)?;
    let parent = host_path
        .parent()
        .ok_or(WasmRequestError::Invalid("missing parent"))?;
    fs::create_dir_all(parent)?;
    // Recheck under the root lock and publish only complete files. No guest
    // process survives execute(), so none can replace the checked ancestors.
    let target = guest_path(dir, path)?;
    let mut pending = tempfile::NamedTempFile::new_in(parent)?;
    pending.write_all(bytes)?;
    pending.persist(target).map_err(|error| error.error)?;
    Ok(())
}

/// Node-local admission observation, not control-plane desired state.
fn sandbox_state(dir: &Path) -> Result<SandboxLifecycle, WasmRequestError> {
    Ok(if dir.join(".stopped").try_exists()? {
        SandboxLifecycle::Stopped
    } else {
        SandboxLifecycle::Started
    })
}

fn sandbox_dir(root: &Path, id: Uuid) -> Result<PathBuf, WasmRequestError> {
    let dir = root.join(id.to_string());
    let metadata = fs::symlink_metadata(&dir)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(WasmRequestError::Invalid("sandbox is not a directory"));
    }
    Ok(dir)
}

fn guest_path(dir: &Path, guest: &str) -> Result<PathBuf, WasmRequestError> {
    let path = Path::new(guest);
    if !path.is_absolute() || path.components().any(|c| matches!(c, Component::ParentDir)) {
        return Err(WasmRequestError::Invalid(
            "guest path must be absolute without traversal",
        ));
    }
    let relative = path
        .strip_prefix("/")
        .map_err(|_| WasmRequestError::Invalid("guest path"))?;
    let first = relative.components().next();
    if !matches!(first, Some(Component::Normal(p)) if p == "tmp" || p == "workspace") {
        return Err(WasmRequestError::Invalid(
            "only /tmp and /workspace are writable",
        ));
    }
    let mut current = dir.to_path_buf();
    for part in relative.components() {
        current.push(part);
        match fs::symlink_metadata(&current) {
            Ok(m) if m.file_type().is_symlink() => {
                return Err(WasmRequestError::Invalid("symlink in guest file path"));
            }
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(current)
}

struct RuntimeChild(Child);

impl Drop for RuntimeChild {
    fn drop(&mut self) {
        if let Ok(pid) = i32::try_from(self.0.id())
            && let Err(error) = killpg(Pid::from_raw(pid), Signal::SIGKILL)
            && error != nix::errno::Errno::ESRCH
        {
            tracing::warn!(%error, "failed to kill experimental runtime process group");
        }
        if let Err(error) = self.0.wait() {
            tracing::warn!(%error, "failed to reap experimental runtime");
        }
    }
}

struct CancellationFile(PathBuf);

impl Drop for CancellationFile {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_file(&self.0)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(%error, "failed to remove experimental cancellation marker");
        }
    }
}

struct ExecutionControl {
    execution_id: Uuid,
    timeout_secs: u64,
}

fn execute(
    config: &WasmConfig,
    tools: &Path,
    dir: &Path,
    argv: &[String],
    env: BTreeMap<String, String>,
    control: ExecutionControl,
) -> Result<Execution, WasmRequestError> {
    let ExecutionControl {
        execution_id,
        timeout_secs,
    } = control;
    let cancellation = CancellationFile(dir.join(format!(".cancel-{execution_id}")));
    if cancellation.0.try_exists()? {
        return Ok(Execution {
            execution_id,
            exit_code: 130,
            timed_out: false,
            cancelled: true,
            stdout: String::new(),
            stderr: String::new(),
            elapsed_ms: 0,
            supervisor_pid: std::process::id(),
            runtime_pid: 0,
        });
    }
    let mut command = Command::new("/usr/bin/prlimit");
    command
        .args([
            "--fsize=1048576",
            "--cpu=120",
            "--nofile=256",
            "--nproc=1024",
            "--",
        ])
        .arg(tools.join("bin/wasmtime"))
        .args([
            "run",
            "-C",
            "cache=n",
            "-O",
            "memory-init-cow=n",
            "-S",
            "inherit-network=n,allow-ip-name-lookup=n,tcp=n,udp=n,inherit-env=n,cwd=/workspace",
            "-W",
            "max-memory-size=1073741824",
        ])
        .arg("--dir")
        .arg(format!("{}::/workspace", dir.join("workspace").display()))
        .arg("--dir")
        .arg(format!("{}::/tmp", dir.join("tmp").display()))
        .env_clear()
        .env("HOME", dir)
        .env("PATH", "/usr/bin:/bin")
        .env("TOKIO_WORKER_THREADS", "2")
        .env("RAYON_NUM_THREADS", "2")
        .current_dir(dir)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0);
    for (guest, host) in &config.preopens {
        command
            .arg("--dir")
            .arg(format!("{}::{guest}", host.display()));
    }
    for (key, value) in env {
        command.arg("--env").arg(format!("{key}={value}"));
    }
    // No shell, native executable fallback, host env inheritance, or network.
    command.arg(&config.module).args(argv);
    let owner = nix::unistd::getpid();
    // SAFETY: the child runs only allocation-free Linux syscalls before exec.
    // Arm before checking getppid so parent loss on either side of setup cannot
    // leave an unowned guest. The dedicated caller remains the spawning thread.
    unsafe {
        command.pre_exec(move || {
            nix::sys::prctl::set_pdeathsig(Signal::SIGKILL).map_err(std::io::Error::from)?;
            if nix::unistd::getppid() != owner {
                return Err(std::io::Error::from_raw_os_error(nix::libc::ESRCH));
            }
            Ok(())
        });
    }
    let started = Instant::now();
    let mut child = RuntimeChild(command.spawn()?);
    let runtime_pid = child.0.id();
    let (limit_tx, limit_rx) = std::sync::mpsc::sync_channel(2);
    let stdout = capture_output(
        child
            .0
            .stdout
            .take()
            .ok_or(WasmRequestError::Invalid("missing stdout pipe"))?,
        limit_tx.clone(),
    )?;
    let stderr = capture_output(
        child
            .0
            .stderr
            .take()
            .ok_or(WasmRequestError::Invalid("missing stderr pipe"))?,
        limit_tx,
    )?;
    let (mut exit_code, timed_out, cancelled) = loop {
        if cancellation.0.try_exists()? {
            break (130, false, true);
        }
        if limit_rx.try_recv().is_ok() {
            break (125, false, false);
        }
        if let Some(status) = child.0.try_wait()? {
            break (
                status
                    .code()
                    .unwrap_or_else(|| 128 + status.signal().unwrap_or(0)),
                false,
                false,
            );
        }
        if started.elapsed() >= Duration::from_secs(timeout_secs) {
            break (124, true, false);
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    // Kill guest forks even when their parent exited normally before collecting
    // output, releasing the global lock, or allowing file operations/deletion.
    drop(child);
    let stdout = stdout
        .join()
        .map_err(|_| WasmRequestError::CapturePanic)??;
    let stderr = stderr
        .join()
        .map_err(|_| WasmRequestError::CapturePanic)??;
    // A fast guest can exit before its collectors report overflow.
    if limit_rx.try_recv().is_ok() {
        exit_code = 125;
    }
    Ok(Execution {
        execution_id,
        exit_code,
        timed_out,
        cancelled,
        stdout,
        stderr,
        elapsed_ms: started.elapsed().as_millis(),
        supervisor_pid: std::process::id(),
        runtime_pid,
    })
}

fn capture_output(
    reader: impl Read + Send + 'static,
    limit: std::sync::mpsc::SyncSender<()>,
) -> Result<std::thread::JoinHandle<Result<String, std::io::Error>>, std::io::Error> {
    std::thread::Builder::new()
        .name("wasm-output".into())
        .spawn(move || {
            let mut bytes = Vec::new();
            reader.take(MAX_BYTES + 1).read_to_end(&mut bytes)?;
            if bytes.len() as u64 > MAX_BYTES {
                // The receiver may already have observed the other stream's
                // overflow and stopped the runtime.
                if limit.send(()).is_err() {
                    tracing::debug!("runtime output-limit receiver already closed");
                }
                bytes.truncate(MAX_BYTES as usize);
            }
            Ok(String::from_utf8_lossy(&bytes).into_owned())
        })
}

#[cfg(test)]
mod tests;
