//! Experimental non-KVM runtime owned by the node supervisor.
//!
//! This deliberately has no public listener or production scheduling registration.
//! A trusted local caller provides configuration; untrusted commands only reach
//! Wasmtime/WASI. Container cgroups must supply the memory limit. Snapshots, networking,
//! arbitrary OCI images, concurrent execution and production tenancy are absent.
//! Additional preopened directories must be mounted read-only by deployment.
//! The configured module is trusted; guest requests only supply its arguments.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::process::{CommandExt, ExitStatusExt};
use std::path::{Component, Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use nix::sys::signal::{Signal, killpg};
use nix::unistd::Pid;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

const MAX_BYTES: u64 = 1_048_576;

#[derive(Debug, Error)]
pub enum WasmRequestError {
    #[error("Wasm runtime I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid Wasm runtime request: {0}")]
    Json(#[from] serde_json::Error),
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
    Create {},
    Execute {
        sandbox_id: Uuid,
        argv: Vec<String>,
        timeout_secs: u64,
        #[serde(default)]
        env: BTreeMap<String, String>,
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
    Delete {
        sandbox_id: Uuid,
    },
}

#[derive(Serialize)]
struct Response {
    runtime: &'static str,
    sandbox_id: Uuid,
    #[serde(skip_serializing_if = "Option::is_none")]
    execution: Option<Execution>,
    #[serde(skip_serializing_if = "Option::is_none")]
    content: Option<String>,
}

#[derive(Serialize)]
struct Execution {
    exit_code: i32,
    timed_out: bool,
    stdout: String,
    stderr: String,
    elapsed_ms: u128,
    supervisor_pid: u32,
    runtime_pid: u32,
}

/// Single-flight local protocol entrypoint. Synchronous by design: this mode is
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
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(root.join(".lock"))?;
    lock.try_lock()?;
    let mut bytes = Vec::new();
    input.take(MAX_BYTES + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_BYTES {
        return Err(WasmRequestError::Invalid("request exceeds 1 MiB"));
    }
    let request: Request = serde_json::from_slice(&bytes)?;
    let mut response = Response {
        runtime: "wasmtime-wasi",
        sandbox_id: Uuid::nil(),
        execution: None,
        content: None,
    };
    match request {
        Request::Create {} => {
            let id = Uuid::now_v7();
            let dir = root.join(id.to_string());
            fs::create_dir(&dir)?;
            fs::create_dir(dir.join("workspace"))?;
            fs::create_dir(dir.join("tmp"))?;
            response.sandbox_id = id;
        }
        Request::Execute {
            sandbox_id,
            argv,
            timeout_secs,
            env,
        } => {
            let dir = sandbox_dir(&root, sandbox_id)?;
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
            response.execution = Some(execute(&config, &tools, &dir, &argv, timeout_secs, env)?);
        }
        Request::Upload {
            sandbox_id,
            path,
            content,
        } => {
            let dir = sandbox_dir(&root, sandbox_id)?;
            let host_path = guest_path(&dir, &path)?;
            let parent = host_path
                .parent()
                .ok_or(WasmRequestError::Invalid("missing parent"))?;
            fs::create_dir_all(parent)?;
            // Recheck after directory creation. The global lock excludes another
            // runtime request; no guest process survives execute().
            fs::write(guest_path(&dir, &path)?, content)?;
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
            fs::remove_dir_all(sandbox_dir(&root, sandbox_id)?)?;
            response.sandbox_id = sandbox_id;
        }
    }
    serde_json::to_writer(&mut output, &response)?;
    output.write_all(b"\n")?;
    Ok(())
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

fn execute(
    config: &WasmConfig,
    tools: &Path,
    dir: &Path,
    argv: &[String],
    timeout_secs: u64,
    env: BTreeMap<String, String>,
) -> Result<Execution, WasmRequestError> {
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
    let (mut exit_code, timed_out) = loop {
        if limit_rx.try_recv().is_ok() {
            break (125, false);
        }
        if let Some(status) = child.0.try_wait()? {
            break (
                status
                    .code()
                    .unwrap_or_else(|| 128 + status.signal().unwrap_or(0)),
                false,
            );
        }
        if started.elapsed() >= Duration::from_secs(timeout_secs) {
            break (124, true);
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
        exit_code,
        timed_out,
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
