use super::*;

use std::fs::Metadata;
use std::os::unix::fs::FileTypeExt as _;

use crate::proto::{
    CaptureSandboxEndpointProofRequest, ProcessEndpointIdentity, SandboxEndpointProof,
    ValidateSandboxEndpointProofRequest, ValidateSandboxEndpointProofResponse,
    VsockEndpointIdentity,
};
use crate::service::disk::read_pid_from_symlink;
use crate::service::validation::validate_sandbox_id;

const MAX_INFLIGHT_ENDPOINT_ATTESTATIONS: usize = 32;
const ENDPOINT_ATTESTATION_ADMISSION_TIMEOUT: std::time::Duration =
    std::time::Duration::from_millis(250);
const UNIX_SOCKET_ACCEPTCON_FLAG: &str = "00010000";
const UNIX_SOCKET_LISTENING_STATE: &str = "01";
static ENDPOINT_ATTESTATION_LIMIT: std::sync::LazyLock<std::sync::Arc<tokio::sync::Semaphore>> =
    std::sync::LazyLock::new(|| {
        std::sync::Arc::new(tokio::sync::Semaphore::new(
            MAX_INFLIGHT_ENDPOINT_ATTESTATIONS,
        ))
    });

#[derive(Debug, thiserror::Error)]
enum EndpointProofError {
    #[error("inspect {subject}: {source}")]
    Inspect {
        subject: String,
        #[source]
        source: std::io::Error,
    },
    #[error("{0}")]
    InvalidProof(String),
    #[error("{0}")]
    MalformedHostState(String),
    #[error("{0}")]
    Mismatch(String),
}

#[derive(Clone, Copy)]
enum EndpointAttestationPhase {
    Capture,
    Validate,
}

#[derive(Clone)]
struct RegisteredEndpointRuntime {
    sandbox_id: String,
    sandbox_uid: u32,
    guest_cid: u32,
    spaced_host_pid: u32,
    runtime_pid: u32,
    runtime_kind: pplx_space_common_core::types::RuntimeKind,
    vsock_path: PathBuf,
}

impl NodeManagerService {
    pub(super) async fn validate_gateway_caller(
        &self,
        request: Request<crate::proto::ValidateGatewayCallerRequest>,
    ) -> Result<Response<crate::proto::ValidateGatewayCallerResponse>, Status> {
        self.authorize_peer(
            &request,
            "ValidateGatewayCaller",
            ALLOW_NODE_ADMIN_OR_SBXMGR,
        )?;
        let request = request.into_inner();
        let sandbox_id: SandboxId = request
            .sandbox_id
            .parse()
            .map_err(|_| Status::invalid_argument("invalid sandbox ID"))?;
        let lease = self
            .pool
            .get(&request.sandbox_id)
            .map_err(slot_store_status)?
            .ok_or_else(|| Status::permission_denied("gateway has no registered sandbox lease"))?;
        let expected_uid = pplx_space_common_core::host::linux::gateway::gateway_uid(
            pplx_space_common_core::types::LinuxUid::new(lease.sandbox_uid),
        );
        if expected_uid.map(|uid| uid.get()) != Some(request.uid)
            || request.launch_capability.len() != 64
            || !request.launch_capability.iter().all(u8::is_ascii_hexdigit)
        {
            return Err(Status::permission_denied(
                "gateway launch identity is not isolated",
            ));
        }
        run_endpoint_attestation(EndpointAttestationPhase::Validate, move || {
            crate::gateway_launch::validate(
                sandbox_id,
                request.pid,
                request.uid,
                &request.launch_capability,
            )
            .map_err(|_| {
                EndpointProofError::Mismatch(
                    "gateway launch evidence does not match; cycle the gateway".into(),
                )
            })
        })
        .await?;
        Ok(Response::new(
            crate::proto::ValidateGatewayCallerResponse {},
        ))
    }

    pub(super) async fn capture_sandbox_endpoint_proof(
        &self,
        request: Request<CaptureSandboxEndpointProofRequest>,
    ) -> Result<Response<SandboxEndpointProof>, Status> {
        self.authorize_peer(
            &request,
            "CaptureSandboxEndpointProof",
            ALLOW_NODE_ADMIN_OR_SBXMGR,
        )?;
        let runtime = self
            .registered_endpoint_runtime(&request.get_ref().sandbox_id)
            .await?;
        let proof = run_endpoint_attestation(EndpointAttestationPhase::Capture, move || {
            capture_endpoint_proof(&runtime)
        })
        .await?;
        Ok(Response::new(proof))
    }

    pub(super) async fn validate_sandbox_endpoint_proof(
        &self,
        request: Request<ValidateSandboxEndpointProofRequest>,
    ) -> Result<Response<ValidateSandboxEndpointProofResponse>, Status> {
        self.authorize_peer(
            &request,
            "ValidateSandboxEndpointProof",
            ALLOW_NODE_ADMIN_OR_SBXMGR,
        )?;
        let proof = request
            .get_ref()
            .proof
            .clone()
            .ok_or_else(|| Status::invalid_argument("endpoint proof is missing"))?;
        let runtime = self.registered_endpoint_runtime(&proof.sandbox_id).await?;
        run_endpoint_attestation(EndpointAttestationPhase::Validate, move || {
            validate_endpoint_proof(&proof, &runtime)
        })
        .await?;
        Ok(Response::new(ValidateSandboxEndpointProofResponse {}))
    }

    async fn registered_endpoint_runtime(
        &self,
        sandbox_id: &str,
    ) -> Result<RegisteredEndpointRuntime, Status> {
        validate_sandbox_id(sandbox_id)?;
        let runtime_kind = self
            .recorded_execution(sandbox_id.to_owned())
            .await?
            .map_or(
                pplx_space_common_core::types::RuntimeKind::Firecracker,
                |execution| execution.runtime().kind,
            );
        let vsock_path =
            pplx_space_node_runtime_core::control::guest_socket_path(runtime_kind, sandbox_id)
                .map_err(|error| Status::failed_precondition(error.to_string()))?;
        let slot = self
            .pool
            .get(sandbox_id)
            .map_err(slot_store_status)?
            .ok_or_else(|| Status::not_found("sandbox network lease is not registered"))?;
        let runtime_pid = read_pid_from_symlink(&format!("{RUN_DIR}/{sandbox_id}/fc.pid"))
            .ok_or_else(|| Status::unavailable("sandbox runtime pid is not registered"))?;
        let spaced_host_pid =
            read_pid_from_symlink(&format!("{RUN_DIR}/{sandbox_id}/space-spaced-host-svc.pid"))
                .ok_or_else(|| Status::unavailable("spaced-host pid is not registered"))?;
        Ok(RegisteredEndpointRuntime {
            sandbox_id: sandbox_id.to_string(),
            sandbox_uid: slot.sandbox_uid,
            guest_cid: slot.slot.checked_add(3).ok_or_else(|| {
                Status::failed_precondition("sandbox slot has no valid guest CID")
            })?,
            spaced_host_pid,
            runtime_pid,
            runtime_kind,
            vsock_path,
        })
    }
}

async fn run_endpoint_attestation<T>(
    phase: EndpointAttestationPhase,
    task: impl FnOnce() -> Result<T, EndpointProofError> + Send + 'static,
) -> Result<T, Status>
where
    T: Send + 'static,
{
    run_endpoint_attestation_with_limit(
        std::sync::Arc::clone(&ENDPOINT_ATTESTATION_LIMIT),
        phase,
        task,
    )
    .await
}

async fn run_endpoint_attestation_with_limit<T>(
    limit: std::sync::Arc<tokio::sync::Semaphore>,
    phase: EndpointAttestationPhase,
    task: impl FnOnce() -> Result<T, EndpointProofError> + Send + 'static,
) -> Result<T, Status>
where
    T: Send + 'static,
{
    run_endpoint_attestation_with_limit_and_timeout(
        limit,
        ENDPOINT_ATTESTATION_ADMISSION_TIMEOUT,
        phase,
        task,
    )
    .await
}

async fn run_endpoint_attestation_with_limit_and_timeout<T>(
    limit: std::sync::Arc<tokio::sync::Semaphore>,
    admission_timeout: std::time::Duration,
    phase: EndpointAttestationPhase,
    task: impl FnOnce() -> Result<T, EndpointProofError> + Send + 'static,
) -> Result<T, Status>
where
    T: Send + 'static,
{
    let permit = tokio::time::timeout(admission_timeout, limit.acquire_owned())
        .await
        .map_err(|_| Status::unavailable("endpoint attestation capacity is saturated"))?
        .map_err(|_| Status::unavailable("endpoint attestation worker is shutting down"))?;
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        task()
    })
    .await
    .map_err(|error| Status::internal(format!("endpoint proof worker failed: {error}")))?
    .map_err(|error| endpoint_proof_status(error, phase))
}

fn capture_endpoint_proof(
    runtime: &RegisteredEndpointRuntime,
) -> Result<SandboxEndpointProof, EndpointProofError> {
    validate_registered_processes(runtime)?;
    let (runtime_executable, runtime_vsock_path) =
        pplx_space_node_runtime_core::control::guest_endpoint_identity(runtime.runtime_kind)
            .map_err(|error| EndpointProofError::Mismatch(error.to_string()))?;
    capture_endpoint_proof_host_state(
        runtime,
        "space-spaced-host-svc",
        runtime_executable,
        runtime_vsock_path,
    )
}

fn capture_endpoint_proof_host_state(
    runtime: &RegisteredEndpointRuntime,
    spaced_host_executable: &str,
    runtime_executable: &str,
    runtime_vsock_path: &Path,
) -> Result<SandboxEndpointProof, EndpointProofError> {
    let spaced_host = capture_process(
        runtime.spaced_host_pid,
        runtime.sandbox_uid,
        spaced_host_executable,
    )?;
    let runtime_process =
        capture_process(runtime.runtime_pid, runtime.sandbox_uid, runtime_executable)?;
    let socket_metadata = socket_metadata(&runtime.vsock_path)?;
    if socket_metadata.uid() != runtime.sandbox_uid {
        return Err(EndpointProofError::Mismatch(format!(
            "vsock endpoint owner mismatch: expected uid={}, got uid={}",
            runtime.sandbox_uid,
            socket_metadata.uid()
        )));
    }
    let (runtime_fd, runtime_socket_inode) =
        find_owning_descriptor(runtime.runtime_pid, runtime_vsock_path)?;
    Ok(SandboxEndpointProof {
        sandbox_id: runtime.sandbox_id.clone(),
        spaced_host: Some(spaced_host),
        runtime_process: Some(runtime_process),
        vsock: Some(VsockEndpointIdentity {
            path: runtime.vsock_path.to_string_lossy().into_owned(),
            device: socket_metadata.dev(),
            inode: socket_metadata.ino(),
            owner_uid: socket_metadata.uid(),
            owner_gid: socket_metadata.gid(),
            guest_cid: runtime.guest_cid,
            runtime_fd,
            runtime_socket_inode,
        }),
    })
}

fn validate_endpoint_proof(
    proof: &SandboxEndpointProof,
    runtime: &RegisteredEndpointRuntime,
) -> Result<(), EndpointProofError> {
    validate_registered_processes(runtime)?;
    let (runtime_executable, runtime_vsock_path) =
        pplx_space_node_runtime_core::control::guest_endpoint_identity(runtime.runtime_kind)
            .map_err(|error| EndpointProofError::Mismatch(error.to_string()))?;
    validate_endpoint_proof_host_state(
        proof,
        runtime,
        "space-spaced-host-svc",
        runtime_executable,
        runtime_vsock_path,
    )
}

fn validate_endpoint_proof_host_state(
    proof: &SandboxEndpointProof,
    runtime: &RegisteredEndpointRuntime,
    spaced_host_executable: &str,
    runtime_executable: &str,
    runtime_vsock_path: &Path,
) -> Result<(), EndpointProofError> {
    let spaced_host = proof.spaced_host.as_ref().ok_or_else(|| {
        EndpointProofError::InvalidProof("spaced-host proof is missing".to_string())
    })?;
    let runtime_process = proof
        .runtime_process
        .as_ref()
        .ok_or_else(|| EndpointProofError::InvalidProof("runtime proof is missing".to_string()))?;
    let vsock = proof
        .vsock
        .as_ref()
        .ok_or_else(|| EndpointProofError::InvalidProof("vsock proof is missing".to_string()))?;
    if proof.sandbox_id != runtime.sandbox_id
        || spaced_host.pid != runtime.spaced_host_pid
        || runtime_process.pid != runtime.runtime_pid
        || vsock.path != runtime.vsock_path.to_string_lossy()
        || vsock.owner_uid != runtime.sandbox_uid
        || vsock.guest_cid != runtime.guest_cid
    {
        return Err(EndpointProofError::Mismatch(
            "endpoint proof does not match the registered sandbox runtime".to_string(),
        ));
    }

    validate_process(spaced_host, runtime.sandbox_uid, spaced_host_executable)?;
    validate_process(runtime_process, runtime.sandbox_uid, runtime_executable)?;
    let socket_metadata = socket_metadata(&runtime.vsock_path)?;
    if socket_metadata.dev() != vsock.device
        || socket_metadata.ino() != vsock.inode
        || socket_metadata.uid() != vsock.owner_uid
        || socket_metadata.gid() != vsock.owner_gid
    {
        return Err(EndpointProofError::Mismatch(
            "vsock endpoint identity changed after activation".to_string(),
        ));
    }
    let runtime_socket_inode = unix_socket_inode_for_path(
        &PathBuf::from(format!("/proc/{}/net/unix", runtime.runtime_pid)),
        runtime_vsock_path,
    )?;
    if runtime_socket_inode != vsock.runtime_socket_inode {
        return Err(EndpointProofError::Mismatch(
            "vsock path no longer names the runtime-owned socket".to_string(),
        ));
    }
    let descriptor_path = PathBuf::from(format!(
        "/proc/{}/fd/{}",
        runtime.runtime_pid, vsock.runtime_fd
    ));
    let descriptor_target = std::fs::read_link(&descriptor_path).map_err(|source| {
        if source.kind() == std::io::ErrorKind::NotFound {
            EndpointProofError::Mismatch(
                "sandbox runtime no longer owns the activation vsock descriptor".to_string(),
            )
        } else {
            EndpointProofError::Inspect {
                subject: descriptor_path.display().to_string(),
                source,
            }
        }
    })?;
    if parse_socket_descriptor_inode(&descriptor_target) != Some(vsock.runtime_socket_inode) {
        return Err(EndpointProofError::Mismatch(
            "sandbox runtime no longer owns the activation vsock descriptor".to_string(),
        ));
    }
    Ok(())
}

fn validate_registered_processes(
    runtime: &RegisteredEndpointRuntime,
) -> Result<(), EndpointProofError> {
    let sidecar_processes = crate::cgroup::sidecar_cgroup_process_ids(&runtime.sandbox_id)
        .map_err(|source| EndpointProofError::Inspect {
            subject: "sandbox sidecar cgroup".to_string(),
            source,
        })?;
    if !sidecar_processes.contains(&runtime.spaced_host_pid) {
        return Err(EndpointProofError::Mismatch(
            "spaced-host is not in the registered sandbox cgroup".to_string(),
        ));
    }
    let vm_processes =
        crate::cgroup::vm_cgroup_process_ids(&runtime.sandbox_id).map_err(|source| {
            EndpointProofError::Inspect {
                subject: "sandbox runtime cgroup".to_string(),
                source,
            }
        })?;
    if !vm_processes.contains(&runtime.runtime_pid) {
        return Err(EndpointProofError::Mismatch(
            "sandbox runtime is not in the registered cgroup".to_string(),
        ));
    }
    validate_spaced_host_arguments(runtime)?;
    Ok(())
}

fn validate_spaced_host_arguments(
    runtime: &RegisteredEndpointRuntime,
) -> Result<(), EndpointProofError> {
    let pid = runtime.spaced_host_pid;
    let path = PathBuf::from(format!("/proc/{pid}/cmdline"));
    let bytes = std::fs::read(&path).map_err(|source| EndpointProofError::Inspect {
        subject: path.display().to_string(),
        source,
    })?;
    if !spaced_host_arguments_match(&bytes, runtime) {
        return Err(EndpointProofError::Mismatch(format!(
            "process {pid} is not configured for the registered sandbox runtime"
        )));
    }
    Ok(())
}

fn spaced_host_arguments_match(bytes: &[u8], runtime: &RegisteredEndpointRuntime) -> bool {
    process_arguments_include(bytes, "--vsock-uds", &runtime.vsock_path.to_string_lossy())
        && process_arguments_include(bytes, "--firecracker-pid", &runtime.runtime_pid.to_string())
        && process_arguments_include(bytes, "--firecracker-uid", &runtime.sandbox_uid.to_string())
}

fn process_arguments_include(bytes: &[u8], flag: &str, expected_value: &str) -> bool {
    let mut arguments = bytes.split(|byte| *byte == 0).collect::<Vec<_>>();
    if bytes.ends_with(&[0]) {
        arguments.pop();
    }
    arguments.windows(2).any(|pair| {
        matches!(
            pair,
            [observed_flag, observed_value]
                if *observed_flag == flag.as_bytes()
                    && *observed_value == expected_value.as_bytes()
        )
    })
}

fn capture_process(
    pid: u32,
    expected_uid: u32,
    executable_name: &str,
) -> Result<ProcessEndpointIdentity, EndpointProofError> {
    let proc_dir = PathBuf::from(format!("/proc/{pid}"));
    let proc_metadata =
        std::fs::metadata(&proc_dir).map_err(|source| EndpointProofError::Inspect {
            subject: format!("process {pid}"),
            source,
        })?;
    if proc_metadata.uid() != expected_uid {
        return Err(EndpointProofError::Mismatch(format!(
            "process {pid} owner mismatch: expected uid={expected_uid}, got uid={}",
            proc_metadata.uid()
        )));
    }
    let executable_path = proc_dir.join("exe");
    let executable_target =
        std::fs::read_link(&executable_path).map_err(|source| EndpointProofError::Inspect {
            subject: executable_path.display().to_string(),
            source,
        })?;
    let observed_executable = executable_target
        .file_name()
        .and_then(|name| name.to_str())
        .map(|name| name.strip_suffix(" (deleted)").unwrap_or(name));
    if observed_executable != Some(executable_name) {
        return Err(EndpointProofError::Mismatch(format!(
            "process {pid} does not run {executable_name}"
        )));
    }
    let executable =
        std::fs::metadata(&executable_path).map_err(|source| EndpointProofError::Inspect {
            subject: executable_path.display().to_string(),
            source,
        })?;
    let stat = std::fs::read_to_string(proc_dir.join("stat")).map_err(|source| {
        EndpointProofError::Inspect {
            subject: format!("process {pid} stat"),
            source,
        }
    })?;
    Ok(ProcessEndpointIdentity {
        pid,
        start_time_ticks: parse_proc_stat_start_time(&stat)?,
        executable_device: executable.dev(),
        executable_inode: executable.ino(),
    })
}

fn validate_process(
    proof: &ProcessEndpointIdentity,
    expected_uid: u32,
    executable_name: &str,
) -> Result<(), EndpointProofError> {
    let current = capture_process(proof.pid, expected_uid, executable_name)?;
    if current != *proof {
        return Err(EndpointProofError::Mismatch(format!(
            "{executable_name} process identity changed after activation"
        )));
    }
    Ok(())
}

fn socket_metadata(path: &Path) -> Result<Metadata, EndpointProofError> {
    let metadata =
        std::fs::symlink_metadata(path).map_err(|source| EndpointProofError::Inspect {
            subject: path.display().to_string(),
            source,
        })?;
    if !metadata.file_type().is_socket() {
        return Err(EndpointProofError::Mismatch(format!(
            "{} is not a Unix socket",
            path.display()
        )));
    }
    Ok(metadata)
}

fn find_owning_descriptor(
    pid: u32,
    runtime_vsock_path: &Path,
) -> Result<(u32, u64), EndpointProofError> {
    find_owning_descriptor_in_proc_root(Path::new("/proc"), pid, runtime_vsock_path)
}

fn find_owning_descriptor_in_proc_root(
    proc_root: &Path,
    pid: u32,
    runtime_vsock_path: &Path,
) -> Result<(u32, u64), EndpointProofError> {
    let runtime_socket_inode = unix_socket_inode_for_path(
        &proc_root.join(pid.to_string()).join("net/unix"),
        runtime_vsock_path,
    )?;
    let fd_dir = proc_root.join(pid.to_string()).join("fd");
    let entries = std::fs::read_dir(&fd_dir).map_err(|source| EndpointProofError::Inspect {
        subject: fd_dir.display().to_string(),
        source,
    })?;
    for entry in entries {
        let entry = entry.map_err(|source| EndpointProofError::Inspect {
            subject: fd_dir.display().to_string(),
            source,
        })?;
        let Ok(fd) = entry.file_name().to_string_lossy().parse::<u32>() else {
            continue;
        };
        let Ok(target) = std::fs::read_link(entry.path()) else {
            continue;
        };
        let Some(socket_inode) = parse_socket_descriptor_inode(&target) else {
            continue;
        };
        if socket_inode == runtime_socket_inode {
            return Ok((fd, socket_inode));
        }
    }
    Err(EndpointProofError::Mismatch(
        "sandbox runtime does not own the configured vsock endpoint".to_string(),
    ))
}

fn unix_socket_inode_for_path(
    proc_path: &Path,
    expected_path: &Path,
) -> Result<u64, EndpointProofError> {
    let contents =
        std::fs::read_to_string(proc_path).map_err(|source| EndpointProofError::Inspect {
            subject: proc_path.display().to_string(),
            source,
        })?;
    let expected_path = expected_path.to_string_lossy();
    let inodes = contents
        .lines()
        .filter_map(|line| {
            let fields = line.split_whitespace().collect::<Vec<_>>();
            (fields.get(3).copied() == Some(UNIX_SOCKET_ACCEPTCON_FLAG)
                && fields.get(5).copied() == Some(UNIX_SOCKET_LISTENING_STATE)
                && fields.get(7).copied() == Some(expected_path.as_ref()))
            .then(|| fields.get(6)?.parse().ok())
            .flatten()
        })
        .collect::<std::collections::HashSet<_>>();
    match inodes.len() {
        0 => Err(EndpointProofError::Mismatch(format!(
            "no listening Unix socket is registered for {expected_path}"
        ))),
        1 => Ok(*inodes.iter().next().expect("one inode is present")),
        _ => Err(EndpointProofError::Mismatch(format!(
            "multiple listening Unix sockets are registered for {expected_path}"
        ))),
    }
}

fn parse_socket_descriptor_inode(target: &Path) -> Option<u64> {
    let target = target.to_str()?;
    target
        .strip_prefix("socket:[")?
        .strip_suffix(']')?
        .parse()
        .ok()
}

fn parse_proc_stat_start_time(stat: &str) -> Result<u64, EndpointProofError> {
    let close_paren = stat.rfind(')').ok_or_else(|| {
        EndpointProofError::MalformedHostState(
            "process stat is missing command terminator".to_string(),
        )
    })?;
    stat.get(close_paren + 1..)
        .into_iter()
        .flat_map(str::split_whitespace)
        .nth(19)
        .ok_or_else(|| {
            EndpointProofError::MalformedHostState(
                "process stat is missing start-time field".to_string(),
            )
        })?
        .parse()
        .map_err(|_| {
            EndpointProofError::MalformedHostState(
                "process stat start-time field is invalid".to_string(),
            )
        })
}

fn endpoint_proof_status(error: EndpointProofError, phase: EndpointAttestationPhase) -> Status {
    match &error {
        EndpointProofError::Inspect { source, .. }
            if source.kind() == std::io::ErrorKind::PermissionDenied =>
        {
            Status::internal(error.to_string())
        }
        EndpointProofError::Inspect { source, .. }
            if source.kind() == std::io::ErrorKind::NotFound
                && matches!(phase, EndpointAttestationPhase::Validate) =>
        {
            Status::failed_precondition(error.to_string())
        }
        EndpointProofError::Inspect { .. } => Status::unavailable(error.to_string()),
        EndpointProofError::InvalidProof(_) => Status::invalid_argument(error.to_string()),
        EndpointProofError::MalformedHostState(_) => Status::internal(error.to_string()),
        EndpointProofError::Mismatch(_) => Status::failed_precondition(error.to_string()),
    }
}

#[cfg(test)]
mod tests;
