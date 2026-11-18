//! Standalone runtime admission. VM control and boot configuration remain owned
//! by the node-manager adapter; WASI boot inputs come from the admitted catalog.
use super::*;
use pplx_space_execution_spec::{EnvironmentReference, RuntimeKind};
use pplx_space_node_runtime_core::{
    artifact_store::release_directory_names,
    authenticated_source::SourcePins,
    catalog::{CompiledEnvironment, compiled_verity_digest},
    proto::{ArtifactIntegrityRequirement, LaunchRuntimeRequest},
    runtime_inventory::host_arch,
};
use std::path::Path;

#[cfg(all(target_os = "linux", feature = "experimental-wasm"))]
mod wasi;

impl RuntimeSupervisorService {
    pub(super) async fn launch_standalone_owned(
        &self,
        req: LaunchRuntimeRequest,
        caller: Option<launch::ProcessHandle>,
    ) -> Result<Response<LaunchRuntimeResponse>, Status> {
        let sandbox_id = parse_sandbox_id(&req.sandbox_id)?;
        let input = req.verified_runtime.as_ref().ok_or_else(|| {
            Status::failed_precondition(
                "standalone runtime requires authenticated immutable sources",
            )
        })?;
        if input.sandbox_id != req.sandbox_id || req.launcher_binary.is_some() {
            return Err(Status::invalid_argument(
                "inconsistent standalone launch identity",
            ));
        }
        if input.integrity_requirement
            != ArtifactIntegrityRequirement::AuthenticatedImmutableSources as i32
        {
            return Err(Status::failed_precondition(
                "unsupported artifact integrity requirement",
            ));
        }
        let reference: EnvironmentReference = input
            .environment_reference
            .parse()
            .map_err(|error| Status::invalid_argument(format!("environment reference: {error}")))?;
        let launch_lock = self.launch_lock((sandbox_id, FIRECRACKER_LAUNCH_LOCK.to_owned()));
        let _launch_guard = await_runtime_lock(launch_lock.lock()).await?;
        let runtime_lock = self.runtime_lock(sandbox_id);
        let _runtime_guard = await_runtime_lock(runtime_lock.read()).await?;
        execution::check_caller(caller.as_ref())?;
        let execution = self
            .check_execution(sandbox_id, &req.execution_id, None, false)
            .await?;
        let config = standalone_config(&req, sandbox_id, reference)
            .map_err(pplx_space_node_runtime_core::launch_admission::admission_refusal)?;
        if let Some(pid) = live_runtime_pid(&sandbox_id, "fc.pid")? {
            let recorded = self
                .execution_io(move |store| {
                    store
                        .runtime_environment(sandbox_id, execution)
                        .map_err(|error| Status::failed_precondition(error.to_string()))
                })
                .await?;
            // Existing VM readers had no companion binding. Keep that rolling
            // upgrade path; a WASI service has never had an unbound launch path.
            if recorded != Some(reference)
                && (recorded.is_some() || config.runtime == RuntimeKind::Wasmtime)
            {
                return Err(Status::failed_precondition(
                    "resident runtime has no matching environment binding",
                ));
            }
            let process = launch::ProcessHandle::open(pid)
                .map_err(|error| Status::unavailable(format!("pin resident runtime: {error}")))?;
            if process
                .program_matches(resident_program(&config.entrypoint))
                .map_err(|error| Status::unavailable(error.to_string()))?
                != Some(true)
                || process
                    .cgroup_matches(&config.cgroup)
                    .map_err(|error| Status::unavailable(error.to_string()))?
                    != Some(true)
                || process
                    .credentials_match(config.uid, config.gid)
                    .map_err(|error| Status::unavailable(error.to_string()))?
                    != Some(true)
            {
                return Err(Status::failed_precondition(
                    "resident runtime does not match the requested process contract",
                ));
            }
            return Ok(Response::new(LaunchRuntimeResponse {
                pid,
                accepted_environment_reference: reference.to_string(),
            }));
        }
        let limits = req.resource_limits.as_ref().ok_or_else(|| {
            Status::invalid_argument("standalone launch requires resource limits")
        })?;
        cgroup::validate_cgroup_update(limits.vcpu_count, limits.memory_mib, limits.pids_max)
            .map_err(cgroup_update_error_to_status)?;
        let limits =
            CgroupLimits::from_request(limits.vcpu_count, limits.memory_mib, limits.pids_max);
        let budget = limits.cpu_budget().map_err(cgroup_update_error_to_status)?;
        let path = config.executable.clone();
        let pins = config.executable_source;
        let boot_sources: Vec<(std::path::PathBuf, SourcePins)> = match &config.entrypoint {
            crate::contained_launcher::Entrypoint::Runtime => Vec::new(),
            #[cfg(all(target_os = "linux", feature = "experimental-wasm"))]
            crate::contained_launcher::Entrypoint::WasiGuest(guest) => vec![
                (guest.module.clone(), guest.module_source),
                (
                    guest.standard_library.clone(),
                    guest.standard_library_source,
                ),
            ],
        };
        self.execution_io(move |store| {
            // Authenticate before publishing intent. The fresh launcher repeats
            // this check on the descriptor it executes, closing path replacement.
            crate::verified_launch::open_catalog_executable(&path, pins)
                .map_err(pplx_space_node_runtime_core::launch_admission::admission_refusal)?;
            for (path, pins) in boot_sources {
                pplx_space_node_runtime_core::authenticated_source::AuthenticatedFile::open_catalog(
                    &path, pins,
                )
                .map_err(|error| Status::failed_precondition(error.to_string()))?;
            }
            if !cgroup::firecracker_is_quiescent(&sandbox_id.to_string())
                .map_err(|e| Status::unavailable(format!("VM occupancy: {e}")))?
            {
                return Err(Status::failed_precondition(
                    "unidentified VM still occupies resource domain",
                ));
            }
            store
                .record_cpu_budget(sandbox_id, execution, budget)
                .map_err(|e| {
                    Status::failed_precondition(format!("record CPU launch budget: {e}"))
                })?;
            store
                .record_runtime_environment(sandbox_id, execution, reference)
                .map_err(|error| {
                    Status::failed_precondition(format!("record runtime environment: {error}"))
                })?;
            cgroup::create_vm_cgroup(&sandbox_id, &limits)
                .map_err(|e| Status::internal(format!("prepare VM cgroup: {e}")))?;
            Ok(())
        })
        .await?;
        let result = async {
            let permit = self.acquire_blocking_slot().await?;
            let launcher = sidecar_launcher::prepare_contained(config, permit)
                .await
                .map_err(Status::internal)?
                .spawn()
                .map_err(Status::internal)?;
            let pid = launcher.pid;
            let pending = track_pending_sidecar(&self.fork_children, pid)?;
            let runtime_pid =
                tokio::time::timeout(SIDECAR_EXEC_TIMEOUT, launcher.wait_for_contained_exec())
                    .await
                    .map_err(|_| Status::deadline_exceeded("contained runtime exec timed out"))?
                    .map_err(Status::internal)?;
            let runtime_process = launch::ProcessHandle::open(runtime_pid)
                .map_err(|e| Status::unavailable(format!("pin contained runtime: {e}")))?;
            if runtime_process
                .cgroup_matches(
                    &pplx_space_node_runtime_core::cgroup_paths::jailer_cgroup_path(
                        &sandbox_id.to_string(),
                    ),
                )
                .map_err(|error| Status::unavailable(error.to_string()))?
                != Some(true)
            {
                return Err(Status::failed_precondition(
                    "contained runtime left its sandbox resource domain",
                ));
            }
            write_pid_symlink(&sandbox_id, "fc.pid", runtime_pid)?;
            pending.disarm();
            Ok(Response::new(LaunchRuntimeResponse {
                pid: runtime_pid,
                accepted_environment_reference: reference.to_string(),
            }))
        }
        .await;
        if result.is_err()
            && let Err(error) = terminate_sandbox_cgroups(
                sandbox_id,
                Arc::clone(&self.blocking_slots),
                cgroup::CgroupScope::Firecracker,
            )
            .await
        {
            tracing::warn!(%sandbox_id, %error, "contained runtime cleanup pending");
        }
        result
    }
}

fn standalone_config(
    req: &LaunchRuntimeRequest,
    sandbox: SandboxId,
    reference: EnvironmentReference,
) -> Result<crate::contained_launcher::Config, Status> {
    let architecture = host_arch().map_err(|e| Status::failed_precondition(e.to_string()))?;
    let environments = CompiledEnvironment::cached_for_architecture(architecture)
        .map_err(|e| Status::failed_precondition(e.to_string()))?;
    let environment = environments
        .iter()
        .find(|e| e.definition().reference() == reference)
        .ok_or_else(|| Status::failed_precondition("unknown compiled environment reference"))?;
    standalone_config_for_environment(req, sandbox, environment)
}

fn standalone_config_for_environment(
    req: &LaunchRuntimeRequest,
    sandbox: SandboxId,
    environment: &CompiledEnvironment,
) -> Result<crate::contained_launcher::Config, Status> {
    let release = environment.release();
    let runtime = release.release.kind;
    let wire_runtime: pplx_space_execution_common::proto::RuntimeKind = runtime.into();
    if req.runtime != wire_runtime as i32 {
        return Err(Status::invalid_argument(
            "runtime differs from admitted environment",
        ));
    }
    let executable = match runtime {
        RuntimeKind::CloudHypervisor => "bin/cloud-hypervisor",
        #[cfg(all(target_os = "linux", feature = "experimental-wasm"))]
        RuntimeKind::Wasmtime => "bin/wasmtime",
        _ => {
            return Err(Status::failed_precondition(
                "environment does not select an implemented standalone runtime",
            ));
        }
    };
    let binary = Path::new(&req.runtime_binary);
    let root = binary
        .parent()
        .and_then(Path::parent)
        .ok_or_else(|| Status::invalid_argument("runtime source has no release directory"))?;
    let directories = release_directory_names(&release.release, release.digest());
    if pplx_space_common_core::types::NodeSpacePath::parse(root).is_err()
        || !directories
            .iter()
            .any(|directory| root.file_name() == Some(std::ffi::OsStr::new(directory)))
        || binary != root.join(executable)
    {
        return Err(Status::invalid_argument(
            "runtime source must name its compiled release directory",
        ));
    }
    let file = release
        .binaries
        .iter()
        .find(|f| f.path == executable)
        .ok_or_else(|| Status::failed_precondition("compiled release lacks runtime executable"))?;
    let digest = compiled_verity_digest(file.sha256)
        .map_err(|e| Status::failed_precondition(e.to_string()))?
        .ok_or_else(|| Status::failed_precondition("runtime executable has no verity pin"))?;
    // The generic wire carries a path, but containment only accepts a validated
    // namespace locator, just like the Firecracker adapter.
    let network_namespace =
        pplx_space_common_core::types::NodeSpacePath::parse(&req.network_namespace)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
    let workspace = Path::new(&req.workspace_root);
    if pplx_space_common_core::types::NodeSpacePath::parse(workspace).is_err() {
        return Err(Status::invalid_argument(
            "workspace must be an absolute canonical path",
        ));
    }
    #[cfg(all(target_os = "linux", feature = "experimental-wasm"))]
    let release_root = root;
    let root = workspace
        .join(runtime.as_str())
        .join(sandbox.to_string())
        .join("root");
    // Paths are locators from node-manager, not executable arguments supplied
    // by a guest. Keep control access private to the VM identity and node-manager.
    if Path::new(&req.console_path) != root.join("console.log") {
        return Err(Status::invalid_argument(
            "runtime output must be inside its isolation root",
        ));
    }
    let entrypoint = match runtime {
        #[cfg(all(target_os = "linux", feature = "experimental-wasm"))]
        RuntimeKind::Wasmtime => wasi::entrypoint(req, environment, release_root, &root)?,
        _ => crate::contained_launcher::Entrypoint::Runtime,
    };
    // The engine is not the resident service in a contained WASI sandbox.
    // Generated code runs in separate, bounded Wasmtime children.
    let arguments = match &entrypoint {
        crate::contained_launcher::Entrypoint::Runtime => vec!["--seccomp".into(), "true".into()],
        #[cfg(all(target_os = "linux", feature = "experimental-wasm"))]
        crate::contained_launcher::Entrypoint::WasiGuest(_) => Vec::new(),
    };
    Ok(crate::contained_launcher::Config {
        runtime,
        entrypoint,
        root,
        registration_directory: pplx_space_host_layout::paths::sandbox_run_dir(
            &sandbox.to_string(),
        )
        .into(),
        network_namespace: network_namespace.as_path().to_owned(),
        executable: binary.to_owned(),
        executable_source: SourcePins {
            sha256: file.sha256,
            verity: digest,
        },
        uid: req.uid,
        gid: req.gid,
        cgroup: pplx_space_node_runtime_core::cgroup_paths::jailer_cgroup_path(
            &sandbox.to_string(),
        ),
        output: req.console_path.clone().into(),
        arguments,
    })
}

fn resident_program(entrypoint: &crate::contained_launcher::Entrypoint) -> &'static str {
    match entrypoint {
        crate::contained_launcher::Entrypoint::Runtime => "cloud-hypervisor",
        #[cfg(all(target_os = "linux", feature = "experimental-wasm"))]
        crate::contained_launcher::Entrypoint::WasiGuest(_) => "space-runtime-supervisor-svc",
    }
}

#[cfg(test)]
mod tests;
