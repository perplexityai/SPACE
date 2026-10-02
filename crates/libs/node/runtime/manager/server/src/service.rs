use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pplx_space_common_core::types::{JobId, RuntimeKind};
use pplx_space_common_svc::{LocalRole, UdsPeerAuthorizer, UdsPeerPrincipal};
use pplx_space_execution_common::proto::{self as execution, ArtifactComponentKind};
use pplx_space_node_runtime_core::runtime_inventory::diagnostic::{
    self, ArtifactKinds, ArtifactScan, DigestCache, ScanLimits, ScanRoots, SystemScanClock,
};
use pplx_space_node_runtime_manager_client::proto::runtime_manager_server::RuntimeManager;
use pplx_space_node_runtime_manager_client::proto::{
    AvailableKernel, CancelOperationRequest, CancelOperationResponse, ExecutionArtifactKind,
    GetOperationRequest, GetOperationResponse, GetStatusRequest, GetStatusResponse,
    InstallKernelRequest, InstallKernelResponse, KernelPresence, ListAvailableKernelsRequest,
    ListAvailableKernelsResponse, ListExecutionArtifactsRequest, ListExecutionArtifactsResponse,
    UninstallKernelRequest, UninstallKernelResponse,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tonic::{Request, Response, Status};

use crate::installs::{
    CancelOperationError, InstallKernelError, Installer, KernelInstall, KernelSource,
    KernelVersion, KernelVersionParseError, LookupOperationError, ReleaseLine,
    ReleaseLineParseError, RuntimeLine, SourceKernel, UninstallKernelError, describe,
};
use crate::protection::{self, AuditIdentity, PathRole};
use crate::run::SERVICE_NAME;

/// Listing, status, and operation lookups are read-only: root and any local
/// role may call them.
const ALLOW_READ: &[UdsPeerPrincipal] = &[
    UdsPeerPrincipal::Root,
    UdsPeerPrincipal::Role(LocalRole::Observer),
];
/// Installing, uninstalling, and cancelling change what is on the node: root
/// and the local operator role, as for the other node services.
const ALLOW_MUTATE: &[UdsPeerPrincipal] = &[
    UdsPeerPrincipal::Root,
    UdsPeerPrincipal::Role(LocalRole::Operator),
];

#[derive(Clone)]
pub struct RuntimeManagerService {
    roots: Arc<ScanRoots>,
    limits: ScanLimits,
    admission_timeout: Duration,
    /// One filesystem walk at a time, listing or status audit. The permit
    /// moves into the blocking walk, so a caller that disconnects cannot start
    /// another walk while its first one is still spending the work budget.
    scan_slot: Arc<Semaphore>,
    digests: Arc<Mutex<DigestCache>>,
    state_dir: PathBuf,
    audit_identity: AuditIdentity,
    peer_authorizer: UdsPeerAuthorizer,
    /// Where available kernels are listed from and installs download from.
    kernel_source: Option<Arc<dyn KernelSource>>,
    /// Present only when the service was started with a kernel install root.
    installer: Option<Installer>,
}

impl RuntimeManagerService {
    pub fn new(
        roots: ScanRoots,
        state_dir: PathBuf,
        limits: ScanLimits,
        admission_timeout: Duration,
    ) -> Self {
        Self {
            roots: Arc::new(roots),
            limits,
            admission_timeout,
            scan_slot: Arc::new(Semaphore::new(1)),
            digests: Arc::new(Mutex::new(DigestCache::default())),
            state_dir,
            audit_identity: AuditIdentity::of_this_process(),
            peer_authorizer: UdsPeerAuthorizer::new_with_default_local_roles(
                SERVICE_NAME.as_str(),
                std::iter::empty(),
            ),
            kernel_source: None,
            installer: None,
        }
    }

    /// List available kernels from `source`.
    pub(crate) fn with_kernel_source(mut self, source: Arc<dyn KernelSource>) -> Self {
        self.kernel_source = Some(source);
        self
    }

    /// Enable kernel installs and uninstalls through `installer`.
    pub(crate) fn with_installer(mut self, installer: Installer) -> Self {
        self.installer = Some(installer);
        self
    }

    fn installer(&self) -> Result<&Installer, Status> {
        self.installer.as_ref().ok_or_else(|| {
            Status::failed_precondition(
                "kernel installs are disabled: the runtime manager was started without a kernel install root",
            )
        })
    }

    fn kernel_source(&self) -> Result<&Arc<dyn KernelSource>, Status> {
        self.kernel_source.as_ref().ok_or_else(|| {
            Status::failed_precondition("the runtime manager has no kernel source configured")
        })
    }

    /// The lines of `runtime` to list kernels for: the named one, or every
    /// line the node has a release directory for. `runtime` must be the
    /// one `source` publishes kernels for.
    async fn runtime_lines(
        &self,
        source: &dyn KernelSource,
        runtime: RuntimeKind,
        requested: Option<&str>,
    ) -> Result<Vec<RuntimeLine>, Status> {
        if runtime != source.runtime() {
            return Err(Status::failed_precondition(format!(
                "the kernel source publishes kernels for {}, not {runtime}",
                source.runtime()
            )));
        }
        if let Some(requested) = requested {
            return parse_line(runtime, requested).map(|line| vec![line]);
        }
        let Some(releases) = self.release_root(runtime) else {
            return Ok(Vec::new());
        };
        tokio::task::spawn_blocking(move || release_lines(runtime, &releases))
            .await
            .map_err(|error| Status::internal(describe(&error)))?
    }

    /// Where `runtime`'s release directories are, if it has a configured
    /// release root.
    fn release_root(&self, runtime: RuntimeKind) -> Option<PathBuf> {
        self.roots
            .runtime_releases
            .iter()
            .find(|root| root.kind == runtime)
            .map(|root| root.path.clone())
    }

    /// Whether `kernel` is on the node. Blocking: reads the install records.
    fn presence(&self, kernel: &SourceKernel, installs: &[KernelInstall]) -> KernelPresence {
        match installs
            .iter()
            .find(|install| install.version == kernel.version)
        {
            Some(install)
                if install.location == kernel.location && kernel.md5 == Some(install.md5) =>
            {
                KernelPresence::Installed
            }
            Some(_) => KernelPresence::VersionPresent,
            None => {
                let root = self
                    .installer
                    .as_ref()
                    .map_or(self.roots.vendor_kernels.as_path(), Installer::kernels_root);
                if std::fs::symlink_metadata(root.join(kernel.version.as_str())).is_ok() {
                    KernelPresence::VersionPresent
                } else {
                    KernelPresence::NotInstalled
                }
            }
        }
    }
}

fn parse_operation_id(value: &str) -> Result<JobId, Status> {
    value
        .parse()
        .map_err(|_| Status::invalid_argument(format!("invalid operation id '{value}'")))
}

/// The runtime a request names; `default` when it names none.
fn parse_runtime(value: i32, default: Option<RuntimeKind>) -> Result<RuntimeKind, Status> {
    let named = execution::RuntimeKind::try_from(value)
        .map_err(|_| Status::invalid_argument(format!("unknown runtime kind {value}")))?;
    if named == execution::RuntimeKind::Unspecified {
        return default.ok_or_else(|| Status::invalid_argument("name the runtime"));
    }
    RuntimeKind::try_from(named).map_err(|error| Status::invalid_argument(error.to_string()))
}

fn parse_line(runtime: RuntimeKind, value: &str) -> Result<RuntimeLine, Status> {
    value
        .parse()
        .map(|release| RuntimeLine::new(runtime, release))
        .map_err(|error: ReleaseLineParseError| Status::invalid_argument(error.to_string()))
}

fn parse_kernel_version(value: &str) -> Result<KernelVersion, Status> {
    value
        .parse()
        .map_err(|error: KernelVersionParseError| Status::invalid_argument(error.to_string()))
}

/// The lines of `runtime`'s release directories under `releases`. Blocking.
fn release_lines(
    runtime: RuntimeKind,
    releases: &std::path::Path,
) -> Result<Vec<RuntimeLine>, Status> {
    let entries = match std::fs::read_dir(releases) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => {
            return Err(Status::internal(format!(
                "list {}: {error}",
                releases.display()
            )));
        }
    };
    let mut lines: Vec<RuntimeLine> = entries
        .filter_map(Result::ok)
        .filter_map(|entry| {
            entry
                .file_name()
                .to_str()
                .and_then(|name| name.parse::<ReleaseLine>().ok())
                .map(|release| RuntimeLine::new(runtime, release))
        })
        .collect();
    lines.sort();
    lines.dedup();
    Ok(lines)
}

fn lookup_status(error: LookupOperationError) -> Status {
    match error {
        LookupOperationError::Worker(status) => *status,
        LookupOperationError::Store(_)
        | LookupOperationError::Payload { .. }
        | LookupOperationError::Status { .. }
        | LookupOperationError::MissingResult { .. }
        | LookupOperationError::Vanished { .. }
        | LookupOperationError::Task(_) => Status::internal(describe(&error)),
    }
}

fn install_status(error: InstallKernelError) -> Status {
    match error {
        InstallKernelError::NotOffered { .. } => Status::not_found(describe(&error)),
        InstallKernelError::RuntimeNotPublished { .. }
        | InstallKernelError::Unverifiable { .. }
        | InstallKernelError::TooLarge { .. }
        | InstallKernelError::Conflict(_) => Status::failed_precondition(describe(&error)),
        InstallKernelError::Source(_) => Status::unavailable(describe(&error)),
        InstallKernelError::Submit(status) => *status,
        InstallKernelError::Lookup(error) => lookup_status(error),
        InstallKernelError::Store(_) | InstallKernelError::Task(_) => {
            Status::internal(describe(&error))
        }
    }
}

fn uninstall_status(error: UninstallKernelError) -> Status {
    match error {
        UninstallKernelError::NotInstalled { .. } => Status::not_found(describe(&error)),
        UninstallKernelError::External { .. }
        | UninstallKernelError::Selected { .. }
        | UninstallKernelError::OutsideRoot { .. } => Status::failed_precondition(describe(&error)),
        UninstallKernelError::Submit(status) => *status,
        UninstallKernelError::Lookup(error) => lookup_status(error),
        UninstallKernelError::Store(_) | UninstallKernelError::Task(_) => {
            Status::internal(describe(&error))
        }
    }
}

fn cancel_status(error: CancelOperationError) -> Status {
    match error {
        CancelOperationError::NotFound(_) => Status::not_found(describe(&error)),
        CancelOperationError::TooLate(_) => Status::failed_precondition(describe(&error)),
        CancelOperationError::Lookup(error) => lookup_status(error),
        CancelOperationError::Worker { source, .. } => *source,
    }
}

/// The scan covers what the request names: every kind when `kinds` is empty,
/// and every component kind when `component_kinds` is empty. Kernels are the
/// only components the scan finds, so a filter naming other component kinds
/// lists no components.
fn parse_kinds(request: &ListExecutionArtifactsRequest) -> Result<ArtifactKinds, Status> {
    let mut runtimes = request.kinds.is_empty();
    let mut components = request.kinds.is_empty();
    for kind in &request.kinds {
        match ExecutionArtifactKind::try_from(*kind) {
            Ok(ExecutionArtifactKind::Runtime) => runtimes = true,
            Ok(ExecutionArtifactKind::Component) => components = true,
            Ok(ExecutionArtifactKind::Unspecified) | Err(_) => {
                return Err(Status::invalid_argument(format!(
                    "unsupported execution artifact kind {kind}"
                )));
            }
        }
    }
    let mut kernels = request.component_kinds.is_empty();
    for kind in &request.component_kinds {
        match ArtifactComponentKind::try_from(*kind) {
            Ok(ArtifactComponentKind::Kernel) => kernels = true,
            Ok(ArtifactComponentKind::Unspecified) | Err(_) => {
                return Err(Status::invalid_argument(format!(
                    "unsupported artifact component kind {kind}"
                )));
            }
            Ok(_) => {}
        }
    }
    Ok(ArtifactKinds {
        runtimes,
        kernels: components && kernels,
    })
}

impl RuntimeManagerService {
    /// Listings and status audits both walk the artifact trees on a blocking
    /// thread. One runs at a time; a caller that cannot get the slot within the
    /// admission timeout is told to retry rather than queued behind it.
    /// Walk the configured roots for `kinds`, marking what the install
    /// records name as space-managed.
    async fn scan(&self, kinds: ArtifactKinds) -> Result<ArtifactScan, Status> {
        let permit = self.admit_walk().await?;
        let roots = self.roots.clone();
        let limits = self.limits;
        let digests = self.digests.clone();
        let installer = self.installer.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            // The cache is a memo of digests, never partially updated in a
            // way that matters; a panicked scan leaves it reusable.
            let mut digests = digests
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let mut scan = diagnostic::scan(&roots, limits, kinds, &SystemScanClock, &mut digests);
            // Ownership comes only from install records, never from where an
            // artifact sits.
            if let Some(installer) = installer {
                let installs = installer
                    .kernel_installs()
                    .map_err(|error| Status::internal(format!("read install records: {error}")))?;
                crate::convert::mark_space_managed(&mut scan, &installs).map_err(|error| {
                    Status::internal(format!("describe an install record's runtime: {error}"))
                })?;
            }
            Ok::<_, Status>(scan)
        })
        .await
        .map_err(|error| Status::internal(format!("artifact scan failed: {error}")))?
    }

    /// Why `version` cannot be uninstalled when the runtime manager has no
    /// install of it: no kernel of that version is on the node, or every
    /// copy is one it did not install and never removes.
    async fn not_installed(&self, version: &KernelVersion) -> Status {
        let scan = match self
            .scan(ArtifactKinds {
                runtimes: false,
                kernels: true,
            })
            .await
        {
            Ok(scan) => scan,
            Err(status) => {
                tracing::warn!(%version, %status, "could not list kernels for an uninstall refusal");
                return Status::not_found(format!(
                    "the runtime manager installed no kernel {version}"
                ));
            }
        };
        let copies: Vec<String> = scan
            .artifacts
            .iter()
            .filter(|artifact| {
                artifact
                    .version
                    .as_ref()
                    .is_some_and(|found| found.to_string() == version.as_str())
            })
            .map(|artifact| artifact.path.display().to_string())
            .collect();
        if copies.is_empty() {
            let incomplete = if scan.is_complete() {
                ""
            } else {
                " (the kernel listing was incomplete)"
            };
            return Status::not_found(format!("no kernel {version} on this node{incomplete}"));
        }
        Status::failed_precondition(format!(
            "kernel {version} is present only where the runtime manager did not install it, and it never removes those: {}",
            copies.join(", ")
        ))
    }

    async fn admit_walk(&self) -> Result<OwnedSemaphorePermit, Status> {
        tokio::time::timeout(
            self.admission_timeout,
            self.scan_slot.clone().acquire_owned(),
        )
        .await
        .map_err(|_| Status::resource_exhausted("another artifact scan is running; retry"))?
        .map_err(|_| Status::unavailable("runtime manager is shutting down"))
    }
}

#[tonic::async_trait]
impl RuntimeManager for RuntimeManagerService {
    async fn list_execution_artifacts(
        &self,
        request: Request<ListExecutionArtifactsRequest>,
    ) -> Result<Response<ListExecutionArtifactsResponse>, Status> {
        self.peer_authorizer
            .authorize_request(&request, "ListExecutionArtifacts", ALLOW_READ)?;
        let kinds = parse_kinds(request.get_ref())?;
        let scan = self.scan(kinds).await?;
        if !scan.is_complete() {
            tracing::warn!(
                issues = scan.issues.len(),
                "execution artifact listing is incomplete"
            );
        }
        Ok(Response::new(crate::convert::list_response(scan)))
    }

    async fn get_status(
        &self,
        request: Request<GetStatusRequest>,
    ) -> Result<Response<GetStatusResponse>, Status> {
        self.peer_authorizer
            .authorize_request(&request, "GetStatus", ALLOW_READ)?;
        // The image-populated install roots are where the runtime manager
        // installs; the cache kernel roots stay root-staged until the runtime
        // manager takes over kernel staging.
        let releases = self
            .roots
            .runtime_releases
            .iter()
            .map(|root| root.path.clone())
            .chain(self.roots.managed_kernels.clone())
            .map(|path| (path, PathRole::InstallRoot));
        let paths: Vec<_> = releases
            .chain([
                (self.roots.vendor_kernels.clone(), PathRole::InstallRoot),
                (self.roots.cache_kernels.clone(), PathRole::RootManagedCache),
                (
                    self.roots.cache_legacy_assets.clone(),
                    PathRole::RootManagedCache,
                ),
                (self.state_dir.clone(), PathRole::StateDirectory),
            ])
            .collect();
        let permit = self.admit_walk().await?;
        let identity = self.audit_identity.clone();
        let reports = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            paths
                .iter()
                .map(|(path, role)| protection::audit(path, *role, &identity))
                .collect::<Vec<_>>()
        })
        .await
        .map_err(|error| Status::internal(format!("protection audit failed: {error}")))?;
        Ok(Response::new(crate::convert::status_response(
            &self.audit_identity,
            reports,
        )))
    }

    async fn list_available_kernels(
        &self,
        request: Request<ListAvailableKernelsRequest>,
    ) -> Result<Response<ListAvailableKernelsResponse>, Status> {
        self.peer_authorizer
            .authorize_request(&request, "ListAvailableKernels", ALLOW_READ)?;
        let source = self.kernel_source()?.clone();
        let architecture = crate::installs::host_arch()
            .ok_or_else(|| Status::failed_precondition("unsupported node architecture"))?;
        let runtime = parse_runtime(request.get_ref().runtime_kind, Some(source.runtime()))?;
        let lines = self
            .runtime_lines(
                source.as_ref(),
                runtime,
                request.get_ref().runtime_version.as_deref(),
            )
            .await?;
        let mut offered = Vec::new();
        for line in &lines {
            offered.extend(
                source
                    .list(*line, architecture)
                    .await
                    .map_err(|error| Status::unavailable(describe(&error)))?,
            );
        }
        let service = self.clone();
        let kernels = tokio::task::spawn_blocking(move || {
            let installs = match &service.installer {
                Some(installer) => installer
                    .kernel_installs()
                    .map_err(|error| Status::internal(format!("read install records: {error}")))?,
                None => Vec::new(),
            };
            Ok::<_, Status>(
                offered
                    .iter()
                    .map(|kernel| AvailableKernel {
                        version: kernel.version.to_string(),
                        runtime_kind: execution::RuntimeKind::from(kernel.line.runtime) as i32,
                        runtime_line: kernel.line.release.to_string(),
                        architecture: kernel.architecture.as_str().to_string(),
                        size: kernel.size,
                        md5: kernel.md5.map(|md5| md5.to_string()),
                        source: kernel.location.to_string(),
                        presence: service.presence(kernel, &installs) as i32,
                    })
                    .collect(),
            )
        })
        .await
        .map_err(|error| Status::internal(format!("kernel listing failed: {error}")))??;
        Ok(Response::new(ListAvailableKernelsResponse {
            kernels,
            runtime_kind: execution::RuntimeKind::from(runtime) as i32,
            runtime_lines: lines.iter().map(|line| line.release.to_string()).collect(),
            source: source.root().to_string(),
        }))
    }

    async fn install_kernel(
        &self,
        request: Request<InstallKernelRequest>,
    ) -> Result<Response<InstallKernelResponse>, Status> {
        self.peer_authorizer
            .authorize_request(&request, "InstallKernel", ALLOW_MUTATE)?;
        let runtime = parse_runtime(request.get_ref().runtime_kind, None)?;
        let line = parse_line(runtime, &request.get_ref().runtime_version)?;
        let version = parse_kernel_version(&request.get_ref().version)?;
        let operation = self
            .installer()?
            .install(line, version)
            .await
            .map_err(install_status)?;
        Ok(Response::new(InstallKernelResponse {
            operation: Some(crate::convert::operation(&operation)),
        }))
    }

    async fn uninstall_kernel(
        &self,
        request: Request<UninstallKernelRequest>,
    ) -> Result<Response<UninstallKernelResponse>, Status> {
        self.peer_authorizer
            .authorize_request(&request, "UninstallKernel", ALLOW_MUTATE)?;
        let version = parse_kernel_version(&request.get_ref().version)?;
        // Removes the runtime manager's own install of the version; copies
        // it did not install, such as a release's bundled kernel, stay.
        let operation = match self.installer()?.uninstall(version).await {
            Ok(operation) => operation,
            Err(UninstallKernelError::NotInstalled { version }) => {
                return Err(self.not_installed(&version).await);
            }
            Err(error) => return Err(uninstall_status(error)),
        };
        Ok(Response::new(UninstallKernelResponse {
            operation: Some(crate::convert::operation(&operation)),
        }))
    }

    async fn get_operation(
        &self,
        request: Request<GetOperationRequest>,
    ) -> Result<Response<GetOperationResponse>, Status> {
        self.peer_authorizer
            .authorize_request(&request, "GetOperation", ALLOW_READ)?;
        let operation_id = parse_operation_id(&request.get_ref().operation_id)?;
        let operation = self
            .installer()?
            .get(operation_id)
            .await
            .map_err(lookup_status)?
            .ok_or_else(|| Status::not_found(format!("no operation {operation_id}")))?;
        Ok(Response::new(GetOperationResponse {
            operation: Some(crate::convert::operation(&operation)),
        }))
    }

    async fn cancel_operation(
        &self,
        request: Request<CancelOperationRequest>,
    ) -> Result<Response<CancelOperationResponse>, Status> {
        self.peer_authorizer
            .authorize_request(&request, "CancelOperation", ALLOW_MUTATE)?;
        let operation_id = parse_operation_id(&request.get_ref().operation_id)?;
        let operation = self
            .installer()?
            .cancel(operation_id)
            .await
            .map_err(cancel_status)?;
        Ok(Response::new(CancelOperationResponse {
            operation: Some(crate::convert::operation(&operation)),
        }))
    }
}

#[cfg(test)]
mod tests;
