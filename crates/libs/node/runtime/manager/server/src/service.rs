use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::installs::{
    CancelOperationError, Installer, KernelSource, LookupOperationError, ReleaseLine,
    ReleaseLineParseError, RuntimeLine, SubmitOperationError, describe,
};
use crate::protection::{self, AuditIdentity, PathRole};
use crate::run::SERVICE_NAME;
use pplx_space_common_core::host::linux::identities::{user_names, users};
use pplx_space_common_core::types::{
    ComponentIdentity, Digest, JobId, RuntimeIdentity, RuntimeKind, RuntimeVersion,
};
use pplx_space_common_svc::{LocalRole, UdsPeerAccount, UdsPeerAuthorizer, UdsPeerPrincipal};
use pplx_space_execution_common::proto::{self as execution, ArtifactComponentKind};
use pplx_space_node_runtime_core::runtime_inventory::diagnostic::{
    self, ArtifactKinds, ArtifactScan, DigestCache, ScanLimits, ScanRoots, SystemScanClock,
};
use pplx_space_node_runtime_manager_client::proto::runtime_manager_server::RuntimeManager;
use pplx_space_node_runtime_manager_client::proto::{self, *};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tonic::{Request, Response, Status};

/// Listing, status, and operation lookups are read-only: root and any local
/// role, plus node-ctl, may call them.
const ALLOW_READ: &[UdsPeerPrincipal] = &[
    UdsPeerPrincipal::Root,
    UdsPeerPrincipal::Role(LocalRole::Observer),
    UdsPeerPrincipal::Service(user_names::SPACE_NODECTL),
];
/// Destructive operations require root or the local operator role.
const ALLOW_MUTATE: &[UdsPeerPrincipal] = &[
    UdsPeerPrincipal::Root,
    UdsPeerPrincipal::Role(LocalRole::Operator),
];

// Provisioning may add approved artifacts but cannot delete or cancel another
// operator's work. Publication still independently checks node-manager's pins.
const ALLOW_INSTALL: &[UdsPeerPrincipal] = &[
    UdsPeerPrincipal::Root,
    UdsPeerPrincipal::Role(LocalRole::Operator),
    UdsPeerPrincipal::Service(user_names::SPACE_NODECTL),
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
    /// One source listing at a time: each walks a remote bucket, so readers
    /// cannot multiply that work.
    listing_slot: Arc<Semaphore>,
    digests: Arc<Mutex<DigestCache>>,
    state_dir: PathBuf,
    audit_identity: AuditIdentity,
    peer_authorizer: UdsPeerAuthorizer,
    /// Where available kernels are listed from and installs download from.
    kernel_source: Option<Arc<dyn KernelSource>>,
    pins: Option<Arc<dyn crate::installs::PinSource>>,
    releases: Option<crate::installs::ReleaseContext>,
    /// Present unless the service runs list-only.
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
            listing_slot: Arc::new(Semaphore::new(1)),
            digests: Arc::new(Mutex::new(DigestCache::default())),
            state_dir,
            audit_identity: AuditIdentity::of_this_process(),
            peer_authorizer: UdsPeerAuthorizer::new_with_default_local_roles(
                SERVICE_NAME.as_str(),
                [UdsPeerAccount::with_linux_user(users::SPACE_NODECTL)],
            ),
            kernel_source: None,
            installer: None,
            pins: None,
            releases: None,
        }
    }

    /// List available kernels from `source`.
    pub(crate) fn with_kernel_source(mut self, source: Arc<dyn KernelSource>) -> Self {
        self.kernel_source = Some(source);
        self
    }

    /// Read-only catalog and source access is independent of mutation readiness.
    pub(crate) fn with_catalog(
        mut self,
        pins: Arc<dyn crate::installs::PinSource>,
        releases: Option<crate::installs::ReleaseContext>,
    ) -> Self {
        self.pins = Some(pins);
        self.releases = releases;
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
                "artifact management is disabled; inspect status and service logs",
            )
        })
    }

    fn kernel_source(&self) -> Result<&Arc<dyn KernelSource>, Status> {
        self.kernel_source.as_ref().ok_or_else(|| {
            Status::failed_precondition("the runtime manager has no kernel source configured")
        })
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

/// The release a runtime request names. The runtime must be named.
fn parse_release(runtime_kind: i32, version: &str) -> Result<RuntimeIdentity, Status> {
    let kind = parse_runtime(runtime_kind, None)?;
    let version = RuntimeVersion::parse(version.strip_prefix('v').unwrap_or(version))
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    Ok(RuntimeIdentity { kind, version })
}

fn require_kernel(kind: i32) -> Result<(), Status> {
    match ArtifactComponentKind::try_from(kind) {
        Ok(ArtifactComponentKind::Kernel) => Ok(()),
        Ok(ArtifactComponentKind::Unspecified) | Err(_) => {
            Err(Status::invalid_argument("a component kind is required"))
        }
        Ok(_) => Err(Status::failed_precondition(
            "this build only installs kernel components",
        )),
    }
}

fn parse_component(
    value: Option<&pplx_space_node_runtime_manager_client::proto::ComponentIdentity>,
) -> Result<ComponentIdentity, Status> {
    let value =
        value.ok_or_else(|| Status::invalid_argument("a component identity is required"))?;
    require_kernel(value.kind)?;
    Ok(ComponentIdentity {
        kind: pplx_space_common_core::types::ArtifactComponentKind::Kernel,
        version: value
            .version
            .parse()
            .map_err(|_| Status::invalid_argument("invalid component version"))?,
        digest: value
            .digest
            .parse()
            .map_err(|_| Status::invalid_argument("invalid component digest"))?,
    })
}

enum RequestedArtifact {
    Runtime(RuntimeIdentity, Digest),
    Component(ComponentIdentity),
}
fn parse_identity(value: Option<&ArtifactIdentity>) -> Result<RequestedArtifact, Status> {
    match value.and_then(|value| value.artifact.as_ref()) {
        Some(proto::artifact_identity::Artifact::Runtime(runtime)) => {
            Ok(RequestedArtifact::Runtime(
                parse_release(runtime.kind, &runtime.version)?,
                runtime
                    .digest
                    .parse()
                    .map_err(|_| Status::invalid_argument("invalid runtime digest"))?,
            ))
        }
        Some(proto::artifact_identity::Artifact::Component(component)) => {
            parse_component(Some(component)).map(RequestedArtifact::Component)
        }
        None => Err(Status::invalid_argument("artifact identity is required")),
    }
}
fn presence(path: &std::path::Path) -> ArtifactPresence {
    match std::fs::symlink_metadata(path) {
        Ok(_) => ArtifactPresence::Present,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => ArtifactPresence::Absent,
        Err(_) => ArtifactPresence::Unreadable,
    }
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

fn submit_status(error: SubmitOperationError) -> Status {
    match error {
        SubmitOperationError::Disabled
        | SubmitOperationError::NotPinned
        | SubmitOperationError::TooLarge
        | SubmitOperationError::Pins(_) => Status::failed_precondition(describe(&error)),
        SubmitOperationError::NotOffered => Status::not_found(describe(&error)),
        SubmitOperationError::Source(_) => Status::unavailable(describe(&error)),
        SubmitOperationError::Submit(status) => *status,
        SubmitOperationError::Lookup(error) => lookup_status(error),
        SubmitOperationError::Task(_) => Status::internal(describe(&error)),
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
    /// Walk the configured roots for `kinds`, including the separate managed store.
    async fn scan(&self, kinds: ArtifactKinds) -> Result<ArtifactScan, Status> {
        let permit = self.admit_walk().await?;
        let roots = self.roots.clone();
        let limits = self.limits;
        let digests = self.digests.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            // The cache is a memo of digests, never partially updated in a
            // way that matters; a panicked scan leaves it reusable.
            let mut digests = digests
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let mut scan = diagnostic::scan(&roots, limits, kinds, &SystemScanClock, &mut digests);
            crate::convert::mark_managed_store(&mut scan, &roots);
            Ok::<_, Status>(scan)
        })
        .await
        .map_err(|error| Status::internal(format!("artifact scan failed: {error}")))?
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
        // Execution roots are diagnostic only. Managed roots are separate.
        let releases = self
            .roots
            .runtime_releases
            .iter()
            .map(|root| root.path.clone())
            .chain(
                self.roots
                    .managed_releases
                    .as_ref()
                    .map(|root| root.path.clone()),
            )
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

    async fn list_available_artifacts(
        &self,
        request: Request<ListAvailableArtifactsRequest>,
    ) -> Result<Response<ListAvailableArtifactsResponse>, Status> {
        self.peer_authorizer
            .authorize_request(&request, "ListAvailableArtifacts", ALLOW_READ)?;
        let filter = request.into_inner();
        let kinds = parse_kinds(&ListExecutionArtifactsRequest {
            kinds: filter.kinds,
            component_kinds: filter.component_kinds,
        })?;
        let runtime = parse_runtime(filter.runtime_kind, Some(RuntimeKind::Firecracker))?;
        let line = filter
            .runtime_version
            .as_deref()
            .map(|version| parse_line(runtime, version))
            .transpose()?;
        let architecture = crate::installs::host_arch()
            .ok_or_else(|| Status::failed_precondition("unsupported architecture"))?;
        let pins = self
            .pins
            .clone()
            .ok_or_else(|| Status::failed_precondition("no artifact catalog configured"))?;
        let releases = self.releases.clone();
        let kernels_root = self.roots.managed_kernels.clone();
        let permit = tokio::time::timeout(
            self.admission_timeout,
            self.listing_slot.clone().acquire_owned(),
        )
        .await
        .map_err(|_| Status::resource_exhausted("another catalog listing is running; retry"))?
        .map_err(|_| Status::unavailable("runtime manager is shutting down"))?;
        let catalog = tokio::task::spawn_blocking(move || pins.catalog())
            .await
            .map_err(|error| Status::internal(describe(&error)))?
            .map_err(|error| Status::failed_precondition(describe(&error)))?;
        let mut offered = Vec::new();
        if kinds.kernels {
            let source = self.kernel_source()?;
            if source.runtime() != runtime {
                return Err(Status::failed_precondition(
                    "no component adapter for this runtime",
                ));
            }
            let mut lines = match line {
                Some(line) => vec![line],
                None => catalog
                    .all_kernels(architecture)
                    .filter(|pin| pin.runtime == runtime)
                    .map(|pin| parse_line(runtime, pin.runtime_line.as_str()))
                    .collect::<Result<Vec<_>, _>>()?,
            };
            lines.sort();
            lines.dedup();
            for line in lines {
                offered.extend(
                    source
                        .list(line, architecture)
                        .await
                        .map_err(|error| Status::unavailable(describe(&error)))?,
                );
            }
        }
        let artifacts = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let mut artifacts = Vec::new();
            if kinds.runtimes
                && let Some(releases) = releases
            {
                for pin in catalog.releases(runtime, architecture) {
                    if line.is_some_and(|line| {
                        pin.release.version.as_str().parse::<ReleaseLine>().ok()
                            != Some(line.release)
                    }) {
                        continue;
                    }
                    artifacts.push(AvailableArtifact {
                        identity: Some(crate::convert::runtime_identity(
                            &pin.release,
                            pin.digest(),
                        )),
                        architecture: architecture.as_str().to_string(),
                        size: None,
                        source: releases.source.root().to_string(),
                        presence: presence(
                            &releases
                                .root
                                .join(pplx_space_node_runtime_core::artifact_store::managed_release_directory(&pin.release, pin.digest())),
                        ) as i32,
                        compatible_runtimes: Vec::new(),
                    });
                }
            }
            for kernel in offered {
                let Some(pin) = catalog
                    .kernel(kernel.version.artifact_version(), architecture)
                    .filter(|pin| {
                        pin.size == kernel.size
                            && pin.runtime == kernel.line.runtime
                            && pin.runtime_line.as_str() == kernel.line.release.to_string()
                    })
                else {
                    continue;
                };
                let identity = crate::convert::component_identity(ComponentIdentity {
                    kind: pplx_space_common_core::types::ArtifactComponentKind::Kernel,
                    version: pin.version.clone(),
                    digest: pin.sha256,
                });
                if artifacts
                    .iter()
                    .any(|entry| entry.identity.as_ref() == Some(&identity))
                {
                    continue;
                }
                artifacts.push(AvailableArtifact {
                    identity: Some(identity),
                    architecture: architecture.as_str().to_string(),
                    size: Some(pin.size),
                    source: kernel.location.to_string(),
                    presence: kernels_root
                        .as_ref()
                        .map_or(ArtifactPresence::Unspecified, |root| {
                            presence(&root.join(pplx_space_node_runtime_core::artifact_store::managed_kernel_directory(&pin.version, pin.sha256)))
                        }) as i32,
                    compatible_runtimes: catalog
                        .releases(runtime, architecture)
                        .filter(|release| {
                            release.release.version.as_str().parse::<ReleaseLine>().ok()
                                == Some(kernel.line.release)
                        })
                        .map(|release| execution::RuntimeIdentity {
                            kind: execution::RuntimeKind::from(release.release.kind) as i32,
                            version: release.release.version.to_string(),
                        })
                        .collect(),
                });
            }
            artifacts
        })
        .await
        .map_err(|error| Status::internal(describe(&error)))?;
        Ok(Response::new(ListAvailableArtifactsResponse { artifacts }))
    }

    async fn install_artifact(
        &self,
        request: Request<InstallArtifactRequest>,
    ) -> Result<Response<InstallArtifactResponse>, Status> {
        self.peer_authorizer
            .authorize_request(&request, "InstallArtifact", ALLOW_INSTALL)?;
        let identity = parse_identity(request.get_ref().identity.as_ref())?;
        let installer = self.installer()?.clone();
        let operation = match identity {
            RequestedArtifact::Runtime(release, digest) => installer
                .install_release(release, digest)
                .await
                .map_err(submit_status)?,
            RequestedArtifact::Component(component) => {
                let checking = installer.clone();
                let requested = component.clone();
                let architecture = crate::installs::host_arch()
                    .ok_or_else(|| Status::failed_precondition("unsupported architecture"))?;
                let pin = tokio::task::spawn_blocking(move || {
                    checking.catalog().map(|catalog| {
                        catalog
                            .kernel(&requested.version, architecture)
                            .filter(|pin| pin.sha256 == requested.digest)
                            .cloned()
                    })
                })
                .await
                .map_err(|error| Status::internal(describe(&error)))?
                .map_err(|error| Status::failed_precondition(describe(&error)))?
                .ok_or_else(|| {
                    Status::failed_precondition("component is not pinned for this node")
                })?;
                let line = parse_line(pin.runtime, pin.runtime_line.as_str())?;
                let version = pin
                    .version
                    .as_str()
                    .parse()
                    .map_err(|_| Status::invalid_argument("unsupported kernel version"))?;
                installer
                    .install(line, version, component.digest)
                    .await
                    .map_err(submit_status)?
            }
        };
        Ok(Response::new(InstallArtifactResponse {
            operation: Some(crate::convert::operation(&operation)),
        }))
    }

    async fn uninstall_artifact(
        &self,
        request: Request<UninstallArtifactRequest>,
    ) -> Result<Response<UninstallArtifactResponse>, Status> {
        self.peer_authorizer
            .authorize_request(&request, "UninstallArtifact", ALLOW_MUTATE)?;
        let identity = parse_identity(request.get_ref().identity.as_ref())?;
        let installer = self.installer()?;
        let operation = match identity {
            RequestedArtifact::Runtime(release, digest) => {
                installer.uninstall_release(release, digest).await
            }
            RequestedArtifact::Component(component) => {
                let version = component
                    .version
                    .as_str()
                    .parse()
                    .map_err(|_| Status::invalid_argument("unsupported kernel version"))?;
                installer.uninstall(version, component.digest).await
            }
        }
        .map_err(submit_status)?;
        Ok(Response::new(UninstallArtifactResponse {
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
