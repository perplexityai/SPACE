//! The node's artifact store: publishing runtime releases and guest kernels
//! the runtime manager staged, and removing them.
//!
//! Node-manager is the store's only writer and exposes the compiled E0 binding
//! plus approved explicit environment bindings
//! to execution. It decides what gets in: it loads the catalog's pins
//! itself for every request and publishes only bytes that match them (see
//! [`pplx_space_node_runtime_core::artifact_store`]). The runtime manager
//! names an artifact by what it is, never by a path. A catalog that cannot
//! be loaded safely refuses the request, not node-manager's startup.

use pplx_space_common_core::types::{
    Arch, ArtifactVersion, ComponentIdentity, RuntimeIdentity, RuntimeVersion,
};
use pplx_space_common_svc::UdsPeerPrincipal;
use pplx_space_common_svc::core::host::linux::identities::user_names;
use pplx_space_node_runtime_core::artifact_store::{
    ArtifactStore, Publication, PublishError, PublishOutcome, RemoveError, RemoveOutcome,
    StagedName,
};
use pplx_space_node_runtime_core::catalog::{InitialEnvironment, RuntimeCatalog};
use tonic::{Request, Response, Status};

use crate::proto::{
    NodeArtifact, PublishArtifactRequest, PublishArtifactResponse, RemoveArtifactRequest,
    RemoveArtifactResponse, node_artifact,
};

use super::NodeManagerService;

mod selection;

use pplx_space_node_runtime_core::runtime_inventory::{
    ExecutionSelectionMode, ExecutionSourceIntegrity, ResolvedExecutionEnvironment,
    RuntimeInstallationFiles,
};

impl NodeManagerService {
    #[cfg(test)]
    pub(super) fn allowed_definition(
        &self,
        reference: Option<pplx_space_common_core::types::EnvironmentReference>,
        mode: ExecutionSelectionMode,
    ) -> Result<pplx_space_execution_common::environment::EnvironmentDefinition, Status> {
        self.allowed_definition_in(PolicyScope::Effective, reference, mode)
    }

    pub(super) fn allowed_definition_in(
        &self,
        scope: PolicyScope,
        reference: Option<pplx_space_common_core::types::EnvironmentReference>,
        mode: ExecutionSelectionMode,
    ) -> Result<pplx_space_execution_common::environment::EnvironmentDefinition, Status> {
        select_definition(&self.execution_policy_in(scope), reference, mode)
    }

    fn execution_policy_in(
        &self,
        scope: PolicyScope,
    ) -> std::sync::Arc<pplx_space_node_runtime_core::selection::ExecutionPolicy> {
        match scope {
            PolicyScope::Effective => self.execution_policy(),
            PolicyScope::Startup => std::sync::Arc::new(self.execution_policy.startup().clone()),
        }
    }

    pub(super) async fn require_verified_environment_layout(
        &self,
        runtime: pplx_space_common_core::types::RuntimeKind,
    ) -> Result<(), Status> {
        if !cfg!(target_os = "linux") {
            return Err(Status::failed_precondition(
                "verified managed execution requires a Linux host",
            ));
        }
        let paths = self.runtime_paths.clone();
        let store = artifact_store(&paths.store, runtime)?;
        self.off_runtime("verify environment layout", move || {
            separate_store(&paths, store.releases_root())
                .map_err(|error| Status::failed_precondition(describe(&error)))
        })
        .await
    }

    pub async fn resolve_execution_environment(
        &self,
        request: Request<crate::proto::ResolveExecutionEnvironmentRequest>,
    ) -> Result<Response<crate::proto::ResolveExecutionEnvironmentResponse>, Status> {
        self.authorize_peer(
            &request,
            "ResolveExecutionEnvironment",
            super::ALLOW_NODE_STATUS,
        )?;
        let request = request.into_inner();
        if request.definition_schema_version
            < pplx_space_execution_common::environment::RUNTIME_NEUTRAL_ENVIRONMENT_SCHEMA_VERSION
        {
            return Err(Status::failed_precondition(
                "generic environment schema 2 is required",
            ));
        }
        if let Some(compatible) = request.compatible {
            if request.reference.is_some() {
                return Err(Status::invalid_argument(
                    "exact and compatible environment selection are mutually exclusive",
                ));
            }
            return self
                .resolve_compatible_environment(compatible)
                .await
                .map(Response::new);
        }
        let reference = request
            .reference
            .map(|reference| reference.parse())
            .transpose()
            .map_err(|error: pplx_space_common_core::types::DigestParseError| {
                Status::invalid_argument(error.to_string())
            })?;
        let (definition, execution) = self
            .resolve_new_cold_boot(&self.execution_policy(), reference)
            .await?;
        // Re-read after discovery, so a policy applied meanwhile still gates
        // the binding callers persist.
        self.execution_policy()
            .check_execution(&execution, ExecutionSelectionMode::NewColdBoot)
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        Ok(Response::new(
            crate::proto::ResolveExecutionEnvironmentResponse {
                accepted_selection: 0,
                definition: Some((&definition).into()),
                execution: Some((&execution).into()),
            },
        ))
    }

    /// Resolve `reference`, or the policy default, for a new cold boot against
    /// verified inventory, as admission would. Pass one policy for the whole
    /// decision, even if node-ctl applies another meanwhile.
    pub(super) async fn resolve_new_cold_boot(
        &self,
        policy: &pplx_space_node_runtime_core::selection::ExecutionPolicy,
        reference: Option<pplx_space_common_core::types::EnvironmentReference>,
    ) -> Result<
        (
            pplx_space_execution_common::environment::EnvironmentDefinition,
            pplx_space_common_core::types::ExecutionMetadata,
        ),
        Status,
    > {
        let definition = select_definition(policy, reference, ExecutionSelectionMode::NewColdBoot)?;
        let requested = definition
            .execution_requirements()
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        self.require_verified_environment_layout(requested.runtime().kind)
            .await?;
        let inventory = self.runtime_inventory().await?;
        let resolved = inventory
            .resolve(
                Some(&requested),
                requested.runtime().kind,
                ExecutionSelectionMode::NewColdBoot,
            )
            .map_err(super::RuntimeInventoryError::into_status)?;
        let execution = definition
            .bind_execution(resolved.execution)
            .map_err(|error| Status::failed_precondition(describe(&error)))?;
        policy
            .check_execution(&execution, ExecutionSelectionMode::NewColdBoot)
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        Ok((definition, execution))
    }

    pub(super) async fn resolve_bound_execution(
        &self,
        requested: Option<&pplx_space_common_core::types::ExecutionMetadata>,
        runtime: pplx_space_common_core::types::RuntimeKind,
        mode: ExecutionSelectionMode,
    ) -> Result<ResolvedExecutionEnvironment, Status> {
        self.resolve_bound_execution_in(PolicyScope::Effective, requested, runtime, mode)
            .await
    }

    pub(super) async fn resolve_bound_execution_in(
        &self,
        scope: PolicyScope,
        requested: Option<&pplx_space_common_core::types::ExecutionMetadata>,
        runtime: pplx_space_common_core::types::RuntimeKind,
        mode: ExecutionSelectionMode,
    ) -> Result<ResolvedExecutionEnvironment, Status> {
        let policy = self.execution_policy_in(scope);
        let definition =
            if let Some(reference) = requested.and_then(|metadata| metadata.environment()) {
                self.require_verified_environment_layout(runtime).await?;
                let definition = select_definition(&policy, Some(reference), mode)?;
                if let Some(requested) = requested {
                    definition
                        .check_execution(requested)
                        .map_err(|error| Status::failed_precondition(describe(&error)))?;
                }
                Some(definition)
            } else {
                None
            };
        let inventory = self.runtime_inventory().await?;
        let mut resolved = inventory
            .resolve(requested, runtime, mode)
            .map_err(super::RuntimeInventoryError::into_status)?;
        if definition.is_none() {
            let initial = initial_environment()?;
            let requires_binding =
                |identity: &RuntimeIdentity, files: &RuntimeInstallationFiles| {
                    identity != &initial.release().release
                        && (files.integrity() == ExecutionSourceIntegrity::FsVerity
                            || files.paths().any(|path| {
                                self.runtime_paths
                                    .store
                                    .releases
                                    .values()
                                    .any(|root| path.starts_with(root))
                                    && !path.starts_with(&self.runtime_paths.vendor)
                            }))
                };
            // Moving verified legacy runtime bytes into the store does not bind
            // historical custom kernels to E0. Check the requested installation
            // too: an unbound new cold boot otherwise chooses the node default.
            let requested_requires_binding = requested
                .and_then(|metadata| inventory.runtime_installation(metadata.runtime()))
                .is_some_and(|installation| {
                    requires_binding(&installation.runtime, &installation.files)
                });
            if requested_requires_binding
                || requires_binding(resolved.execution.runtime(), &resolved.runtime_files)
            {
                return Err(Status::failed_precondition(
                    "managed non-default execution requires an explicit environment binding",
                ));
            }
        }
        if let Some(definition) = definition {
            resolved.execution = definition
                .bind_execution(resolved.execution)
                .map_err(|error| Status::failed_precondition(describe(&error)))?;
        }
        // Re-read after the awaits above, so a policy applied meanwhile
        // still gates this admission.
        self.execution_policy_in(scope)
            .check_execution(&resolved.execution, mode)
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        Ok(resolved)
    }
}

const UNREADABLE_POLICY: &str = "node can't read its persisted cluster execution policy and admits no new work until the policy is reapplied";

/// Which policy a decision obeys. Admission obeys the effective policy;
/// captures, which execute nothing, obey the startup settings.
#[derive(Clone, Copy, Debug)]
pub(super) enum PolicyScope {
    Effective,
    Startup,
}

fn select_definition(
    policy: &pplx_space_node_runtime_core::selection::ExecutionPolicy,
    reference: Option<pplx_space_common_core::types::EnvironmentReference>,
    mode: ExecutionSelectionMode,
) -> Result<pplx_space_execution_common::environment::EnvironmentDefinition, Status> {
    if policy.cluster_policy_unreadable() {
        return Err(Status::failed_precondition(UNREADABLE_POLICY));
    }
    let reference = reference.or(policy.default_reference()).ok_or_else(|| {
        Status::failed_precondition(if policy.default_withdrawn() {
            "node has no usable default execution environment under the cluster policy"
        } else {
            "host has no qualified initial execution environment"
        })
    })?;
    if !policy.allows(reference) {
        return Err(Status::failed_precondition(
            "execution environment is not allowed on this node",
        ));
    }
    let entry = policy
        .catalog()
        .iter()
        .find(|entry| entry.definition().reference() == reference)
        .ok_or_else(|| Status::failed_precondition("unknown compiled execution environment"))?;
    if mode == ExecutionSelectionMode::PersistedStateRestore && !entry.supports_memory_state() {
        return Err(Status::failed_precondition(
            "execution environment does not support memory-state restore",
        ));
    }
    Ok(entry.definition().clone())
}

const ALLOW_ROOT_OR_RTMGR: &[UdsPeerPrincipal] = &[
    UdsPeerPrincipal::Root,
    UdsPeerPrincipal::Service(user_names::SPACE_RTMGR),
];

/// The largest staged file a publication copies: well above any release
/// binary or guest kernel.
const MAX_ARTIFACT_FILE_BYTES: u64 = 512 * 1024 * 1024;

/// An artifact a request names.
#[derive(Clone, Debug)]
enum Artifact {
    Release(RuntimeIdentity, pplx_space_common_core::types::Digest),
    Kernel(ArtifactVersion, pplx_space_common_core::types::Digest),
    Boot(ComponentIdentity),
}

impl Artifact {
    fn belongs_to_initial(&self, initial: &InitialEnvironment) -> bool {
        match self {
            Self::Release(release, digest) => {
                *release == initial.release().release && *digest == initial.release().digest()
            }
            Self::Kernel(_, digest) => *digest == initial.kernel().sha256,
            Self::Boot(_) => false,
        }
    }

    fn belongs_to_environment(
        &self,
        environment: &pplx_space_node_runtime_core::catalog::CompiledEnvironment,
    ) -> bool {
        match self {
            Self::Release(release, digest) => {
                release == &environment.release().release
                    && *digest == environment.release().digest()
            }
            Self::Kernel(_, digest) => environment
                .kernel()
                .is_some_and(|kernel| *digest == kernel.sha256),
            Self::Boot(identity) => environment
                .definition()
                .descriptor()
                .spec()
                .boot
                .components()
                .any(|(_, component, _)| component == identity),
        }
    }

    fn check_removal(&self, initial: &InitialEnvironment) -> Result<(), Status> {
        // Compiled bindings remain protected across restarts and deployment changes.
        // Releasing them requires a future reference-aware retirement mechanism.
        if self.belongs_to_initial(initial)
            || compiled_environments()?
                .iter()
                .any(|environment| self.belongs_to_environment(environment))
        {
            Err(Status::failed_precondition(
                "artifact belongs to compiled E0 or another approved execution environment",
            ))
        } else {
            Ok(())
        }
    }
    fn parse(artifact: Option<&NodeArtifact>) -> Result<Self, Status> {
        match artifact.and_then(|artifact| artifact.artifact.as_ref()) {
            Some(node_artifact::Artifact::RuntimeRelease(release)) => {
                let named =
                    pplx_space_execution_common::proto::RuntimeKind::try_from(release.runtime_kind)
                        .map_err(|_| Status::invalid_argument("unknown runtime kind"))?;
                let kind = pplx_space_common_core::types::RuntimeKind::try_from(named)
                    .map_err(|error| Status::invalid_argument(error.to_string()))?;
                let version = RuntimeVersion::parse(&release.version)
                    .map_err(|error| Status::invalid_argument(error.to_string()))?;
                let digest = release
                    .digest
                    .parse()
                    .map_err(|_| Status::invalid_argument("invalid runtime digest"))?;
                Ok(Self::Release(RuntimeIdentity { kind, version }, digest))
            }
            Some(node_artifact::Artifact::Component(component)) => {
                use pplx_space_execution_common::proto::ArtifactComponentKind;
                let named = ArtifactComponentKind::try_from(component.kind)
                    .map_err(|_| Status::invalid_argument("unknown component kind"))?;
                match named {
                    ArtifactComponentKind::Kernel | ArtifactComponentKind::Firmware => {}
                    ArtifactComponentKind::Unspecified => {
                        return Err(Status::invalid_argument("a component kind is required"));
                    }
                    _ => {
                        return Err(Status::failed_precondition(
                            "no publisher for this component kind",
                        ));
                    }
                }
                let version = ArtifactVersion::parse(&component.version)
                    .map_err(|error| Status::invalid_argument(error.to_string()))?;
                let digest = component
                    .digest
                    .parse()
                    .map_err(|_| Status::invalid_argument("invalid component digest"))?;
                let kind = pplx_space_common_core::types::ArtifactComponentKind::try_from(named)
                    .map_err(|error| Status::invalid_argument(error.to_string()))?;
                if kind == pplx_space_common_core::types::ArtifactComponentKind::Kernel {
                    Ok(Self::Kernel(version, digest))
                } else {
                    Ok(Self::Boot(ComponentIdentity {
                        kind,
                        version,
                        digest,
                    }))
                }
            }
            None => Err(Status::invalid_argument("name the artifact")),
        }
    }
}

/// Initial discovery retains the Firecracker migration fallback. Every allowed
/// non-initial binding must also have a separate runtime store.
/// Publication and removal already check the selected runtime's store separately.
pub(super) fn execution_inventory(
    paths: &crate::config::RuntimePaths,
    policy: &pplx_space_node_runtime_core::selection::ExecutionPolicy,
) -> Result<super::RuntimeInventory, Status> {
    let store = artifact_store(
        &paths.store,
        pplx_space_common_core::types::RuntimeKind::Firecracker,
    )?;
    if execution_roots_overlap(paths, store.releases_root())
        .map_err(|error| Status::failed_precondition(describe(&error)))?
    {
        tracing::warn!(
            "managed provisioning disabled; retaining prototype external execution until layout migration"
        );
        return super::RuntimeInventory::discover(
            [
                pplx_space_node_runtime_core::runtime_inventory::RuntimeInstallationRoot::new(
                    pplx_space_common_core::types::RuntimeKind::Firecracker,
                    &paths.vendor,
                ),
            ],
            &paths.assets,
            &paths.kernels,
        )
        .map_err(super::RuntimeInventoryError::into_status);
    }
    let initial = initial_environment()?;
    let mut inventory = super::RuntimeInventory::discover_with_store(
        &store,
        initial,
        &paths.vendor,
        &paths.assets,
        &paths.kernels,
    )
    .map_err(super::RuntimeInventoryError::into_status)?;
    for environment in policy
        .catalog()
        .iter()
        .filter(|entry| !entry.is_initial() && policy.allows(entry.definition().reference()))
    {
        let environment_store = match artifact_store(
            &paths.store,
            environment.release().release.kind,
        ) {
            Ok(store) => store,
            Err(error) => {
                tracing::warn!(%error, "environment store unavailable; retaining other bindings");
                continue;
            }
        };
        if let Err(error) = separate_store(paths, environment_store.releases_root()) {
            tracing::warn!(%error, "environment store overlaps execution roots; retaining other bindings");
            continue;
        }
        if let Err(error) =
            inventory.include_environment(&environment_store, environment, &paths.kernels)
        {
            tracing::warn!(%error, "environment inventory unavailable; retaining other bindings");
        }
    }
    Ok(inventory)
}

pub(super) fn compiled_environments()
-> Result<&'static [pplx_space_node_runtime_core::catalog::CompiledEnvironment], Status> {
    pplx_space_node_runtime_core::catalog::CompiledEnvironment::cached_for_architecture(host_arch()?)
        .map_err(|error| Status::failed_precondition(describe(error)))
}

pub(super) fn initial_environment() -> Result<&'static InitialEnvironment, Status> {
    static INITIAL: std::sync::OnceLock<Result<InitialEnvironment, Status>> =
        std::sync::OnceLock::new();
    INITIAL
        .get_or_init(|| {
            InitialEnvironment::for_architecture(host_arch()?)
                .map_err(|error| Status::failed_precondition(describe(&error)))
        })
        .as_ref()
        .map_err(Clone::clone)
}

fn host_arch() -> Result<Arch, Status> {
    std::env::consts::ARCH.parse().map_err(|_| {
        Status::failed_precondition(format!(
            "node architecture {} has no artifact pins",
            std::env::consts::ARCH
        ))
    })
}

/// `error` and every error that caused it, as one line.
fn describe(error: &(dyn std::error::Error + 'static)) -> String {
    let mut text = error.to_string();
    let mut cause = error.source();
    while let Some(next) = cause {
        text.push_str(": ");
        text.push_str(&next.to_string());
        cause = next.source();
    }
    text
}

fn publish_status(error: PublishError) -> Status {
    match error {
        PublishError::InvalidVersion { .. }
        | PublishError::Mismatch { .. }
        | PublishError::WrongSize { .. }
        | PublishError::NotRegularFile { .. }
        | PublishError::Linked { .. }
        | PublishError::TooLarge { .. } => Status::invalid_argument(describe(&error)),
        PublishError::ExistingArtifact(ref source) if source.is_transient() => {
            Status::internal(describe(&error))
        }
        PublishError::UnsafeRoot { .. } | PublishError::ExistingArtifact(_) => {
            Status::failed_precondition(describe(&error))
        }
        PublishError::Source { .. } | PublishError::Io { .. } if error.is_transient() => {
            Status::internal(describe(&error))
        }
        PublishError::Source { .. } | PublishError::Io { .. } => {
            Status::failed_precondition(describe(&error))
        }
    }
}

fn remove_status(error: RemoveError) -> Status {
    match error {
        RemoveError::InvalidVersion { .. } => Status::invalid_argument(describe(&error)),
        RemoveError::IdentityChanged { .. } | RemoveError::UnsafeRoot { .. } => {
            Status::failed_precondition(describe(&error))
        }
        RemoveError::Io { .. } if error.is_transient() => Status::internal(describe(&error)),
        RemoveError::Io { .. } => Status::failed_precondition(describe(&error)),
    }
}

/// Earlier prototype images linked execution roots into the store. Refuse
/// mutations there while retaining the existing external execution path. Root-owned deployment
/// configuration is stable during a mutation; recheck it on each request.
fn separate_store(
    paths: &crate::config::RuntimePaths,
    root: &std::path::Path,
) -> Result<(), std::io::Error> {
    if execution_roots_overlap(paths, root)? {
        Err(std::io::Error::other(
            "execution and managed artifact roots overlap; repair provisioning before managing artifacts",
        ))
    } else {
        Ok(())
    }
}

fn execution_roots_overlap(
    paths: &crate::config::RuntimePaths,
    root: &std::path::Path,
) -> Result<bool, std::io::Error> {
    fn resolve(path: &std::path::Path) -> Result<std::path::PathBuf, std::io::Error> {
        match std::fs::canonicalize(path) {
            Ok(path) => Ok(path),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                // An absent root is fine, but a dangling execution link is not
                // evidence that it is separate from the managed store.
                match std::fs::symlink_metadata(path) {
                    Err(missing) if missing.kind() == std::io::ErrorKind::NotFound => {
                        Ok(path.to_path_buf())
                    }
                    _ => Err(error),
                }
            }
            Err(error) => Err(error),
        }
    }
    let vendor_kernels = std::path::Path::new(
        pplx_space_common_core::host::linux::paths::FIRECRACKER_KERNEL_VENDOR_DIR,
    );
    for managed in [
        root,
        paths.store.kernels.as_path(),
        &pplx_space_node_runtime_core::artifact_layout::boot_artifacts_root(&paths.store.kernels),
    ] {
        let managed = resolve(managed)?;
        for execution in [
            paths.vendor.as_path(),
            &paths.assets,
            &paths.kernels,
            vendor_kernels,
        ] {
            let execution = resolve(execution)?;
            if execution.starts_with(&managed) || managed.starts_with(&execution) {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn artifact_runtime(artifact: &Artifact) -> pplx_space_common_core::types::RuntimeKind {
    match artifact {
        Artifact::Release(release, _) => release.kind,
        Artifact::Kernel(..) | Artifact::Boot(..) => {
            pplx_space_common_core::types::RuntimeKind::Firecracker
        }
    }
}

fn artifact_store(
    paths: &crate::config::ArtifactStorePaths,
    runtime: pplx_space_common_core::types::RuntimeKind,
) -> Result<ArtifactStore, Status> {
    let root = paths.releases.get(&runtime).ok_or_else(|| {
        Status::failed_precondition(format!("no managed release store configured for {runtime}"))
    })?;
    Ok(ArtifactStore::new(
        root,
        &paths.kernels,
        &paths.staging,
        MAX_ARTIFACT_FILE_BYTES,
    ))
}

impl NodeManagerService {
    /// The slot an artifact-store change runs in, or `RESOURCE_EXHAUSTED`
    /// while another holds it, which the runtime manager retries.
    fn artifact_slot(&self) -> Result<tokio::sync::OwnedSemaphorePermit, Status> {
        self.artifact_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("the artifact store is busy; retry"))
    }

    fn artifact_store(&self, artifact: &Artifact) -> Result<ArtifactStore, Status> {
        let runtime = artifact_runtime(artifact);
        artifact_store(&self.runtime_paths.store, runtime)
    }

    /// Best-effort preparation of the separate managed store. Existing launch
    /// roots and aliases are never moved or repaired here.
    pub(crate) async fn prepare_artifact_store(&self) {
        for (&runtime, root) in &self.runtime_paths.store.releases {
            let stores = &self.runtime_paths.store;
            let store = ArtifactStore::new(
                root,
                &stores.kernels,
                &stores.staging,
                MAX_ARTIFACT_FILE_BYTES,
            );
            let paths = self.runtime_paths.clone();
            let cleanup_store = store.clone();
            let cleanup_paths = paths.clone();
            let prepared = tokio::task::spawn_blocking(move || {
                separate_store(&paths, store.releases_root())?;
                store.prepare_roots()?;
                if let Some(account) = nix::unistd::User::from_name(user_names::SPACE_RTMGR)
                    .map_err(std::io::Error::other)?
                {
                    store.ensure_staging_root(account.uid.as_raw(), account.gid.as_raw())?;
                }
                Ok::<_, std::io::Error>(())
            })
            .await;
            match prepared {
                Ok(Ok(())) => tracing::info!(%runtime, "managed artifact store prepared"),
                Ok(Err(error)) => {
                    tracing::warn!(%runtime, %error, "artifact store preparation incomplete; requests recheck the store")
                }
                Err(error) => {
                    tracing::warn!(%runtime, %error, "artifact store preparation task failed")
                }
            }
            self.device_op_tasks.spawn_blocking(move || {
                match separate_store(&cleanup_paths, cleanup_store.releases_root()).and_then(|()| cleanup_store.clear_leftovers()) {
                    Ok(cleared) => tracing::info!(%runtime, cleared, "managed artifact leftovers cleared"),
                    Err(error) => tracing::warn!(%runtime, %error, "managed artifact cleanup failed; retry on next startup"),
                }
            });
        }
        // Warm in the tracked background lane. Inventory consumers await the same
        // serialized discovery; unrelated lifecycle RPCs can serve.
        let service = self.clone();
        self.device_op_tasks.spawn(async move {
            if let Err(error) = service.runtime_inventory().await {
                tracing::warn!(%error, "execution cache preparation incomplete; inventory requests retry binding discovery");
            }
        });
    }

    pub(super) async fn publish_artifact(
        &self,
        request: Request<PublishArtifactRequest>,
    ) -> Result<Response<PublishArtifactResponse>, Status> {
        self.authorize_peer(&request, "PublishArtifact", ALLOW_ROOT_OR_RTMGR)?;
        let request = request.into_inner();
        let artifact = Artifact::parse(request.artifact.as_ref())?;
        let staged = StagedName::parse(request.staging_name)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let architecture = host_arch()?;
        let store = self.artifact_store(&artifact)?;
        let paths = self.runtime_paths.clone();
        let catalog_dir = self.runtime_paths.store.catalog.clone();
        let publication = self.publish_artifact_work(artifact.clone(), move || {
                separate_store(&paths, store.releases_root())
                    .map_err(|error| Status::failed_precondition(describe(&error)))?;
                let catalog = RuntimeCatalog::load(&catalog_dir)
                    .map_err(|error| Status::failed_precondition(describe(&error)))?;
                match &artifact {
                    Artifact::Release(release, digest) => {
                        let pin = catalog
                            .release(release, architecture)
                            .ok_or_else(|| {
                                Status::failed_precondition(format!(
                                    "{release} has no pins for {architecture}"
                                ))
                            })?;
                        if pin.digest() != *digest {
                            return Err(Status::failed_precondition(format!("{release} requested digest {digest} differs from current catalog digest {}", pin.digest())));
                        }
                        store.publish_release(pin, &staged).map_err(publish_status)
                    }
                    Artifact::Boot(identity) => {
                        let pin = catalog.boot_artifact(identity, architecture)
                            .ok_or_else(|| Status::failed_precondition("boot artifact is not pinned for this node"))?;
                        store.publish_boot_artifact(pin, &staged).map_err(publish_status)
                    }
                    Artifact::Kernel(version, digest) => {
                        let pin = catalog
                            .kernel(version, architecture)
                            .ok_or_else(|| {
                                Status::failed_precondition(format!(
                                    "kernel {version} has no pins for {architecture}"
                                ))
                            })?;
                        if pin.sha256 != *digest {
                            return Err(Status::failed_precondition(format!("kernel {version} requested digest {digest} differs from current catalog digest {}", pin.sha256)));
                        }
                        store.publish_kernel(pin, &staged).map_err(publish_status)
                    }
                }
            }).await?;
        Ok(Response::new(PublishArtifactResponse {
            path: publication.path.to_string_lossy().into_owned(),
            digest: publication.digest.to_string(),
            already_present: publication.outcome == PublishOutcome::AlreadyPresent,
        }))
    }

    async fn publish_artifact_work(
        &self,
        artifact: Artifact,
        publish: impl FnOnce() -> Result<Publication, Status> + Send + 'static,
    ) -> Result<Publication, Status> {
        let initial = initial_environment()?;
        // Inventory is discovered against the startup ceiling.
        let policy = self.execution_policy.startup().clone();
        let affects_execution = artifact.belongs_to_initial(initial)
            || policy.catalog().iter().any(|environment| {
                policy.allows(environment.definition().reference())
                    && artifact.belongs_to_environment(environment)
            });
        let slot = self.artifact_slot()?;
        // Serialize selected publication with discovery, while readers keep the
        // previous immutable snapshot. Unselected artifacts skip this refresh lock, but
        // discovery can still wait for their store publication locks.
        let guard = if affects_execution {
            Some(
                self.runtime_inventory_refresh_lock
                    .clone()
                    .lock_owned()
                    .await,
            )
        } else {
            None
        };
        let snapshot = self.runtime_inventory.clone();
        let paths = self.runtime_paths.clone();
        self.off_runtime("publish artifact", move || {
            let _slot = slot;
            let _guard = guard;
            let publication = publish()?;
            let unchanged = publication.outcome == PublishOutcome::AlreadyPresent
                && snapshot.load_full().is_some_and(|inventory| inventory.check_files_unchanged());
            if affects_execution && !unchanged {
                // Build first, then swap. E0 verification failures withdraw only
                // that binding; a scan failure prevents advertising stale state.
                match execution_inventory(&paths, &policy) {
                    Ok(inventory) => snapshot.store(Some(std::sync::Arc::new(inventory))),
                    Err(error) => {
                        snapshot.store(None);
                        tracing::warn!(%error, "artifact published but execution discovery is unavailable");
                    }
                }
            }
            Ok(publication)
        }).await
    }

    pub(super) async fn remove_artifact(
        &self,
        request: Request<RemoveArtifactRequest>,
    ) -> Result<Response<RemoveArtifactResponse>, Status> {
        self.authorize_peer(&request, "RemoveArtifact", ALLOW_ROOT_OR_RTMGR)?;
        let artifact = Artifact::parse(request.get_ref().artifact.as_ref())?;
        artifact.check_removal(initial_environment()?)?;
        let store = self.artifact_store(&artifact)?;
        let paths = self.runtime_paths.clone();
        let slot = self.artifact_slot()?;
        // Only unselected candidates reach removal. Neither external files
        // nor E0 can be removed, so executable inventory is unaffected.
        let outcome = self
            .off_runtime("remove artifact", move || {
                let _slot = slot;
                separate_store(&paths, store.releases_root())
                    .map_err(|error| Status::failed_precondition(describe(&error)))?;
                match &artifact {
                    Artifact::Release(release, digest) => store
                        .remove_release(release, *digest)
                        .map_err(remove_status),
                    Artifact::Kernel(version, digest) => {
                        store.remove_kernel(version, *digest).map_err(remove_status)
                    }
                    Artifact::Boot(identity) => {
                        store.remove_boot_artifact(identity).map_err(remove_status)
                    }
                }
            })
            .await?;
        Ok(Response::new(RemoveArtifactResponse {
            removed: outcome == RemoveOutcome::Removed,
        }))
    }
}

#[cfg(test)]
mod tests;
