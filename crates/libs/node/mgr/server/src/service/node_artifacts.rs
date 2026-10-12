//! The node's artifact store: publishing runtime releases and guest kernels
//! the runtime manager staged, and removing them.
//!
//! Node-manager is the store's only writer. Execution does not read this
//! store. Node-manager decides what gets in: it loads the catalog's pins
//! itself for every request and publishes only bytes that match them (see
//! [`pplx_space_node_runtime_core::artifact_store`]). The runtime manager
//! names an artifact by what it is, never by a path. A catalog that cannot
//! be loaded safely refuses the request, not node-manager's startup.

use pplx_space_common_core::types::{Arch, ArtifactVersion, RuntimeIdentity, RuntimeVersion};
use pplx_space_common_svc::UdsPeerPrincipal;
use pplx_space_common_svc::core::host::linux::identities::user_names;
use pplx_space_node_runtime_core::artifact_store::{
    ArtifactStore, Publication, PublishError, PublishOutcome, RemoveError, RemoveOutcome,
    StagedName,
};
use pplx_space_node_runtime_core::catalog::RuntimeCatalog;
use tonic::{Request, Response, Status};

use crate::proto::{
    NodeArtifact, PublishArtifactRequest, PublishArtifactResponse, RemoveArtifactRequest,
    RemoveArtifactResponse, node_artifact,
};

use super::NodeManagerService;

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
}

impl Artifact {
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
                match ArtifactComponentKind::try_from(component.kind) {
                    Ok(ArtifactComponentKind::Kernel) => {}
                    Ok(ArtifactComponentKind::Unspecified) | Err(_) => {
                        return Err(Status::invalid_argument("a component kind is required"));
                    }
                    Ok(_) => {
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
                Ok(Self::Kernel(version, digest))
            }
            None => Err(Status::invalid_argument("name the artifact")),
        }
    }
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
        PublishError::UnsafeRoot { .. } | PublishError::TargetDiffers { .. } => {
            Status::failed_precondition(describe(&error))
        }
        PublishError::Staged { .. } | PublishError::Io { .. } => Status::internal(describe(&error)),
    }
}

fn remove_status(error: RemoveError) -> Status {
    match error {
        RemoveError::InvalidVersion { .. } => Status::invalid_argument(describe(&error)),
        RemoveError::IdentityChanged { .. } | RemoveError::UnsafeRoot { .. } => {
            Status::failed_precondition(describe(&error))
        }
        RemoveError::Io { .. } => Status::internal(describe(&error)),
    }
}

/// Earlier prototype images linked execution roots into the store. Refuse
/// mutations there without changing launch behavior. Root-owned deployment
/// configuration is stable during a mutation; recheck it on each request.
fn separate_store(paths: &crate::config::RuntimePaths) -> Result<(), std::io::Error> {
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
    for managed in [&paths.store.releases, &paths.store.kernels] {
        let managed = resolve(managed)?;
        for execution in [
            paths.vendor.as_path(),
            &paths.assets,
            &paths.kernels,
            vendor_kernels,
        ] {
            let execution = resolve(execution)?;
            if execution.starts_with(&managed) || managed.starts_with(&execution) {
                return Err(std::io::Error::other(
                    "execution and managed artifact roots overlap; repair provisioning before managing artifacts",
                ));
            }
        }
    }
    Ok(())
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

    fn artifact_store(&self) -> ArtifactStore {
        let paths = &self.runtime_paths.store;
        ArtifactStore::new(
            &paths.releases,
            &paths.kernels,
            &paths.staging,
            MAX_ARTIFACT_FILE_BYTES,
        )
    }

    /// Best-effort preparation of the separate managed store. Existing launch
    /// roots and aliases are never moved or repaired here.
    pub(crate) async fn prepare_artifact_store(&self) {
        let store = self.artifact_store();
        let paths = self.runtime_paths.clone();
        let prepared = tokio::task::spawn_blocking(move || {
            separate_store(&paths)?;
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
            Ok(Ok(())) => tracing::info!("managed artifact store prepared"),
            Ok(Err(error)) => {
                tracing::warn!(%error, "artifact store preparation incomplete; requests recheck the store; execution roots unchanged")
            }
            Err(error) => {
                tracing::warn!(%error, "artifact store preparation failed; execution roots unchanged")
            }
        }
        // Reclaim independently of staging/account provisioning, and never hold
        // node readiness behind recursive deletion. Tracked for shutdown.
        let store = self.artifact_store();
        let paths = self.runtime_paths.clone();
        self.device_op_tasks.spawn_blocking(move || {
            let result = separate_store(&paths).and_then(|()| store.clear_leftovers());
            match result {
                Ok(cleared) => tracing::info!(cleared, "managed artifact leftovers cleared"),
                Err(error) => {
                    tracing::warn!(%error, "managed artifact cleanup failed; retry on next startup")
                }
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
        let store = self.artifact_store();
        let paths = self.runtime_paths.clone();
        let catalog_dir = self.runtime_paths.store.catalog.clone();
        let slot = self.artifact_slot()?;
        let publication: Publication = self
            .off_runtime("publish artifact", move || {
                let _slot = slot;
                separate_store(&paths)
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
            })
            .await?;
        Ok(Response::new(PublishArtifactResponse {
            path: publication.path.to_string_lossy().into_owned(),
            digest: publication.digest.to_string(),
            already_present: publication.outcome == PublishOutcome::AlreadyPresent,
        }))
    }

    pub(super) async fn remove_artifact(
        &self,
        request: Request<RemoveArtifactRequest>,
    ) -> Result<Response<RemoveArtifactResponse>, Status> {
        self.authorize_peer(&request, "RemoveArtifact", ALLOW_ROOT_OR_RTMGR)?;
        let artifact = Artifact::parse(request.get_ref().artifact.as_ref())?;
        let store = self.artifact_store();
        let paths = self.runtime_paths.clone();
        let slot = self.artifact_slot()?;
        let outcome = self
            .off_runtime("remove artifact", move || {
                let _slot = slot;
                separate_store(&paths)
                    .map_err(|error| Status::failed_precondition(describe(&error)))?;
                match &artifact {
                    Artifact::Release(release, digest) => store
                        .remove_release(release, *digest)
                        .map_err(remove_status),
                    Artifact::Kernel(version, digest) => {
                        store.remove_kernel(version, *digest).map_err(remove_status)
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
