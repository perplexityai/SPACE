//! The node's artifact store: publishing runtime releases and guest kernels
//! the runtime manager staged, and removing them.
//!
//! Node-manager is the store's only writer and the only consumer that runs
//! what is in it, so it decides what gets in: it loads the catalog's pins
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

use super::{NodeManagerService, RuntimeInventoryCache};

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
    Release(RuntimeIdentity),
    Kernel(ArtifactVersion),
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
                Ok(Self::Release(RuntimeIdentity { kind, version }))
            }
            Some(node_artifact::Artifact::Kernel(kernel)) => {
                ArtifactVersion::parse(&kernel.version)
                    .map(Self::Kernel)
                    .map_err(|error| Status::invalid_argument(error.to_string()))
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
        PublishError::Mismatch { .. }
        | PublishError::WrongSize { .. }
        | PublishError::NotRegularFile { .. }
        | PublishError::Linked { .. }
        | PublishError::TooLarge { .. } => Status::invalid_argument(describe(&error)),
        PublishError::UnsafeRoot { .. }
        | PublishError::TargetDiffers { .. }
        | PublishError::NoSelection { .. } => Status::failed_precondition(describe(&error)),
        PublishError::Staged { .. } | PublishError::Io { .. } => Status::internal(describe(&error)),
    }
}

fn remove_status(error: RemoveError) -> Status {
    match error {
        RemoveError::Protected { .. }
        | RemoveError::Selected { .. }
        | RemoveError::Last { .. }
        | RemoveError::UnsafeRoot { .. } => Status::failed_precondition(describe(&error)),
        RemoveError::Io { .. } => Status::internal(describe(&error)),
    }
}

/// Drops the cached runtime inventory when a store change ends, however it
/// ends. Runs on the blocking task, which finishes even when the request that
/// started it is cancelled.
struct ForgetOnDrop<'a>(&'a RuntimeInventoryCache);

impl Drop for ForgetOnDrop<'_> {
    fn drop(&mut self) {
        self.0.forget();
    }
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

    /// Prepare the store before any request: give the runtime manager its
    /// staging root, which lives on the cache that is recreated at boot and
    /// whose parent it cannot write, and remove what interrupted
    /// publications and removals left. A store that cannot be prepared is
    /// logged, not fatal: launches do not depend on it.
    pub(crate) async fn prepare_artifact_store(&self) {
        let store = self.artifact_store();
        let prepared = tokio::task::spawn_blocking(move || {
            let staging = match nix::unistd::User::from_name(user_names::SPACE_RTMGR) {
                Ok(Some(account)) => {
                    Some(store.ensure_staging_root(account.uid.as_raw(), account.gid.as_raw()))
                }
                // No runtime manager on this node.
                Ok(None) => None,
                Err(error) => Some(Err(std::io::Error::from(error))),
            };
            (staging, store.clear_leftovers())
        })
        .await;
        let (staging, cleared) = match prepared {
            Ok(prepared) => prepared,
            Err(error) => {
                tracing::warn!(%error, "artifact store preparation task failed");
                return;
            }
        };
        match staging {
            Some(Ok(true)) => tracing::info!(
                path = %self.runtime_paths.store.staging.display(),
                "created the runtime manager's staging root"
            ),
            Some(Err(error)) => tracing::warn!(
                %error,
                path = %self.runtime_paths.store.staging.display(),
                "could not prepare the runtime manager's staging root"
            ),
            Some(Ok(false)) | None => {}
        }
        match cleared {
            Ok(0) => {}
            Ok(cleared) => tracing::info!(cleared, "cleared interrupted artifact store work"),
            Err(error) => {
                tracing::warn!(%error, "could not clear interrupted artifact store work");
            }
        }
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
        let catalog_dir = self.runtime_paths.store.catalog.clone();
        let inventory = self.runtime_inventory.clone();
        let slot = self.artifact_slot()?;
        let publication: Publication = self
            .off_runtime("publish artifact", move || {
                let _slot = slot;
                let catalog = RuntimeCatalog::load(&catalog_dir)
                    .map_err(|error| Status::failed_precondition(describe(&error)))?;
                // Whatever the outcome, and even if the caller is gone: a
                // retry that finds the artifact present must not keep an
                // inventory from before it landed.
                let _forget = ForgetOnDrop(&inventory);
                match &artifact {
                    Artifact::Release(release) => {
                        let pin = catalog.release(release, architecture).ok_or_else(|| {
                            Status::failed_precondition(format!(
                                "{release} has no pins for {architecture}"
                            ))
                        })?;
                        store.publish_release(pin, &staged).map_err(publish_status)
                    }
                    Artifact::Kernel(version) => {
                        let pin = catalog.kernel(version, architecture).ok_or_else(|| {
                            Status::failed_precondition(format!(
                                "kernel {version} has no pins for {architecture}"
                            ))
                        })?;
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
        let inventory = self.runtime_inventory.clone();
        let slot = self.artifact_slot()?;
        let outcome = self
            .off_runtime("remove artifact", move || {
                let _slot = slot;
                let _forget = ForgetOnDrop(&inventory);
                match &artifact {
                    Artifact::Release(release) => {
                        store.remove_release(release).map_err(remove_status)
                    }
                    Artifact::Kernel(version) => {
                        store.remove_kernel(version).map_err(remove_status)
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
