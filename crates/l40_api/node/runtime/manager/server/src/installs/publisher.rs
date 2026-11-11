//! Where the runtime manager's work crosses into the node's trust: the pins
//! it checks downloads against, and node-manager, which publishes staged
//! kernels and releases into the node's root-owned store.
//!
//! The runtime manager writes only its own staging directory. A job that has
//! staged and verified an artifact asks node-manager to publish it, naming
//! it by what it is and by the job's staging directory; node-manager copies
//! the staged bytes into the store only if they match its own copy of the
//! pins. A removal is asked for the same way. Both are idempotent, so a job
//! that lost an answer asks again.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use pplx_space_common_core::types::{ComponentIdentity, Digest, RuntimeIdentity};
use pplx_space_node_mgr_client::proto as node;
use pplx_space_node_runtime_core::artifact_store::StagedName;
use pplx_space_node_runtime_core::catalog::{CatalogError, RuntimeCatalog};

use super::source::{BoxError, KernelVersion};

/// Bound on one publication or removal, connecting included: copying a
/// kernel or release is a few hundred milliseconds of disk work.
const PUBLISHER_TIMEOUT: Duration = Duration::from_secs(120);

/// What to publish or remove.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum NodeArtifactRef {
    Release(RuntimeIdentity, Digest),
    Kernel(KernelVersion, Digest),
    Boot(ComponentIdentity),
}

/// A kernel or release in the node's store.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Published {
    pub(crate) path: PathBuf,
    pub(crate) digest: Digest,
    /// The pinned bytes were already there; nothing was written.
    pub(crate) already_present: bool,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum PublisherError {
    /// node-manager will refuse the same request again: the bytes are not
    /// the pinned ones, the artifact has no pins, a guard refuses it, or the
    /// store needs an operator.
    #[error("node-manager refused: {}", .0.message())]
    Refused(Box<tonic::Status>),
    /// The request may succeed later.
    #[error("node-manager did not complete the request")]
    Unavailable(#[source] BoxError),
    #[error("node-manager answered with an invalid {field}")]
    InvalidResponse { field: &'static str },
}

impl PublisherError {
    pub(crate) const fn is_permanent(&self) -> bool {
        matches!(self, Self::Refused(_) | Self::InvalidResponse { .. })
    }
}

/// Publishes staged artifacts into the node's store and removes them.
#[async_trait::async_trait]
pub(crate) trait ArtifactPublisher: Send + Sync {
    async fn publish(
        &self,
        artifact: &NodeArtifactRef,
        staged: &StagedName,
    ) -> Result<Published, PublisherError>;

    /// Returns whether the artifact was there.
    async fn remove(&self, artifact: &NodeArtifactRef) -> Result<bool, PublisherError>;
}

/// The pins downloads are checked against.
pub(crate) trait PinSource: Send + Sync {
    /// The current catalog. Loaded on every call, so a changed catalog file
    /// takes effect without a restart, and an unsafe one refuses installs.
    fn catalog(&self) -> Result<RuntimeCatalog, CatalogError>;
}

/// The compiled pins plus the root-owned catalog files in a directory.
pub(crate) struct DirectoryCatalog(pub(crate) PathBuf);

impl PinSource for DirectoryCatalog {
    fn catalog(&self) -> Result<RuntimeCatalog, CatalogError> {
        RuntimeCatalog::load(&self.0)
    }
}

/// node-manager, over its node-local socket.
pub(crate) struct NodeManagerPublisher {
    client: pplx_space_node_mgr_client::NodeManagerGrpcClient,
}

impl NodeManagerPublisher {
    /// A publisher that connects to node-manager on first use.
    pub(crate) fn connect_lazily() -> Arc<Self> {
        Arc::new(Self {
            client: pplx_space_node_mgr_client::lazy(),
        })
    }
}

fn node_artifact(artifact: &NodeArtifactRef) -> node::NodeArtifact {
    let artifact = match artifact {
        NodeArtifactRef::Boot(identity) => {
            node::node_artifact::Artifact::Component(node::ComponentArtifact {
                kind: pplx_space_execution_common::proto::ArtifactComponentKind::from(identity.kind)
                    as i32,
                version: identity.version.to_string(),
                digest: identity.digest.to_string(),
            })
        }
        NodeArtifactRef::Release(release, digest) => {
            node::node_artifact::Artifact::RuntimeRelease(node::RuntimeReleaseArtifact {
                runtime_kind: pplx_space_execution_common::proto::RuntimeKind::from(release.kind)
                    as i32,
                version: release.version.to_string(),
                digest: digest.to_string(),
            })
        }
        NodeArtifactRef::Kernel(version, digest) => {
            node::node_artifact::Artifact::Component(node::ComponentArtifact {
                kind: pplx_space_execution_common::proto::ArtifactComponentKind::Kernel as i32,
                version: version.to_string(),
                digest: digest.to_string(),
            })
        }
    };
    node::NodeArtifact {
        artifact: Some(artifact),
    }
}

/// A status node-manager would answer the same way again is a refusal;
/// anything else may clear up. `UNIMPLEMENTED` is an older node-manager,
/// which the rollout replaces, so it is retried within the job's attempts
/// rather than failing the install at once; `RESOURCE_EXHAUSTED` is a busy
/// store.
fn classify(status: tonic::Status) -> PublisherError {
    match status.code() {
        tonic::Code::InvalidArgument
        | tonic::Code::FailedPrecondition
        | tonic::Code::NotFound
        | tonic::Code::PermissionDenied
        | tonic::Code::Unauthenticated => PublisherError::Refused(Box::new(status)),
        _ => PublisherError::Unavailable(Box::new(status)),
    }
}

async fn bounded<T>(
    call: impl std::future::Future<Output = Result<tonic::Response<T>, tonic::Status>>,
) -> Result<T, PublisherError> {
    tokio::time::timeout(PUBLISHER_TIMEOUT, call)
        .await
        .map_err(|elapsed| PublisherError::Unavailable(Box::new(elapsed)))?
        .map(tonic::Response::into_inner)
        .map_err(classify)
}

#[async_trait::async_trait]
impl ArtifactPublisher for NodeManagerPublisher {
    async fn publish(
        &self,
        artifact: &NodeArtifactRef,
        staged: &StagedName,
    ) -> Result<Published, PublisherError> {
        let mut client = self.client.clone();
        let response = bounded(client.publish_artifact(node::PublishArtifactRequest {
            artifact: Some(node_artifact(artifact)),
            staging_name: staged.as_str().to_string(),
        }))
        .await?;
        Ok(Published {
            path: PathBuf::from(response.path),
            digest: response
                .digest
                .parse()
                .map_err(|_| PublisherError::InvalidResponse { field: "digest" })?,
            already_present: response.already_present,
        })
    }

    async fn remove(&self, artifact: &NodeArtifactRef) -> Result<bool, PublisherError> {
        let mut client = self.client.clone();
        let response = bounded(client.remove_artifact(node::RemoveArtifactRequest {
            artifact: Some(node_artifact(artifact)),
        }))
        .await?;
        Ok(response.removed)
    }
}

#[cfg(test)]
mod tests;
