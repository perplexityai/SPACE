//! Privileged btrfs operations, delegated to node-manager.
//!
//! Sbxmgr runs as a non-root user with `runAsNonRoot: true` + no
//! `capabilities.add` in production — it cannot issue the
//! `BTRFS_IOC_SEND` / `BTRFS_IOC_RECEIVE` / `BTRFS_IOC_SNAP_DESTROY_V2`
//! ioctls the suspend codec depends on. Node-manager runs as the
//! privileged daemon and exposes three gRPC RPCs for the
//! suspend-codec lifecycle: encode, receive, delete.
//!
//! These helpers wrap the per-call gRPC dial + map errors. Every
//! call takes domain identifiers (`sandbox_id`, `template_name`,
//! `content_hash`); nodemgr derives canonical host paths internally
//! so callers can't smuggle arbitrary paths.

use pplx_space_node_mgr_client::execution_fence::{ExecutionFence, InstanceTarget};
use pplx_space_node_mgr_client::proto::{
    BtrfsDeleteSandboxInstanceRequest, BtrfsEncodeRootfsSendRequest,
    BtrfsEncodeSnapshotRootfsSendRequest, BtrfsInspectSendSourceRequest,
    BtrfsReceiveInstanceRequest, GetBtrfsSendCapabilitiesRequest,
};
use thiserror::Error;
use tonic::Status;

use crate::node_manager_transport::NodeManagerConnector;

// `pub` only because `RestoreError::BtrfsReceive` carries it and `RestoreError`
// is public; the module is `pub(crate)`, so the type is still not exported.
#[derive(Debug, Error)]
pub enum BtrfsViaNodemgrError {
    #[error("connect to node-manager: {0}")]
    Connect(String),
    #[error("node-manager rpc {rpc} failed: {status}")]
    Rpc {
        rpc: &'static str,
        #[source]
        status: tonic::Status,
    },
    #[error("node-manager rpc {rpc} returned an unusable response: {reason}")]
    InvalidResponse {
        rpc: &'static str,
        reason: &'static str,
    },
}

impl BtrfsViaNodemgrError {
    /// Convert the error back to a `tonic::Status` for propagation
    /// to sbxmgr's gRPC callers. Preserves nodemgr's
    /// `FailedPrecondition` codes (used for codec precondition
    /// mismatches the caller can retry with format=full) and
    /// collapses transport / unspecified failures to `Internal`.
    pub(crate) fn into_status(self) -> Status {
        match self {
            BtrfsViaNodemgrError::Connect(reason) => {
                Status::internal(format!("node-manager unreachable: {reason}"))
            }
            BtrfsViaNodemgrError::Rpc { rpc: _, status } => status,
            // Not FailedPrecondition: a deferred capture has already removed
            // rootfs.ext4, so falling back to the Full codec cannot help.
            error @ BtrfsViaNodemgrError::InvalidResponse { .. } => {
                Status::internal(error.to_string())
            }
        }
    }
}

/// What a btrfs-send capture left in the artifact directory.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CapturedRootfs {
    /// `rootfs.btrfs-send` was written. A retained source, if any, only
    /// regenerates the stream after it is reclaimed.
    Stream {
        stream_bytes: u64,
        source_id: Option<uuid::Uuid>,
    },
    /// No stream was written; volume-manager streams from the retained source.
    Deferred {
        source_id: uuid::Uuid,
        /// The retained rootfs image length, not a stream length.
        rootfs_bytes: u64,
    },
}

impl CapturedRootfs {
    /// Capability probes are advisory, so validate what node-manager actually
    /// did. A deferral without the retained size fails rather than becoming a
    /// zero-sized artifact. A materialized capture needs no size, so an older
    /// node-manager's response stays valid.
    fn from_response(
        rpc: &'static str,
        defer_requested: bool,
        stream_bytes: u64,
        source_id: Option<&str>,
        rootfs_bytes: Option<u64>,
    ) -> Result<Self, BtrfsViaNodemgrError> {
        let invalid = |reason| BtrfsViaNodemgrError::InvalidResponse { rpc, reason };
        let source_id = source_id
            .map(str::parse::<uuid::Uuid>)
            .transpose()
            .map_err(|_| invalid("invalid retained source identity"))?;
        // A btrfs send stream always has a header, so zero bytes with a
        // retained source is node-manager's deferral acknowledgement.
        match source_id {
            Some(source_id) if defer_requested && stream_bytes == 0 => Ok(Self::Deferred {
                source_id,
                rootfs_bytes: rootfs_bytes
                    .ok_or_else(|| invalid("deferred capture omitted the retained rootfs size"))?,
            }),
            source_id => Ok(Self::Stream {
                stream_bytes,
                source_id,
            }),
        }
    }

    pub(crate) fn source_id(self) -> Option<uuid::Uuid> {
        match self {
            Self::Stream { source_id, .. } => source_id,
            Self::Deferred { source_id, .. } => Some(source_id),
        }
    }

    pub(crate) fn stream_bytes(self) -> u64 {
        match self {
            Self::Stream { stream_bytes, .. } => stream_bytes,
            Self::Deferred { .. } => 0,
        }
    }
}

/// Encode a sandbox's rootfs as a `btrfs send -p` stream via
/// node-manager. The output stream lands in `snapshot_dir`; nodemgr also
/// cleans up the intermediate `instance.ro` subvolume on success.
pub(crate) async fn encode_rootfs_send(
    sandbox_id: &str,
    template_name: &str,
    content_hash: &str,
    snapshot_dir: &str,
    execution_fence: ExecutionFence,
    node_manager: &NodeManagerConnector,
) -> Result<CapturedRootfs, BtrfsViaNodemgrError> {
    const RPC: &str = "BtrfsEncodeRootfsSend";
    let mut client = node_manager
        .connect()
        .await
        .map_err(|e| BtrfsViaNodemgrError::Connect(e.to_string()))?;
    let defer = defer_send(node_manager).await;
    let response = client
        .btrfs_encode_rootfs_send(BtrfsEncodeRootfsSendRequest {
            sandbox_id: sandbox_id.to_string(),
            template_name: template_name.to_string(),
            content_hash: content_hash.to_string(),
            dest_dir: Some(snapshot_dir.to_string()),
            execution_fence: Some(execution_fence.into()),
            retain_source: retain_send_sources(),
            defer_send: defer,
        })
        .await
        .map_err(|status| BtrfsViaNodemgrError::Rpc { rpc: RPC, status })?
        .into_inner();
    CapturedRootfs::from_response(
        RPC,
        defer,
        response.stream_bytes,
        response.source_id.as_deref(),
        response.rootfs_bytes,
    )
}

/// Encode an already-created immutable named snapshot. This is distinct from
/// [`encode_rootfs_send`], which captures the live instance itself.
///
/// Always materialized, never retained: these artifacts have no local sidecar
/// to persist a source identity, and their stream is never reclaimed.
pub(crate) async fn encode_snapshot_rootfs_send(
    sandbox_id: &str,
    template_name: &str,
    content_hash: &str,
    snapshot_name: &str,
    dest_dir: &str,
    node_manager: &NodeManagerConnector,
) -> Result<u64, BtrfsViaNodemgrError> {
    let mut client = node_manager
        .connect()
        .await
        .map_err(|e| BtrfsViaNodemgrError::Connect(e.to_string()))?;
    let response = client
        .btrfs_encode_snapshot_rootfs_send(BtrfsEncodeSnapshotRootfsSendRequest {
            sandbox_id: sandbox_id.to_string(),
            template_name: template_name.to_string(),
            content_hash: content_hash.to_string(),
            snapshot_name: snapshot_name.to_string(),
            dest_dir: dest_dir.to_string(),
            retain_source: false,
            defer_send: false,
        })
        .await
        .map_err(|status| BtrfsViaNodemgrError::Rpc {
            rpc: "BtrfsEncodeSnapshotRootfsSend",
            status,
        })?
        .into_inner();
    Ok(response.stream_bytes)
}

/// Consume a `rootfs.btrfs-send` stream from
/// `<sandbox_dir>/snapshot/rootfs.btrfs-send` and reconstruct the
/// sandbox's `instance/` subvolume against the local template via
/// `btrfs receive`. Nodemgr also cleans up stale `instance` /
/// `instance.ro` from prior failed attempts, renames the received
/// subvolume to `instance/`, and toggles RW so FC can write.
pub(crate) async fn receive_instance(
    sandbox_id: &str,
    template_name: &str,
    content_hash: &str,
    target: InstanceTarget,
    node_manager: &NodeManagerConnector,
) -> Result<(), BtrfsViaNodemgrError> {
    let mut client = node_manager
        .connect()
        .await
        .map_err(|e| BtrfsViaNodemgrError::Connect(e.to_string()))?;
    client
        .btrfs_receive_instance(BtrfsReceiveInstanceRequest {
            sandbox_id: sandbox_id.to_string(),
            template_name: template_name.to_string(),
            content_hash: content_hash.to_string(),
            target: Some(target.into()),
        })
        .await
        .map_err(|status| BtrfsViaNodemgrError::Rpc {
            rpc: "BtrfsReceiveInstance",
            status,
        })?;
    Ok(())
}

/// Subvolume-aware delete of a sandbox's `instance/` (and any stray
/// `instance.ro` from a crashed prior encode). Used by failure-path
/// cleanups in the restore dispatcher.
pub(crate) async fn delete_sandbox_instance(
    sandbox_id: &str,
    target: InstanceTarget,
    node_manager: &NodeManagerConnector,
) -> Result<(), BtrfsViaNodemgrError> {
    let mut client = node_manager
        .connect()
        .await
        .map_err(|e| BtrfsViaNodemgrError::Connect(e.to_string()))?;
    client
        .btrfs_delete_sandbox_instance(BtrfsDeleteSandboxInstanceRequest {
            sandbox_id: sandbox_id.to_string(),
            target: Some(target.into()),
        })
        .await
        .map_err(|status| BtrfsViaNodemgrError::Rpc {
            rpc: "BtrfsDeleteSandboxInstance",
            status,
        })?;
    Ok(())
}

/// Capture policy is separate from recovery: disabling new captures must not
/// prevent an already-published source from being regenerated.
fn retain_send_sources() -> bool {
    static ENABLED: std::sync::LazyLock<bool> = std::sync::LazyLock::new(|| {
        std::env::var("SPACE_SNAPSHOT_STREAM_BTRFS_SEND").is_ok_and(|value| value == "true")
    });
    *ENABLED
}

pub(crate) async fn materialize_rootfs_send(
    sandbox_id: &str,
    artifact_dir: &std::path::Path,
    source_id: uuid::Uuid,
    node_manager: &NodeManagerConnector,
) -> Result<u64, BtrfsViaNodemgrError> {
    let mut client = node_manager
        .connect()
        .await
        .map_err(|e| BtrfsViaNodemgrError::Connect(e.to_string()))?;
    client
        .btrfs_materialize_rootfs_send(
            pplx_space_node_mgr_client::proto::BtrfsMaterializeRootfsSendRequest {
                sandbox_id: sandbox_id.to_owned(),
                artifact_dir: artifact_dir.to_string_lossy().into_owned(),
                source_id: source_id.to_string(),
            },
        )
        .await
        .map(|response| response.into_inner().stream_bytes)
        .map_err(|status| BtrfsViaNodemgrError::Rpc {
            rpc: "BtrfsMaterializeRootfsSend",
            status,
        })
}

/// Read-only check of a retained source. Node-manager maps an absent bundle to
/// `NotFound` and an unusable one to `FailedPrecondition`; any other status,
/// including `Unimplemented` from an older node-manager, proves nothing.
pub(crate) async fn inspect_send_source(
    sandbox_id: &str,
    artifact_dir: &std::path::Path,
    source_id: uuid::Uuid,
    node_manager: &NodeManagerConnector,
) -> Result<u64, BtrfsViaNodemgrError> {
    // Inspections are frequent and bounded; they share node-manager's lazy,
    // reconnecting channel instead of dialing the socket per call. A dead
    // socket surfaces as an RPC status, which proves nothing about the source.
    node_manager
        .lazy()
        .btrfs_inspect_send_source(BtrfsInspectSendSourceRequest {
            sandbox_id: sandbox_id.to_owned(),
            artifact_dir: artifact_dir.to_string_lossy().into_owned(),
            source_id: source_id.to_string(),
        })
        .await
        .map(|response| response.into_inner().rootfs_bytes)
        .map_err(|status| BtrfsViaNodemgrError::Rpc {
            rpc: "BtrfsInspectSendSource",
            status,
        })
}

const CAPABILITY_PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);
/// How long a capability answer is reused. Capabilities change only with a
/// peer's deployment, so every capture and backup need not pay a round trip;
/// a peer that answered negatively or not at all is asked again sooner.
const CAPABILITY_CACHE_TTL: std::time::Duration = std::time::Duration::from_secs(30);
const CAPABILITY_RETRY_TTL: std::time::Duration = std::time::Duration::from_secs(5);

/// A capability answer with the moment it was obtained.
pub(crate) struct CapabilityCache(std::sync::Mutex<Option<(std::time::Instant, bool)>>);

impl CapabilityCache {
    pub(crate) const fn new() -> Self {
        Self(std::sync::Mutex::new(None))
    }

    /// The cached answer while it is fresh, otherwise `probe`'s, cached.
    pub(crate) async fn get<F>(&self, probe: F) -> bool
    where
        F: std::future::Future<Output = bool>,
    {
        let now = std::time::Instant::now();
        if let Some((at, value)) = *self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
        {
            let ttl = if value {
                CAPABILITY_CACHE_TTL
            } else {
                CAPABILITY_RETRY_TTL
            };
            if now.duration_since(at) < ttl {
                return value;
            }
        }
        let value = probe.await;
        *self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            Some((std::time::Instant::now(), value));
        value
    }

    #[cfg(test)]
    pub(crate) fn clear(&self) {
        *self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
    }
}

static VOLUME_MANAGER_STREAMS: CapabilityCache = CapabilityCache::new();

/// Whether volume-manager can stream an upload from a retained source. Gates
/// existing retained artifacts too: without it they materialize first.
pub(crate) async fn volume_manager_streams_btrfs_send() -> bool {
    VOLUME_MANAGER_STREAMS
        .get(probe_volume_manager_streams_btrfs_send())
        .await
}

async fn probe_volume_manager_streams_btrfs_send() -> bool {
    let probe = async {
        let mut client = pplx_space_volmgr_client::connect(pplx_space_volmgr_client::VOLMGR_SOCKET)
            .await
            .map_err(|error| tonic::Status::unavailable(error.to_string()))?;
        client
            .get_push_capabilities(pplx_space_volmgr_client::proto::GetPushCapabilitiesRequest {})
            .await
            .map(|response| response.into_inner().btrfs_send)
    };
    match tokio::time::timeout(CAPABILITY_PROBE_TIMEOUT, probe).await {
        Ok(Ok(enabled)) => enabled,
        result => {
            tracing::debug!(
                ?result,
                "using materialized send without streaming capability"
            );
            false
        }
    }
}

/// Whether node-manager can inspect retained sources and report their size.
/// A deferred artifact is unverifiable without both.
async fn node_manager_inspects_send_sources(node_manager: &NodeManagerConnector) -> bool {
    node_manager
        .inspects_send_sources_cache()
        .get(probe_node_manager_inspects_send_sources(node_manager))
        .await
}

async fn probe_node_manager_inspects_send_sources(node_manager: &NodeManagerConnector) -> bool {
    let probe = async {
        let mut client = node_manager.lazy();
        client
            .get_btrfs_send_capabilities(GetBtrfsSendCapabilitiesRequest {})
            .await
            .map(|response| response.into_inner().inspect_send_source)
    };
    match tokio::time::timeout(CAPABILITY_PROBE_TIMEOUT, probe).await {
        Ok(Ok(supported)) => supported,
        result => {
            tracing::debug!(
                ?result,
                "using materialized send without retained-source inspection"
            );
            false
        }
    }
}

async fn defer_send(node_manager: &NodeManagerConnector) -> bool {
    if !retain_send_sources() {
        return false;
    }
    let (streams, inspects) = tokio::join!(
        volume_manager_streams_btrfs_send(),
        node_manager_inspects_send_sources(node_manager)
    );
    streams && inspects
}

#[cfg(test)]
mod capability_cache_tests;
#[cfg(test)]
mod tests;
