//! Privileged btrfs operations, delegated to node-manager.
//!
//! Tmplmgr runs as a non-root user with `runAsNonRoot: true` + no
//! `capabilities.add` in production — it cannot issue the
//! `BTRFS_IOC_SET_RECEIVED_SUBVOL` and related ioctls the
//! suspend codec depends on. Node-manager runs as the privileged
//! daemon and exposes three gRPC RPCs for the template-volume
//! lifecycle: create, finalize (stamp received_uuid + RO), delete.
//!
//! These helpers wrap the per-call gRPC dial + map errors into
//! `BtrfsViaNodemgrError`, which is what `volume.rs` and
//! `import_helpers.rs` consume. The wire surface is intentionally
//! narrow: every call takes domain identifiers
//! (`template_name`, `content_hash`); nodemgr derives the canonical
//! host paths internally so callers can't smuggle arbitrary paths.

use pplx_space_node_mgr_client::proto::{
    BtrfsCreateTemplateSubvolumeRequest, BtrfsDeleteTemplateSubvolumeRequest,
    BtrfsFinalizeTemplateSubvolumeRequest,
};
use std::path::PathBuf;
use thiserror::Error;
use uuid::Uuid;

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
    #[error("node-manager returned invalid received_uuid {value:?}: {error}")]
    InvalidUuid { value: String, error: String },
}

/// Create the canonical template digest dir as a btrfs subvolume.
/// Returns the path nodemgr wrote at — always derived from
/// `(name, content_hash)`, but echoed back for logging.
///
/// `AlreadyExists` from nodemgr is mapped to a successful return: the
/// dir was already created by a prior coalesced pull or a partial
/// install we adopted. Callers that need stricter create-or-fail
/// semantics should check via stat before calling.
pub async fn create_template_subvolume(
    name: &str,
    content_hash: &str,
) -> Result<PathBuf, BtrfsViaNodemgrError> {
    let mut client = pplx_space_node_mgr_client::connect()
        .await
        .map_err(|e| BtrfsViaNodemgrError::Connect(e.to_string()))?;
    match client
        .btrfs_create_template_subvolume(BtrfsCreateTemplateSubvolumeRequest {
            template_name: name.to_string(),
            content_hash: content_hash.to_string(),
        })
        .await
    {
        Ok(response) => Ok(PathBuf::from(response.into_inner().digest_dir)),
        Err(status) if status.code() == tonic::Code::AlreadyExists => {
            // The canonical digest path is the same for every call
            // with the same identifiers, so adopting an existing dir
            // is correct here: tmplmgr's per-(name, hash) lock
            // serialized this pull behind whoever created it, and
            // their cleanup-on-error guard would have wiped it if
            // their install failed.
            Ok(PathBuf::from(format!(
                "{}/{name}/{content_hash}",
                pplx_space_common_core::host::linux::paths::TEMPLATE_DIR
            )))
        }
        Err(status) => Err(BtrfsViaNodemgrError::Rpc {
            rpc: "BtrfsCreateTemplateSubvolume",
            status,
        }),
    }
}

/// Stamp the deterministic UUIDv5 `received_uuid` on the template
/// subvolume and toggle it RO. Returns the UUIDv5 nodemgr computed
/// so the caller can log it. The UUIDv5 derivation is canonical on
/// the nodemgr side; callers can recompute via
/// `pplx_space_common_core::host::btrfs::deterministic_template_received_uuid`
/// for sanity-checking.
pub async fn finalize_template_subvolume(
    name: &str,
    content_hash: &str,
) -> Result<Uuid, BtrfsViaNodemgrError> {
    let mut client = pplx_space_node_mgr_client::connect()
        .await
        .map_err(|e| BtrfsViaNodemgrError::Connect(e.to_string()))?;
    let response = client
        .btrfs_finalize_template_subvolume(BtrfsFinalizeTemplateSubvolumeRequest {
            template_name: name.to_string(),
            content_hash: content_hash.to_string(),
        })
        .await
        .map_err(|status| BtrfsViaNodemgrError::Rpc {
            rpc: "BtrfsFinalizeTemplateSubvolume",
            status,
        })?
        .into_inner();
    Uuid::parse_str(&response.received_uuid).map_err(|e| BtrfsViaNodemgrError::InvalidUuid {
        value: response.received_uuid,
        error: e.to_string(),
    })
}

/// Subvolume-aware delete of the canonical template digest dir.
/// Idempotent — returns Ok if the dir doesn't exist on nodemgr's
/// side. Coarse primitive (see proto doc): callers must ensure the
/// template is unpublished before calling.
pub async fn delete_template_subvolume(
    name: &str,
    content_hash: &str,
) -> Result<(), BtrfsViaNodemgrError> {
    let mut client = pplx_space_node_mgr_client::connect()
        .await
        .map_err(|e| BtrfsViaNodemgrError::Connect(e.to_string()))?;
    client
        .btrfs_delete_template_subvolume(BtrfsDeleteTemplateSubvolumeRequest {
            template_name: name.to_string(),
            content_hash: content_hash.to_string(),
        })
        .await
        .map_err(|status| BtrfsViaNodemgrError::Rpc {
            rpc: "BtrfsDeleteTemplateSubvolume",
            status,
        })?;
    Ok(())
}
