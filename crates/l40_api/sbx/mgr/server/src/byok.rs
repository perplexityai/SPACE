//! Shared BYOK plumbing for the encrypted-volume flows.
//!
//! Both encrypted-volume flows — the live rootfs ([`crate::rootfs_encryption`])
//! and the suspend/restore snapshot volume ([`crate::snapshot_encryption`]) —
//! key off the sandbox's **immutable `volume_encryption` flag**: stamped by the
//! control plane at create, persisted on the local `sandboxes` row, and carried
//! across nodes by the snapshot metadata (`SnapshotMeta.encrypted`) and the
//! transfer headers. Call sites read that local state directly; no service
//! round-trip re-decides encryption for an existing sandbox, so a
//! credential-service outage cannot affect plaintext sandboxes (warm-pool
//! members included — they are stamped plaintext at create). The warm-pool
//! composite shape ([`crate::rootfs_encryption`]'s pool arm) does add a
//! node-local node-mgr round-trip for selected plaintext sandboxes, but that
//! is a device-layout choice — no key material and no credential-service
//! dependency is involved.
//!
//! When the flag is set, failures preparing encrypted volumes are HARD errors:
//! silently launching a BYOK-mandated sandbox on plaintext storage would be an
//! at-rest breach, so callers fail closed.
//!
//! The DEK itself never passes through sbx-mgr: node-mgr (the privileged
//! daemon hosting the encrypted-volume delegation RPCs) self-serves the
//! sandbox's single volume DEK from credmgr keyed by sandbox_id. The org→KEK
//! resolution and the wrap storage live entirely behind the credential
//! service.

/// Root of the per-sandbox on-disk layout the encrypted-volume backing files
/// live under (`<CACHE_DIR>/<sandbox_id>/instance/...`).
pub(crate) const CACHE_DIR: &str = pplx_space_common_core::host::linux::paths::SANDBOX_CACHE_DIR;
