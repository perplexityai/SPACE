//! BYOK artifact-protection policy for vol-mgr's transfer pipeline.
//!
//! The streaming AES-256-GCM *mechanics* (the SPACEAEA wire format,
//! the chunked AEAD, the file/stream entry points) live in the
//! domain-neutral [`pplx_space_common_io::fs`] crate so other
//! consumers — and future btrfs full/diff stream handling — can reuse
//! them without depending on vol-mgr. This module re-exports them and
//! adds the BYOK-domain *policy*: which AAD binds a wrapped DEK and
//! its ciphertext to a specific identity.
//!
//! Volume Manager remains the orchestrator/authority: it owns the
//! key lifecycle (credmgr `IssueSandboxArtifactDek` /
//! `UnwrapSandboxArtifactDek`) and decides the per-entry AAD;
//! `common-io` only knows "encrypt these bytes under this key+AAD".

pub use pplx_space_common_io::fs::{
    CHUNK_SIZE, CryptoError, HEADER_LEN, KEY_LEN, decrypt_file_blocking, decrypt_stream_blocking,
    decrypted_sibling, encrypt_file_blocking, encrypt_stream_blocking, encrypted_sibling,
};

use pplx_space_common_core::types::{OrgId, SandboxId, artifact_wrap_aad};

use crate::artifact::ArtifactEntryRole;

/// AAD bound into an artifact's wrapped DEK. credmgr derives the same bytes
/// itself from the sandbox's durable owner at `IssueSandboxArtifactDek` /
/// `UnwrapSandboxArtifactDek` time; vol-mgr needs them only as the prefix of
/// each entry's [`entry_aad`]. See
/// [`pplx_space_common_core::types::artifact_wrap_aad`] for the layout.
pub fn wrap_aad(org_id: OrgId, sandbox_id: &SandboxId) -> Vec<u8> {
    artifact_wrap_aad(org_id, sandbox_id)
}

/// AAD bound into every per-chunk AES-256-GCM encryption of one file
/// (entry) within an artifact set.
///
/// Layout: `wrap_aad || 0x00 || role_bytes`, where `wrap_aad` is the
/// artifact-level AAD from [`wrap_aad`] and `role` is the entry's
/// validated role label. Because the role is bound into the chunk AEAD,
/// feeding a `memfile` ciphertext into the `rootfs` slot (same artifact,
/// same DEK) fails authentication — the entries are not interchangeable.
/// The NUL separator keeps `entry_aad` unambiguous against any artifact
/// whose UUID bytes happened to end with the role bytes.
pub fn entry_aad(wrap_aad: &[u8], role: &ArtifactEntryRole) -> Vec<u8> {
    let role = role.as_str().as_bytes();
    let mut out = Vec::with_capacity(wrap_aad.len() + 1 + role.len());
    out.extend_from_slice(wrap_aad);
    out.push(0);
    out.extend_from_slice(role);
    out
}

#[cfg(test)]
mod tests;
