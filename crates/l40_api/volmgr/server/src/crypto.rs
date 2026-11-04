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
//! key lifecycle (credmgr `IssueDek`/`UnwrapDek`) and decides the AAD;
//! `common-io` only knows "encrypt these bytes under this key+AAD".

pub use pplx_space_common_io::fs::{
    CHUNK_SIZE, CryptoError, HEADER_LEN, KEY_LEN, decrypt_file_blocking, decrypt_stream_blocking,
    decrypted_sibling, encrypt_file_blocking, encrypt_stream_blocking, encrypted_sibling,
};

use pplx_space_common_core::types::{OrgId, SandboxId};

use crate::artifact::ArtifactEntryRole;

/// Domain prefix bound into the artifact-level wrap AAD. Pins a wrapped
/// DEK to a specific `(org, sandbox)` pair, so a wrap issued for org A's
/// sandbox X cannot be misapplied to org B or to sandbox Y even if the
/// rows were swapped by mistake. Cross-artifact non-interchangeability
/// inside one `(org, sandbox)` rides on per-DEK isolation (each artifact
/// gets a fresh DEK from credmgr `IssueDek`), not on this prefix.
const WRAP_AAD_PREFIX: &[u8] = b"space-artifact-wrap/v1";

/// AAD bound into an artifact's wrapped DEK at credmgr `IssueDek` /
/// `UnwrapDek` time.
///
/// Layout: `WRAP_AAD_PREFIX || 0x00 || org_id(16) || sandbox_id(16)`.
/// The NUL separator prevents prefix-collision attacks
/// even though every id is a fixed-length UUID. Deterministic from
/// `(org_id, sandbox_id)` — the same bytes are produced on push and on
/// pull without the caller needing to persist or thread artifact-scoped
/// state through cluster-api's sidecar.
///
/// `artifact_id` is intentionally NOT in the AAD. Two artifacts within
/// the same `(org, sandbox)` already have independent DEKs (one fresh
/// `IssueDek` per artifact), so AEAD's natural per-DEK isolation
/// prevents cross-artifact ciphertext swaps — the wrap bytes for
/// artifact A wouldn't even unwrap as DEK_B, and DEK_A can't decrypt
/// artifact B's ciphertext. Binding `artifact_id` into the wrap AAD
/// would be redundant defense at the cost of forcing every caller to
/// persist `artifact_id` alongside the wrap.
///
/// This binds the *DEK*, not individual files. The per-chunk AEAD for
/// each file in the artifact is bound to [`entry_aad`], which extends
/// this with the file's role so two files sharing the artifact DEK are
/// not cryptographically interchangeable.
pub fn wrap_aad(org_id: OrgId, sandbox_id: &SandboxId) -> Vec<u8> {
    let mut out = Vec::with_capacity(WRAP_AAD_PREFIX.len() + 1 + 16 + 16);
    out.extend_from_slice(WRAP_AAD_PREFIX);
    out.push(0);
    out.extend_from_slice(org_id.as_uuid().as_bytes());
    out.extend_from_slice(sandbox_id.as_uuid().as_bytes());
    out
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
