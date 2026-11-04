//! In-process stub [`KeyWrapper`] for tests and explicit local fixtures.
//!
//! KEKs are random 32-byte AES-256 keys generated lazily from the OS
//! RNG and held only in this process's heap. Handles only
//! [`KekProvider::InMemory`].
//!
//! **NOT FOR PRODUCTION.** A process restart loses every KEK ever generated and
//! invalidates every wrapped DEK ever issued. Wrapped DEKs do not survive past
//! process lifetime, and separate processes cannot unwrap each other's output.
//! The credential-manager service binary installs only production backends;
//! tests that need this stub must construct it explicitly.
//!
//! Cross-tenant safety is structural: KEKs are keyed on
//! `(OrgId, MasterKeyIdentifier)`, so an attacker who lifts a
//! `wrapped_dek` from org A's storage cannot unwrap it while claiming
//! org B — the lookup misses, or the resolved KEK is different
//! material and AEAD auth fails.

use std::collections::HashMap;
use std::sync::RwLock;

use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use async_trait::async_trait;
use pplx_space_common_svc::core::types::{KekProvider, MasterKeyIdentifier, OrgId, SandboxId};

use crate::key_wrapper::{DekKeySpec, GenerateDekOutput, KeyWrapper, UnwrapError, WrapError};

/// AES-GCM nonce length (96-bit, the only widely supported size).
const NONCE_LEN: usize = 12;

/// AES-GCM authentication tag length (128-bit).
const TAG_LEN: usize = 16;

/// In-memory `KeyWrapper` backend. See module docs for the
/// "not for production" caveats.
pub struct InMemoryKeyWrapper {
    /// `(OrgId, MasterKeyIdentifier) → 32-byte KEK`. `RwLock` because
    /// reads (the hot path) vastly outnumber writes (one per new
    /// `(org, key)`). Never held across `.await`.
    keks: RwLock<HashMap<(OrgId, MasterKeyIdentifier), [u8; 32]>>,
}

impl Default for InMemoryKeyWrapper {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryKeyWrapper {
    /// Construct an empty wrapper. KEKs are generated on demand.
    pub fn new() -> Self {
        Self {
            keks: RwLock::new(HashMap::new()),
        }
    }

    /// Look up an existing KEK or generate one from the OS RNG and
    /// cache it. Used by `generate_and_wrap_dek`.
    fn get_or_create_kek(&self, org: OrgId, key_id: &MasterKeyIdentifier) -> [u8; 32] {
        let map_key = (org, key_id.clone());
        // Common case: KEK already present. Fast path under read lock.
        if let Some(key) = self
            .keks
            .read()
            .expect("KEK map RwLock poisoned")
            .get(&map_key)
        {
            return *key;
        }
        // Slow path: insert under write lock. Re-check under the write
        // lock so a concurrent inserter wins exactly once.
        let mut keks = self.keks.write().expect("KEK map RwLock poisoned");
        *keks.entry(map_key).or_insert_with(|| {
            let mut key = [0u8; 32];
            rand::fill(&mut key[..]);
            key
        })
    }

    /// Look up an existing KEK without generating one. Used by
    /// `unwrap_dek` so an unwrap against an unknown `(org, key_id)` is
    /// rejected instead of silently producing a fresh KEK that would
    /// then fail AEAD auth.
    fn get_kek(&self, org: OrgId, key_id: &MasterKeyIdentifier) -> Option<[u8; 32]> {
        self.keks
            .read()
            .expect("KEK map RwLock poisoned")
            .get(&(org, key_id.clone()))
            .copied()
    }
}

#[async_trait]
impl KeyWrapper for InMemoryKeyWrapper {
    async fn generate_and_wrap_dek(
        &self,
        org: OrgId,
        _requesting_sandbox: Option<SandboxId>,
        provider: KekProvider,
        master_key_identifier: &MasterKeyIdentifier,
        spec: DekKeySpec,
        aad: &[u8],
    ) -> Result<GenerateDekOutput, WrapError> {
        if provider != KekProvider::InMemory {
            return Err(WrapError::UnsupportedProvider(provider));
        }

        // CSRNG DEK. `rand::fill` is the workspace-standard CSPRNG draw.
        let mut plaintext_dek = vec![0u8; spec.byte_len()];
        rand::fill(&mut plaintext_dek[..]);

        let kek = self.get_or_create_kek(org, master_key_identifier);
        let cipher = Aes256Gcm::new_from_slice(&kek).expect("AES-256-GCM accepts a 32-byte key");

        let mut nonce_bytes = [0u8; NONCE_LEN];
        rand::fill(&mut nonce_bytes[..]);
        let nonce = Nonce::from(nonce_bytes);
        let ciphertext = cipher
            .encrypt(
                &nonce,
                Payload {
                    msg: &plaintext_dek,
                    aad,
                },
            )
            .map_err(|e| WrapError::Backend(format!("aes-gcm encrypt: {e}")))?;

        // Wire format: nonce(12) || ciphertext_with_tag.
        let mut wrapped_dek = Vec::with_capacity(NONCE_LEN + ciphertext.len());
        wrapped_dek.extend_from_slice(&nonce_bytes);
        wrapped_dek.extend_from_slice(&ciphertext);

        Ok(GenerateDekOutput {
            plaintext_dek,
            wrapped_dek,
        })
    }

    async fn unwrap_dek(
        &self,
        org: OrgId,
        _requesting_sandbox: Option<SandboxId>,
        provider: KekProvider,
        master_key_identifier: &MasterKeyIdentifier,
        wrapped_dek: &[u8],
        aad: &[u8],
    ) -> Result<Vec<u8>, UnwrapError> {
        if provider != KekProvider::InMemory {
            return Err(UnwrapError::UnsupportedProvider(provider));
        }
        let kek = self
            .get_kek(org, master_key_identifier)
            .ok_or(UnwrapError::UnknownOrgKek)?;

        if wrapped_dek.len() < NONCE_LEN + TAG_LEN {
            return Err(UnwrapError::Malformed);
        }
        let (nonce_bytes, ciphertext) = wrapped_dek.split_at(NONCE_LEN);
        let cipher = Aes256Gcm::new_from_slice(&kek).expect("AES-256-GCM accepts a 32-byte key");
        let nonce_bytes: [u8; NONCE_LEN] =
            nonce_bytes.try_into().map_err(|_| UnwrapError::Malformed)?;
        let nonce = Nonce::from(nonce_bytes);
        cipher
            .decrypt(
                &nonce,
                Payload {
                    msg: ciphertext,
                    aad,
                },
            )
            .map_err(|_| UnwrapError::AuthFailed)
    }
}

#[cfg(test)]
mod tests;
