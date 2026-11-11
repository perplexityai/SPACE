//! Single-shot AES-256-GCM sealing for small payloads (credential values,
//! injection-rule blobs). This is the in-process half of the `Encrypt` /
//! `Decrypt` RPCs: the DEK is generated, used, and dropped inside credmgr,
//! so only the wrapped form ever crosses a wire.
//!
//! Wire format: a version byte (1), a 12-byte nonce, then the AES-256-GCM
//! ciphertext and tag. Bulk artifacts use the streaming `SPACEAEA` format
//! in vol-mgr instead; this format is deliberately minimal because payloads
//! are capped well below one GCM invocation's limits.

use aes_gcm::aead::{Aead, Payload};
use aes_gcm::{Aes256Gcm, Key, KeyInit, Nonce};

const VERSION: u8 = 1;
const NONCE_LEN: usize = 12;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum OpenError {
    #[error("sealed payload is too short to carry a version, nonce, and tag")]
    Malformed,
    #[error("unsupported sealed-payload version {0}")]
    UnsupportedVersion(u8),
    #[error("authentication failed (wrong DEK, AAD mismatch, or tampered ciphertext)")]
    AuthFailed,
}

/// Seal `plaintext` under a 32-byte DEK, binding `aad`.
pub fn seal(dek: &[u8; 32], plaintext: &[u8], aad: &[u8]) -> Vec<u8> {
    let cipher = Aes256Gcm::new_from_slice(dek).expect("AES-256-GCM accepts a 32-byte key");
    let mut nonce_bytes = [0u8; NONCE_LEN];
    rand::fill(&mut nonce_bytes[..]);
    let nonce = Nonce::from(nonce_bytes);
    let ciphertext = cipher
        .encrypt(
            &nonce,
            Payload {
                msg: plaintext,
                aad,
            },
        )
        .expect("AES-256-GCM encryption is infallible for in-range plaintext lengths");

    let mut sealed = Vec::with_capacity(1 + NONCE_LEN + ciphertext.len());
    sealed.push(VERSION);
    sealed.extend_from_slice(&nonce_bytes);
    sealed.extend_from_slice(&ciphertext);
    sealed
}

/// Recover the plaintext sealed by [`seal`]. The same DEK and `aad` must be
/// supplied; any mismatch or tampering is [`OpenError::AuthFailed`].
pub fn open(dek: &[u8; 32], sealed: &[u8], aad: &[u8]) -> Result<Vec<u8>, OpenError> {
    let (&version, rest) = sealed.split_first().ok_or(OpenError::Malformed)?;
    if version != VERSION {
        return Err(OpenError::UnsupportedVersion(version));
    }
    if rest.len() < NONCE_LEN {
        return Err(OpenError::Malformed);
    }
    let (nonce_bytes, ciphertext) = rest.split_at(NONCE_LEN);

    let key = Key::<Aes256Gcm>::from(*dek);
    let cipher = Aes256Gcm::new(&key);
    let nonce_bytes: [u8; NONCE_LEN] = nonce_bytes.try_into().map_err(|_| OpenError::Malformed)?;
    let nonce = Nonce::from(nonce_bytes);
    cipher
        .decrypt(
            &nonce,
            Payload {
                msg: ciphertext,
                aad,
            },
        )
        .map_err(|_| OpenError::AuthFailed)
}

/// Errors decoding the self-describing wrapped-DEK envelope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum DekEnvelopeError {
    #[error("wrapped DEK is too short to carry a version and KEK id")]
    Malformed,
    #[error("unsupported wrapped-DEK envelope version {0}")]
    UnsupportedVersion(u8),
}

/// Wrap-envelope format: a version byte (1), the 16-byte `org_master_keys`
/// id of the wrapping KEK, then the backend wrap. Callers store the result
/// verbatim and never learn which KEK it names — Decrypt reads the id back
/// out and verifies the row belongs to the asserted organization.
pub fn encode_wrapped_dek(cmk_id: uuid::Uuid, backend_wrap: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(1 + 16 + backend_wrap.len());
    encoded.push(VERSION);
    encoded.extend_from_slice(cmk_id.as_bytes());
    encoded.extend_from_slice(backend_wrap);
    encoded
}

/// Inverse of [`encode_wrapped_dek`].
pub fn decode_wrapped_dek(encoded: &[u8]) -> Result<(uuid::Uuid, &[u8]), DekEnvelopeError> {
    let (&version, rest) = encoded.split_first().ok_or(DekEnvelopeError::Malformed)?;
    if version != VERSION {
        return Err(DekEnvelopeError::UnsupportedVersion(version));
    }
    if rest.len() < 16 {
        return Err(DekEnvelopeError::Malformed);
    }
    let (id_bytes, backend_wrap) = rest.split_at(16);
    let id_bytes: [u8; 16] = id_bytes
        .try_into()
        .map_err(|_| DekEnvelopeError::Malformed)?;
    let cmk_id = uuid::Uuid::from_bytes(id_bytes);
    Ok((cmk_id, backend_wrap))
}

#[cfg(test)]
mod tests;
