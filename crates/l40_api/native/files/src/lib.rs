//! What a Native `files:*` request means to its idempotency key, for every
//! server that answers one: the fingerprint a mutation's key is bound to, and
//! the key the sandbox sees for it.
//!
//! Both are persisted or replayed by key: the binding lives in the control
//! plane's idempotency records, and the sandbox remembers the guest key. A
//! retry that lands on another server must derive the same values, so the
//! encodings here change only together with every reader of the old ones.

use pplx_space_guest_process::{GuestUserName, IdempotencyKey, ProcessParseError};
use pplx_space_public_api::{PublicApiParseError, RequestFingerprint, routes::NativeOperation};
use sha2::{Digest as _, Sha256};

/// A digest that cannot be represented as the value it stands for. Neither
/// can happen for a SHA-256 digest; the parsers are checked rather than
/// trusted.
#[derive(Debug, thiserror::Error)]
pub enum FingerprintError {
    #[error("request digest is not a request fingerprint: {0}")]
    Fingerprint(#[source] PublicApiParseError),
    #[error("derived guest key is not an idempotency key: {0}")]
    GuestKey(#[source] ProcessParseError),
}

/// The key the sandbox sees for a request bound to `key`. The sandbox
/// replays a keyed write or rename by key alone, and nothing bounds how long
/// it holds a record whose operation is still running, so its memory of a key
/// can outlast the binding. Deriving its key from the request as well means a
/// different request never reaches another one's record, however long that
/// lives, while a retry of the same request still replays its own.
pub fn guest_key(
    route: NativeOperation,
    key: &IdempotencyKey,
    fingerprint: &RequestFingerprint,
) -> Result<IdempotencyKey, FingerprintError> {
    let mut digest = RequestDigest::default();
    digest
        .field(route.path().as_bytes())
        .field(key.as_str().as_bytes())
        .field(fingerprint.as_str().as_bytes());
    IdempotencyKey::parse(format!("native-file-{}", digest.finish()?.as_str()))
        .map_err(FingerprintError::GuestKey)
}

/// The fingerprint of a `files:write`, over its decoded data messages.
pub fn write_fingerprint(
    path: &str,
    append: bool,
    parents: bool,
    user: Option<&GuestUserName>,
    chunks: &[Vec<u8>],
) -> Result<RequestFingerprint, FingerprintError> {
    let mut fingerprint = RequestDigest::default();
    fingerprint
        .field(path.as_bytes())
        .flag(append)
        .flag(parents)
        .user(user)
        .chunks(chunks);
    fingerprint.finish()
}

/// One `files:write_batch` entry as its fingerprint reads it.
#[derive(Clone, Copy, Debug)]
pub struct BatchEntry<'a> {
    pub path: &'a str,
    pub append: bool,
    pub data: &'a [u8],
}

/// The fingerprint of a `files:write_batch`, over its entries in order.
pub fn write_batch_fingerprint<'a>(
    parents: bool,
    user: Option<&GuestUserName>,
    entries: impl ExactSizeIterator<Item = BatchEntry<'a>>,
) -> Result<RequestFingerprint, FingerprintError> {
    let mut fingerprint = RequestDigest::default();
    fingerprint.flag(parents).user(user).count(entries.len());
    for entry in entries {
        fingerprint
            .field(entry.path.as_bytes())
            .flag(entry.append)
            .field(entry.data);
    }
    fingerprint.finish()
}

/// The fingerprint of a `files:mkdir`.
pub fn mkdir_fingerprint(
    path: &str,
    parents: bool,
    user: Option<&GuestUserName>,
) -> Result<RequestFingerprint, FingerprintError> {
    let mut fingerprint = RequestDigest::default();
    fingerprint.field(path.as_bytes()).flag(parents).user(user);
    fingerprint.finish()
}

/// The fingerprint of a `files:remove`.
pub fn remove_fingerprint(
    path: &str,
    recursive: bool,
    user: Option<&GuestUserName>,
) -> Result<RequestFingerprint, FingerprintError> {
    let mut fingerprint = RequestDigest::default();
    fingerprint
        .field(path.as_bytes())
        .flag(recursive)
        .user(user);
    fingerprint.finish()
}

/// The fingerprint of a `files:rename`.
pub fn rename_fingerprint(
    old_path: &str,
    new_path: &str,
    user: Option<&GuestUserName>,
) -> Result<RequestFingerprint, FingerprintError> {
    let mut fingerprint = RequestDigest::default();
    fingerprint
        .field(old_path.as_bytes())
        .field(new_path.as_bytes())
        .user(user);
    fingerprint.finish()
}

/// What a file mutation asks the sandbox to do, as the fingerprint its key is
/// bound to. Every field is framed, so no two requests share an encoding.
/// Write data is hashed from the decoded chunks the request already holds,
/// with no further copy of the payload.
#[derive(Default)]
pub struct RequestDigest(Sha256);

impl RequestDigest {
    pub fn field(&mut self, value: &[u8]) -> &mut Self {
        self.count(value.len());
        self.0.update(value);
        self
    }

    pub fn count(&mut self, value: usize) -> &mut Self {
        self.0.update((value as u64).to_be_bytes());
        self
    }

    pub fn flag(&mut self, value: bool) -> &mut Self {
        self.0.update([u8::from(value)]);
        self
    }

    pub fn user(&mut self, user: Option<&GuestUserName>) -> &mut Self {
        match user {
            Some(user) => self.flag(true).field(user.as_str().as_bytes()),
            None => self.flag(false),
        }
    }

    /// One field split across write messages.
    pub fn chunks(&mut self, chunks: &[Vec<u8>]) -> &mut Self {
        self.count(chunks.iter().map(Vec::len).sum());
        for chunk in chunks {
            self.0.update(chunk);
        }
        self
    }

    pub fn finish(self) -> Result<RequestFingerprint, FingerprintError> {
        RequestFingerprint::parse(hex::encode(self.0.finalize()))
            .map_err(FingerprintError::Fingerprint)
    }
}

#[cfg(test)]
mod tests;
