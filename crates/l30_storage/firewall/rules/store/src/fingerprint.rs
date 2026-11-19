//! The fingerprint that binds a mutation receipt to the request it answers.

use sha2::{Digest as _, Sha256};

/// Hashed first, so a later encoding can't produce an earlier one's digests.
const ENCODING: &[u8] = b"firewall-rules-mutation-request/v1";

/// One operator mutation as its fingerprint reads it. Every field is the
/// caller's canonical form: never raw HTTP formatting, credentials, trace IDs
/// or an ID the mutation generates.
#[derive(Clone, Copy, Debug)]
pub struct MutationRequest<'a> {
    /// The HTTP method, such as `POST`.
    pub method: &'a str,
    /// The canonical operation path, including the resource it acts on.
    pub path: &'a str,
    /// The canonical body: the complete semantic request, never a diff.
    pub body: &'a [u8],
    /// The semantic `If-Match` value, such as a rollout generation.
    pub if_match: Option<&'a str>,
}

impl MutationRequest<'_> {
    /// Fields are length-prefixed and `if_match` is tagged absent or present,
    /// so no two requests share an encoding.
    pub fn fingerprint(&self) -> RequestFingerprint {
        let mut digest = Sha256::new();
        field(&mut digest, ENCODING);
        field(&mut digest, self.method.as_bytes());
        field(&mut digest, self.path.as_bytes());
        field(&mut digest, self.body);
        match self.if_match {
            None => digest.update([0]),
            Some(value) => {
                digest.update([1]);
                field(&mut digest, value.as_bytes());
            }
        }
        RequestFingerprint(hex::encode(digest.finalize()))
    }
}

fn field(digest: &mut Sha256, value: &[u8]) {
    digest.update((value.len() as u64).to_be_bytes());
    digest.update(value);
}

/// A [`MutationRequest`]'s lowercase hex SHA-256, as receipts store it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestFingerprint(String);

impl RequestFingerprint {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[cfg(test)]
mod tests;
