//! Identity newtypes for Volume Manager artifacts.
//!
//! A multi-file artifact transfer is identified by a caller-minted
//! [`ArtifactId`], and each file within it by an [`ArtifactEntryRole`].
//! The role is bound into each file's per-chunk AEAD AAD (see
//! [`crate::crypto::entry_aad`]) so two entries sharing the artifact's
//! DEK cannot be substituted for one another. The artifact id is NOT
//! bound into the wrap AAD — cross-artifact non-interchangeability
//! comes from per-DEK isolation (one fresh `IssueDek` per artifact),
//! so a caller that lost the original `artifact_id` can still recover
//! the ciphertext with the persisted wrap.
//!
//! The persisted, executed artifact record itself is
//! [`crate::executor::ArtifactPayload`] (serialized into the on-node job
//! queue); these are the typed pieces of its identity.

use uuid::Uuid;

/// Stable identity of a multi-file artifact. Minted by the caller that
/// owns the artifact's lifecycle and supplied to Volume Manager on both
/// the push and the pull. Volume Manager treats it as the job dedup
/// key: a retry is idempotent and recovers the same persisted wrapped
/// DEK. Intentionally NOT bound into the encryption AAD —
/// cross-artifact ciphertext replay is prevented by per-DEK isolation
/// (each artifact gets a fresh `IssueDek`), so an entry's ciphertext
/// can't be replayed against a different artifact even though the
/// wrap AADs match. The pull side therefore doesn't need the same
/// `ArtifactId` to reproduce the AAD; it only needs the persisted
/// wrap.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ArtifactId(Uuid);

/// `ArtifactId` parse failure (malformed UUID on the wire).
#[derive(Debug, thiserror::Error)]
#[error("invalid artifact id {input:?}: {source}")]
pub struct ArtifactIdParseError {
    input: String,
    #[source]
    source: uuid::Error,
}

impl ArtifactId {
    /// Mint a fresh sortable identity for a new artifact. The caller
    /// that owns the artifact's lifecycle uses this to create the id it
    /// then supplies to Volume Manager. No `Default`: a randomly-minted
    /// id is not a meaningful zero value, matching the `OrgId`/`SandboxId`
    /// newtype convention.
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    /// Wrap an existing UUID (e.g. one read back from a persisted
    /// pull-side record).
    pub const fn from_uuid(value: Uuid) -> Self {
        Self(value)
    }

    /// The underlying UUID; `as_bytes()` on the result is what binds
    /// into the AAD.
    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

impl std::fmt::Display for ArtifactId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::str::FromStr for ArtifactId {
    type Err = ArtifactIdParseError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(s)
            .map(Self)
            .map_err(|source| ArtifactIdParseError {
                input: s.to_string(),
                source,
            })
    }
}

/// Maximum byte length of an [`ArtifactEntryRole`]. Bounded because
/// the role is bound verbatim into a per-entry AEAD AAD; an unbounded
/// caller-supplied label has no business being arbitrarily long.
const MAX_ENTRY_ROLE_LEN: usize = 64;

/// The role/identity of a single entry within an artifact set — the
/// file's name/role in the set (e.g. `memfile`, `rootfs`,
/// `metadata.json`). Opaque to Volume Manager: it is bound into the
/// per-entry encryption AAD so that two entries sharing one artifact
/// DEK are **not** cryptographically interchangeable (a `memfile`
/// ciphertext fed into the `rootfs` slot fails authentication), but
/// vol-mgr never interprets its meaning — the caller derives it from
/// its own artifact layout.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ArtifactEntryRole(String);

/// `ArtifactEntryRole` validation failure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ArtifactEntryRoleError {
    /// Empty role — every entry must carry a distinguishing label.
    #[error("artifact entry role must not be empty")]
    Empty,
    /// Longer than [`MAX_ENTRY_ROLE_LEN`] bytes.
    #[error("artifact entry role exceeds {MAX_ENTRY_ROLE_LEN} bytes")]
    TooLong,
    /// Contains a NUL byte, which would collide with the NUL
    /// separators used to frame the AAD.
    #[error("artifact entry role must not contain a NUL byte")]
    ContainsNul,
}

impl ArtifactEntryRole {
    /// Parse and validate a role label at the wire boundary.
    pub fn parse(raw: impl Into<String>) -> Result<Self, ArtifactEntryRoleError> {
        let raw = raw.into();
        if raw.is_empty() {
            return Err(ArtifactEntryRoleError::Empty);
        }
        if raw.len() > MAX_ENTRY_ROLE_LEN {
            return Err(ArtifactEntryRoleError::TooLong);
        }
        if raw.as_bytes().contains(&0) {
            return Err(ArtifactEntryRoleError::ContainsNul);
        }
        Ok(Self(raw))
    }

    /// The validated label; `as_bytes()` on this is what binds into
    /// the per-entry AAD.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::str::FromStr for ArtifactEntryRole {
    type Err = ArtifactEntryRoleError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

#[cfg(test)]
mod tests;
