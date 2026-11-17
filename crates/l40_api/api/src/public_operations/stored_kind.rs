//! The kind a `public_operations` row records: a public [`OperationKind`], or
//! a kind only the system enqueues, which no client sees under its own name.

use std::fmt;
use std::str::FromStr;

use pplx_space_public_api::{OperationKind, PublicEnumParseError};

use super::TEARDOWN_KINDS;

/// The kind of a stored operation.
///
/// The public contract freezes [`OperationKind`]'s values, and generated
/// clients read them as a closed enum, so a system kind is rendered to
/// clients as the public kind it does the work of ([`Self::public_kind`]):
/// in the operation record, in the `kind` its events and terminal errors
/// carry, and nowhere under its stored name. The admin API, metrics and
/// logs use the stored name ([`Self::as_str`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum StoredOperationKind {
    Public(OperationKind),
    /// A forced node drain's restore of a sandbox it suspended, toward the
    /// lifecycle the sandbox had. Only the system enqueues it, and clients
    /// see a `restore_sandbox`.
    DrainRestoreSandbox,
}

impl StoredOperationKind {
    const DRAIN_RESTORE_SANDBOX: &'static str = "drain_restore_sandbox";

    /// The stored value, which [`FromStr`] accepts back.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Public(kind) => kind.as_str(),
            Self::DrainRestoreSandbox => Self::DRAIN_RESTORE_SANDBOX,
        }
    }

    /// The kind a client sees.
    pub const fn public_kind(self) -> OperationKind {
        match self {
            Self::Public(kind) => kind,
            Self::DrainRestoreSandbox => OperationKind::RestoreSandbox,
        }
    }

    /// The public kind this is, or `None` for a system kind.
    pub const fn as_public(self) -> Option<OperationKind> {
        match self {
            Self::Public(kind) => Some(kind),
            Self::DrainRestoreSandbox => None,
        }
    }

    /// Whether this is a teardown ([`TEARDOWN_KINDS`]).
    pub fn is_teardown(self) -> bool {
        self.as_public()
            .is_some_and(|kind| TEARDOWN_KINDS.contains(&kind))
    }
}

impl From<OperationKind> for StoredOperationKind {
    fn from(kind: OperationKind) -> Self {
        Self::Public(kind)
    }
}

impl PartialEq<OperationKind> for StoredOperationKind {
    fn eq(&self, other: &OperationKind) -> bool {
        *self == Self::Public(*other)
    }
}

impl fmt::Display for StoredOperationKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Strict: a public kind, or a system kind's stored name.
impl FromStr for StoredOperationKind {
    type Err = PublicEnumParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value == Self::DRAIN_RESTORE_SANDBOX {
            return Ok(Self::DrainRestoreSandbox);
        }
        value.parse().map(Self::Public)
    }
}

impl serde::Serialize for StoredOperationKind {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

#[cfg(test)]
mod tests;
