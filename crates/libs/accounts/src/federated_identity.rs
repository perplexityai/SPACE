//! Federated identity ownership types.

use std::fmt;
use std::str::FromStr;

use super::AuthParseError;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

const MAX_UPSTREAM_ID_BYTES: usize = 128;

macro_rules! upstream_id {
    (
        $(#[$meta:meta])*
        $name:ident,
        $err:literal
    ) => {
        $(#[$meta])*
        #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
        #[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
        #[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
        pub struct $name(String);

        impl $name {
            /// Parses a bounded opaque upstream identifier.
            pub fn parse(value: impl Into<String>) -> Result<Self, AuthParseError> {
                let value = value.into();
                if invalid_upstream_id(&value) {
                    return Err(AuthParseError::new($err));
                }
                Ok(Self(value))
            }

            /// Returns the upstream identifier exactly as SPACE received it.
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.debug_tuple(stringify!($name)).field(&self.0).finish()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(f)
            }
        }

        impl FromStr for $name {
            type Err = AuthParseError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::parse(value)
            }
        }

        impl TryFrom<String> for $name {
            type Error = AuthParseError;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::parse(value)
            }
        }

        impl From<$name> for String {
            fn from(value: $name) -> Self {
                value.0
            }
        }
    };
}

upstream_id!(
    /// Stable configured identity issuer for a federated SPACE principal.
    UpstreamIssuer,
    "upstream issuer is invalid"
);

upstream_id!(
    /// Upstream tenant identifier asserted by a trusted federated caller.
    ///
    /// Opaque bounded text, not a local org UUID: id formats are owned by the
    /// upstream issuer and may differ between issuers.
    UpstreamOrgId,
    "upstream org id is invalid"
);

upstream_id!(
    /// Upstream subject identifier asserted by a trusted federated caller.
    ///
    /// Opaque bounded text, not a local user UUID: an upstream subject may be an
    /// integer user id rendered as a string.
    UpstreamSubjectId,
    "upstream subject id is invalid"
);

upstream_id!(
    /// Configured audience shared by federated grant writers and consumers.
    FederatedGrantAudience,
    "federated grant audience is invalid"
);

/// Source of sandbox tenant ownership.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum SandboxOwnerSource {
    Local,
    FederatedIdentity,
}

impl SandboxOwnerSource {
    /// Returns the persisted owner source label.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::FederatedIdentity => "federated_identity",
        }
    }
}

impl fmt::Display for SandboxOwnerSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_str().fmt(f)
    }
}

impl FromStr for SandboxOwnerSource {
    type Err = AuthParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "local" => Ok(Self::Local),
            "federated_identity" => Ok(Self::FederatedIdentity),
            _ => Err(AuthParseError::new("sandbox owner source is invalid")),
        }
    }
}

impl TryFrom<String> for SandboxOwnerSource {
    type Error = AuthParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::from_str(&value)
    }
}

impl From<SandboxOwnerSource> for String {
    fn from(value: SandboxOwnerSource) -> Self {
        value.as_str().to_string()
    }
}

#[cfg(feature = "sqlx")]
impl sqlx::Type<sqlx::Postgres> for SandboxOwnerSource {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <str as sqlx::Type<sqlx::Postgres>>::type_info()
    }

    fn compatible(ty: &sqlx::postgres::PgTypeInfo) -> bool {
        <str as sqlx::Type<sqlx::Postgres>>::compatible(ty)
    }
}

#[cfg(feature = "sqlx")]
impl<'q> sqlx::Encode<'q, sqlx::Postgres> for SandboxOwnerSource {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        <&str as sqlx::Encode<'q, sqlx::Postgres>>::encode(self.as_str(), buf)
    }
}

#[cfg(feature = "sqlx")]
impl<'r> sqlx::Decode<'r, sqlx::Postgres> for SandboxOwnerSource {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let raw = <&str as sqlx::Decode<'r, sqlx::Postgres>>::decode(value)?;
        raw.parse::<Self>().map_err(Into::into)
    }
}

fn invalid_upstream_id(value: &str) -> bool {
    value.is_empty()
        || value.len() > MAX_UPSTREAM_ID_BYTES
        || value
            .chars()
            .any(|ch| ch.is_control() || is_bidi_control(ch))
}

fn is_bidi_control(ch: char) -> bool {
    matches!(ch, '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}')
}

#[cfg(test)]
mod tests;
