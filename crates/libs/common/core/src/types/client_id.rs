//! The id of the client whose activation created a sandbox's runtime.

use std::fmt;
use std::sync::LazyLock;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::types::LabelValue;

/// The id of the client whose activation created a sandbox's runtime: what an
/// ECW caller sent in `X-Client-ID`, or one of the reserved ids.
///
/// It's what's stored, in the `sandboxes.activating_client_id` record, the
/// `space.pplx.net/client-id` pod label and persisted operations. An id is
/// unbounded: a caller can send anything. Limits and metrics never use an id
/// directly; they resolve it to a bounded key at the moment of use.
///
/// Every id is a non-empty Kubernetes label value. [`ClientId::from_header`]
/// turns a header into an id once, at the request boundary.
///
/// ```
/// # use pplx_space_common_core::types::ClientId;
/// assert_eq!(ClientId::from_header(Some(" batch-7 ")).as_str(), "batch-7");
/// assert_eq!(ClientId::from_header(Some("Not A Label!")), ClientId::unknown());
/// assert_eq!(ClientId::from_header(Some("NATIVE")), ClientId::unknown());
/// assert_eq!(ClientId::from_header(None), ClientId::unknown());
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ClientId(LabelValue);

/// A value that isn't a non-empty Kubernetes label value.
#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("client id must be a non-empty Kubernetes label value")]
pub struct ClientIdParseError;

impl ClientId {
    /// The id of an ECW caller that sent no valid, unreserved `X-Client-ID`.
    pub const UNKNOWN: &'static str = "unknown";
    /// The id of a native `/v1` or admin call.
    pub const NATIVE: &'static str = "native";
    /// Reserved for metrics, which fold rare keys into it.
    pub const OVERFLOW: &'static str = "overflow";

    /// The ids no caller can claim, compared case-insensitively.
    pub const RESERVED: [&'static str; 3] = [Self::UNKNOWN, Self::NATIVE, Self::OVERFLOW];

    /// Read an `X-Client-ID` value into an id. A missing, blank or invalid
    /// value, or one naming a reserved id in any case, becomes
    /// [`ClientId::unknown`], so every ECW activation has an id.
    pub fn from_header(value: Option<&str>) -> Self {
        value
            .map(str::trim)
            .filter(|value| !Self::is_reserved(value))
            .and_then(|value| Self::parse(value).ok())
            .unwrap_or_else(Self::unknown)
    }

    /// Parse a stored id: a record, a pod label or a persisted operation. The
    /// reserved ids are valid here, since the API stores them itself.
    pub fn parse(value: &str) -> Result<Self, ClientIdParseError> {
        if value.is_empty() {
            return Err(ClientIdParseError);
        }
        LabelValue::parse(value)
            .map(Self)
            .map_err(|_| ClientIdParseError)
    }

    /// The shared id of ECW callers without a valid id of their own.
    pub fn unknown() -> Self {
        static UNKNOWN: LazyLock<ClientId> =
            LazyLock::new(|| ClientId::parse(ClientId::UNKNOWN).expect("reserved id parses"));
        UNKNOWN.clone()
    }

    /// The id of native `/v1` and admin calls.
    pub fn native() -> Self {
        static NATIVE: LazyLock<ClientId> =
            LazyLock::new(|| ClientId::parse(ClientId::NATIVE).expect("reserved id parses"));
        NATIVE.clone()
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    fn is_reserved(value: &str) -> bool {
        Self::RESERVED
            .iter()
            .any(|reserved| reserved.eq_ignore_ascii_case(value))
    }
}

impl fmt::Display for ClientId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl TryFrom<String> for ClientId {
    type Error = ClientIdParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(&value)
    }
}

impl From<ClientId> for String {
    fn from(value: ClientId) -> Self {
        value.as_str().to_string()
    }
}

#[cfg(feature = "sqlx")]
impl sqlx::Type<sqlx::Postgres> for ClientId {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <str as sqlx::Type<sqlx::Postgres>>::type_info()
    }

    fn compatible(ty: &sqlx::postgres::PgTypeInfo) -> bool {
        <str as sqlx::Type<sqlx::Postgres>>::compatible(ty)
    }
}

#[cfg(feature = "sqlx")]
impl<'q> sqlx::Encode<'q, sqlx::Postgres> for ClientId {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        <&str as sqlx::Encode<'q, sqlx::Postgres>>::encode(self.as_str(), buf)
    }
}

#[cfg(feature = "sqlx")]
impl<'r> sqlx::Decode<'r, sqlx::Postgres> for ClientId {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let raw = <&str as sqlx::Decode<'r, sqlx::Postgres>>::decode(value)?;
        Self::parse(raw).map_err(Into::into)
    }
}

#[cfg(test)]
#[path = "client_id/tests.rs"]
mod tests;
