//! Kubernetes pod and node identifiers: pod UID, name and resource version,
//! and node name and UID. They encode Kubernetes' own naming rules; no SPACE
//! node-model types or logic.
//!
//! They are their own crate so that code can name them without depending on
//! `pplx-space-common-core`. `pplx-space-common-core` re-exports them from
//! `types::node`.

use std::{fmt, str::FromStr};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Kubernetes pod UID.
///
/// Kubernetes treats UIDs as opaque strings. SPACE keeps them typed because
/// they are execution identity material, not user content.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct PodUid(String);

#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("pod uid must not be empty")]
pub struct PodUidParseError;

impl PodUid {
    pub fn parse(value: impl AsRef<str>) -> Result<Self, PodUidParseError> {
        value.as_ref().parse()
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for PodUid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for PodUid {
    type Err = PodUidParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let value = value.trim();
        if value.is_empty() {
            return Err(PodUidParseError);
        }
        Ok(Self(value.to_string()))
    }
}

impl From<PodUid> for String {
    fn from(value: PodUid) -> Self {
        value.0
    }
}

impl TryFrom<String> for PodUid {
    type Error = PodUidParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

#[cfg(feature = "sqlx")]
impl sqlx::Type<sqlx::Postgres> for PodUid {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <str as sqlx::Type<sqlx::Postgres>>::type_info()
    }

    fn compatible(ty: &sqlx::postgres::PgTypeInfo) -> bool {
        <str as sqlx::Type<sqlx::Postgres>>::compatible(ty)
    }
}

#[cfg(feature = "sqlx")]
impl<'q> sqlx::Encode<'q, sqlx::Postgres> for PodUid {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        <&str as sqlx::Encode<'q, sqlx::Postgres>>::encode(self.as_str(), buf)
    }
}

#[cfg(feature = "sqlx")]
impl<'r> sqlx::Decode<'r, sqlx::Postgres> for PodUid {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let raw = <&str as sqlx::Decode<'r, sqlx::Postgres>>::decode(value)?;
        Self::from_str(raw).map_err(Into::into)
    }
}

/// Kubernetes pod name (`metadata.name`).
///
/// Validation is deliberately minimal — non-empty and RFC 1123 DNS
/// subdomain-shaped (lowercase alphanumerics, `-`, `.`, at most 253 chars,
/// alphanumeric at both ends) — enough to reject an obviously wrong value
/// without re-implementing the apiserver's validation.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct PodName(String);

#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("pod name must be a non-empty lowercase DNS subdomain")]
pub struct PodNameParseError;

impl PodName {
    pub fn parse(value: impl AsRef<str>) -> Result<Self, PodNameParseError> {
        value.as_ref().parse()
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for PodName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for PodName {
    type Err = PodNameParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let value = value.trim();
        if !is_dns_subdomain(value) {
            return Err(PodNameParseError);
        }
        Ok(Self(value.to_string()))
    }
}

impl From<PodName> for String {
    fn from(value: PodName) -> Self {
        value.0
    }
}

impl TryFrom<String> for PodName {
    type Error = PodNameParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

#[cfg(feature = "sqlx")]
impl sqlx::Type<sqlx::Postgres> for PodName {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <str as sqlx::Type<sqlx::Postgres>>::type_info()
    }

    fn compatible(ty: &sqlx::postgres::PgTypeInfo) -> bool {
        <str as sqlx::Type<sqlx::Postgres>>::compatible(ty)
    }
}

#[cfg(feature = "sqlx")]
impl<'q> sqlx::Encode<'q, sqlx::Postgres> for PodName {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        <&str as sqlx::Encode<'q, sqlx::Postgres>>::encode(self.as_str(), buf)
    }
}

#[cfg(feature = "sqlx")]
impl<'r> sqlx::Decode<'r, sqlx::Postgres> for PodName {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let raw = <&str as sqlx::Decode<'r, sqlx::Postgres>>::decode(value)?;
        Self::from_str(raw).map_err(Into::into)
    }
}

/// Kubernetes pod `metadata.resourceVersion`.
///
/// The apiserver defines this as an opaque, non-comparable token: it is only
/// ever tested for equality, and a client that parses or orders it is wrong.
/// Typing it keeps it from being mixed with a UID or a name — the three are
/// all strings and all pod identity material, and the one place they are used
/// together is a delete precondition, where confusing two of them silently
/// weakens the fence instead of failing.
///
/// Deliberately not `Ord`. Kubernetes defines resourceVersion as opaque with
/// no ordering — only equality is meaningful — so deriving it would make
/// `a < b` compile and read as "older than" while actually comparing strings
/// lexicographically.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct PodResourceVersion(String);

#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("pod resource version must not be empty")]
pub struct PodResourceVersionParseError;

impl PodResourceVersion {
    pub fn parse(value: impl AsRef<str>) -> Result<Self, PodResourceVersionParseError> {
        value.as_ref().parse()
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for PodResourceVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for PodResourceVersion {
    type Err = PodResourceVersionParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let value = value.trim();
        if value.is_empty() {
            return Err(PodResourceVersionParseError);
        }
        Ok(Self(value.to_string()))
    }
}

impl From<PodResourceVersion> for String {
    fn from(value: PodResourceVersion) -> Self {
        value.0
    }
}

impl TryFrom<String> for PodResourceVersion {
    type Error = PodResourceVersionParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

/// Kubernetes node name (`node_k8s.k8s_node_name`, the kube Node's
/// `metadata.name`).
///
/// Same deliberately minimal DNS-subdomain validation as [`PodName`]. This
/// is the kube-side name, not a SPACE node id — resolving one to the other
/// goes through the `node_k8s` mapping.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct K8sNodeName(String);

#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("kubernetes node name must be a non-empty lowercase DNS subdomain")]
pub struct K8sNodeNameParseError;

impl K8sNodeName {
    pub fn parse(value: impl AsRef<str>) -> Result<Self, K8sNodeNameParseError> {
        value.as_ref().parse()
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for K8sNodeName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for K8sNodeName {
    type Err = K8sNodeNameParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let value = value.trim();
        if !is_dns_subdomain(value) {
            return Err(K8sNodeNameParseError);
        }
        Ok(Self(value.to_string()))
    }
}

impl From<K8sNodeName> for String {
    fn from(value: K8sNodeName) -> Self {
        value.0
    }
}

impl TryFrom<String> for K8sNodeName {
    type Error = K8sNodeNameParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

#[cfg(feature = "sqlx")]
impl sqlx::Type<sqlx::Postgres> for K8sNodeName {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <str as sqlx::Type<sqlx::Postgres>>::type_info()
    }

    fn compatible(ty: &sqlx::postgres::PgTypeInfo) -> bool {
        <str as sqlx::Type<sqlx::Postgres>>::compatible(ty)
    }
}

#[cfg(feature = "sqlx")]
impl<'q> sqlx::Encode<'q, sqlx::Postgres> for K8sNodeName {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        <&str as sqlx::Encode<'q, sqlx::Postgres>>::encode(self.as_str(), buf)
    }
}

#[cfg(feature = "sqlx")]
impl<'r> sqlx::Decode<'r, sqlx::Postgres> for K8sNodeName {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let raw = <&str as sqlx::Decode<'r, sqlx::Postgres>>::decode(value)?;
        Self::from_str(raw).map_err(Into::into)
    }
}

/// Kubernetes node UID (`node_k8s.k8s_node_uid`, the kube Node's
/// `metadata.uid`).
///
/// Like [`PodUid`], kube treats it as an opaque string and so does SPACE:
/// it is node-identity material — a same-name replacement node gets a fresh
/// UID — so only non-emptiness is validated.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct K8sNodeUid(String);

#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("kubernetes node uid must not be empty")]
pub struct K8sNodeUidParseError;

impl K8sNodeUid {
    pub fn parse(value: impl AsRef<str>) -> Result<Self, K8sNodeUidParseError> {
        value.as_ref().parse()
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for K8sNodeUid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for K8sNodeUid {
    type Err = K8sNodeUidParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let value = value.trim();
        if value.is_empty() {
            return Err(K8sNodeUidParseError);
        }
        Ok(Self(value.to_string()))
    }
}

impl From<K8sNodeUid> for String {
    fn from(value: K8sNodeUid) -> Self {
        value.0
    }
}

impl TryFrom<String> for K8sNodeUid {
    type Error = K8sNodeUidParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

#[cfg(feature = "sqlx")]
impl sqlx::Type<sqlx::Postgres> for K8sNodeUid {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <str as sqlx::Type<sqlx::Postgres>>::type_info()
    }

    fn compatible(ty: &sqlx::postgres::PgTypeInfo) -> bool {
        <str as sqlx::Type<sqlx::Postgres>>::compatible(ty)
    }
}

#[cfg(feature = "sqlx")]
impl<'q> sqlx::Encode<'q, sqlx::Postgres> for K8sNodeUid {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        <&str as sqlx::Encode<'q, sqlx::Postgres>>::encode(self.as_str(), buf)
    }
}

#[cfg(feature = "sqlx")]
impl<'r> sqlx::Decode<'r, sqlx::Postgres> for K8sNodeUid {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let raw = <&str as sqlx::Decode<'r, sqlx::Postgres>>::decode(value)?;
        Self::from_str(raw).map_err(Into::into)
    }
}

/// RFC 1123 DNS subdomain shape, the loose common denominator for kube pod
/// and node names.
fn is_dns_subdomain(value: &str) -> bool {
    let alnum = |b: u8| b.is_ascii_lowercase() || b.is_ascii_digit();
    let bytes = value.as_bytes();
    bytes.len() <= 253
        && bytes.iter().all(|&b| alnum(b) || b == b'-' || b == b'.')
        && bytes.first().copied().is_some_and(alnum)
        && bytes.last().copied().is_some_and(alnum)
}

#[cfg(test)]
mod tests;
