//! A route that the SPACE API resolved for a session it just created, signed so
//! the data plane can open that session's stream without resolving it again.
//!
//! The API mints a handover only for the in-cluster request it makes itself
//! with the session's fresh attach token. The data plane accepts one only on
//! its internal listener, and only when every bound field matches the request:
//! the session, the SHA-256 of the presented attach token, the session kind,
//! and the sandbox and execution generation that the embedded node route token
//! names. Anything else falls back to the ordinary route resolution.
//!
//! Handovers are signed with the route signing key under their own token
//! prefix and audience. Each prefix is part of the signed bytes, so a node
//! never accepts a handover as a route token and the data plane never accepts
//! a route token as a handover.

use super::*;
use pplx_space_common_core::types::{ExecutionGeneration, NodeId, OrgId, SandboxId};

/// Request header that carries a handover from the API to the data plane.
pub const DATA_PLANE_ROUTE_HANDOVER_HEADER: &str = "x-space-route-handover";
pub const DATA_PLANE_ROUTE_HANDOVER_AUDIENCE: &str = "space-data-plane-route-handover";
pub const DATA_PLANE_ROUTE_HANDOVER_SCHEMA_VERSION: u8 = 1;
/// The API sends a handover on a request it is making right away.
pub const DATA_PLANE_ROUTE_HANDOVER_TTL_SECS: u64 = 10;
const DATA_PLANE_ROUTE_HANDOVER_MAX_LIFETIME_SECS: u64 = 30;
const DATA_PLANE_ROUTE_HANDOVER_CLOCK_SKEW_SECS: u64 = 5;
const HANDOVER_TOKEN_PREFIX: &str = "dp-handover-ed25519";
/// Bounds the header; a larger route (a long command line or environment)
/// is resolved by the data plane instead.
pub const DATA_PLANE_ROUTE_HANDOVER_MAX_BYTES: usize = 16 * 1024;

/// The route fields the data plane would otherwise get from route resolution.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DataPlaneRouteHandoverRoute {
    pub session_id: String,
    pub token_sha256: String,
    pub org_id: OrgId,
    pub sandbox_id: SandboxId,
    pub kind: String,
    pub route_generation: u64,
    pub current_node_id: NodeId,
    pub backend_endpoint: String,
    pub execution_generation: ExecutionGeneration,
    pub revocation_epoch: u64,
    pub org_revocation_epoch: u64,
    pub adapter_state_json: String,
    pub space_api_route_token: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DataPlaneRouteHandoverClaims {
    #[serde(rename = "version")]
    pub schema_version: u8,
    pub issuer: SpaceApiRouteEd25519Issuer,
    pub audience: String,
    pub cluster_id: ClusterId,
    #[serde(flatten)]
    pub route: DataPlaneRouteHandoverRoute,
    pub issued_at_unix_secs: u64,
    pub expires_at_unix_secs: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum DataPlaneRouteHandoverError {
    #[error("route handover is malformed")]
    Malformed,
    #[error("route handover exceeds {DATA_PLANE_ROUTE_HANDOVER_MAX_BYTES} bytes")]
    TooLarge,
    #[error("route handover key id is unknown")]
    UnknownKeyId,
    #[error("route handover signature is invalid")]
    InvalidSignature,
    #[error("route handover claims are invalid: {0}")]
    InvalidClaims(#[from] serde_json::Error),
    #[error("route handover schema version is unsupported")]
    UnsupportedVersion,
    #[error("route handover issuer is not trusted")]
    WrongIssuer,
    #[error("route handover audience is wrong")]
    WrongAudience,
    #[error("route handover was issued for another cluster")]
    WrongCluster,
    #[error("route handover is expired")]
    Expired,
    #[error("route handover lifetime is invalid")]
    InvalidLifetime,
    #[error("route handover could not be signed: {0}")]
    Sign(#[from] SpaceApiRouteEd25519Error),
}

impl DataPlaneRouteHandoverError {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Malformed => "malformed",
            Self::TooLarge => "too_large",
            Self::UnknownKeyId => "unknown_key_id",
            Self::InvalidSignature => "invalid_signature",
            Self::InvalidClaims(_) => "invalid_claims",
            Self::UnsupportedVersion => "unsupported_version",
            Self::WrongIssuer => "wrong_issuer",
            Self::WrongAudience => "wrong_audience",
            Self::WrongCluster => "wrong_cluster",
            Self::Expired => "expired",
            Self::InvalidLifetime => "invalid_lifetime",
            Self::Sign(_) => "sign_failed",
        }
    }
}

impl SpaceApiRouteEd25519Authority {
    pub fn mint_data_plane_route_handover(
        &self,
        route: DataPlaneRouteHandoverRoute,
    ) -> Result<String, DataPlaneRouteHandoverError> {
        let issued_at_unix_secs = unix_now_secs();
        let claims = DataPlaneRouteHandoverClaims {
            schema_version: DATA_PLANE_ROUTE_HANDOVER_SCHEMA_VERSION,
            issuer: self.issuer.clone(),
            audience: DATA_PLANE_ROUTE_HANDOVER_AUDIENCE.to_string(),
            cluster_id: self.cluster_id.clone(),
            route,
            issued_at_unix_secs,
            expires_at_unix_secs: issued_at_unix_secs + DATA_PLANE_ROUTE_HANDOVER_TTL_SECS,
        };
        self.signer.mint_data_plane_route_handover(&claims)
    }

    /// Signs `claims` as given, lifetime included; for exercising verifiers.
    pub fn sign_data_plane_route_handover_claims(
        &self,
        claims: &DataPlaneRouteHandoverClaims,
    ) -> Result<String, DataPlaneRouteHandoverError> {
        self.signer.mint_data_plane_route_handover(claims)
    }
}

impl SpaceApiRouteEd25519Signer {
    pub fn mint_data_plane_route_handover(
        &self,
        claims: &DataPlaneRouteHandoverClaims,
    ) -> Result<String, DataPlaneRouteHandoverError> {
        let payload = URL_SAFE_NO_PAD.encode(serde_json::to_vec(claims)?);
        let signing_input = format!("{HANDOVER_TOKEN_PREFIX}.{}.{payload}", self.key_id);
        let signature = self.key_pair.sign(signing_input.as_bytes());
        let handover = format!(
            "{signing_input}.{}",
            URL_SAFE_NO_PAD.encode(signature.as_ref())
        );
        if handover.len() > DATA_PLANE_ROUTE_HANDOVER_MAX_BYTES {
            return Err(DataPlaneRouteHandoverError::TooLarge);
        }
        Ok(handover)
    }
}

impl SpaceApiRouteEd25519Verifier {
    /// Checks the signature, issuer, audience, cluster, and lifetime. The
    /// caller still has to match the route against the request it arrived on.
    pub fn verify_data_plane_route_handover(
        &self,
        handover: &str,
    ) -> Result<DataPlaneRouteHandoverRoute, DataPlaneRouteHandoverError> {
        if handover.len() > DATA_PLANE_ROUTE_HANDOVER_MAX_BYTES {
            return Err(DataPlaneRouteHandoverError::TooLarge);
        }
        let mut parts = handover.split('.');
        if parts.next() != Some(HANDOVER_TOKEN_PREFIX) {
            return Err(DataPlaneRouteHandoverError::Malformed);
        }
        let key_id = parts
            .next()
            .and_then(|value| value.parse::<SpaceApiRouteEd25519KeyId>().ok())
            .ok_or(DataPlaneRouteHandoverError::Malformed)?;
        let encoded_payload = parts.next().ok_or(DataPlaneRouteHandoverError::Malformed)?;
        let encoded_signature = parts.next().ok_or(DataPlaneRouteHandoverError::Malformed)?;
        if parts.next().is_some() {
            return Err(DataPlaneRouteHandoverError::Malformed);
        }
        let signature = URL_SAFE_NO_PAD
            .decode(encoded_signature)
            .map_err(|_| DataPlaneRouteHandoverError::Malformed)?;
        let public_key = self
            .keys
            .get(&key_id)
            .ok_or(DataPlaneRouteHandoverError::UnknownKeyId)?;
        let signing_input = format!("{HANDOVER_TOKEN_PREFIX}.{key_id}.{encoded_payload}");
        ED25519
            .verify_sig(public_key, signing_input.as_bytes(), &signature)
            .map_err(|_| DataPlaneRouteHandoverError::InvalidSignature)?;
        let payload = URL_SAFE_NO_PAD
            .decode(encoded_payload)
            .map_err(|_| DataPlaneRouteHandoverError::Malformed)?;
        let claims = serde_json::from_slice::<DataPlaneRouteHandoverClaims>(&payload)?;
        if claims.schema_version != DATA_PLANE_ROUTE_HANDOVER_SCHEMA_VERSION {
            return Err(DataPlaneRouteHandoverError::UnsupportedVersion);
        }
        if claims.issuer != self.expected_issuer {
            return Err(DataPlaneRouteHandoverError::WrongIssuer);
        }
        if claims.audience != DATA_PLANE_ROUTE_HANDOVER_AUDIENCE {
            return Err(DataPlaneRouteHandoverError::WrongAudience);
        }
        if claims.cluster_id != self.expected_cluster_id {
            return Err(DataPlaneRouteHandoverError::WrongCluster);
        }
        let lifetime = claims
            .expires_at_unix_secs
            .checked_sub(claims.issued_at_unix_secs)
            .ok_or(DataPlaneRouteHandoverError::InvalidLifetime)?;
        if lifetime == 0 || lifetime > DATA_PLANE_ROUTE_HANDOVER_MAX_LIFETIME_SECS {
            return Err(DataPlaneRouteHandoverError::InvalidLifetime);
        }
        let now = unix_now_secs();
        let skew = DATA_PLANE_ROUTE_HANDOVER_CLOCK_SKEW_SECS;
        if claims.issued_at_unix_secs > now.saturating_add(skew) {
            return Err(DataPlaneRouteHandoverError::InvalidLifetime);
        }
        if claims.expires_at_unix_secs.saturating_add(skew) <= now {
            return Err(DataPlaneRouteHandoverError::Expired);
        }
        Ok(claims.route)
    }
}

#[cfg(test)]
mod tests;
