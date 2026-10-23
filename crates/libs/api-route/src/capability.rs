//! Scheme-neutral SPACE API route capability vocabulary and metadata.

use std::fmt;
use std::str::FromStr;

use pplx_space_common_core::types::{GuestPort, SandboxRouteIdentity};
use serde::{Deserialize, Serialize};
use tonic::metadata::MetadataMap;

pub const SPACE_API_ROUTE_ED25519_TOKEN_METADATA_KEY: &str = "x-space-api-route-token-ed25519";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpaceApiRoutePurpose {
    SpaceApiBridge,
    ServicePortIngress,
}

impl SpaceApiRoutePurpose {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SpaceApiBridge => "space_api_bridge",
            Self::ServicePortIngress => "service_port_ingress",
        }
    }
}

impl fmt::Display for SpaceApiRoutePurpose {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for SpaceApiRoutePurpose {
    type Err = SpaceApiRoutePurposeParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Ok(match value {
            "space_api_bridge" => Self::SpaceApiBridge,
            "service_port_ingress" => Self::ServicePortIngress,
            _ => return Err(SpaceApiRoutePurposeParseError),
        })
    }
}

#[derive(Debug, thiserror::Error)]
#[error("unknown SPACE API route purpose")]
pub struct SpaceApiRoutePurposeParseError;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpaceApiRouteProtocol {
    Tcp,
}

impl SpaceApiRouteProtocol {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Tcp => "tcp",
        }
    }
}

impl fmt::Display for SpaceApiRouteProtocol {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for SpaceApiRouteProtocol {
    type Err = SpaceApiRouteProtocolParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "tcp" => Ok(Self::Tcp),
            _ => Err(SpaceApiRouteProtocolParseError),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("unknown SPACE API route protocol")]
pub struct SpaceApiRouteProtocolParseError;

/// Scheme-neutral authority to reach one concrete sandbox execution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SandboxRouteCapability {
    purpose: SpaceApiRoutePurpose,
    route_identity: SandboxRouteIdentity,
    service_port: Option<GuestPort>,
    protocol: Option<SpaceApiRouteProtocol>,
}

impl SandboxRouteCapability {
    pub fn space_api_bridge(route_identity: SandboxRouteIdentity) -> Self {
        Self {
            purpose: SpaceApiRoutePurpose::SpaceApiBridge,
            route_identity,
            service_port: None,
            protocol: None,
        }
    }

    pub fn service_port_ingress(
        route_identity: SandboxRouteIdentity,
        service_port: GuestPort,
        protocol: SpaceApiRouteProtocol,
    ) -> Self {
        Self {
            purpose: SpaceApiRoutePurpose::ServicePortIngress,
            route_identity,
            service_port: Some(service_port),
            protocol: Some(protocol),
        }
    }

    pub const fn purpose(&self) -> SpaceApiRoutePurpose {
        self.purpose
    }

    pub const fn route_identity(&self) -> &SandboxRouteIdentity {
        &self.route_identity
    }

    pub const fn service_port(&self) -> Option<GuestPort> {
        self.service_port
    }

    pub const fn protocol(&self) -> Option<SpaceApiRouteProtocol> {
        self.protocol
    }
}

pub fn route_token_metadata(metadata: &MetadataMap) -> MetadataMap {
    let mut allowed = MetadataMap::new();
    if let Some(value) = metadata.get(SPACE_API_ROUTE_ED25519_TOKEN_METADATA_KEY) {
        allowed.insert(SPACE_API_ROUTE_ED25519_TOKEN_METADATA_KEY, value.clone());
    }
    allowed
}
