//! Template lookup scope propagation during a mixed-version rollout.
//! Receipts confirm protocol support, not authorization. Existing access checks
//! remain authoritative until a separate enforcement protocol is deployed.

use std::net::IpAddr;

use pplx_space_common_core::types::SandboxId;
use tonic::{Request, Response, Status};

const SCOPE_HEADER: &str = "x-space-template-lookup-scope";
const RECEIPT_HEADER: &str = "x-space-template-lookup-scope-received";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TemplateLookupScope {
    /// Online lookup using the service's catalog authority and optional org filter.
    Cluster,
    /// Original authenticated node before forwarding.
    Node(IpAddr),
    /// Sandbox identity and, when forwarded, its original node.
    Sandbox {
        id: SandboxId,
        caller_node: Option<IpAddr>,
    },
    /// Execution requested by an authenticated node-local principal.
    LocalExecution,
}

impl std::fmt::Display for TemplateLookupScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Cluster => f.write_str("cluster"),
            Self::Node(address) => write!(f, "node:{address}"),
            Self::Sandbox { id, caller_node } => {
                write!(f, "sandbox:{id}")?;
                if let Some(address) = caller_node {
                    write!(f, "@{address}")?;
                }
                Ok(())
            }
            Self::LocalExecution => f.write_str("local"),
        }
    }
}

impl std::str::FromStr for TemplateLookupScope {
    type Err = Status;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "cluster" => Ok(Self::Cluster),
            "local" => Ok(Self::LocalExecution),
            _ => {
                if let Some(address) = value.strip_prefix("node:") {
                    return address
                        .parse()
                        .map(Self::Node)
                        .map_err(|_| Status::invalid_argument("invalid template caller address"));
                }
                if let Some(id) = value.strip_prefix("sandbox:") {
                    let (id, address) = id
                        .split_once('@')
                        .map_or((id, None), |(id, address)| (id, Some(address)));
                    return Ok(Self::Sandbox {
                        id: id.parse().map_err(|_| {
                            Status::invalid_argument("invalid template sandbox identity")
                        })?,
                        caller_node: address.map(str::parse).transpose().map_err(|_| {
                            Status::invalid_argument("invalid template caller address")
                        })?,
                    });
                }
                Err(Status::invalid_argument("invalid template lookup scope"))
            }
        }
    }
}

impl TemplateLookupScope {
    pub fn from_request<T>(request: &Request<T>) -> Result<Option<Self>, Status> {
        request
            .metadata()
            .get(SCOPE_HEADER)
            .map(|value| {
                value
                    .to_str()
                    .map_err(|_| Status::invalid_argument("invalid template lookup scope"))?
                    .parse()
            })
            .transpose()
    }

    pub fn attach<T>(self, request: &mut Request<T>) {
        request.metadata_mut().insert(
            SCOPE_HEADER,
            self.to_string().parse().expect("scope is ASCII"),
        );
    }

    /// A receipt must not satisfy the authorization acknowledgement used by strict peers.
    pub fn receipt<T>(self, response: &mut Response<T>) {
        response.metadata_mut().insert(
            RECEIPT_HEADER,
            self.to_string().parse().expect("scope is ASCII"),
        );
    }

    pub fn observe_response<T>(self, response: &Response<T>) -> Result<(), Status> {
        match response.metadata().get(RECEIPT_HEADER) {
            None => {
                pplx_space_metrics::auth::template_lookup_compatibility("missing_receipt");
                tracing::warn!(
                    "template lookup peer has no scope receipt; retaining legacy behavior"
                );
                Ok(())
            }
            Some(value) if value.to_str().ok() == Some(self.to_string().as_str()) => {
                pplx_space_metrics::auth::template_lookup_compatibility("received");
                Ok(())
            }
            Some(_) => Err(Status::failed_precondition(
                "template lookup scope receipt mismatch",
            )),
        }
    }
}
