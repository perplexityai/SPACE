//! How sandbox-manager obtains node-manager clients.
//!
//! Every node-manager client sandbox-manager uses comes from one
//! [`NodeManagerConnector`], which the service hands to the subsystems it
//! constructs. Production uses [`NodeManagerConnector::host`]: each method
//! calls the `pplx-space-node-mgr-client` constructor of the same name, so
//! every client dials node-manager's fixed Unix socket with that
//! constructor's retry policy and connection timing. Tests substitute a
//! transport serving an in-process node-manager.

use std::sync::Arc;

pub use pplx_space_node_mgr_client::{ConnectError, NodeManagerGrpcClient};

/// A source of node-manager clients. The methods mirror the client crate's
/// constructors; a caller picks the one whose dial timing it needs.
#[async_trait::async_trait]
pub trait NodeManagerTransport: Send + Sync {
    /// A connected client, dialing under the default connect-retry policy.
    async fn connect(&self) -> Result<NodeManagerGrpcClient, ConnectError>;

    /// A connected client after a single dial, for callers that own their
    /// retry loop.
    async fn connect_once(&self) -> Result<NodeManagerGrpcClient, ConnectError>;

    /// A client whose channel dials on first use and reconnects afterwards.
    fn lazy(&self) -> NodeManagerGrpcClient;
}

/// Node-manager's fixed host socket.
struct HostSocket;

#[async_trait::async_trait]
impl NodeManagerTransport for HostSocket {
    async fn connect(&self) -> Result<NodeManagerGrpcClient, ConnectError> {
        pplx_space_node_mgr_client::connect().await
    }

    async fn connect_once(&self) -> Result<NodeManagerGrpcClient, ConnectError> {
        pplx_space_node_mgr_client::connect_once().await
    }

    fn lazy(&self) -> NodeManagerGrpcClient {
        pplx_space_node_mgr_client::lazy()
    }
}

/// Cloneable handle to the node-manager transport.
#[derive(Clone)]
pub struct NodeManagerConnector {
    transport: Arc<dyn NodeManagerTransport>,
    /// Whether this node-manager inspects retained btrfs-send sources. The
    /// answer belongs to the node-manager the transport reaches, so it is
    /// cached per connector and shared by its clones.
    inspects_send_sources: Arc<crate::btrfs_via_nodemgr::CapabilityCache>,
}

impl NodeManagerConnector {
    /// Node-manager at [`pplx_space_node_mgr_client::SOCKET_PATH`]. Every call
    /// returns the same connector, so the process keeps one capability cache
    /// for the host's node-manager however many places construct it.
    pub fn host() -> Self {
        static HOST: std::sync::LazyLock<NodeManagerConnector> =
            std::sync::LazyLock::new(|| NodeManagerConnector::new(Arc::new(HostSocket)));
        HOST.clone()
    }

    pub fn new(transport: Arc<dyn NodeManagerTransport>) -> Self {
        Self {
            transport,
            inspects_send_sources: Arc::new(crate::btrfs_via_nodemgr::CapabilityCache::new()),
        }
    }

    pub async fn connect(&self) -> Result<NodeManagerGrpcClient, ConnectError> {
        self.transport.connect().await
    }

    pub async fn connect_once(&self) -> Result<NodeManagerGrpcClient, ConnectError> {
        self.transport.connect_once().await
    }

    pub fn lazy(&self) -> NodeManagerGrpcClient {
        self.transport.lazy()
    }

    pub(crate) fn inspects_send_sources_cache(&self) -> &crate::btrfs_via_nodemgr::CapabilityCache {
        &self.inspects_send_sources
    }
}

#[cfg(test)]
pub(crate) mod fake;
