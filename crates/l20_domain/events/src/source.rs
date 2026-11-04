use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SandboxEventSource {
    Api,
    ClusterController,
    Substrate,
    SbxManager,
    NodeManager,
    NetGateway,
}

pub const SANDBOX_EVENT_SOURCES: &[SandboxEventSource] = &[
    SandboxEventSource::Api,
    SandboxEventSource::ClusterController,
    SandboxEventSource::Substrate,
    SandboxEventSource::SbxManager,
    SandboxEventSource::NodeManager,
    SandboxEventSource::NetGateway,
];

impl SandboxEventSource {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Api => "api",
            Self::ClusterController => "cluster_controller",
            Self::Substrate => "substrate",
            Self::SbxManager => "sbx_manager",
            Self::NodeManager => "node_manager",
            Self::NetGateway => "net_gateway",
        }
    }
}

impl fmt::Display for SandboxEventSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
