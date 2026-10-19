use std::path::PathBuf;

use anyhow::Context;
use clap::Parser;
use pplx_space_common_core::types::{NodeManagedBy, SandboxId, SpaceApiRouteIdentity};
use pplx_space_net_common::DomainName;
use pplx_space_net_gateway::InitialPolicyState;
use pplx_space_net_gateway::tcp_proxy::TcpDomainAuthorizationMode;
use pplx_space_net_gateway::{NetGatewayRunConfig, SERVICE_NAME};

#[derive(Parser)]
#[command(
    name = "space-netgtw-svc",
    about = "SPACE per-sandbox network gateway (transparent L4 proxy)",
    version,
    disable_version_flag = true
)]
struct Cli {
    /// Print version.
    #[arg(short = 'v', long = "version", action = clap::ArgAction::Version)]
    _version: (),

    /// Unix socket path for the control service (sbx-manager connects here)
    #[arg(long)]
    socket: PathBuf,

    /// Sandbox ID this gateway is managing
    #[arg(long)]
    sandbox_id: SandboxId,

    /// Inherited TCP listener FD (created by node-manager with `IP_TRANSPARENT`)
    #[arg(long)]
    tcp_fd: Option<i32>,

    /// Inherited UDP socket FD (created by node-manager with `IP_TRANSPARENT` + `IP_RECVORIGDSTADDR`)
    #[arg(long)]
    udp_fd: Option<i32>,

    /// Start closed without tenant identity. Used for warm-pool members.
    #[arg(long)]
    no_identity: bool,

    /// Deprecated no-op: admission always uses DNS attribution. Older
    /// sandbox-managers still pass this option when launching or cycling a
    /// gateway; retain its accepted values across independent upgrades.
    #[arg(long = "tcp-domain-authorization-mode", hide = true)]
    _tcp_domain_authorization_mode: Option<TcpDomainAuthorizationMode>,

    /// Exact domains whose failed DNS or TCP flows should emit an error.
    #[arg(long = "known-egress-domain", value_name = "DOMAIN")]
    known_egress_domains: Vec<DomainName>,

    /// Deprecated no-op: TLS ClientHello SNI sniffing is always on. Still
    /// accepted so a sandbox-manager predating the toggle's removal can launch
    /// this binary during the deploy window where the on-disk gateway binary
    /// is newer than the running sandbox-manager. Remove once no deployed
    /// sandbox-manager passes it.
    #[arg(long = "sni-sniffing", hide = true)]
    _sni_sniffing: bool,

    /// Deprecated no-op: V1 policies always apply explicit denies before
    /// allows. Accepted while older sandbox-manager builds can still launch
    /// this binary with the rollout argument.
    #[arg(
        long = "v1-deny-before-allow-mode",
        hide = true,
        default_value = "disabled"
    )]
    _v1_deny_before_allow_mode: String,

    /// SPACE route generation this gateway is bound to for ServicePort ingress.
    #[arg(long)]
    route_generation: Option<u64>,

    /// SPACE node id this gateway is bound to for ServicePort ingress.
    #[arg(long)]
    current_node_id: Option<String>,

    /// Sandbox execution generation this gateway is bound to for ServicePort ingress.
    #[arg(long)]
    execution_generation: Option<u64>,

    /// Kubernetes pod uid this gateway is bound to for ServicePort ingress.
    #[arg(long)]
    pod_uid: Option<String>,

    /// Activation dispatch this gateway is bound to for ServicePort ingress.
    #[arg(long)]
    activation_attempt_id: Option<String>,

    /// Runtime substrate (`space` for direct or `k8s` for Kubernetes).
    #[arg(long)]
    substrate_kind: Option<NodeManagedBy>,
}

impl Cli {
    fn into_config(self) -> anyhow::Result<NetGatewayRunConfig> {
        let route_identity = self.route_identity()?;
        Ok(NetGatewayRunConfig {
            socket_path: self.socket,
            sandbox_id: self.sandbox_id,
            tcp_fd: self.tcp_fd,
            udp_fd: self.udp_fd,
            initial_policy_state: if self.no_identity {
                InitialPolicyState::NoIdentity
            } else {
                InitialPolicyState::PendingPolicy
            },
            known_egress_domains: self.known_egress_domains,
            route_identity,
            // Set by sandbox-manager when a traced request launches the
            // gateway, as the OpenTelemetry environment carrier names it.
            launch_traceparent: std::env::var("TRACEPARENT")
                .ok()
                .filter(|value| !value.is_empty()),
        })
    }

    fn route_identity(&self) -> anyhow::Result<Option<SpaceApiRouteIdentity>> {
        if self.route_generation.is_none()
            && self.current_node_id.is_none()
            && self.execution_generation.is_none()
            && self.pod_uid.is_none()
            && self.activation_attempt_id.is_none()
            && self.substrate_kind.is_none()
        {
            return Ok(None);
        }
        use pplx_space_net_gateway::proto::{SandboxSubstrateKind, SpaceApiRouteIdentity};
        pplx_space_net_gateway::ingress::route_identity_from_proto(Some(SpaceApiRouteIdentity {
            sandbox_id: Some(self.sandbox_id.to_string()),
            route_generation: self.route_generation,
            current_node_id: self.current_node_id.clone(),
            substrate_kind: match self.substrate_kind {
                None => SandboxSubstrateKind::Unspecified as i32,
                Some(NodeManagedBy::Space) => SandboxSubstrateKind::Direct as i32,
                Some(NodeManagedBy::Kubernetes) => SandboxSubstrateKind::Kubernetes as i32,
            },
            execution_generation: self
                .execution_generation
                .map(i64::try_from)
                .transpose()
                .context("invalid ServicePort execution generation")?,
            pod_uid: self.pod_uid.clone(),
            activation_attempt_id: self.activation_attempt_id.clone(),
        }))
        .context("invalid ServicePort route identity")
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Cli::parse().into_config()?;
    let telemetry = pplx_space_common_svc::init_service_with_log_context(
        SERVICE_NAME,
        pplx_space_common_svc::LogContext::default().with_sandbox_id(config.sandbox_id.to_string()),
    )?;
    let shutdown_token = pplx_space_common_svc::shutdown_token();
    let result = pplx_space_net_gateway::run(config, shutdown_token).await;
    pplx_space_common_svc::shutdown_telemetry(telemetry).await;
    Ok(result?)
}

#[cfg(test)]
mod tests;
