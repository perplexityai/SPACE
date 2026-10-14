//! The gateway under test.
//!
//! Runs the production [`tcp_proxy::run`] and [`udp_proxy::run`] loops on real
//! `IP_TRANSPARENT` sockets, against a policy installed through the production
//! `refresh` path. What this replaces is only the parts a bench cannot have: the
//! host-control RPC that normally supplies the policy, and the gRPC control
//! socket. Everything a packet touches is the shipped code.

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Context;
use pplx_space_net_common::{
    EffectiveNetworkPolicyRef, EffectiveNetworkPolicyRefScope, NetworkPolicyV1,
};
use pplx_space_net_gateway::debug::GatewayDebugState;
use pplx_space_net_gateway::dns::DnsAllowCache;
use pplx_space_net_gateway::egress::DnsAttributedTcpTracker;
use pplx_space_net_gateway::known_egress::KnownEgressState;
use pplx_space_net_gateway::reserved_dns::ReservedDnsCore;
use pplx_space_net_gateway::state::{
    GatewayNetworkState, NetworkStateReason, PolicyLoader, PulledNetworkPolicy,
    ReservedDnsCancellation,
};
use pplx_space_net_gateway::stats::ProxyStats;
use pplx_space_net_gateway::{tcp_proxy, udp_proxy};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::info;

/// A policy ref has to parse, and its digest is not otherwise meaningful here.
const BENCH_POLICY_SHA256: &str =
    "0000000000000000000000000000000000000000000000000000000000000000";

/// Which policy the gateway under test should install before serving.
#[derive(Clone, Copy, Debug, Eq, PartialEq, clap::ValueEnum)]
pub enum PolicyKind {
    /// V1 default-allow. What the allow-path workloads run against.
    AllowAll,
    /// V1 default-deny. Every destination is denied by tenant policy.
    DenyAll,
    /// No policy installed at all, so flows are denied for lack of one.
    Pending,
}

impl PolicyKind {
    fn policy(self) -> Option<NetworkPolicyV1> {
        match self {
            Self::AllowAll => Some(NetworkPolicyV1::allow_all()),
            Self::DenyAll => Some(NetworkPolicyV1::deny_all()),
            Self::Pending => None,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AllowAll => "allow-all",
            Self::DenyAll => "deny-all",
            Self::Pending => "pending",
        }
    }
}

pub struct GatewayArgs {
    pub sandbox_id: String,
    pub tcp_port: u16,
    pub udp_port: u16,
    pub policy: PolicyKind,
    pub ready_file: Option<std::path::PathBuf>,
}

pub async fn serve(args: GatewayArgs) -> anyhow::Result<()> {
    // Per-sandbox process in production: the id is set once and lives until
    // exit, which is what lets the proxy tasks carry it as a `&'static str`.
    let sandbox_id: &'static str = args.sandbox_id.clone().leak();

    let tcp_listener = transparent_tcp_listener(args.tcp_port).with_context(|| {
        format!(
            "failed to bind transparent TCP listener on {}",
            args.tcp_port
        )
    })?;
    let udp_socket = transparent_udp_socket(args.udp_port)
        .with_context(|| format!("failed to bind transparent UDP socket on {}", args.udp_port))?;

    let stats = Arc::new(ProxyStats::new());
    let dns_allow_cache = DnsAllowCache::default();
    let network_state = GatewayNetworkState::pending_policy();
    let known_egress_state = KnownEgressState::default();
    let installed_policy_ref = install_policy(&network_state, args.policy).await?;
    let reserved_dns =
        ReservedDnsCore::new().context("failed to initialize reserved-DNS resolution")?;

    let shutdown = CancellationToken::new();
    let tcp = tokio::spawn(tcp_proxy::run(tcp_proxy::TcpProxyConfig {
        sandbox_id,
        listener: tcp_listener,
        stats: stats.clone(),
        network_state: network_state.clone(),
        dns_allow_cache: dns_allow_cache.clone(),
        dns_attributed_connections: DnsAttributedTcpTracker::default(),
        tls_interception: Default::default(),
        reserved_dns: reserved_dns.clone(),
        reserved_dns_cancellation: ReservedDnsCancellation::default(),
        known_egress_state: known_egress_state.clone(),
        debug_state: GatewayDebugState::default(),
        shutdown_token: shutdown.child_token(),
    }));
    let udp = tokio::spawn(Box::pin(udp_proxy::run(udp_proxy::UdpProxyConfig {
        sandbox_id,
        listener: udp_socket,
        stats,
        network_state,
        dns_allow_cache,
        known_egress_state,
        reserved_dns,
        reserved_dns_cancellation: ReservedDnsCancellation::default(),
        shutdown_token: shutdown.child_token(),
    })));

    let pid = std::process::id();
    info!(
        sandbox_id,
        pid,
        tcp_port = args.tcp_port,
        udp_port = args.udp_port,
        policy = args.policy.as_str(),
        %installed_policy_ref,
        "bench gateway ready"
    );
    if let Some(path) = &args.ready_file {
        let ready = serde_json::json!({
            "pid": pid,
            "policy": args.policy.as_str(),
            "installed_policy_ref": installed_policy_ref,
        });
        write_atomically(path, &format!("{ready}\n"))
            .with_context(|| format!("failed to write ready file {}", path.display()))?;
    }

    let _ = tokio::signal::ctrl_c().await;
    shutdown.cancel();
    let _ = tokio::join!(tcp, udp);
    Ok(())
}

/// Drive the state machine to `Ready` through the same `refresh` the control RPC
/// uses, with a loader that returns a fixed policy instead of calling
/// host-control. `Pending` skips the refresh so the proxy sees no policy.
async fn install_policy(state: &GatewayNetworkState, kind: PolicyKind) -> anyhow::Result<String> {
    let Some(policy) = kind.policy() else {
        return Ok(String::new());
    };
    let policy_ref = EffectiveNetworkPolicyRef::local(
        EffectiveNetworkPolicyRefScope::Sandbox,
        "netgtw-bench",
        BENCH_POLICY_SHA256,
    )
    .context("bench policy ref must parse")?;
    let loader: PolicyLoader = {
        let policy_ref = policy_ref.clone();
        Arc::new(move || {
            let (policy, policy_ref) = (policy.clone(), policy_ref.clone());
            Box::pin(async move {
                Ok(PulledNetworkPolicy {
                    policy,
                    policy_ref,
                    network_credentials: Default::default(),
                })
            })
        })
    };
    let snapshot = state
        .refresh(loader, None, NetworkStateReason::Startup)
        .await;
    anyhow::ensure!(
        snapshot.policy.is_some(),
        "gateway did not reach a policy-installed state: {:?} ({})",
        snapshot.mode,
        snapshot.last_error
    );
    Ok(snapshot.installed_policy_ref)
}

/// Mirrors the service's own fallback listener: `IP_TRANSPARENT` is what lets
/// the accepted socket report the guest's original destination as its local
/// address.
fn transparent_tcp_listener(port: u16) -> anyhow::Result<TcpListener> {
    let socket = socket2::Socket::new(
        socket2::Domain::IPV4,
        socket2::Type::STREAM,
        Some(socket2::Protocol::TCP),
    )?;
    pplx_space_net_gateway::orig_dst::set_ip_transparent(&socket)?;
    socket.set_reuse_address(true)?;
    socket.set_nonblocking(true)?;
    socket.bind(&SocketAddr::from(([0, 0, 0, 0], port)).into())?;
    socket.listen(1024)?;
    Ok(TcpListener::from_std(socket.into())?)
}

fn transparent_udp_socket(port: u16) -> anyhow::Result<std::net::UdpSocket> {
    let socket = socket2::Socket::new(
        socket2::Domain::IPV4,
        socket2::Type::DGRAM,
        Some(socket2::Protocol::UDP),
    )?;
    pplx_space_net_gateway::orig_dst::set_ip_transparent(&socket)?;
    pplx_space_net_gateway::orig_dst::set_recvorigdstaddr(&socket)?;
    socket.set_reuse_address(true)?;
    socket.set_nonblocking(true)?;
    socket.bind(&SocketAddr::from(([0, 0, 0, 0], port)).into())?;
    Ok(socket.into())
}

/// Rename into place so a reader never sees a half-written ready file.
fn write_atomically(path: &std::path::Path, contents: &str) -> std::io::Result<()> {
    let temporary = path.with_extension("tmp");
    std::fs::write(&temporary, contents)?;
    std::fs::rename(&temporary, path)
}
