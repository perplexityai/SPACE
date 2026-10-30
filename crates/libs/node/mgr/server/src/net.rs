use crate::host_command::Command;
use pplx_space_common_core::types::LinuxNetworkSlot;
use std::io;
use std::process::{ExitStatus, Output};
use std::sync::OnceLock;

pub use native::NAMESPACE_ICMP_RATEMASK;

pub const DEFAULT_MAX_SLOTS: u32 = 300;
pub const SANDBOX_UID_RANGE_START: u32 =
    pplx_space_common_core::host::linux::gateway::SANDBOX_UID_BASE;

#[derive(Debug, Clone, thiserror::Error)]
pub enum NetworkProbeError {
    #[error("failed to query link {name}: {detail}")]
    LinkQuery { name: String, detail: String },
    #[error("failed to dump routes: {detail}")]
    RouteDump { detail: String },
    #[error("failed to inspect root default route: {detail}")]
    RootDefaultRoute { detail: String },
    #[error("failed to read sysctl {key}: {detail}")]
    Sysctl { key: String, detail: String },
    #[error("failed to inspect nft rules: {detail}")]
    Nft { detail: String },
    #[error("failed to inspect network namespace {ns}: {detail}")]
    Namespace { ns: String, detail: String },
}

#[derive(Debug, thiserror::Error)]
pub enum NetworkTeardownError {
    #[error(transparent)]
    Probe(#[from] NetworkProbeError),
    #[error("network teardown left {resource}")]
    Remaining { resource: String },
    #[error("network teardown ingress cleanup failed: {0}")]
    Ingress(#[from] crate::firewall::FirewallError),
}

fn require_network_absent(
    resource: String,
    present: Result<bool, NetworkProbeError>,
) -> Result<(), NetworkTeardownError> {
    if present? {
        return Err(NetworkTeardownError::Remaining { resource });
    }
    Ok(())
}

fn try_namespace_exists(ns: &str) -> Result<bool, NetworkProbeError> {
    std::path::Path::new(&format!("/run/netns/{ns}"))
        .try_exists()
        .map_err(|error| NetworkProbeError::Namespace {
            ns: ns.to_owned(),
            detail: error.to_string(),
        })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NetworkErrorPersistence {
    Transient,
    Permanent,
}

#[derive(Debug, Clone, thiserror::Error)]
#[error("{detail}")]
pub struct NetworkOpError {
    detail: String,
    persistence: NetworkErrorPersistence,
}

impl NetworkOpError {
    pub fn transient(detail: impl Into<String>) -> Self {
        Self {
            detail: detail.into(),
            persistence: NetworkErrorPersistence::Transient,
        }
    }

    pub fn permanent(detail: impl Into<String>) -> Self {
        Self {
            detail: detail.into(),
            persistence: NetworkErrorPersistence::Permanent,
        }
    }

    pub fn is_transient(&self) -> bool {
        self.persistence == NetworkErrorPersistence::Transient
    }

    pub fn detail(&self) -> &str {
        &self.detail
    }

    pub fn from_io_error(error: io::Error) -> Self {
        if is_transient_io_error(&error) {
            Self::transient(error.to_string())
        } else {
            Self::permanent(error.to_string())
        }
    }
}

impl From<String> for NetworkOpError {
    fn from(detail: String) -> Self {
        Self::transient(detail)
    }
}

/// Failure to enter a network namespace before running an operation inside
/// it. Kept typed (instead of a formatted string) so callers can distinguish
/// "the namespace's /run/netns entry no longer exists" — which is terminal
/// until something recreates the namespace — from transient entry failures.
#[derive(Debug, thiserror::Error)]
pub enum NetnsEnterError {
    #[error("failed to open current network namespace: {source}")]
    OpenCurrent {
        #[source]
        source: io::Error,
    },
    #[error("failed to open network namespace {path}: {source}")]
    OpenTarget {
        path: String,
        #[source]
        source: io::Error,
    },
    #[error("{detail}")]
    SetNs { detail: String },
    #[error("network namespace thread panicked for {ns}")]
    ThreadPanicked { ns: String },
}

impl From<NetnsEnterError> for String {
    fn from(error: NetnsEnterError) -> Self {
        error.to_string()
    }
}

impl From<NetnsEnterError> for NetworkOpError {
    /// Namespace-entry failures on the setup/teardown paths were historically
    /// formatted into strings and classified transient (retry with cap);
    /// preserve that disposition.
    fn from(error: NetnsEnterError) -> Self {
        Self::transient(error.to_string())
    }
}

impl From<NetnsEnterError> for io::Error {
    fn from(error: NetnsEnterError) -> Self {
        // Namespace entry/restore failures retain the setup retry policy;
        // command errors keep their original kind and errno independently.
        Self::new(io::ErrorKind::Interrupted, error)
    }
}

/// Failure to resolve the default interface of a namespace (or the root
/// namespace when `None`).
#[derive(Debug, thiserror::Error)]
pub enum DefaultIfaceError {
    /// The namespace's /run/netns entry is gone (e.g. CNI tore down the pod
    /// while a node-manager allocation still references it). Terminal for
    /// the caller: retrying cannot succeed until the namespace is recreated.
    #[error("network namespace {path} is missing: {source}")]
    NetnsMissing {
        path: String,
        #[source]
        source: io::Error,
    },
    #[error("{0}")]
    Other(String),
}

impl From<NetnsEnterError> for DefaultIfaceError {
    fn from(error: NetnsEnterError) -> Self {
        match error {
            NetnsEnterError::OpenTarget { path, source }
                if source.kind() == io::ErrorKind::NotFound =>
            {
                Self::NetnsMissing { path, source }
            }
            other => Self::Other(other.to_string()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NetworkSetupDisposition {
    RetryWithCap,
    Quarantine,
}

#[derive(Debug, thiserror::Error)]
pub enum NetworkSetupError {
    #[error("failed to determine default network interface: {source}")]
    DefaultInterface {
        #[source]
        source: NetworkOpError,
    },
    #[error("failed to create network namespace {ns}: {source}")]
    Namespace {
        ns: String,
        #[source]
        source: NetworkOpError,
    },
    #[error("failed to set up host veth pair: {source}")]
    HostVeth {
        #[source]
        source: NetworkOpError,
    },
    #[error("failed to set up sandbox namespace links: {source}")]
    NamespaceLinks {
        #[source]
        source: NetworkOpError,
    },
    #[error("failed to apply firewall rules: {source}")]
    Firewall {
        #[source]
        source: NetworkOpError,
    },
    #[error("failed to apply sysctl {key}: {source}")]
    Sysctl {
        key: &'static str,
        #[source]
        source: NetworkOpError,
    },
    #[error("failed to add host route: {source}")]
    HostRoute {
        #[source]
        source: NetworkOpError,
    },
}

impl NetworkSetupError {
    pub fn disposition(&self) -> NetworkSetupDisposition {
        let source = match self {
            Self::DefaultInterface { source }
            | Self::Namespace { source, .. }
            | Self::HostVeth { source }
            | Self::NamespaceLinks { source }
            | Self::Firewall { source }
            | Self::Sysctl { source, .. }
            | Self::HostRoute { source } => source,
        };
        if source.is_transient() {
            NetworkSetupDisposition::RetryWithCap
        } else {
            NetworkSetupDisposition::Quarantine
        }
    }

    pub fn is_transient(&self) -> bool {
        self.disposition() == NetworkSetupDisposition::RetryWithCap
    }
}

fn is_transient_io_error(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::Interrupted | io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
    ) || matches!(
        error.raw_os_error(),
        Some(code) if code == libc::EMFILE || code == libc::ENFILE || code == libc::ENOMEM
    )
}

fn is_transient_firewall_stderr(stderr: &str) -> bool {
    stderr.contains("netlink operation timed out")
}

fn classify_firewall_setup_error(error: crate::firewall::FirewallError) -> NetworkOpError {
    match error {
        crate::firewall::FirewallError::ListStart(error)
        | crate::firewall::FirewallError::DeleteStart(error)
        | crate::firewall::FirewallError::Exec(error) => NetworkOpError::from_io_error(error),
        crate::firewall::FirewallError::ReadPreTproxyPolicy { source, .. }
            if is_transient_io_error(&source) =>
        {
            NetworkOpError::transient(source.to_string())
        }
        crate::firewall::FirewallError::ReadPreTproxyPolicy { source, .. } => {
            NetworkOpError::permanent(source.to_string())
        }
        crate::firewall::FirewallError::ListFailed { stderr } => NetworkOpError::transient(stderr),
        error @ crate::firewall::FirewallError::MissingRuleHandle { .. } => {
            NetworkOpError::permanent(error.to_string())
        }
        error @ crate::firewall::FirewallError::GuestL2IngressMissing => {
            NetworkOpError::permanent(error.to_string())
        }
        crate::firewall::FirewallError::Apply { stderr, .. }
            if is_transient_firewall_stderr(&stderr) =>
        {
            NetworkOpError::transient(stderr)
        }
        crate::firewall::FirewallError::Apply { stderr, .. } => NetworkOpError::permanent(stderr),
        crate::firewall::FirewallError::DeleteFailed {
            family,
            table,
            chain,
            handle,
        } => NetworkOpError::permanent(format!(
            "nft delete rule {family} {table} {chain} handle {handle} failed"
        )),
        crate::firewall::FirewallError::ListJson(error) => {
            NetworkOpError::permanent(error.to_string())
        }
    }
}

pub fn sandbox_uid_for_slot(slot: u32) -> u32 {
    SANDBOX_UID_RANGE_START + slot
}

#[derive(Clone, Debug)]
pub struct SlotInfo {
    pub sandbox_id: String,
    pub slot: u32,
    pub generation: u64,
    pub sandbox_uid: u32,
    pub pid: Option<u32>,
    /// If set, the pod's network namespace name (from CNI). The sandbox namespace
    /// bridges to this namespace via a veth pair + MASQUERADE instead of to the
    /// root namespace.
    pub pod_netns: Option<String>,
}

impl SlotInfo {
    pub fn slot_label(&self) -> String {
        slot_label(self.slot)
    }

    /// The SPACE-managed sandbox namespace name (always owned by SPACE).
    pub fn ns_name(&self) -> String {
        format!("sns-{}", self.slot_label())
    }

    /// The sandbox namespace path.
    pub fn ns_path(&self) -> String {
        format!("/var/run/netns/{}", self.ns_name())
    }
}

/// Network identity for a sandbox slot. Computes all interface names and IPs.
pub struct NetId {
    pub label: String,
    pub slot: u32,
}

impl NetId {
    pub fn from_slot_info(info: &SlotInfo) -> Self {
        Self::from_slot(info.slot)
    }

    pub fn from_slot(slot: u32) -> Self {
        Self {
            label: slot_label(slot),
            slot,
        }
    }

    pub fn ns(&self) -> String {
        format!("sns-{}", self.label)
    }
    pub fn ns_path(&self) -> String {
        format!("/var/run/netns/sns-{}", self.label)
    }
    pub fn sveth(&self) -> String {
        format!("sveth-{}", self.label)
    }
    pub fn vpeer(&self) -> String {
        format!("vpeer-{}", self.label)
    }
    pub fn sveth_ip(&self) -> String {
        format!(
            "{}/32",
            ipv4_from_offset(SANDBOX_IPV4_BASE, self.ip_slot_offset())
        )
    }
    pub fn vpeer_ip(&self) -> String {
        format!(
            "{}/31",
            ipv4_from_offset(
                SANDBOX_IPV4_BASE,
                VPEER_IPV4_BASE_OFFSET + self.ip_slot_offset() * 2
            )
        )
    }
    pub fn vpeer_ip_bare(&self) -> String {
        ipv4_from_offset(
            SANDBOX_IPV4_BASE,
            VPEER_IPV4_BASE_OFFSET + self.ip_slot_offset() * 2,
        )
    }
    pub fn sveth_ip_bare(&self) -> String {
        ipv4_from_offset(SANDBOX_IPV4_BASE, self.ip_slot_offset())
    }
    fn ip_slot_offset(&self) -> u32 {
        self.slot + 1
    }
    pub fn guest_mac_bytes(&self) -> [u8; 6] {
        let [_, b2, b1, b0] = self.slot.to_be_bytes();
        [0xaa, 0xfc, 0, b2, b1, b0]
    }
    pub fn guest_mac(&self) -> String {
        let [a, b, c, d, e, f] = self.guest_mac_bytes();
        format!("{a:02X}:{b:02X}:{c:02X}:{d:02X}:{e:02X}:{f:02X}")
    }
}

pub fn slot_label(slot: u32) -> String {
    LinuxNetworkSlot::try_from_u32(
        slot,
        pplx_space_common_core::types::MAX_LINUX_NETWORK_SLOT + 1,
    )
    .map(|slot| slot.label())
    .unwrap_or_else(|_| format!("{slot:06}"))
}

const SANDBOX_IPV4_BASE: std::net::Ipv4Addr = std::net::Ipv4Addr::new(10, 12, 0, 0);
const VPEER_IPV4_BASE_OFFSET: u32 = 32_768;
pub const MAX_IPV4_NETWORK_SLOTS: u32 = 16_383;

fn ipv4_from_offset(base: std::net::Ipv4Addr, offset: u32) -> String {
    let base = u32::from(base);
    std::net::Ipv4Addr::from(base + offset).to_string()
}

pub fn is_slot_vpeer_ip(ip: std::net::Ipv4Addr) -> bool {
    let base = u32::from(SANDBOX_IPV4_BASE);
    let Some(offset) = u32::from(ip).checked_sub(base) else {
        return false;
    };
    let first_vpeer_offset = VPEER_IPV4_BASE_OFFSET + 2;
    let last_vpeer_offset = VPEER_IPV4_BASE_OFFSET + MAX_IPV4_NETWORK_SLOTS * 2;
    offset >= first_vpeer_offset && offset <= last_vpeer_offset && offset % 2 == 0
}

pub const SANDBOX_SUBNET: &str = "10.12.0.0";
pub const SANDBOX_SUBNET_PREFIX_LEN: u8 = 16;
pub const GUEST_IP: &str = pplx_space_common_core::guest::linux::network::IPV4_ADDR;
pub const GUEST_GATEWAY: &str = pplx_space_common_core::guest::linux::network::GATEWAY_IPV4_ADDR;
pub const TAP_IP: &str = pplx_space_common_core::guest::linux::network::GATEWAY_IPV4_CIDR;
pub const TAP_DEVICE: &str = "tap0";

static ROOT_DEFAULT_IFACE: OnceLock<String> = OnceLock::new();
static ROOT_IP_FORWARD_ENABLED: OnceLock<()> = OnceLock::new();

/// Set up networking for a sandbox.
///
/// SPACE always creates its own network namespace. When `pod_netns` is provided
/// (CNI/K8s), the veth pair and host-side rules are created in the pod namespace
/// instead of the root namespace. The inner namespace setup is identical in both cases.
///
/// `ns` — the sandbox network namespace name (in `/var/run/netns/`).
/// `pod_netns` — if Some, the pod namespace to bridge to (CNI); if None, root namespace.
pub fn setup_network(
    ns: &str,
    net: &NetId,
    pod_netns: Option<&str>,
) -> Result<(), NetworkSetupError> {
    // Determine the default outgoing interface in the host namespace
    let host_iface = if let Some(pod_ns) = pod_netns {
        get_default_iface_in_ns(pod_ns)
    } else {
        get_default_iface_cached()
    }
    .map_err(|error| NetworkSetupError::DefaultInterface {
        source: NetworkOpError::transient(error.to_string()),
    })?;

    // Always create our own namespace
    native::add_netns(ns).map_err(|source| NetworkSetupError::Namespace {
        ns: ns.to_string(),
        source,
    })?;

    // Create veth pair — in pod namespace (CNI) or root namespace (local)
    native::setup_host_veth(ns, net, pod_netns)
        .map_err(|source| NetworkSetupError::HostVeth { source })?;

    // Inner namespace setup — identical regardless of pod_netns
    native::setup_namespace_links(ns, net)
        .map_err(|source| NetworkSetupError::NamespaceLinks { source })?;

    // Host-side rules — in pod namespace or root namespace
    crate::firewall::ensure_host_table(&host_iface, pod_netns).map_err(|error| {
        NetworkSetupError::Firewall {
            source: classify_firewall_setup_error(error),
        }
    })?;
    crate::firewall::setup_namespace_rules(ns, &net.vpeer(), &net.vpeer_ip_bare(), GUEST_IP)
        .map_err(|error| NetworkSetupError::Firewall {
            source: classify_firewall_setup_error(error),
        })?;
    if let Some(pod_ns) = pod_netns {
        native::write_sysctl(Some(pod_ns), "net.ipv4.ip_forward", "1").map_err(|detail| {
            NetworkSetupError::Sysctl {
                key: "net.ipv4.ip_forward",
                source: NetworkOpError::permanent(detail),
            }
        })?;
    } else {
        ensure_root_ip_forward().map_err(|detail| NetworkSetupError::Sysctl {
            key: "net.ipv4.ip_forward",
            source: NetworkOpError::permanent(detail),
        })?;
    }
    native::add_host_route(net, pod_netns)
        .map_err(|source| NetworkSetupError::HostRoute { source })?;
    native::setup_namespace_sysctls(ns).map_err(|detail| NetworkSetupError::Sysctl {
        key: "namespace",
        source: detail,
    })?;
    Ok(())
}

/// Tear down networking for a sandbox.
///
/// `ns` — the sandbox network namespace name.
/// `pod_netns` — if Some, clean up the veth/route in the pod namespace; if None, root namespace.
pub fn teardown_network(
    ns: &str,
    net: &NetId,
    pod_netns: Option<&str>,
) -> Result<(), NetworkTeardownError> {
    crate::firewall::invalidate_host_table(pod_netns);
    finish_network_teardown(
        || teardown_network_links(ns, net, pod_netns),
        || {
            if let Some(namespace) = pod_netns
                && !try_namespace_exists(namespace)?
            {
                return Ok(());
            }
            crate::firewall::remove_ingress_dnat_rules(
                pod_netns,
                crate::firewall::PortSelector::All,
                "",
                &net.vpeer_ip_bare(),
            )?;
            Ok(())
        },
    )
}

fn finish_network_teardown(
    links: impl FnOnce() -> Result<(), NetworkTeardownError>,
    ingress: impl FnOnce() -> Result<(), NetworkTeardownError>,
) -> Result<(), NetworkTeardownError> {
    // Independent obligations: a failed link/route probe must not suppress
    // ingress removal. Neither failure permits reuse of the slot identity.
    let links = links();
    let ingress = ingress().inspect_err(|error| {
        tracing::warn!(%error, "network teardown ingress cleanup incomplete");
    });
    links.and(ingress)
}

fn teardown_network_links(
    ns: &str,
    net: &NetId,
    pod_netns: Option<&str>,
) -> Result<(), NetworkTeardownError> {
    let mut removed_leaked_root_peer = false;

    if ns_exists(ns) {
        crate::firewall::teardown_namespace_rules(ns);
        log_cleanup_result(
            "delete sandbox tap link",
            native::delete_link(TAP_DEVICE, Some(ns)),
        );
        // Always delete the sandbox namespace (SPACE always owns it)
        log_cleanup_result("delete sandbox netns", native::delete_netns(ns));
    }

    // Clean up sveth and host route — in pod namespace or root
    if let Some(pod_ns) = pod_netns {
        // Delete sveth in pod namespace (also removes vpeer)
        log_cleanup_result(
            "delete pod namespace sveth",
            native::delete_link(&net.sveth(), Some(pod_ns)),
        );
        log_cleanup_result(
            "delete pod namespace host route",
            native::delete_host_route(net, Some(pod_ns)),
        );
    } else {
        if link_exists(&net.sveth()) {
            log_cleanup_result(
                "delete root namespace sveth",
                native::delete_link(&net.sveth(), None),
            );
        }
        // Defensive cleanup for malformed slots from failed/incomplete native
        // setup. These objects are namespace-owned and must never live in root.
        let leaked_peer = link_exists(&net.vpeer());
        removed_leaked_root_peer = leaked_peer;
        if leaked_peer {
            log_cleanup_result(
                "delete leaked root vpeer",
                native::delete_link(&net.vpeer(), None),
            );
        }
        if leaked_peer && link_exists(TAP_DEVICE) {
            log_cleanup_result(
                "delete leaked root tap",
                native::delete_link(TAP_DEVICE, None),
            );
        }
        log_cleanup_result(
            "delete root sveth default route",
            native::delete_default_route_for_link(&net.sveth(), None),
        );
        log_cleanup_result(
            "delete root vpeer default route",
            native::delete_default_route_for_link(&net.vpeer(), None),
        );
        log_cleanup_result(
            "delete root host route",
            native::delete_host_route(net, None),
        );
    }

    // Best-effort deletion is not completion evidence. In particular, native
    // route deletion can race or fail after a successful dump. Do not publish
    // missing/ready (and reuse the UID/address) until absence is verified.
    require_network_absent(format!("namespace {ns}"), try_namespace_exists(ns))?;
    if removed_leaked_root_peer {
        require_network_absent(
            format!("leaked root tap {TAP_DEVICE}"),
            try_link_exists_in(TAP_DEVICE, None),
        )?;
    }
    if let Some(pod_ns) = pod_netns
        && !try_namespace_exists(pod_ns)?
    {
        // CNI has removed this namespace; there is no remaining host-side
        // addressable state to clean there. Never interpret a probe error so.
        return Ok(());
    }
    for name in [net.sveth(), net.vpeer()] {
        let present = try_link_exists_in(&name, pod_netns);
        require_network_absent(format!("interface {name}"), present)?;
    }
    require_network_absent(
        format!("route for {}", net.vpeer_ip_bare()),
        try_host_route_exists_in(net, pod_netns),
    )?;
    Ok(())
}

fn log_cleanup_result(operation: &str, result: Result<(), String>) {
    if let Err(error) = result {
        tracing::debug!(operation, %error, "best-effort network cleanup failed");
    }
}

/// Reset a reusable network slot in place.
///
/// This preserves the stable namespace/veth/tap objects and only scrubs
/// lease-scoped mutable state. If core slot objects are missing, callers should
/// fall back to full teardown + setup.
pub fn reset_network_slot(ns: &str, net: &NetId, pod_netns: Option<&str>) -> Result<(), String> {
    if !ns_exists(ns) {
        return Err(format!("missing namespace {ns}"));
    }
    if !link_exists_in(&net.sveth(), pod_netns) {
        return Err(format!("missing host-side interface {}", net.sveth()));
    }
    if !ns_link_exists(ns, &net.vpeer()) {
        return Err(format!("missing namespace interface {}", net.vpeer()));
    }
    if !ns_link_exists(ns, TAP_DEVICE) {
        return Err(format!("missing namespace tap {TAP_DEVICE}"));
    }
    let vpeer_ip = net.vpeer_ip_bare();
    crate::firewall::remove_ingress_dnat_rules(
        pod_netns,
        crate::firewall::PortSelector::All,
        "",
        &vpeer_ip,
    )
    .map_err(|e| e.to_string())?;
    crate::firewall::teardown_proxy_rules(ns);
    crate::firewall::teardown_namespace_rules(ns);
    clear_tproxy_policy_routing(ns);
    flush_namespace_transient_state(ns);
    native::reset_namespace_links(ns, net)?;
    crate::firewall::setup_namespace_rules(ns, &net.vpeer(), &net.vpeer_ip_bare(), GUEST_IP)
        .map_err(|e| e.to_string())?;
    if let Some(pod_ns) = pod_netns {
        native::write_sysctl(Some(pod_ns), "net.ipv4.ip_forward", "1")?;
    } else {
        ensure_root_ip_forward()?;
    }
    native::add_host_route(net, pod_netns).map_err(|error| error.to_string())?;
    native::setup_namespace_sysctls(ns).map_err(|error| error.to_string())?;

    Ok(())
}

/// Set up TPROXY rules and policy routing in a namespace for net-gateway.
/// `ns` — the network namespace name.
/// `vpeer` — the vpeer interface name (for inbound DROP rule).
pub fn setup_tproxy(ns: &str, uid: u32, vpeer: &str, dhcp: bool) -> Result<(), String> {
    crate::firewall::setup_netgtw_tproxy_rules(ns, uid, vpeer, dhcp).map_err(|e| e.to_string())?;

    // Use "replace" to be idempotent — the rule/route may already exist if
    // the namespace was reused (e.g., suspend → restore reuses the same ns).
    native::add_tproxy_rule_tolerant(
        ns,
        crate::firewall::TPROXY_MARK,
        crate::firewall::TPROXY_RT_TABLE,
    );
    native::replace_tproxy_route(ns, crate::firewall::TPROXY_RT_TABLE)?;
    Ok(())
}

fn clear_tproxy_policy_routing(ns: &str) {
    log_cleanup_result(
        "clear tproxy policy routing",
        native::clear_tproxy_policy_routing(
            ns,
            crate::firewall::TPROXY_MARK,
            crate::firewall::TPROXY_RT_TABLE,
        ),
    );
}

fn flush_namespace_transient_state(ns: &str) {
    log_cleanup_result("flush namespace neighbours", native::flush_neighbours(ns));
}

/// Get the default outgoing interface from the root namespace.
fn get_default_iface() -> Result<String, DefaultIfaceError> {
    native::default_iface(None)
}

pub fn host_network_ready() -> Result<(), String> {
    let iface = get_default_iface().map_err(|error| error.to_string())?;
    if iface == "lo" {
        return Err("default network interface is loopback".to_string());
    }
    if !link_exists(&iface) {
        return Err(format!("default network interface {iface} does not exist"));
    }
    Ok(())
}

fn get_default_iface_cached() -> Result<String, DefaultIfaceError> {
    if let Some(iface) = ROOT_DEFAULT_IFACE.get() {
        return Ok(iface.clone());
    }
    let iface = get_default_iface()?;
    let _ = ROOT_DEFAULT_IFACE.set(iface.clone());
    Ok(iface)
}

fn ensure_root_ip_forward() -> Result<(), String> {
    if ROOT_IP_FORWARD_ENABLED.get().is_some() {
        return Ok(());
    }
    native::write_sysctl(None, "net.ipv4.ip_forward", "1")?;
    let _ = ROOT_IP_FORWARD_ENABLED.set(());
    Ok(())
}

/// Get the default outgoing interface from inside a network namespace.
fn get_default_iface_in_ns(ns: &str) -> Result<String, DefaultIfaceError> {
    native::default_iface(Some(ns))
}

/// Return the ingress interface to bind host-port DNAT rules to.
///
/// In Kubernetes/CNI mode the DNAT rule lives in the pod namespace, so the
/// namespace's default interface is the external ingress boundary. In local
/// mode the rule lives in the root namespace and uses the root default
/// interface cached during networking setup.
///
/// Returns [`DefaultIfaceError::NetnsMissing`] when the pod namespace's
/// /run/netns entry is gone, so ExposePort can classify the failure as
/// terminal instead of retryable.
pub fn ingress_iface_for_namespace(ns: Option<&str>) -> Result<String, DefaultIfaceError> {
    match ns {
        Some(ns) => get_default_iface_in_ns(ns),
        None => get_default_iface_cached(),
    }
}

pub fn ns_exists(name: &str) -> bool {
    native::ns_exists(name)
}

pub fn link_exists(name: &str) -> bool {
    native::link_exists(name, None)
}

/// Check if a link exists — in a specific namespace, or root if None.
pub fn link_exists_in(name: &str, ns: Option<&str>) -> bool {
    native::link_exists(name, ns)
}

pub fn try_link_exists_in(name: &str, ns: Option<&str>) -> Result<bool, NetworkProbeError> {
    native::try_link_exists(name, ns)
}

pub fn ns_link_exists(ns: &str, name: &str) -> bool {
    native::link_exists(name, Some(ns))
}

pub fn host_route_exists(net: &NetId) -> bool {
    host_route_exists_in(net, None)
}

/// Check if the host route exists — in a specific namespace, or root if None.
pub fn host_route_exists_in(net: &NetId, ns: Option<&str>) -> bool {
    native::host_route_exists(net, ns)
}

pub fn try_host_route_exists_in(net: &NetId, ns: Option<&str>) -> Result<bool, NetworkProbeError> {
    native::try_host_route_exists(net, ns)
}

pub fn root_default_route_uses_space_link() -> bool {
    native::root_default_route_uses_space_link()
}

pub fn try_root_default_route_uses_space_link() -> Result<bool, NetworkProbeError> {
    native::try_root_default_route_uses_space_link()
}

/// Check if a host nftables rule exists matching a pattern.
pub fn nft_rule_exists(table: &str, chain: &str, pattern: &str) -> bool {
    crate::host_command::output(
        Command::new("nft").args(["list", "chain", "inet", table, chain]),
        crate::host_command::PROBE_BUDGET,
        &[],
    )
    .map(|o| o.status.success() && String::from_utf8_lossy(&o.stdout).contains(pattern))
    .unwrap_or(false)
}

/// Check if a namespace nftables rule exists matching a pattern.
pub fn ns_nft_rule_exists(ns: &str, table: &str, chain: &str, pattern: &str) -> bool {
    command_output_in_ns(ns, "nft", &["list", "chain", "inet", table, chain])
        .map(|o| o.status.success() && String::from_utf8_lossy(&o.stdout).contains(pattern))
        .unwrap_or(false)
}

pub(crate) fn command_output_in_ns(ns: &str, program: &str, args: &[&str]) -> io::Result<Output> {
    native::command_output_in_ns(ns, program, args)
}

pub(crate) fn command_status_in_ns(
    ns: &str,
    program: &str,
    args: &[&str],
) -> io::Result<ExitStatus> {
    native::command_status_in_ns(ns, program, args)
}

pub(crate) fn command_output_with_stdin_in_ns(
    ns: &str,
    program: &str,
    args: &[&str],
    stdin: &[u8],
) -> io::Result<Output> {
    native::command_output_with_stdin_in_ns(ns, program, args, stdin)
}

pub fn sysctl_check(key: &str, expected: &str) -> bool {
    native::read_sysctl(None, key)
        .map(|value| value.trim() == expected)
        .unwrap_or(false)
}

pub fn ns_sysctl_check(ns: &str, key: &str, expected: &str) -> bool {
    native::read_sysctl(Some(ns), key)
        .map(|value| value.trim() == expected)
        .unwrap_or(false)
}

pub fn try_ns_sysctl_check(ns: &str, key: &str, expected: &str) -> Result<bool, NetworkProbeError> {
    native::read_sysctl(Some(ns), key)
        .map(|value| value.trim() == expected)
        .map_err(|detail| NetworkProbeError::Sysctl {
            key: key.to_string(),
            detail,
        })
}

#[cfg(target_os = "linux")]
#[path = "net/linux.rs"]
mod native;

#[cfg(not(target_os = "linux"))]
#[path = "net/fallback.rs"]
mod native;

#[cfg(test)]
mod tests;

/// Create a connected descriptor in the lease's namespace, with a bounded
/// connect. Call on the blocking host-work executor, never a Tokio worker.
pub(crate) fn guest_tcp_stream(
    ns: &str,
    port: std::num::NonZeroU16,
) -> io::Result<std::net::TcpStream> {
    native::guest_tcp_stream(ns, port)
}

pub(crate) fn guest_dhcp_socket(ns: &str) -> io::Result<std::net::UdpSocket> {
    native::guest_dhcp_socket(ns)
}
