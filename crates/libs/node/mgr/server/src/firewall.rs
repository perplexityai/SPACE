//! nftables-based firewall management for SPACE sandboxes.
//!
//! Uses `nft` CLI with JSON input for atomic rule application, except the
//! guest L2 ingress policy, whose device-bound chain only native syntax can
//! create on every supported nftables version.
//! Host-level rules live in the "space" table; per-namespace rules live in
//! a "sandbox" table created inside each network namespace.

use crate::host_command::Command;
use std::fs;
use std::net::{IpAddr, Ipv4Addr};
use std::process::Output;
mod host_cache;

use crate::net::{SANDBOX_SUBNET, SANDBOX_SUBNET_PREFIX_LEN};
use pplx_space_metrics::node_manager::GuestUdp443Action;
use pplx_space_net_common::PLATFORM_EGRESS_HARD_DENY_CIDRS;

const SPACE_TABLE: &str = "space";
const SANDBOX_TABLE: &str = "sandbox";
const SANDBOX_L2_TABLE: &str = "sandbox_l2";
const GUEST_VLAN_DROP_COMMENT: &str = "drop-guest-vlan";
const STALE_GATEWAY_MAC_COMMENT: &str = "accept-stale-gateway-mac";
const INBOUND_REPLY_COMMENT: &str = "ct-inbound-replies";
const TPROXY_INBOUND_REPLY_COMMENT: &str = "skip-tproxy-inbound-replies";
const LOCAL_SERVICE_REPLY_COMMENT: &str = "skip-tproxy-local-service-replies";
const INGRESS_TABLE: &str = "ingress";
const INGRESS_DNAT_COMMENT_PREFIX: &str = "space-dnat-";
const LEGACY_INGRESS_DNAT_COMMENT_PREFIX: &str = "dnat-";
const PRE_TPROXY_GUARDRAILS_CHAIN: &str = "pre_tproxy_guardrails";
const DHCP_BOOTSTRAP_COMMENT: &str = "execution-dhcp-bootstrap";
const UDP443_COMMENT: &str = "platform-egress-pre-tproxy-guardrail-quic-udp443";
const PRE_TPROXY_GUARDRAILS_PRIO: i32 = -175;
const PRE_TPROXY_GUARDRAILS_POLICY_PATH: &str =
    "/etc/space/nftables/sandbox-pre-tproxy-guardrails.json";

/// The guest L2 ingress policy as one `nft -f` transaction: the netdev table,
/// its ingress chain bound to the TAP, the two VLAN drop rules, and the rule
/// that accepts the guest's frames for a stale gateway MAC. Flushing the owned
/// chain first prevents duplicate rules when it is re-applied.
///
/// A restored guest keeps the neighbour table it was snapshotted with, whose
/// gateway entry holds the MAC of the TAP on the slot the snapshot was taken
/// on. Every TAP has its own MAC, and a restore normally lands on another
/// slot, on this node or another, so the guest addresses its first frames to
/// a MAC this TAP does not have. The kernel marks those frames for another
/// host and drops them before IPv4 or ARP sees them, and the guest, whose
/// entry still reads as reachable, does not ask again for many seconds. On
/// this point-to-point link every IPv4 or ARP frame from the guest is meant
/// for the host, so the last rule reclassifies these as addressed to it. It
/// changes no verdict: the namespace's IPv4 rules filter them exactly as
/// frames sent to the TAP's own MAC, and the VLAN drops run first. The host's
/// next ARP exchange with the guest then corrects the guest's entry.
///
/// Written in nft's native syntax because its JSON input cannot create this
/// chain on nftables 1.0.4 (Amazon Linux 2023): given `"dev": "tap0"` it skips
/// the device-bound chain without error, and the first rule then fails with
/// "No such file or directory". The native syntax works on every version.
/// Validation still reads the result back as JSON, plus the native listing
/// for the device binding, which the JSON output omits.
fn guest_l2_ingress_policy_script() -> String {
    let tap = crate::net::TAP_DEVICE;
    format!(
        "add table netdev {SANDBOX_L2_TABLE}
add chain netdev {SANDBOX_L2_TABLE} ingress {{ type filter hook ingress device \"{tap}\" priority -500; }}
flush chain netdev {SANDBOX_L2_TABLE} ingress
add rule netdev {SANDBOX_L2_TABLE} ingress meta protocol {{ 0x8100, 0x88a8 }} counter drop comment \"{GUEST_VLAN_DROP_COMMENT}\"
add rule netdev {SANDBOX_L2_TABLE} ingress ether type {{ 0x8100, 0x88a8 }} counter drop comment \"{GUEST_VLAN_DROP_COMMENT}\"
add rule netdev {SANDBOX_L2_TABLE} ingress meta protocol {{ 0x0800, 0x0806 }} meta pkttype other counter meta pkttype set host comment \"{STALE_GATEWAY_MAC_COMMENT}\"
"
    )
}

/// Atomically install or refresh guest L2 ingress filtering in an existing
/// sandbox namespace. Flushing the owned chain prevents duplicate rules on
/// node-manager restart and resets only this policy's kernel counters.
pub fn ensure_guest_l2_ingress_policy(ns: &str) -> Result<(), FirewallValidationError> {
    apply_nft_script(&guest_l2_ingress_policy_script(), Some(ns))?;
    let chain =
        nft_chain_json(Some(ns), "netdev", SANDBOX_L2_TABLE, "ingress")?.ok_or_else(|| {
            FirewallValidationError::Missing(
                "guest L2 ingress chain is missing after apply".to_string(),
            )
        })?;
    validate_guest_l2_ingress_chain(&chain)?;
    validate_guest_l2_ingress_binding(ns)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PortSelector {
    All,
    Port(u16),
}

#[derive(Debug, thiserror::Error)]
pub enum FirewallError {
    #[error("nft list chain failed to start: {0}")]
    ListStart(#[source] std::io::Error),
    #[error("nft list chain failed: {stderr}")]
    ListFailed { stderr: String },
    #[error("failed to parse nft JSON output: {0}")]
    ListJson(#[source] serde_json::Error),
    #[error("guest L2 ingress policy is missing")]
    GuestL2IngressMissing,
    #[error("owned nft rule {comment} is missing its handle")]
    MissingRuleHandle { comment: &'static str },
    #[error("nft delete rule failed to start: {0}")]
    DeleteStart(#[source] std::io::Error),
    #[error("nft delete rule {family} {table} {chain} handle {handle} failed")]
    DeleteFailed {
        family: String,
        table: String,
        chain: String,
        handle: u64,
    },
    #[error("failed to read pre-TPROXY guardrails policy file {path}: {source}")]
    ReadPreTproxyPolicy {
        path: &'static str,
        #[source]
        source: std::io::Error,
    },
    #[error("nft exec failed: {0}")]
    Exec(#[source] std::io::Error),
    #[error("nft apply failed: {stderr}\ninput: {input}")]
    Apply { stderr: String, input: String },
}

pub type FirewallResult<T> = Result<T, FirewallError>;

#[derive(Debug, thiserror::Error)]
pub enum FirewallValidationError {
    #[error(transparent)]
    Inspect(#[from] FirewallError),
    #[error("{0}")]
    Missing(String),
    #[error(transparent)]
    PackagedPolicy(#[from] PackagedPolicyError),
}

/// The image-baked guardrails policy could not be parsed.
///
/// This is a host fault, not namespace drift. It fails identically for every
/// namespace on the node and rebuilding one cannot repair it, so callers fence
/// the slot and retry rather than quarantining a healthy namespace. Keeping the
/// `serde_json::Error` as the source preserves the line and column an operator
/// needs to find the bad edit.
#[derive(Debug, thiserror::Error)]
#[error(
    "packaged pre-TPROXY guardrails policy at {} is malformed",
    PRE_TPROXY_GUARDRAILS_POLICY_PATH
)]
pub struct PackagedPolicyError(#[source] pub(crate) serde_json::Error);

/// Ensure the host-level "space" nftables table exists with MASQUERADE + FORWARD chains.
/// Idempotent — flushes and replaces atomically.
///
/// `host_ns` — if Some, apply rules in the pod namespace (CNI); if None, root namespace (local).
pub fn ensure_host_table(host_iface: &str, host_ns: Option<&str>) -> FirewallResult<()> {
    let evidence = host_cache::Evidence::open(host_ns, host_iface)?;
    if evidence.matches_live()? {
        return Ok(());
    }

    // Step 1: Create table + chains (idempotent — nft "add" is a no-op if exists)
    let setup = format!(
        r#"{{ "nftables": [
  {{ "add": {{ "table": {{ "family": "inet", "name": "{SPACE_TABLE}" }} }} }},
  {{ "add": {{ "chain": {{ "family": "inet", "table": "{SPACE_TABLE}", "name": "postrouting",
    "type": "nat", "hook": "postrouting", "prio": 100 }} }} }},
  {{ "add": {{ "chain": {{ "family": "inet", "table": "{SPACE_TABLE}", "name": "forward",
    "type": "filter", "hook": "forward", "prio": 0 }} }} }},
  {{ "add": {{ "chain": {{ "family": "inet", "table": "{SPACE_TABLE}", "name": "output",
    "type": "filter", "hook": "output", "prio": 0 }} }} }}
] }}"#
    );
    apply_nft_json(&setup, host_ns)?;

    // Step 2: Flush + re-add MASQUERADE + wildcard FORWARD rules (idempotent refresh)
    let host_guardrail_rules =
        platform_guardrail_drop_rules_json("inet", SPACE_TABLE, "forward", "sveth-*");
    let rules = format!(
        r#"{{ "nftables": [
  {{ "flush": {{ "chain": {{ "family": "inet", "table": "{SPACE_TABLE}", "name": "postrouting" }} }} }},
  {{ "add": {{ "rule": {{ "family": "inet", "table": "{SPACE_TABLE}", "chain": "postrouting",
    "expr": [
      {{ "match": {{ "left": {{ "payload": {{ "protocol": "ip", "field": "saddr" }} }},
                     "right": {{ "prefix": {{ "addr": "{SANDBOX_SUBNET}", "len": {SANDBOX_SUBNET_PREFIX_LEN} }} }},
                     "op": "==" }} }},
      {{ "match": {{ "left": {{ "meta": {{ "key": "oifname" }} }},
                     "right": "{host_iface}",
                     "op": "==" }} }},
      {{ "masquerade": null }}
    ] }} }} }},
  {{ "flush": {{ "chain": {{ "family": "inet", "table": "{SPACE_TABLE}", "name": "forward" }} }} }},
  {{ "add": {{ "rule": {{ "family": "inet", "table": "{SPACE_TABLE}", "chain": "forward",
    "comment": "block-cross-sandbox",
    "expr": [
      {{ "match": {{ "left": {{ "meta": {{ "key": "iifname" }} }},
                     "right": "sveth-*", "op": "==" }} }},
      {{ "match": {{ "left": {{ "meta": {{ "key": "oifname" }} }},
                     "right": "sveth-*", "op": "==" }} }},
      {{ "drop": null }}
    ] }} }} }},
{host_guardrail_rules},
  {{ "add": {{ "rule": {{ "family": "inet", "table": "{SPACE_TABLE}", "chain": "forward",
    "expr": [
      {{ "match": {{ "left": {{ "meta": {{ "key": "iifname" }} }},
                     "right": "sveth-*", "op": "==" }} }},
      {{ "accept": null }}
    ] }} }} }},
  {{ "add": {{ "rule": {{ "family": "inet", "table": "{SPACE_TABLE}", "chain": "forward",
    "expr": [
      {{ "match": {{ "left": {{ "meta": {{ "key": "oifname" }} }},
                     "right": "sveth-*", "op": "==" }} }},
      {{ "accept": null }}
    ] }} }} }},
  {{ "flush": {{ "chain": {{ "family": "inet", "table": "{SPACE_TABLE}", "name": "output" }} }} }},
  {{ "add": {{ "rule": {{ "family": "inet", "table": "{SPACE_TABLE}", "chain": "output",
    "comment": "block-non-root-sandbox-access",
    "expr": [
      {{ "match": {{ "left": {{ "meta": {{ "key": "skuid" }} }},
                     "right": 0, "op": "!=" }} }},
      {{ "match": {{ "left": {{ "meta": {{ "key": "oifname" }} }},
                     "right": "sveth-*", "op": "==" }} }},
      {{ "match": {{ "left": {{ "payload": {{ "protocol": "ip", "field": "daddr" }} }},
                     "right": {{ "prefix": {{ "addr": "{SANDBOX_SUBNET}", "len": {SANDBOX_SUBNET_PREFIX_LEN} }} }},
                     "op": "==" }} }},
      {{ "drop": null }}
    ] }} }} }}
] }}"#
    );
    apply_nft_json(&rules, host_ns)?;
    evidence.remember()
}

pub(crate) fn invalidate_host_table(host_ns: Option<&str>) {
    host_cache::invalidate(host_ns);
}

/// Create the per-namespace "sandbox" nftables table with SNAT + DNAT rules.
pub fn setup_namespace_rules(
    ns: &str,
    vpeer: &str,
    vpeer_ip: &str,
    guest_ip: &str,
) -> FirewallResult<()> {
    let pre_tproxy_guardrail_rules = load_pre_tproxy_guardrail_rules()?;
    // Guest L2 filtering first, so the TAP drops VLAN frames before the
    // namespace's routing and NAT rules exist.
    apply_nft_script(&guest_l2_ingress_policy_script(), Some(ns))?;
    apply_nft_json(
        &namespace_rules_json(vpeer, vpeer_ip, guest_ip, &pre_tproxy_guardrail_rules),
        Some(ns),
    )?;
    ensure_namespace_forward_drop_rules(ns, vpeer)
}

fn namespace_rules_json(
    vpeer: &str,
    vpeer_ip: &str,
    guest_ip: &str,
    pre_tproxy_guardrail_rules: &str,
) -> String {
    // Use "ip" family (not "inet") because snat/dnat require unambiguous address family.
    // "inet" would require "snat ip" qualifier which the JSON API doesn't support cleanly.
    let namespace_guardrail_rules =
        platform_guardrail_drop_rules_json("ip", SANDBOX_TABLE, "guardrails", "tap0");
    // This rule must precede the packaged policy's established/related return.
    // A guest can otherwise forge the reverse tuple of a gateway-owned socket,
    // inherit its conntrack state, and reach the local socket without TPROXY.
    format!(
        r#"{{ "nftables": [
  {{ "add": {{ "table": {{ "family": "ip", "name": "{SANDBOX_TABLE}" }} }} }},
  {{ "add": {{ "chain": {{ "family": "ip", "table": "{SANDBOX_TABLE}", "name": "{PRE_TPROXY_GUARDRAILS_CHAIN}",
    "type": "filter", "hook": "prerouting", "prio": {PRE_TPROXY_GUARDRAILS_PRIO} }} }} }},
  {{ "add": {{ "rule": {{ "family": "ip", "table": "{SANDBOX_TABLE}", "chain": "{PRE_TPROXY_GUARDRAILS_CHAIN}",
    "comment": "drop-spoofed-guest-source",
    "expr": [
      {{ "match": {{ "left": {{ "meta": {{ "key": "iifname" }} }},
                     "right": "tap0", "op": "==" }} }},
      {{ "match": {{ "left": {{ "payload": {{ "protocol": "ip", "field": "saddr" }} }},
                     "right": "{guest_ip}", "op": "!=" }} }},
      {{ "drop": null }}
    ] }} }} }},
{pre_tproxy_guardrail_rules},
  {{ "add": {{ "chain": {{ "family": "ip", "table": "{SANDBOX_TABLE}", "name": "guardrails",
    "type": "filter", "hook": "forward", "prio": -200 }} }} }},
{namespace_guardrail_rules},
  {{ "add": {{ "chain": {{ "family": "ip", "table": "{SANDBOX_TABLE}", "name": "postrouting",
    "type": "nat", "hook": "postrouting", "prio": 100 }} }} }},
  {{ "add": {{ "rule": {{ "family": "ip", "table": "{SANDBOX_TABLE}", "chain": "postrouting",
    "expr": [
      {{ "match": {{ "left": {{ "meta": {{ "key": "mark" }} }},
                     "right": {OUTBOUND_MARK}, "op": "==" }} }},
      {{ "match": {{ "left": {{ "meta": {{ "key": "oifname" }} }},
                     "right": "{vpeer}", "op": "==" }} }},
      {{ "snat": {{ "addr": "{vpeer_ip}" }} }}
    ] }} }} }},
  {{ "add": {{ "chain": {{ "family": "ip", "table": "{SANDBOX_TABLE}", "name": "prerouting",
    "type": "nat", "hook": "prerouting", "prio": -100 }} }} }},
  {{ "add": {{ "rule": {{ "family": "ip", "table": "{SANDBOX_TABLE}", "chain": "prerouting",
    "expr": [
      {{ "match": {{ "left": {{ "meta": {{ "key": "iifname" }} }},
                     "right": "{vpeer}", "op": "==" }} }},
      {{ "match": {{ "left": {{ "payload": {{ "protocol": "ip", "field": "daddr" }} }},
                     "right": "{vpeer_ip}", "op": "==" }} }},
      {{ "dnat": {{ "addr": "{guest_ip}" }} }}
    ] }} }} }}
] }}"#
    )
}

/// Install only for an execution with a DHCP responder. The narrow DHCP
/// exception precedes source validation so a stale lease can receive a NAK.
/// All other traffic retains the normal anti-spoofing policy.
pub(crate) fn ensure_guest_dhcp_bootstrap(ns: &str) -> FirewallResult<()> {
    let chain = nft_chain_json(Some(ns), "ip", SANDBOX_TABLE, PRE_TPROXY_GUARDRAILS_CHAIN)?;
    let updates = guest_dhcp_bootstrap_updates(chain.as_ref())?;
    if updates.is_empty() {
        return Ok(());
    }
    apply_nft_json(
        &serde_json::json!({"nftables": updates}).to_string(),
        Some(ns),
    )
}

fn guest_dhcp_bootstrap_updates(
    chain: Option<&serde_json::Value>,
) -> FirewallResult<Vec<serde_json::Value>> {
    let desired = guest_dhcp_bootstrap_rule();
    let expressions = |rule: &serde_json::Value| {
        rule.get("expr")
            .and_then(serde_json::Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .map(|item| {
                        let mut item = normalize_set_membership_op(item);
                        if let Some(set) = item
                            .pointer_mut("/match/right/set")
                            .and_then(serde_json::Value::as_array_mut)
                        {
                            set.sort_by_key(serde_json::Value::to_string);
                        }
                        item
                    })
                    .collect::<Vec<_>>()
            })
    };
    let rules = chain
        .and_then(|chain| chain.get("nftables"))
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|entry| entry.get("rule"))
        .collect::<Vec<_>>();
    let owned = rules
        .iter()
        .filter(|rule| rule["comment"] == DHCP_BOOTSTRAP_COMMENT)
        .count();
    if owned == 1
        && rules.first().is_some_and(|rule| {
            rule["comment"] == DHCP_BOOTSTRAP_COMMENT && expressions(rule) == expressions(&desired)
        })
    {
        return Ok(Vec::new());
    }
    // Repair old source-zero-only rules and duplicates in one transaction.
    let mut updates = owned_rule_removals(
        chain,
        SANDBOX_TABLE,
        PRE_TPROXY_GUARDRAILS_CHAIN,
        DHCP_BOOTSTRAP_COMMENT,
    )?;
    updates.push(serde_json::json!({"insert": {"rule": desired}}));
    Ok(updates)
}

fn guest_dhcp_proxy_rule() -> serde_json::Value {
    serde_json::json!({
        "family": "ip", "table": PROXY_TABLE, "chain": "prerouting",
        "comment": "execution-local-dhcp",
        "expr": [
            {"match": {"left": {"meta": {"key": "iifname"}}, "right": "tap0", "op": "=="}},
            {"match": {"left": {"payload": {"protocol": "udp", "field": "sport"}}, "right": 68, "op": "=="}},
            {"match": {"left": {"payload": {"protocol": "udp", "field": "dport"}}, "right": 67, "op": "=="}},
            {"match": {"left": {"payload": {"protocol": "ip", "field": "daddr"}}, "right": {"set": ["255.255.255.255", crate::net::GUEST_GATEWAY]}, "op": "=="}},
            {"return": null}
        ]
    })
}

fn guest_dhcp_bootstrap_rule() -> serde_json::Value {
    serde_json::json!({
        "family": "ip", "table": SANDBOX_TABLE, "chain": PRE_TPROXY_GUARDRAILS_CHAIN,
        "comment": DHCP_BOOTSTRAP_COMMENT,
        "expr": [
            {"match": {"left": {"meta": {"key": "iifname"}}, "right": "tap0", "op": "=="}},
            {"match": {"left": {"payload": {"protocol": "ip", "field": "daddr"}}, "right": {"set": ["255.255.255.255", crate::net::GUEST_GATEWAY]}, "op": "=="}},
            {"match": {"left": {"payload": {"protocol": "udp", "field": "sport"}}, "right": 68, "op": "=="}},
            {"match": {"left": {"payload": {"protocol": "udp", "field": "dport"}}, "right": 67, "op": "=="}},
            {"return": null}
        ]
    })
}

fn load_pre_tproxy_guardrail_rules() -> FirewallResult<String> {
    let policy = fs::read_to_string(PRE_TPROXY_GUARDRAILS_POLICY_PATH).map_err(|source| {
        FirewallError::ReadPreTproxyPolicy {
            path: PRE_TPROXY_GUARDRAILS_POLICY_PATH,
            source,
        }
    })?;
    tracing::info!(
        path = PRE_TPROXY_GUARDRAILS_POLICY_PATH,
        source = "static_config",
        "loaded pre-TPROXY guardrails policy"
    );
    Ok(policy.trim_end().to_string())
}

fn platform_guardrail_drop_rules_json(
    family: &str,
    table: &str,
    chain: &str,
    iifname: &str,
) -> String {
    PLATFORM_EGRESS_HARD_DENY_CIDRS
        .iter()
        .filter_map(|guardrail| {
            let (addr, prefix_len) = guardrail
                .cidr
                .split_once('/')
                .expect("platform egress hard-deny CIDRs must include a prefix length");
            let addr_family = match addr
                .parse::<IpAddr>()
                .expect("platform egress hard-deny CIDR address must be valid")
            {
                IpAddr::V4(_) => "ip",
                IpAddr::V6(_) => "ip6",
            };
            if family != "inet" && family != addr_family {
                return None;
            }
            format!(
                r#"  {{ "add": {{ "rule": {{ "family": "{family}", "table": "{table}", "chain": "{chain}",
    "comment": "platform-egress-guardrail-{id}",
    "expr": [
      {{ "match": {{ "left": {{ "meta": {{ "key": "iifname" }} }},
                     "right": "{iifname}", "op": "==" }} }},
      {{ "match": {{ "left": {{ "payload": {{ "protocol": "{addr_family}", "field": "daddr" }} }},
                     "right": {{ "prefix": {{ "addr": "{addr}", "len": {prefix_len} }} }},
                     "op": "==" }} }},
      {{ "drop": null }}
    ] }} }} }}"#,
                id = guardrail.id
            )
            .into()
        })
        .collect::<Vec<_>>()
        .join(",\n")
}

/// Delete the per-namespace "sandbox" table.
pub fn teardown_namespace_rules(ns: &str) {
    if let Err(error) = nft_status(Some(ns), &["delete", "table", "ip", SANDBOX_TABLE]) {
        tracing::debug!(ns, %error, "sandbox nft table cleanup failed");
    }
    if let Err(error) = nft_status(Some(ns), &["delete", "table", "netdev", SANDBOX_L2_TABLE]) {
        tracing::debug!(ns, %error, "sandbox L2 nft table cleanup failed");
    }
}

/// Delete the per-namespace net-gateway TPROXY table.
pub fn teardown_proxy_rules(ns: &str) {
    if let Err(error) = nft_status(Some(ns), &["delete", "table", "ip", PROXY_TABLE]) {
        tracing::debug!(ns, %error, "proxy nft table cleanup failed");
    }
}

/// Check if the "space" table has wildcard forward rules — in a specific namespace, or root if None.
pub fn has_forward_rule_in(pattern: &str, ns: Option<&str>) -> bool {
    let output = nft_output(ns, &["list", "chain", "inet", SPACE_TABLE, "forward"]);
    output
        .map(|o| String::from_utf8_lossy(&o.stdout).contains(pattern))
        .unwrap_or(false)
}

/// Check if a namespace has SNAT rules.
pub fn ns_has_snat(ns: &str) -> bool {
    try_ns_has_snat(ns).unwrap_or(false)
}

pub fn try_ns_has_snat(ns: &str) -> Result<bool, FirewallError> {
    Ok(
        nft_chain_output(Some(ns), "ip", SANDBOX_TABLE, "postrouting")?
            .is_some_and(|output| String::from_utf8_lossy(&output.stdout).contains("snat")),
    )
}

/// Check if a namespace has DNAT rules.
pub fn ns_has_dnat(ns: &str) -> bool {
    try_ns_has_dnat(ns).unwrap_or(false)
}

pub fn try_ns_has_dnat(ns: &str) -> Result<bool, FirewallError> {
    Ok(
        nft_chain_output(Some(ns), "ip", SANDBOX_TABLE, "prerouting")?
            .is_some_and(|output| String::from_utf8_lossy(&output.stdout).contains("dnat")),
    )
}

// Proxy ports for net-gateway transparent proxy (TPROXY targets).
pub const NETGTW_TCP_PORT: u16 = pplx_space_net_common::proxy::TCP_PROXY_PORT;
pub const NETGTW_UDP_PORT: u16 = pplx_space_net_common::proxy::UDP_PROXY_PORT;

/// TPROXY fwmark used for policy routing (local delivery of intercepted packets).
pub const TPROXY_MARK: u32 = pplx_space_net_common::proxy::TPROXY_MARK;
/// Outbound fwmark set on proxy-originated sockets via SO_MARK.
/// Packets with this mark skip TPROXY re-interception in the prerouting chain.
pub const OUTBOUND_MARK: u32 = pplx_space_net_common::proxy::OUTBOUND_MARK;
/// Policy routing table ID for TPROXY.
pub const TPROXY_RT_TABLE: u32 = pplx_space_net_common::proxy::TPROXY_RT_TABLE;

/// Table name for the TPROXY mangle rules.
const PROXY_TABLE: &str = "proxy";

/// Set up net-gateway TPROXY rules inside a namespace:
/// 1. TPROXY TCP/UDP traffic from tap0 to net-gateway proxy ports.
/// 2. Keep the established reply to a gateway-local service connection local.
/// 3. Anti-loop: skip traffic originating from the proxy process (by UID).
/// 4. DROP all forwarded traffic from tap0 (fail-closed egress).
/// 5. DROP all forwarded traffic to tap0 via vpeer (fail-closed ingress).
/// 6. Create "inbound" chain at priority -1 for guest port allowlist (empty = all closed).
///
/// Requires policy routing to be set up separately via `setup_tproxy_policy_routing`.
pub fn setup_netgtw_tproxy_rules(
    ns: &str,
    uid: u32,
    vpeer: &str,
    dhcp: bool,
) -> FirewallResult<()> {
    ensure_namespace_forward_drop_rules(ns, vpeer)?;
    apply_nft_json(&netgtw_tproxy_rules_json(uid, dhcp), Some(ns))
}

fn netgtw_tproxy_rules_json(uid: u32, dhcp: bool) -> String {
    // This function exclusively owns proxy/prerouting and proxy/divert; other
    // callers must not add rules there. Rebuild both in one nft transaction so
    // legacy broad reply exceptions cannot survive ahead of the corrected rules.
    let guest_gateway = crate::net::GUEST_GATEWAY;
    let dhcp_rule = if dhcp {
        format!(
            "{},",
            serde_json::json!({"add": {"rule": guest_dhcp_proxy_rule()}})
        )
    } else {
        String::new()
    };
    format!(
        r#"{{ "nftables": [
  {{ "add": {{ "table": {{ "family": "ip", "name": "{PROXY_TABLE}" }} }} }},
  {{ "add": {{ "chain": {{ "family": "ip", "table": "{PROXY_TABLE}", "name": "prerouting",
    "type": "filter", "hook": "prerouting", "prio": -150 }} }} }},
  {{ "add": {{ "chain": {{ "family": "ip", "table": "{PROXY_TABLE}", "name": "divert",
    "type": "filter", "hook": "prerouting", "prio": -149 }} }} }},
  {{ "flush": {{ "chain": {{ "family": "ip", "table": "{PROXY_TABLE}", "name": "prerouting" }} }} }},
  {{ "flush": {{ "chain": {{ "family": "ip", "table": "{PROXY_TABLE}", "name": "divert" }} }} }},
  {dhcp_rule}
  {{ "add": {{ "rule": {{ "family": "ip", "table": "{PROXY_TABLE}", "chain": "prerouting",
    "expr": [
      {{ "match": {{ "left": {{ "meta": {{ "key": "mark" }} }},
                     "right": {OUTBOUND_MARK}, "op": "==" }} }},
      {{ "return": null }}
    ] }} }} }},
  {{ "add": {{ "rule": {{ "family": "ip", "table": "{PROXY_TABLE}", "chain": "prerouting",
    "expr": [
      {{ "match": {{ "left": {{ "meta": {{ "key": "skuid" }} }},
                     "right": {uid}, "op": "==" }} }},
      {{ "return": null }}
    ] }} }} }},
  {{ "add": {{ "rule": {{ "family": "ip", "table": "{PROXY_TABLE}", "chain": "prerouting",
    "comment": "{LOCAL_SERVICE_REPLY_COMMENT}",
    "expr": [
      {{ "match": {{ "left": {{ "meta": {{ "key": "iifname" }} }},
                     "right": "tap0", "op": "==" }} }},
      {{ "match": {{ "left": {{ "meta": {{ "key": "l4proto" }} }},
                     "right": "tcp", "op": "==" }} }},
      {{ "match": {{ "left": {{ "ct": {{ "key": "state" }} }},
                     "right": "established", "op": "==" }} }},
      {{ "match": {{ "left": {{ "ct": {{ "key": "direction" }} }},
                     "right": "reply", "op": "==" }} }},
      {{ "match": {{ "left": {{ "payload": {{ "protocol": "ip", "field": "daddr" }} }},
                     "right": "{guest_gateway}", "op": "==" }} }},
      {{ "return": null }}
    ] }} }} }},
  {{ "add": {{ "rule": {{ "family": "ip", "table": "{PROXY_TABLE}", "chain": "prerouting",
    "expr": [
      {{ "match": {{ "left": {{ "meta": {{ "key": "iifname" }} }},
                     "right": "tap0", "op": "==" }} }},
      {{ "match": {{ "left": {{ "meta": {{ "key": "l4proto" }} }},
                     "right": "tcp", "op": "==" }} }},
      {{ "tproxy": {{ "port": {NETGTW_TCP_PORT} }} }},
      {{ "mangle": {{ "key": {{ "meta": {{ "key": "mark" }} }}, "value": {TPROXY_MARK} }} }},
      {{ "accept": null }}
    ] }} }} }},
  {{ "add": {{ "rule": {{ "family": "ip", "table": "{PROXY_TABLE}", "chain": "prerouting",
    "expr": [
      {{ "match": {{ "left": {{ "meta": {{ "key": "iifname" }} }},
                     "right": "tap0", "op": "==" }} }},
      {{ "match": {{ "left": {{ "meta": {{ "key": "l4proto" }} }},
                     "right": "udp", "op": "==" }} }},
      {{ "tproxy": {{ "port": {NETGTW_UDP_PORT} }} }},
      {{ "mangle": {{ "key": {{ "meta": {{ "key": "mark" }} }}, "value": {TPROXY_MARK} }} }},
      {{ "accept": null }}
    ] }} }} }},
  {{ "add": {{ "rule": {{ "family": "ip", "table": "{PROXY_TABLE}", "chain": "divert",
    "expr": [
      {{ "match": {{ "left": {{ "meta": {{ "key": "l4proto" }} }},
                     "right": "tcp", "op": "==" }} }},
      {{ "match": {{ "left": {{ "socket": {{ "key": "transparent" }} }},
                     "right": 1, "op": "==" }} }},
      {{ "mangle": {{ "key": {{ "meta": {{ "key": "mark" }} }}, "value": {TPROXY_MARK} }} }},
      {{ "accept": null }}
    ] }} }} }},
  {{ "add": {{ "rule": {{ "family": "ip", "table": "{PROXY_TABLE}", "chain": "divert",
    "expr": [
      {{ "match": {{ "left": {{ "meta": {{ "key": "l4proto" }} }},
                     "right": "udp", "op": "==" }} }},
      {{ "match": {{ "left": {{ "socket": {{ "key": "transparent" }} }},
                     "right": 1, "op": "==" }} }},
      {{ "mangle": {{ "key": {{ "meta": {{ "key": "mark" }} }}, "value": {TPROXY_MARK} }} }},
      {{ "accept": null }}
    ] }} }} }},
  {{ "add": {{ "rule": {{ "family": "ip", "table": "{PROXY_TABLE}", "chain": "prerouting",
    "comment": "net-gateway-tproxy-ready-marker",
    "expr": [
      {{ "match": {{ "left": {{ "meta": {{ "key": "iifname" }} }},
                     "right": "tap0", "op": "==" }} }},
      {{ "return": null }}
    ] }} }} }}
] }}"#
    )
}

pub(crate) fn guest_udp443_packets(ns: &str) -> FirewallResult<Option<(u64, GuestUdp443Action)>> {
    let chain = nft_chain_json(Some(ns), "ip", SANDBOX_TABLE, PRE_TPROXY_GUARDRAILS_CHAIN)?;
    Ok(udp443_counter(chain.as_ref()))
}

pub(crate) fn guest_vlan_packets(ns: &str) -> FirewallResult<u64> {
    let chain = nft_chain_json(Some(ns), "netdev", SANDBOX_L2_TABLE, "ingress")?
        .ok_or(FirewallError::GuestL2IngressMissing)?;
    Ok(guest_vlan_counter(Some(&chain)))
}

fn guest_vlan_counter(chain: Option<&serde_json::Value>) -> u64 {
    chain
        .and_then(|chain| chain["nftables"].as_array())
        .into_iter()
        .flatten()
        .filter_map(|entry| entry.get("rule"))
        .filter(|rule| rule["comment"] == GUEST_VLAN_DROP_COMMENT)
        .filter_map(|rule| rule["expr"].as_array())
        .flatten()
        .filter_map(|expr| expr.pointer("/counter/packets")?.as_u64())
        .sum()
}

fn udp443_counter(chain: Option<&serde_json::Value>) -> Option<(u64, GuestUdp443Action)> {
    let entries = chain?["nftables"].as_array()?;
    let rule = entries
        .iter()
        .filter_map(|entry| entry.get("rule"))
        .find(|rule| rule["comment"] == UDP443_COMMENT)?;
    let exprs = rule["expr"].as_array()?;
    let packets = exprs
        .iter()
        .find_map(|expr| expr.pointer("/counter/packets")?.as_u64())?;
    let action = if exprs.iter().any(|expr| expr.get("reject").is_some()) {
        GuestUdp443Action::Reject
    } else {
        GuestUdp443Action::Observe
    };
    Some((packets, action))
}

fn nft_chain_json(
    ns: Option<&str>,
    family: &str,
    table: &str,
    chain: &str,
) -> FirewallResult<Option<serde_json::Value>> {
    let Some(output) = nft_chain_output_with_args(ns, family, table, chain, &["-j"])? else {
        return Ok(None);
    };
    serde_json::from_slice(&output.stdout)
        .map(Some)
        .map_err(FirewallError::ListJson)
}

fn nft_chain_output(
    ns: Option<&str>,
    family: &str,
    table: &str,
    chain: &str,
) -> FirewallResult<Option<Output>> {
    nft_chain_output_with_args(ns, family, table, chain, &[])
}

fn nft_chain_output_with_args(
    ns: Option<&str>,
    family: &str,
    table: &str,
    chain: &str,
    leading_args: &[&str],
) -> FirewallResult<Option<Output>> {
    let mut args = leading_args.to_vec();
    args.extend_from_slice(&["list", "chain", family, table, chain]);
    let output = nft_output(ns, &args).map_err(FirewallError::ListStart)?;
    if output.status.success() {
        return Ok(Some(output));
    }
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    if is_missing_chain_list_error(&stderr, family, table, chain) {
        return Ok(None);
    }
    Err(FirewallError::ListFailed { stderr })
}

fn nft_chain_has_rule_comment(chain: &serde_json::Value, expected_comment: &str) -> bool {
    chain
        .get("nftables")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|entry| entry.get("rule"))
        .any(|rule| {
            rule.get("comment")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|comment| comment == expected_comment)
        })
}

pub fn validate_namespace_firewall_config(
    ns: &str,
    vpeer: &str,
    guest_ip: &str,
) -> Result<(), FirewallValidationError> {
    let sandbox_forward =
        nft_chain_json(Some(ns), "ip", SANDBOX_TABLE, "forward")?.ok_or_else(|| {
            FirewallValidationError::Missing(
                "namespace sandbox forward chain is missing".to_string(),
            )
        })?;
    validate_forward_drop_chain(&sandbox_forward, vpeer)
        .map_err(FirewallValidationError::Missing)?;

    let pre_tproxy_guardrails =
        nft_chain_json(Some(ns), "ip", SANDBOX_TABLE, PRE_TPROXY_GUARDRAILS_CHAIN)?.ok_or_else(
            || {
                FirewallValidationError::Missing(
                    "namespace pre-TPROXY guardrails chain is missing".to_string(),
                )
            },
        )?;
    let guest_l2_ingress = nft_chain_json(Some(ns), "netdev", SANDBOX_L2_TABLE, "ingress")?
        .ok_or_else(|| {
            FirewallValidationError::Missing(
                "namespace guest L2 ingress chain is missing".to_string(),
            )
        })?;
    validate_guest_l2_ingress_chain(&guest_l2_ingress)?;
    validate_guest_l2_ingress_binding(ns)?;
    // A rollback must fence idle namespaces that still carry a newer verdict.
    let policy = load_pre_tproxy_guardrail_rules()?;
    let packaged: Vec<serde_json::Value> =
        serde_json::from_str(&format!("[{}]", policy)).map_err(PackagedPolicyError)?;
    validate_pre_tproxy_guardrails_chain(&pre_tproxy_guardrails, guest_ip, &packaged)
}

fn validate_guest_l2_ingress_chain(
    chain: &serde_json::Value,
) -> Result<(), FirewallValidationError> {
    let entries = chain
        .get("nftables")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| {
            FirewallValidationError::Missing("guest L2 ingress output is malformed".to_string())
        })?;
    let chain = entries
        .iter()
        .filter_map(|entry| entry.get("chain"))
        .find(|chain| {
            chain.get("family") == Some(&serde_json::json!("netdev"))
                && chain.get("table") == Some(&serde_json::json!(SANDBOX_L2_TABLE))
                && chain.get("name") == Some(&serde_json::json!("ingress"))
        })
        .ok_or_else(|| {
            FirewallValidationError::Missing(
                "guest L2 ingress chain metadata is missing".to_string(),
            )
        })?;
    if chain.get("type") != Some(&serde_json::json!("filter"))
        || chain.get("hook") != Some(&serde_json::json!("ingress"))
        || chain.get("prio") != Some(&serde_json::json!(-500))
    {
        return Err(FirewallValidationError::Missing(
            "guest L2 ingress chain must be a filter/ingress chain at priority -500".to_string(),
        ));
    }

    let rules: Vec<_> = entries
        .iter()
        .filter_map(|entry| entry.get("rule"))
        .collect();
    let [metadata_rule, raw_frame_rule, stale_gateway_mac_rule] = rules.as_slice() else {
        return Err(FirewallValidationError::Missing(
            "guest L2 ingress chain does not contain exactly its three owned rules".to_string(),
        ));
    };
    if !guest_l2_drop_rule_matches(
        metadata_rule,
        serde_json::json!({ "meta": { "key": "protocol" } }),
    ) || !guest_l2_drop_rule_matches(
        raw_frame_rule,
        serde_json::json!({ "payload": { "protocol": "ether", "field": "type" } }),
    ) {
        return Err(FirewallValidationError::Missing(
            "guest L2 ingress chain does not contain the two owned VLAN drop rules".to_string(),
        ));
    }
    if !guest_l2_stale_gateway_mac_rule_matches(stale_gateway_mac_rule) {
        return Err(FirewallValidationError::Missing(
            "guest L2 ingress chain does not end with the owned stale gateway MAC rule".to_string(),
        ));
    }
    Ok(())
}

fn guest_l2_stale_gateway_mac_rule_matches(rule: &serde_json::Value) -> bool {
    if rule.get("comment") != Some(&serde_json::json!(STALE_GATEWAY_MAC_COMMENT)) {
        return false;
    }
    let Some(exprs) = rule.get("expr").and_then(serde_json::Value::as_array) else {
        return false;
    };
    let [protocol_expr, pkttype_expr, counter_expr, mangle_expr] = exprs.as_slice() else {
        return false;
    };
    let expected_protocols = normalize_set_membership_op(&serde_json::json!({
        "match": {
            "left": { "meta": { "key": "protocol" } },
            "right": { "set": [2048, 2054] },
            "op": "in"
        }
    }));
    let pkttype = serde_json::json!({ "meta": { "key": "pkttype" } });
    normalize_set_membership_op(protocol_expr) == expected_protocols
        && pkttype_expr.pointer("/match/op") == Some(&serde_json::json!("=="))
        && pkttype_expr.pointer("/match/left") == Some(&pkttype)
        && matches!(pkttype_expr.pointer("/match/right"), Some(right) if right == "other" || right == 3)
        && counter_expr.get("counter").is_some()
        && mangle_expr.pointer("/mangle/key") == Some(&pkttype)
        && matches!(mangle_expr.pointer("/mangle/value"), Some(value) if value == "host" || value == 0)
}

fn guest_l2_drop_rule_matches(rule: &serde_json::Value, left: serde_json::Value) -> bool {
    if rule.get("comment") != Some(&serde_json::json!(GUEST_VLAN_DROP_COMMENT)) {
        return false;
    }
    let Some(exprs) = rule.get("expr").and_then(serde_json::Value::as_array) else {
        return false;
    };
    let [match_expr, counter_expr, drop_expr] = exprs.as_slice() else {
        return false;
    };
    let expected_match = normalize_set_membership_op(&serde_json::json!({
        "match": {
            "left": left,
            "right": { "set": [33024, 34984] },
            "op": "in"
        }
    }));
    normalize_set_membership_op(match_expr) == expected_match
        && counter_expr.get("counter").is_some()
        && drop_expr.get("drop").is_some()
}

fn validate_guest_l2_ingress_binding(ns: &str) -> Result<(), FirewallValidationError> {
    let output =
        nft_chain_output_with_args(Some(ns), "netdev", SANDBOX_L2_TABLE, "ingress", &["-n"])?
            .ok_or_else(|| {
                FirewallValidationError::Missing(
                    "guest L2 ingress chain is missing while checking its device".to_string(),
                )
            })?;
    validate_guest_l2_ingress_native_binding(&String::from_utf8_lossy(&output.stdout))
}

fn validate_guest_l2_ingress_native_binding(chain: &str) -> Result<(), FirewallValidationError> {
    let bound_to_tap = chain.lines().any(|line| {
        let Some((header, _)) = line.split_once(';') else {
            return false;
        };
        let fields: Vec<_> = header.split_whitespace().collect();
        let [
            "type",
            "filter",
            "hook",
            "ingress",
            "device",
            device,
            "priority",
            "-500",
        ] = fields.as_slice()
        else {
            return false;
        };
        device
            .strip_prefix('"')
            .and_then(|device| device.strip_suffix('"'))
            .unwrap_or(device)
            == crate::net::TAP_DEVICE
    });
    if !bound_to_tap {
        return Err(FirewallValidationError::Missing(
            "guest L2 ingress chain is not bound to tap0 at priority -500".to_string(),
        ));
    }
    Ok(())
}

fn validate_forward_drop_chain(
    sandbox_forward: &serde_json::Value,
    vpeer: &str,
) -> Result<(), String> {
    if !nft_chain_has_rule_exprs(
        sandbox_forward,
        &[
            serde_json::json!({
                "match": {
                    "left": { "meta": { "key": "iifname" } },
                    "right": "tap0",
                    "op": "=="
                }
            }),
            serde_json::json!({ "drop": null }),
        ],
    ) {
        return Err("namespace sandbox forward chain is missing iifname tap0 drop".into());
    }
    if !nft_chain_has_rule_exprs(
        sandbox_forward,
        &[
            serde_json::json!({
                "match": {
                    "left": { "meta": { "key": "iifname" } },
                    "right": vpeer,
                    "op": "=="
                }
            }),
            serde_json::json!({
                "match": {
                    "left": { "meta": { "key": "oifname" } },
                    "right": "tap0",
                    "op": "=="
                }
            }),
            serde_json::json!({ "drop": null }),
        ],
    ) {
        return Err(format!(
            "namespace sandbox forward chain is missing iifname {vpeer} oifname tap0 drop"
        ));
    }
    Ok(())
}

fn validate_pre_tproxy_guardrails_chain(
    pre_tproxy_guardrails: &serde_json::Value,
    guest_ip: &str,
    packaged: &[serde_json::Value],
) -> Result<(), FirewallValidationError> {
    let source_guard = [
        serde_json::json!({
            "match": {
                "left": { "meta": { "key": "iifname" } },
                "right": "tap0",
                "op": "=="
            }
        }),
        serde_json::json!({
            "match": {
                "left": { "payload": { "protocol": "ip", "field": "saddr" } },
                "right": guest_ip,
                "op": "!="
            }
        }),
        serde_json::json!({ "drop": null }),
    ];
    // The packaged pre-TPROXY return only exits the early guardrail chain, whose
    // drops apply to new connections. The later proxy and forward exceptions
    // independently require reply direction, so established guest requests still
    // reach the gateway. Keep this validator compatible with the packaged rule.
    let established_bypass = [
        serde_json::json!({
            "match": {
                "left": { "meta": { "key": "iifname" } },
                "right": "tap0",
                "op": "=="
            }
        }),
        serde_json::json!({
            "match": {
                "left": { "ct": { "key": "state" } },
                "right": { "set": ["established", "related"] },
                "op": "in"
            }
        }),
        serde_json::json!({ "return": null }),
    ];
    let source_guard_position = nft_chain_rule_position(pre_tproxy_guardrails, &source_guard)
        .ok_or_else(|| {
            FirewallValidationError::Missing(
                "namespace pre-TPROXY guardrails chain is missing tap0 guest source guard"
                    .to_string(),
            )
        })?;
    let established_bypass_position =
        nft_chain_rule_position(pre_tproxy_guardrails, &established_bypass).ok_or_else(|| {
            FirewallValidationError::Missing(
                "namespace pre-TPROXY guardrails chain is missing established/related return"
                    .to_string(),
            )
        })?;
    if source_guard_position >= established_bypass_position {
        return Err(FirewallValidationError::Missing(
            "namespace tap0 guest source guard must precede established/related return".to_string(),
        ));
    }
    let expected = packaged
        .iter()
        .filter_map(|entry| entry.pointer("/add/rule"))
        .find(|rule| rule["comment"] == UDP443_COMMENT);
    let mut actual = pre_tproxy_guardrails
        .get("nftables")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|entry| entry.get("rule"))
        .enumerate()
        .filter(|(_, rule)| rule["comment"] == UDP443_COMMENT);
    let actual = actual.next().filter(|_| actual.next().is_none());
    if actual.is_some_and(|(position, _)| position <= source_guard_position) {
        return Err(FirewallValidationError::Missing(
            "namespace tap0 guest source guard must precede UDP/443 guardrail".into(),
        ));
    }
    let expressions = |rule: &serde_json::Value| {
        rule["expr"].as_array().map(|expr| {
            expr.iter()
                .map(|expr| {
                    if expr.get("counter").is_some() {
                        serde_json::json!({"counter": null})
                    } else {
                        normalize_set_membership_op(expr)
                    }
                })
                .collect::<Vec<_>>()
        })
    };
    let expected = expected.and_then(expressions);
    if expected.is_none() || expected != actual.and_then(|(_, rule)| expressions(rule)) {
        return Err(FirewallValidationError::Missing(
            "namespace UDP/443 guardrail differs from packaged policy".into(),
        ));
    }
    Ok(())
}

/// nft canonicalizes set operators and some protocol constants when listing a
/// rule. Normalize both sides before comparing rather than pinning an input or
/// output spelling.
fn normalize_set_membership_op(expr: &serde_json::Value) -> serde_json::Value {
    let mut expr = expr.clone();
    if let Some(matcher) = expr.get_mut("match")
        && matcher
            .pointer("/left/ct/key")
            .and_then(serde_json::Value::as_str)
            == Some("state")
        && matcher["right"].is_string()
    {
        matcher["right"] = serde_json::json!({"set": [matcher["right"]]});
    }
    if let Some(matcher) = expr.get_mut("match")
        && matcher
            .get("right")
            .is_some_and(|right| right.get("set").is_some())
        && matcher.get("op") == Some(&serde_json::json!("in"))
    {
        matcher["op"] = serde_json::json!("==");
    }
    if let Some(matcher) = expr.get_mut("match")
        && (matcher
            .pointer("/left/meta/key")
            .and_then(serde_json::Value::as_str)
            == Some("protocol")
            || (matcher
                .pointer("/left/payload/protocol")
                .and_then(serde_json::Value::as_str)
                == Some("ether")
                && matcher
                    .pointer("/left/payload/field")
                    .and_then(serde_json::Value::as_str)
                    == Some("type")))
        && let Some(values) = matcher
            .pointer_mut("/right/set")
            .and_then(serde_json::Value::as_array_mut)
    {
        for value in values.iter_mut() {
            if value == "8021q" {
                *value = serde_json::json!(33024);
            } else if value == "8021ad" {
                *value = serde_json::json!(34984);
            } else if value == "ip" {
                *value = serde_json::json!(2048);
            } else if value == "arp" {
                *value = serde_json::json!(2054);
            }
        }
        values.sort_unstable_by_key(serde_json::Value::as_u64);
    }
    expr
}

fn nft_chain_rule_position(
    chain: &serde_json::Value,
    expected: &[serde_json::Value],
) -> Option<usize> {
    let expected: Vec<serde_json::Value> =
        expected.iter().map(normalize_set_membership_op).collect();
    chain
        .get("nftables")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|entry| entry.get("rule"))
        .filter_map(|rule| rule.get("expr").and_then(serde_json::Value::as_array))
        .position(|expr| {
            expected.iter().all(|needle| {
                expr.iter()
                    .any(|entry| &normalize_set_membership_op(entry) == needle)
            })
        })
}

fn nft_chain_has_rule_exprs(chain: &serde_json::Value, expected: &[serde_json::Value]) -> bool {
    nft_chain_rule_position(chain, expected).is_some()
}

/// Remove the forwarding exception used by direct DNAT ingress. Handle
/// deletion leaves unrelated ingress allowlists in place.
fn inbound_reply_rule_removals(
    existing: Option<&serde_json::Value>,
) -> FirewallResult<Vec<serde_json::Value>> {
    owned_rule_removals(existing, SANDBOX_TABLE, "forward", INBOUND_REPLY_COMMENT)
}

fn local_service_reply_rule() -> serde_json::Value {
    serde_json::json!({
        "family": "ip", "table": PROXY_TABLE, "chain": "prerouting",
        "comment": LOCAL_SERVICE_REPLY_COMMENT,
        "expr": [
            { "match": { "left": { "meta": { "key": "iifname" } }, "right": "tap0", "op": "==" } },
            { "match": { "left": { "meta": { "key": "l4proto" } }, "right": "tcp", "op": "==" } },
            { "match": { "left": { "ct": { "key": "state" } }, "right": "established", "op": "==" } },
            { "match": { "left": { "ct": { "key": "direction" } }, "right": "reply", "op": "==" } },
            { "match": { "left": { "payload": { "protocol": "ip", "field": "daddr" } }, "right": crate::net::GUEST_GATEWAY, "op": "==" } },
            { "return": null }
        ]
    })
}

/// Replace any broad TPROXY reply exception with the local-only rule required
/// by the authenticated service-port ingress stream. An absent chain belongs
/// to a namespace with no active gateway and needs no exception yet.
fn local_service_reply_rule_updates(
    existing: Option<&serde_json::Value>,
) -> FirewallResult<Vec<serde_json::Value>> {
    let Some(existing) = existing else {
        return Ok(Vec::new());
    };
    let desired = local_service_reply_rule();
    let desired_expr: Vec<_> = desired
        .get("expr")
        .and_then(serde_json::Value::as_array)
        .expect("literal rule expression array")
        .iter()
        .map(normalize_set_membership_op)
        .collect();
    let mut installed = false;
    let mut updates = Vec::new();
    for rule in existing
        .get("nftables")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|entry| entry.get("rule"))
        .filter(|rule| {
            rule.get("comment")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|comment| {
                    comment == TPROXY_INBOUND_REPLY_COMMENT
                        || comment == LOCAL_SERVICE_REPLY_COMMENT
                })
        })
    {
        let comment = if rule.get("comment").and_then(serde_json::Value::as_str)
            == Some(TPROXY_INBOUND_REPLY_COMMENT)
        {
            TPROXY_INBOUND_REPLY_COMMENT
        } else {
            LOCAL_SERVICE_REPLY_COMMENT
        };
        let handle = rule
            .get("handle")
            .and_then(serde_json::Value::as_u64)
            .ok_or(FirewallError::MissingRuleHandle { comment })?;
        let normalized: Vec<_> = rule
            .get("expr")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .map(normalize_set_membership_op)
            .collect();
        if !installed && comment == LOCAL_SERVICE_REPLY_COMMENT && normalized == desired_expr {
            installed = true;
            continue;
        }
        if !installed {
            let mut replacement = desired.clone();
            replacement
                .as_object_mut()
                .expect("literal rule object")
                .insert("handle".into(), handle.into());
            updates.push(serde_json::json!({ "replace": { "rule": replacement } }));
            installed = true;
        } else {
            updates.push(serde_json::json!({ "delete": { "rule": {
                "family": "ip", "table": PROXY_TABLE, "chain": "prerouting", "handle": handle
            } } }));
        }
    }
    if !installed {
        updates.push(serde_json::json!({ "insert": { "rule": desired } }));
    }
    Ok(updates)
}

/// Remove direct forwarding replies while retaining only the local service
/// ingress return path. All changes use one namespace-local nft transaction.
pub fn constrain_direct_inbound_reply_path(ns: &str) -> FirewallResult<()> {
    let forward = nft_chain_json(Some(ns), "ip", SANDBOX_TABLE, "forward")?;
    let proxy = nft_chain_json(Some(ns), "ip", PROXY_TABLE, "prerouting")?;
    let mut removals = inbound_reply_rule_removals(forward.as_ref())?;
    removals.extend(local_service_reply_rule_updates(proxy.as_ref())?);
    if removals.is_empty() {
        return Ok(());
    }
    let nftables = removals
        .into_iter()
        .map(|removal| removal.to_string())
        .collect::<Vec<_>>()
        .join(",\n");
    apply_nft_json(
        &format!(
            r#"{{ "nftables": [
{nftables}
] }}"#
        ),
        Some(ns),
    )
}

fn owned_rule_removals(
    existing: Option<&serde_json::Value>,
    table: &'static str,
    chain: &'static str,
    comment: &'static str,
) -> FirewallResult<Vec<serde_json::Value>> {
    let mut removals = Vec::new();
    for rule in existing
        .and_then(|chain| chain.get("nftables"))
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|entry| entry.get("rule"))
        .filter(|rule| rule.get("comment").and_then(serde_json::Value::as_str) == Some(comment))
    {
        let handle = rule
            .get("handle")
            .and_then(serde_json::Value::as_u64)
            .ok_or(FirewallError::MissingRuleHandle { comment })?;
        removals.push(serde_json::json!({ "delete": { "rule": {
            "family": "ip", "table": table, "chain": chain, "handle": handle
        } } }));
    }
    Ok(removals)
}

fn ensure_namespace_forward_drop_rules(ns: &str, vpeer: &str) -> FirewallResult<()> {
    let existing = nft_chain_json(Some(ns), "ip", SANDBOX_TABLE, "forward")?;
    let chain_exists = existing.is_some();
    let has_egress_drop = existing
        .as_ref()
        .is_some_and(|chain| nft_chain_has_rule_comment(chain, "fail-closed-guest-egress"));
    let has_ingress_drop = existing
        .as_ref()
        .is_some_and(|chain| nft_chain_has_rule_comment(chain, "fail-closed-guest-ingress"));
    let inbound_reply_rules = inbound_reply_rule_removals(existing.as_ref())?;
    if chain_exists && has_egress_drop && has_ingress_drop && inbound_reply_rules.is_empty() {
        return Ok(());
    }

    let add_chain = if chain_exists {
        String::new()
    } else {
        format!(
            r#"  {{ "add": {{ "chain": {{ "family": "ip", "table": "{SANDBOX_TABLE}", "name": "forward",
    "type": "filter", "hook": "forward", "prio": 0 }} }} }}
"#
        )
    };
    let egress_drop_rule = if has_egress_drop {
        String::new()
    } else {
        format!(
            r#"  {{ "add": {{ "rule": {{ "family": "ip", "table": "{SANDBOX_TABLE}", "chain": "forward",
    "comment": "fail-closed-guest-egress",
    "expr": [
      {{ "match": {{ "left": {{ "meta": {{ "key": "iifname" }} }},
                     "right": "tap0", "op": "==" }} }},
      {{ "drop": null }}
    ] }} }} }}"#
        )
    };
    let ingress_drop_rule = if has_ingress_drop {
        String::new()
    } else {
        format!(
            r#"  {{ "add": {{ "rule": {{ "family": "ip", "table": "{SANDBOX_TABLE}", "chain": "forward",
    "comment": "fail-closed-guest-ingress",
    "expr": [
      {{ "match": {{ "left": {{ "meta": {{ "key": "iifname" }} }},
                     "right": "{vpeer}", "op": "==" }} }},
      {{ "match": {{ "left": {{ "meta": {{ "key": "oifname" }} }},
                     "right": "tap0", "op": "==" }} }},
      {{ "drop": null }}
    ] }} }} }}"#
        )
    };
    let rules = inbound_reply_rules
        .into_iter()
        .map(|rule| rule.to_string())
        .chain([egress_drop_rule, ingress_drop_rule])
        .filter(|rule| !rule.is_empty())
        .collect::<Vec<_>>();
    if rules.is_empty() {
        return Ok(());
    }
    let mut nftables = Vec::new();
    if !add_chain.is_empty() {
        nftables.push(add_chain.trim_end().to_string());
    }
    nftables.extend(rules);
    let json = format!(
        r#"{{ "nftables": [
{}
] }}"#,
        nftables.join(",\n")
    );
    apply_nft_json(&json, Some(ns))
}

/// Check if a namespace has net-gateway TPROXY rules.
pub fn ns_has_tproxy(ns: &str) -> bool {
    nft_output(
        Some(ns),
        &["list", "chain", "ip", PROXY_TABLE, "prerouting"],
    )
    .map(|o| o.status.success() && String::from_utf8_lossy(&o.stdout).contains("tproxy"))
    .unwrap_or(false)
}

/// Check if a namespace has the forward DROP rule for tap0.
pub fn ns_has_forward_drop(ns: &str) -> bool {
    nft_output(Some(ns), &["list", "chain", "ip", SANDBOX_TABLE, "forward"])
        .map(|o| {
            let out = String::from_utf8_lossy(&o.stdout);
            o.status.success() && out.contains("tap0") && out.contains("drop")
        })
        .unwrap_or(false)
}

// ---------------------------------------------------------------------------
// Ingress DNAT rules — per-port DNAT in pod/root namespace for inbound traffic.
// Uses a separate "ip" family table so the "inet" space table stays IPv6-ready.
// ---------------------------------------------------------------------------

/// Ensure the "ingress" nftables table + prerouting chain exist in a namespace.
/// Idempotent — `nft add` is a no-op if already present.
pub fn ensure_ingress_table(ns: Option<&str>) -> FirewallResult<()> {
    let json = format!(
        r#"{{ "nftables": [
  {{ "add": {{ "table": {{ "family": "ip", "name": "{INGRESS_TABLE}" }} }} }},
  {{ "add": {{ "chain": {{ "family": "ip", "table": "{INGRESS_TABLE}", "name": "prerouting",
    "type": "nat", "hook": "prerouting", "prio": -100 }} }} }}
] }}"#
    );
    apply_nft_json(&json, ns)
}

/// Add a per-port DNAT rule in the selected namespace's "ingress" table.
/// `host_port` is matched on incoming traffic; `guest_port` is the DNAT target.
/// Tags the rule with a comment for later identification and removal.
pub fn add_ingress_dnat_rule(
    ns: Option<&str>,
    host_port: u16,
    guest_port: u16,
    protocol: &str,
    vpeer_ip: &str,
    ingress_iface: &str,
) -> FirewallResult<()> {
    ensure_ingress_table(ns)?;
    let json = format!(
        r#"{{ "nftables": [
  {{ "add": {{ "rule": {{ "family": "ip", "table": "{INGRESS_TABLE}", "chain": "prerouting",
    "comment": "{INGRESS_DNAT_COMMENT_PREFIX}{vpeer_ip}-{protocol}-{host_port}",
    "expr": [
      {{ "match": {{ "left": {{ "meta": {{ "key": "iifname" }} }},
                     "right": "{ingress_iface}", "op": "==" }} }},
      {{ "match": {{ "left": {{ "payload": {{ "protocol": "{protocol}", "field": "dport" }} }},
                     "right": {host_port}, "op": "==" }} }},
      {{ "dnat": {{ "addr": "{vpeer_ip}", "port": {guest_port} }} }}
    ] }} }} }}
] }}"#
    );
    apply_nft_json(&json, ns)
}

/// Remove ingress DNAT rules from the selected namespace.
pub fn remove_ingress_dnat_rules(
    ns: Option<&str>,
    port: PortSelector,
    protocol: &str,
    vpeer_ip: &str,
) -> FirewallResult<()> {
    let output = match list_chain_with_handles(ns, "ip", INGRESS_TABLE, "prerouting") {
        Ok(output) => output,
        Err(FirewallError::ListFailed { stderr }) => {
            if is_missing_ingress_chain_list_error(&stderr) {
                return Ok(());
            }
            return Err(FirewallError::ListFailed { stderr });
        }
        Err(error) => return Err(error),
    };
    let handles = extract_ingress_dnat_rule_handles_for_selector(&output, vpeer_ip, protocol, port);
    delete_rules_by_handles(ns, "ip", INGRESS_TABLE, "prerouting", &handles)
}

/// Remove SPACE-owned service-port DNAT rules from the root namespace.
///
/// Kubernetes service-port ingress must live in the sandbox pod namespace. A
/// root-namespace ingress rule would match every pod's node-ingress traffic on
/// the same destination port, so startup and admin repair sweep any historical
/// rules that were installed before the namespace invariant was enforced.
pub fn remove_root_ingress_dnat_rules() -> FirewallResult<usize> {
    let output = match list_chain_with_handles(None, "ip", INGRESS_TABLE, "prerouting") {
        Ok(output) => output,
        Err(FirewallError::ListFailed { stderr }) => {
            if is_missing_ingress_chain_list_error(&stderr) {
                return Ok(0);
            }
            return Err(FirewallError::ListFailed { stderr });
        }
        Err(error) => return Err(error),
    };
    let handles = extract_space_ingress_dnat_rule_handles(&output);
    let removed = handles.len();
    delete_rules_by_handles(None, "ip", INGRESS_TABLE, "prerouting", &handles)?;
    Ok(removed)
}

/// Check if any ingress DNAT rules exist for a sandbox's vpeer_ip.
pub fn has_ingress_dnat(ns: Option<&str>, vpeer_ip: &str) -> bool {
    let Ok(vpeer_ip) = vpeer_ip.parse::<Ipv4Addr>() else {
        return false;
    };
    list_chain_with_handles(ns, "ip", INGRESS_TABLE, "prerouting")
        .map(|output| {
            output.lines().any(|line| {
                parse_ingress_dnat_comment_from_line(line)
                    .is_some_and(|comment| comment.vpeer_ip == vpeer_ip)
            })
        })
        .unwrap_or(false)
}

/// Look up the current DNAT target for `(protocol, host_port)` in the namespace's
/// ingress table. Returns `Some((target_ip, target_port))` when a rule exists,
/// `None` otherwise. Used to make `ExposePort` idempotent: callers compare the
/// existing target against the request and skip the install if they match.
pub fn find_ingress_dnat_target(
    ns: Option<&str>,
    protocol: &str,
    host_port: u16,
) -> Option<(String, u16)> {
    let output = list_chain_with_handles(ns, "ip", INGRESS_TABLE, "prerouting").ok()?;
    parse_ingress_dnat_target(&output, protocol, host_port)
}

// ---------------------------------------------------------------------------
// Guest port allowlist — per-port ACCEPT rules in sandbox namespace "inbound" chain.
// The "inbound" chain (priority -1) runs before the "forward" chain (priority 0)
// which has a default DROP for iifname vpeer oifname tap0.
// ---------------------------------------------------------------------------

/// Allow inbound traffic to a guest port. Inserts an ACCEPT rule at the
/// beginning of the sandbox namespace's "forward" chain (before the DROP rules).
pub fn allow_guest_port(ns: &str, vpeer: &str, port: u16, protocol: &str) -> FirewallResult<()> {
    let json = format!(
        r#"{{ "nftables": [
  {{ "insert": {{ "rule": {{ "family": "ip", "table": "{SANDBOX_TABLE}", "chain": "forward",
    "comment": "allow-{protocol}-{port}",
    "expr": [
      {{ "match": {{ "left": {{ "meta": {{ "key": "iifname" }} }},
                     "right": "{vpeer}", "op": "==" }} }},
      {{ "match": {{ "left": {{ "payload": {{ "protocol": "{protocol}", "field": "dport" }} }},
                     "right": {port}, "op": "==" }} }},
      {{ "accept": null }}
    ] }} }} }}
] }}"#
    );
    apply_nft_json(&json, Some(ns))
}

/// Remove guest port rules from the allowlist.
pub fn deny_guest_port(ns: &str, port: PortSelector, protocol: &str) -> FirewallResult<()> {
    let output = list_chain_with_handles(Some(ns), "ip", SANDBOX_TABLE, "forward")?;

    let handles = match port {
        PortSelector::All => {
            let mut handles = Vec::new();
            for prefix in ["allow-tcp-", "allow-udp-"] {
                handles.extend(extract_rule_handles(&output, prefix));
            }
            handles
        }
        PortSelector::Port(port) => {
            let needle = quoted_comment_needle(&format!("allow-{protocol}-{port}"));
            extract_rule_handles(&output, &needle)
        }
    };
    delete_rules_by_handles(Some(ns), "ip", SANDBOX_TABLE, "forward", &handles)
}

/// Check if a guest port is in the allowlist.
pub fn has_guest_port_allowed(ns: &str, port: u16, protocol: &str) -> bool {
    list_chain_with_handles(Some(ns), "ip", SANDBOX_TABLE, "forward")
        .map(|output| output.contains(&quoted_comment_needle(&format!("allow-{protocol}-{port}"))))
        .unwrap_or(false)
}

/// List rules in a chain with handle numbers (`nft -a list chain`).
fn list_chain_with_handles(
    ns: Option<&str>,
    family: &str,
    table: &str,
    chain: &str,
) -> FirewallResult<String> {
    let output = nft_output(ns, &["-a", "list", "chain", family, table, chain]);
    let output = output.map_err(FirewallError::ListStart)?;
    if !output.status.success() {
        return Err(FirewallError::ListFailed {
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
        });
    }
    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

/// Extract rule handles from `nft -a` output for lines matching `needle`.
/// Lines look like: `  tcp dport 22 dnat to 10.12.0.0:22 # handle 4`
fn extract_rule_handles(output: &str, needle: &str) -> Vec<u64> {
    output
        .lines()
        .filter(|line| line.contains(needle))
        .filter_map(|line| {
            line.rsplit_once("# handle ")
                .and_then(|(_, h)| h.trim().parse::<u64>().ok())
        })
        .collect()
}

fn extract_space_ingress_dnat_rule_handles(output: &str) -> Vec<u64> {
    output
        .lines()
        .filter(|line| is_space_owned_ingress_dnat_rule(line))
        .filter_map(parse_rule_handle)
        .collect()
}

fn extract_ingress_dnat_rule_handles_for_selector(
    output: &str,
    vpeer_ip: &str,
    protocol: &str,
    port: PortSelector,
) -> Vec<u64> {
    let Ok(vpeer_ip) = vpeer_ip.parse::<Ipv4Addr>() else {
        return Vec::new();
    };
    output
        .lines()
        .filter(|line| {
            let Some(comment) = parse_ingress_dnat_comment_from_line(line) else {
                return false;
            };
            if comment.vpeer_ip != vpeer_ip {
                return false;
            }
            match port {
                PortSelector::All => true,
                PortSelector::Port(port) => {
                    comment.protocol == protocol && comment.host_port == port
                }
            }
        })
        .filter_map(parse_rule_handle)
        .collect()
}

#[derive(Debug, Eq, PartialEq)]
struct IngressDnatComment<'a> {
    vpeer_ip: Ipv4Addr,
    protocol: &'a str,
    host_port: u16,
}

fn is_space_owned_ingress_dnat_rule(line: &str) -> bool {
    let Some(comment) = parse_ingress_dnat_comment_from_line(line) else {
        return false;
    };
    if !crate::net::is_slot_vpeer_ip(comment.vpeer_ip) {
        return false;
    }
    let Some((target_ip, _target_port)) = parse_dnat_target_from_line(line) else {
        return false;
    };
    target_ip == comment.vpeer_ip
        && line.contains(&format!("{} dport {}", comment.protocol, comment.host_port))
}

fn parse_ingress_dnat_comment_from_line(line: &str) -> Option<IngressDnatComment<'_>> {
    let comment = extract_nft_comment(line)?;
    parse_ingress_dnat_comment(comment)
}

fn parse_ingress_dnat_comment(comment: &str) -> Option<IngressDnatComment<'_>> {
    let body = comment
        .strip_prefix(INGRESS_DNAT_COMMENT_PREFIX)
        .or_else(|| comment.strip_prefix(LEGACY_INGRESS_DNAT_COMMENT_PREFIX))?;
    let (body, host_port) = body.rsplit_once('-')?;
    let (vpeer_ip, protocol) = body.rsplit_once('-')?;
    if protocol != "tcp" && protocol != "udp" {
        return None;
    }
    let host_port = host_port.parse::<u16>().ok().filter(|port| *port > 0)?;
    Some(IngressDnatComment {
        vpeer_ip: vpeer_ip.parse().ok()?,
        protocol,
        host_port,
    })
}

fn extract_nft_comment(line: &str) -> Option<&str> {
    let after_comment = line.split_once("comment \"")?.1;
    let (comment, _) = after_comment.split_once('"')?;
    Some(comment)
}

fn parse_dnat_target_from_line(line: &str) -> Option<(Ipv4Addr, u16)> {
    let after_dnat = line.split_once(" dnat to ")?.1;
    let target = after_dnat.split_whitespace().next()?;
    let (ip, port) = target.rsplit_once(':')?;
    Some((ip.parse().ok()?, port.parse().ok()?))
}

fn parse_rule_handle(line: &str) -> Option<u64> {
    line.rsplit_once("# handle ")
        .and_then(|(_, h)| h.trim().parse::<u64>().ok())
}

fn quoted_comment_needle(comment: &str) -> String {
    format!("comment \"{comment}\"")
}

fn parse_ingress_dnat_target(
    output: &str,
    protocol: &str,
    host_port: u16,
) -> Option<(String, u16)> {
    output.lines().find_map(|line| {
        parse_ingress_dnat_comment_from_line(line)
            .is_some_and(|comment| comment.protocol == protocol && comment.host_port == host_port)
            .then(|| {
                let (ip, port) = parse_dnat_target_from_line(line)?;
                Some((ip.to_string(), port))
            })
            .flatten()
    })
}

fn is_missing_ingress_chain_list_error(stderr: &str) -> bool {
    is_missing_chain_list_error(stderr, "ip", INGRESS_TABLE, "prerouting")
}

fn is_missing_chain_list_error(stderr: &str, family: &str, table: &str, chain: &str) -> bool {
    let missing_object = stderr.contains("No such file or directory")
        || stderr.contains("No such table")
        || stderr.contains("No such chain");
    let requested_chain = stderr.contains(&format!("list chain {family} {table} {chain}"))
        || (stderr.contains(table) && stderr.contains(chain));
    missing_object && requested_chain
}

/// Delete multiple rules by handle number in one nft transaction.
fn delete_rules_by_handles(
    ns: Option<&str>,
    family: &str,
    table: &str,
    chain: &str,
    handles: &[u64],
) -> FirewallResult<()> {
    if handles.is_empty() {
        return Ok(());
    }

    let deletes = handles
        .iter()
        .map(|handle| {
            format!(
                r#"{{ "delete": {{ "rule": {{ "family": "{family}", "table": "{table}", "chain": "{chain}", "handle": {handle} }} }} }}"#
            )
        })
        .collect::<Vec<_>>()
        .join(",\n");
    let json = format!(r#"{{ "nftables": [{deletes}] }}"#);
    apply_nft_json(&json, ns)
}

fn nft_output(ns: Option<&str>, args: &[&str]) -> std::io::Result<std::process::Output> {
    if let Some(ns) = ns {
        crate::net::command_output_in_ns(ns, "nft", args)
    } else {
        crate::host_command::output(
            Command::new("nft").args(args),
            crate::host_command::PROBE_BUDGET,
            &[],
        )
    }
}

fn nft_status(ns: Option<&str>, args: &[&str]) -> std::io::Result<std::process::ExitStatus> {
    if let Some(ns) = ns {
        crate::net::command_status_in_ns(ns, "nft", args)
    } else {
        crate::host_command::output(
            Command::new("nft").args(args),
            crate::host_command::MUTATION_BUDGET,
            &[],
        )
        .map(|output| output.status)
    }
}

fn nft_output_with_stdin(
    ns: Option<&str>,
    args: &[&str],
    stdin: &[u8],
) -> std::io::Result<std::process::Output> {
    if let Some(ns) = ns {
        crate::net::command_output_with_stdin_in_ns(ns, "nft", args, stdin)
    } else {
        crate::host_command::output(
            Command::new("nft").args(args),
            crate::host_command::MUTATION_BUDGET,
            stdin,
        )
    }
}

/// Apply nftables JSON ruleset via `nft -j -f -`.
/// If `ns` is Some, run inside that network namespace.
fn apply_nft_script(script: &str, ns: Option<&str>) -> FirewallResult<()> {
    let result = nft_output_with_stdin(ns, &["-f", "-"], script.as_bytes());
    match result {
        Ok(o) if !o.status.success() => Err(FirewallError::Apply {
            stderr: String::from_utf8_lossy(&o.stderr).trim().to_string(),
            input: script.to_string(),
        }),
        Err(e) => Err(FirewallError::Exec(e)),
        _ => Ok(()),
    }
}

fn apply_nft_json(json: &str, ns: Option<&str>) -> FirewallResult<()> {
    let result = nft_output_with_stdin(ns, &["-j", "-f", "-"], json.as_bytes());

    match result {
        Ok(o) if !o.status.success() => {
            let stderr = String::from_utf8_lossy(&o.stderr);
            Err(FirewallError::Apply {
                stderr: stderr.trim().to_string(),
                input: json.to_string(),
            })
        }
        Err(e) => Err(FirewallError::Exec(e)),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests;
