//! Shared network policy types.
//!
//! This crate is intentionally dependency-light. Service crates own storage,
//! transport, and async integration; this crate owns policy semantics that must
//! stay identical across API, sbx-manager, node-manager, and net-gateway.

#[cfg(feature = "serde")]
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

use std::collections::BTreeMap;
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::str::FromStr;
use std::sync::OnceLock;

#[cfg(feature = "host-sockets")]
pub mod orig_dst;
const DEFAULT_NETWORK_CREDENTIAL_PORT: u16 = 443;
const MAX_NETWORK_CREDENTIAL_PATH_PREFIX_BYTES: usize = 2048;

pub mod tls_interception;
pub use tls_interception::{TlsInterception, TlsInterceptionParseError};

/// Host/sidecar transparent-proxy contract.
pub mod proxy {
    /// TCP transparent proxy port.
    pub const TCP_PROXY_PORT: u16 = 15001;
    /// UDP transparent proxy port.
    pub const UDP_PROXY_PORT: u16 = 15002;
    /// Policy-routing mark for intercepted packets.
    pub const TPROXY_MARK: u32 = 1;
    /// Mark on proxy-originated sockets to bypass interception.
    pub const OUTBOUND_MARK: u32 = 2;
    /// Routing table for intercepted packets.
    pub const TPROXY_RT_TABLE: u32 = 100;
}

pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/space.net.policy.v1.rs"));
}

#[derive(Debug, thiserror::Error)]
pub enum NetworkPolicyProtoError {
    #[error("network_policy.tls_interception must be unspecified, off, or on")]
    InvalidTlsInterception,
    #[error("network_policy.version must be v1")]
    UnspecifiedVersion,
    #[error("network_policy.egress_default_action must be allow or deny")]
    UnspecifiedEgressDefaultAction,
    #[error("network_policy.{field}[{index}] is missing a destination")]
    MissingDestination { field: &'static str, index: usize },
    #[error("network_policy.{field}[{index}] is invalid: {message}")]
    InvalidDestination {
        field: &'static str,
        index: usize,
        message: String,
    },
    #[error("network_policy.network_credential_rules[{index}] is invalid: {message}")]
    InvalidNetworkCredentialRule { index: usize, message: String },
}

#[derive(Debug, thiserror::Error)]
pub enum NetworkPolicyValidationError {
    #[error("cidr '{0}' must be a valid IPv4 or IPv6 CIDR")]
    InvalidCidr(String),
    #[error("cidr '{cidr}' has invalid prefix length {prefix_len}")]
    InvalidPrefixLength { cidr: String, prefix_len: u8 },
    #[error("domain '{0}' must be a valid DNS name")]
    InvalidDomain(String),
    #[error("wildcard domain '{0}' must be in the form *.example.com")]
    InvalidWildcardDomain(String),
    #[error("network-credential rules require tls_interception=on")]
    NetworkCredentialsRequireTlsInterception,
    #[error("network-credential destination port is invalid")]
    InvalidNetworkCredentialPort,
    #[error("network-credential path_prefix is invalid")]
    InvalidNetworkCredentialPathPrefix,
    #[error("credential_reference_id is invalid")]
    InvalidCredentialReferenceId,
    #[error("network-credential rule must contain at least one header binding")]
    InvalidNetworkCredentialHeaderCount,
    #[error("network-credential header name is invalid")]
    InvalidNetworkCredentialHeaderName,
    #[error("network-credential header name '{0}' is reserved")]
    ReservedNetworkCredentialHeaderName(String),
    #[error("network-credential supported_http_methods must contain GET, POST or HEAD")]
    InvalidNetworkCredentialHttpMethods,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PlatformEgressHardDenyCidr {
    pub id: &'static str,
    pub cidr: &'static str,
    pub description: &'static str,
}

const EC2_IMDS: PlatformEgressHardDenyCidr = PlatformEgressHardDenyCidr {
    id: "ec2-imds",
    cidr: "169.254.169.254/32",
    description: "EC2 instance metadata service",
};
const LINK_LOCAL: PlatformEgressHardDenyCidr = PlatformEgressHardDenyCidr {
    id: "link-local",
    cidr: "169.254.0.0/16",
    description: "link-local infrastructure range",
};
const SPACE_SLOTS_A: PlatformEgressHardDenyCidr = PlatformEgressHardDenyCidr {
    id: "space-slots-a",
    cidr: "10.11.0.0/16",
    description: "SPACE sandbox slot range",
};
const SPACE_SLOTS_B: PlatformEgressHardDenyCidr = PlatformEgressHardDenyCidr {
    id: "space-slots-b",
    cidr: "10.12.0.0/16",
    description: "SPACE sandbox slot range",
};
const LOOPBACK: PlatformEgressHardDenyCidr = PlatformEgressHardDenyCidr {
    id: "loopback",
    cidr: "127.0.0.0/8",
    description: "guest loopback range",
};
const IPV6_LOOPBACK: PlatformEgressHardDenyCidr = PlatformEgressHardDenyCidr {
    id: "ipv6-loopback",
    cidr: "::1/128",
    description: "guest IPv6 loopback",
};
const IPV6_LINK_LOCAL: PlatformEgressHardDenyCidr = PlatformEgressHardDenyCidr {
    id: "ipv6-link-local",
    cidr: "fe80::/10",
    description: "IPv6 link-local infrastructure range",
};
/// Platform-owned egress guardrails that always take precedence over tenant policy.
///
/// These are not user-authored policy rules. They are SPACE isolation invariants
/// and should be shown as non-editable effective behavior wherever policy is
/// surfaced to operators.
pub const PLATFORM_EGRESS_HARD_DENY_CIDRS: &[PlatformEgressHardDenyCidr] = &[
    EC2_IMDS,
    LINK_LOCAL,
    SPACE_SLOTS_A,
    SPACE_SLOTS_B,
    LOOPBACK,
    IPV6_LOOPBACK,
    IPV6_LINK_LOCAL,
];

/// Only the guest's own address is exempt from the link-local guardrail.
const GUEST_LINK_LOCAL_CARVE_OUT: (Ipv4Addr, u8) = (Ipv4Addr::new(169, 254, 0, 21), 32);
static PARSED_PLATFORM_EGRESS_HARD_DENY_CIDRS: OnceLock<
    Vec<(&'static PlatformEgressHardDenyCidr, IpCidr)>,
> = OnceLock::new();

/// The outcome of one egress evaluation: allow, or deny with a reason.
///
/// This is deliberately just the *decision* — allow vs deny plus, for a deny,
/// why. Callers combine it with their own connection context for metrics and
/// logging.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EgressDecision {
    Allow,
    Deny { reason: DenyReason },
}

/// The highest-precedence platform or explicit policy match for one address.
///
/// Unlike [`EgressDecision`], this does not apply the policy's default action
/// or DNS-derived evidence. DNS answer filtering uses this distinction to
/// reject denied addresses without requiring a separate address allow.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EgressAddressMatch {
    PlatformGuardrail { guardrail_id: &'static str },
    ExplicitDeny,
    ExplicitAllow,
    NoMatch,
}

/// Why an egress flow was denied. Ordered by precedence: a platform guardrail
/// is the platform isolation invariant and always precedes tenant policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DenyReason {
    /// A platform isolation invariant (IMDS, RFC1918, …) — absolute, precedes
    /// all tenant policy.
    PlatformGuardrail { guardrail_id: &'static str },
    /// Matched an explicit deny entry in the policy.
    Rule,
    /// No allow matched and the configured default action is deny.
    Default,
}

impl DenyReason {
    pub const fn label(self) -> &'static str {
        match self {
            Self::PlatformGuardrail { .. } => "platform_guardrail",
            Self::Rule => "explicit_deny",
            Self::Default => "default_deny",
        }
    }
}

impl EgressDecision {
    /// Construct a platform-guardrail deny.
    pub const fn platform_guardrail(guardrail_id: &'static str) -> Self {
        Self::Deny {
            reason: DenyReason::PlatformGuardrail { guardrail_id },
        }
    }

    pub const fn is_allowed(self) -> bool {
        matches!(self, Self::Allow)
    }

    /// The reason for a deny, or `None` when allowed.
    pub const fn deny_reason(self) -> Option<DenyReason> {
        match self {
            Self::Allow => None,
            Self::Deny { reason } => Some(reason),
        }
    }

    pub const fn label(self) -> &'static str {
        match self {
            Self::Allow => "allow",
            Self::Deny { reason } => reason.label(),
        }
    }
}

/// DNS attribution of one destination IP against the configured domain rules.
/// The gateway computes this from DNS responses it observed for the sandbox.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DnsDomainAttribution {
    pub allowed: bool,
}

pub fn platform_egress_hard_deny_for_ip(ip: IpAddr) -> Option<&'static PlatformEgressHardDenyCidr> {
    let ip = normalize_ip(ip);
    parsed_platform_egress_hard_deny_cidrs()
        .iter()
        .find_map(|(guardrail, cidr)| {
            guardrail_contains_ip(guardrail, *cidr, ip).then_some(*guardrail)
        })
}

pub fn is_platform_egress_hard_denied_ip(ip: IpAddr) -> bool {
    platform_egress_hard_deny_for_ip(ip).is_some()
}

fn parsed_platform_egress_hard_deny_cidrs()
-> &'static [(&'static PlatformEgressHardDenyCidr, IpCidr)] {
    PARSED_PLATFORM_EGRESS_HARD_DENY_CIDRS.get_or_init(|| {
        PLATFORM_EGRESS_HARD_DENY_CIDRS
            .iter()
            .map(|guardrail| {
                let cidr = guardrail
                    .cidr
                    .parse::<IpCidr>()
                    .expect("static platform guardrail CIDR must be valid");
                (guardrail, cidr)
            })
            .collect()
    })
}

fn guardrail_contains_ip(guardrail: &PlatformEgressHardDenyCidr, cidr: IpCidr, ip: IpAddr) -> bool {
    if !cidr.contains(ip) {
        return false;
    }
    if guardrail.id == "link-local"
        && let IpAddr::V4(ip) = ip
        && ipv4_cidr_contains(
            ip,
            GUEST_LINK_LOCAL_CARVE_OUT.0,
            GUEST_LINK_LOCAL_CARVE_OUT.1,
        )
    {
        return false;
    }
    true
}

fn normalize_ip(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(ip) => ip
            .to_ipv4_mapped()
            .map(IpAddr::V4)
            .unwrap_or(IpAddr::V6(ip)),
        IpAddr::V4(_) => ip,
    }
}

fn ipv4_cidr_contains(ip: Ipv4Addr, network: Ipv4Addr, prefix_len: u8) -> bool {
    debug_assert!(prefix_len <= 32);
    let mask = if prefix_len == 0 {
        0
    } else {
        u32::MAX << (32 - prefix_len)
    };
    (u32::from(ip) & mask) == (u32::from(network) & mask)
}

fn ipv6_cidr_contains(ip: Ipv6Addr, network: Ipv6Addr, prefix_len: u8) -> bool {
    debug_assert!(prefix_len <= 128);
    let mask = if prefix_len == 0 {
        0
    } else {
        u128::MAX << (128 - prefix_len)
    };
    (u128::from(ip) & mask) == (u128::from(network) & mask)
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum NetworkPolicyVersion {
    #[default]
    V1,
}

/// Top-level fields are additive: Rust readers ignore unknown fields, so a new
/// field must be safe for an older Rust reader to drop. Fields that restrict
/// traffic belong in a strict nested type such as [`EgressPolicyV1`].
///
/// The containerd shim decodes the pod's policy annotation strictly at every
/// level. A new field, top-level or nested, must be omitted when unused
/// (`skip_serializing_if`); every node's shim must accept it before any API sets
/// it; and the shim must keep accepting it while pods annotated with it exist.
/// `shim/testdata/network-policy-v1.json` pins this serialization for both sides.
#[cfg_attr(feature = "serde", derive(Serialize))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NetworkPolicyV1 {
    #[cfg_attr(feature = "serde", serde(default))]
    pub version: NetworkPolicyVersion,
    pub egress: EgressPolicyV1,
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "TlsInterception::is_off")
    )]
    pub tls_interception: TlsInterception,
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Vec::is_empty")
    )]
    pub network_credential_rules: Vec<NetworkCredentialRuleV1>,
    /// Destinations relayed without TLS termination after normal egress admission.
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Vec::is_empty")
    )]
    pub tls_interception_exemptions: Vec<EgressDestinationV1>,
}

#[cfg(feature = "serde")]
impl<'de> Deserialize<'de> for NetworkPolicyV1 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Helper {
            #[serde(default)]
            version: NetworkPolicyVersion,
            egress: EgressPolicyV1,
            #[serde(default)]
            tls_interception: TlsInterception,
            #[serde(default)]
            network_credential_rules: Vec<NetworkCredentialRuleV1>,
            #[serde(default)]
            tls_interception_exemptions: Vec<EgressDestinationV1>,
        }

        let mut value = Helper::deserialize(deserializer)?;
        validate_network_credential_policy(
            value.tls_interception,
            &mut value.network_credential_rules,
        )
        .map_err(de::Error::custom)?;
        Ok(Self {
            version: value.version,
            egress: value.egress,
            tls_interception: value.tls_interception,
            network_credential_rules: value.network_credential_rules,
            tls_interception_exemptions: value.tls_interception_exemptions,
        })
    }
}

/// The only network-policy document supported by SPACE.
pub type NetworkPolicy = NetworkPolicyV1;

/// Canonical identity digest shared by policy producers and credential
/// authority. Deserialization normalizes the closed policy value before this
/// serialization, so formatting differences in stored JSON do not change its
/// identity.
#[cfg(feature = "serde")]
pub fn network_policy_hash(policy: &NetworkPolicy) -> String {
    use sha2::Digest;

    let bytes = serde_json::to_vec(policy).expect("NetworkPolicy should serialize");
    hex::encode(sha2::Sha256::digest(bytes))
}

impl NetworkPolicyV1 {
    pub fn allow_all() -> Self {
        Self {
            version: NetworkPolicyVersion::V1,
            egress: EgressPolicyV1::allow_all(),
            tls_interception: TlsInterception::Off,
            tls_interception_exemptions: Vec::new(),
            network_credential_rules: Vec::new(),
        }
    }

    pub fn deny_all() -> Self {
        Self {
            version: NetworkPolicyVersion::V1,
            egress: EgressPolicyV1::deny_all(),
            tls_interception: TlsInterception::Off,
            tls_interception_exemptions: Vec::new(),
            network_credential_rules: Vec::new(),
        }
    }

    /// Select transport only; an exemption grants no egress permission.
    /// The caller must verify the hostname is DNS-attributed to this IP.
    pub fn tls_interception_for(
        &self,
        ip: IpAddr,
        attributed_host: Option<&DomainName>,
    ) -> TlsInterception {
        if self.tls_interception_exemptions.iter().any(|destination| {
            destination.matches_ip(ip)
                || attributed_host.is_some_and(|host| destination.matches_domain_name(host))
        }) {
            TlsInterception::Off
        } else {
            self.tls_interception
        }
    }

    pub const fn default_action(&self) -> EgressDefaultAction {
        self.egress.default_action
    }

    pub fn egress_decision_for_ip(&self, ip: IpAddr) -> EgressDecision {
        self.egress_decision_for_ip_with_dns_allow(ip, false)
    }

    /// Evaluate V1 egress with explicit deny rules taking precedence over
    /// overlapping explicit allows.
    pub fn egress_decision_for_socket(
        &self,
        destination: SocketAddr,
        dns_allowed_ip: bool,
    ) -> EgressDecision {
        self.egress_decision_for_socket_parts(destination.ip(), destination.port(), dns_allowed_ip)
    }

    pub fn egress_decision_for_socket_parts(
        &self,
        ip: IpAddr,
        _port: u16,
        dns_allowed_ip: bool,
    ) -> EgressDecision {
        self.egress_decision_for_ip_with_dns_allow(ip, dns_allowed_ip)
    }

    pub fn egress_decision_for_ip_with_dns_allow(
        &self,
        ip: IpAddr,
        dns_allowed_ip: bool,
    ) -> EgressDecision {
        match self.egress_match_for_ip(ip) {
            EgressAddressMatch::PlatformGuardrail { guardrail_id } => {
                return EgressDecision::platform_guardrail(guardrail_id);
            }
            EgressAddressMatch::ExplicitDeny => {
                return EgressDecision::Deny {
                    reason: DenyReason::Rule,
                };
            }
            EgressAddressMatch::ExplicitAllow => return EgressDecision::Allow,
            EgressAddressMatch::NoMatch => {}
        }

        if dns_allowed_ip {
            return EgressDecision::Allow;
        }

        if self.default_action().allows_egress() {
            EgressDecision::Allow
        } else {
            EgressDecision::Deny {
                reason: DenyReason::Default,
            }
        }
    }

    /// Return the highest-precedence explicit address match without applying
    /// DNS-derived evidence or the policy's default action.
    pub fn egress_match_for_ip(&self, ip: IpAddr) -> EgressAddressMatch {
        if let Some(guardrail) = platform_egress_hard_deny_for_ip(ip) {
            return EgressAddressMatch::PlatformGuardrail {
                guardrail_id: guardrail.id,
            };
        }
        if self.egress.deny.iter().any(|rule| rule.matches_ip(ip)) {
            return EgressAddressMatch::ExplicitDeny;
        }
        if self.egress.allow.iter().any(|rule| rule.matches_ip(ip)) {
            return EgressAddressMatch::ExplicitAllow;
        }
        EgressAddressMatch::NoMatch
    }

    pub fn matches_domain_allow_rule(&self, domain: &DomainName) -> bool {
        self.egress
            .allow
            .iter()
            .any(|rule| rule.matches_domain_name(domain))
    }

    pub fn denies_domain(&self, domain: &DomainName) -> bool {
        self.egress
            .deny
            .iter()
            .any(|rule| rule.matches_domain_name(domain))
    }

    /// Whether an explicit domain allow matches without an overlapping deny.
    ///
    /// This is a permission predicate, not a verdict. A domain deny only
    /// withdraws DNS-derived permission; it does not block a destination that
    /// the default action or an address rule already allows.
    pub fn explicitly_allows_domain_name(&self, domain: &DomainName) -> bool {
        !self.denies_domain(domain) && self.matches_domain_allow_rule(domain)
    }

    pub fn allows_domain(&self, domain: &str) -> bool {
        domain
            .parse::<DomainName>()
            .is_ok_and(|domain| self.explicitly_allows_domain_name(&domain))
    }

    /// Resolve every header binding authorized for one request.
    ///
    /// Overlapping rules retain the existing first-rule-wins behavior for each
    /// header. Results are sorted by normalized header name after precedence
    /// has been resolved.
    pub fn matching_network_credential_bindings(
        &self,
        host: &DomainName,
        port: u16,
        path: &str,
        method: NetworkCredentialHttpMethodV1,
    ) -> Vec<&CredentialHeaderBindingV1> {
        let mut bindings = BTreeMap::new();
        for rule in &self.network_credential_rules {
            if rule.destination.exact_host != *host
                || rule.destination.port != port
                || !rule.destination.matches_path(path)
                || !rule.supported_http_methods.contains(&method)
            {
                continue;
            }
            for binding in &rule.credential_header_bindings {
                bindings
                    .entry(binding.header_name.as_str())
                    .or_insert(binding);
            }
        }
        bindings.into_values().collect()
    }
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(deny_unknown_fields))]
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct CredentialHeaderBindingV1 {
    pub header_name: String,
    pub credential_reference_id: uuid::Uuid,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(deny_unknown_fields))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NetworkCredentialRuleV1 {
    pub rule_id: String,
    pub destination: NetworkCredentialDestinationV1,
    pub credential_header_bindings: Vec<CredentialHeaderBindingV1>,
    pub supported_http_methods: Vec<NetworkCredentialHttpMethodV1>,
}

#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "serde", serde(deny_unknown_fields))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NetworkCredentialDestinationV1 {
    pub exact_host: DomainName,
    #[cfg_attr(
        feature = "serde",
        serde(skip_serializing_if = "network_credential_port_is_default")
    )]
    pub port: u16,
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    pub path_prefix: Option<String>,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "UPPERCASE"))]
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum NetworkCredentialHttpMethodV1 {
    Get,
    Post,
    Head,
}

impl NetworkCredentialHttpMethodV1 {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Post => "POST",
            Self::Head => "HEAD",
        }
    }

    pub fn from_http(method: &http::Method) -> Option<Self> {
        if method == http::Method::GET {
            Some(Self::Get)
        } else if method == http::Method::POST {
            Some(Self::Post)
        } else if method == http::Method::HEAD {
            Some(Self::Head)
        } else {
            None
        }
    }
}

impl fmt::Display for NetworkCredentialHttpMethodV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for NetworkCredentialHttpMethodV1 {
    type Err = NetworkPolicyValidationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "GET" => Ok(Self::Get),
            "POST" => Ok(Self::Post),
            "HEAD" => Ok(Self::Head),
            _ => Err(NetworkPolicyValidationError::InvalidNetworkCredentialHttpMethods),
        }
    }
}

const fn network_credential_port_is_default(port: &u16) -> bool {
    *port == DEFAULT_NETWORK_CREDENTIAL_PORT
}

impl NetworkCredentialDestinationV1 {
    pub fn new(
        exact_host: DomainName,
        port: Option<u32>,
        path_prefix: Option<String>,
    ) -> Result<Self, NetworkPolicyValidationError> {
        let port = port.unwrap_or(u32::from(DEFAULT_NETWORK_CREDENTIAL_PORT));
        let port = u16::try_from(port)
            .ok()
            .filter(|port| *port != 0)
            .ok_or(NetworkPolicyValidationError::InvalidNetworkCredentialPort)?;
        if path_prefix
            .as_deref()
            .is_some_and(|path| !safe_network_credential_path(path))
        {
            return Err(NetworkPolicyValidationError::InvalidNetworkCredentialPathPrefix);
        }
        Ok(Self {
            exact_host,
            port,
            path_prefix,
        })
    }

    pub fn matches_path(&self, path: &str) -> bool {
        if !safe_network_credential_path(path) {
            return false;
        }
        self.path_prefix.as_deref().is_none_or(|prefix| {
            path == prefix
                || (path.starts_with(prefix)
                    && (prefix.ends_with('/') || path.as_bytes().get(prefix.len()) == Some(&b'/')))
        })
    }
}

#[cfg(feature = "serde")]
impl<'de> Deserialize<'de> for NetworkCredentialDestinationV1 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Helper {
            exact_host: DomainName,
            port: Option<u32>,
            path_prefix: Option<String>,
        }
        let value = Helper::deserialize(deserializer)?;
        Self::new(value.exact_host, value.port, value.path_prefix).map_err(de::Error::custom)
    }
}

fn safe_network_credential_path(path: &str) -> bool {
    path.starts_with('/')
        && path.len() <= MAX_NETWORK_CREDENTIAL_PATH_PREFIX_BYTES
        && !path
            .chars()
            .any(|character| matches!(character, '\\' | '?' | '#' | '%' | '\0'))
        && !path.contains("//")
        && !path.split('/').any(|segment| matches!(segment, "." | ".."))
}

fn is_reserved_network_credential_header(name: &str) -> bool {
    name.starts_with("x-space-")
        || matches!(
            name,
            "connection"
                | "content-length"
                | "host"
                | "http2-settings"
                | "keep-alive"
                | "proxy-authenticate"
                | "proxy-authorization"
                | "proxy-connection"
                | "space-credential"
                | "te"
                | "trailer"
                | "transfer-encoding"
                | "upgrade"
        )
}

fn validate_network_credential_policy(
    tls_interception: TlsInterception,
    rules: &mut [NetworkCredentialRuleV1],
) -> Result<(), NetworkPolicyValidationError> {
    if !rules.is_empty() && tls_interception != TlsInterception::On {
        return Err(NetworkPolicyValidationError::NetworkCredentialsRequireTlsInterception);
    }

    for rule in rules.iter_mut() {
        if rule.supported_http_methods.is_empty() {
            return Err(NetworkPolicyValidationError::InvalidNetworkCredentialHttpMethods);
        }
        rule.supported_http_methods.sort_unstable();
        rule.supported_http_methods.dedup();

        if rule.credential_header_bindings.is_empty() {
            return Err(NetworkPolicyValidationError::InvalidNetworkCredentialHeaderCount);
        }
        for binding in &mut rule.credential_header_bindings {
            let name = http::HeaderName::from_bytes(binding.header_name.as_bytes())
                .map_err(|_| NetworkPolicyValidationError::InvalidNetworkCredentialHeaderName)?;
            let name = name.as_str().to_owned();
            if is_reserved_network_credential_header(&name) {
                return Err(
                    NetworkPolicyValidationError::ReservedNetworkCredentialHeaderName(name),
                );
            }
            binding.header_name = name;
        }
        rule.credential_header_bindings.sort_unstable();
        if rule
            .credential_header_bindings
            .windows(2)
            .any(|bindings| match bindings {
                [first, second] => first.header_name == second.header_name,
                _ => false,
            })
        {
            return Err(NetworkPolicyValidationError::InvalidNetworkCredentialHeaderName);
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EffectiveNetworkPolicyRefScope {
    Template,
    Sandbox,
}

impl EffectiveNetworkPolicyRefScope {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Template => "template",
            Self::Sandbox => "sandbox",
        }
    }
}

impl fmt::Display for EffectiveNetworkPolicyRefScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for EffectiveNetworkPolicyRefScope {
    type Err = EffectiveNetworkPolicyRefParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "template" => Ok(Self::Template),
            "sandbox" => Ok(Self::Sandbox),
            _ => Err(EffectiveNetworkPolicyRefParseError::InvalidScope),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EffectiveNetworkPolicyRef {
    kind: EffectiveNetworkPolicyRefKind,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum EffectiveNetworkPolicyRefKind {
    Cluster {
        scope: EffectiveNetworkPolicyRefScope,
        owner: String,
        generation: u64,
        sha256: String,
    },
    Local {
        scope: EffectiveNetworkPolicyRefScope,
        owner: String,
        sha256: String,
    },
    NoIdentity,
}

impl EffectiveNetworkPolicyRef {
    pub fn cluster(
        scope: EffectiveNetworkPolicyRefScope,
        owner: impl Into<String>,
        generation: u64,
        sha256: impl Into<String>,
    ) -> Result<Self, EffectiveNetworkPolicyRefParseError> {
        let owner = owner.into();
        let sha256 = sha256.into();
        validate_policy_ref_owner(&owner)?;
        validate_policy_ref_sha256(&sha256)?;
        Ok(Self {
            kind: EffectiveNetworkPolicyRefKind::Cluster {
                scope,
                owner,
                generation,
                sha256,
            },
        })
    }

    pub fn local(
        scope: EffectiveNetworkPolicyRefScope,
        owner: impl Into<String>,
        sha256: impl Into<String>,
    ) -> Result<Self, EffectiveNetworkPolicyRefParseError> {
        let owner = owner.into();
        let sha256 = sha256.into();
        validate_policy_ref_owner(&owner)?;
        validate_policy_ref_sha256(&sha256)?;
        Ok(Self {
            kind: EffectiveNetworkPolicyRefKind::Local {
                scope,
                owner,
                sha256,
            },
        })
    }

    pub fn no_identity() -> Self {
        Self {
            kind: EffectiveNetworkPolicyRefKind::NoIdentity,
        }
    }

    pub fn authority_label(&self) -> &'static str {
        match &self.kind {
            EffectiveNetworkPolicyRefKind::Cluster { .. } => "cluster",
            EffectiveNetworkPolicyRefKind::Local { .. } => "local",
            EffectiveNetworkPolicyRefKind::NoIdentity => "no_identity",
        }
    }

    /// The cluster-authority generation embedded in the ref, if any. Local and
    /// no-identity refs carry no generation.
    pub fn generation(&self) -> Option<u64> {
        match &self.kind {
            EffectiveNetworkPolicyRefKind::Cluster { generation, .. } => Some(*generation),
            EffectiveNetworkPolicyRefKind::Local { .. }
            | EffectiveNetworkPolicyRefKind::NoIdentity => None,
        }
    }

    /// Whether this ref is the placeholder `no-identity`, i.e. it names no
    /// concrete installed policy revision.
    pub fn is_no_identity(&self) -> bool {
        matches!(self.kind, EffectiveNetworkPolicyRefKind::NoIdentity)
    }
}

impl fmt::Display for EffectiveNetworkPolicyRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            EffectiveNetworkPolicyRefKind::Cluster {
                scope,
                owner,
                generation,
                sha256,
            } => write!(
                f,
                "cluster:{scope}:{owner}:generation:{generation}:sha256:{sha256}"
            ),
            EffectiveNetworkPolicyRefKind::Local {
                scope,
                owner,
                sha256,
            } => write!(f, "local:{scope}:{owner}:sha256:{sha256}"),
            EffectiveNetworkPolicyRefKind::NoIdentity => f.write_str("no-identity"),
        }
    }
}

impl FromStr for EffectiveNetworkPolicyRef {
    type Err = EffectiveNetworkPolicyRefParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.is_empty() {
            return Err(EffectiveNetworkPolicyRefParseError::Empty);
        }
        if value == "no-identity" {
            return Ok(Self::no_identity());
        }

        let parts = value.split(':').collect::<Vec<_>>();
        match parts.as_slice() {
            [
                "cluster",
                scope,
                owner,
                "generation",
                generation,
                "sha256",
                sha256,
            ] => {
                let scope = scope.parse()?;
                validate_policy_ref_owner(owner)?;
                let generation = generation
                    .parse::<u64>()
                    .map_err(|_| EffectiveNetworkPolicyRefParseError::InvalidGeneration)?;
                validate_policy_ref_sha256(sha256)?;
                Ok(Self {
                    kind: EffectiveNetworkPolicyRefKind::Cluster {
                        scope,
                        owner: (*owner).to_string(),
                        generation,
                        sha256: (*sha256).to_string(),
                    },
                })
            }
            ["local", scope, owner, "sha256", sha256] => {
                let scope = scope.parse()?;
                validate_policy_ref_owner(owner)?;
                validate_policy_ref_sha256(sha256)?;
                Ok(Self {
                    kind: EffectiveNetworkPolicyRefKind::Local {
                        scope,
                        owner: (*owner).to_string(),
                        sha256: (*sha256).to_string(),
                    },
                })
            }
            _ => Err(EffectiveNetworkPolicyRefParseError::InvalidFormat),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum EffectiveNetworkPolicyRefParseError {
    #[error("effective network policy ref is empty")]
    Empty,
    #[error("effective network policy ref has invalid format")]
    InvalidFormat,
    #[error("effective network policy ref has invalid scope")]
    InvalidScope,
    #[error("effective network policy ref owner is empty")]
    EmptyOwner,
    #[error("effective network policy ref generation is invalid")]
    InvalidGeneration,
    #[error("effective network policy ref sha256 digest is invalid")]
    InvalidSha256,
}

pub fn effective_network_policy_refs_match(installed: &str, expected: &str) -> bool {
    if expected.is_empty() {
        return true;
    }
    match (
        installed.parse::<EffectiveNetworkPolicyRef>(),
        expected.parse::<EffectiveNetworkPolicyRef>(),
    ) {
        (Ok(installed), Ok(expected)) => installed == expected,
        _ => false,
    }
}

pub fn effective_network_policy_ref_authority_label(policy_ref: &str) -> &'static str {
    if policy_ref.is_empty() {
        return "unknown";
    }
    policy_ref
        .parse::<EffectiveNetworkPolicyRef>()
        .map(|policy_ref| policy_ref.authority_label())
        .unwrap_or("unknown")
}

fn validate_policy_ref_owner(owner: &str) -> Result<(), EffectiveNetworkPolicyRefParseError> {
    if owner.is_empty() {
        Err(EffectiveNetworkPolicyRefParseError::EmptyOwner)
    } else if owner.contains(':') {
        Err(EffectiveNetworkPolicyRefParseError::InvalidFormat)
    } else {
        Ok(())
    }
}

fn validate_policy_ref_sha256(sha256: &str) -> Result<(), EffectiveNetworkPolicyRefParseError> {
    if sha256.len() == 64 && sha256.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Ok(())
    } else {
        Err(EffectiveNetworkPolicyRefParseError::InvalidSha256)
    }
}

impl TryFrom<&proto::NetworkPolicyV1> for NetworkPolicyV1 {
    type Error = NetworkPolicyProtoError;

    fn try_from(policy: &proto::NetworkPolicyV1) -> Result<Self, Self::Error> {
        let version = proto::NetworkPolicyVersion::try_from(policy.version)
            .unwrap_or(proto::NetworkPolicyVersion::Unspecified);
        match version {
            proto::NetworkPolicyVersion::V1 => {}
            proto::NetworkPolicyVersion::Unspecified => {
                return Err(NetworkPolicyProtoError::UnspecifiedVersion);
            }
        }
        let action = proto::EgressDefaultAction::try_from(policy.egress_default_action)
            .unwrap_or(proto::EgressDefaultAction::Unspecified);
        let default_action = match action {
            proto::EgressDefaultAction::Allow => EgressDefaultAction::Allow,
            proto::EgressDefaultAction::Deny => EgressDefaultAction::Deny,
            proto::EgressDefaultAction::Unspecified => {
                return Err(NetworkPolicyProtoError::UnspecifiedEgressDefaultAction);
            }
        };
        let allow = proto_destinations_to_common("allow", &policy.allow)?;
        let deny = proto_destinations_to_common("deny", &policy.deny)?;
        let tls_interception_exemptions = proto_destinations_to_common(
            "tls_interception_exemptions",
            &policy.tls_interception_exemptions,
        )?;
        let tls_interception = match proto::TlsInterception::try_from(policy.tls_interception) {
            Ok(proto::TlsInterception::Unspecified | proto::TlsInterception::Off) => {
                TlsInterception::Off
            }
            Ok(proto::TlsInterception::On) => TlsInterception::On,
            Err(_) => return Err(NetworkPolicyProtoError::InvalidTlsInterception),
        };
        let mut network_credential_rules = policy
            .network_credential_rules
            .iter()
            .enumerate()
            .map(|(index, rule)| {
                NetworkCredentialRuleV1::try_from(rule).map_err(|error| {
                    NetworkPolicyProtoError::InvalidNetworkCredentialRule {
                        index,
                        message: error.to_string(),
                    }
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        validate_network_credential_policy(tls_interception, &mut network_credential_rules)
            .map_err(
                |error| NetworkPolicyProtoError::InvalidNetworkCredentialRule {
                    index: 0,
                    message: error.to_string(),
                },
            )?;
        EgressPolicyV1::new(default_action, allow, deny)
            .map(|egress| Self {
                version: NetworkPolicyVersion::V1,
                egress,
                tls_interception,
                network_credential_rules,
                tls_interception_exemptions,
            })
            .map_err(NetworkPolicyProtoError::from_validation_error)
    }
}

impl From<&NetworkPolicyV1> for proto::NetworkPolicyV1 {
    fn from(policy: &NetworkPolicyV1) -> Self {
        let egress_default_action = match policy.default_action() {
            EgressDefaultAction::Allow => proto::EgressDefaultAction::Allow,
            EgressDefaultAction::Deny => proto::EgressDefaultAction::Deny,
        };
        Self {
            version: proto::NetworkPolicyVersion::V1.into(),
            egress_default_action: egress_default_action.into(),
            allow: policy.egress.allow.iter().map(Into::into).collect(),
            deny: policy.egress.deny.iter().map(Into::into).collect(),
            tls_interception: match policy.tls_interception {
                // Keep the pre-field protobuf encoding for legacy policies.
                // Unspecified is defined to mean Off at this boundary.
                TlsInterception::Off => proto::TlsInterception::Unspecified,
                TlsInterception::On => proto::TlsInterception::On,
            }
            .into(),
            tls_interception_exemptions: policy
                .tls_interception_exemptions
                .iter()
                .map(Into::into)
                .collect(),
            network_credential_rules: policy
                .network_credential_rules
                .iter()
                .map(Into::into)
                .collect(),
        }
    }
}

impl TryFrom<&proto::NetworkCredentialRuleV1> for NetworkCredentialRuleV1 {
    type Error = NetworkPolicyValidationError;

    fn try_from(rule: &proto::NetworkCredentialRuleV1) -> Result<Self, Self::Error> {
        let destination = rule
            .destination
            .as_ref()
            .ok_or(NetworkPolicyValidationError::InvalidNetworkCredentialPathPrefix)?;
        let destination = NetworkCredentialDestinationV1::new(
            destination.exact_host.parse()?,
            destination.port,
            destination.path_prefix.clone(),
        )?;
        let credential_header_bindings = rule
            .credential_header_bindings
            .iter()
            .map(|binding| {
                let reference_id = uuid::Uuid::parse_str(&binding.credential_reference_id)
                    .map_err(|_| NetworkPolicyValidationError::InvalidCredentialReferenceId)?;
                Ok(CredentialHeaderBindingV1 {
                    header_name: binding.header_name.clone(),
                    credential_reference_id: reference_id,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let supported_http_methods = rule
            .supported_http_methods
            .iter()
            .map(
                |method| match proto::NetworkCredentialHttpMethodV1::try_from(*method) {
                    Ok(proto::NetworkCredentialHttpMethodV1::Get) => {
                        Ok(NetworkCredentialHttpMethodV1::Get)
                    }
                    Ok(proto::NetworkCredentialHttpMethodV1::Post) => {
                        Ok(NetworkCredentialHttpMethodV1::Post)
                    }
                    Ok(proto::NetworkCredentialHttpMethodV1::Head) => {
                        Ok(NetworkCredentialHttpMethodV1::Head)
                    }
                    Ok(proto::NetworkCredentialHttpMethodV1::Unspecified) | Err(_) => {
                        Err(NetworkPolicyValidationError::InvalidNetworkCredentialHttpMethods)
                    }
                },
            )
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            rule_id: rule.rule_id.clone(),
            destination,
            credential_header_bindings,
            supported_http_methods,
        })
    }
}

impl From<&NetworkCredentialRuleV1> for proto::NetworkCredentialRuleV1 {
    fn from(rule: &NetworkCredentialRuleV1) -> Self {
        Self {
            rule_id: rule.rule_id.clone(),
            destination: Some(proto::NetworkCredentialDestinationV1 {
                exact_host: rule.destination.exact_host.to_string(),
                port: (!network_credential_port_is_default(&rule.destination.port))
                    .then_some(u32::from(rule.destination.port)),
                path_prefix: rule.destination.path_prefix.clone(),
            }),
            credential_header_bindings: rule
                .credential_header_bindings
                .iter()
                .map(|binding| proto::CredentialHeaderBindingV1 {
                    header_name: binding.header_name.clone(),
                    credential_reference_id: binding.credential_reference_id.to_string(),
                })
                .collect(),
            supported_http_methods: rule
                .supported_http_methods
                .iter()
                .map(|method| {
                    (match method {
                        NetworkCredentialHttpMethodV1::Get => {
                            proto::NetworkCredentialHttpMethodV1::Get
                        }
                        NetworkCredentialHttpMethodV1::Post => {
                            proto::NetworkCredentialHttpMethodV1::Post
                        }
                        NetworkCredentialHttpMethodV1::Head => {
                            proto::NetworkCredentialHttpMethodV1::Head
                        }
                    }) as i32
                })
                .collect(),
        }
    }
}

impl NetworkPolicyProtoError {
    fn from_validation_error(error: NetworkPolicyValidationError) -> Self {
        Self::InvalidDestination {
            field: "policy",
            index: 0,
            message: error.to_string(),
        }
    }
}

fn proto_destinations_to_common(
    field: &'static str,
    values: &[proto::EgressDestinationV1],
) -> Result<Vec<EgressDestinationV1>, NetworkPolicyProtoError> {
    values
        .iter()
        .enumerate()
        .map(|(index, value)| {
            EgressDestinationV1::try_from(value).map_err(|error| match error {
                ProtoDestinationError::MissingDestination => {
                    NetworkPolicyProtoError::MissingDestination { field, index }
                }
                ProtoDestinationError::Invalid(message) => {
                    NetworkPolicyProtoError::InvalidDestination {
                        field,
                        index,
                        message,
                    }
                }
            })
        })
        .collect()
}

impl Default for NetworkPolicyV1 {
    fn default() -> Self {
        Self::deny_all()
    }
}

#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "serde", serde(deny_unknown_fields))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EgressPolicyV1 {
    pub default_action: EgressDefaultAction,
    #[cfg_attr(feature = "serde", serde(default))]
    pub allow: Vec<EgressDestinationV1>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub deny: Vec<EgressDestinationV1>,
}

impl EgressPolicyV1 {
    pub fn new(
        default_action: EgressDefaultAction,
        allow: Vec<EgressDestinationV1>,
        deny: Vec<EgressDestinationV1>,
    ) -> Result<Self, NetworkPolicyValidationError> {
        Ok(Self {
            default_action,
            allow,
            deny,
        })
    }

    pub fn allow_all() -> Self {
        Self {
            default_action: EgressDefaultAction::Allow,
            allow: Vec::new(),
            deny: Vec::new(),
        }
    }

    pub fn deny_all() -> Self {
        Self {
            default_action: EgressDefaultAction::Deny,
            allow: Vec::new(),
            deny: Vec::new(),
        }
    }

    pub fn allow_counts(&self) -> EgressDestinationCounts {
        EgressDestinationCounts::of(&self.allow)
    }

    pub fn deny_counts(&self) -> EgressDestinationCounts {
        EgressDestinationCounts::of(&self.deny)
    }
}

/// Per-class rule counts for one direction (allow or deny) of an egress
/// policy. Bounded and low-cardinality by construction, so it is safe to
/// attach to policy update/hydration logs where the full destination lists
/// would not be.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct EgressDestinationCounts {
    pub total: usize,
    pub ips: usize,
    pub cidrs: usize,
    pub domains: usize,
    pub wildcard_domains: usize,
}

impl EgressDestinationCounts {
    pub fn of(rules: &[EgressDestinationV1]) -> Self {
        let mut counts = Self {
            total: rules.len(),
            ..Self::default()
        };
        for rule in rules {
            match rule {
                EgressDestinationV1::Ip(_) => counts.ips += 1,
                EgressDestinationV1::Cidr(_) => counts.cidrs += 1,
                EgressDestinationV1::Domain(_) => counts.domains += 1,
                EgressDestinationV1::WildcardDomain(_) => counts.wildcard_domains += 1,
            }
        }
        counts
    }
}

#[cfg(feature = "serde")]
impl<'de> Deserialize<'de> for EgressPolicyV1 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Helper {
            default_action: EgressDefaultAction,
            #[serde(default)]
            allow: Vec<EgressDestinationV1>,
            #[serde(default)]
            deny: Vec<EgressDestinationV1>,
        }

        let helper = Helper::deserialize(deserializer)?;
        EgressPolicyV1::new(helper.default_action, helper.allow, helper.deny)
            .map_err(de::Error::custom)
    }
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum EgressDefaultAction {
    Allow,
    #[default]
    Deny,
}

impl EgressDefaultAction {
    pub const fn allows_egress(self) -> bool {
        matches!(self, Self::Allow)
    }
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EgressDestinationV1 {
    Ip(IpAddr),
    Cidr(IpCidr),
    Domain(DomainName),
    WildcardDomain(WildcardDomainName),
}

impl EgressDestinationV1 {
    pub fn matches_ip(&self, ip: IpAddr) -> bool {
        let ip = normalize_ip(ip);
        match self {
            Self::Ip(rule_ip) => normalize_ip(*rule_ip) == ip,
            Self::Cidr(cidr) => cidr.contains(ip),
            Self::Domain(_) | Self::WildcardDomain(_) => false,
        }
    }

    pub fn matches_domain(&self, domain: &str) -> bool {
        domain
            .parse::<DomainName>()
            .is_ok_and(|domain| self.matches_domain_name(&domain))
    }

    pub fn matches_domain_name(&self, domain: &DomainName) -> bool {
        match self {
            Self::Domain(rule) => rule == domain,
            Self::WildcardDomain(rule) => rule.matches_domain_name(domain),
            Self::Ip(_) | Self::Cidr(_) => false,
        }
    }
}

impl fmt::Display for EgressDestinationV1 {
    /// Canonical textual form of the destination (the inner value's `Display`).
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ip(ip) => write!(formatter, "{ip}"),
            Self::Cidr(cidr) => write!(formatter, "{cidr}"),
            Self::Domain(domain) => write!(formatter, "{domain}"),
            Self::WildcardDomain(domain) => write!(formatter, "{domain}"),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ProtoDestinationError {
    #[error("destination oneof has no variant set")]
    MissingDestination,
    #[error("invalid destination: {0}")]
    Invalid(String),
}

impl TryFrom<&proto::EgressDestinationV1> for EgressDestinationV1 {
    type Error = ProtoDestinationError;

    fn try_from(value: &proto::EgressDestinationV1) -> Result<Self, Self::Error> {
        use proto::egress_destination_v1::Destination;

        match value.destination.as_ref() {
            Some(Destination::Ip(value)) => value
                .parse::<IpAddr>()
                .map(Self::Ip)
                .map_err(|error| ProtoDestinationError::Invalid(error.to_string())),
            Some(Destination::Cidr(value)) => value
                .parse::<IpCidr>()
                .map(Self::Cidr)
                .map_err(|error| ProtoDestinationError::Invalid(error.to_string())),
            Some(Destination::Domain(value)) => value
                .parse::<DomainName>()
                .map(Self::Domain)
                .map_err(|error| ProtoDestinationError::Invalid(error.to_string())),
            Some(Destination::WildcardDomain(value)) => value
                .parse::<WildcardDomainName>()
                .map(Self::WildcardDomain)
                .map_err(|error| ProtoDestinationError::Invalid(error.to_string())),
            None => Err(ProtoDestinationError::MissingDestination),
        }
    }
}

impl From<&EgressDestinationV1> for proto::EgressDestinationV1 {
    fn from(value: &EgressDestinationV1) -> Self {
        use proto::egress_destination_v1::Destination;

        let destination = match value {
            EgressDestinationV1::Ip(ip) => Destination::Ip(ip.to_string()),
            EgressDestinationV1::Cidr(cidr) => Destination::Cidr(cidr.to_string()),
            EgressDestinationV1::Domain(domain) => Destination::Domain(domain.to_string()),
            EgressDestinationV1::WildcardDomain(domain) => {
                Destination::WildcardDomain(domain.to_string())
            }
        };
        Self {
            destination: Some(destination),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IpCidr {
    network: IpAddr,
    prefix_len: u8,
}

impl IpCidr {
    /// True for genuinely-IPv6 networks. IPv4-mapped IPv6 CIDRs normalize to
    /// IPv4 at parse time, so they never report as IPv6 here.
    pub const fn is_ipv6(self) -> bool {
        matches!(self.network, IpAddr::V6(_))
    }

    pub fn contains(self, ip: IpAddr) -> bool {
        match (normalize_ip(self.network), normalize_ip(ip)) {
            (IpAddr::V4(network), IpAddr::V4(ip)) if self.prefix_len <= 32 => {
                ipv4_cidr_contains(ip, network, self.prefix_len)
            }
            (IpAddr::V6(network), IpAddr::V6(ip)) if self.prefix_len <= 128 => {
                ipv6_cidr_contains(ip, network, self.prefix_len)
            }
            _ => false,
        }
    }

    /// The addresses of this network outside `excluded`, as CIDRs. An
    /// `excluded` that covers this network leaves nothing, and one that does
    /// not overlap it leaves it whole. IPv6 parts inside the IPv4-mapped range
    /// (`::ffff:0:0/96`) are left out: an IPv6 network matches no address
    /// there, but their text form parses as an IPv4 network, so they would
    /// change meaning in a policy protobuf.
    pub fn without(self, excluded: Self) -> Vec<Self> {
        if excluded.prefix_len <= self.prefix_len && excluded.contains(self.network) {
            return Vec::new();
        }
        if !self.contains(excluded.network) {
            return vec![self];
        }
        let mut remaining = Vec::new();
        let mut current = self;
        while current.prefix_len < excluded.prefix_len {
            let (lower, upper) = current.halves();
            if lower.contains(excluded.network) {
                remaining.push(upper);
                current = lower;
            } else {
                remaining.push(lower);
                current = upper;
            }
        }
        remaining.retain(|cidr| {
            !matches!(cidr.network, IpAddr::V6(network) if network.to_ipv4_mapped().is_some())
        });
        remaining
    }

    fn halves(self) -> (Self, Self) {
        let prefix_len = self.prefix_len + 1;
        let upper = match self.network {
            IpAddr::V4(network) => IpAddr::V4(Ipv4Addr::from(
                u32::from(network) | (1 << (32 - prefix_len)),
            )),
            IpAddr::V6(network) => IpAddr::V6(Ipv6Addr::from(
                u128::from(network) | (1 << (128 - prefix_len)),
            )),
        };
        (
            Self {
                network: self.network,
                prefix_len,
            },
            Self {
                network: upper,
                prefix_len,
            },
        )
    }
}

impl FromStr for IpCidr {
    type Err = NetworkPolicyValidationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let Some((ip, prefix)) = value.split_once('/') else {
            return Err(NetworkPolicyValidationError::InvalidCidr(value.to_string()));
        };
        let mut ip = ip
            .parse::<IpAddr>()
            .map_err(|_| NetworkPolicyValidationError::InvalidCidr(value.to_string()))?;
        let mut prefix_len = prefix
            .parse::<u8>()
            .map_err(|_| NetworkPolicyValidationError::InvalidCidr(value.to_string()))?;
        if let IpAddr::V6(ipv6) = ip
            && let Some(ipv4) = ipv6.to_ipv4_mapped()
        {
            if prefix_len < 96 {
                return Err(NetworkPolicyValidationError::InvalidPrefixLength {
                    cidr: value.to_string(),
                    prefix_len,
                });
            }
            ip = IpAddr::V4(ipv4);
            prefix_len -= 96;
        }
        let max = match ip {
            IpAddr::V4(_) => 32,
            IpAddr::V6(_) => 128,
        };
        if prefix_len > max {
            return Err(NetworkPolicyValidationError::InvalidPrefixLength {
                cidr: value.to_string(),
                prefix_len,
            });
        }
        let network = match ip {
            IpAddr::V4(ip) => {
                let mask = if prefix_len == 0 {
                    0
                } else {
                    u32::MAX << (32 - prefix_len)
                };
                IpAddr::V4(Ipv4Addr::from(u32::from(ip) & mask))
            }
            IpAddr::V6(ip) => {
                let mask = if prefix_len == 0 {
                    0
                } else {
                    u128::MAX << (128 - prefix_len)
                };
                IpAddr::V6(Ipv6Addr::from(u128::from(ip) & mask))
            }
        };
        Ok(Self {
            network,
            prefix_len,
        })
    }
}

impl fmt::Display for IpCidr {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}/{}", self.network, self.prefix_len)
    }
}

#[cfg(feature = "serde")]
impl Serialize for IpCidr {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

#[cfg(feature = "serde")]
impl<'de> Deserialize<'de> for IpCidr {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(de::Error::custom)
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct DomainName(String);

impl DomainName {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for DomainName {
    type Err = NetworkPolicyValidationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        normalize_domain(value)
            .map(Self)
            .ok_or_else(|| NetworkPolicyValidationError::InvalidDomain(value.to_string()))
    }
}

impl fmt::Display for DomainName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[cfg(feature = "serde")]
impl Serialize for DomainName {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

#[cfg(feature = "serde")]
impl<'de> Deserialize<'de> for DomainName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(de::Error::custom)
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct WildcardDomainName(DomainName);

impl WildcardDomainName {
    pub fn matches(&self, domain: &str) -> bool {
        domain
            .parse::<DomainName>()
            .is_ok_and(|domain| self.matches_domain_name(&domain))
    }

    pub fn matches_domain_name(&self, domain: &DomainName) -> bool {
        let suffix = self.0.as_str();
        domain
            .as_str()
            .strip_suffix(suffix)
            .is_some_and(|prefix| prefix.ends_with('.') && prefix.len() > 1)
    }
}

impl FromStr for WildcardDomainName {
    type Err = NetworkPolicyValidationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let Some(suffix) = value.strip_prefix("*.") else {
            return Err(NetworkPolicyValidationError::InvalidWildcardDomain(
                value.to_string(),
            ));
        };
        let suffix = DomainName::from_str(suffix)
            .map_err(|_| NetworkPolicyValidationError::InvalidWildcardDomain(value.to_string()))?;
        Ok(Self(suffix))
    }
}

impl fmt::Display for WildcardDomainName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "*.{}", self.0)
    }
}

#[cfg(feature = "serde")]
impl Serialize for WildcardDomainName {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

#[cfg(feature = "serde")]
impl<'de> Deserialize<'de> for WildcardDomainName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(de::Error::custom)
    }
}

fn normalize_domain(value: &str) -> Option<String> {
    let value = value.trim().trim_end_matches('.').to_ascii_lowercase();
    if value.is_empty() || value.len() > 253 || value.contains('*') {
        return None;
    }
    let labels = value.split('.').collect::<Vec<_>>();
    if labels.len() < 2 {
        return None;
    }
    if labels.iter().all(|label| valid_domain_label(label)) {
        Some(value)
    } else {
        None
    }
}

fn valid_domain_label(label: &str) -> bool {
    if label.is_empty() || label.len() > 63 {
        return false;
    }
    let bytes = label.as_bytes();
    bytes.first().is_some_and(u8::is_ascii_alphanumeric)
        && bytes.last().is_some_and(u8::is_ascii_alphanumeric)
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || *byte == b'-')
}

#[cfg(test)]
mod tests;

pub mod slot_dhcp;

pub mod guest_link;
