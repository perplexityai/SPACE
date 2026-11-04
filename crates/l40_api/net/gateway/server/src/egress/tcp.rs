//! TCP admission and established-connection policy revalidation.

use std::net::SocketAddr;
use std::time::Instant;

use pplx_space_net_common::{
    DenyReason, DomainName, EgressDecision, NetworkPolicyV1, TlsInterception,
};

use super::tracker::DnsAttributedConnection;
use super::{
    EgressContext, EgressProtocol, authorize_domain, dns_allow_required, record_decision,
    record_latency,
};
use crate::dns::DnsAllowCache;
use crate::sniff::MissingClientHello;
use crate::tls_sni::ClientHelloScan;

pub(crate) fn dns_attributed_host<'a>(
    destination: SocketAddr,
    scan: &'a ClientHelloScan,
    dns_allow_cache: &DnsAllowCache,
) -> Option<&'a DomainName> {
    scan.server_name()
        .filter(|host| dns_allow_cache.contains_host_binding(destination.ip(), host.as_str()))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AdmittedTcp {
    Tunnel(SocketAddr),
    HostBoundHttp(SocketAddr),
}

impl AdmittedTcp {
    pub(crate) const fn destination(self) -> SocketAddr {
        match self {
            Self::Tunnel(destination) | Self::HostBoundHttp(destination) => destination,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("TCP admission denied: {label}", label = self.label())]
pub(crate) enum TcpAdmissionDenial {
    DnsAttribution(DenyReason),
    PolicyChanged,
}

impl TcpAdmissionDenial {
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::DnsAttribution(reason) => reason.label(),
            Self::PolicyChanged => "policy_changed",
        }
    }
}

pub(crate) fn missing_client_hello(
    destination: SocketAddr,
    policy: &NetworkPolicyV1,
    dns_allow_cache: &DnsAllowCache,
) -> MissingClientHello {
    match dns_allow_required(destination, policy) {
        Some(DenyReason::Default) if dns_allow_cache.allows_ip(destination.ip(), policy) => {
            MissingClientHello::Denied
        }
        None if !policy.tls_interception_for(destination.ip(), None).is_off() => {
            MissingClientHello::Denied
        }
        _ => MissingClientHello::Proceeds,
    }
}

pub(crate) fn admit_tcp(
    destination: SocketAddr,
    scan: &ClientHelloScan,
    policy: &NetworkPolicyV1,
    dns_allow_cache: &DnsAllowCache,
) -> Result<AdmittedTcp, TcpAdmissionDenial> {
    let dns_allowed = dns_allow_cache.allows_ip(destination.ip(), policy);
    let attributed_host = dns_attributed_host(destination, scan, dns_allow_cache);
    let context = EgressContext::Admission(EgressProtocol::Tcp);
    let started_at = Instant::now();
    let result = check_tcp(
        context,
        destination,
        scan,
        policy,
        dns_allowed,
        attributed_host,
    );
    record_tcp_decision(context, started_at, &result, policy);
    result.map_err(TcpAdmissionDenial::DnsAttribution)
}

pub(super) fn revalidate_tcp(
    connection: &DnsAttributedConnection,
    policy: &NetworkPolicyV1,
    dns_allow_cache: &DnsAllowCache,
) -> bool {
    // Exemption evidence survives DNS expiry for an established connection.
    let attributed_host = connection
        .admitted_exempt_host
        .as_ref()
        .or_else(|| dns_attributed_host(connection.destination, &connection.scan, dns_allow_cache));
    let interception = policy.tls_interception_for(connection.destination.ip(), attributed_host);
    // Opaque TLS cannot become inspected on the same transport.
    if connection.scan != ClientHelloScan::NotTls
        && connection.admitted_tls_interception.is_off()
        && interception == TlsInterception::On
    {
        return false;
    }
    let dns_allowed = dns_allow_cache.allows_ip(connection.destination.ip(), policy)
        || connection
            .admitted_exempt_host
            .as_ref()
            .is_some_and(|host| policy.explicitly_allows_domain_name(host));
    let started_at = Instant::now();
    let result = check_tcp(
        EgressContext::Resweep,
        connection.destination,
        &connection.scan,
        policy,
        dns_allowed,
        attributed_host,
    );
    record_tcp_decision(EgressContext::Resweep, started_at, &result, policy);
    result.is_ok()
}

fn record_tcp_decision(
    context: EgressContext,
    started_at: Instant,
    result: &Result<AdmittedTcp, DenyReason>,
    policy: &NetworkPolicyV1,
) {
    let decision = match result {
        Ok(_) => EgressDecision::Allow,
        Err(reason) => EgressDecision::Deny { reason: *reason },
    };
    record_latency(context, started_at);
    record_decision(context, decision, policy.tls_interception);
}

fn check_tcp(
    context: EgressContext,
    destination: SocketAddr,
    scan: &ClientHelloScan,
    policy: &NetworkPolicyV1,
    dns_allowed: bool,
    attributed_host: Option<&DomainName>,
) -> Result<AdmittedTcp, DenyReason> {
    let decision = policy.egress_decision_for_socket(destination, dns_allowed);
    let interception = policy.tls_interception_for(destination.ip(), attributed_host);
    let explicit_domain_deny = scan
        .server_name()
        .is_some_and(|host| policy.denies_domain(host));
    let exempt = !policy.tls_interception.is_off() && interception.is_off();
    if explicit_domain_deny
        && ((exempt && decision.is_allowed())
            || matches!(
                decision,
                EgressDecision::Deny {
                    reason: DenyReason::Default
                }
            ))
    {
        return Err(DenyReason::Rule);
    }
    if let EgressDecision::Deny { reason } = decision {
        return Err(reason);
    }
    let Some(fail_closed_reason) = dns_allow_required(destination, policy) else {
        return Ok(AdmittedTcp::Tunnel(destination));
    };
    let (evidence, outcome, result) = match scan {
        ClientHelloScan::ServerName(host) => {
            let attributed = attributed_host == Some(host);
            if authorize_domain(policy, host, attributed).is_ok() {
                (
                    "tls_sni",
                    if interception.is_off() {
                        "opaque_relay"
                    } else {
                        "allowed"
                    },
                    Ok(AdmittedTcp::Tunnel(destination)),
                )
            } else {
                (
                    "tls_sni",
                    if attributed {
                        "host_not_allowed"
                    } else {
                        "host_unattributed"
                    },
                    Err(if explicit_domain_deny {
                        DenyReason::Rule
                    } else {
                        fail_closed_reason
                    }),
                )
            }
        }
        ClientHelloScan::NotTls if matches!(context, EgressContext::Admission(_)) => (
            "http_host",
            "plaintext_http",
            Ok(AdmittedTcp::HostBoundHttp(destination)),
        ),
        // A plaintext connection may change Host on every request. Reconnect
        // under the new policy when its permission still depends on DNS.
        ClientHelloScan::NotTls
        | ClientHelloScan::Incomplete
        | ClientHelloScan::NoServerName
        | ClientHelloScan::Malformed => ("tls_sni", "host_missing", Err(fail_closed_reason)),
    };
    if matches!(context, EgressContext::Admission(_)) {
        pplx_space_metrics::gateway::domain_binding("tcp", evidence, outcome, None);
    }
    result
}
