//! Gateway egress authorization. Protocol adapters supply parsed facts;
//! this module owns policy decisions and established-connection revalidation.

use std::fmt;
use std::net::SocketAddr;
use std::time::Instant;

use pplx_space_net_common::{DenyReason, DomainName, EgressDecision, NetworkPolicyV1};

use crate::dns::DnsAllowCache;

mod tcp;
mod tracker;
pub(crate) use tcp::{AdmittedTcp, TcpAdmissionDenial, admit_tcp, missing_client_hello};
pub use tracker::DnsAttributedTcpTracker;

/// Transport an egress decision applies to.
///
/// Replaces the `"tcp"` / `"udp"` string literals that were previously threaded
/// into metric tags and log fields at each decision site.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EgressProtocol {
    Tcp,
    Udp,
}

impl EgressProtocol {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Tcp => "tcp",
            Self::Udp => "udp",
        }
    }
}

impl fmt::Display for EgressProtocol {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Which phase of egress enforcement produced a decision.
///
/// Becomes the `phase` tag on `gateway.policy.decision` so a burst of
/// [`Resweep`](Self::Resweep) re-evaluations on policy install does not inflate
/// the live [`Admission`](Self::Admission) rate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EgressPhase {
    /// A new guest flow asking to leave.
    Admission,
    /// Re-evaluation of an already-established flow against a freshly installed
    /// policy.
    Resweep,
}

impl EgressPhase {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Admission => "admission",
            Self::Resweep => "resweep",
        }
    }
}

impl fmt::Display for EgressPhase {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Why an egress verdict is being computed.
///
/// This distinguishes the two semantically different callers of the unified
/// decision point so the single `gateway.policy.decision` emission can tag them
/// apart:
///
/// * [`Admission`](Self::Admission) — a brand-new guest flow asking to leave on
///   the given transport. This is the per-flow hot path and the thing the
///   decision rate is meant to measure.
/// * [`Resweep`](Self::Resweep) — re-evaluation of an *already-established* flow
///   when a new policy is installed, to cancel flows the new policy now denies.
///   A resweep produces a burst of verdicts over steady-state connections;
///   tagging it `phase = resweep` keeps it from inflating the new-flow admission
///   rate while still making the enforcement pass observable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EgressContext {
    Admission(EgressProtocol),
    Resweep,
}

impl EgressContext {
    /// Transport tag for the decision metric. The connection sweep runs only
    /// over TCP flows, so a resweep is always TCP.
    fn protocol(self) -> EgressProtocol {
        match self {
            Self::Admission(protocol) => protocol,
            Self::Resweep => EgressProtocol::Tcp,
        }
    }

    /// Phase distinguishing a live admission decision from a policy resweep.
    fn phase(self) -> EgressPhase {
        match self {
            Self::Admission(_) => EgressPhase::Admission,
            Self::Resweep => EgressPhase::Resweep,
        }
    }
}

/// Decide whether `destination` may be reached under the installed `policy`,
/// given whether its IP has current DNS allow evidence, and record the egress
/// decision metrics tagged by `context` (transport + admission/resweep phase).
///
/// Reserved-resolver transports use this without DNS evidence. Other
/// transports use the complete protocol decisions in this module.
pub fn evaluate_egress(
    context: EgressContext,
    destination: SocketAddr,
    dns_allowed: bool,
    policy: &NetworkPolicyV1,
) -> EgressDecision {
    let started_at = Instant::now();
    let decision = evaluate_egress_unmetered(destination, dns_allowed, policy);
    // Measured before emitting the decision counter so the latency reflects
    // the policy evaluation and protocol gate, but not the metric plumbing
    // around them.
    record_latency(context, started_at);
    record_decision(context, decision, policy.tls_interception);

    decision
}

/// UDP cannot bind a payload to the hostname that supplied DNS permission.
pub(crate) fn admit_udp(
    cache: &DnsAllowCache,
    destination: SocketAddr,
    policy: &NetworkPolicyV1,
) -> EgressDecision {
    let dns_allowed = cache.allows_ip(destination.ip(), policy);
    let started_at = Instant::now();
    let mut decision = evaluate_egress_unmetered(destination, dns_allowed, policy);
    if decision.is_allowed()
        && let Some(reason) = dns_allow_required(destination, policy)
    {
        pplx_space_metrics::gateway::domain_binding("udp", "unavailable", "host_unavailable", None);
        decision = EgressDecision::Deny { reason };
    }
    let context = EgressContext::Admission(EgressProtocol::Udp);
    record_latency(context, started_at);
    record_decision(context, decision, policy.tls_interception);
    decision
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub(crate) enum HostBindingError {
    #[error("hostname is not allowed by policy")]
    HostNotAllowed,
    #[error("hostname has no DNS attribution for the destination")]
    HostUnattributed,
    #[error("destination denied by policy: {0:?}")]
    PolicyDenied(DenyReason),
}

fn authorize_domain(
    policy: &NetworkPolicyV1,
    host: &DomainName,
    attributed: bool,
) -> Result<(), HostBindingError> {
    if !policy.explicitly_allows_domain_name(host) {
        Err(HostBindingError::HostNotAllowed)
    } else if !attributed {
        Err(HostBindingError::HostUnattributed)
    } else {
        Ok(())
    }
}

pub(crate) fn authorize_host(
    policy: &NetworkPolicyV1,
    destination: SocketAddr,
    host: &DomainName,
    cache: &DnsAllowCache,
) -> Result<(), HostBindingError> {
    if dns_allow_required(destination, policy).is_none() {
        return Ok(());
    }
    authorize_domain(
        policy,
        host,
        cache.contains_host_binding(destination.ip(), host.as_str()),
    )?;
    match evaluate_egress_unmetered(
        destination,
        cache.allows_ip(destination.ip(), policy),
        policy,
    ) {
        EgressDecision::Allow => Ok(()),
        EgressDecision::Deny { reason } => Err(HostBindingError::PolicyDenied(reason)),
    }
}

pub(crate) fn allows_dns_query(policy: &NetworkPolicyV1, domain: &DomainName) -> bool {
    if policy.denies_domain(domain) {
        false
    } else if policy.matches_domain_allow_rule(domain) {
        true
    } else {
        policy.default_action().allows_egress()
    }
}

/// Compute a verdict without emitting per-decision telemetry. The plaintext
/// HTTP request gate uses this for every request on an already-admitted flow;
/// recording those as new TCP admissions would inflate the flow metric.
pub(crate) fn evaluate_egress_unmetered(
    destination: SocketAddr,
    dns_allowed: bool,
    policy: &NetworkPolicyV1,
) -> EgressDecision {
    policy.egress_decision_for_socket(destination, dns_allowed)
}

/// If removing DNS-domain allow evidence would turn this flow's allow into a
/// deny, return that fail-closed reason. Call only after the metered evaluation
/// allowed the flow.
pub(crate) fn dns_allow_required(
    destination: SocketAddr,
    policy: &NetworkPolicyV1,
) -> Option<pplx_space_net_common::DenyReason> {
    match evaluate_egress_unmetered(destination, false, policy) {
        EgressDecision::Allow => None,
        EgressDecision::Deny { reason } => Some(reason),
    }
}

/// Record how long the verdict above took to compute. Fractional milliseconds:
/// a single decision is usually well under a millisecond, so rounding to whole
/// milliseconds would report zero almost every time and hide regressions as the
/// rule set grows.
fn record_latency(context: EgressContext, started_at: Instant) {
    pplx_space_metrics::gateway::policy_decision_latency_ms(
        context.protocol().as_str(),
        context.phase().as_str(),
        started_at.elapsed().as_secs_f64() * 1_000.0,
    );
}

/// `interception` is the policy choice in force for this verdict, so admission
/// panels can be split by rollout state instead of inferred from deploy timing.
fn record_decision(
    context: EgressContext,
    decision: EgressDecision,
    interception: pplx_space_net_common::TlsInterception,
) {
    pplx_space_metrics::gateway::policy_decision(
        pplx_space_metrics::gateway::PolicyDecisionKind::DnsAttributed,
        context.protocol().as_str(),
        context.phase().as_str(),
        "v1",
        decision.label(),
        interception.as_str(),
    );
}

#[cfg(test)]
mod tests;
