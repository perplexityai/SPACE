use super::TcpProxy;
use std::net::SocketAddr;
use std::sync::Arc;

use pplx_space_net_common::{EgressDecision, NetworkPolicyV1};

use crate::egress::{
    AdmittedTcp, EgressContext, EgressProtocol, TcpAdmissionDenial, evaluate_egress,
};
use crate::sniff::MissingClientHello;
use crate::state::TrafficPolicy;
use crate::tls_sni::ClientHelloScan;

impl TcpProxy {
    /// Apply normal destination admission before interpreting traffic to a
    /// reserved resolver as DNS. DNS attribution cannot authorize access to
    /// the resolver address itself.
    pub(super) fn admit_reserved_dns_destination(
        &self,
        destination: SocketAddr,
        policy: &NetworkPolicyV1,
    ) -> Result<AdmittedTcp, TcpAdmissionDenial> {
        match evaluate_egress(
            EgressContext::Admission(EgressProtocol::Tcp),
            destination,
            false,
            policy,
        ) {
            EgressDecision::Allow => Ok(AdmittedTcp::Tunnel(destination)),
            EgressDecision::Deny { reason } => Err(TcpAdmissionDenial::DnsAttribution(reason)),
        }
    }

    /// Confirm that an admission still belongs to the current policy. The
    /// caller performs this after registering the flow so a concurrent policy
    /// install must either sweep the registration or invalidate this check.
    pub(super) fn validate_admission_policy(
        &self,
        policy: &Arc<NetworkPolicyV1>,
    ) -> Result<(), TcpAdmissionDenial> {
        if self.network_state.is_current_tcp_policy(policy) {
            Ok(())
        } else {
            Err(TcpAdmissionDenial::PolicyChanged)
        }
    }

    /// How the ClientHello peek must treat a flow that shows none:
    ///
    /// - denied: a destination the default deny refuses without DNS evidence,
    ///   and the guest's DNS evidence admits (see [`Self::admit`]);
    /// - probed: with TLS interception on, a destination admitted without DNS
    ///   evidence whose IP is not exempt. Interception closes a flow without
    ///   SNI, so a silent guest first gets the server-first probe
    ///   (see `super::server_first`).
    ///
    /// A guardrail or an explicit deny refuses the flow before its ClientHello
    /// is read, and with interception off every other flow is decided without
    /// SNI, so neither waits for one.
    pub(super) fn missing_client_hello(
        &self,
        destination: SocketAddr,
        policy: &TrafficPolicy,
    ) -> MissingClientHello {
        let TrafficPolicy::Ready(policy) = policy else {
            return MissingClientHello::Proceeds;
        };
        crate::egress::missing_client_hello(
            destination,
            policy,
            self.network_state.dns_allow_cache(),
        )
    }

    /// Admission based on the original destination and guest DNS attribution.
    pub(super) fn admit(
        &self,
        destination: SocketAddr,
        scan: &ClientHelloScan,
        policy: &NetworkPolicyV1,
    ) -> Result<AdmittedTcp, TcpAdmissionDenial> {
        crate::egress::admit_tcp(
            destination,
            scan,
            policy,
            self.network_state.dns_allow_cache(),
        )
    }

    /// [`Self::admit`] for a flow the server-first probe may admit again.
    pub(super) fn admit_deferred(
        &self,
        destination: SocketAddr,
        scan: &ClientHelloScan,
        policy: &NetworkPolicyV1,
    ) -> (
        Result<AdmittedTcp, TcpAdmissionDenial>,
        crate::egress::TcpDecision,
    ) {
        crate::egress::admit_tcp_deferred(
            destination,
            scan,
            policy,
            self.network_state.dns_allow_cache(),
        )
    }
}
