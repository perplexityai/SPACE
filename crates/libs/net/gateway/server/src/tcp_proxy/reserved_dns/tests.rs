use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;

use hickory_resolver::proto::op::{Message, MessageType, OpCode, Query, ResponseCode};
use hickory_resolver::proto::rr::{Name, RecordType};
use pplx_space_net_common::{
    EffectiveNetworkPolicyRef, EffectiveNetworkPolicyRefScope, EgressDefaultAction,
    EgressDestinationV1, EgressPolicyV1, NetworkPolicyV1,
};
use tokio::io::DuplexStream;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio_util::sync::CancellationToken;

use crate::debug::GatewayDebugState;
use crate::dns::DnsAllowCache;
use crate::reserved_dns::ReservedDnsCancellation;
use crate::reserved_dns::test_support::RecordedResolver;
use crate::state::{
    GatewayNetworkState, NetworkStateReason, PolicyInvalidation, PolicyLoader, PulledNetworkPolicy,
};
use crate::stats::ProxyStats;
use crate::tcp_proxy::tracker::DnsAttributedTcpTracker;

use super::super::{FlowOutcome, TcpAdmissionDenial, TcpProxy};

#[tokio::test]
async fn reserved_destination_requires_ordinary_address_admission() {
    let resolver = RecordedResolver::new(Ipv4Addr::new(192, 0, 2, 10));
    let state = ready_state(policy(&[EgressDestinationV1::Domain(
        "allowed.example".parse().unwrap(),
    )]))
    .await;
    let policy = state.snapshot().await.policy.unwrap();
    let shutdown = CancellationToken::new();
    let proxy = proxy(state, &resolver, shutdown.clone());

    assert!(matches!(
        proxy.admit_reserved_dns_destination("1.1.1.1:53".parse().unwrap(), policy.as_ref()),
        Err(TcpAdmissionDenial::DnsAttribution(_))
    ));
    assert!(resolver.requests().is_empty());
}

#[tokio::test]
async fn every_reserved_tcp_frame_is_authorized_independently() {
    let resolver = RecordedResolver::new(Ipv4Addr::new(192, 0, 2, 10));
    let state = ready_state(policy(&[
        EgressDestinationV1::Ip("8.8.8.8".parse().unwrap()),
        EgressDestinationV1::Domain("allowed.example".parse().unwrap()),
    ]))
    .await;
    let policy = state.snapshot().await.policy.unwrap();
    let (mut guest, accepted) = tcp_pair();
    let shutdown = CancellationToken::new();
    let proxy = proxy(state, &resolver, shutdown.clone());
    let stats = Arc::clone(&proxy.stats);
    let serving_proxy = proxy.clone();
    let outcome = tokio::spawn(async move {
        super::serve(
            &serving_proxy,
            accepted,
            crate::reserved_dns::ReservedResolver::Google,
            policy.as_ref(),
            &shutdown,
        )
        .await
    });

    let denied = framed(&query("denied.example", RecordType::A));
    guest.write_all(&denied[..1]).await.unwrap();
    let allowed = framed(&query("ALLOWED.Example.", RecordType::A));
    let mut remainder = denied[1..].to_vec();
    remainder.extend_from_slice(&allowed);
    guest.write_all(&remainder).await.unwrap();

    let denied_reply = read_response(&mut guest).await;
    assert_eq!(denied_reply.code, ResponseCode::Refused);
    let allowed_reply = read_response(&mut guest).await;
    assert_eq!(allowed_reply.code, ResponseCode::NoError);
    drop(guest);
    let outcome = outcome.await.unwrap();

    assert!(matches!(
        outcome,
        FlowOutcome::HandledDns { messages: 2, .. }
    ));
    proxy.record("8.8.8.8:53".parse().unwrap(), None, outcome);
    assert_eq!(stats.tcp_total(), 1);
    assert_eq!(
        stats.network_tx_bytes(),
        u64::try_from(denied.len() + allowed.len()).unwrap()
    );
    assert_eq!(
        stats.network_rx_bytes(),
        denied_reply.wire_bytes + allowed_reply.wire_bytes
    );
    let requests = resolver.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].0, "8.8.8.8".parse::<IpAddr>().unwrap());
    assert_eq!(requests[0].1.name.to_ascii(), "allowed.example.");
}

#[tokio::test]
async fn oversized_opaque_tcp_is_closed_without_generic_relay() {
    let resolver = RecordedResolver::new(Ipv4Addr::new(192, 0, 2, 10));
    let state = ready_state(policy(&[EgressDestinationV1::Ip(
        "1.1.1.1".parse().unwrap(),
    )]))
    .await;
    let policy = state.snapshot().await.policy.unwrap();
    let (mut guest, accepted) = tcp_pair();
    let shutdown = CancellationToken::new();
    let proxy = proxy(state, &resolver, shutdown.clone());
    let outcome = tokio::spawn(async move {
        super::serve(
            &proxy,
            accepted,
            crate::reserved_dns::ReservedResolver::Cloudflare,
            policy.as_ref(),
            &shutdown,
        )
        .await
    });

    guest
        .write_all(b"\x16\x03\x03\x00\x10TLS marker")
        .await
        .unwrap();
    assert_closed(&mut guest).await;
    assert!(matches!(
        outcome.await.unwrap(),
        FlowOutcome::HandledDns { messages: 0, .. }
    ));
    assert!(resolver.requests().is_empty());
}

#[tokio::test]
async fn policy_install_cancels_an_incomplete_tcp_message() {
    let resolver = RecordedResolver::new(Ipv4Addr::new(192, 0, 2, 10));
    let state = ready_state(policy(&[
        EgressDestinationV1::Ip("1.1.1.1".parse().unwrap()),
        EgressDestinationV1::Domain("allowed.example".parse().unwrap()),
    ]))
    .await;
    let policy = state.snapshot().await.policy.unwrap();
    let (mut guest, accepted) = tcp_pair();
    let gateway_shutdown = CancellationToken::new();
    let proxy = proxy(state, &resolver, gateway_shutdown);
    let policy_cancellation = ReservedDnsCancellation::default();
    let operation = policy_cancellation.operation_token();
    let outcome = tokio::spawn(async move {
        super::serve(
            &proxy,
            accepted,
            crate::reserved_dns::ReservedResolver::Cloudflare,
            policy.as_ref(),
            &operation,
        )
        .await
    });

    guest.write_all(&[0]).await.unwrap();
    policy_cancellation.cancel_in_flight();

    assert!(matches!(outcome.await.unwrap(), FlowOutcome::Cancelled));
    assert!(resolver.requests().is_empty());
}

#[derive(Clone, Copy, Debug)]
enum ReadyInvalidation {
    NoIdentity,
    FailedRefresh,
}

#[tokio::test]
async fn ready_invalidation_cancels_open_reserved_tcp_connections() {
    for invalidation in [
        ReadyInvalidation::NoIdentity,
        ReadyInvalidation::FailedRefresh,
    ] {
        let resolver = RecordedResolver::new(Ipv4Addr::new(192, 0, 2, 10));
        let policy_cancellation = ReservedDnsCancellation::default();
        let invalidation_cancellation = policy_cancellation.clone();
        let state = GatewayNetworkState::pending_policy().with_policy_invalidation_hook(Arc::new(
            move |invalidation| {
                if invalidation == PolicyInvalidation::ReadyState {
                    invalidation_cancellation.cancel_in_flight();
                }
            },
        ));
        let state = install_ready(
            state,
            policy(&[
                EgressDestinationV1::Ip("1.1.1.1".parse().unwrap()),
                EgressDestinationV1::Domain("allowed.example".parse().unwrap()),
            ]),
        )
        .await;
        let policy = state.snapshot().await.policy.unwrap();
        let (mut guest, accepted) = tcp_pair();
        let proxy = proxy(state.clone(), &resolver, CancellationToken::new());
        let operation = policy_cancellation.operation_token();
        let outcome = tokio::spawn(async move {
            super::serve(
                &proxy,
                accepted,
                crate::reserved_dns::ReservedResolver::Cloudflare,
                policy.as_ref(),
                &operation,
            )
            .await
        });

        guest
            .write_all(&framed(&query("allowed.example", RecordType::A)))
            .await
            .unwrap();
        assert_eq!(read_response(&mut guest).await.code, ResponseCode::NoError);
        guest.write_all(&[0]).await.unwrap();

        match invalidation {
            ReadyInvalidation::NoIdentity => {
                state.set_no_identity(NetworkStateReason::NoIdentity).await;
            }
            ReadyInvalidation::FailedRefresh => {
                const SHA: &str =
                    "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
                let expected = EffectiveNetworkPolicyRef::local(
                    EffectiveNetworkPolicyRefScope::Sandbox,
                    "sandbox-1",
                    SHA,
                )
                .unwrap();
                let loader: PolicyLoader =
                    Arc::new(|| Box::pin(async { Err("unavailable".into()) }));
                let snapshot = state
                    .refresh(loader, Some(expected), NetworkStateReason::ControlRefresh)
                    .await;
                assert_eq!(snapshot.mode, crate::proto::NetworkState::PolicyError);
            }
        }

        assert!(
            matches!(outcome.await.unwrap(), FlowOutcome::Cancelled),
            "{invalidation:?}"
        );
    }
}

fn policy(allow: &[EgressDestinationV1]) -> NetworkPolicyV1 {
    NetworkPolicyV1 {
        version: Default::default(),
        tls_interception: Default::default(),
        tls_interception_exemptions: Vec::new(),
        network_credential_rules: vec![],
        egress: EgressPolicyV1::new(EgressDefaultAction::Deny, allow.to_vec(), Vec::new()).unwrap(),
    }
}

async fn ready_state(policy: NetworkPolicyV1) -> GatewayNetworkState {
    install_ready(GatewayNetworkState::pending_policy(), policy).await
}

async fn install_ready(state: GatewayNetworkState, policy: NetworkPolicyV1) -> GatewayNetworkState {
    const SHA: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let policy_ref =
        EffectiveNetworkPolicyRef::local(EffectiveNetworkPolicyRefScope::Sandbox, "sandbox-1", SHA)
            .unwrap();
    let installed_ref = policy_ref.to_string();
    let loader: PolicyLoader = Arc::new(move || {
        let policy = policy.clone();
        let installed_ref = installed_ref.clone();
        Box::pin(async move {
            Ok(PulledNetworkPolicy {
                policy,
                policy_ref: installed_ref,
                network_credentials: Default::default(),
            })
        })
    });
    let snapshot = state
        .refresh(loader, Some(policy_ref), NetworkStateReason::ControlRefresh)
        .await;
    assert_eq!(snapshot.mode, crate::proto::NetworkState::Ready);
    state
}

fn proxy(
    network_state: GatewayNetworkState,
    resolver: &RecordedResolver,
    shutdown_token: CancellationToken,
) -> TcpProxy {
    TcpProxy {
        sandbox_id: "test",
        stats: Arc::new(ProxyStats::new()),
        network_state,
        dns_allow_cache: DnsAllowCache::default(),
        dns_attributed_connections: DnsAttributedTcpTracker::default(),
        tls_interception: Default::default(),
        reserved_dns: resolver.core(),
        reserved_dns_cancellation: ReservedDnsCancellation::default(),
        known_egress_state: crate::known_egress::KnownEgressState::default(),
        debug_state: GatewayDebugState::default(),
        shutdown_token,
        refusal_warnings: Arc::default(),
        connect_failure_warnings: Arc::default(),
    }
}

fn tcp_pair() -> (DuplexStream, DuplexStream) {
    tokio::io::duplex(64 * 1024)
}

fn query(name: &str, record_type: RecordType) -> Vec<u8> {
    let absolute = if name.ends_with('.') {
        name.to_string()
    } else {
        format!("{name}.")
    };
    let mut message = Message::new(0x1234, MessageType::Query, OpCode::Query);
    message.queries.push(Query::query(
        Name::from_ascii(absolute).unwrap(),
        record_type,
    ));
    message.to_vec().unwrap()
}

fn framed(wire: &[u8]) -> Vec<u8> {
    let mut framed = u16::try_from(wire.len()).unwrap().to_be_bytes().to_vec();
    framed.extend_from_slice(wire);
    framed
}

struct Response {
    code: ResponseCode,
    wire_bytes: u64,
}

async fn read_response<T>(stream: &mut T) -> Response
where
    T: AsyncRead + Unpin,
{
    let length = usize::from(stream.read_u16().await.unwrap());
    let mut wire = vec![0_u8; length];
    stream.read_exact(&mut wire).await.unwrap();
    Response {
        code: Message::from_vec(&wire).unwrap().metadata.response_code,
        wire_bytes: u64::try_from(length + 2).unwrap(),
    }
}

async fn assert_closed(stream: &mut DuplexStream) {
    let mut byte = [0_u8; 1];
    match stream.read(&mut byte).await {
        Ok(0) => {}
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
            ) => {}
        result => panic!("expected closed connection, got {result:?}"),
    }
}
