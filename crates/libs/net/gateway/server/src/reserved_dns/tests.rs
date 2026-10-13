use std::sync::Arc;

use hickory_resolver::proto::rr::rdata::A;
use pplx_space_net_common::{
    EgressDefaultAction, EgressDestinationV1, EgressPolicyV1, NetworkPolicyVersion,
};
use tokio::sync::Notify;

use super::*;
use crate::reserved_dns::test_support::RecordedResolver;

fn core_with_backend(resolver: Arc<dyn ResolverBackend>, capacity: usize) -> ReservedDnsCore {
    ReservedDnsCore::with_resolver(resolver, capacity)
}

#[test]
fn resolver_tcp_socket_is_marked_before_it_is_returned() {
    let observed_mark = std::cell::Cell::new(None);
    let socket = apply_outbound_mark(
        42_u8,
        Some(pplx_space_net_gateway_client::OUTBOUND_MARK),
        |_socket, mark| {
            observed_mark.set(Some(mark));
            Ok(())
        },
    )
    .unwrap();

    assert_eq!(
        observed_mark.get(),
        Some(pplx_space_net_gateway_client::OUTBOUND_MARK)
    );
    assert_eq!(socket, 42);
}

#[test]
fn classifier_uses_only_reserved_ip() {
    let resolver = Arc::new(RecordedResolver::new(Ipv4Addr::new(192, 0, 2, 1)));
    let core = resolver.core();
    assert_eq!(
        core.resolver_for("1.1.1.1".parse().unwrap()),
        Some(ReservedResolver::Cloudflare)
    );
    assert_eq!(
        core.resolver_for("8.8.8.8".parse().unwrap()),
        Some(ReservedResolver::Google)
    );
    assert!(core.resolver_for("9.9.9.9".parse().unwrap()).is_none());
}

#[test]
fn policy_install_cancels_only_operations_from_the_previous_policy() {
    let cancellation = ReservedDnsCancellation::default();
    let previous = cancellation.operation_token();

    cancellation.cancel_in_flight();

    assert!(previous.is_cancelled());
    assert!(!cancellation.operation_token().is_cancelled());
}

#[tokio::test]
async fn denied_qname_never_reaches_resolver_even_when_destination_is_allowed() {
    let resolver = Arc::new(RecordedResolver::new(Ipv4Addr::new(192, 0, 2, 1)));
    let policy = policy(
        EgressDefaultAction::Deny,
        &[destination("8.8.8.8"), destination("allowed.example")],
        &[],
    );
    let core = resolver.core();

    let reply = resolve_wire(
        &core,
        "8.8.8.8",
        &query("denied.example", RecordType::A),
        &policy,
    )
    .await;

    assert_eq!(response_code(&reply), ResponseCode::Refused);
    assert!(resolver.requests().is_empty());
}

#[tokio::test]
async fn capacity_replies_preserve_response_codes() {
    let resolver = Arc::new(RecordedResolver::new(Ipv4Addr::new(192, 0, 2, 1)));
    let policy = NetworkPolicyV1::allow_all();
    let core = core_with_backend(resolver.clone(), 0);
    for (record_type, code) in [
        (RecordType::A, ResponseCode::ServFail),
        (RecordType::AAAA, ResponseCode::NoError),
    ] {
        let wire = query("allowed.example", record_type);
        let reply = resolve_wire(&core, "1.1.1.1", &wire, &policy).await;
        assert_eq!(response_code(&reply), code);
        let fallback = core.udp_capacity_response("test", ReservedResolver::Cloudflare, &wire);
        assert_eq!(response_code(&fallback), ResponseCode::ServFail);
    }
    assert!(resolver.requests().is_empty());
}

#[tokio::test]
async fn aaaa_queries_are_policy_gated_local_nodata() {
    let resolver = Arc::new(RecordedResolver::new(Ipv4Addr::new(192, 0, 2, 1)));
    let policy = policy(
        EgressDefaultAction::Deny,
        &[destination("1.1.1.1"), destination("allowed.example")],
        &[],
    );
    let core = resolver.core();

    let allowed = resolve_wire(
        &core,
        "1.1.1.1",
        &query("ALLOWED.Example", RecordType::AAAA),
        &policy,
    )
    .await;
    let allowed = Message::from_vec(&allowed).unwrap();
    assert_eq!(allowed.metadata.response_code, ResponseCode::NoError);
    assert_eq!(
        allowed.queries,
        vec![Query::query(
            Name::from_ascii("allowed.example.").unwrap(),
            RecordType::AAAA,
        )]
    );
    assert!(allowed.answers.is_empty());

    let denied = resolve_wire(
        &core,
        "1.1.1.1",
        &query("denied.example", RecordType::AAAA),
        &policy,
    )
    .await;
    assert_eq!(response_code(&denied), ResponseCode::Refused);
    assert!(resolver.requests().is_empty());
}

#[tokio::test]
async fn allowed_query_reuses_resolver_ip_but_not_guest_fields() {
    let resolver = Arc::new(RecordedResolver::new(Ipv4Addr::new(192, 0, 2, 1)));
    let policy = policy(
        EgressDefaultAction::Deny,
        &[
            destination("1.1.1.1"),
            destination("allowed.example"),
            destination("xn--bcher-kva.example"),
        ],
        &[],
    );
    let core = resolver.core();
    for (guest_name, policy_domain, clean_query) in [
        ("allowed.example", "allowed.example", "allowed.example."),
        ("ALLOWED.Example", "allowed.example", "allowed.example."),
        ("allowed.example.", "allowed.example", "allowed.example."),
        (
            "XN--BCHER-KVA.Example.",
            "xn--bcher-kva.example",
            "xn--bcher-kva.example.",
        ),
    ] {
        let mut message = guest_message(guest_name, RecordType::A);
        message.metadata.authentic_data = true;
        message.metadata.checking_disabled = true;
        message.answers.push(Record::from_rdata(
            Name::from_ascii("marker.attacker.").unwrap(),
            60,
            RData::A(A::new(203, 0, 113, 9)),
        ));
        message.additionals.push(Record::from_rdata(
            Name::from_ascii("additional-marker.attacker.").unwrap(),
            60,
            RData::A(A::new(203, 0, 113, 10)),
        ));
        let guest_id = message.metadata.id;
        let guest_wire = message.to_vec().unwrap();

        let parsed = parse_guest_request(&guest_wire).unwrap();
        assert_eq!(
            parsed.domain,
            policy_domain.parse::<DomainName>().unwrap(),
            "wrong policy name for {guest_name}",
        );

        let request_index = resolver.requests().len();
        let reply = resolve_wire(&core, "1.1.1.1", &guest_wire, &policy).await;
        let requests = resolver.requests();
        assert_eq!(requests.len(), request_index + 1, "{guest_name}");
        assert_eq!(
            requests[request_index],
            (
                "1.1.1.1".parse::<IpAddr>().unwrap(),
                Query::query(Name::from_ascii(clean_query).unwrap(), RecordType::A),
            ),
            "wrong clean Hickory query for {guest_name}",
        );
        assert_eq!(response_id(&reply), guest_id, "{guest_name}");
    }
}

#[tokio::test]
async fn invalid_requests_fail_before_hickory_lookup() {
    let resolver = Arc::new(RecordedResolver::new(Ipv4Addr::new(192, 0, 2, 1)));
    let policy = NetworkPolicyV1::allow_all();
    let core = resolver.core();
    let mut non_in = guest_message("allowed.example", RecordType::A);
    non_in.queries[0].query_class = DNSClass::CH;
    let mut non_in_aaaa = non_in.clone();
    non_in_aaaa.queries[0].query_type = RecordType::AAAA;
    let mut response = guest_message("allowed.example", RecordType::AAAA);
    response.metadata.message_type = MessageType::Response;
    let mut multiple = guest_message("allowed.example", RecordType::A);
    multiple.queries.push(Query::query(
        Name::from_ascii("denied.example.").unwrap(),
        RecordType::A,
    ));
    let mut trailing = query("allowed.example", RecordType::A);
    trailing.extend_from_slice(b"attacker-marker.example");

    for (wire, expected) in [
        (
            query("allowed.example", RecordType::TXT),
            ResponseCode::NotImp,
        ),
        (non_in.to_vec().unwrap(), ResponseCode::NotImp),
        (non_in_aaaa.to_vec().unwrap(), ResponseCode::NotImp),
        (response.to_vec().unwrap(), ResponseCode::FormErr),
        (multiple.to_vec().unwrap(), ResponseCode::FormErr),
        (trailing, ResponseCode::FormErr),
        (
            raw_query(&[&[b'a'; 63], &[b'b'; 63], &[b'c'; 63], &[b'd'; 63]]),
            ResponseCode::FormErr,
        ),
    ] {
        let reply = resolve_wire(&core, "1.1.1.1", &wire, &policy).await;
        assert_eq!(response_code(&reply), expected);
        assert_eq!(
            response_code(&core.udp_capacity_response("test", ReservedResolver::Cloudflare, &wire)),
            expected
        );
    }
    assert!(resolver.requests().is_empty());
}

#[tokio::test]
async fn malformed_escaped_and_non_ascii_names_never_reach_hickory() {
    let resolver = Arc::new(RecordedResolver::new(Ipv4Addr::new(192, 0, 2, 1)));
    let policy = NetworkPolicyV1::allow_all();
    let core = resolver.core();

    for (kind, wire) in [
        ("malformed", self_pointer_query()),
        ("escaped", raw_query(&[b"a.b", b"example"])),
        ("non-ascii", raw_query(&[&[0xff, 0xfe], b"example"])),
    ] {
        let reply = resolve_wire(&core, "1.1.1.1", &wire, &policy).await;
        assert_eq!(response_code(&reply), ResponseCode::FormErr, "{kind}");
        assert!(resolver.requests().is_empty(), "{kind} reached Hickory");
    }
}

#[tokio::test]
async fn compressed_qname_is_canonicalized_before_the_clean_lookup() {
    let resolver = Arc::new(RecordedResolver::new(Ipv4Addr::new(192, 0, 2, 1)));
    let policy = policy(
        EgressDefaultAction::Deny,
        &[destination("8.8.8.8"), destination("a.b")],
        &[],
    );
    let core = resolver.core();

    let reply = resolve_wire(&core, "8.8.8.8", &header_pointer_query(), &policy).await;

    assert_eq!(response_code(&reply), ResponseCode::NoError);
    let requests = resolver.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].1.name.to_ascii(), "a.b.");
    assert_eq!(requests[0].1.query_type, RecordType::A);
}

#[tokio::test]
async fn response_keeps_only_reachable_allowed_addresses_and_clamps_ttl() {
    let resolver = Arc::new(RecordedResolver::with_answer(|question| ResolverAnswer {
        question: question.clone(),
        code: ResponseCode::NoError,
        answers: vec![
            Record::from_rdata(
                question.name.clone(),
                600,
                RData::CNAME(CNAME(Name::from_ascii("edge.example.").unwrap())),
            ),
            Record::from_rdata(
                Name::from_ascii("edge.example.").unwrap(),
                600,
                RData::A(A::new(192, 0, 2, 10)),
            ),
            Record::from_rdata(
                Name::from_ascii("unrelated.example.").unwrap(),
                60,
                RData::A(A::new(203, 0, 113, 10)),
            ),
        ],
        valid_until: Instant::now() + Duration::from_secs(600),
    }));
    let admitted_policy = policy(
        EgressDefaultAction::Deny,
        &[destination("8.8.8.8"), destination("allowed.example")],
        &[destination("203.0.113.0/24")],
    );
    let core = resolver.core();

    let reply = resolve_wire(
        &core,
        "8.8.8.8",
        &query("allowed.example", RecordType::A),
        &admitted_policy,
    )
    .await;
    let message = Message::from_vec(&reply).unwrap();
    assert_eq!(message.metadata.response_code, ResponseCode::NoError);
    assert_eq!(message.answers.len(), 2);
    assert!(message.answers.iter().all(|answer| answer.ttl <= 300));
    assert!(message.answers.iter().all(|answer| {
        !matches!(answer.data, RData::A(address) if address.0 == Ipv4Addr::new(203, 0, 113, 10))
    }));
    let denied_addresses = policy(
        EgressDefaultAction::Allow,
        &[],
        &[destination("192.0.2.0/24")],
    );
    let rejected = resolve_wire(
        &core,
        "8.8.8.8",
        &query("allowed.example", RecordType::A),
        &denied_addresses,
    )
    .await;
    assert_eq!(response_code(&rejected), ResponseCode::ServFail);
}

#[tokio::test]
async fn private_answers_for_allowed_names_need_an_explicit_address_allow() {
    // Whoever controls an allowed zone can point it at a VPC, node
    // or pod address. Those answers must not reach the guest unless an
    // explicit address rule (or an open default) already permits them.
    let resolver = Arc::new(RecordedResolver::with_answer(|question| ResolverAnswer {
        question: question.clone(),
        code: ResponseCode::NoError,
        answers: vec![
            Record::from_rdata(question.name.clone(), 60, RData::A(A::new(10, 0, 3, 7))),
            Record::from_rdata(question.name.clone(), 60, RData::A(A::new(100, 64, 0, 9))),
            Record::from_rdata(question.name.clone(), 60, RData::A(A::new(192, 0, 2, 10))),
        ],
        valid_until: Instant::now() + Duration::from_secs(60),
    }));
    let core = resolver.core();
    let a_answers = |wire: &[u8]| -> Vec<Ipv4Addr> {
        Message::from_vec(wire)
            .unwrap()
            .answers
            .iter()
            .filter_map(|answer| match answer.data {
                RData::A(address) => Some(address.0),
                _ => None,
            })
            .collect()
    };

    let closed = policy(
        EgressDefaultAction::Deny,
        &[destination("8.8.8.8"), destination("allowed.example")],
        &[],
    );
    let reply = resolve_wire(
        &core,
        "8.8.8.8",
        &query("allowed.example", RecordType::A),
        &closed,
    )
    .await;
    assert_eq!(response_code(&reply), ResponseCode::NoError);
    assert_eq!(a_answers(&reply), vec![Ipv4Addr::new(192, 0, 2, 10)]);

    let opted_in = policy(
        EgressDefaultAction::Deny,
        &[
            destination("8.8.8.8"),
            destination("allowed.example"),
            destination("10.0.3.0/24"),
        ],
        &[],
    );
    let reply = resolve_wire(
        &core,
        "8.8.8.8",
        &query("allowed.example", RecordType::A),
        &opted_in,
    )
    .await;
    assert_eq!(
        a_answers(&reply),
        vec![Ipv4Addr::new(10, 0, 3, 7), Ipv4Addr::new(192, 0, 2, 10)]
    );

    let open = policy(EgressDefaultAction::Allow, &[], &[]);
    let reply = resolve_wire(
        &core,
        "8.8.8.8",
        &query("allowed.example", RecordType::A),
        &open,
    )
    .await;
    assert_eq!(a_answers(&reply).len(), 3);
}

#[tokio::test]
async fn negative_answers_and_sanitization_failures_preserve_response_codes() {
    let empty = ResolverAnswer {
        question: Query::query(Name::from_ascii("allowed.example.").unwrap(), RecordType::A),
        code: ResponseCode::NoError,
        answers: Vec::new(),
        valid_until: Instant::now() + MAX_NEGATIVE_TTL,
    };
    let cname = |owner: &str, target: &str| {
        Record::from_rdata(
            Name::from_ascii(owner).unwrap(),
            60,
            RData::CNAME(CNAME(Name::from_ascii(target).unwrap())),
        )
    };
    let long_chain = (0..9)
        .map(|hop| {
            cname(
                &if hop == 0 {
                    "allowed.example.".to_owned()
                } else {
                    format!("hop{hop}.example.")
                },
                &format!("hop{}.example.", hop + 1),
            )
        })
        .collect();
    for (answer, expected) in [
        (empty.clone(), ResponseCode::NoError),
        (
            ResolverAnswer {
                code: ResponseCode::NXDomain,
                ..empty.clone()
            },
            ResponseCode::NXDomain,
        ),
        (
            ResolverAnswer {
                question: Query::query(Name::from_ascii("wrong.example.").unwrap(), RecordType::A),
                ..empty.clone()
            },
            ResponseCode::ServFail,
        ),
        (
            ResolverAnswer {
                code: ResponseCode::ServFail,
                ..empty.clone()
            },
            ResponseCode::ServFail,
        ),
        (
            ResolverAnswer {
                answers: vec![
                    cname("allowed.example.", "one.example."),
                    cname("allowed.example.", "two.example."),
                ],
                ..empty.clone()
            },
            ResponseCode::ServFail,
        ),
        (
            ResolverAnswer {
                answers: vec![
                    cname("allowed.example.", "one.example."),
                    cname("one.example.", "allowed.example."),
                ],
                ..empty.clone()
            },
            ResponseCode::ServFail,
        ),
        (
            ResolverAnswer {
                answers: long_chain,
                ..empty
            },
            ResponseCode::ServFail,
        ),
    ] {
        let resolver = RecordedResolver::with_answer(move |_| answer.clone());
        let reply = resolve_wire(
            &resolver.core(),
            "8.8.8.8",
            &query("allowed.example", RecordType::A),
            &NetworkPolicyV1::allow_all(),
        )
        .await;
        assert_eq!(response_code(&reply), expected);
    }
}

#[tokio::test]
async fn cancellation_while_lookup_is_pending_discards_answer() {
    let resolver = Arc::new(BlockingResolver::new());
    let policy = policy(
        EgressDefaultAction::Deny,
        &[destination("1.1.1.1"), destination("allowed.example")],
        &[],
    );
    let core = core_with_backend(resolver.clone(), 4);
    let cancellation = CancellationToken::new();
    let task_cancellation = cancellation.clone();
    let task_core = core.clone();
    let task = tokio::spawn(async move {
        task_core
            .resolve(
                "test",
                ReservedResolver::Cloudflare,
                &query("allowed.example", RecordType::A),
                &policy,
                &task_cancellation,
            )
            .await
    });
    resolver.wait_started().await;

    cancellation.cancel();
    resolver.release();
    assert!(matches!(task.await.unwrap(), ResolveOutcome::Cancelled));
}

async fn resolve_wire(
    core: &ReservedDnsCore,
    destination: &str,
    request: &[u8],
    policy: &NetworkPolicyV1,
) -> Vec<u8> {
    let resolver = core
        .resolver_for(destination.parse().unwrap())
        .expect("test destination must be a reserved resolver");
    match core
        .resolve("test", resolver, request, policy, &CancellationToken::new())
        .await
    {
        ResolveOutcome::Reply(reply) => reply,
        ResolveOutcome::Cancelled => panic!("resolution unexpectedly cancelled"),
    }
}

fn query(name: &str, record_type: RecordType) -> Vec<u8> {
    guest_message(name, record_type).to_vec().unwrap()
}

fn guest_message(name: &str, record_type: RecordType) -> Message {
    let absolute = if name.ends_with('.') {
        name.to_string()
    } else {
        format!("{name}.")
    };
    let mut message = Message::new(0x1234, MessageType::Query, OpCode::Query);
    message.metadata.recursion_desired = true;
    message.queries.push(Query::query(
        Name::from_ascii(absolute).unwrap(),
        record_type,
    ));
    message
}

fn raw_query(labels: &[&[u8]]) -> Vec<u8> {
    let mut wire = vec![
        0x12, 0x34, 0x01, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    ];
    for label in labels {
        wire.push(u8::try_from(label.len()).unwrap());
        wire.extend_from_slice(label);
    }
    wire.extend_from_slice(&[0, 0, 1, 0, 1]);
    wire
}

fn self_pointer_query() -> Vec<u8> {
    let mut wire = vec![
        0x12, 0x34, 0x01, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    ];
    wire.extend_from_slice(&[0xc0, 0x0c, 0, 1, 0, 1]);
    wire
}

/// The ID/flags bytes also spell `a.b.`. The guest question is a compression
/// pointer to that header offset; none of those guest fields survive the typed
/// Query passed to Hickory's resolver.
fn header_pointer_query() -> Vec<u8> {
    vec![
        0x01, b'a', 0x01, b'b', 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xc0, 0x00, 0x00,
        0x01, 0x00, 0x01,
    ]
}

fn response_code(wire: &[u8]) -> ResponseCode {
    Message::from_vec(wire).unwrap().metadata.response_code
}

fn response_id(wire: &[u8]) -> u16 {
    Message::from_vec(wire).unwrap().metadata.id
}

fn destination(value: &str) -> EgressDestinationV1 {
    if let Ok(ip) = value.parse::<IpAddr>() {
        EgressDestinationV1::Ip(ip)
    } else if value.contains('/') {
        EgressDestinationV1::Cidr(value.parse().unwrap())
    } else {
        EgressDestinationV1::Domain(value.parse().unwrap())
    }
}

fn policy(
    default_action: EgressDefaultAction,
    allow: &[EgressDestinationV1],
    deny: &[EgressDestinationV1],
) -> NetworkPolicyV1 {
    NetworkPolicyV1 {
        version: NetworkPolicyVersion::V1,
        tls_interception: Default::default(),
        tls_interception_exemptions: Vec::new(),
        network_credential_rules: vec![],
        egress: EgressPolicyV1::new(default_action, allow.to_vec(), deny.to_vec()).unwrap(),
    }
}

struct BlockingResolver {
    started: Arc<Notify>,
    released: Arc<Notify>,
}

impl BlockingResolver {
    fn new() -> Self {
        Self {
            started: Arc::new(Notify::new()),
            released: Arc::new(Notify::new()),
        }
    }

    async fn wait_started(&self) {
        self.started.notified().await;
    }

    fn release(&self) {
        self.released.notify_one();
    }
}

impl ResolverBackend for BlockingResolver {
    fn lookup<'a>(
        &'a self,
        _resolver: ReservedResolver,
        question: Query,
        _shutdown: &'a CancellationToken,
    ) -> ResolverFuture<'a> {
        Box::pin(async move {
            self.started.notify_one();
            self.released.notified().await;
            Ok(ResolverAnswer {
                question: question.clone(),
                code: ResponseCode::NoError,
                answers: vec![Record::from_rdata(
                    question.name.clone(),
                    60,
                    RData::A(A::new(192, 0, 2, 1)),
                )],
                valid_until: Instant::now() + Duration::from_secs(60),
            })
        })
    }
}
