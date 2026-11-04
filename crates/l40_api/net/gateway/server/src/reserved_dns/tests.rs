use crate::state::ReservedDnsCancellation;
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
        (
            query("_http._tcp.service.example", RecordType::SRV),
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
        let capacity_reply =
            core.udp_capacity_response("test", ReservedResolver::Cloudflare, &wire);
        for reply in [reply, capacity_reply] {
            let response = Message::from_vec(&reply).unwrap();
            assert_eq!(response.metadata.response_code, expected);
            if expected == ResponseCode::NotImp {
                // Resolvers match errors to the original question before handling the RCODE.
                let request = Message::from_vec(&wire).unwrap();
                assert_eq!(response.metadata.id, request.metadata.id);
                assert_eq!(response.queries, request.queries);
            } else {
                assert!(response.queries.is_empty());
            }
        }
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
    fn lookup<'a>(&'a self, _resolver: ReservedResolver, question: Query) -> ResolverFuture<'a> {
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

#[test]
fn exhausted_resolver_slots_do_not_block_the_other_resolver() {
    let slots = ResolverSlots::new(1);
    let _google = slots.get(ReservedResolver::Google).try_acquire().unwrap();
    let _cloudflare = slots
        .get(ReservedResolver::Cloudflare)
        .try_acquire()
        .unwrap();
}

/// Blocks every lookup until `release_all`, counting upstream lookups and
/// tracking how many are still alive (not yet completed or dropped).
struct GatedResolver {
    calls: Arc<std::sync::atomic::AtomicUsize>,
    alive: Arc<std::sync::atomic::AtomicUsize>,
    started: Arc<Notify>,
    gate: Arc<Semaphore>,
}

impl GatedResolver {
    fn new() -> Self {
        Self {
            calls: Arc::default(),
            alive: Arc::default(),
            started: Arc::new(Notify::new()),
            gate: Arc::new(Semaphore::new(0)),
        }
    }

    fn calls(&self) -> usize {
        self.calls.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn alive(&self) -> usize {
        self.alive.load(std::sync::atomic::Ordering::SeqCst)
    }

    async fn wait_started(&self) {
        self.started.notified().await;
    }

    fn release_all(&self) {
        self.gate.close();
    }
}

struct AliveGuard(Arc<std::sync::atomic::AtomicUsize>);

impl Drop for AliveGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}

impl ResolverBackend for GatedResolver {
    fn lookup<'a>(&'a self, _resolver: ReservedResolver, question: Query) -> ResolverFuture<'a> {
        Box::pin(async move {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.alive.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let _alive = AliveGuard(self.alive.clone());
            self.started.notify_one();
            // Closed => released; acquire fails immediately for every waiter.
            let _ = self.gate.acquire().await;
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

fn spawn_resolve(
    core: &ReservedDnsCore,
    resolver: ReservedResolver,
    name: &str,
    policy: NetworkPolicyV1,
    cancellation: CancellationToken,
) -> tokio::task::JoinHandle<ResolveOutcome> {
    let core = core.clone();
    let wire = query(name, RecordType::A);
    tokio::spawn(async move {
        core.resolve("test", resolver, &wire, &policy, &cancellation)
            .await
    })
}

fn expect_reply(outcome: ResolveOutcome) -> Vec<u8> {
    match outcome {
        ResolveOutcome::Reply(reply) => reply,
        ResolveOutcome::Cancelled => panic!("resolution unexpectedly cancelled"),
    }
}

/// Let spawned resolves reach (and register with) the shared lookup.
async fn settle() {
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
}

#[tokio::test]
async fn concurrent_identical_queries_share_one_upstream_lookup_and_slot() {
    let backend = Arc::new(GatedResolver::new());
    // One lookup slot: uncoalesced, every query after the first would fail
    // with lookup capacity.
    let core = core_with_backend(backend.clone(), 1);
    let policy = NetworkPolicyV1::allow_all();

    // Mixed spellings of one name: the coalescing key is the canonical
    // (lowercased) question, so these must all share one lookup.
    let spellings = ["example.com", "Example.COM", "EXAMPLE.com", "eXaMpLe.CoM"];
    let tasks: Vec<_> = (0..8)
        .map(|i| {
            spawn_resolve(
                &core,
                ReservedResolver::Google,
                spellings[i % spellings.len()],
                policy.clone(),
                CancellationToken::new(),
            )
        })
        .collect();
    backend.wait_started().await;
    settle().await;
    assert_eq!(backend.calls(), 1);

    backend.release_all();
    for task in tasks {
        let reply = expect_reply(task.await.unwrap());
        let message = Message::from_vec(&reply).unwrap();
        assert_eq!(message.metadata.response_code, ResponseCode::NoError);
        assert_eq!(message.answers.len(), 1);
    }
    assert_eq!(backend.calls(), 1);
    assert_eq!(core.inflight.len(), 0);
    assert_eq!(
        core.lookup_slots
            .get(ReservedResolver::Google)
            .available_permits(),
        1
    );
}

#[tokio::test]
async fn coalescing_is_keyed_by_resolver_and_question() {
    let backend = Arc::new(GatedResolver::new());
    let core = core_with_backend(backend.clone(), 8);
    let policy = NetworkPolicyV1::allow_all();

    let tasks = [
        spawn_resolve(
            &core,
            ReservedResolver::Google,
            "a.example",
            policy.clone(),
            CancellationToken::new(),
        ),
        spawn_resolve(
            &core,
            ReservedResolver::Cloudflare,
            "a.example",
            policy.clone(),
            CancellationToken::new(),
        ),
        spawn_resolve(
            &core,
            ReservedResolver::Google,
            "b.example",
            policy.clone(),
            CancellationToken::new(),
        ),
        spawn_resolve(
            &core,
            ReservedResolver::Google,
            "a.example",
            policy.clone(),
            CancellationToken::new(),
        ),
    ];
    settle().await;
    assert_eq!(backend.calls(), 3);

    backend.release_all();
    for task in tasks {
        expect_reply(task.await.unwrap());
    }
    assert_eq!(core.inflight.len(), 0);
}

#[tokio::test]
async fn cancelling_one_waiter_does_not_cancel_the_shared_lookup() {
    let backend = Arc::new(GatedResolver::new());
    let core = core_with_backend(backend.clone(), 4);
    let policy = NetworkPolicyV1::allow_all();

    let cancelled = CancellationToken::new();
    let first = spawn_resolve(
        &core,
        ReservedResolver::Google,
        "example.com",
        policy.clone(),
        cancelled.clone(),
    );
    backend.wait_started().await;
    let second = spawn_resolve(
        &core,
        ReservedResolver::Google,
        "example.com",
        policy,
        CancellationToken::new(),
    );
    settle().await;

    cancelled.cancel();
    assert!(matches!(first.await.unwrap(), ResolveOutcome::Cancelled));
    assert_eq!(backend.alive(), 1);

    backend.release_all();
    let reply = expect_reply(second.await.unwrap());
    assert_eq!(response_code(&reply), ResponseCode::NoError);
    assert_eq!(backend.calls(), 1);
}

#[tokio::test]
async fn abandoned_lookup_is_dropped_and_releases_its_slot() {
    let backend = Arc::new(GatedResolver::new());
    let core = core_with_backend(backend.clone(), 1);
    let policy = NetworkPolicyV1::allow_all();

    let cancellation = CancellationToken::new();
    let waiters: Vec<_> = (0..3)
        .map(|_| {
            spawn_resolve(
                &core,
                ReservedResolver::Google,
                "example.com",
                policy.clone(),
                cancellation.clone(),
            )
        })
        .collect();
    backend.wait_started().await;
    settle().await;
    cancellation.cancel();
    for waiter in waiters {
        assert!(matches!(waiter.await.unwrap(), ResolveOutcome::Cancelled));
    }
    assert_eq!(backend.alive(), 0);
    assert_eq!(core.inflight.len(), 0);
    assert_eq!(
        core.lookup_slots
            .get(ReservedResolver::Google)
            .available_permits(),
        1
    );

    // A later query starts a fresh lookup instead of joining the dead one.
    backend.release_all();
    let reply = expect_reply(
        spawn_resolve(
            &core,
            ReservedResolver::Google,
            "example.com",
            policy,
            CancellationToken::new(),
        )
        .await
        .unwrap(),
    );
    assert_eq!(response_code(&reply), ResponseCode::NoError);
    assert_eq!(backend.calls(), 2);
}

#[tokio::test]
async fn shared_answer_is_sanitized_against_each_waiters_policy() {
    let backend = Arc::new(GatedResolver::new());
    let core = core_with_backend(backend.clone(), 4);
    let allowing = NetworkPolicyV1::allow_all();
    let denying = policy(
        EgressDefaultAction::Allow,
        &[],
        &[destination("192.0.2.0/24")],
    );

    let allowed = spawn_resolve(
        &core,
        ReservedResolver::Google,
        "example.com",
        allowing,
        CancellationToken::new(),
    );
    backend.wait_started().await;
    let denied = spawn_resolve(
        &core,
        ReservedResolver::Google,
        "example.com",
        denying,
        CancellationToken::new(),
    );
    settle().await;
    backend.release_all();

    let allowed = Message::from_vec(&expect_reply(allowed.await.unwrap())).unwrap();
    assert_eq!(allowed.metadata.response_code, ResponseCode::NoError);
    assert_eq!(allowed.answers.len(), 1);
    let denied = expect_reply(denied.await.unwrap());
    assert_eq!(response_code(&denied), ResponseCode::ServFail);
    assert_eq!(backend.calls(), 1);
}
