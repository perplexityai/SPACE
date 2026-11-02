//! Policy-gated classic DNS for the two resolver addresses reserved to guests.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::future::Future;
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use hickory_resolver::config::{CLOUDFLARE, GOOGLE, ResolveHosts, ResolverConfig, ResolverOpts};
use hickory_resolver::net::runtime::iocompat::AsyncIoTokioAsStd;
use hickory_resolver::net::runtime::{RuntimeProvider, TokioHandle, TokioTime};
use hickory_resolver::net::{DnsError, NetError};
use hickory_resolver::proto::op::{Header, Message, MessageType, OpCode, Query, ResponseCode};
use hickory_resolver::proto::rr::rdata::CNAME;
use hickory_resolver::proto::rr::{DNSClass, Name, RData, Record, RecordType};
use hickory_resolver::proto::serialize::binary::{BinDecodable, BinDecoder};
use pplx_space_net_common::{DomainName, EgressAddressMatch, NetworkPolicyV1};
use tokio::net::{TcpSocket, TcpStream, UdpSocket};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

mod inflight;
mod metrics;
mod recovery;
mod upstream;

use crate::egress::allows_dns_query as domain_is_allowed;
use inflight::{InflightLookups, LookupRole};
use recovery::RecoveringResolver;

pub(crate) const MAX_DNS_MESSAGE_BYTES: usize = 4 * 1024;
const MAX_INPUT_RECORDS: usize = 64;
const MAX_RESPONSE_RECORDS: usize = 64;
const MAX_CNAME_HOPS: usize = 8;
const MAX_CONCURRENT_LOOKUPS: usize =
    crate::resource_budget::MAX_ACTIVE_RESERVED_DNS_DATAGRAMS_PER_RESOLVER;
const MAX_POSITIVE_TTL: u32 = 300;
const MAX_NEGATIVE_TTL: Duration = Duration::from_secs(5);
const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(4);
const TOTAL_LOOKUP_TIMEOUT: Duration = Duration::from_secs(8);
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const CACHE_ENTRIES_PER_RESOLVER: u64 = 2_048;

const CLOUDFLARE_DNS: Ipv4Addr = Ipv4Addr::new(1, 1, 1, 1);
const GOOGLE_DNS: Ipv4Addr = Ipv4Addr::new(8, 8, 8, 8);

#[derive(Debug, thiserror::Error)]
pub enum BuildReservedDnsCoreError {
    #[error("failed to build the Hickory reserved-DNS resolver")]
    Resolver(#[source] NetError),
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ReservedResolver {
    Cloudflare,
    Google,
}

impl ReservedResolver {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Cloudflare => "cloudflare",
            Self::Google => "google",
        }
    }

    fn from_ip(destination: IpAddr) -> Option<Self> {
        match destination {
            IpAddr::V4(CLOUDFLARE_DNS) => Some(Self::Cloudflare),
            IpAddr::V4(GOOGLE_DNS) => Some(Self::Google),
            IpAddr::V4(_) | IpAddr::V6(_) => None,
        }
    }
}

/// One admission semaphore per reserved resolver, so a stalled upstream can
/// only exhaust its own slots and guests can still fall back to the other.
#[derive(Clone, Debug)]
pub(crate) struct ResolverSlots {
    cloudflare: Arc<Semaphore>,
    google: Arc<Semaphore>,
}

impl ResolverSlots {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            cloudflare: Arc::new(Semaphore::new(capacity)),
            google: Arc::new(Semaphore::new(capacity)),
        }
    }

    pub(crate) fn get(&self, resolver: ReservedResolver) -> &Arc<Semaphore> {
        match resolver {
            ReservedResolver::Cloudflare => &self.cloudflare,
            ReservedResolver::Google => &self.google,
        }
    }
}

#[derive(Debug)]
pub enum ResolveOutcome {
    Reply(Vec<u8>),
    Cancelled,
}

/// The transport-neutral DNS policy and Hickory resolution core.
#[derive(Clone)]
pub struct ReservedDnsCore {
    resolver: Arc<dyn ResolverBackend>,
    lookup_slots: ResolverSlots,
    /// Concurrent identical questions share one upstream lookup.
    inflight: Arc<InflightLookups>,
}

impl fmt::Debug for ReservedDnsCore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("ReservedDnsCore").finish()
    }
}

impl ReservedDnsCore {
    pub fn new() -> Result<Self, BuildReservedDnsCoreError> {
        let lookup_slots = ResolverSlots::new(MAX_CONCURRENT_LOOKUPS);
        Ok(Self {
            resolver: Arc::new(HickoryResolverSet::new(&lookup_slots)?),
            lookup_slots,
            inflight: Arc::default(),
        })
    }

    #[cfg(test)]
    fn with_resolver(resolver: Arc<dyn ResolverBackend>, lookup_capacity: usize) -> Self {
        Self {
            resolver,
            lookup_slots: ResolverSlots::new(lookup_capacity),
            inflight: Arc::default(),
        }
    }

    async fn resolve_a(
        &self,
        resolver: ReservedResolver,
        request: &GuestRequest,
        admitted_policy: &NetworkPolicyV1,
        shutdown: &CancellationToken,
    ) -> Result<SanitizedAnswer, ResolverFailure> {
        // Identical concurrent questions to the same reserved resolver share
        // one upstream lookup (and one lookup slot). The shared answer is
        // still sanitized against this request's own admitted policy below.
        let (lookup, role) = self.inflight.join_or_start(
            &self.resolver,
            self.lookup_slots.get(resolver),
            resolver,
            &request.question,
        );
        // Leaders are counted inside the shared lookup once it holds its slot.
        if role == LookupRole::Follower {
            pplx_space_metrics::gateway::reserved_dns_lookup_coalesce(
                resolver.as_str(),
                role.as_str(),
            );
        }
        // Policy cancellation detaches only this waiter; the shared lookup
        // keeps running for any other waiter and is dropped with the last one.
        let answer = tokio::select! {
            biased;
            () = shutdown.cancelled() => return Err(ResolverFailure::Cancelled),
            answer = lookup => answer?,
        };
        let answer = sanitize_resolver_answer(request, answer, admitted_policy)
            .map_err(ResolverFailure::Sanitization)?;
        if shutdown.is_cancelled() {
            return Err(ResolverFailure::Cancelled);
        }
        Ok(answer)
    }

    pub fn resolver_for(&self, destination: IpAddr) -> Option<ReservedResolver> {
        ReservedResolver::from_ip(destination)
    }

    pub fn udp_capacity_response(
        &self,
        sandbox_id: &str,
        resolver: ReservedResolver,
        wire: &[u8],
    ) -> Vec<u8> {
        let request = parse_guest_request(wire);
        let started_at = Instant::now();
        let result = Err(match &request {
            Ok(_) => ResolutionFailure::UdpCapacity,
            Err(failure) => ResolutionFailure::InvalidRequest(failure.code),
        });
        metrics::record_request_outcome(
            sandbox_id,
            resolver,
            &request,
            &result,
            started_at.elapsed(),
        );
        match request {
            Ok(request) => local_error(&request, ResponseCode::ServFail),
            Err(failure) => failure.into_reply(),
        }
    }

    pub async fn resolve(
        &self,
        sandbox_id: &str,
        resolver: ReservedResolver,
        wire: &[u8],
        admitted_policy: &NetworkPolicyV1,
        shutdown: &CancellationToken,
    ) -> ResolveOutcome {
        let request = parse_guest_request(wire);
        let started_at = Instant::now();
        let result = async {
            let request = request
                .as_ref()
                .map_err(|failure| ResolutionFailure::InvalidRequest(failure.code))?;
            if !domain_is_allowed(admitted_policy, &request.domain) {
                return Err(ResolutionFailure::Refused);
            }
            match request.question.query_type {
                RecordType::AAAA => Ok(ResolutionAnswer::LocalNoData),
                RecordType::A => self
                    .resolve_a(resolver, request, admitted_policy, shutdown)
                    .await
                    .map(ResolutionAnswer::Upstream)
                    .map_err(ResolutionFailure::Lookup),
                _ => Err(ResolutionFailure::InvalidRequest(ResponseCode::NotImp)),
            }
        }
        .await;
        metrics::record_request_outcome(
            sandbox_id,
            resolver,
            &request,
            &result,
            started_at.elapsed(),
        );
        let request = match request {
            Ok(request) => request,
            Err(failure) => return ResolveOutcome::Reply(failure.into_reply()),
        };
        match result {
            Ok(answer) => ResolveOutcome::Reply(reply_from_answer(&request, answer)),
            Err(ResolutionFailure::Lookup(ResolverFailure::Cancelled)) => ResolveOutcome::Cancelled,
            Err(failure) => ResolveOutcome::Reply(local_error(&request, failure.response_code())),
        }
    }
}

fn local_error(request: &GuestRequest, code: ResponseCode) -> Vec<u8> {
    encode_message(response_message(request, code))
}

fn reply_from_answer(request: &GuestRequest, answer: ResolutionAnswer) -> Vec<u8> {
    let (code, answers) = match answer {
        ResolutionAnswer::LocalNoData => (ResponseCode::NoError, Vec::new()),
        ResolutionAnswer::Upstream(answer) => (answer.code, answer.answers),
    };
    let mut response = response_message(request, code);
    response.answers = answers;
    encode_message(response)
}

fn encode_message(message: Message) -> Vec<u8> {
    message
        .to_vec()
        .ok()
        .filter(|wire| wire.len() <= MAX_DNS_MESSAGE_BYTES)
        .unwrap_or_default()
}

#[derive(Debug)]
struct GuestRequest {
    id: u16,
    domain: DomainName,
    question: Query,
}

#[derive(Debug)]
enum ResolutionAnswer {
    LocalNoData,
    Upstream(SanitizedAnswer),
}

#[derive(Debug, thiserror::Error)]
enum ResolutionFailure {
    #[error("invalid DNS request: {0}")]
    InvalidRequest(ResponseCode),
    #[error("DNS query denied by network policy")]
    Refused,
    #[error("reserved DNS UDP admission capacity exhausted")]
    UdpCapacity,
    #[error("DNS lookup failed: {0}")]
    Lookup(#[source] ResolverFailure),
}

impl ResolutionFailure {
    fn response_code(&self) -> ResponseCode {
        match self {
            Self::InvalidRequest(code) => *code,
            Self::Refused => ResponseCode::Refused,
            Self::UdpCapacity | Self::Lookup(_) => ResponseCode::ServFail,
        }
    }
}

#[derive(Debug)]
struct RequestFailure {
    id: u16,
    code: ResponseCode,
    qtype: Option<RecordType>,
    question: Option<Query>,
}

impl RequestFailure {
    fn into_reply(self) -> Vec<u8> {
        let mut response = Message::response(self.id, OpCode::Query);
        response.metadata.recursion_available = true;
        response.metadata.response_code = self.code;
        // Resolvers can discard even error responses whose question does not match.
        // Echo only a decoded single query; malformed packets stay header-only.
        response.queries.extend(self.question);
        encode_message(response)
    }
}

fn parse_guest_request(wire: &[u8]) -> Result<GuestRequest, RequestFailure> {
    let id = wire
        .get(..2)
        .and_then(|bytes| <[u8; 2]>::try_from(bytes).ok())
        .map(u16::from_be_bytes)
        .unwrap_or_default();
    let message = decode_message(wire).map_err(|()| RequestFailure {
        id,
        code: ResponseCode::FormErr,
        qtype: None,
        question: None,
    })?;
    let failure = |code| RequestFailure {
        id,
        code,
        qtype: message.queries.first().map(|query| query.query_type),
        question: match message.queries.as_slice() {
            [query]
                if message.metadata.message_type == MessageType::Query
                    && message.metadata.op_code == OpCode::Query =>
            {
                Some(query.clone())
            }
            _ => None,
        },
    };
    if message.metadata.message_type != MessageType::Query
        || message.metadata.op_code != OpCode::Query
        || message.queries.len() != 1
    {
        return Err(failure(ResponseCode::FormErr));
    }
    let query = message
        .queries
        .first()
        .ok_or_else(|| failure(ResponseCode::FormErr))?;
    if query.query_class != DNSClass::IN
        || !matches!(query.query_type, RecordType::A | RecordType::AAAA)
    {
        return Err(failure(ResponseCode::NotImp));
    }

    let canonical_name = query.name.to_lowercase();
    let canonical_ascii = canonical_name.to_ascii();
    let domain = canonical_ascii
        .parse::<DomainName>()
        .map_err(|_| failure(ResponseCode::FormErr))?;
    if domain.as_str().parse::<IpAddr>().is_ok() {
        return Err(failure(ResponseCode::FormErr));
    }
    Ok(GuestRequest {
        id,
        question: Query::query(canonical_name, query.query_type),
        domain,
    })
}

fn decode_message(wire: &[u8]) -> Result<Message, ()> {
    if wire.len() > MAX_DNS_MESSAGE_BYTES {
        return Err(());
    }
    // Hickory preallocates section vectors from the header counts. Decode the
    // Hickory header first so a small packet cannot claim 65,535 records and
    // force a disproportionate allocation before the full decode fails.
    let mut header_decoder = BinDecoder::new(wire);
    let header = Header::read(&mut header_decoder).map_err(|_| ())?;
    let records = usize::from(header.counts.answers)
        .saturating_add(usize::from(header.counts.authorities))
        .saturating_add(usize::from(header.counts.additionals));
    if usize::from(header.counts.queries) > 1 || records > MAX_INPUT_RECORDS {
        return Err(());
    }

    let mut decoder = BinDecoder::new(wire);
    let message = Message::read(&mut decoder).map_err(|_| ())?;
    // `Message::from_vec` uses the same Hickory decoder but does not expose how
    // many bytes it consumed. Keep the decoder so opaque trailing bytes cannot
    // be accepted as part of a DNS request.
    if decoder.index() != wire.len() {
        return Err(());
    }
    Ok(message)
}

#[derive(Clone)]
struct ResolverAnswer {
    question: Query,
    code: ResponseCode,
    answers: Vec<Record>,
    valid_until: Instant,
}

#[derive(Clone, Debug, thiserror::Error)]
enum ResolverFailure {
    #[error("DNS lookup cancelled")]
    Cancelled,
    #[error("Hickory reported a timeout; transport phase unavailable")]
    Timeout,
    #[error("DNS lookup exceeded the total lookup deadline")]
    LookupDeadline,
    #[error("reserved DNS lookup capacity exhausted")]
    Capacity,
    #[error("Hickory resolver is busy")]
    Busy,
    #[error("DNS transport failed: {0}")]
    Transport(#[source] NetError),
    #[error("resolver returned a mismatched query or too many records")]
    Malformed,
    #[error("resolver answer rejected: {0}")]
    Sanitization(#[source] SanitizationFailure),
    #[error("upstream resolver returned {0}")]
    Upstream(ResponseCode),
}

impl From<NetError> for ResolverFailure {
    fn from(error: NetError) -> Self {
        match error {
            NetError::Dns(DnsError::ResponseCode(code)) => Self::Upstream(code),
            NetError::Timeout => Self::Timeout,
            NetError::Busy => Self::Busy,
            error => Self::Transport(error),
        }
    }
}

type ResolverFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ResolverAnswer, ResolverFailure>> + Send + 'a>>;

trait ResolverBackend: Send + Sync {
    fn lookup<'a>(&'a self, resolver: ReservedResolver, question: Query) -> ResolverFuture<'a>;
}

struct HickoryResolverSet {
    cloudflare: RecoveringResolver,
    google: RecoveringResolver,
}

impl HickoryResolverSet {
    fn new(lookup_slots: &ResolverSlots) -> Result<Self, BuildReservedDnsCoreError> {
        let build = |resolver| build_hickory_resolver(resolver, lookup_slots.get(resolver).clone());
        Ok(Self {
            cloudflare: build(ReservedResolver::Cloudflare)?,
            google: build(ReservedResolver::Google)?,
        })
    }

    fn get(&self, resolver: ReservedResolver) -> &RecoveringResolver {
        match resolver {
            ReservedResolver::Cloudflare => &self.cloudflare,
            ReservedResolver::Google => &self.google,
        }
    }
}

impl ResolverBackend for HickoryResolverSet {
    fn lookup<'a>(&'a self, resolver: ReservedResolver, question: Query) -> ResolverFuture<'a> {
        Box::pin(async move {
            // The deadline covers both the original lookup and its one
            // recovery attempt, under the same lookup slot. Policy
            // cancellation is applied per waiter by the coalescing layer;
            // dropping this future (last waiter gone) cancels the lookup.
            let lookup = self.get(resolver).lookup(resolver, &question);
            let lookup = match tokio::time::timeout(TOTAL_LOOKUP_TIMEOUT, lookup).await {
                Err(_) => return Err(ResolverFailure::LookupDeadline),
                Ok(Err(NetError::Dns(DnsError::NoRecordsFound(no_records)))) => {
                    return Ok(ResolverAnswer {
                        question,
                        code: if no_records.response_code == ResponseCode::NXDomain {
                            ResponseCode::NXDomain
                        } else {
                            ResponseCode::NoError
                        },
                        answers: Vec::new(),
                        valid_until: Instant::now() + MAX_NEGATIVE_TTL,
                    });
                }
                Ok(Err(error)) => return Err(error.into()),
                Ok(Ok(lookup)) => lookup,
            };
            if lookup.query() != &question || lookup.answers().len() > MAX_RESPONSE_RECORDS {
                return Err(ResolverFailure::Malformed);
            }
            Ok(ResolverAnswer {
                question,
                code: ResponseCode::NoError,
                answers: lookup.answers().to_vec(),
                valid_until: lookup.valid_until(),
            })
        })
    }
}

fn build_hickory_resolver(
    resolver: ReservedResolver,
    lookup_slots: Arc<Semaphore>,
) -> Result<RecoveringResolver, BuildReservedDnsCoreError> {
    let (expected_ip, servers) = match resolver {
        ReservedResolver::Cloudflare => (CLOUDFLARE_DNS, &CLOUDFLARE),
        ReservedResolver::Google => (GOOGLE_DNS, &GOOGLE),
    };
    let server = servers
        .tls()
        .find(|server| server.ip == IpAddr::V4(expected_ip))
        .expect("Hickory public resolver configuration contains the reviewed IPv4 endpoint");
    let config = ResolverConfig::from_parts(None, Vec::new(), vec![server]);
    let mut options = ResolverOpts::default();
    options.timeout = ATTEMPT_TIMEOUT;
    options.attempts = 1;
    options.cache_size = CACHE_ENTRIES_PER_RESOLVER;
    options.use_hosts_file = ResolveHosts::Never;
    options.positive_min_ttl = None;
    options.negative_min_ttl = None;
    options.positive_max_ttl = Some(Duration::from_secs(u64::from(MAX_POSITIVE_TTL)));
    options.negative_max_ttl = Some(MAX_NEGATIVE_TTL);
    options.num_concurrent_reqs = 1;
    options.max_active_requests = MAX_CONCURRENT_LOOKUPS;
    options.preserve_intermediates = true;

    RecoveringResolver::new(
        config,
        options,
        GatewayRuntimeProvider::marked(lookup_slots),
        resolver,
    )
}

/// Hickory owns its sockets, so its runtime provider is the only point where
/// the gateway can apply the standard outbound `SO_MARK` before network I/O.
/// The kernel consumes that mark to bypass transparent interception, exactly
/// as it does for ordinary TCP and UDP relay sockets.
#[derive(Clone)]
struct GatewayRuntimeProvider {
    handle: TokioHandle,
    outbound_mark: Option<u32>,
    lookup_slots: Arc<Semaphore>,
}

impl GatewayRuntimeProvider {
    fn marked(lookup_slots: Arc<Semaphore>) -> Self {
        Self {
            handle: TokioHandle::default(),
            outbound_mark: Some(pplx_space_net_gateway_client::OUTBOUND_MARK),
            lookup_slots,
        }
    }
}

impl RuntimeProvider for GatewayRuntimeProvider {
    type Handle = TokioHandle;
    type Timer = TokioTime;
    type Udp = UdpSocket;
    type Tcp = AsyncIoTokioAsStd<TcpStream>;

    fn create_handle(&self) -> Self::Handle {
        self.handle.clone()
    }

    fn connect_tcp(
        &self,
        server_addr: SocketAddr,
        bind_addr: Option<SocketAddr>,
        timeout: Option<Duration>,
    ) -> Pin<Box<dyn Send + Future<Output = Result<Self::Tcp, io::Error>>>> {
        let outbound_mark = self.outbound_mark;
        Box::pin(async move {
            let socket = tcp_socket(server_addr, outbound_mark)?;
            if let Some(bind_addr) = bind_addr {
                socket.bind(bind_addr)?;
            }
            socket.set_nodelay(true)?;
            let stream = tokio::time::timeout(
                timeout.unwrap_or(DEFAULT_CONNECT_TIMEOUT),
                socket.connect(server_addr),
            )
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "DNS connect timed out"))??;
            Ok(AsyncIoTokioAsStd(stream))
        })
    }

    fn bind_udp(
        &self,
        local_addr: SocketAddr,
        _server_addr: SocketAddr,
    ) -> Pin<Box<dyn Send + Future<Output = Result<Self::Udp, io::Error>>>> {
        let outbound_mark = self.outbound_mark;
        Box::pin(async move {
            let socket = UdpSocket::bind(local_addr).await?;
            if let Some(mark) = outbound_mark {
                crate::orig_dst::set_so_mark(&socket, mark)?;
            }
            Ok(socket)
        })
    }
}

fn tcp_socket(server_addr: SocketAddr, outbound_mark: Option<u32>) -> io::Result<TcpSocket> {
    let socket = match server_addr {
        SocketAddr::V4(_) => TcpSocket::new_v4(),
        SocketAddr::V6(_) => TcpSocket::new_v6(),
    }?;
    apply_outbound_mark(socket, outbound_mark, |socket, mark| {
        crate::orig_dst::set_so_mark(socket, mark)
    })
}

fn apply_outbound_mark<T>(
    socket: T,
    outbound_mark: Option<u32>,
    mark_socket: impl FnOnce(&T, u32) -> io::Result<()>,
) -> io::Result<T> {
    if let Some(mark) = outbound_mark {
        mark_socket(&socket, mark)?;
    }
    Ok(socket)
}

#[derive(Debug)]
struct SanitizedAnswer {
    code: ResponseCode,
    answers: Vec<Record>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
enum SanitizationFailure {
    #[error("resolver answer does not match the question")]
    QuestionMismatch,
    #[error("unexpected resolver response code: {0}")]
    ResponseCode(ResponseCode),
    #[error("conflicting CNAME targets")]
    ConflictingCname,
    #[error("CNAME chain contains a cycle")]
    CnameCycle,
    #[error("CNAME chain exceeds the hop limit")]
    CnameHopLimit,
    #[error("all answer addresses are denied by policy")]
    AddressDenied,
}

fn sanitize_resolver_answer(
    request: &GuestRequest,
    answer: ResolverAnswer,
    policy: &NetworkPolicyV1,
) -> Result<SanitizedAnswer, SanitizationFailure> {
    if answer.question != request.question {
        return Err(SanitizationFailure::QuestionMismatch);
    }
    match answer.code {
        ResponseCode::NXDomain => {
            return Ok(SanitizedAnswer {
                code: ResponseCode::NXDomain,
                answers: Vec::new(),
            });
        }
        ResponseCode::NoError => {}
        code => return Err(SanitizationFailure::ResponseCode(code)),
    }

    // Hickory associates the response with the generated query, but its
    // `Lookup` can retain unrelated answer records. Restrict the local reply to
    // the question-rooted CNAME chain so those records cannot seed attribution.
    let mut cname_edges = HashMap::<Name, Name>::new();
    for record in &answer.answers {
        if record.dns_class != DNSClass::IN {
            continue;
        }
        let RData::CNAME(CNAME(target)) = &record.data else {
            continue;
        };
        let owner = record.name.to_lowercase();
        let target = target.to_lowercase();
        match cname_edges.entry(owner) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(target);
            }
            std::collections::hash_map::Entry::Occupied(entry) if entry.get() == &target => {}
            std::collections::hash_map::Entry::Occupied(_) => {
                return Err(SanitizationFailure::ConflictingCname);
            }
        }
    }

    let mut owner = request.question.name.to_lowercase();
    let mut reachable = HashSet::from([owner.clone()]);
    for _ in 0..MAX_CNAME_HOPS {
        let Some(target) = cname_edges.get(&owner) else {
            break;
        };
        if !reachable.insert(target.clone()) {
            return Err(SanitizationFailure::CnameCycle);
        }
        owner = target.clone();
    }
    if cname_edges.contains_key(&owner) {
        return Err(SanitizationFailure::CnameHopLimit);
    }

    let remaining_ttl = u32::try_from(
        answer
            .valid_until
            .saturating_duration_since(Instant::now())
            .as_secs(),
    )
    .unwrap_or(u32::MAX)
    .min(MAX_POSITIVE_TTL);
    let mut answers = Vec::new();
    let mut candidate_addresses = 0_usize;
    let mut allowed_addresses = 0_usize;
    for record in answer.answers {
        if answers.len() >= MAX_RESPONSE_RECORDS || record.dns_class != DNSClass::IN {
            continue;
        }
        let owner = record.name.to_lowercase();
        if !reachable.contains(&owner) {
            continue;
        }
        match record.data {
            RData::CNAME(CNAME(target)) => {
                let target = target.to_lowercase();
                if cname_edges
                    .get(&owner)
                    .is_some_and(|expected| expected == &target)
                {
                    answers.push(Record::from_rdata(
                        owner,
                        record.ttl.min(remaining_ttl),
                        RData::CNAME(CNAME(target)),
                    ));
                }
            }
            RData::A(address) if request.question.query_type == RecordType::A => {
                candidate_addresses += 1;
                if answer_address_allowed(policy, IpAddr::V4(address.0)) {
                    allowed_addresses += 1;
                    answers.push(Record::from_rdata(
                        owner,
                        record.ttl.min(remaining_ttl),
                        RData::A(address),
                    ));
                }
            }
            _ => {}
        }
    }
    if candidate_addresses > 0 && allowed_addresses == 0 {
        return Err(SanitizationFailure::AddressDenied);
    }
    Ok(SanitizedAnswer {
        code: ResponseCode::NoError,
        answers,
    })
}

fn answer_address_allowed(policy: &NetworkPolicyV1, address: IpAddr) -> bool {
    matches!(
        policy.egress_match_for_ip(address),
        EgressAddressMatch::ExplicitAllow | EgressAddressMatch::NoMatch
    )
}

fn response_message(request: &GuestRequest, code: ResponseCode) -> Message {
    let mut response = Message::response(request.id, OpCode::Query);
    response.metadata.recursion_available = true;
    response.metadata.response_code = code;
    response.queries.push(request.question.clone());
    response
}

#[cfg(test)]
mod recovery_tests;
#[cfg(test)]
pub(crate) mod test_support;
#[cfg(test)]
mod tests;
