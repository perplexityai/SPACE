//! Replay of sandbox-manager RPCs across a restart handoff.
//!
//! During a deployment handoff the predecessor process closes state-writer
//! admission and rejects new state-changing RPCs before dispatch with an
//! `UNAVAILABLE` status carrying [`RESTART_RETRY_ON_SUCCESSOR_METADATA`].
//! Because the restart-admission middleware emits that rejection before the
//! request reaches any handler, a request that receives it has provably not
//! executed and can be replayed for every RPC, idempotent or not.
//! [`call_with_restart_handoff`] owns that replay loop for node API forwards.
//!
//! [`RESTART_RETRY_ON_SUCCESSOR_METADATA`]: pplx_space_sbx_mgr_client::RESTART_RETRY_ON_SUCCESSOR_METADATA

use std::future::Future;
use std::time::Duration;

use pplx_space_common_grpc::{GrpcRetryContext, GrpcRetryPolicy};
use pplx_space_common_svc::DEFAULT_DRAIN_TIMEOUT;
use tonic::metadata::MetadataMap;
use tonic::{Request, Response, Status};

use pplx_space_sbx_mgr_client::{
    SandboxManagerGrpcClient, connect_fresh, is_restart_retry_on_successor,
};

/// Retry budget spanning a full sandbox-manager restart handoff: the
/// predecessor drains state writers for up to [`DEFAULT_DRAIN_TIMEOUT`]
/// before it exits and the successor can bind the socket. The budget is a
/// hard deadline covering backoff sleeps, fresh dials, and in-flight
/// replays — not just the decision to retry. Callers with a tighter
/// `grpc-timeout` deadline are still bounded by it.
const RESTART_HANDOFF_RETRY_POLICY: GrpcRetryPolicy = GrpcRetryPolicy {
    max_retries: 128,
    initial_delay: Duration::from_millis(50),
    backoff_multiplier: 2.0,
    max_delay: Duration::from_millis(500),
    max_elapsed: DEFAULT_DRAIN_TIMEOUT.saturating_add(Duration::from_secs(5)),
    jitter_factor: 0.2,
    max_server_retry_after: Duration::from_secs(2),
};

/// How long one shared dial result stays authoritative for every concurrent
/// handoff caller, and the hard bound on each in-flight dial attempt.
/// Matches the backoff cadence cap so the process dials at most about twice
/// per second during a handoff, no matter how many callers are waiting, and
/// so one stalled connect surrenders the single-flight lock within a
/// generation instead of blocking every follower.
const SHARED_DIAL_TTL: Duration = Duration::from_millis(500);

/// Replay classification for the RPC being forwarded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RpcReplaySafety {
    /// Replay only on provably-untransmitted failures: the pre-dispatch
    /// drain marker or a typed client-side dial failure. Required for
    /// effectful RPCs, whose duplicate execution is unsafe.
    RequiresProof,
    /// The RPC is read-only and inherently replay-safe, so a
    /// transport-shaped failure (which may have executed) earns one
    /// fresh-connection probe in case the shared channel was the problem.
    /// The drain-sized budget stays reserved for handoff proofs: a live
    /// handler returning unmarked `UNAVAILABLE` (for example, a started
    /// sandbox whose spaced-host socket is absent) must not be re-executed
    /// in a loop. Never use this for an RPC with side effects.
    ReadOnly,
}

/// What a failure earns inside the handoff loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReplayClass {
    /// Provably untransmitted: replay within the full handoff budget.
    Proof,
    /// Ambiguous but the RPC is read-only: consumes the single probe.
    ReadProbe,
    /// Not replayable: return the failure unchanged.
    Fatal,
}

impl RpcReplaySafety {
    fn probe_allowance(self) -> usize {
        match self {
            Self::RequiresProof => 0,
            Self::ReadOnly => 1,
        }
    }

    fn classify(self, status: &Status) -> ReplayClass {
        if is_restart_retry_on_successor(status)
            || pplx_space_common_grpc::is_client_connect_failure(status)
        {
            return ReplayClass::Proof;
        }
        match self {
            Self::RequiresProof => ReplayClass::Fatal,
            Self::ReadOnly => {
                if pplx_space_common_grpc::is_temporary_tcp_failure(status) {
                    ReplayClass::ReadProbe
                } else {
                    ReplayClass::Fatal
                }
            }
        }
    }
}

/// Single-flight provider of fresh sandbox-manager connections during a
/// handoff. Every concurrent marked caller shares one dial result per
/// [`SHARED_DIAL_TTL`] window instead of each running its own dial loop —
/// without this, N waiting callers times the backoff cadence times the drain
/// duration becomes a Unix-socket connect storm against the node.
struct RestartHandoffDialer<C> {
    ttl: Duration,
    state: tokio::sync::Mutex<DialerState<C>>,
}

struct DialerState<C> {
    epoch: u64,
    dialed_at: Option<tokio::time::Instant>,
    result: Option<Result<C, Status>>,
}

impl<C: Clone> RestartHandoffDialer<C> {
    fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            state: tokio::sync::Mutex::new(DialerState {
                epoch: 0,
                dialed_at: None,
                result: None,
            }),
        }
    }

    /// Returns the shared dial result for an epoch newer than `seen_epoch`,
    /// dialing at most once per TTL window across all concurrent callers.
    /// The lock is held across the dial deliberately: queued callers wake to
    /// the cached result instead of dialing themselves. Each dial attempt is
    /// itself bounded by the TTL, so a stalled connect (for example, one
    /// accepted by the dying predecessor whose handshake never completes)
    /// records a failure and releases the lock instead of head-of-line
    /// blocking every follower behind one dead generation. Callers remain
    /// bounded by their own deadlines and cancellation — dropping a queued
    /// or dialing caller releases the lock.
    async fn fresh_client<D, DFut>(&self, seen_epoch: u64, dial: &D) -> (u64, Result<C, Status>)
    where
        D: Fn() -> DFut,
        DFut: Future<Output = Result<C, Status>>,
    {
        let mut state = self.state.lock().await;
        if state.epoch > seen_epoch
            && let Some(result) = &state.result
            && matches!(state.dialed_at, Some(at) if at.elapsed() < self.ttl)
        {
            return (state.epoch, result.clone());
        }
        let result = match tokio::time::timeout(self.ttl, dial()).await {
            Ok(result) => result,
            Err(_) => Err(Status::unavailable(format!(
                "dial sandbox-manager: connect attempt timed out after {:?}",
                self.ttl
            ))),
        };
        state.epoch += 1;
        state.dialed_at = Some(tokio::time::Instant::now());
        state.result = Some(result.clone());
        (state.epoch, result)
    }
}

fn production_dialer() -> &'static RestartHandoffDialer<SandboxManagerGrpcClient> {
    static DIALER: std::sync::OnceLock<RestartHandoffDialer<SandboxManagerGrpcClient>> =
        std::sync::OnceLock::new();
    DIALER.get_or_init(|| RestartHandoffDialer::new(SHARED_DIAL_TTL))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RestartHandoffOutcome {
    Succeeded,
    Failed,
    DeadlineExceeded,
    Exhausted,
}

impl RestartHandoffOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::DeadlineExceeded => "deadline_exceeded",
            Self::Exhausted => "exhausted",
        }
    }
}

impl std::fmt::Display for RestartHandoffOutcome {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Forward one sandbox-manager RPC, replaying it only across a restart
/// handoff.
///
/// Replay begins on either of two provably-untransmitted failures: the
/// restart-drain rejection, whose explicit metadata is emitted by the
/// sandbox-manager restart-admission middleware before the request is
/// dispatched to any handler; or a client-side dial failure with a typed
/// local error chain, which covers a request whose first attempt lands in
/// the gap between predecessor exit and successor socket bind. Both prove
/// the request never executed, so replay is safe for every RPC, idempotent
/// or not. RPCs declared [`RpcReplaySafety::ReadOnly`] additionally earn a
/// single fresh-connection probe on an ambiguous transport-shaped failure —
/// enough to escape a dead pinned channel, without re-executing a live
/// erroring handler for the drain-sized budget. Every other failure is
/// returned unchanged because the RPC may have executed.
///
/// Replays never reuse `client`; each one uses a fresh, uncached connection
/// to reach whichever process currently owns the socket, because an
/// established HTTP/2 connection can stay pinned to the draining
/// predecessor. Fresh dials are single-flighted process-wide: all concurrent
/// marked callers share one dial result per TTL window rather than each
/// running its own dial loop.
///
/// The whole wait — sleeps, dials, and in-flight replays — is bounded by the
/// sooner of the caller's `grpc-timeout` deadline and the hard handoff
/// budget, and a non-read replay that times out in flight is surfaced as a
/// deadline, never replayed again.
pub(crate) async fn call_with_restart_handoff<Req, Resp, F, Fut>(
    method: &'static str,
    safety: RpcReplaySafety,
    request: Request<Req>,
    client: SandboxManagerGrpcClient,
    call: F,
) -> Result<Response<Resp>, Status>
where
    Req: Clone + Send + Sync,
    F: Fn(SandboxManagerGrpcClient, Request<Req>) -> Fut + Send,
    Fut: Future<Output = Result<Response<Resp>, Status>> + Send,
{
    call_with_restart_handoff_via(
        method,
        safety,
        request,
        client,
        production_dialer(),
        dial_fresh,
        call,
    )
    .await
}

async fn call_with_restart_handoff_via<Req, Resp, D, DFut, F, Fut>(
    method: &'static str,
    safety: RpcReplaySafety,
    request: Request<Req>,
    client: SandboxManagerGrpcClient,
    dialer: &RestartHandoffDialer<SandboxManagerGrpcClient>,
    dial: D,
    call: F,
) -> Result<Response<Resp>, Status>
where
    Req: Clone + Send + Sync,
    D: Fn() -> DFut + Send + Sync,
    DFut: Future<Output = Result<SandboxManagerGrpcClient, Status>> + Send,
    F: Fn(SandboxManagerGrpcClient, Request<Req>) -> Fut + Send,
    Fut: Future<Output = Result<Response<Resp>, Status>> + Send,
{
    let metadata = request.metadata().clone();
    let message = request.into_inner();
    retry_restart_handoff(
        method,
        RESTART_HANDOFF_RETRY_POLICY,
        safety,
        metadata,
        message,
        client,
        dialer,
        dial,
        call,
    )
    .await
}

async fn dial_fresh() -> Result<SandboxManagerGrpcClient, Status> {
    connect_fresh()
        .await
        .map_err(|error| Status::unavailable(format!("dial sandbox-manager: {error}")))
}

#[expect(
    clippy::too_many_arguments,
    reason = "internal core behind a thin public wrapper"
)]
async fn retry_restart_handoff<C, Req, Resp, D, DFut, F, Fut>(
    method: &'static str,
    policy: GrpcRetryPolicy,
    safety: RpcReplaySafety,
    metadata: MetadataMap,
    message: Req,
    client: C,
    dialer: &RestartHandoffDialer<C>,
    dial: D,
    call: F,
) -> Result<Response<Resp>, Status>
where
    C: Clone + Send,
    Req: Clone + Send + Sync,
    D: Fn() -> DFut + Send + Sync,
    DFut: Future<Output = Result<C, Status>> + Send,
    F: Fn(C, Request<Req>) -> Fut + Send,
    Fut: Future<Output = Result<Response<Resp>, Status>> + Send,
{
    let context = GrpcRetryContext::none().with_grpc_timeout_metadata(&metadata);
    let build_request = || {
        let mut request = Request::new(message.clone());
        *request.metadata_mut() = metadata.clone();
        request
    };

    let status = match run_until_deadline(&context, method, call(client, build_request())).await {
        Ok(response) => return Ok(response),
        Err(status) => status,
    };
    // Enter replay only when this RPC's safety classification allows it: a
    // provably-untransmitted failure (pre-dispatch marker, or typed
    // client-side dial failure covering the exit-to-bind gap — wire statuses
    // carry no local error chain, so a server echoing connect-failure
    // wording cannot enter here), or — for read-only RPCs — one
    // fresh-connection probe on an ambiguous transport-shaped failure.
    let mut probes_remaining = safety.probe_allowance();
    match safety.classify(&status) {
        ReplayClass::Proof => {}
        ReplayClass::ReadProbe => probes_remaining -= 1,
        ReplayClass::Fatal => return Err(status),
    }

    // The handoff budget is a hard deadline for everything below — backoff
    // sleeps, fresh dials, and in-flight replays — so a hung Unix-socket
    // connect or HTTP/2 handshake cannot pin one attempt past the advertised
    // budget even when the caller supplied no `grpc-timeout`.
    let started_at = tokio::time::Instant::now();
    let handoff_deadline = started_at + policy.max_elapsed;
    let mut last_status = status;
    let mut attempts = 0usize;
    let mut seen_epoch = 0u64;
    let outcome = loop {
        let Some(delay) = policy.backoff_delay(attempts) else {
            break RestartHandoffOutcome::Exhausted;
        };
        if started_at.elapsed() + delay >= policy.max_elapsed {
            last_status = handoff_deadline_status(method);
            break RestartHandoffOutcome::DeadlineExceeded;
        }
        if matches!(context.remaining(), Some(remaining) if delay >= remaining) {
            last_status = handoff_deadline_status(method);
            break RestartHandoffOutcome::DeadlineExceeded;
        }
        tokio::time::sleep(delay).await;
        attempts += 1;
        pplx_space_metrics::grpc::client_retry("sandbox-manager", method, "rpc", "restart_handoff");
        let budget = attempt_budget(&context, handoff_deadline);
        let shared_dial = async { Ok(dialer.fresh_client(seen_epoch, &dial).await) };
        let (epoch, dialed) = match run_within(budget, method, shared_dial).await {
            Ok(shared) => shared,
            Err(deadline_status) => {
                last_status = deadline_status;
                break RestartHandoffOutcome::DeadlineExceeded;
            }
        };
        seen_epoch = epoch;
        let client = match dialed {
            Ok(client) => client,
            Err(dial_status) => {
                last_status = dial_status;
                continue;
            }
        };
        let budget = attempt_budget(&context, handoff_deadline);
        match run_within(budget, method, call(client, build_request())).await {
            Ok(response) => {
                log_restart_handoff_outcome(
                    method,
                    RestartHandoffOutcome::Succeeded,
                    attempts,
                    started_at,
                );
                return Ok(response);
            }
            Err(status) if status.code() == tonic::Code::DeadlineExceeded => {
                last_status = status;
                break RestartHandoffOutcome::DeadlineExceeded;
            }
            Err(status) => match safety.classify(&status) {
                ReplayClass::Proof => {
                    last_status = status;
                }
                ReplayClass::ReadProbe if probes_remaining > 0 => {
                    probes_remaining -= 1;
                    last_status = status;
                }
                ReplayClass::ReadProbe | ReplayClass::Fatal => {
                    last_status = status;
                    break RestartHandoffOutcome::Failed;
                }
            },
        }
    };
    log_restart_handoff_outcome(method, outcome, attempts, started_at);
    Err(last_status)
}

async fn run_until_deadline<T>(
    context: &GrpcRetryContext,
    method: &'static str,
    future: impl Future<Output = Result<T, Status>>,
) -> Result<T, Status> {
    match context.remaining() {
        Some(remaining) if remaining.is_zero() => Err(handoff_deadline_status(method)),
        Some(remaining) => match tokio::time::timeout(remaining, future).await {
            Ok(result) => result,
            Err(_) => Err(handoff_deadline_status(method)),
        },
        None => future.await,
    }
}

/// Time left before the sooner of the caller's `grpc-timeout` deadline and
/// the hard handoff budget.
fn attempt_budget(context: &GrpcRetryContext, handoff_deadline: tokio::time::Instant) -> Duration {
    let handoff_remaining = handoff_deadline.saturating_duration_since(tokio::time::Instant::now());
    match context.remaining() {
        Some(caller_remaining) => caller_remaining.min(handoff_remaining),
        None => handoff_remaining,
    }
}

async fn run_within<T>(
    budget: Duration,
    method: &'static str,
    future: impl Future<Output = Result<T, Status>>,
) -> Result<T, Status> {
    if budget.is_zero() {
        return Err(handoff_deadline_status(method));
    }
    match tokio::time::timeout(budget, future).await {
        Ok(result) => result,
        Err(_) => Err(handoff_deadline_status(method)),
    }
}

fn handoff_deadline_status(method: &'static str) -> Status {
    Status::deadline_exceeded(format!(
        "sandbox-manager {method} restart handoff deadline exceeded"
    ))
}

fn log_restart_handoff_outcome(
    method: &'static str,
    outcome: RestartHandoffOutcome,
    attempts: usize,
    started_at: tokio::time::Instant,
) {
    tracing::info!(
        method,
        outcome = %outcome,
        attempts,
        wait_ms = u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX),
        "sandbox-manager restart handoff wait finished"
    );
}

#[cfg(test)]
mod tests;
