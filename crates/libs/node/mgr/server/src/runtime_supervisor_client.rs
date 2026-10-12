//! Lazily connected runtime-supervisor client that rides out a supervisor
//! restart.
//!
//! The runtime supervisor restarts with `KillMode=process`, so running VMs and
//! sidecars are unaffected but its Unix socket disappears briefly between
//! listener close and rebind. An RPC that fails because the socket is absent or
//! refusing connections provably never reached a server, so replaying it is
//! safe for every supervisor RPC, idempotent or not. Statuses a server returned
//! and mid-stream transport breaks are never replayed — those may have
//! executed. (LaunchFirecracker/LaunchSidecar are additionally idempotent
//! through pidfile re-adoption, but the retry class must not be widened on
//! that basis.)

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use pplx_space_common_grpc::{GrpcRetryPolicy, is_client_connect_failure, should_reconnect_status};
use tonic::Status;

use pplx_space_node_runtime_core::{RuntimeSupervisorGrpcClient, proto};

/// Retry budget spanning a runtime-supervisor restart. The socket gap between
/// listener close and rebind is sub-second, so short delays ride it out
/// quickly; the elapsed budget covers a slow supervisor startup without
/// holding a caller much beyond the node-manager RPC deadlines above it.
const SUPERVISOR_RESTART_RETRY_POLICY: GrpcRetryPolicy = GrpcRetryPolicy {
    max_retries: 128,
    initial_delay: std::time::Duration::from_millis(20),
    backoff_multiplier: 2.0,
    max_delay: std::time::Duration::from_millis(250),
    max_elapsed: std::time::Duration::from_secs(15),
    jitter_factor: 0.2,
    max_server_retry_after: std::time::Duration::from_secs(2),
};

/// Bound on one dial attempt so a hung connect cannot pin an RPC for the whole
/// retry budget.
const CONNECT_ATTEMPT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);

type SupervisorConnectFuture =
    Pin<Box<dyn Future<Output = Result<RuntimeSupervisorGrpcClient, Status>> + Send>>;
type SupervisorConnectFn = Box<dyn FnMut() -> SupervisorConnectFuture + Send>;

/// Lazily connected runtime-supervisor client over its node-local Unix socket.
///
/// Clones share one evictable cached generated client while running
/// independent RPCs concurrently. Transport-like gRPC failures clear the
/// connection they used without evicting a newer connection established by
/// another caller, and provably-untransmitted client-side connect failures are
/// replayed against a fresh dial (see the module docs for why that is the only
/// replayed class).
#[derive(Clone)]
pub struct RuntimeSupervisorLazyClient {
    cache: Arc<tokio::sync::Mutex<SupervisorClientCache>>,
}

#[derive(Clone)]
struct CachedSupervisorClient {
    connection_id: Arc<()>,
    client: RuntimeSupervisorGrpcClient,
}

struct SupervisorClientCache {
    client: Option<CachedSupervisorClient>,
    connect: SupervisorConnectFn,
}

impl SupervisorClientCache {
    async fn get_or_connect(&mut self) -> Result<CachedSupervisorClient, Status> {
        if let Some(client) = &self.client {
            return Ok(client.clone());
        }

        let client = match tokio::time::timeout(CONNECT_ATTEMPT_TIMEOUT, (self.connect)()).await {
            Ok(Ok(client)) => client,
            Ok(Err(status)) => return Err(status),
            Err(_) => {
                return Err(Status::unavailable(format!(
                    "runtime-supervisor connect attempt timed out after {CONNECT_ATTEMPT_TIMEOUT:?}"
                )));
            }
        };
        let client = CachedSupervisorClient {
            connection_id: Arc::new(()),
            client,
        };
        self.client = Some(client.clone());
        Ok(client)
    }

    fn clear_if_same(&mut self, failed: &CachedSupervisorClient) {
        if self
            .client
            .as_ref()
            .is_some_and(|cached| Arc::ptr_eq(&cached.connection_id, &failed.connection_id))
        {
            self.client = None;
        }
    }
}

/// How one failed attempt relates to the request having been transmitted.
enum SupervisorAttemptFailure {
    /// The request provably never reached a server: the dial itself failed, or
    /// the RPC failed with a typed client-side connect error chain. Safe to
    /// replay for every RPC.
    Untransmitted(Status),
    /// A server returned this status, or the transport broke mid-stream — the
    /// request may have executed, so it is never replayed.
    Terminal(Status),
}

macro_rules! runtime_supervisor_lazy_unary_methods {
    ($(
        $method:ident, $request:ty, $response:ty;
    )*) => {
        $(
            pub async fn $method(
                &self,
                request: $request,
            ) -> Result<tonic::Response<$response>, Status> {
                self.call_unary(
                    stringify!($method),
                    request,
                    |mut client, request| async move { client.$method(request).await },
                )
                .await
            }
        )*
    };
}

/// True when a failed RPC provably never reached a server because the
/// client-side HTTP/2 connection was already closing when the request was
/// dispatched. Two typed shapes qualify, both untransmitted by construction:
/// hyper's `Canceled` ("a pending item was dropped before ever being
/// processed" — the shape a graceful-shutdown GOAWAY produces for requests
/// issued through a draining connection), and h2 `REFUSED_STREAM`, which a
/// peer only sends for streams it did not process. A `Cancelled` status
/// without one of those typed sources is NOT classified: generic
/// cancellation can also describe a request that executed.
///
/// Module-local for now rather than in `pplx-space-common-grpc`: promotion
/// is deferred until a second consumer (the sandbox-manager restart-handoff
/// replay is the candidate) adopts the classification deliberately, since
/// widening a shared replay predicate changes what other clients consider
/// safe to resend.
fn is_request_refused_before_transmission(status: &Status) -> bool {
    if !matches!(
        status.code(),
        tonic::Code::Cancelled | tonic::Code::Unavailable
    ) {
        return false;
    }
    let mut source = std::error::Error::source(status);
    while let Some(error) = source {
        if let Some(hyper_error) = error.downcast_ref::<hyper::Error>()
            && hyper_error.is_canceled()
        {
            return true;
        }
        if let Some(h2_error) = error.downcast_ref::<h2::Error>()
            && h2_error.reason() == Some(h2::Reason::REFUSED_STREAM)
        {
            return true;
        }
        source = error.source();
    }
    false
}

/// Bound on the dial-time liveness round-trip.
const DIAL_LIVENESS_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(500);

/// Connect and prove the server end is alive with a health-check
/// round-trip before handing the connection to callers.
///
/// An eager tonic connect to a Unix socket completes once the local
/// preface is written — the kernel accepts into the listen backlog even
/// when no process will ever service it, which is exactly the state a
/// supervisor draining out of `systemctl restart` leaves behind. Without
/// the round-trip, a request rides that doomed connection and dies as an
/// untyped mid-handshake io error that cannot be proven untransmitted.
/// With it, every failure is either a dial failure (replay-safe by
/// construction) or happens on a connection a live server accepted, where
/// the typed GOAWAY/refusal classes apply.
async fn connect_supervisor_proven_live() -> Result<RuntimeSupervisorGrpcClient, Status> {
    let channel = pplx_space_node_runtime_core::connect_runtime_supervisor_channel()
        .await
        .map_err(|error| {
            Status::unavailable(format!("connect to runtime-supervisor failed: {error}"))
        })?;
    prove_live_and_wrap(channel).await
}

/// The liveness half of the dial, shared with the test harness so tests
/// exercise the exact production connect semantics.
async fn prove_live_and_wrap(
    channel: tonic::transport::Channel,
) -> Result<RuntimeSupervisorGrpcClient, Status> {
    let mut health = tonic_health::pb::health_client::HealthClient::new(channel.clone());
    let check = health.check(tonic_health::pb::HealthCheckRequest {
        service: String::new(),
    });
    match tokio::time::timeout(DIAL_LIVENESS_TIMEOUT, check).await {
        Ok(Ok(_)) => Ok(pplx_space_node_runtime_core::runtime_supervisor_client(
            channel,
        )),
        Ok(Err(status)) => Err(Status::unavailable(format!(
            "runtime-supervisor liveness check failed: {status}"
        ))),
        Err(_) => Err(Status::unavailable(format!(
            "runtime-supervisor liveness check timed out after {DIAL_LIVENESS_TIMEOUT:?}"
        ))),
    }
}

impl RuntimeSupervisorLazyClient {
    /// Client for the production runtime-supervisor socket.
    pub fn new() -> Self {
        Self::from_connect_fn(Box::new(|| Box::pin(connect_supervisor_proven_live())))
    }

    fn from_connect_fn(connect: SupervisorConnectFn) -> Self {
        Self {
            cache: Arc::new(tokio::sync::Mutex::new(SupervisorClientCache {
                client: None,
                connect,
            })),
        }
    }

    async fn call_unary<Request, Response, Op, OpFut>(
        &self,
        command: &'static str,
        request: Request,
        mut operation: Op,
    ) -> Result<tonic::Response<Response>, Status>
    where
        Request: Clone,
        Op: FnMut(RuntimeSupervisorGrpcClient, Request) -> OpFut,
        OpFut: Future<Output = Result<tonic::Response<Response>, Status>>,
    {
        let policy = SUPERVISOR_RESTART_RETRY_POLICY;
        let started_at = tokio::time::Instant::now();
        let mut retry_index = 0;

        loop {
            let status = match self.attempt(request.clone(), &mut operation).await {
                Ok(response) => return Ok(response),
                Err(SupervisorAttemptFailure::Untransmitted(status)) => status,
                Err(SupervisorAttemptFailure::Terminal(status)) => return Err(status),
            };

            let Some(delay) = policy.backoff_delay(retry_index) else {
                return Err(status);
            };
            if started_at.elapsed() + delay >= policy.max_elapsed {
                return Err(status);
            }
            retry_index += 1;
            pplx_space_metrics::grpc::client_retry(
                "runtime-supervisor",
                command,
                "rpc",
                "connect_failure",
            );
            tracing::debug!(
                command,
                status_code = ?status.code(),
                retry = retry_index,
                retry_in = ?delay,
                "runtime-supervisor RPC failed before transmission; will redial and retry"
            );
            tokio::time::sleep(delay).await;
        }
    }

    async fn attempt<Request, Response, Op, OpFut>(
        &self,
        request: Request,
        operation: &mut Op,
    ) -> Result<tonic::Response<Response>, SupervisorAttemptFailure>
    where
        Op: FnMut(RuntimeSupervisorGrpcClient, Request) -> OpFut,
        OpFut: Future<Output = Result<tonic::Response<Response>, Status>>,
    {
        let client = match self.cache.lock().await.get_or_connect().await {
            Ok(client) => client,
            // A dial failure happens before any request exists, so it is
            // always replay-safe.
            Err(status) => return Err(SupervisorAttemptFailure::Untransmitted(status)),
        };
        match operation(client.client.clone(), request).await {
            Ok(response) => Ok(response),
            Err(status) => {
                // Reconnect is broader than retry: a mid-stream break is not
                // replayed, but the cached channel it used is suspect enough
                // that the next independent RPC should dial fresh.
                if should_reconnect_status(&status) {
                    self.cache.lock().await.clear_if_same(&client);
                }
                // Two untransmitted-by-construction shapes: the dial-level
                // connect failures (socket absent/refusing), and requests the
                // client refused to send on a connection already draining
                // from the predecessor's graceful-shutdown GOAWAY — the race
                // a `systemctl restart` runs against every live cached
                // channel.
                if is_client_connect_failure(&status)
                    || is_request_refused_before_transmission(&status)
                {
                    Err(SupervisorAttemptFailure::Untransmitted(status))
                } else {
                    Err(SupervisorAttemptFailure::Terminal(status))
                }
            }
        }
    }

    runtime_supervisor_lazy_unary_methods! {
        launch_firecracker, proto::LaunchFirecrackerRequest, proto::LaunchFirecrackerResponse;
        launch_sidecar, proto::LaunchSidecarRequest, proto::LaunchSidecarResponse;
        stop_sandbox_runtime, proto::StopSandboxRuntimeRequest, proto::StopSandboxRuntimeResponse;
        bind_execution, proto::BindExecutionRequest, proto::BindExecutionResponse;
        retire_execution, proto::RetireExecutionRequest, proto::RetireExecutionResponse;
        stop_process, proto::StopProcessRequest, proto::StopProcessResponse;
        update_cgroup, proto::UpdateCgroupRequest, proto::UpdateCgroupResponse;
    }
}

impl std::fmt::Debug for RuntimeSupervisorLazyClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuntimeSupervisorLazyClient")
            .finish_non_exhaustive()
    }
}

impl Default for RuntimeSupervisorLazyClient {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
pub(crate) mod tests;
