//! Deadline updates across a worker restart. A restart drops the API's pooled
//! connection: the redial then fails before any bytes are sent, or an
//! in-flight request loses its connection after the worker may have applied
//! it. The first is safe to replay; the second must be observed, never resent.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use pplx_space_common_core::types::{NodeId, SandboxId};
use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, TestDb, pg_test_db};
use pplx_space_node_api_sim::SimWorkerHarness;
use pplx_space_sbx_mgr_client::proto::{
    AutoLifecycleStatus, SandboxInfo, SandboxLifecycleState, StateResponse,
};
use pplx_space_test_env::TestResult;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;

use axum::http::StatusCode;
use axum::response::IntoResponse;

use super::*;

const GENERATION: i64 = 7;
const HTTP2_PREFACE_LEN: usize = 24;
const HTTP2_FRAME_HEADER_LEN: usize = 9;
const HTTP2_HEADERS_FRAME: u8 = 0x1;

fn timestamp(seconds: i64) -> prost_types::Timestamp {
    prost_types::Timestamp { seconds, nanos: 0 }
}

fn reset_operation() -> AccessIdleOperation {
    AccessIdleOperation::ResetPolicy(crate::controllers::auto_lifecycle::policy_to_proto(
        &AutoLifecyclePolicy::default(),
    ))
}

async fn refused_dial_status() -> TestResult<tonic::Status> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    drop(listener);
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))?.connect_lazy();
    let status = tonic_health::pb::health_client::HealthClient::new(channel)
        .check(tonic_health::pb::HealthCheckRequest::default())
        .await
        .expect_err("nothing listens on a released port");
    Ok(status)
}

#[tokio::test]
async fn deadline_send_failures_split_by_whether_the_worker_saw_the_request() -> TestResult {
    let refused = refused_dial_status().await?;
    assert_eq!(
        classify_access_idle_send_failure(&refused),
        AccessIdleSendFailure::Untransmitted,
        "{refused:?}"
    );
    for (label, status) in [
        // Only a local dial error proves non-transmission; the same words
        // decoded from a peer's trailers prove nothing.
        (
            "echoed refusal",
            tonic::Status::unavailable("connection refused"),
        ),
        ("reset", tonic::Status::unknown("transport error")),
        (
            "keepalive",
            tonic::Status::internal("h2 protocol error: http2 error: keep-alive timed out"),
        ),
        (
            "closed",
            tonic::Status::cancelled("operation was canceled: connection closed"),
        ),
    ] {
        assert_eq!(
            classify_access_idle_send_failure(&status),
            AccessIdleSendFailure::Ambiguous,
            "{label}"
        );
    }
    for status in [
        tonic::Status::failed_precondition("access deadline execution changed"),
        tonic::Status::deadline_exceeded("access deadline update expired"),
        tonic::Status::internal("persist access deadline: disk full"),
    ] {
        assert_eq!(
            classify_access_idle_send_failure(&status),
            AccessIdleSendFailure::Rejected,
            "{status:?}"
        );
    }
    Ok(())
}

#[test]
fn unconfirmed_deadline_update_is_a_paced_retryable_unavailable() {
    let response = unconfirmed_access_idle_update().into_response();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        response.headers().get(axum::http::header::RETRY_AFTER),
        Some(&axum::http::HeaderValue::from_static("1"))
    );
}

#[test]
fn only_an_observed_extension_floor_proves_an_ambiguous_update() {
    let sandbox_id = SandboxId::new();
    let until = 2_000;
    let extend = AccessIdleOperation::ExtendUntil(timestamp(until));
    let started = |generation, next_auto_event_at: Option<i64>| StateResponse {
        sandboxes: vec![SandboxInfo {
            sandbox_id: sandbox_id.to_string(),
            lifecycle: Some(SandboxLifecycleState::Started as i32),
            execution_generation: generation,
            auto_lifecycle_status: Some(AutoLifecycleStatus {
                next_auto_event_at: next_auto_event_at.map(timestamp),
                ..Default::default()
            }),
            ..Default::default()
        }],
        missing_ids: Vec::new(),
    };
    let observed = |operation: &AccessIdleOperation, response| {
        access_idle_update_observed(sandbox_id, GENERATION, operation, response)
    };

    assert!(observed(&extend, started(Some(GENERATION), Some(until))).unwrap());
    assert!(observed(&extend, started(Some(GENERATION), Some(until + 60))).unwrap());
    assert!(!observed(&extend, started(Some(GENERATION), Some(until - 1))).unwrap());
    assert!(!observed(&extend, started(Some(GENERATION), None)).unwrap());
    // An unidentified execution's deadline cannot be attributed to ours.
    assert!(!observed(&extend, started(None, Some(until))).unwrap());
    // A restarted idle clock is indistinguishable from other activity.
    assert!(!observed(&reset_operation(), started(Some(GENERATION), Some(until))).unwrap());
    assert!(
        !observed(
            &extend,
            StateResponse {
                sandboxes: Vec::new(),
                missing_ids: vec![sandbox_id.to_string()],
            },
        )
        .unwrap()
    );
    let replaced = observed(&extend, started(Some(GENERATION + 1), Some(until)))
        .expect_err("a replacement execution must not satisfy the fence");
    assert_eq!(replaced.status(), StatusCode::PRECONDITION_FAILED);
}

/// A TCP front for a worker whose connections can all be severed at once,
/// standing in for the node process restarting under a pooled channel.
struct RestartingFront {
    addr: std::net::SocketAddr,
    tasks: JoinSet<()>,
}

impl RestartingFront {
    async fn start(backend: std::net::SocketAddr) -> TestResult<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let mut tasks = JoinSet::new();
        tasks.spawn(async move {
            let mut forwards = JoinSet::new();
            while let Ok((mut inbound, _)) = listener.accept().await {
                forwards.spawn(async move {
                    if let Ok(mut outbound) = TcpStream::connect(backend).await {
                        let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
                    }
                });
            }
        });
        Ok(Self { addr, tasks })
    }

    /// Dropping the accept task drops its listener and every forward.
    async fn stop(mut self) {
        self.tasks.shutdown().await;
    }
}

/// Accepts one connection and closes it as soon as a request's HEADERS
/// frame arrives: the worker received the request but never answered.
async fn start_dropping_worker() -> TestResult<(std::net::SocketAddr, Arc<AtomicUsize>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let requests = Arc::new(AtomicUsize::new(0));
    let seen = Arc::clone(&requests);
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let seen = Arc::clone(&seen);
            tokio::spawn(async move {
                let mut preface = [0; HTTP2_PREFACE_LEN];
                if stream.read_exact(&mut preface).await.is_err() {
                    return;
                }
                // An empty server SETTINGS frame completes the handshake.
                let _ = stream.write_all(&[0, 0, 0, 0x4, 0, 0, 0, 0, 0]).await;
                loop {
                    let mut header = [0; HTTP2_FRAME_HEADER_LEN];
                    if stream.read_exact(&mut header).await.is_err() {
                        return;
                    }
                    let length = usize::from(header[0]) << 16
                        | usize::from(header[1]) << 8
                        | usize::from(header[2]);
                    let mut payload = vec![0; length];
                    if stream.read_exact(&mut payload).await.is_err() {
                        return;
                    }
                    if header[3] == HTTP2_HEADERS_FRAME {
                        seen.fetch_add(1, Ordering::SeqCst);
                        return;
                    }
                }
            });
        }
    });
    Ok((addr, requests))
}

struct Fixture {
    db: TestDb,
    controller: SandboxController,
    node: NodeId,
    sandbox: SandboxId,
}

impl Fixture {
    async fn new(endpoint: &str) -> TestResult<Self> {
        let db = pg_test_db().await?;
        let node = NodeId::new();
        sqlx::query(sql!(
            "INSERT INTO nodes (id, address, role, managed_by, state)
             VALUES ($1, '127.0.0.1', 'worker', 'space', 'registered')"
        ))
        .bind(node)
        .execute(&db.pool)
        .await?;
        sqlx::query(sql!(
            "INSERT INTO node_direct (node_id, node_api_endpoint) VALUES ($1, $2)"
        ))
        .bind(node)
        .bind(endpoint)
        .execute(&db.pool)
        .await?;
        let sandbox = SandboxId::new();
        sqlx::query(sql!(
            "INSERT INTO sandboxes
                 (id, user_id, org_id, managed_by, lifecycle, placement,
                  active_node_id, network_policy_generation)
             VALUES ($1, $2, $3, 'space', 'started', 'on_node', $4, 1)"
        ))
        .bind(sandbox)
        .bind(SEED_USER_ID)
        .bind(SEED_ORG_ID)
        .bind(node)
        .execute(&db.pool)
        .await?;
        let controller =
            SandboxController::new(crate::controllers::substrate::Substrate::Direct(Box::new(
                crate::controllers::substrate::direct::DirectSubstrate::new(db.pool.clone()),
            )));
        Ok(Self {
            db,
            controller,
            node,
            sandbox,
        })
    }

    async fn move_worker(&self, endpoint: &str) -> TestResult {
        sqlx::query(sql!(
            "UPDATE node_direct SET node_api_endpoint = $2 WHERE node_id = $1"
        ))
        .bind(self.node)
        .bind(endpoint)
        .execute(&self.db.pool)
        .await?;
        Ok(())
    }

    async fn send(
        &self,
        client: RuntimeClient,
        operation: AccessIdleOperation,
    ) -> Result<(), ControllerError> {
        self.controller
            .send_access_idle_deadline(
                client,
                self.sandbox,
                GENERATION,
                timestamp(jiff::Timestamp::now().as_second()),
                tokio::time::Instant::now() + Duration::from_secs(10),
                operation,
            )
            .await
    }
}

async fn start_worker(sandbox: SandboxId) -> TestResult<SimWorkerHarness> {
    let worker = SimWorkerHarness::builder(NodeId::new().to_string())
        .start()
        .await?;
    let key = sandbox.to_string();
    worker.state().ensure_default_running(&key).await?;
    worker
        .state()
        .set_route_identity_provenance(&key, Some(1), Some(GENERATION), None)
        .await?;
    Ok(worker)
}

#[tokio::test(flavor = "current_thread")]
async fn untransmitted_update_is_replayed_once_on_a_fresh_client() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    for (label, operation) in [
        (
            "extension",
            AccessIdleOperation::ExtendUntil(timestamp(jiff::Timestamp::now().as_second() + 900)),
        ),
        ("reset", reset_operation()),
    ] {
        let placeholder = TcpListener::bind("127.0.0.1:0").await?;
        let fixture = Fixture::new(&placeholder.local_addr()?.to_string()).await?;
        drop(placeholder);
        let worker = start_worker(fixture.sandbox).await?;
        let backend: std::net::SocketAddr = worker.endpoint().parse()?;
        let front = RestartingFront::start(backend).await?;
        fixture.move_worker(&front.addr.to_string()).await?;
        // Use the pod-client dial path: pooled direct channels retry refused
        // redials internally before surfacing the failure.
        let client = crate::controllers::substrate::connect_worker(
            tonic::transport::Endpoint::from_shared(format!("http://{}", front.addr))?,
        )
        .await?;
        front.stop().await;
        // Let the client observe the closed connection, so the next request
        // takes the redial path rather than racing the close.
        tokio::time::sleep(Duration::from_millis(100)).await;
        fixture.move_worker(worker.endpoint()).await?;

        let mut reader = client.clone();
        let state = reader
            .call(
                &fixture.controller.substrate,
                fixture.sandbox,
                tokio::time::Instant::now() + Duration::from_secs(10),
                tonic::Request::new(pplx_space_sbx_mgr_client::proto::StateRequest {
                    sandbox_ids: vec![fixture.sandbox.to_string()],
                }),
                |mut client, request| async move { client.sandbox_state(request).await },
            )
            .await?
            .into_inner();
        assert_eq!(state.sandboxes.len(), 1);
        assert_eq!(state.sandboxes[0].execution_generation, Some(GENERATION));

        fixture
            .send(client, operation.clone())
            .await
            .map_err(|error| format!("{label}: {error}"))?;

        let record = worker.state().get(&fixture.sandbox.to_string()).await?;
        match operation {
            AccessIdleOperation::ExtendUntil(until) => {
                assert_eq!(record.access_extended_until, Some(until.seconds), "{label}");
            }
            AccessIdleOperation::ResetPolicy(policy) => {
                use prost::Message;
                assert_eq!(record.access_idle_policy, policy.encode_to_vec(), "{label}");
            }
        }
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn interrupted_update_is_observed_not_resent() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    for (label, operation) in [
        (
            "extension",
            AccessIdleOperation::ExtendUntil(timestamp(jiff::Timestamp::now().as_second() + 900)),
        ),
        ("reset", reset_operation()),
    ] {
        let (dropping, requests) = start_dropping_worker().await?;
        let fixture = Fixture::new(&dropping.to_string()).await?;
        let interrupted = crate::controllers::substrate::connect_worker(
            tonic::transport::Endpoint::from_shared(format!("http://{dropping}"))?,
        )
        .await?;
        // The observer reaches the restarted worker, which cannot attribute
        // any deadline to this update.
        let worker = start_worker(fixture.sandbox).await?;
        fixture.move_worker(worker.endpoint()).await?;
        let before = worker.state().get(&fixture.sandbox.to_string()).await?;

        let error = fixture
            .send(interrupted, operation)
            .await
            .expect_err("an unobserved update cannot succeed");

        assert_eq!(error.status(), StatusCode::SERVICE_UNAVAILABLE, "{label}");
        assert!(error.retry_after_header().is_some(), "{label}");
        assert_eq!(requests.load(Ordering::SeqCst), 1, "{label}: sent once");
        let after = worker.state().get(&fixture.sandbox.to_string()).await?;
        assert_eq!(
            after.access_extended_until, before.access_extended_until,
            "{label}: no resend to the successor"
        );
        assert_eq!(
            after.access_idle_policy, before.access_idle_policy,
            "{label}: no resend to the successor"
        );
    }
    Ok(())
}

// A worker whose address stops answering SYNs fails a request in the pooled
// pod client's redial, before any bytes exist. SPACE's TCP connector marks
// that failure, so it is untransmitted and `RuntimeClient::call` replays it
// exactly once, on the fresh client the probe resolves, which reaches the
// restarted worker; the access deadline update then applies.
#[cfg(target_os = "linux")]
#[tokio::test(flavor = "current_thread")]
async fn a_blackholed_worker_redial_is_untransmitted_and_replayed_once() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let placeholder = TcpListener::bind("127.0.0.1:0").await?;
    let fixture = Fixture::new(&placeholder.local_addr()?.to_string()).await?;
    drop(placeholder);
    let worker = start_worker(fixture.sandbox).await?;
    let backend: std::net::SocketAddr = worker.endpoint().parse()?;
    let front = RestartingFront::start(backend).await?;
    let front_addr = front.addr;
    let client = crate::controllers::substrate::connect_worker(
        tonic::transport::Endpoint::from_shared(format!("http://{front_addr}"))?
            .connect_timeout(Duration::from_millis(500)),
    )
    .await?;
    front.stop().await;
    // Let the client observe the closed connection, so the next request
    // redials instead of racing the close.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let blackhole = pplx_space_common_grpc::test_support::Blackhole::bind(front_addr).await?;
    fixture.move_worker(worker.endpoint()).await?;
    let state_request = || {
        tonic::Request::new(pplx_space_sbx_mgr_client::proto::StateRequest {
            sandbox_ids: vec![fixture.sandbox.to_string()],
        })
    };

    let status = client
        .clone()
        .sandbox_state(state_request())
        .await
        .expect_err("nothing answers the redial");
    // A timed-out dial, not a refused one, so only the connector's marker can
    // prove the request was never sent.
    let timed_out =
        std::iter::successors(std::error::Error::source(&status), |error| error.source()).any(
            |error| {
                error
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|error| error.kind() == std::io::ErrorKind::TimedOut)
            },
        );
    assert!(timed_out, "{status:?}");
    assert_eq!(
        classify_access_idle_send_failure(&status),
        AccessIdleSendFailure::Untransmitted,
        "{status:?}"
    );

    let mut attempts = 0;
    let state = client
        .clone()
        .call(
            &fixture.controller.substrate,
            fixture.sandbox,
            tokio::time::Instant::now() + Duration::from_secs(10),
            state_request(),
            |mut client, request| {
                attempts += 1;
                async move { client.sandbox_state(request).await }
            },
        )
        .await?
        .into_inner();
    assert_eq!(attempts, 2, "one attempt, then one replay");
    assert_eq!(state.sandboxes[0].execution_generation, Some(GENERATION));

    let until = timestamp(jiff::Timestamp::now().as_second() + 900);
    fixture
        .send(client, AccessIdleOperation::ExtendUntil(until))
        .await?;
    let record = worker.state().get(&fixture.sandbox.to_string()).await?;
    assert_eq!(record.access_extended_until, Some(until.seconds));
    drop(blackhole);
    Ok(())
}
