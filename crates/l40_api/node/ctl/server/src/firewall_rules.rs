//! Firewall rule delivery: node-ctl fetches its worker's current instructions
//! once at start and again on each typed `FirewallRulesChanged` wake, sends
//! them unchanged to node-manager, and reports node-manager's answer to a
//! pending round. A wake means "fetch again", never "install this revision".
//! Only a Report whose acknowledgement may be lost is resent, unchanged;
//! node-manager's replay protection and the control plane's acceptance decide
//! the rest. Only a test enables it.

use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use pplx_space_common_grpc::{GrpcRetryContext, GrpcRetryPolicy};
use pplx_space_node_api_client::proto::{
    FetchFirewallRulesRequest, FetchFirewallRulesResponse, FirewallRulesAssignment,
    FirewallRulesPendingRound, FirewallRulesReportOutcome, ReportFirewallRulesRequest,
    ReportFirewallRulesResponse,
};
use pplx_space_node_mgr_client::NodeManagerGrpcClient;
use pplx_space_node_mgr_client::proto as node_manager;
use tokio::sync::Notify;
use tokio::time::Instant;
use tokio_util::task::AbortOnDropHandle;
use tonic::{Code, Request, Response, Status};

const FETCH_BUDGET: Duration = Duration::from_secs(10);
const SYNC_BUDGET: Duration = Duration::from_secs(10);
/// A Sync that asks a question waits up to this: under node-manager's 120 s
/// limit per call, and over one sweep plus a slot's dispatch wait.
const OBSERVE_BUDGET: Duration = Duration::from_secs(100);
const REPORT_BUDGET: Duration = Duration::from_secs(10);
/// An outer bound only has to catch a hung connect, so it trails the request
/// deadline enough for the deadline's answer to arrive first.
const CONNECT_MARGIN: Duration = Duration::from_secs(2);
/// The control plane's window for a report, from its round's first issue
/// (the firewall rules store's `REPORT_MAX_AGE`). It only bounds how long
/// node-ctl tries.
const REPORT_WINDOW: Duration = Duration::from_secs(300);
/// Two resends of a Report whose answer was lost, within the round's window.
const REPORT_RETRY: GrpcRetryPolicy = GrpcRetryPolicy {
    max_retries: 2,
    initial_delay: Duration::from_secs(1),
    backoff_multiplier: 2.0,
    max_delay: Duration::from_secs(2),
    max_elapsed: REPORT_WINDOW,
    jitter_factor: 0.2,
    max_server_retry_after: Duration::from_secs(2),
};
/// What a round's window keeps after the observation: each Report attempt,
/// and twice the longest backoff before it, which covers its jitter.
const REPORT_RESERVE: Duration = REPORT_BUDGET
    .saturating_add(CONNECT_MARGIN)
    .saturating_add(REPORT_RETRY.max_delay.saturating_mul(2))
    .saturating_mul(REPORT_RETRY.max_retries as u32 + 1);

/// Wakes the delivery task. At most one wake waits, so a wake during a
/// delivery causes exactly one more fetch.
#[derive(Clone)]
pub(crate) struct Wake(Arc<Notify>);

impl Wake {
    pub(crate) fn wake(&self) {
        self.0.notify_one();
    }
}

pub(crate) trait Hops: Send + Sync + 'static {
    fn fetch(&self) -> impl Future<Output = Result<FetchFirewallRulesResponse, Status>> + Send;

    /// `budget` bounds the call on node-manager's side.
    fn sync(
        &self,
        request: node_manager::SyncFirewallRulesRequest,
        budget: Duration,
    ) -> impl Future<Output = Result<node_manager::SyncFirewallRulesResponse, Status>> + Send;

    fn report(
        &self,
        request: ReportFirewallRulesRequest,
    ) -> impl Future<Output = Result<ReportFirewallRulesResponse, Status>> + Send;
}

/// Fetch from and report to the cluster API with node-ctl's bearer, and sync
/// with `manager`.
#[cfg_attr(
    not(any(test, feature = "test-support")),
    expect(dead_code, reason = "production enables no firewall delivery yet")
)]
pub(crate) struct ClusterAndManager {
    pub(crate) manager: NodeManagerGrpcClient,
}

impl Hops for ClusterAndManager {
    async fn fetch(&self) -> Result<FetchFirewallRulesResponse, Status> {
        let mut cluster = crate::connect_cluster_api()
            .await
            .map_err(|error| Status::unavailable(error.to_string()))?;
        let mut request = Request::new(FetchFirewallRulesRequest {});
        request.set_timeout(FETCH_BUDGET);
        cluster
            .fetch_firewall_rules(request)
            .await
            .map(Response::into_inner)
    }

    async fn sync(
        &self,
        request: node_manager::SyncFirewallRulesRequest,
        budget: Duration,
    ) -> Result<node_manager::SyncFirewallRulesResponse, Status> {
        let mut request = crate::local_manager_request(request, false);
        // Bounds the call on node-manager's side too; the caller's timeout
        // also bounds connecting.
        request.set_timeout(budget);
        self.manager
            .clone()
            .sync_firewall_rules(request)
            .await
            .map(Response::into_inner)
    }

    async fn report(
        &self,
        request: ReportFirewallRulesRequest,
    ) -> Result<ReportFirewallRulesResponse, Status> {
        let mut cluster = crate::connect_cluster_api()
            .await
            .map_err(|error| Status::unavailable(error.to_string()))?;
        let mut request = Request::new(request);
        request.set_timeout(REPORT_BUDGET);
        cluster
            .report_firewall_rules(request)
            .await
            .map(Response::into_inner)
    }
}

/// Start the delivery task, which fetches once at start and stops when its
/// handle drops; deliveries never overlap.
#[cfg_attr(
    not(any(test, feature = "test-support")),
    expect(dead_code, reason = "production enables no firewall delivery yet")
)]
pub(crate) fn spawn(hops: impl Hops) -> (Wake, AbortOnDropHandle<()>) {
    let notify = Arc::new(Notify::new());
    let wake = Wake(notify.clone());
    // The startup pass, so work already pending needs no wake.
    wake.wake();
    let delivery = tokio::spawn(async move {
        loop {
            notify.notified().await;
            deliver(&hops).await;
        }
    });
    (wake, AbortOnDropHandle::new(delivery))
}

/// The pending round a pass asks about, and when its answer stops counting.
struct Question {
    round: u64,
    generation: u64,
    deadline: Instant,
}

impl Question {
    /// None for a round an older server sent without its generation or its
    /// issue time: neither is inferred.
    fn of(pending: FirewallRulesPendingRound) -> Option<Self> {
        let generation = pending.assignment_generation?;
        let issued_at = SystemTime::try_from(pending.issued_at?).ok()?;
        // A clock behind the control plane's reads no age.
        let age = SystemTime::now()
            .duration_since(issued_at)
            .unwrap_or_default();
        Some(Self {
            round: pending.round,
            generation,
            deadline: Instant::now() + REPORT_WINDOW.saturating_sub(age),
        })
    }

    /// How long the Sync may wait for the answer and still leave its Report time.
    fn observe_budget(&self) -> Duration {
        let observed_by = self.deadline.checked_sub(REPORT_RESERVE);
        let left = observed_by.map_or(Duration::ZERO, |by| {
            by.saturating_duration_since(Instant::now())
        });
        left.min(OBSERVE_BUDGET)
    }

    fn local(&self) -> node_manager::FirewallRulesQuestion {
        node_manager::FirewallRulesQuestion {
            round: Some(self.round),
            assignment_generation: Some(self.generation),
        }
    }

    /// The Report of `observation`, moved as is, if it answers this question
    /// for `node_id`.
    fn report(
        &self,
        node_id: &str,
        observation: node_manager::FirewallRulesObservation,
    ) -> Option<ReportFirewallRulesRequest> {
        let node_manager::FirewallRulesObservation {
            node_id: observed,
            round,
            assignment_generation,
            revision_id,
            manager_epoch,
            census,
            census_gaps,
            slots,
            extra_namespaces,
        } = observation;
        let answers = observed == node_id
            && round == Some(self.round)
            && assignment_generation == Some(self.generation);
        answers.then_some(ReportFirewallRulesRequest {
            node_id: observed,
            round,
            assignment_generation,
            revision_id,
            manager_epoch,
            census,
            census_gaps,
            slots,
            extra_namespaces,
        })
    }
}

async fn deliver(hops: &impl Hops) {
    let FetchFirewallRulesResponse {
        node_id,
        assignment,
        pending_round,
    } = match tokio::time::timeout(FETCH_BUDGET + CONNECT_MARGIN, hops.fetch()).await {
        Ok(Ok(response)) => response,
        Ok(Err(error)) => {
            tracing::warn!(%error, "firewall rules fetch failed");
            return;
        }
        Err(_elapsed) => {
            tracing::warn!("firewall rules fetch timed out");
            return;
        }
    };
    // Asked only while the round leaves its answer at least an assignment's budget.
    let question = pending_round
        .and_then(Question::of)
        .filter(|question| question.observe_budget() >= SYNC_BUDGET);
    // No assignment leaves local state alone; one to an empty revision doesn't.
    if assignment.is_none() && question.is_none() {
        return;
    }
    let budget = question
        .as_ref()
        .map_or(SYNC_BUDGET, Question::observe_budget);
    let request = node_manager::SyncFirewallRulesRequest {
        node_id: node_id.clone(),
        assignment: assignment.map(
            |FirewallRulesAssignment {
                 generation,
                 revision_id,
                 revision,
             }| node_manager::FirewallRulesAssignment {
                generation,
                revision_id,
                revision,
            },
        ),
        question: question.as_ref().map(Question::local),
    };
    // A lost reply proves neither outcome, so this claims neither.
    let reply =
        match tokio::time::timeout(budget + CONNECT_MARGIN, hops.sync(request, budget)).await {
            Ok(Ok(reply)) => reply,
            Ok(Err(error)) => {
                tracing::warn!(%error, "node-manager didn't answer firewall sync");
                return;
            }
            Err(_elapsed) => {
                tracing::warn!("node-manager didn't answer firewall sync in time");
                return;
            }
        };
    tracing::debug!(?reply.acceptance, ?reply.dispatch, "node-manager answered firewall sync");
    let Some(question) = question else {
        return;
    };
    let Some(observation) = reply.observation else {
        tracing::info!(
            question.round,
            unanswered = reply.unanswered,
            "firewall round unanswered"
        );
        return;
    };
    let Some(report) = question.report(&node_id, observation) else {
        tracing::warn!(
            question.round,
            "node-manager answered another firewall question"
        );
        return;
    };
    send(hops, report, question.deadline).await;
}

/// Sends `report` until the control plane answers it, resending it unchanged
/// after a lost answer, at most until `deadline`.
async fn send(hops: &impl Hops, report: ReportFirewallRulesRequest, deadline: Instant) {
    let round = report.round;
    let context =
        GrpcRetryContext::with_timeout(deadline.saturating_duration_since(Instant::now()));
    let sent = pplx_space_common_grpc::retry_grpc_status_with_context_if(
        "cluster-api",
        "ReportFirewallRules",
        REPORT_RETRY,
        context,
        // Errors a stored report can also come back as.
        |status| {
            matches!(
                status.code(),
                Code::Internal | Code::DeadlineExceeded | Code::Cancelled
            )
        },
        || {
            let attempt = hops.report(report.clone());
            async move {
                tokio::time::timeout(REPORT_BUDGET + CONNECT_MARGIN, attempt)
                    .await
                    .unwrap_or_else(|_elapsed| {
                        Err(Status::deadline_exceeded("firewall rules report timed out"))
                    })
            }
        },
    )
    .await;
    match sent.map(|reply| reply.outcome()) {
        Ok(FirewallRulesReportOutcome::Accepted | FirewallRulesReportOutcome::Duplicate) => {
            tracing::debug!(?round, "firewall rules report stored");
        }
        // Final for this answer; the next wake fetches again.
        Ok(outcome) => tracing::warn!(?round, ?outcome, "firewall rules report not accepted"),
        Err(error) => tracing::warn!(?round, %error, "firewall rules report failed"),
    }
}

/// Serve node-ctl's gRPC service on `listener` with firewall delivery on,
/// fetching from and reporting to `cluster_endpoint` with the bearer
/// production reads, and syncing with the node-manager on `node_mgr_socket`.
/// The reconcile worker doesn't run. Call once per process.
#[cfg(any(test, feature = "test-support"))]
pub async fn serve_firewall_wake_for_test(
    listener: tokio::net::UnixListener,
    cluster_endpoint: String,
    node_mgr_socket: &std::path::Path,
    shutdown: tokio_util::sync::CancellationToken,
) -> Result<(), tonic::transport::Error> {
    crate::CLUSTER_NODE_API_ENDPOINT
        .set(Some(cluster_endpoint))
        .expect("cluster endpoint configured once per process");
    // Nothing drains the reconcile queue, so drains are dropped.
    let (tx, _) = tokio::sync::mpsc::channel(1);
    let (wake, delivery) = spawn(ClusterAndManager {
        manager: pplx_space_node_mgr_client::lazy_at(node_mgr_socket),
    });
    let service = crate::NodeCtlService::new(
        tx.clone(),
        crate::ClusterReconcileTrigger::new(tx, ""),
        Arc::new(std::sync::Mutex::new(
            rusqlite::Connection::open_in_memory().expect("open in-memory SQLite"),
        )),
        Some(wake),
    );
    let served = pplx_space_grpc_server::grpc_server_builder()
        .add_service(
            pplx_space_node_api_client::proto::node_ctl_server::NodeCtlServer::new(service),
        )
        .serve_with_incoming_shutdown(
            tokio_stream::wrappers::UnixListenerStream::new(listener),
            shutdown.cancelled_owned(),
        )
        .await;
    // Stopping delivery wherever it is means no Sync or Report starts after this returns.
    delivery.abort();
    let _stopped = delivery.await;
    served
}

#[cfg(test)]
mod tests;
