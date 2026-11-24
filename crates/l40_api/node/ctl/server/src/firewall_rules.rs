//! Firewall rule delivery: a typed `FirewallRulesChanged` wake makes node-ctl
//! fetch its worker's current assignment and send it unchanged to node-manager.
//! A wake means "fetch again", never "install this revision". Nothing here
//! retries or remembers a delivery; node-manager's replay protection and the
//! control plane's verification decide. Only a test enables it.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use pplx_space_node_api_client::proto::{
    FetchFirewallRulesRequest, FetchFirewallRulesResponse, FirewallRulesAssignment,
};
use pplx_space_node_mgr_client::NodeManagerGrpcClient;
use pplx_space_node_mgr_client::proto as node_manager;
use tokio::sync::Notify;
use tokio_util::task::AbortOnDropHandle;
use tonic::{Request, Response, Status};

const FETCH_BUDGET: Duration = Duration::from_secs(10);
const SYNC_BUDGET: Duration = Duration::from_secs(10);
/// An outer bound only has to catch a hung connect, so it trails the request
/// deadline enough for the deadline's answer to arrive first.
const CONNECT_MARGIN: Duration = Duration::from_secs(2);

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

    fn sync(
        &self,
        request: node_manager::SyncFirewallRulesRequest,
    ) -> impl Future<Output = Result<node_manager::SyncFirewallRulesResponse, Status>> + Send;
}

/// Fetch from the cluster API with node-ctl's bearer, and sync with `manager`.
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
    ) -> Result<node_manager::SyncFirewallRulesResponse, Status> {
        let mut request = crate::local_manager_request(request, false);
        // Bounds the call on node-manager's side too; the caller's timeout
        // also bounds connecting.
        request.set_timeout(SYNC_BUDGET);
        self.manager
            .clone()
            .sync_firewall_rules(request)
            .await
            .map(Response::into_inner)
    }
}

/// Start the delivery task, which stops when its handle drops; deliveries
/// never overlap.
#[cfg_attr(
    not(any(test, feature = "test-support")),
    expect(dead_code, reason = "production enables no firewall delivery yet")
)]
pub(crate) fn spawn(hops: impl Hops) -> (Wake, AbortOnDropHandle<()>) {
    let notify = Arc::new(Notify::new());
    let wake = Wake(notify.clone());
    let delivery = tokio::spawn(async move {
        loop {
            notify.notified().await;
            deliver(&hops).await;
        }
    });
    (wake, AbortOnDropHandle::new(delivery))
}

async fn deliver(hops: &impl Hops) {
    let FetchFirewallRulesResponse {
        node_id,
        assignment,
        // This PR neither acts on nor acknowledges a verification round.
        pending_round: _,
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
    // No assignment leaves local state alone; one to an empty revision doesn't.
    let Some(FirewallRulesAssignment {
        generation,
        revision_id,
        revision,
    }) = assignment
    else {
        return;
    };
    let request = node_manager::SyncFirewallRulesRequest {
        node_id,
        assignment: Some(node_manager::FirewallRulesAssignment {
            generation,
            revision_id,
            revision,
        }),
        question: None,
    };
    // A lost reply proves neither outcome, so this claims neither.
    match tokio::time::timeout(SYNC_BUDGET + CONNECT_MARGIN, hops.sync(request)).await {
        Ok(Ok(response)) => tracing::debug!(?response, "node-manager answered firewall sync"),
        Ok(Err(error)) => {
            tracing::warn!(generation, %error, "node-manager didn't confirm firewall generation");
        }
        Err(_elapsed) => {
            tracing::warn!(
                generation,
                "node-manager didn't confirm firewall generation in time"
            );
        }
    }
}

/// Serve node-ctl's gRPC service on `listener` with firewall delivery on,
/// fetching from `cluster_endpoint` with the bearer production reads and
/// syncing with the node-manager on `node_mgr_socket`. The reconcile worker
/// doesn't run. Call once per process.
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
    // Stopping delivery wherever it is means no Sync starts after this returns.
    delivery.abort();
    let _stopped = delivery.await;
    served
}

#[cfg(test)]
mod tests;
