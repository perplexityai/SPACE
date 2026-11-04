//! Shared in-process test harness for spawning a [`SimState`]-backed worker
//! node-api simulator on an ephemeral TCP port.
//!
//! Every SPACE test suite that needs a fake worker node-api to point a
//! `NodeApiClient`, `Substrate`, or ECW route at rebuilt the same shape by
//! hand: bind `127.0.0.1:0`, mount `SimWorkerApiService` (optionally
//! `SimWorkerApiInternalService`), serve with a `CancellationToken`-driven
//! graceful shutdown, and abort the server task on `Drop`. This harness is
//! the single implementation; callers select the services and environment
//! they need through [`SimWorkerHarnessBuilder`].

use std::io;

use pplx_space_node_api_client::proto::{
    worker_api_internal_server::WorkerApiInternalServer, worker_api_server::WorkerApiServer,
};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tonic::transport::{Server, server::TcpIncoming};

use crate::state::SimState;
use crate::worker::{SimWorkerApiInternalService, SimWorkerApiService};

/// Failure modes for standing up a [`SimWorkerHarness`].
#[derive(Debug, thiserror::Error)]
pub enum SimWorkerHarnessError {
    #[error("failed to create sim worker temp directory: {0}")]
    TempDir(#[source] io::Error),
    #[error("failed to bind sim worker listener: {0}")]
    Bind(#[source] io::Error),
}

/// A running in-process `space-node-api-svc` simulator, bound to an
/// ephemeral `127.0.0.1` TCP port.
///
/// Dropping the harness cancels the server's shutdown token and aborts its
/// serving task; the backing [`tempfile::TempDir`] is removed when the
/// harness itself is dropped.
pub struct SimWorkerHarness {
    endpoint: String,
    port: u16,
    state: SimState,
    shutdown: CancellationToken,
    task: JoinHandle<()>,
    _dir: tempfile::TempDir,
}

impl Drop for SimWorkerHarness {
    fn drop(&mut self) {
        self.shutdown.cancel();
        self.task.abort();
    }
}

impl SimWorkerHarness {
    /// Start building a harness for a simulated worker node identified as
    /// `node_id`.
    pub fn builder(node_id: impl Into<String>) -> SimWorkerHarnessBuilder {
        SimWorkerHarnessBuilder {
            node_id: node_id.into(),
            with_internal_api: false,
            activation_policy_support: true,
        }
    }

    /// The `host:port` the simulator is listening on.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// The TCP port the simulator is listening on.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// The in-memory state backing the simulator, shared with every RPC
    /// handler the harness spawned.
    pub fn state(&self) -> &SimState {
        &self.state
    }
}

/// Builder for [`SimWorkerHarness`]. See the module docs for the duplication
/// this replaces.
pub struct SimWorkerHarnessBuilder {
    node_id: String,
    with_internal_api: bool,
    activation_policy_support: bool,
}

impl SimWorkerHarnessBuilder {
    /// Also mount `WorkerApiInternal` (pool adoption, wake) alongside the
    /// always-mounted `WorkerApi` surface.
    pub fn with_internal_api(mut self) -> Self {
        self.with_internal_api = true;
        self
    }

    /// Model a worker predating policy-bearing activation requests.
    pub fn without_activation_policy_support(mut self) -> Self {
        self.activation_policy_support = false;
        self
    }

    /// Start the simulator and return the running harness.
    pub async fn start(self) -> Result<SimWorkerHarness, SimWorkerHarnessError> {
        let dir = tempfile::tempdir().map_err(SimWorkerHarnessError::TempDir)?;
        let state = SimState::new(
            dir.path().join("snapshots"),
            dir.path().join("sandboxes"),
            self.node_id,
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(SimWorkerHarnessError::Bind)?;
        let addr = listener.local_addr().map_err(SimWorkerHarnessError::Bind)?;
        let shutdown = CancellationToken::new();
        let server_shutdown = shutdown.clone();
        let service = SimWorkerApiService::new(state.clone());

        let (health_reporter, health_service) = pplx_space_common_svc::grpc_health_service();
        if self.activation_policy_support {
            health_reporter
                .set_service_status(
                    pplx_space_sbx_mgr_client::ACTIVATION_POLICY_HEALTH_SERVICE,
                    tonic_health::ServingStatus::Serving,
                )
                .await;
        }
        let task = if self.with_internal_api {
            let internal_service = SimWorkerApiInternalService::new(state.clone());
            health_reporter
                .set_service_status(
                    pplx_space_sbx_mgr_client::OPERATION_ACTIVATION_RECEIPT_HEALTH_SERVICE,
                    tonic_health::ServingStatus::Serving,
                )
                .await;
            tokio::spawn(async move {
                let result = Server::builder()
                    .add_service(health_service)
                    .add_service(WorkerApiServer::new(service))
                    .add_service(WorkerApiInternalServer::new(internal_service))
                    .serve_with_incoming_shutdown(
                        TcpIncoming::from(listener).with_nodelay(Some(true)),
                        server_shutdown.cancelled_owned(),
                    )
                    .await;
                if let Err(error) = result {
                    tracing::warn!(%error, "test sim worker server exited with error");
                }
            })
        } else {
            tokio::spawn(async move {
                let result = Server::builder()
                    .add_service(health_service)
                    .add_service(WorkerApiServer::new(service))
                    .serve_with_incoming_shutdown(
                        TcpIncoming::from(listener).with_nodelay(Some(true)),
                        server_shutdown.cancelled_owned(),
                    )
                    .await;
                if let Err(error) = result {
                    tracing::warn!(%error, "test sim worker server exited with error");
                }
            })
        };

        Ok(SimWorkerHarness {
            endpoint: addr.to_string(),
            port: addr.port(),
            state,
            shutdown,
            task,
            _dir: dir,
        })
    }
}
