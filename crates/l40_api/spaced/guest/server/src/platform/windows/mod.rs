//! Native Windows guest implementation of the shared spaced bridge protocol.
#![cfg(windows)]

mod execution;
mod filesystem;
mod identity;
mod launch;
mod lifecycle;
mod meta;
mod path;
mod process;
mod recovery;
pub use recovery::{RecoveryError, recover};
mod service;
mod transport;
#[path = "../../maintenance.rs"]
mod update;
mod update_backend;

pub use identity::IdentityError;
use pplx_space_spaced_guest_protocol::proto::bridge_guest::{
    filesystem_server::FilesystemServer, guest_update_server::GuestUpdateServer,
    process_server::ProcessServer,
};
pub use service::EventLogWriter;
use std::{sync::Arc, time::Duration};
use tokio::sync::{Semaphore, watch};
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};

#[derive(Clone)]
pub(super) struct SpacedGuestService {
    inner: Arc<ServiceInner>,
}
struct ServiceInner {
    update: update::UpdateRuntime,
    execution: execution::Execution,
    service_generation: String,
    shutdown: CancellationToken,
    loopback_bridge: Forwarding,
    ready: watch::Sender<bool>,
    scm_status: Option<windows_service::service_control_handler::ServiceStatusHandle>,
}
struct Forwarding;
impl Forwarding {
    async fn authorized_ports(&self) -> Vec<u32> {
        vec![]
    }
    async fn replace_ports(&self, ports: &[u16]) -> Result<(), Status> {
        if ports.is_empty() {
            Ok(())
        } else {
            Err(unsupported("Windows forwarding is not qualified"))
        }
    }
}
impl SpacedGuestService {
    fn ready(&self) {
        self.inner.ready.send_replace(true);
    }
}

fn unsupported(message: &'static str) -> tonic::Status {
    pplx_space_guest_capability::guest_capability_unsupported_status(
        pplx_space_guest_capability::GuestCapability::PlatformOperations,
        message,
    )
}

#[derive(Debug, thiserror::Error)]
pub enum RunError {
    #[error("Windows guest identity failed: {0}")]
    Identity(#[from] IdentityError),
    #[error("Windows service manager failed: {0}")]
    Service(#[from] windows_service::Error),
    #[error("Windows guest transport failed: {0}")]
    Transport(#[from] std::io::Error),
    #[error("Windows guest RPC server failed: {0}")]
    Server(#[from] tonic::transport::Error),
    #[error("Windows guest maintenance initialization failed: {0}")]
    Maintenance(#[from] tonic::Status),
    #[error("Windows guest recovery initialization failed: {0}")]
    Recovery(#[from] RecoveryError),
}

/// Starts only the operations implemented and declared by this native backend.
/// The listener accepts host-CID connections; no IP interface is exposed.
pub fn run(console: bool) -> Result<(), RunError> {
    if console {
        serve(CancellationToken::new(), None)
    } else {
        service::dispatch().map_err(RunError::from)
    }
}

fn serve(
    shutdown: CancellationToken,
    scm_status: Option<windows_service::service_control_handler::ServiceStatusHandle>,
) -> Result<(), RunError> {
    let identity = identity::Identity::load()?.map(std::sync::Arc::new);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    // Opening the viosock device uses synchronous provider IOCTLs. Do it on
    // the service entry thread, before entering the async server.
    let mut incoming = {
        let _entered = runtime.enter();
        transport::Listener::bind(
            pplx_space_spaced_guest_protocol::BRIDGE_GUEST_VSOCK_PORT,
            shutdown.clone(),
        )?
    };
    let result = runtime.block_on(async {
        let stop = shutdown.clone();
        let signal = tokio::spawn(async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                stop.cancel();
            }
        });
        let tasks = tokio_util::task::TaskTracker::new();
        let admission = execution::Admission::new(shutdown.clone());
        let filesystem = identity.clone().map(|identity| {
            filesystem::Filesystem::new(identity, tasks.clone(), admission.clone())
        });
        let processes = identity.clone().map(|identity| {
            process::Processes::new(identity, shutdown.clone(), tasks.clone(), admission.clone())
        });
        let service = SpacedGuestService {
            inner: Arc::new(ServiceInner {
                update: update::UpdateRuntime::new(),
                execution: execution::Execution {
                    admission,
                    filesystem: filesystem.clone(),
                    processes: processes.clone(),
                },
                service_generation: uuid::Uuid::now_v7().to_string(),
                shutdown: shutdown.clone(),
                loopback_bridge: Forwarding,
                ready: watch::channel(false).0,
                scm_status,
            }),
        };
        let result: Result<(), RunError> = async {
            service.prepare_update_startup().await?;
            if !service.inner.update.trial_gated() {
                service.publish_platform_readiness().await?;
                service.ready();
            }
            let server = tonic::transport::Server::builder()
                .concurrency_limit_per_connection(32)
                .max_concurrent_streams(128)
                .http2_keepalive_interval(Some(Duration::from_secs(10)))
                .http2_keepalive_timeout(Some(Duration::from_secs(5)))
                .add_service(meta::service(identity.is_some(), service.clone()))
                .add_service(
                    GuestUpdateServer::new(service.clone()).max_decoding_message_size(128 * 1024),
                )
                .add_optional_service(filesystem.map(FilesystemServer::new))
                .add_optional_service(processes.map(ProcessServer::new))
                .serve_with_incoming_shutdown(incoming.stream(), shutdown.cancelled());
            server.await.map_err(RunError::from)
        }
        .await;
        shutdown.cancel();
        service.inner.update.drain().await;
        signal.abort();
        let _ = signal.await;
        incoming.close().await?;
        tasks.close();
        tasks.wait().await;
        result
    });
    runtime.shutdown_timeout(std::time::Duration::from_secs(5));
    result
}
