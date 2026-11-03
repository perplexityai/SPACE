//! Native Windows guest implementation of the shared spaced bridge protocol.
#![cfg(windows)]

mod filesystem;
mod identity;
mod launch;
mod meta;
mod path;
mod process;
mod service;
mod transport;

pub use identity::IdentityError;
pub use service::EventLogWriter;
use tokio_util::sync::CancellationToken;

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
}

/// Starts only the operations implemented and declared by this native backend.
/// The listener accepts host-CID connections; no IP interface is exposed.
pub fn run(console: bool) -> Result<(), RunError> {
    if console {
        serve(CancellationToken::new(), || Ok(()))
    } else {
        service::dispatch().map_err(RunError::from)
    }
}

fn serve(
    shutdown: CancellationToken,
    ready: impl FnOnce() -> Result<(), RunError>,
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
    let readiness = ready();
    let result = runtime.block_on(async {
        if let Err(error) = readiness {
            incoming.close().await?;
            return Err(error);
        }
        let stop = shutdown.clone();
        let signal = tokio::spawn(async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                stop.cancel();
            }
        });
        let tasks = tokio_util::task::TaskTracker::new();
        let filesystem = identity.clone().map(|identity| filesystem::Filesystem::new(identity, tasks.clone())).map(
            pplx_space_spaced_guest_protocol::proto::bridge_guest::filesystem_server::FilesystemServer::new);
        let processes = identity.clone().map(|identity| process::Processes::new(identity, shutdown.clone(), tasks.clone())).map(
            pplx_space_spaced_guest_protocol::proto::bridge_guest::process_server::ProcessServer::new);
        let result = tonic::transport::Server::builder()
            .concurrency_limit_per_connection(32)
            .max_concurrent_streams(128)
            .http2_keepalive_interval(Some(std::time::Duration::from_secs(10)))
            .http2_keepalive_timeout(Some(std::time::Duration::from_secs(5)))
            .add_service(meta::service(identity.is_some()))
            .add_optional_service(filesystem)
            .add_optional_service(processes)
            .serve_with_incoming_shutdown(incoming.stream(), shutdown.cancelled())
            .await;
        shutdown.cancel();
        signal.abort();
        let _ = signal.await;
        incoming.close().await?;
        tasks.close();
        tasks.wait().await;
        result.map_err(RunError::from)
    });
    runtime.shutdown_timeout(std::time::Duration::from_secs(5));
    result
}
