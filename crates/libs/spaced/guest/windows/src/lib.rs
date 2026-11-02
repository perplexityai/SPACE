//! Native Windows guest implementation of the shared spaced bridge protocol.
#![cfg(windows)]

mod meta;
mod service;
mod transport;

use tokio_util::sync::CancellationToken;

#[derive(Debug, thiserror::Error)]
pub enum RunError {
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
        let result = tonic::transport::Server::builder()
            .concurrency_limit_per_connection(32)
            .add_service(meta::service())
            .serve_with_incoming_shutdown(incoming.stream(), shutdown.cancelled())
            .await;
        shutdown.cancel();
        signal.abort();
        let _ = signal.await;
        incoming.close().await?;
        result.map_err(RunError::from)
    });
    runtime.shutdown_timeout(std::time::Duration::from_secs(5));
    result
}
