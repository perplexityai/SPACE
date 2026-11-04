//! Standalone in-VM smoke for the spaced-guest loopback bridge.
//!
//! 1. Spins up an in-process echo listener on `127.0.0.1:<port>`.
//! 2. Calls [`LoopbackBridgeManager::open`] for that port.
//! 3. Asserts the manager reports `LoopbackForwarded`.
//! 4. Dials `eth0_ip:<port>` and confirms bytes round-trip through the
//!    forwarder back to the in-process echo.
//!
//! Bypasses the bridge-guest RPC + relay layers; exercises only the
//! spaced-guest layer end-to-end. Cross-compile with
//! `cargo build -p pplx-space-spaced-loopback-test --release \
//!     --target x86_64-unknown-linux-musl`, ship the binary into a sandbox,
//! and run it inside the VM. Exits 0 on success, with a 30-second deadline.
//!
//! Execution owner: the operator validating a guest networking/template change
//! runs this manually inside the affected VM. Cargo/CI compilation does not
//! execute this probe; native unit tests do not validate eth0 autodetection.

use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use pplx_space_spaced_common::proto::bridge_guest::LoopbackBridgeStatus;
use pplx_space_spaced_guest_server::loopback_diagnostic::LoopbackBridgeManager;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

const PROBE_PORT: u16 = 47521;
const ECHO_PAYLOAD: &[u8] = b"hello-loopback-bridge";

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with_target(false)
        .init();
    tokio::time::timeout(Duration::from_secs(30), run_probe())
        .await
        .context("loopback probe exceeded 30 seconds")?
}

async fn run_probe() -> Result<()> {
    tracing::info!("starting in-process echo listener on 127.0.0.1:{PROBE_PORT}");
    let listener = TcpListener::bind(("127.0.0.1", PROBE_PORT))
        .await
        .with_context(|| format!("bind 127.0.0.1:{PROBE_PORT}"))?;
    let echo = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let (mut read, mut write) = stream.split();
        tokio::io::copy(&mut read, &mut write).await
    }));

    let shutdown = CancellationToken::new();
    let _cancel_on_exit = shutdown.clone().drop_guard();
    let mgr = LoopbackBridgeManager::new(shutdown);
    tracing::info!("calling LoopbackBridgeManager::open(port={PROBE_PORT}, wait=3s)");
    let (status, detail) = mgr.open(PROBE_PORT, Some(Duration::from_secs(3))).await;
    tracing::info!("open returned status={status:?} detail={detail:?}");
    if status != LoopbackBridgeStatus::LoopbackForwarded {
        return Err(anyhow!(
            "expected LoopbackForwarded, got {status:?} (detail={detail:?})"
        ));
    }

    let bind_ip = detect_external_ip()
        .ok_or_else(|| anyhow!("could not autodetect external IPv4 for client probe"))?;
    let target = format!("{bind_ip}:{PROBE_PORT}");
    tracing::info!("probing forwarder at {target}");
    let mut stream = TcpStream::connect(&target)
        .await
        .with_context(|| format!("connect {target}"))?;
    stream.write_all(ECHO_PAYLOAD).await?;
    stream.shutdown().await?;
    let mut received = Vec::with_capacity(ECHO_PAYLOAD.len());
    stream.read_to_end(&mut received).await?;
    echo.await
        .context("echo task failed")?
        .context("echo I/O failed")?;
    if received != ECHO_PAYLOAD {
        return Err(anyhow!(
            "echo mismatch: expected {:?}, got {:?}",
            std::str::from_utf8(ECHO_PAYLOAD).unwrap_or("<binary>"),
            String::from_utf8_lossy(&received)
        ));
    }

    tracing::info!(
        "loopback bridge round-trip succeeded: 127.0.0.1:{PROBE_PORT} <-> {bind_ip}:{PROBE_PORT}"
    );
    println!(
        "OK status={status:?} bind_ip={bind_ip} payload_bytes={}",
        received.len()
    );
    Ok(())
}

/// Mirrors the autodetect logic in `LoopbackBridgeManager` so the test
/// independently knows which IP to dial. Restricted to `eth0` for the same
/// reason the manager is — secondary interfaces aren't on the host-side
/// tap path so a forwarder bound there is unreachable from outside.
fn detect_external_ip() -> Option<std::net::Ipv4Addr> {
    use nix::ifaddrs::getifaddrs;
    use nix::net::if_::InterfaceFlags;

    for ifaddr in getifaddrs().ok()? {
        if ifaddr.interface_name != "eth0" {
            continue;
        }
        if !ifaddr.flags.contains(InterfaceFlags::IFF_UP)
            || !ifaddr.flags.contains(InterfaceFlags::IFF_RUNNING)
            || ifaddr.flags.contains(InterfaceFlags::IFF_LOOPBACK)
        {
            continue;
        }
        let Some(addr) = ifaddr.address.and_then(|a| a.as_sockaddr_in().copied()) else {
            continue;
        };
        let ip = addr.ip();
        if ip.is_loopback() || ip.is_unspecified() || ip.is_multicast() {
            continue;
        }
        return Some(ip);
    }
    None
}
