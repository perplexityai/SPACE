//! The "internet" side of the harness.
//!
//! Two listeners. The protocol port reads a [`wire`] header and then does what
//! it says; the raw-echo port echoes from the first byte, which is what the SNI
//! workloads need because their first bytes must be a ClientHello.

use std::io;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::wire::{self, Op};

const BLOCK: usize = 64 * 1024;

/// Deep enough that a burst of simultaneous connections queues here instead of
/// overflowing. The high-concurrency workloads overrun the default depth; the
/// kernel then drops handshakes, and the flows the client loses look like the
/// gateway dropping them. The upstream must never be the constraint under test.
const BACKLOG: i32 = 4096;

/// What `TcpListener::bind` would have given us. Named so the invariant below
/// says what it is guarding.
const TOKIO_DEFAULT_BACKLOG: i32 = 1024;

const _: () = assert!(
    BACKLOG > TOKIO_DEFAULT_BACKLOG,
    "the bench upstream must queue deeper than tokio's default, or a burst of \
     connections overflows its accept queue and the resulting losses read as \
     the gateway dropping flows"
);

pub async fn serve(
    protocol_port: u16,
    raw_echo_port: u16,
    shutdown: CancellationToken,
) -> io::Result<()> {
    let protocol = listener(protocol_port)?;
    let raw_echo = listener(raw_echo_port)?;
    info!(
        protocol_port,
        raw_echo_port, BACKLOG, "bench upstream listening"
    );

    loop {
        tokio::select! {
            () = shutdown.cancelled() => return Ok(()),
            accepted = protocol.accept() => {
                let (stream, _peer) = accepted?;
                tokio::spawn(async move {
                    if let Err(error) = protocol_connection(stream).await {
                        debug!(%error, "upstream protocol connection ended");
                    }
                });
            }
            accepted = raw_echo.accept() => {
                let (stream, _peer) = accepted?;
                tokio::spawn(async move {
                    if let Err(error) = echo(stream).await {
                        debug!(%error, "upstream raw-echo connection ended");
                    }
                });
            }
        }
    }
}

fn listener(port: u16) -> io::Result<TcpListener> {
    let socket = socket2::Socket::new(
        socket2::Domain::IPV4,
        socket2::Type::STREAM,
        Some(socket2::Protocol::TCP),
    )?;
    socket.set_reuse_address(true)?;
    socket.set_nonblocking(true)?;
    socket.bind(&std::net::SocketAddr::from(([0, 0, 0, 0], port)).into())?;
    socket.listen(BACKLOG)?;
    TcpListener::from_std(socket.into())
}

async fn protocol_connection(mut stream: TcpStream) -> io::Result<()> {
    stream.set_nodelay(true)?;
    let mut header = [0u8; wire::HEADER_LEN];
    stream.read_exact(&mut header).await?;
    let (op, arg) = wire::decode(&header)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad bench header"))?;
    match op {
        Op::Echo => echo(stream).await,
        Op::Sink => sink(stream).await,
        Op::Source => source(stream, arg).await,
    }
}

/// Echo until the peer half-closes. `copy` on the split halves keeps this to
/// one read and one write per buffer, so the upstream is never the bottleneck.
async fn echo(mut stream: TcpStream) -> io::Result<()> {
    stream.set_nodelay(true)?;
    let (mut reader, mut writer) = stream.split();
    tokio::io::copy(&mut reader, &mut writer).await?;
    writer.shutdown().await
}

async fn sink(mut stream: TcpStream) -> io::Result<()> {
    let mut buffer = vec![0u8; BLOCK];
    let mut total = 0u64;
    loop {
        let read = stream.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        total = total.saturating_add(read as u64);
    }
    stream.write_all(&total.to_be_bytes()).await?;
    stream.shutdown().await
}

async fn source(mut stream: TcpStream, bytes: u64) -> io::Result<()> {
    let block = vec![b'u'; BLOCK];
    let mut remaining = bytes;
    while remaining > 0 {
        let take = usize::try_from(remaining).unwrap_or(BLOCK).min(BLOCK);
        let chunk = block
            .get(..take)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "short source block"))?;
        stream.write_all(chunk).await?;
        remaining -= take as u64;
    }
    stream.shutdown().await
}

#[cfg(test)]
mod tests;
