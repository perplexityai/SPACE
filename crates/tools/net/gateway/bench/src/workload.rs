//! The workload registry.
//!
//! # Registering a workload
//!
//! Write an `async fn workload(&Env) -> Result<Measured, WorkloadError>` and add
//! one `workload!` line to [`REGISTRY`]. That is the whole contract: the runner
//! picks it up for both `bench` (run K times, measure) and `smoke` (run once,
//! any `Err` is a failure), and the report derives throughput and CPU-per-unit
//! from whatever the workload counts in its [`Measured`].
//!
//! A workload asserts its own correctness by returning `Err`. There is no
//! separate assertion table, so a workload cannot be measured without also
//! being checked.

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::client_hello;
use crate::wire::{self, Op};

/// Everything a workload needs to reach the gateway. Addresses are the
/// upstream's, not the gateway's: the guest dials the destination and TPROXY
/// captures it, exactly as a sandbox does.
#[derive(Clone, Debug)]
pub struct Env {
    /// Speaks the [`wire`] header protocol.
    pub upstream: SocketAddr,
    /// Echoes from the first byte, for workloads whose opening bytes are fixed.
    pub raw_echo: SocketAddr,
    /// A destination the platform guardrails hard-deny.
    pub denied: SocketAddr,
    /// Scales every size and count below, as a percentage. 100 is as written.
    pub scale_percent: u32,
}

impl Env {
    fn scaled(&self, value: u64) -> u64 {
        value
            .saturating_mul(u64::from(self.scale_percent))
            .div_euclid(100)
            .max(1)
    }
}

/// What one run of a workload moved. The report turns these into throughput and
/// cost-per-unit; a workload leaves at zero whatever it does not produce.
#[derive(Clone, Debug, Default)]
pub struct Measured {
    pub bytes_to_upstream: u64,
    pub bytes_from_upstream: u64,
    pub connections: u64,
    /// Round trips, requests, or whatever the workload's natural unit of work
    /// is. Drives the per-operation CPU and latency columns.
    pub operations: u64,
    pub operation_latencies_us: Vec<u64>,
}

#[derive(Debug, thiserror::Error)]
pub enum WorkloadError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("expected {expected} bytes back, got {actual}")]
    ShortRead { expected: u64, actual: u64 },
    #[error("the bytes returned did not match the bytes sent")]
    PayloadMismatch,
    #[error("upstream counted {reported} bytes, we sent {sent}")]
    SinkCountMismatch { sent: u64, reported: u64 },
    #[error("policy denies {destination} but the gateway carried {bytes} bytes to it")]
    UnexpectedlyAllowed { destination: SocketAddr, bytes: u64 },
    #[error("a spawned half of the workload did not finish: {0}")]
    Join(#[from] tokio::task::JoinError),
}

type RunFuture<'a> = Pin<Box<dyn Future<Output = Result<Measured, WorkloadError>> + Send + 'a>>;

pub struct Workload {
    pub name: &'static str,
    pub summary: &'static str,
    run: for<'a> fn(&'a Env) -> RunFuture<'a>,
}

impl Workload {
    pub fn run<'a>(&self, env: &'a Env) -> RunFuture<'a> {
        (self.run)(env)
    }
}

macro_rules! workload {
    ($name:literal, $summary:literal, $function:path) => {
        Workload {
            name: $name,
            summary: $summary,
            run: |env| Box::pin($function(env)),
        }
    };
}

pub const REGISTRY: &[Workload] = &[
    workload!(
        "idle",
        "no traffic; the gateway's resting CPU and RSS",
        idle
    ),
    workload!(
        "connect-churn",
        "many sequential short connections: accept, admit, register, relay, teardown",
        connect_churn
    ),
    workload!(
        "request-response",
        "one connection, many small round trips: the relay under latency-bound traffic",
        request_response
    ),
    workload!(
        "bulk-upload",
        "one connection streaming guest to upstream: outbound throughput",
        bulk_upload
    ),
    workload!(
        "bulk-download",
        "one connection streaming upstream to guest: inbound throughput",
        bulk_download
    ),
    workload!(
        "bulk-echo",
        "one connection saturating both directions at once",
        bulk_echo
    ),
    workload!(
        "concurrent-fanout",
        "hundreds of simultaneous connections: task fan-out and per-flow state",
        concurrent_fanout
    ),
    workload!(
        "connection-limit",
        "more simultaneous connections than the proxy admits, so the accept loop stops accepting",
        connection_limit
    ),
    workload!(
        "tls-sni",
        "connections opening with a real ClientHello: the SNI peek's hit path",
        tls_sni
    ),
    workload!(
        "tls-sni-absent",
        "connections opening with a ClientHello carrying no SNI",
        tls_sni_absent
    ),
    workload!(
        "silent-client",
        "connections that send nothing at first: the peek's first-bytes timeout",
        silent_client
    ),
    workload!(
        "denied-destination",
        "connections to a guardrailed destination, which must all be refused",
        denied_destination
    ),
    workload!(
        "many-tiny-writes",
        "one connection written a byte at a time: worst-case per-syscall relay",
        many_tiny_writes
    ),
    workload!(
        "half-close-download",
        "half-close upstream, then drain a full download",
        half_close_download
    ),
];

pub fn lookup(name: &str) -> Option<&'static Workload> {
    REGISTRY.iter().find(|workload| workload.name == name)
}

// --- workloads --------------------------------------------------------------

// Sized so one iteration of every workload takes long enough that scheduler
// noise does not dominate its CPU sample. A workload whose iteration lands in
// single-digit milliseconds produces deltas that change sign between runs.
const IDLE_MS: u64 = 3_000;
const CHURN_CONNECTIONS: u64 = 300;
const CHURN_PAYLOAD: usize = 32;
const ROUND_TRIPS: u64 = 3_000;
const ROUND_TRIP_PAYLOAD: usize = 64;
const BULK_BYTES: u64 = 512 * 1024 * 1024;
const ECHO_BYTES: u64 = 256 * 1024 * 1024;
const FANOUT_CONNECTIONS: u64 = 256;
const FANOUT_BYTES: u64 = 1024 * 1024;
/// Above the proxy's admission semaphore, so the accept loop has to stop
/// accepting. Payload kept small: this is about connection count, not bytes.
const LIMIT_CONNECTIONS: u64 = 1_900;
const LIMIT_BYTES: u64 = 4 * 1024;
/// How many connections `connection-limit` opens at a time.
const DIAL_WAVE: usize = 64;
/// Pause between waves, so the proxy's accept loop keeps up with the dialling.
///
/// A wave on its own does not bound the accept queue: the kernel completes the
/// handshake from the backlog, so `connect` returns whether or not the proxy has
/// accepted, and the client can run thousands of connections ahead of it. Without
/// the pause a full run overflowed by ~50 handshakes even at 1900 connections.
const DIAL_WAVE_PAUSE: Duration = Duration::from_millis(10);

/// The proxy's admission cap and its listener depth, as the gateway sets them.
/// This crate cannot read the gateway's own constants; update both sides
/// together when either limit changes.
const PROXY_ADMISSION_CAP: u64 = 1_744;
const PROXY_LISTEN_BACKLOG: u64 = 1_024;
/// How much of the backlog is reserved for what the arithmetic cannot see.
///
/// Load-bearing, not padding, for two reasons. The proxy holds a permit for a
/// flow's whole life, so an iteration can start with some still held by the
/// previous one; and the dial pacing bounds the queue only approximately, because
/// the client cannot observe accepts. Sized at 2400 connections — a nominal depth
/// of 664 against a 1024-deep backlog — a full run overflowed by ~300 handshakes.
/// `run.sh` reads the kernel's counter after every bench, which is what turns a
/// sizing this arithmetic calls fine into a failure.
const UNDRAINED_HEADROOM: u64 = 700;

const _: () = assert!(
    LIMIT_CONNECTIONS > PROXY_ADMISSION_CAP,
    "connection-limit must exceed the proxy's admission cap, or it stops \
     exercising the accept loop's limiter while still reporting numbers"
);
const _: () = assert!(
    LIMIT_CONNECTIONS - PROXY_ADMISSION_CAP + DIAL_WAVE as u64 + UNDRAINED_HEADROOM
        <= PROXY_LISTEN_BACKLOG,
    "the queue behind the proxy's cap, plus one dial wave, plus headroom for \
     flows it has yet to release, must fit in the listener's backlog; otherwise \
     the kernel drops handshakes and their retransmits are charged to the gateway"
);
const SNI_CONNECTIONS: u64 = 300;
const SILENT_CONNECTIONS: u64 = 32;
const SILENT_QUIET_MS: u64 = 120;
const DENIED_CONNECTIONS: u64 = 400;
/// How long to wait for a byte before calling a flow refused. Comfortably past
/// the gateway's own ClientHello first-bytes timeout.
const DENY_VERDICT_WAIT: Duration = Duration::from_millis(500);
const TINY_WRITES: u64 = 20_000;
const BENCH_SERVER_NAME: &str = "bench.netgtw.example";
const BLOCK: usize = 64 * 1024;

async fn idle(env: &Env) -> Result<Measured, WorkloadError> {
    tokio::time::sleep(Duration::from_millis(env.scaled(IDLE_MS))).await;
    Ok(Measured::default())
}

async fn connect_churn(env: &Env) -> Result<Measured, WorkloadError> {
    let connections = env.scaled(CHURN_CONNECTIONS);
    let payload = vec![b'c'; CHURN_PAYLOAD];
    let mut measured = Measured::default();
    for _ in 0..connections {
        let started = Instant::now();
        let mut stream = dial(env.upstream).await?;
        stream.write_all(&wire::encode(Op::Echo, 0)).await?;
        stream.write_all(&payload).await?;
        stream.shutdown().await?;
        let echoed = read_to_end(&mut stream).await?;
        if echoed != payload {
            return Err(WorkloadError::PayloadMismatch);
        }
        measured.operation_latencies_us.push(elapsed_us(started));
    }
    let sent = connections.saturating_mul((wire::HEADER_LEN + CHURN_PAYLOAD) as u64);
    measured.bytes_to_upstream = sent;
    measured.bytes_from_upstream = connections.saturating_mul(CHURN_PAYLOAD as u64);
    measured.connections = connections;
    measured.operations = connections;
    Ok(measured)
}

async fn request_response(env: &Env) -> Result<Measured, WorkloadError> {
    let round_trips = env.scaled(ROUND_TRIPS);
    let request = vec![b'r'; ROUND_TRIP_PAYLOAD];
    let mut response = vec![0u8; ROUND_TRIP_PAYLOAD];
    let mut stream = dial(env.upstream).await?;
    stream.write_all(&wire::encode(Op::Echo, 0)).await?;

    let mut measured = Measured::default();
    for _ in 0..round_trips {
        let started = Instant::now();
        stream.write_all(&request).await?;
        stream.read_exact(&mut response).await?;
        if response != request {
            return Err(WorkloadError::PayloadMismatch);
        }
        measured.operation_latencies_us.push(elapsed_us(started));
    }

    let payload_bytes = round_trips.saturating_mul(ROUND_TRIP_PAYLOAD as u64);
    measured.bytes_to_upstream = payload_bytes.saturating_add(wire::HEADER_LEN as u64);
    measured.bytes_from_upstream = payload_bytes;
    measured.connections = 1;
    measured.operations = round_trips;
    Ok(measured)
}

async fn bulk_upload(env: &Env) -> Result<Measured, WorkloadError> {
    let bytes = env.scaled(BULK_BYTES);
    let mut stream = dial(env.upstream).await?;
    stream.write_all(&wire::encode(Op::Sink, 0)).await?;
    write_bytes(&mut stream, bytes).await?;
    stream.shutdown().await?;

    // The upstream's count starts after the header it already consumed, so this
    // compares payload against payload.
    let mut reported = [0u8; 8];
    stream.read_exact(&mut reported).await?;
    let reported = u64::from_be_bytes(reported);
    if reported != bytes {
        return Err(WorkloadError::SinkCountMismatch {
            sent: bytes,
            reported,
        });
    }
    Ok(Measured {
        bytes_to_upstream: bytes.saturating_add(wire::HEADER_LEN as u64),
        bytes_from_upstream: 8,
        connections: 1,
        operations: 1,
        operation_latencies_us: Vec::new(),
    })
}

async fn bulk_download(env: &Env) -> Result<Measured, WorkloadError> {
    let bytes = env.scaled(BULK_BYTES);
    let mut stream = dial(env.upstream).await?;
    stream.write_all(&wire::encode(Op::Source, bytes)).await?;
    let received = drain(&mut stream).await?;
    if received != bytes {
        return Err(WorkloadError::ShortRead {
            expected: bytes,
            actual: received,
        });
    }
    Ok(Measured {
        bytes_to_upstream: wire::HEADER_LEN as u64,
        bytes_from_upstream: received,
        connections: 1,
        operations: 1,
        operation_latencies_us: Vec::new(),
    })
}

async fn bulk_echo(env: &Env) -> Result<Measured, WorkloadError> {
    let bytes = env.scaled(ECHO_BYTES);
    let mut stream = dial(env.upstream).await?;
    stream.write_all(&wire::encode(Op::Echo, 0)).await?;
    let (mut reader, mut writer) = stream.into_split();

    let sender = tokio::spawn(async move {
        write_bytes(&mut writer, bytes).await?;
        writer.shutdown().await
    });
    let received = drain(&mut reader).await?;
    sender.await??;

    if received != bytes {
        return Err(WorkloadError::ShortRead {
            expected: bytes,
            actual: received,
        });
    }
    Ok(Measured {
        bytes_to_upstream: bytes.saturating_add(wire::HEADER_LEN as u64),
        bytes_from_upstream: received,
        connections: 1,
        operations: 1,
        operation_latencies_us: Vec::new(),
    })
}

async fn concurrent_fanout(env: &Env) -> Result<Measured, WorkloadError> {
    fanout(
        env.upstream,
        env.scaled(FANOUT_CONNECTIONS),
        env.scaled(FANOUT_BYTES),
    )
    .await
}

/// More simultaneous connections than the proxy's admission semaphore allows, so
/// it stops accepting and the excess waits in the listener's backlog.
///
/// Every connection is opened *before* any of them sends a byte, which is what
/// makes exceeding the cap a fact rather than a hope: driving the work
/// concurrently would not, since a connection can finish before the next begins.
/// The gateway holds a permit for a flow's whole life, so the connections it has
/// admitted stay admitted while the rest queue.
///
/// Dialled in waves for a reason the kernel counts: one burst of thousands of
/// connects overruns the listener's backlog — 1024, as in production —
/// `TcpExtListenOverflows` climbs, and the handshakes the kernel then drops
/// surface as flows the gateway appears to have lost. Waves keep the queue depth
/// to the excess over the cap.
///
/// Only exceeds the cap at full `--scale`.
async fn connection_limit(env: &Env) -> Result<Measured, WorkloadError> {
    let requested = usize::try_from(env.scaled(LIMIT_CONNECTIONS)).unwrap_or(usize::MAX);
    let bytes = env.scaled(LIMIT_BYTES);

    let upstream = env.upstream;
    let mut open = Vec::with_capacity(requested);
    while open.len() < requested {
        let wave = DIAL_WAVE.min(requested - open.len());
        let dials: Vec<_> = (0..wave)
            .map(|_| tokio::spawn(async move { dial(upstream).await }))
            .collect();
        for dial in dials {
            open.push(dial.await??);
        }
        tokio::time::sleep(DIAL_WAVE_PAUSE).await;
    }

    let mut tasks = Vec::with_capacity(open.len());
    for stream in open {
        tasks.push(tokio::spawn(async move {
            let started = Instant::now();
            let mut stream = stream;
            stream.write_all(&wire::encode(Op::Echo, 0)).await?;
            let (mut reader, mut writer) = stream.into_split();
            let sender = tokio::spawn(async move {
                write_bytes(&mut writer, bytes).await?;
                writer.shutdown().await
            });
            let received = drain(&mut reader).await?;
            sender.await??;
            if received != bytes {
                return Err(WorkloadError::ShortRead {
                    expected: bytes,
                    actual: received,
                });
            }
            Ok::<u64, WorkloadError>(elapsed_us(started))
        }));
    }

    let connections = tasks.len() as u64;
    let mut measured = Measured::default();
    for task in tasks {
        measured.operation_latencies_us.push(task.await??);
    }
    measured.bytes_to_upstream = connections.saturating_mul(bytes + wire::HEADER_LEN as u64);
    measured.bytes_from_upstream = connections.saturating_mul(bytes);
    measured.connections = connections;
    measured.operations = connections;
    Ok(measured)
}

async fn fanout(
    upstream: SocketAddr,
    connections: u64,
    bytes: u64,
) -> Result<Measured, WorkloadError> {
    let mut tasks = Vec::new();
    for _ in 0..connections {
        tasks.push(tokio::spawn(async move {
            let started = Instant::now();
            let mut stream = dial(upstream).await?;
            stream.write_all(&wire::encode(Op::Echo, 0)).await?;
            let (mut reader, mut writer) = stream.into_split();
            let sender = tokio::spawn(async move {
                write_bytes(&mut writer, bytes).await?;
                writer.shutdown().await
            });
            let received = drain(&mut reader).await?;
            sender.await??;
            if received != bytes {
                return Err(WorkloadError::ShortRead {
                    expected: bytes,
                    actual: received,
                });
            }
            Ok::<u64, WorkloadError>(elapsed_us(started))
        }));
    }

    let mut measured = Measured::default();
    for task in tasks {
        measured.operation_latencies_us.push(task.await??);
    }
    measured.bytes_to_upstream = connections.saturating_mul(bytes + wire::HEADER_LEN as u64);
    measured.bytes_from_upstream = connections.saturating_mul(bytes);
    measured.connections = connections;
    measured.operations = connections;
    Ok(measured)
}

async fn tls_sni(env: &Env) -> Result<Measured, WorkloadError> {
    sni_connections(env, &client_hello::with_server_name(BENCH_SERVER_NAME)).await
}

async fn tls_sni_absent(env: &Env) -> Result<Measured, WorkloadError> {
    sni_connections(env, &client_hello::without_server_name()).await
}

/// Open each connection with `hello` as its first bytes, then echo a payload.
/// The peek does not consume, so every byte of the ClientHello must come back.
async fn sni_connections(env: &Env, hello: &[u8]) -> Result<Measured, WorkloadError> {
    let connections = env.scaled(SNI_CONNECTIONS);
    let payload = vec![b's'; CHURN_PAYLOAD];
    let mut expected = hello.to_vec();
    expected.extend_from_slice(&payload);

    let mut measured = Measured::default();
    for _ in 0..connections {
        let started = Instant::now();
        let mut stream = dial(env.raw_echo).await?;
        stream.write_all(hello).await?;
        stream.write_all(&payload).await?;
        stream.shutdown().await?;
        if read_to_end(&mut stream).await? != expected {
            return Err(WorkloadError::PayloadMismatch);
        }
        measured.operation_latencies_us.push(elapsed_us(started));
    }
    let bytes = connections.saturating_mul(expected.len() as u64);
    measured.bytes_to_upstream = bytes;
    measured.bytes_from_upstream = bytes;
    measured.connections = connections;
    measured.operations = connections;
    Ok(measured)
}

/// Connections that stay quiet past the peek's first-bytes timeout. Run
/// together so the workload costs one timeout, not one per connection, and so
/// the gateway holds many pending peeks at once.
async fn silent_client(env: &Env) -> Result<Measured, WorkloadError> {
    let connections = env.scaled(SILENT_CONNECTIONS);
    let quiet = Duration::from_millis(env.scaled(SILENT_QUIET_MS));
    let mut tasks = Vec::new();
    for _ in 0..connections {
        let upstream = env.upstream;
        tasks.push(tokio::spawn(async move {
            let started = Instant::now();
            let mut stream = dial(upstream).await?;
            tokio::time::sleep(quiet).await;
            let payload = vec![b'q'; CHURN_PAYLOAD];
            stream.write_all(&wire::encode(Op::Echo, 0)).await?;
            stream.write_all(&payload).await?;
            stream.shutdown().await?;
            if read_to_end(&mut stream).await? != payload {
                return Err(WorkloadError::PayloadMismatch);
            }
            Ok::<u64, WorkloadError>(elapsed_us(started))
        }));
    }

    let mut measured = Measured::default();
    for task in tasks {
        measured.operation_latencies_us.push(task.await??);
    }
    measured.bytes_to_upstream =
        connections.saturating_mul((wire::HEADER_LEN + CHURN_PAYLOAD) as u64);
    measured.bytes_from_upstream = connections.saturating_mul(CHURN_PAYLOAD as u64);
    measured.connections = connections;
    measured.operations = connections;
    Ok(measured)
}

/// The deny path. TPROXY means the handshake completes with the gateway before
/// policy runs, so a refusal looks like a connection that closes without ever
/// carrying a byte.
async fn denied_destination(env: &Env) -> Result<Measured, WorkloadError> {
    let connections = env.scaled(DENIED_CONNECTIONS);
    let mut measured = Measured::default();
    for _ in 0..connections {
        let started = Instant::now();
        let mut stream = dial(env.denied).await?;
        // Both writes and the read may fail once the gateway drops the flow;
        // only bytes actually returned prove it was carried. Half-closing first
        // means an echo peer on the far side would close too, so an allowed flow
        // answers rather than sitting idle.
        let _ = stream.write_all(&wire::encode(Op::Echo, 0)).await;
        let _ = stream.write_all(b"denied").await;
        let _ = stream.shutdown().await;
        let carried = bytes_carried(&mut stream).await;
        if carried > 0 {
            return Err(WorkloadError::UnexpectedlyAllowed {
                destination: env.denied,
                bytes: carried,
            });
        }
        measured.operation_latencies_us.push(elapsed_us(started));
    }
    measured.connections = connections;
    measured.operations = connections;
    Ok(measured)
}

/// How many bytes came back, bounded in time. A denied flow closes at once, so
/// the deadline only binds when the destination turns out to be reachable and
/// silent — which the caller must report as a failure, not wait out.
async fn bytes_carried(stream: &mut TcpStream) -> u64 {
    let mut buffer = [0u8; 512];
    match tokio::time::timeout(DENY_VERDICT_WAIT, stream.read(&mut buffer)).await {
        // A read of zero is EOF: the gateway closed the flow.
        Ok(Ok(read)) => read as u64,
        // A reset, or no answer at all within the deadline. Neither carried a byte.
        Ok(Err(_)) | Err(_) => 0,
    }
}

async fn many_tiny_writes(env: &Env) -> Result<Measured, WorkloadError> {
    let writes = env.scaled(TINY_WRITES);
    let mut stream = dial(env.upstream).await?;
    stream.write_all(&wire::encode(Op::Echo, 0)).await?;
    let (mut reader, mut writer) = stream.into_split();

    let sender = tokio::spawn(async move {
        for _ in 0..writes {
            writer.write_all(b"x").await?;
        }
        writer.shutdown().await
    });
    let received = drain(&mut reader).await?;
    sender.await??;

    if received != writes {
        return Err(WorkloadError::ShortRead {
            expected: writes,
            actual: received,
        });
    }
    Ok(Measured {
        bytes_to_upstream: writes.saturating_add(wire::HEADER_LEN as u64),
        bytes_from_upstream: received,
        connections: 1,
        operations: writes,
        operation_latencies_us: Vec::new(),
    })
}

/// Half-close the guest's write side before the download starts, so the relay
/// carries one direction to completion after the other has already ended.
async fn half_close_download(env: &Env) -> Result<Measured, WorkloadError> {
    let bytes = env.scaled(BULK_BYTES);
    let mut stream = dial(env.upstream).await?;
    stream.write_all(&wire::encode(Op::Source, bytes)).await?;
    stream.shutdown().await?;
    let received = drain(&mut stream).await?;
    if received != bytes {
        return Err(WorkloadError::ShortRead {
            expected: bytes,
            actual: received,
        });
    }
    Ok(Measured {
        bytes_to_upstream: wire::HEADER_LEN as u64,
        bytes_from_upstream: received,
        connections: 1,
        operations: 1,
        operation_latencies_us: Vec::new(),
    })
}

// --- helpers ----------------------------------------------------------------

async fn dial(destination: SocketAddr) -> Result<TcpStream, WorkloadError> {
    let stream = TcpStream::connect(destination).await?;
    // Without this, delayed ACK and Nagle dominate every small-frame workload
    // and the gateway's own cost disappears into the noise.
    stream.set_nodelay(true)?;
    Ok(stream)
}

async fn write_bytes<W>(writer: &mut W, bytes: u64) -> std::io::Result<()>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    let block = vec![b'b'; BLOCK];
    let mut remaining = bytes;
    while remaining > 0 {
        let take = usize::try_from(remaining).unwrap_or(BLOCK).min(BLOCK);
        let chunk = block.get(..take).unwrap_or(&block);
        writer.write_all(chunk).await?;
        remaining -= chunk.len() as u64;
    }
    Ok(())
}

async fn drain<R>(reader: &mut R) -> std::io::Result<u64>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut buffer = vec![0u8; BLOCK];
    let mut total = 0u64;
    loop {
        let read = reader.read(&mut buffer).await?;
        if read == 0 {
            return Ok(total);
        }
        total = total.saturating_add(read as u64);
    }
}

async fn read_to_end(stream: &mut TcpStream) -> std::io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    stream.read_to_end(&mut bytes).await?;
    Ok(bytes)
}

fn elapsed_us(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests;
