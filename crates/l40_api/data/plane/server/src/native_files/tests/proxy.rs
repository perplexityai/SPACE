//! A proxy between the data plane and the fake runtime that makes the
//! connections it holds faulty on request, as connections kept to a sandbox
//! endpoint become when the endpoint is a pod address a later pod was given.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, watch};

/// What a faulty connection does.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Fault {
    /// It is reset when the client starts its next call on it, as the later
    /// pod resets a connection it never saw. The client's control frames
    /// still pass, since a connection that carries no call can outlive its
    /// far end unnoticed.
    Reset,
    /// It stays open but carries nothing more either way, as where the
    /// network drops the packets of a connection the later pod never saw.
    Quiet,
}

/// Forwards each connection to `target`, and makes connections faulty with
/// [`Fault`] when asked; connections opened after that are forwarded, as to
/// the pod given the address since.
pub(super) struct FaultyProxy {
    pub(super) addr: SocketAddr,
    fault: Fault,
    /// Connections numbered below this are faulty.
    faulty_below: Arc<AtomicU64>,
    pub(super) opened: Arc<AtomicU64>,
    /// How many connections the client has closed.
    pub(super) closed: Arc<watch::Sender<u64>>,
    /// Each connection's relay, asked to have the client answer a ping.
    pings: Arc<Mutex<Vec<mpsc::UnboundedSender<oneshot::Sender<()>>>>>,
}

impl FaultyProxy {
    pub(super) async fn start(target: SocketAddr, fault: Fault) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy = Self {
            addr: listener.local_addr().unwrap(),
            fault,
            faulty_below: Arc::default(),
            opened: Arc::default(),
            closed: Arc::new(watch::Sender::new(0)),
            pings: Arc::default(),
        };
        let (faulty_below, opened, closed, pings) = (
            Arc::clone(&proxy.faulty_below),
            Arc::clone(&proxy.opened),
            Arc::clone(&proxy.closed),
            Arc::clone(&proxy.pings),
        );
        tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                let number = opened.fetch_add(1, Ordering::SeqCst);
                let upstream = TcpStream::connect(target).await.unwrap();
                let (ping, pinged) = mpsc::unbounded_channel();
                pings.lock().unwrap().push(ping);
                tokio::spawn(relay(
                    Connection {
                        client,
                        upstream,
                        number,
                        fault,
                    },
                    Arc::clone(&faulty_below),
                    Arc::clone(&closed),
                    pinged,
                ));
            }
        });
        proxy
    }

    /// Every connection open now is faulty. A connection that resets then
    /// carries a ping and its answer, a control frame from the client as a
    /// late window update is, before this returns: whatever the client sent
    /// for its earlier calls is past the proxy by then, and only its next
    /// call resets the connection.
    pub(super) async fn fault_open_connections(&self) {
        self.faulty_below
            .store(self.opened.load(Ordering::SeqCst), Ordering::SeqCst);
        if self.fault == Fault::Quiet {
            return;
        }
        let pings = self.pings.lock().unwrap().clone();
        for ping in pings {
            let (answered, answer) = oneshot::channel();
            if ping.send(answered).is_ok() {
                tokio::time::timeout(Duration::from_secs(5), answer)
                    .await
                    .expect("the client answers a ping in time")
                    .expect("a faulty connection carries the client's ping answer");
            }
        }
    }

    /// Every connection, open now or later, is faulty.
    pub(super) fn fault_every_connection(&self) {
        self.faulty_below.store(u64::MAX, Ordering::SeqCst);
    }
}

/// The client connection preface, before its first frame.
const CLIENT_PREFACE_LEN: usize = 24;
const FRAME_HEADER_LEN: usize = 9;
const HEADERS_FRAME: u8 = 0x1;
const PING_FRAME: u8 = 0x6;
const PING_ACK: u8 = 0x1;
/// The payload of the proxy's own pings.
const PROXY_PING: [u8; 8] = *b"proxy-pi";

/// One HTTP/2 frame, or the client preface, as it was read.
struct Frame {
    /// `None` for the preface.
    kind: Option<u8>,
    flags: u8,
    bytes: Vec<u8>,
}

impl Frame {
    fn is_proxy_ping_answer(&self) -> bool {
        self.kind == Some(PING_FRAME)
            && self.flags & PING_ACK != 0
            && self.bytes[FRAME_HEADER_LEN..] == PROXY_PING
    }
}

/// One direction of a connection, cut into whole frames.
struct Frames {
    buffer: Vec<u8>,
    /// Bytes of preface still to come before the first frame.
    preface: usize,
}

impl Frames {
    fn new(preface: usize) -> Self {
        Self {
            buffer: Vec::new(),
            preface,
        }
    }

    fn next(&mut self) -> Option<Frame> {
        if self.preface > 0 {
            if self.buffer.len() < self.preface {
                return None;
            }
            let bytes = self.buffer.drain(..self.preface).collect();
            self.preface = 0;
            return Some(Frame {
                kind: None,
                flags: 0,
                bytes,
            });
        }
        let header = self.buffer.get(..FRAME_HEADER_LEN)?;
        let length = u32::from_be_bytes([0, header[0], header[1], header[2]]) as usize;
        if self.buffer.len() < FRAME_HEADER_LEN + length {
            return None;
        }
        let bytes: Vec<u8> = self.buffer.drain(..FRAME_HEADER_LEN + length).collect();
        Some(Frame {
            kind: Some(bytes[3]),
            flags: bytes[4],
            bytes,
        })
    }
}

struct Connection {
    client: TcpStream,
    upstream: TcpStream,
    number: u64,
    fault: Fault,
}

async fn relay(
    connection: Connection,
    faulty_below: Arc<AtomicU64>,
    closed: Arc<watch::Sender<u64>>,
    mut pings: mpsc::UnboundedReceiver<oneshot::Sender<()>>,
) {
    let Connection {
        mut client,
        mut upstream,
        number,
        fault,
    } = connection;
    if fault == Fault::Reset {
        // Dropping the client's socket resets it rather than closing it. A
        // zero linger discards unsent data on drop, so it never blocks.
        #[allow(deprecated)]
        client.set_linger(Some(Duration::ZERO)).unwrap();
    }
    let faulty = || number < faulty_below.load(Ordering::SeqCst);
    let (mut from_client, mut from_upstream) = (Frames::new(CLIENT_PREFACE_LEN), Frames::new(0));
    let (mut client_read, mut upstream_read) = (vec![0; 64 * 1024], vec![0; 64 * 1024]);
    let mut awaiting_answer: Option<oneshot::Sender<()>> = None;
    loop {
        tokio::select! {
            read = client.read(&mut client_read) => {
                let Ok(read @ 1..) = read else {
                    closed.send_modify(|count| *count += 1);
                    return;
                };
                from_client.buffer.extend_from_slice(&client_read[..read]);
                while let Some(frame) = from_client.next() {
                    if frame.is_proxy_ping_answer() {
                        if let Some(answered) = awaiting_answer.take() {
                            let _ = answered.send(());
                        }
                        continue;
                    }
                    if faulty() {
                        match fault {
                            Fault::Quiet => continue,
                            // The call never reaches the runtime.
                            Fault::Reset if frame.kind == Some(HEADERS_FRAME) => return,
                            Fault::Reset => {}
                        }
                    }
                    if upstream.write_all(&frame.bytes).await.is_err() {
                        return;
                    }
                }
            }
            read = upstream.read(&mut upstream_read) => {
                let Ok(read @ 1..) = read else { return };
                from_upstream.buffer.extend_from_slice(&upstream_read[..read]);
                while let Some(frame) = from_upstream.next() {
                    if fault == Fault::Quiet && faulty() {
                        continue;
                    }
                    if client.write_all(&frame.bytes).await.is_err() {
                        return;
                    }
                }
            }
            Some(answered) = pings.recv() => {
                // Between the runtime's whole frames, so the client reads it
                // as a frame of its own.
                let mut ping = vec![0, 0, 8, PING_FRAME, 0, 0, 0, 0, 0];
                ping.extend_from_slice(&PROXY_PING);
                if client.write_all(&ping).await.is_err() {
                    return;
                }
                awaiting_answer = Some(answered);
            }
        }
    }
}
