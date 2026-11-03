//! viosock's Winsock provider does not implement overlapped I/O or AFD polling.
//! Keep synchronous provider calls on bounded workers, with in-memory backpressure.

use socket2::{Domain, SockAddr, SockAddrStorage, Socket, Type};
use std::{
    future::Future,
    io::{self, Read, Write},
    mem::size_of,
    net::Shutdown,
    os::windows::io::AsRawSocket,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tokio::io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf};
use tokio::sync::{Semaphore, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::{io::SyncIoBridge, sync::CancellationToken, task::TaskTracker};
use windows_sys::Win32::Networking::WinSock::{FD_SET, TIMEVAL, WSAGetLastError, select};

const AF_VSOCK: u16 = 40;
const HOST_CID: u32 = 2;
const MAX_CONNECTIONS: usize = 32;
const BUFFER_BYTES: usize = 64 * 1024;
const IO_TICK: Duration = Duration::from_millis(100);

// Windows SOCKADDR_VM is 12 bytes; it has no Linux sockaddr_vm trailing padding.
// Family, port and CID are native-endian.
#[repr(C)]
#[derive(Clone, Copy)]
struct VmAddress {
    family: u16,
    reserved: u16,
    port: u32,
    cid: u32,
}

fn address(port: u32) -> SockAddr {
    let mut storage = SockAddrStorage::zeroed();
    // SAFETY: storage is aligned and large enough for the Windows SOCKADDR_VM ABI.
    let vm = unsafe { storage.view_as::<VmAddress>() };
    *vm = VmAddress {
        family: AF_VSOCK,
        reserved: 0,
        port,
        cid: u32::MAX,
    };
    // SAFETY: all fields and padding are initialized; the family and length agree.
    unsafe { SockAddr::new(storage, size_of::<VmAddress>() as _) }
}

fn is_host(address: &SockAddr) -> bool {
    if address.family() != AF_VSOCK
        || usize::try_from(address.len()).ok() != Some(size_of::<VmAddress>())
    {
        return false;
    }
    // SAFETY: the provider returned a full SOCKADDR_VM, checked above. Read
    // unaligned because SockAddr's public pointer does not promise this alignment.
    let address = unsafe { address.as_ptr().cast::<VmAddress>().read_unaligned() };
    address.cid == HOST_CID
}

#[derive(Clone, Copy)]
enum Interest {
    Read,
    Write,
}
fn ready(socket: &Socket, interest: Interest) -> io::Result<bool> {
    let mut set = FD_SET {
        fd_count: 1,
        fd_array: [0; 64],
    };
    set.fd_array[0] = socket.as_raw_socket() as _;
    let timeout = TIMEVAL {
        tv_sec: 0,
        tv_usec: IO_TICK.as_micros() as _,
    };
    // SAFETY: one live owned socket, initialized fd_set and timeout; Winsock
    // does not retain the pointers. Unlike mio, select calls the viosock provider.
    let result = unsafe {
        select(
            0,
            if matches!(interest, Interest::Read) {
                &mut set
            } else {
                std::ptr::null_mut()
            },
            if matches!(interest, Interest::Write) {
                &mut set
            } else {
                std::ptr::null_mut()
            },
            std::ptr::null_mut(),
            &timeout,
        )
    };
    if result < 0 {
        // SAFETY: WSAGetLastError reads this thread's last Winsock error.
        Err(io::Error::from_raw_os_error(unsafe { WSAGetLastError() }))
    } else {
        Ok(result > 0)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ListenerFailure {
    Retry,
    Backoff,
    Fatal,
}
fn listener_failure(error: &io::Error) -> ListenerFailure {
    use windows_sys::Win32::Networking::WinSock::{
        WSAECONNABORTED, WSAECONNRESET, WSAEINTR, WSAENOBUFS, WSAEWOULDBLOCK,
    };
    // Winsock codes are not consistently mapped by the Win32 ErrorKind decoder.
    match error.raw_os_error() {
        Some(WSAEINTR | WSAEWOULDBLOCK | WSAECONNABORTED | WSAECONNRESET) => {
            return ListenerFailure::Retry;
        }
        Some(WSAENOBUFS) => return ListenerFailure::Backoff,
        _ => {}
    }
    if matches!(
        error.kind(),
        io::ErrorKind::Interrupted
            | io::ErrorKind::WouldBlock
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
    ) {
        ListenerFailure::Retry
    } else {
        ListenerFailure::Fatal
    }
}

fn listener_retry(
    error: &io::Error,
    stop: &CancellationToken,
    runtime: &tokio::runtime::Handle,
    last_warning: &mut Option<std::time::Instant>,
) -> bool {
    match listener_failure(error) {
        ListenerFailure::Fatal => false,
        ListenerFailure::Retry => true,
        ListenerFailure::Backoff => {
            if last_warning.is_none_or(|time| time.elapsed() >= Duration::from_secs(60)) {
                tracing::warn!(%error, "viosock listener waiting for resources");
                *last_warning = Some(std::time::Instant::now());
            }
            runtime.block_on(async {
                tokio::select! {
                    _ = stop.cancelled() => {},
                    _ = tokio::time::sleep(IO_TICK) => {},
                }
            });
            true
        }
    }
}

pub(super) struct Listener {
    incoming: Option<mpsc::Receiver<io::Result<Stream>>>,
    shutdown: CancellationToken,
    workers: TaskTracker,
    listener: tokio::task::JoinHandle<io::Result<()>>,
}

impl Listener {
    pub(super) fn bind(port: u32, shutdown: CancellationToken) -> io::Result<Self> {
        let socket = Socket::new(Domain::from(AF_VSOCK as i32), Type::STREAM, None)?;
        socket.bind(&address(port))?;
        socket.listen(MAX_CONNECTIONS as i32)?;
        socket.set_nonblocking(true)?;
        let (sender, incoming) = mpsc::channel(MAX_CONNECTIONS);
        let permits = Arc::new(Semaphore::new(MAX_CONNECTIONS));
        let workers = TaskTracker::new();
        let tasks = workers.clone();
        let stop = shutdown.clone();
        let runtime = tokio::runtime::Handle::current();
        let listener = workers.spawn_blocking(move || {
            // A failed listener must stop existing RPC connections too; otherwise
            // SCM could keep reporting Running with no way to accept a new host.
            let _cancel_on_exit = stop.clone().drop_guard();
            let mut last_capacity_warning = None;
            let mut last_resource_warning = None;
            while !stop.is_cancelled() {
                match ready(&socket, Interest::Read) {
                    Ok(false) => continue,
                    Err(error)
                        if listener_retry(&error, &stop, &runtime, &mut last_resource_warning) =>
                    {
                        continue;
                    }
                    Err(error) => {
                        tracing::error!(%error, "viosock listener failed");
                        return Err(error);
                    }
                    Ok(true) => {}
                }
                let (peer, address) = match socket.accept() {
                    Ok(connection) => connection,
                    Err(error)
                        if matches!(
                            error.kind(),
                            io::ErrorKind::WouldBlock
                                | io::ErrorKind::Interrupted
                                | io::ErrorKind::ConnectionAborted
                                | io::ErrorKind::ConnectionReset
                        ) =>
                    {
                        continue;
                    }
                    Err(error)
                        if listener_retry(&error, &stop, &runtime, &mut last_resource_warning) =>
                    {
                        continue;
                    }
                    Err(error) => {
                        tracing::error!(%error, "viosock listener failed");
                        return Err(error);
                    }
                };
                if !is_host(&address) {
                    tracing::warn!(
                        family = address.family(),
                        length = address.len(),
                        "refusing non-host vsock peer"
                    );
                    continue;
                }
                let Ok(permit) = permits.clone().try_acquire_owned() else {
                    if last_capacity_warning.is_none_or(|time: std::time::Instant| {
                        time.elapsed() >= Duration::from_secs(60)
                    }) {
                        tracing::warn!(
                            limit = MAX_CONNECTIONS,
                            "viosock connection capacity exhausted"
                        );
                        last_capacity_warning = Some(std::time::Instant::now());
                    }
                    continue;
                };
                match Stream::new(peer, stop.clone(), &tasks, &runtime, permit) {
                    Ok(stream) => {
                        if sender.try_send(Ok(stream)).is_err() {
                            break;
                        }
                    }
                    Err(error) => {
                        tracing::warn!(%error, "viosock connection setup failed");
                        continue;
                    }
                }
            }
            Ok(())
        });
        Ok(Self {
            incoming: Some(incoming),
            shutdown,
            workers,
            listener,
        })
    }

    pub(super) fn stream(&mut self) -> ReceiverStream<io::Result<Stream>> {
        ReceiverStream::new(self.incoming.take().expect("listener stream taken once"))
    }

    pub(super) async fn close(self) -> io::Result<()> {
        self.shutdown.cancel();
        self.workers.close();
        tokio::time::timeout(Duration::from_secs(5), async {
            let outcome = self.listener.await.map_err(io::Error::other)?;
            self.workers.wait().await;
            outcome
        })
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "viosock workers did not stop"))?
    }
}

pub(super) struct Stream {
    io: DuplexStream,
    socket: Arc<Socket>,
    shutdown: CancellationToken,
    cancelled: Pin<Box<tokio_util::sync::WaitForCancellationFutureOwned>>,
}

impl Stream {
    fn new(
        socket: Socket,
        parent: CancellationToken,
        workers: &TaskTracker,
        runtime: &tokio::runtime::Handle,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> io::Result<Self> {
        socket.set_nonblocking(true)?;
        let socket = Arc::new(socket);
        let shutdown = parent.child_token();
        let (io, bridge) = tokio::io::duplex(BUFFER_BYTES);
        let (reader, writer) = tokio::io::split(bridge);
        let permit = Arc::new(permit);
        let read_permit = permit.clone();
        let read_socket = socket.clone();
        let read_stop = shutdown.clone();
        let handle = runtime.clone();
        workers.spawn_blocking(move || {
            let _permit = read_permit;
            let _runtime = handle.enter();
            let mut sink = SyncIoBridge::new(writer);
            let mut buffer = [0; 16 * 1024];
            while !read_stop.is_cancelled() {
                match ready(&read_socket, Interest::Read) {
                    Ok(false) => continue,
                    Ok(true) => {}
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                    Err(error) => {
                        tracing::debug!(%error, "viosock read readiness failed");
                        break;
                    }
                }
                match (&*read_socket).read(&mut buffer) {
                    Ok(0) => {
                        // Forward the peer's half-close without closing our write side.
                        let _ = sink.shutdown();
                        return;
                    }
                    Ok(count) => {
                        if sink
                            .write_all(buffer.get(..count).expect("read count bounded by buffer"))
                            .is_err()
                        {
                            break;
                        }
                    }
                    Err(error)
                        if matches!(
                            error.kind(),
                            io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                        ) =>
                    {
                        continue;
                    }
                    Err(error) => {
                        tracing::debug!(%error, "viosock stream IO failed");
                        break;
                    }
                }
            }
            read_stop.cancel();
            let _ = read_socket.shutdown(Shutdown::Both);
        });
        let write_socket = socket.clone();
        let write_stop = shutdown.clone();
        let handle = runtime.clone();
        workers.spawn_blocking(move || {
            let _permit = permit;
            let _runtime = handle.enter();
            let mut source = SyncIoBridge::new(reader);
            let mut buffer = [0; 16 * 1024];
            'writing: while !write_stop.is_cancelled() {
                match source.read(&mut buffer) {
                    Ok(0) => {
                        let _ = write_socket.shutdown(Shutdown::Write);
                        return;
                    }
                    Err(error) => {
                        tracing::debug!(%error, "viosock stream IO failed");
                        break;
                    }
                    Ok(count) => {
                        let mut offset = 0;
                        while offset < count {
                            if write_stop.is_cancelled() {
                                break 'writing;
                            }
                            match ready(&write_socket, Interest::Write) {
                                Ok(false) => continue,
                                Ok(true) => {}
                                Err(error) if error.kind() == io::ErrorKind::Interrupted => {
                                    continue;
                                }
                                Err(_) => break 'writing,
                            }
                            match (&*write_socket).write(
                                buffer
                                    .get(offset..count)
                                    .expect("write offsets bounded by read count"),
                            ) {
                                Ok(0) => break 'writing,
                                Ok(written) => offset += written,
                                Err(error)
                                    if matches!(
                                        error.kind(),
                                        io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                                    ) =>
                                {
                                    continue;
                                }
                                Err(_) => break 'writing,
                            }
                        }
                    }
                }
            }
            write_stop.cancel();
            let _ = write_socket.shutdown(Shutdown::Both);
        });
        let cancelled = Box::pin(shutdown.clone().cancelled_owned());
        Ok(Self {
            io,
            socket,
            shutdown,
            cancelled,
        })
    }
}

impl Drop for Stream {
    fn drop(&mut self) {
        self.shutdown.cancel();
        let _ = self.socket.shutdown(Shutdown::Both);
    }
}
impl tonic::transport::server::Connected for Stream {
    type ConnectInfo = ();
    fn connect_info(&self) {}
}
impl AsyncRead for Stream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.cancelled.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut self.io).poll_read(cx, buffer)
    }
}
impl AsyncWrite for Stream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.cancelled.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
        }
        Pin::new(&mut self.io).poll_write(cx, data)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests;
