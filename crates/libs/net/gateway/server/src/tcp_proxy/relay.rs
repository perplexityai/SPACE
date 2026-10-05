//! Carrying one admitted flow to its destination.

use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::copy_bidirectional;
use tokio::net::TcpStream;

use crate::orig_dst;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Which half of the relay failed.
pub(super) enum Failure {
    Connect(io::Error),
    Copy(io::Error),
}

/// Dial `dst` and shuttle bytes until both directions close. Returns the wire
/// bytes carried, guest-to-destination first.
pub(super) async fn run(mut client: TcpStream, dst: SocketAddr) -> Result<(u64, u64), Failure> {
    let mut upstream = connect(dst).await.map_err(Failure::Connect)?;
    copy_bidirectional(&mut client, &mut upstream)
        .await
        .map_err(Failure::Copy)
}

/// `SO_MARK` keeps the reply from being re-captured by this gateway's own TPROXY
/// rule, which would otherwise make it connect to itself.
pub(super) async fn connect(dst: SocketAddr) -> io::Result<TcpStream> {
    let socket = socket2::Socket::new(
        socket2::Domain::IPV4,
        socket2::Type::STREAM,
        Some(socket2::Protocol::TCP),
    )?;
    orig_dst::set_so_mark(&socket, pplx_space_net_gateway_client::OUTBOUND_MARK)?;
    socket.set_nonblocking(true)?;
    match socket.connect(&dst.into()) {
        Ok(()) => {}
        Err(error) if connect_in_progress(&error) => {}
        Err(error) => return Err(error),
    }

    // A non-blocking connect signals completion by becoming writable, and
    // reports failure only through SO_ERROR — a failed connect is writable too.
    let upstream = TcpStream::from_std(std::net::TcpStream::from(socket))?;
    tokio::time::timeout(CONNECT_TIMEOUT, upstream.writable())
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "TCP connect timed out"))??;
    match upstream.take_error()? {
        Some(error) => Err(error),
        None => Ok(upstream),
    }
}

fn connect_in_progress(error: &io::Error) -> bool {
    matches!(
        error.raw_os_error(),
        Some(libc::EINPROGRESS) | Some(libc::EALREADY)
    ) || error.kind() == io::ErrorKind::WouldBlock
}
