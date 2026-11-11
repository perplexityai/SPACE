//! Establish a marked outbound TCP connection.
use crate::orig_dst;
use std::{io, net::SocketAddr, time::Duration};
use tokio::net::TcpStream;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, thiserror::Error)]
pub(super) enum Error {
    #[error("TCP connect exceeded its ten-second deadline")]
    Timeout,
    #[error("TCP connect failed ({kind:?})", kind = .0.kind())]
    Io(#[from] io::Error),
}

impl Error {
    pub(super) fn io_kind(&self) -> Option<io::ErrorKind> {
        match self {
            Self::Timeout => None,
            Self::Io(error) => Some(error.kind()),
        }
    }
}

/// `SO_MARK` keeps the reply from being re-captured by this gateway's own TPROXY
/// rule, which would otherwise make it connect to itself.
pub(super) async fn connect(dst: SocketAddr) -> Result<TcpStream, Error> {
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
        Err(error) => return Err(error.into()),
    }

    // A non-blocking connect signals completion by becoming writable, and
    // reports failure only through SO_ERROR — a failed connect is writable too.
    let upstream = TcpStream::from_std(std::net::TcpStream::from(socket))?;
    tokio::time::timeout(CONNECT_TIMEOUT, upstream.writable())
        .await
        .map_err(|_| Error::Timeout)??;
    match upstream.take_error()? {
        Some(error) => Err(error.into()),
        None => Ok(upstream),
    }
}

fn connect_in_progress(error: &io::Error) -> bool {
    matches!(
        error.raw_os_error(),
        Some(libc::EINPROGRESS) | Some(libc::EALREADY)
    ) || error.kind() == io::ErrorKind::WouldBlock
}
