//! TCP listeners for the services' HTTP servers.

use tokio::net::{TcpListener, TcpStream};

/// A listener whose accepted connections have Nagle's algorithm disabled.
pub type NodelayListener = axum::serve::TapIo<TcpListener, fn(&mut TcpStream)>;

/// Accept connections with `TCP_NODELAY` set.
///
/// `axum::serve` leaves Nagle enabled on a plain `TcpListener`. A response
/// written in two small pieces, headers and then a short body, then waits for
/// the client's delayed ACK before the body goes out, which adds about 40ms to
/// every small response on a reused connection.
pub fn nodelay(listener: TcpListener) -> NodelayListener {
    fn set_nodelay(tcp: &mut TcpStream) {
        if let Err(error) = tcp.set_nodelay(true) {
            tracing::debug!(%error, "failed to set TCP_NODELAY on an accepted connection");
        }
    }
    axum::serve::ListenerExt::tap_io(listener, set_nodelay as fn(&mut TcpStream))
}

#[cfg(test)]
mod tests;
