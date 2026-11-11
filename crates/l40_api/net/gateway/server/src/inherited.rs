use std::io;
use std::os::fd::{FromRawFd, RawFd};

use tokio::net::TcpListener;

pub fn tcp_listener_from_fd(fd: RawFd) -> io::Result<TcpListener> {
    // SAFETY: node-manager passes the gateway a valid, owned TCP listener fd.
    // `from_raw_fd` takes ownership so the fd is closed exactly once.
    let listener = unsafe { std::net::TcpListener::from_raw_fd(fd) };
    listener.set_nonblocking(true)?;
    TcpListener::from_std(listener)
}

pub fn udp_socket_from_fd(fd: RawFd) -> io::Result<std::net::UdpSocket> {
    // SAFETY: node-manager passes the gateway a valid, owned UDP socket fd.
    // `from_raw_fd` takes ownership so the fd is closed exactly once.
    let socket = unsafe { std::net::UdpSocket::from_raw_fd(fd) };
    socket.set_nonblocking(true)?;
    Ok(socket)
}
