use std::io;
use std::io::IoSliceMut;
use std::net::{SocketAddr, SocketAddrV4};
use std::os::fd::{AsFd, AsRawFd};

use nix::errno::Errno;
use nix::sys::socket::{
    ControlMessageOwned, MsgFlags, SockaddrIn, getsockopt, recvmsg, setsockopt, sockopt,
};

/// Retrieve the original destination of a TCP connection that was REDIRECT'd
/// by nftables. Uses `getsockopt(SOL_IP, SO_ORIGINAL_DST)`.
pub fn get_original_dst_v4(socket: &impl AsFd) -> io::Result<SocketAddrV4> {
    let addr = getsockopt(socket, sockopt::OriginalDst).map_err(errno_to_io)?;
    sockaddr_in_to_v4(addr).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "SO_ORIGINAL_DST returned a non-IPv4 address",
        )
    })
}

/// Set `IP_RECVORIGDSTADDR` on a UDP socket so that `recvmsg` returns
/// the original destination as ancillary data.
pub fn set_recvorigdstaddr(socket: &impl AsFd) -> io::Result<()> {
    setsockopt(socket, sockopt::Ipv4OrigDstAddr, &true).map_err(errno_to_io)
}

/// Set `IP_TRANSPARENT` on a socket. Required for TPROXY — allows the socket
/// to receive packets destined for non-local addresses.
/// Must be called with `CAP_NET_ADMIN` (i.e., as root, before dropping privileges).
pub fn set_ip_transparent(socket: &impl AsFd) -> io::Result<()> {
    setsockopt(socket, sockopt::IpTransparent, &true).map_err(errno_to_io)
}

/// Create a UDP socket with `IP_TRANSPARENT`, bound to the given address.
/// `IP_TRANSPARENT` is set BEFORE bind, allowing binding to non-local addresses.
/// Used for per-flow outbound sockets that impersonate the guest's source address.
/// Requires `CAP_NET_ADMIN` in the calling process.
pub fn create_transparent_udp_socket(bind_addr: SocketAddr) -> io::Result<tokio::net::UdpSocket> {
    if !bind_addr.is_ipv4() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "IPv4 only"));
    }

    let socket = socket2::Socket::new(
        socket2::Domain::IPV4,
        socket2::Type::DGRAM,
        Some(socket2::Protocol::UDP),
    )?;
    socket.set_nonblocking(true)?;
    // Set IP_TRANSPARENT before bind so non-local guest addresses are accepted.
    set_ip_transparent(&socket)?;
    socket.bind(&socket2::SockAddr::from(bind_addr))?;
    let std_socket: std::net::UdpSocket = socket.into();
    tokio::net::UdpSocket::from_std(std_socket)
}

/// Set `SO_MARK` on a socket. Used to mark outbound proxy traffic so nftables
/// can skip TPROXY re-interception for proxy-originated packets.
pub fn set_so_mark(socket: &impl AsFd, mark: u32) -> io::Result<()> {
    setsockopt(socket, sockopt::Mark, &mark).map_err(errno_to_io)
}

/// Set `IP_FREEBIND` on a socket so it can bind to non-local IPv4 addresses.
pub fn set_ip_freebind(socket: &impl AsFd) -> io::Result<()> {
    setsockopt(socket, sockopt::IpFreebind, &true).map_err(errno_to_io)
}

/// Allocate a control-message buffer large enough for IPv4 original destination metadata.
pub fn original_dst_cmsg_space() -> Vec<u8> {
    nix::cmsg_space!(libc::sockaddr_in)
}

fn sockaddr_in_to_v4(addr: libc::sockaddr_in) -> Option<SocketAddrV4> {
    if addr.sin_family != libc::AF_INET as libc::sa_family_t {
        return None;
    }
    Some(SockaddrIn::from(addr).into())
}

/// Receive a UDP datagram and extract its original destination from ancillary data.
pub fn recvmsg_with_original_dst(
    socket: &impl AsFd,
    buf: &mut [u8],
    cmsg_buf: &mut [u8],
) -> io::Result<(usize, SocketAddr, SocketAddrV4)> {
    let mut iov = [IoSliceMut::new(buf)];
    let msg = recvmsg::<SockaddrIn>(
        socket.as_fd().as_raw_fd(),
        &mut iov,
        Some(cmsg_buf),
        MsgFlags::MSG_DONTWAIT,
    )
    .map_err(errno_to_io)?;

    let client_addr = msg.address.map(SocketAddr::from).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "recvmsg did not return a UDP peer address",
        )
    })?;

    for cmsg in msg.cmsgs().map_err(cmsg_error_to_io)? {
        if let ControlMessageOwned::Ipv4OrigDstAddr(addr) = cmsg
            && let Some(dst) = sockaddr_in_to_v4(addr)
        {
            return Ok((msg.bytes, client_addr, dst));
        }
    }

    Err(io::Error::other("no IP_ORIGDSTADDR in cmsg"))
}

fn cmsg_error_to_io(error: Errno) -> io::Error {
    if error == Errno::ENOBUFS {
        io::Error::other("truncated original-destination control data")
    } else {
        errno_to_io(error)
    }
}

fn errno_to_io(error: Errno) -> io::Error {
    io::Error::from_raw_os_error(error as i32)
}

#[cfg(test)]
mod tests;
