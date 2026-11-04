use std::io;
use std::net::{SocketAddr, SocketAddrV4};
use std::os::fd::AsFd;

pub fn get_original_dst_v4(_socket: &impl AsFd) -> io::Result<SocketAddrV4> {
    Err(unsupported())
}

pub fn set_recvorigdstaddr(_socket: &impl AsFd) -> io::Result<()> {
    Err(unsupported())
}

pub fn set_ip_transparent(_socket: &impl AsFd) -> io::Result<()> {
    Err(unsupported())
}

pub fn create_transparent_udp_socket(_bind_addr: SocketAddr) -> io::Result<tokio::net::UdpSocket> {
    Err(unsupported())
}

pub fn set_so_mark(_socket: &impl AsFd, _mark: u32) -> io::Result<()> {
    Err(unsupported())
}

pub fn set_ip_freebind(_socket: &impl AsFd) -> io::Result<()> {
    Err(unsupported())
}

pub fn original_dst_cmsg_space() -> Vec<u8> {
    Vec::new()
}

pub fn recvmsg_with_original_dst(
    _socket: &impl AsFd,
    _buf: &mut [u8],
    _cmsg_buf: &mut [u8],
) -> io::Result<(usize, SocketAddr, SocketAddrV4)> {
    Err(unsupported())
}

fn unsupported() -> io::Error {
    io::Error::new(
        io::ErrorKind::Unsupported,
        "transparent proxy original-destination helpers are only supported on Linux",
    )
}
