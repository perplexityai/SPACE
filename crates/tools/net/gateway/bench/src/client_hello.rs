//! A deterministic TLS ClientHello, built byte by byte.
//!
//! The SNI workloads need the gateway's ClientHello peek to succeed on real
//! wire bytes, but a real TLS handshake would put key exchange into the
//! measurement. Emitting the handshake record directly keeps the sniff path
//! exercised and the numbers free of crypto cost. Mirrors the byte shapes the
//! gateway's own `tls_sni` fixtures cover, which are crate-private to it.

const TLS_HANDSHAKE_CONTENT_TYPE: u8 = 0x16;
const CLIENT_HELLO_MSG_TYPE: u8 = 0x01;
const SERVER_NAME_EXTENSION: u16 = 0x0000;
const HOST_NAME_TYPE: u8 = 0x00;
// rustls requires signature_algorithms before it exposes a ClientHello.
const SIGNATURE_ALGORITHMS_EXTENSION: [u8; 8] = [0x00, 0x0d, 0x00, 0x04, 0x00, 0x02, 0x08, 0x04];

/// A complete TLS handshake record carrying a ClientHello with one `host_name`
/// SNI entry.
pub fn with_server_name(server_name: &str) -> Vec<u8> {
    handshake_record(
        CLIENT_HELLO_MSG_TYPE,
        &client_hello_body(&sni_extension(server_name.as_bytes())),
    )
}

/// The same record without SNI, so the peek finds no server name.
pub fn without_server_name() -> Vec<u8> {
    handshake_record(CLIENT_HELLO_MSG_TYPE, &client_hello_body(&[]))
}

/// Append the body written by `f`, preceded by its length in `prefix_bytes`
/// big-endian bytes.
fn with_len_prefix(out: &mut Vec<u8>, prefix_bytes: usize, f: impl FnOnce(&mut Vec<u8>)) {
    let len_pos = out.len();
    out.extend(std::iter::repeat_n(0u8, prefix_bytes));
    let body_start = out.len();
    f(out);
    let body_len = out.len() - body_start;
    let encoded = body_len.to_be_bytes();
    let encoded_tail = encoded
        .get(encoded.len() - prefix_bytes..)
        .expect("prefix_bytes never exceeds the width of a usize");
    out.get_mut(len_pos..body_start)
        .expect("the placeholder bytes were just reserved")
        .copy_from_slice(encoded_tail);
}

fn handshake_record(msg_type: u8, body: &[u8]) -> Vec<u8> {
    let mut record = vec![TLS_HANDSHAKE_CONTENT_TYPE, 0x03, 0x01];
    with_len_prefix(&mut record, 2, |record| {
        record.push(msg_type);
        with_len_prefix(record, 3, |handshake| handshake.extend_from_slice(body));
    });
    record
}

fn client_hello_body(raw_extensions: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&[0x03, 0x03]); // legacy_version = TLS 1.2
    body.extend_from_slice(&[0x42; 32]); // random
    with_len_prefix(&mut body, 1, |_session_id| {});
    with_len_prefix(&mut body, 2, |suites| {
        suites.extend_from_slice(&[0x13, 0x01])
    });
    with_len_prefix(&mut body, 1, |compression| compression.push(0x00));
    with_len_prefix(&mut body, 2, |extensions| {
        extensions.extend_from_slice(raw_extensions);
        extensions.extend_from_slice(&SIGNATURE_ALGORITHMS_EXTENSION);
    });
    body
}

fn sni_extension(server_name: &[u8]) -> Vec<u8> {
    let mut extension = SERVER_NAME_EXTENSION.to_be_bytes().to_vec();
    with_len_prefix(&mut extension, 2, |data| {
        with_len_prefix(data, 2, |list| {
            list.push(HOST_NAME_TYPE);
            with_len_prefix(list, 2, |name| name.extend_from_slice(server_name));
        });
    });
    extension
}

#[cfg(test)]
mod tests;
