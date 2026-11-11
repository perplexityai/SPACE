//! Passive extraction of the TLS Server Name Indication (SNI) from the
//! unencrypted ClientHello at the head of a TCP flow.
//!
//! TLS sends the first ClientHello in the clear, and a client that uses SNI
//! (RFC 6066) names the destination host in a `server_name` extension. The
//! gateway peeks those leading bytes — without consuming them — and runs them
//! through [`scan_client_hello`] to learn the intended host *before* the
//! encrypted session begins. The extracted name is the input a domain-egress
//! policy can allow/deny on.
//!
//! Parsing is delegated to rustls, which also handles the gateway's TLS
//! interception. The bytes are attacker-controlled guest traffic, and the
//! scanner returns a typed verdict without consuming the forwarded stream.

use pplx_space_net_common::DomainName;
use rustls::{Error as TlsError, InvalidMessage, PeerMisbehaved, server::Acceptor};
use std::io::Cursor;

/// First byte of a TLS handshake record (`content_type = handshake`). A cheap
/// pre-filter: a flow whose first byte is anything else is not a TLS
/// ClientHello and is rejected without invoking the parser.
const TLS_HANDSHAKE_CONTENT_TYPE: u8 = 0x16;

/// Verdict of scanning the head of a TCP flow for a TLS ClientHello SNI.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClientHelloScan {
    /// The flow does not begin with a TLS handshake record — e.g. plaintext
    /// HTTP or any non-TLS protocol. Not an error: most flows are not TLS.
    NotTls,
    /// The bytes begin like a TLS handshake record but the ClientHello is not
    /// yet complete; the caller peeked too few bytes (or the client has not
    /// sent them all). Best-effort sniffing skips these rather than blocking.
    Incomplete,
    /// A ClientHello carrying a usable SNI `host_name`.
    ServerName(DomainName),
    /// A well-formed ClientHello with no usable SNI `host_name` (the extension
    /// is absent or rustls ignores an IP literal in it).
    NoServerName,
    /// Bytes that announce a TLS handshake record but fail to parse as a valid
    /// ClientHello, or whose SNI value is not a usable host name. Suspicious;
    /// worth surfacing distinctly.
    Malformed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ClientHelloParseDetail {
    SniNotFound,
    SniInvalid,
    MalformedClientHello,
}

type ClientHelloScanWithDetail = (
    ClientHelloScan,
    ClientAlpnOffer,
    Option<ClientHelloParseDetail>,
);

impl ClientHelloScan {
    /// The extracted host name, if this verdict carries one.
    pub fn server_name(&self) -> Option<&DomainName> {
        match self {
            Self::ServerName(name) => Some(name),
            _ => None,
        }
    }
}

/// An HTTP application protocol that the interception proxy can terminate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum HttpAlpn {
    Http2,
    Http1,
}

impl HttpAlpn {
    pub(crate) fn from_wire(value: &[u8]) -> Option<Self> {
        match value {
            b"h2" => Some(Self::Http2),
            b"http/1.1" => Some(Self::Http1),
            _ => None,
        }
    }

    pub(crate) const fn wire_value(self) -> &'static [u8] {
        match self {
            Self::Http2 => b"h2",
            Self::Http1 => b"http/1.1",
        }
    }
}

/// Ordered HTTP protocols offered in a TLS ClientHello after clamping the
/// guest's list to the protocols the interception proxy can terminate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ClientAlpnOffer {
    Unavailable,
    NoAlpn,
    Http1,
    Http2,
    Http2ThenHttp1,
    Http1ThenHttp2,
    Unsupported,
}

impl ClientAlpnOffer {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Unavailable => "unavailable",
            Self::NoAlpn => "none",
            Self::Http1 => "http1",
            Self::Http2 => "h2_without_http1",
            Self::Http2ThenHttp1 => "h2_http1",
            Self::Http1ThenHttp2 => "http1_h2",
            Self::Unsupported => "other",
        }
    }

    pub(crate) const fn supported(self) -> Option<&'static [HttpAlpn]> {
        match self {
            Self::Unavailable | Self::Unsupported => None,
            Self::NoAlpn => Some(&[]),
            Self::Http1 => Some(&[HttpAlpn::Http1]),
            Self::Http2 => Some(&[HttpAlpn::Http2]),
            Self::Http2ThenHttp1 => Some(&[HttpAlpn::Http2, HttpAlpn::Http1]),
            Self::Http1ThenHttp2 => Some(&[HttpAlpn::Http1, HttpAlpn::Http2]),
        }
    }

    pub(crate) fn supports(self, protocol: HttpAlpn) -> bool {
        self.supported()
            .is_some_and(|offered| offered.contains(&protocol))
    }
}

/// Scan the leading bytes of a TCP flow for a TLS ClientHello and extract its
/// SNI. `buf` is the (possibly partial) prefix peeked from the client; the
/// scan is read-only and the bytes are left in the stream for the real proxy.
///
/// Never panics: any byte sequence maps to one of the [`ClientHelloScan`]
/// variants.
pub fn scan_client_hello(buf: &[u8]) -> ClientHelloScan {
    scan_client_hello_with_alpn(buf).0
}

pub(crate) fn scan_client_hello_with_alpn(buf: &[u8]) -> (ClientHelloScan, ClientAlpnOffer) {
    let (scan, alpn, _) = scan_client_hello_with_detail(buf);
    (scan, alpn)
}

pub(crate) fn scan_client_hello_with_detail(buf: &[u8]) -> ClientHelloScanWithDetail {
    match buf.first() {
        None => {
            return (
                ClientHelloScan::Incomplete,
                ClientAlpnOffer::Unavailable,
                None,
            );
        }
        Some(&first) if first != TLS_HANDSHAKE_CONTENT_TYPE => {
            return (ClientHelloScan::NotTls, ClientAlpnOffer::Unavailable, None);
        }
        Some(_) => {}
    }

    let mut acceptor = Acceptor::default();
    let mut cursor = Cursor::new(buf);
    loop {
        let read = match acceptor.read_tls(&mut cursor) {
            Ok(read) => read,
            Err(_) => return invalid_client_hello(ClientHelloParseDetail::MalformedClientHello),
        };
        match acceptor.accept() {
            Ok(Some(accepted)) => {
                let hello = accepted.client_hello();
                let alpn = client_alpn_offer(hello.alpn());
                return match hello.server_name() {
                    Some(raw_name) => match raw_name.parse::<DomainName>() {
                        Ok(name) => (ClientHelloScan::ServerName(name), alpn, None),
                        Err(_) => missing_sni(
                            ClientHelloScan::Malformed,
                            alpn,
                            ClientHelloParseDetail::SniInvalid,
                        ),
                    },
                    None => missing_sni(
                        ClientHelloScan::NoServerName,
                        alpn,
                        ClientHelloParseDetail::SniNotFound,
                    ),
                };
            }
            Ok(None) if read == 0 || cursor.position() >= buf.len() as u64 => {
                return (
                    ClientHelloScan::Incomplete,
                    ClientAlpnOffer::Unavailable,
                    None,
                );
            }
            Ok(None) => {}
            Err((error, _)) => {
                let detail = match error {
                    TlsError::InvalidMessage(InvalidMessage::InvalidServerName)
                    | TlsError::PeerMisbehaved(PeerMisbehaved::ServerNameMustContainOneHostName) => {
                        ClientHelloParseDetail::SniInvalid
                    }
                    _ => ClientHelloParseDetail::MalformedClientHello,
                };
                return invalid_client_hello(detail);
            }
        }
    }
}

fn invalid_client_hello(detail: ClientHelloParseDetail) -> ClientHelloScanWithDetail {
    missing_sni(
        ClientHelloScan::Malformed,
        ClientAlpnOffer::Unavailable,
        detail,
    )
}

fn missing_sni(
    scan: ClientHelloScan,
    alpn: ClientAlpnOffer,
    detail: ClientHelloParseDetail,
) -> ClientHelloScanWithDetail {
    (scan, alpn, Some(detail))
}

fn client_alpn_offer<'a>(protocols: Option<impl Iterator<Item = &'a [u8]>>) -> ClientAlpnOffer {
    let Some(protocols) = protocols else {
        return ClientAlpnOffer::NoAlpn;
    };
    let mut first = None;
    let mut second = None;
    for protocol in protocols {
        let supported = HttpAlpn::from_wire(protocol);
        let Some(supported) = supported else {
            continue;
        };
        if first.is_none() {
            first = Some(supported);
        } else if first != Some(supported) && second.is_none() {
            second = Some(supported);
        }
    }
    match (first, second) {
        (Some(HttpAlpn::Http1), None) => ClientAlpnOffer::Http1,
        (Some(HttpAlpn::Http2), None) => ClientAlpnOffer::Http2,
        (Some(HttpAlpn::Http2), Some(HttpAlpn::Http1)) => ClientAlpnOffer::Http2ThenHttp1,
        (Some(HttpAlpn::Http1), Some(HttpAlpn::Http2)) => ClientAlpnOffer::Http1ThenHttp2,
        (None, None) => ClientAlpnOffer::Unsupported,
        (None, Some(_)) | (Some(_), Some(_)) => ClientAlpnOffer::Unsupported,
    }
}

/// ClientHello fixture builders. Public to the crate so the gateway's
/// socket-level tests can drive the same byte shapes the unit tests cover.
#[cfg(test)]
pub(crate) mod test_fixtures {
    use super::TLS_HANDSHAKE_CONTENT_TYPE;

    /// Append a length-prefixed block: the body written by `f`, preceded by its
    /// length in `prefix_bytes` big-endian bytes (1, 2, or 3).
    fn with_len_prefix(out: &mut Vec<u8>, prefix_bytes: usize, f: impl FnOnce(&mut Vec<u8>)) {
        let len_pos = out.len();
        out.extend(std::iter::repeat_n(0u8, prefix_bytes));
        let body_start = out.len();
        f(out);
        let body_len = out.len() - body_start;
        let encoded = body_len.to_be_bytes();
        out[len_pos..body_start].copy_from_slice(&encoded[encoded.len() - prefix_bytes..]);
    }

    /// Wrap a handshake message (`msg_type` + `body`) in a TLS handshake record.
    pub(crate) fn handshake_record(msg_type: u8, body: &[u8]) -> Vec<u8> {
        let mut record = vec![TLS_HANDSHAKE_CONTENT_TYPE, 0x03, 0x01];
        with_len_prefix(&mut record, 2, |record| {
            record.push(msg_type);
            with_len_prefix(record, 3, |hs| hs.extend_from_slice(body));
        });
        record
    }

    /// A ClientHello message body carrying the given raw extensions block.
    fn client_hello_body(raw_extensions: &[u8]) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&[0x03, 0x03]); // legacy_version = TLS 1.2
        body.extend_from_slice(&[0x42; 32]); // random
        with_len_prefix(&mut body, 1, |_sid| {}); // empty session_id
        with_len_prefix(&mut body, 2, |cs| cs.extend_from_slice(&[0x13, 0x01])); // one suite
        with_len_prefix(&mut body, 1, |comp| comp.push(0x00)); // null compression
        with_len_prefix(&mut body, 2, |exts| {
            exts.extend_from_slice(raw_extensions);
            // rustls requires this extension on a TLS ClientHello. Keep the
            // tested SNI/ALPN payload unchanged while making fixtures valid.
            exts.extend_from_slice(&[0x00, 0x0d, 0x00, 0x04, 0x00, 0x02, 0x08, 0x04]);
        });
        body
    }

    /// A full ClientHello record carrying the given raw extensions block.
    pub(crate) fn client_hello_with_extensions(raw_extensions: &[u8]) -> Vec<u8> {
        handshake_record(0x01, &client_hello_body(raw_extensions))
    }

    /// Build a `server_name` (SNI) extension from `(name_type, name)` entries.
    pub(crate) fn sni_extension(entries: &[(u8, &[u8])]) -> Vec<u8> {
        let mut extension = vec![0x00, 0x00]; // extension_type = server_name
        with_len_prefix(&mut extension, 2, |ext_data| {
            with_len_prefix(ext_data, 2, |list| {
                for (name_type, name) in entries {
                    list.push(*name_type);
                    with_len_prefix(list, 2, |n| n.extend_from_slice(name));
                }
            });
        });
        extension
    }

    /// Build an application-layer protocol negotiation extension from wire
    /// protocol names.
    pub(crate) fn alpn_extension(protocols: &[&[u8]]) -> Vec<u8> {
        let mut extension = vec![0x00, 0x10]; // extension_type = ALPN
        with_len_prefix(&mut extension, 2, |ext_data| {
            with_len_prefix(ext_data, 2, |list| {
                for protocol in protocols {
                    with_len_prefix(list, 1, |name| name.extend_from_slice(protocol));
                }
            });
        });
        extension
    }

    /// A non-SNI extension (e.g. a GREASE or padding extension) of the given
    /// type and payload length, used to exercise extension-order independence.
    pub(crate) fn filler_extension(ext_type: u16, payload_len: usize) -> Vec<u8> {
        let mut extension = ext_type.to_be_bytes().to_vec();
        with_len_prefix(&mut extension, 2, |data| {
            data.extend(std::iter::repeat_n(0u8, payload_len))
        });
        extension
    }

    /// Convenience: a ClientHello with a single `host_name` SNI (or none).
    pub(crate) fn client_hello(server_name: Option<&str>) -> Vec<u8> {
        let extensions = match server_name {
            Some(name) => sni_extension(&[(0x00, name.as_bytes())]),
            None => Vec::new(),
        };
        client_hello_with_extensions(&extensions)
    }
}

#[cfg(test)]
mod tests;
