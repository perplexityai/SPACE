//! Server-speaks-first TCP under TLS interception.
//!
//! Interception needs a ClientHello, but FTP, SMTP, POP3 and IMAP clients wait
//! silently for the server's greeting. For a silent guest whose destination
//! was admitted without DNS evidence, [`probe`] dials the destination and
//! waits for one side to speak:
//!
//! - Destination first: the flow is relayed opaquely, greeting included.
//! - Guest first (data or EOF), or the destination closes without speaking:
//!   the guest's late bytes are scanned and admitted on the current policy,
//!   as on-time bytes would be. A plain relay of them reuses the unwritten
//!   probe connection, because some destinations accept only one (an FTP
//!   passive data port takes the first connection as the upload and an empty
//!   close as a complete one). A probe connection no relay takes is reset
//!   rather than closed, so such a destination sees a failure.
//! - Neither within [`PROBE_WINDOW`]: the flow is refused as before.
//!
//! No guest byte reaches the destination before it speaks or the guest's own
//! bytes are admitted. After a greeting the session is opaque, so a later
//! STARTTLS is not inspected, as with interception off for the same
//! address-allowed destination.

use std::future::Future;
use std::io;
use std::mem::MaybeUninit;
use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt as _};
use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use super::transport;

/// How long the probe waits, from the dial, for either side to speak. It
/// matches the dial's own bound and leaves room for servers that delay their
/// greeting.
pub(super) const PROBE_WINDOW: Duration = Duration::from_secs(10);

/// The most the probe reads of the destination's first write; the relay
/// carries the rest.
const GREETING_READ_BYTES: usize = 4096;

/// A probe connection nothing was written to, with what the destination sent.
pub(super) struct Probed<U> {
    pub(super) upstream: U,
    pub(super) received: Vec<u8>,
}

pub(super) enum Probe<U> {
    /// The destination wrote while the guest was silent.
    ServerFirst(Probed<U>),
    /// The guest wrote or closed first. The probe connection is kept unless
    /// the destination had closed it.
    GuestFirst(Option<Probed<U>>),
    /// Neither side spoke within the window.
    TimedOut,
    /// The destination closed or failed before speaking.
    UpstreamClosed,
    ConnectFailed(transport::Error),
    /// Shutdown or a policy resweep cancelled the flow.
    Cancelled,
}

/// Dial `dst`, then wait for the first of: cancellation, the guest speaking or
/// closing, the destination speaking, or `window` elapsing. The window covers
/// the dial too, and a dial that outlasts it fails as a connect timeout. The
/// guest does not cut the dial short, so a plain relay can reuse the
/// connection. The guest wins a tie. Writes nothing to the destination and
/// consumes nothing from the guest.
pub(super) async fn probe<F, Fut, U>(
    client: &TcpStream,
    dst: SocketAddr,
    connect: F,
    window: Duration,
    cancellation: &CancellationToken,
) -> Probe<U>
where
    F: FnOnce(SocketAddr) -> Fut,
    Fut: Future<Output = Result<U, transport::Error>>,
    U: AsyncRead + transport::Close + Unpin,
{
    enum Settled {
        Guest,
        /// What the destination sent, or `None` if it closed.
        Destination(Option<Vec<u8>>),
        Window,
    }

    let window = tokio::time::sleep(window);
    tokio::pin!(window);
    let mut upstream = tokio::select! {
        biased;
        () = cancellation.cancelled() => return Probe::Cancelled,
        connected = connect(dst) => match connected {
            Ok(upstream) => upstream,
            Err(error) => return Probe::ConnectFailed(error),
        },
        () = &mut window => return Probe::ConnectFailed(transport::Error::Timeout),
    };
    if let Err(error) = upstream.set_abortive_close(true) {
        warn!(%error, "could not make an unused probe connection's close abortive");
    }
    let destination_speaks = async {
        let mut greeting = vec![0_u8; GREETING_READ_BYTES];
        match upstream.read(&mut greeting).await {
            Ok(0) | Err(_) => None,
            Ok(read) => {
                greeting.truncate(read);
                Some(greeting)
            }
        }
    };
    // A peek consumes nothing, so the ClientHello scan still sees the bytes.
    let mut first_byte = [0_u8; 1];
    let settled = tokio::select! {
        biased;
        () = cancellation.cancelled() => return Probe::Cancelled,
        _ = client.peek(&mut first_byte) => Settled::Guest,
        greeting = destination_speaks => Settled::Destination(greeting),
        () = &mut window => Settled::Window,
    };
    // Tokio may not have seen the guest's bytes yet when the destination
    // settles, so ask the socket before letting the destination win.
    let guest_first = matches!(settled, Settled::Guest) || guest_has_spoken(client);
    match settled {
        Settled::Destination(None) if guest_first => Probe::GuestFirst(None),
        Settled::Destination(None) => Probe::UpstreamClosed,
        Settled::Destination(Some(received)) if guest_first => {
            Probe::GuestFirst(Some(Probed { upstream, received }))
        }
        Settled::Destination(Some(received)) => Probe::ServerFirst(Probed { upstream, received }),
        Settled::Window if !guest_first => Probe::TimedOut,
        Settled::Guest | Settled::Window => Probe::GuestFirst(Some(Probed {
            upstream,
            received: Vec::new(),
        })),
    }
}

/// Whether the guest has sent data or closed, read from the socket itself
/// rather than Tokio's readiness. Any error other than "nothing yet" counts,
/// so the flow takes the guest-first path, which fails closed.
fn guest_has_spoken(client: &TcpStream) -> bool {
    let mut byte = [MaybeUninit::<u8>::uninit()];
    // The socket is non-blocking, so this returns at once.
    loop {
        match socket2::SockRef::from(client).peek(&mut byte) {
            Ok(_) => return true,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return error.kind() != io::ErrorKind::WouldBlock,
        }
    }
}
