use std::io;

use hickory_resolver::net::NetError;

use super::{Report, SendPath};

/// A failed shared reconnect has its own WARN, and a reused connection's
/// close is the lookup's to report, since Hickory retries the request.
#[test]
fn send_failure_level_follows_its_cause() {
    let io = |kind| NetError::from(io::Error::from(kind));
    let reused = SendPath::Exchange { reused: true };
    let fresh = SendPath::Exchange { reused: false };
    for (error, path, report) in [
        (NetError::Timeout, SendPath::SharedReconnect, Report::Debug),
        (
            io(io::ErrorKind::ConnectionRefused),
            SendPath::SharedReconnect,
            Report::Debug,
        ),
        (
            io(io::ErrorKind::UnexpectedEof),
            reused,
            Report::UnlessLookupFails,
        ),
        (io(io::ErrorKind::UnexpectedEof), fresh, Report::Warn),
        (io(io::ErrorKind::ConnectionReset), reused, Report::Warn),
        (NetError::Timeout, reused, Report::Warn),
    ] {
        assert_eq!(Report::of(&error, path), report, "{error:?} on {path:?}");
    }
}
