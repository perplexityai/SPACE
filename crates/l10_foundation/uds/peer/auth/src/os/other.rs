//! Non-Linux fallback for peer process inspection.
//!
//! `/proc` is Linux-only. On any other host this returns
//! `io::ErrorKind::Unsupported`, which the authorizer maps to
//! `UNAUTHENTICATED`. Opt-in process checks therefore **fail closed**
//! on non-Linux platforms; the rest of UDS peer auth (filesystem
//! perms + `SO_PEERCRED` uid match) is unaffected.

use super::super::PeerProcessSnapshot;

pub(crate) fn read_peer_process(_pid: i32) -> std::io::Result<PeerProcessSnapshot> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "peer process inspection requires Linux /proc",
    ))
}
