//! The rows a host-control caller's peer credentials name.

use crate::db::{Connection, Result, SandboxRow};

/// Which registered process the peer credentials are matched against.
#[derive(Clone, Copy, Debug)]
pub enum Peer {
    /// A net gateway: only rows in states where a live gateway can exist
    /// match, because terminal rows keep the pid and slot uid of a dead
    /// gateway that a later sandbox's gateway may reuse.
    NetGateway { netgtw_pid: u32, uid: u32 },
    /// A spaced host: every row with the registered pid and uid matches.
    SpacedHost { spaced_host_pid: u32, uid: u32 },
}

/// At most two rows: two prove the credentials ambiguous. Whole rows decode
/// strictly.
pub fn by_peer(conn: &Connection, peer: Peer) -> Result<Vec<SandboxRow>> {
    match peer {
        Peer::NetGateway { netgtw_pid, uid } => {
            crate::db::list_by_netgtw_pid_uid(conn, netgtw_pid, uid)
        }
        Peer::SpacedHost {
            spaced_host_pid,
            uid,
        } => crate::db::list_by_spaced_host_pid_uid(conn, spaced_host_pid, uid),
    }
}
