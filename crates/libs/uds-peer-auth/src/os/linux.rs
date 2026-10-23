//! Linux `/proc`-based peer process inspection.
//!
//! Reads `/proc/<pid>/{status,exe}` and returns a normalized
//! [`PeerProcessSnapshot`] for the per-account checks in
//! [`super::super::evaluate_process_checks`]. Status is read first so
//! a dead or non-existent pid fails before we bother resolving the
//! exe symlink. The exe path has any kernel ` (deleted)` marker
//! stripped (`strip_exe_deleted_marker`) before being stored, so a
//! replaced-but-running peer doesn't poison the allowlist comparison.

use pplx_space_common_core::host::linux::paths::{exe_link_was_deleted, strip_exe_deleted_marker};

use super::super::PeerProcessSnapshot;

/// `/proc/<pid>/{status,exe}` snapshot for a peer pid.
pub(crate) fn read_peer_process(pid: i32) -> std::io::Result<PeerProcessSnapshot> {
    let status_text = std::fs::read_to_string(format!("/proc/{pid}/status"))?;
    let parsed = parse_proc_status(&status_text).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "malformed /proc/<pid>/status: missing Uid or CapEff",
        )
    })?;
    let raw_exe = std::fs::read_link(format!("/proc/{pid}/exe"))?;
    Ok(PeerProcessSnapshot {
        exe: strip_exe_deleted_marker(&raw_exe),
        exe_deleted: exe_link_was_deleted(&raw_exe),
        effective_uid: parsed.effective_uid,
        cap_effective: parsed.cap_effective,
    })
}

/// `CapEff:` of this process, for the cross-uid readability preflight.
/// `None` when `/proc/self/status` cannot be read or parsed.
pub(crate) fn current_process_cap_effective() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    Some(parse_proc_status(&status)?.cap_effective)
}

/// Subset of `/proc/<pid>/status` we care about: the effective uid (Uid
/// column 2) plus the `CapEff:` bitmask. The real uid (column 1) is parsed
/// only to locate the effective one and is not retained — authorization
/// compares `SO_PEERCRED` against the *effective* uid.
#[derive(Clone, Debug, Eq, PartialEq)]
struct ParsedProcStatus {
    effective_uid: u32,
    cap_effective: u64,
}

/// Parse the `Uid:` and `CapEff:` lines out of `/proc/<pid>/status`.
/// Returns `None` if either is missing or malformed; callers treat
/// that as a hard failure.
fn parse_proc_status(s: &str) -> Option<ParsedProcStatus> {
    let mut effective_uid = None;
    let mut cap_effective = None;
    for line in s.lines() {
        if let Some(rest) = line.strip_prefix("Uid:") {
            // Layout: "Uid: <real> <effective> <saved> <fs>". Both columns
            // must be present for the line to be well-formed, so parse the
            // real uid to validate the layout, then keep only the effective.
            let mut iter = rest.split_whitespace();
            let real_uid = iter.next().and_then(|s| s.parse::<u32>().ok());
            effective_uid = real_uid.and(iter.next().and_then(|s| s.parse().ok()));
        } else if let Some(rest) = line.strip_prefix("CapEff:") {
            cap_effective = u64::from_str_radix(rest.trim(), 16).ok();
        }
    }
    Some(ParsedProcStatus {
        effective_uid: effective_uid?,
        cap_effective: cap_effective?,
    })
}

#[cfg(test)]
mod tests;
