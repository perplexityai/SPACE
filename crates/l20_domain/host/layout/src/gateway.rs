//! Disjoint, slot-derived host identities for VM control and network gateways.

use pplx_space_linux_ids::{LinuxUid, MAX_LINUX_NETWORK_SLOT};

/// First UID in the existing VM/control identity range.
pub const SANDBOX_UID_BASE: u32 = 10_000;
/// First UID in the reserved gateway range, disjoint across the full slot space.
pub const GATEWAY_UID_BASE: u32 = 2_000_000;
/// Supervisor-issued secret, delivered only to the isolated gateway process.
pub const LAUNCH_CAPABILITY_ENV: &str = "SPACE_GATEWAY_LAUNCH_CAPABILITY";

/// Derive an isolated gateway identity from its allocated VM identity.
pub fn gateway_uid(sandbox_uid: LinuxUid) -> Option<LinuxUid> {
    let slot = sandbox_uid.get().checked_sub(SANDBOX_UID_BASE)?;
    (slot <= MAX_LINUX_NETWORK_SLOT).then(|| LinuxUid::new(GATEWAY_UID_BASE + slot))
}

/// Resolve a gateway peer to the sandbox identity stored with its exact PID.
/// Legacy gateways remain supported while resident processes are cycled.
pub fn sandbox_uid(peer_uid: LinuxUid) -> Option<LinuxUid> {
    if gateway_uid(peer_uid).is_some() {
        return Some(peer_uid);
    }
    let slot = peer_uid.get().checked_sub(GATEWAY_UID_BASE)?;
    (slot <= MAX_LINUX_NETWORK_SLOT).then(|| LinuxUid::new(SANDBOX_UID_BASE + slot))
}

#[cfg(test)]
mod tests;
