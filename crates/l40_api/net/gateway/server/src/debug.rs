//! Per-sandbox gateway debug-logging deadline.
//!
//! `EnableDebug` sets a wall-clock deadline; the proxy hot paths read it through
//! [`GatewayDebugState::is_enabled`] to gate verbose, sandbox-scoped
//! network-policy logs. The state is a lone expiry timestamp held inside the
//! gateway and clamped to at most [`MAX_DEBUG_WINDOW`], so a debug session always
//! self-expires and can never become a standing production log stream. It also
//! clears on gateway restart because the state is process-local. Consumers only
//! ever see a boolean via [`GatewayDebugState::is_enabled`].

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Upper bound on how far in the future a debug deadline may be set. The gateway
/// clamps any requested deadline to `now + MAX_DEBUG_WINDOW`, so debug logging
/// always self-expires regardless of what a caller asks for.
pub const MAX_DEBUG_WINDOW: Duration = Duration::from_secs(15 * 60);

/// Cheap, cloneable handle to a gateway's debug-logging deadline. Shared between
/// the control service (which sets it on `EnableDebug`) and the proxy paths
/// (which read it to gate verbose logging).
///
/// The state is a single expiry timestamp in unix milliseconds: `0` means
/// disabled; any timestamp in the future means enabled until that instant.
#[derive(Clone, Debug, Default)]
pub struct GatewayDebugState {
    enabled_until_unix_ms: Arc<AtomicU64>,
}

impl GatewayDebugState {
    /// Keep debug logging enabled until `until_unix_ms` (wall-clock, unix
    /// milliseconds), clamped to at most [`MAX_DEBUG_WINDOW`] from now. A zero or
    /// already-past deadline disables it. Returns the effective deadline that was
    /// stored (`0` when disabled).
    pub fn set_deadline(&self, until_unix_ms: u64) -> u64 {
        let now_ms = unix_now_ms();
        let effective = if until_unix_ms <= now_ms {
            0
        } else {
            until_unix_ms.min(now_ms.saturating_add(max_window_ms()))
        };
        self.enabled_until_unix_ms
            .store(effective, Ordering::Relaxed);
        effective
    }

    /// Whether debug logging is currently active (deadline set in the future).
    pub fn is_enabled(&self) -> bool {
        self.enabled_until_unix_ms.load(Ordering::Relaxed) > unix_now_ms()
    }
}

fn max_window_ms() -> u64 {
    u64::try_from(MAX_DEBUG_WINDOW.as_millis()).unwrap_or(u64::MAX)
}

fn unix_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests;
