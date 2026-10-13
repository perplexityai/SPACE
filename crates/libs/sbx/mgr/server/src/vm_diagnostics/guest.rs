//! spaced's WARN and ERROR callsites, drained by the health probe's Ping.
//!
//! The guest sends callsite identity only (level, target, file, line, count),
//! never message text. Its answer is still tenant-controlled: the probe's
//! client bounds the bytes it decodes
//! ([`pplx_space_spaced_common::BRIDGE_GUEST_PING_MAX_DECODE_BYTES`]), and this
//! module bounds the work per drain, so strings are truncated and stripped of
//! control characters, counts clamped
//! and only a fixed number of entries read; nothing reaches a metric tag.

use pplx_space_metrics::sandbox::vm::{VmDiagnosticLevel, VmDiagnosticOutcome, VmDiagnosticSource};
use pplx_space_spaced_common::proto::bridge_guest::{GuestDiagnosticCallsite, PingResponse};

use super::sanitized;

/// Callsites re-logged per drain. The probe drains a sandbox about every 24
/// seconds, so this bounds each sandbox to a few lines a minute.
const LOGGED_PER_DRAIN: usize = 4;
/// Entries read per drain: the guest's own bound, which a well-behaved guest
/// never exceeds. Entries past it are counted as dropped, not visited.
const READ_PER_DRAIN: usize = pplx_space_observability::warning_tap::MAX_CALLSITES;
/// The most events one guest-reported count is taken to stand for.
const MAX_COUNT_PER_ENTRY: u64 = 1_000_000;
const MAX_FIELD_BYTES: usize = 128;

/// Sandboxes restored from a snapshot whose next drain is the snapshot's.
///
/// A restored guest's memory holds the record of the sandbox the snapshot was
/// taken from. The guest discards it on a snapshot-restore resume notice, but
/// that notice is best effort and is not sent at all for a restore that ends
/// paused, so the host also drops the first drain after every restore.
#[derive(Clone, Default)]
pub(crate) struct RestoredGuests(
    std::sync::Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
);

impl RestoredGuests {
    pub(crate) fn mark(&self, sandbox_id: &str) {
        self.lock().insert(sandbox_id.to_string());
    }

    pub(crate) fn forget(&self, sandbox_id: &str) {
        self.lock().remove(sandbox_id);
    }

    /// Whether this drain is the first since a restore, clearing the mark.
    fn take(&self, sandbox_id: &str) -> bool {
        self.lock().remove(sandbox_id)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, std::collections::HashSet<String>> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

pub(crate) fn forward_guest_diagnostics(
    sandbox_id: &str,
    response: &PingResponse,
    restored: &RestoredGuests,
) {
    let Some(drain) = admitted_drain(sandbox_id, response, restored) else {
        tracing::debug!(
            sandbox_id,
            "dropping the first guest diagnostics drain after a snapshot restore"
        );
        return;
    };
    for callsite in &drain.logged {
        // Logged at WARN whatever the guest's level: the guest is the tenant's
        // VM, and its errors are evidence for an investigation, not a page.
        tracing::warn!(
            sandbox_id,
            guest_level = callsite.level.as_str(),
            guest_target = callsite.target.as_str(),
            guest_file = callsite.file.as_str(),
            guest_line = callsite.line,
            guest_count = callsite.count,
            "spaced guest diagnostic"
        );
        emit(callsite.level, VmDiagnosticOutcome::Logged, callsite.count);
    }
    emit(
        VmDiagnosticLevel::Warn,
        VmDiagnosticOutcome::RateLimited,
        drain.rate_limited_warn,
    );
    emit(
        VmDiagnosticLevel::Error,
        VmDiagnosticOutcome::RateLimited,
        drain.rate_limited_error,
    );
    emit(
        VmDiagnosticLevel::Warn,
        VmDiagnosticOutcome::DroppedInGuest,
        drain.dropped,
    );
}

fn emit(level: VmDiagnosticLevel, outcome: VmDiagnosticOutcome, count: u64) {
    if count > 0 {
        pplx_space_metrics::sandbox::vm::vm_diagnostics(
            VmDiagnosticSource::Guest,
            level,
            outcome,
            count,
        );
    }
}

/// What one drain amounts to once bounded.
#[derive(Debug, Default, PartialEq, Eq)]
struct GuestDrain {
    logged: Vec<LoggedCallsite>,
    rate_limited_warn: u64,
    rate_limited_error: u64,
    /// Events the guest dropped past its bound, plus every entry past
    /// [`READ_PER_DRAIN`], counted one each.
    dropped: u64,
}

#[derive(Debug, PartialEq, Eq)]
struct LoggedCallsite {
    level: VmDiagnosticLevel,
    target: String,
    file: String,
    line: u32,
    count: u64,
}

/// What a drain forwards: nothing for the first drain after a restore, else
/// its bounded summary.
fn admitted_drain(
    sandbox_id: &str,
    response: &PingResponse,
    restored: &RestoredGuests,
) -> Option<GuestDrain> {
    (!restored.take(sandbox_id)).then(|| summarize(response))
}

fn summarize(response: &PingResponse) -> GuestDrain {
    let mut drain = GuestDrain::default();
    for (index, callsite) in response.diagnostics.iter().take(READ_PER_DRAIN).enumerate() {
        let level = level(callsite);
        let count = callsite.count.min(MAX_COUNT_PER_ENTRY);
        if index < LOGGED_PER_DRAIN {
            drain.logged.push(LoggedCallsite {
                level,
                target: sanitized(&callsite.target, MAX_FIELD_BYTES),
                file: sanitized(&callsite.file, MAX_FIELD_BYTES),
                line: callsite.line,
                count,
            });
        } else if level == VmDiagnosticLevel::Error {
            drain.rate_limited_error = drain.rate_limited_error.saturating_add(count);
        } else {
            drain.rate_limited_warn = drain.rate_limited_warn.saturating_add(count);
        }
    }
    let unread = response.diagnostics.len().saturating_sub(READ_PER_DRAIN);
    drain.dropped = response
        .diagnostics_dropped
        .min(MAX_COUNT_PER_ENTRY)
        .saturating_add(u64::try_from(unread).unwrap_or(u64::MAX));
    drain
}

fn level(callsite: &GuestDiagnosticCallsite) -> VmDiagnosticLevel {
    if callsite.level == "error" {
        VmDiagnosticLevel::Error
    } else {
        VmDiagnosticLevel::Warn
    }
}

#[cfg(test)]
#[path = "guest_tests.rs"]
mod tests;
