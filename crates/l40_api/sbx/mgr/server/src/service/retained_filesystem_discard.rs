//! An operator's delete of a sandbox whose retained filesystem may be its only
//! copy.
//!
//! An interrupted suspend that leaves no usable capture of its own quarantines
//! its row with `suspend_publish_failed_no_artifact_runtime_gone` and keeps the
//! sandbox's instance, because that filesystem may hold writes no snapshot
//! has. Nothing recovers such a row automatically, and an ordinary delete
//! refuses it (`SandboxEraseSafety::RetainedFilesystemOnlyCopy`), so on a
//! Kubernetes node its pod's teardown is refused and the pod stays
//! Terminating. A discard delete is the operator's way out: an operator on the
//! node accepts losing that filesystem, says why, and the delete proceeds. It
//! applies to no other row. A cluster-deleted report's eviction refuses such
//! a row too, so it stays for the discard even after the control plane has
//! deleted the sandbox.

use std::fmt;
use std::time::Duration;

use super::suspend_snapshot_publication::SUSPEND_ATTEMPT_RECOVERY_WINDOW;

/// The `operation` a discard delete's `delete_requested` event carries, beside
/// the operator's reason in `reason`.
pub(crate) const DISCARD_OPERATION: &str = "discard_retained_filesystem";

/// Test hook (`pplx_space_test_hooks::test_hook_var`, compiled in only with
/// the `test-hooks` feature): the seconds a discard waits out from its suspend
/// attempt's start, in place of [`SUSPEND_ATTEMPT_RECOVERY_WINDOW`], so a Kind
/// smoke can reach a discard after a short wait. Only the discard reads it;
/// recovery keeps the full window, and release binaries can't shorten it.
pub(crate) const DISCARD_ATTEMPT_WINDOW_TEST_HOOK: &str = "SPACE_SBX_DISCARD_ATTEMPT_WINDOW_SECS";

/// The window a discard refuses within, from the suspend attempt's start.
pub(crate) fn discard_attempt_window() -> Duration {
    discard_attempt_window_from(
        pplx_space_test_hooks::test_hook_var(DISCARD_ATTEMPT_WINDOW_TEST_HOOK).as_deref(),
    )
}

fn discard_attempt_window_from(test_hook: Option<&str>) -> Duration {
    test_hook
        .and_then(|value| value.trim().parse::<u64>().ok())
        .map_or(SUSPEND_ATTEMPT_RECOVERY_WINDOW, Duration::from_secs)
}

/// Whether `attempt`, started at `started_at` (epoch seconds), is still inside
/// `window` at `now`. A clock that moved backward reads as inside it.
pub(crate) fn attempt_inside_window(started_at: i64, now: i64, window: Duration) -> bool {
    now.saturating_sub(started_at) < i64::try_from(window.as_secs()).unwrap_or(i64::MAX)
}

/// The longest operator reason a discard accepts, in characters.
pub(crate) const MAX_DISCARD_REASON_CHARS: usize = 512;

/// Why an operator discarded a retained filesystem: non-empty after trimming,
/// at most [`MAX_DISCARD_REASON_CHARS`] characters, and free of control,
/// line-separator, invisible-format and bidirectional characters, so it
/// reads as one line, in order and in full, in an event and a log line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DiscardReason(String);

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum DiscardReasonError {
    #[error("a discard needs a reason")]
    Empty,
    #[error("a discard reason is at most {MAX_DISCARD_REASON_CHARS} characters, got {0}")]
    TooLong(usize),
    #[error(
        "a discard reason can't contain control, line-separator, zero-width or bidirectional characters"
    )]
    ControlCharacter,
}

impl DiscardReason {
    pub(crate) fn parse(raw: &str) -> Result<Self, DiscardReasonError> {
        let reason = raw.trim();
        if reason.is_empty() {
            return Err(DiscardReasonError::Empty);
        }
        let chars = reason.chars().count();
        if chars > MAX_DISCARD_REASON_CHARS {
            return Err(DiscardReasonError::TooLong(chars));
        }
        if reason.chars().any(|c| {
            c.is_control()
                || matches!(
                    c,
                    '\u{00AD}'
                        | '\u{061C}'
                        | '\u{180E}'
                        | '\u{2028}'
                        | '\u{2029}'
                        | '\u{200E}'
                        | '\u{200F}'
                        | '\u{FEFF}'
                )
                || ('\u{200B}'..='\u{200D}').contains(&c)
                || ('\u{202A}'..='\u{202E}').contains(&c)
                // Word joiner, invisible operators, bidirectional isolates and
                // the deprecated format controls.
                || ('\u{2060}'..='\u{206F}').contains(&c)
                || ('\u{FFF9}'..='\u{FFFB}').contains(&c)
                // Tag characters, which can spell invisible text.
                || ('\u{E0000}'..='\u{E007F}').contains(&c)
        }) {
            return Err(DiscardReasonError::ControlCharacter);
        }
        Ok(Self(reason.to_owned()))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for DiscardReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests;
