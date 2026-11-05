//! The external console authentication metrics the API server records.

use pplx_space_metrics_emit::{counter, gauge, tag};

/// One external console authentication decision on a console gate.
///
/// `result`: `"accepted"`, `"missing_assertion"`, `"unauthenticated"`,
/// `"forbidden"`, `"unavailable"`, `"csrf_rejected"`, `"no_user"` or
/// `"lookup_failed"`. Only console requests (those carrying the console
/// request header) are counted.
pub fn external_console_decision(result: &str) {
    counter(
        "auth.external_console.decision",
        1,
        &[tag("result", result)],
    );
}

/// One password login or password-session request while an external console
/// authenticator is installed; password mode is not counted.
///
/// `result`: `"login_accepted"`, `"login_refused"` (a console request, or an
/// account not listed for password sessions), `"login_invalid"` (a listed
/// account with wrong credentials), `"session_accepted"` or
/// `"session_refused"` (a valid session whose account is no longer listed).
pub fn external_console_password_session(result: &str) {
    counter(
        "auth.external_console.password_session",
        1,
        &[tag("result", result)],
    );
}

/// Accounts listed for password sessions beside an installed external
/// console authenticator, reported every minute while one is installed.
/// Nonzero marks a deployment whose listed accounts sign in without the
/// external provider.
pub fn external_console_password_session_accounts(count: usize) {
    gauge(
        "auth.external_console.password_session_accounts",
        count as f64,
        &[],
    );
}
