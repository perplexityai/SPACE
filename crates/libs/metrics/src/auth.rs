//! Authentication metrics recorded by node-api, internal-api and the API
//! server. The shared service-auth families live with their recorders in
//! `pplx-space-rpc-auth` (`metrics`, `template_lookup::metrics`) and
//! `pplx-space-uds-peer-auth` (`metrics`).

use crate::emit::{counter, gauge, tag};

/// Classification applied before node-api forwards SpacedWaitReady to the
/// local sandbox-manager.
pub fn node_api_wait_ready_policy(policy: &str, route_token: &str) {
    counter(
        "auth.node_api.wait_ready_policy",
        1,
        &[tag("policy", policy), tag("route_token", route_token)],
    );
}

/// Outcome of binding a reviewed pod token to registered template-catalog nodes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TemplateNodeBinding {
    Bound,
    /// Missing pod/node metadata uses registered-address matching.
    AddressFallback,
    /// Malformed or ambiguous supplied claims.
    Unbound,
    NoInventoryMatch,
}

impl TemplateNodeBinding {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Bound => "bound",
            Self::AddressFallback => "address_fallback",
            Self::Unbound => "unbound",
            Self::NoInventoryMatch => "no_inventory_match",
        }
    }
}

impl std::fmt::Display for TemplateNodeBinding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One Kubernetes template-catalog read, including reads using cached TokenReview.
/// `address_fallback` retains the legacy address scope; `unbound` and
/// `no_inventory_match` restrict the caller to generic templates.
pub fn template_node_binding(result: TemplateNodeBinding) {
    counter(
        "auth.template_node_binding",
        1,
        &[tag("result", result.as_str())],
    );
}

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
