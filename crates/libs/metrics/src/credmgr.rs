//! Credential-manager metrics. The node credential agent (`--role agent`)
//! meters every RPC it forwards upstream, from the proxy so the coverage is
//! uniform — see `pplx_space_credmgr::node_service::NodeCredentialManager`. The
//! authority meters node intermediate CA issuance.

use crate::emit::{counter, tag, timing_ms};

/// Every Kubernetes caller pod authorization check, outside the TokenReview cache.
pub fn pod_authorization(check: &str, result: &str) {
    counter(
        "credmgr.pod_authorization",
        1,
        &[tag("check", check), tag("result", result)],
    );
}

/// Record one RPC the node credential agent forwarded upstream.
///
/// `method` is the CredentialManager RPC name; `result` is `"ok"`,
/// `"unreachable"` (a transport-level failure to reach the authority —
/// `Code::Unavailable`), or `"upstream_error"` (the authority was reached and
/// answered with a non-OK status); `code` is the gRPC status code name (`"ok"`
/// on success). The counter is tagged with all three so the reachability of
/// the agent→authority link (`result`) stays separable from the authority's
/// own error responses, with the exact code as the breakdown; the latency
/// timing is tagged by method + result.
pub fn agent_upstream_call(method: &str, result: &str, code: &str, latency_ms: f64) {
    counter(
        "credmgr.agent.upstream.requests",
        1,
        &[
            tag("method", method),
            tag("result", result),
            tag("code", code),
        ],
    );
    timing_ms(
        "credmgr.agent.upstream.latency_ms",
        latency_ms,
        &[tag("method", method), tag("result", result)],
    );
}

/// Record which stored grant served a federated org's intermediate-key identity
/// resolution (one per provider RPC — cache hits never resolve an identity).
///
/// `source` is `"own_sandbox"` (the requesting sandbox's own live grant),
/// `"org_fallback"` (a sandbox-triggered operation served by an org
/// sibling's grant — the deprecation-watch series),
/// `"org_wide"` (an org-scoped operation with no requesting sandbox), or
/// `"none"` (no live usable grant found; the request travels grantless).
pub fn federated_grant_identity_source(source: &str) {
    counter(
        "credmgr.federated_grant.identity_source",
        1,
        &[tag("source", source)],
    );
}

/// Record the node credential agent asking the authority for its intermediate
/// CA again after a failed acquisition, at the moment the wait after that
/// failure elapses and a gateway request triggers the next attempt.
///
/// `reason` is why the last attempt left the node without a CA:
/// `"authority_unavailable"` (an outage, retried on the fast schedule) or one
/// of the refusals re-probed on the slow one — `"authority_not_configured"`,
/// `"authority_refused"`, `"unusable_certificate"`. The values are the same
/// suffixes gateways see behind the `node_ca_unavailable:` status reason.
pub fn node_ca_reprobe(reason: &str) {
    counter("credmgr.node_ca.reprobes", 1, &[tag("reason", reason)]);
}

/// Record one node intermediate CA issuance attempt at the authority.
///
/// `result` is `"ok"` or the failure class. Latency covers the whole issuance
/// including asynchronous polling, so it is what the node's RPC deadline and
/// the authority's poll bound should be sized against.
pub fn intermediate_ca_issue(result: &str, latency_ms: f64) {
    counter(
        "credmgr.intermediate_ca.issues",
        1,
        &[tag("result", result)],
    );
    timing_ms(
        "credmgr.intermediate_ca.issue_latency_ms",
        latency_ms,
        &[tag("result", result)],
    );
}
