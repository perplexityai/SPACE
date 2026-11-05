//! Service-to-service authentication metrics: per-decision counters and
//! TokenReview cache/limiter events. Emitted by the credmgr JWT identity
//! path (and any other service that adopts projected-token TokenReview auth).

use pplx_space_metrics_emit::{counter, tag};

/// One authentication decision on a service identity surface.
///
/// - `mode`: `"k8s"` (projected-token TokenReview) or `"static"` (the
///   shared-secret fallback during rollout).
/// - `surface`: the listener/identity surface (e.g. `"credmgr-tcp"`).
/// - `result`: `"allow"` or `"deny"`.
/// - `reason`: machine-readable cause (`"ok"`, `"service_account_not_allowed"`,
///   `"token_review_rejected"`, …).
pub fn decision(mode: &str, surface: &str, result: &str, reason: &str) {
    counter(
        "auth.decision",
        1,
        &[
            tag("mode", mode),
            tag("surface", surface),
            tag("result", result),
            tag("reason", reason),
        ],
    );
}

/// One authentication decision on the node-api identity surface. Same shape as
/// [`decision`] but emitted under a dedicated metric for node-api callers.
pub fn node_api_decision(mode: &str, surface: &str, result: &str, reason: &str) {
    counter(
        "auth.node_api.decision",
        1,
        &[
            tag("mode", mode),
            tag("surface", surface),
            tag("result", result),
            tag("reason", reason),
        ],
    );
}

/// One TokenReview cache or limiter event (`"hit_authenticated"`, `"miss"`,
/// `"rate_limited"`, `"limiter_wait"`, `"evict_capacity"`, …).
pub fn token_review_cache(result: &str) {
    counter("auth.token_review.cache", 1, &[tag("result", result)]);
}

/// One cache or refusal event of a build reviewer, tagged with its build
/// surface (`build-manager`, `node-api-build` or `template-publication`).
/// `result` is one of `hit_authenticated`, `miss`, `in_flight_wait`,
/// `insert_authenticated`, `evict_expired` and `evict_capacity` (as in
/// [`token_review_cache`]; a build reviewer caches no denials), `busy` (every
/// review permit in use), `rate_limited` (the second's review budget is spent)
/// and `in_flight_timeout` (the call's deadline passed while another call's
/// review of the token was in flight, or left too little time to review). Kept apart from `auth.token_review.cache`, which node-api's and
/// internal-api's own verifiers emit in the same processes.
pub fn build_token_review_cache(surface: &str, result: &str) {
    counter(
        "auth.build.token_review.cache",
        1,
        &[tag("surface", surface), tag("result", result)],
    );
}
