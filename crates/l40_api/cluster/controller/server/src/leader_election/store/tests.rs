use super::{LeaseError, LeaseFailure, takeable};
use std::time::Duration;

const TTL: Duration = Duration::from_secs(30);

#[test]
fn free_or_released_holder_is_takeable() {
    assert!(takeable(None, "me", Duration::ZERO, TTL));
    assert!(takeable(Some(""), "me", Duration::ZERO, TTL));
}

#[test]
fn our_own_lease_is_renewable_regardless_of_elapsed() {
    // Held by us — renewable even if the revision hasn't changed for a while.
    assert!(takeable(Some("me"), "me", Duration::from_secs(60), TTL));
}

#[test]
fn peers_lease_within_duration_is_denied() {
    // Held by a peer; the revision changed 10s ago (< 30s duration).
    assert!(!takeable(Some("peer"), "me", Duration::from_secs(10), TTL));
}

#[test]
fn peers_lease_past_duration_is_takeable() {
    // Held by a peer; unchanged for 31s (> 30s) — expired on our clock.
    assert!(takeable(Some("peer"), "me", Duration::from_secs(31), TTL));
    // Exactly at the duration boundary is takeable.
    assert!(takeable(Some("peer"), "me", TTL, TTL));
}

/// The apiserver's answer decides the category, and the mapping is asserted
/// against status codes rather than against the classifier's own arms — a table
/// keyed on `LeaseFailure` would restate the implementation, while a table keyed
/// on the codes the apiserver actually returns states the contract.
#[test]
fn apiserver_status_codes_map_to_their_failure_category() {
    // Overload: the control plane said it cannot serve this *now*. 5xx is a
    // range, so both ends of it are pinned.
    for code in [429, 500, 502, 503, 504, 599] {
        assert_eq!(
            api_error(code).classify(),
            LeaseFailure::Overloaded,
            "HTTP {code} is the apiserver shedding load"
        );
    }
    // Definitive refusal: the same request gets the same answer from a fresh
    // client. 401/403 are the RBAC shapes; the rest are request-shaped.
    for code in [400, 401, 403, 404, 405, 422, 410, 499] {
        assert_eq!(
            api_error(code).classify(),
            LeaseFailure::Refused,
            "HTTP {code} is a definitive refusal"
        );
    }
    // No usable status code. `0` is the one that matters and it is reachable in
    // production, not a synthetic edge: every `Status` field is `#[serde(default)]`
    // and `kube` uses a parsed error body verbatim instead of overriding it with
    // the HTTP status, so a 503 page from a proxy whose JSON body carries no
    // `code` arrives as `code: 0`. Calling that a refusal would suppress the
    // restart and log that no restart can help, which is strictly worse than the
    // behaviour before any classification existed.
    for code in [0, 100, 200, 301, 600] {
        assert_eq!(
            api_error(code).classify(),
            LeaseFailure::Unreachable,
            "HTTP {code} carries no actionable answer, so it must not be quieter \
             than a call that never got one"
        );
    }
}

/// No answer at all is the category the liveness budget exists for.
///
/// Of the three below only `Service` can actually arise on a lease call —
/// `KubeLeaseStore` does get/create/replace and never watches, so `ReadEvents`
/// and `LinesCodecMaxLineLengthExceeded` are watch-stream errors that cannot
/// reach it. They are here as *wildcard* probes: they are not in the explicit
/// refusal list, so they pin the direction of the catch-all, which is the
/// property `failures_decided_by_the_peers_bytes_are_unreachable` depends on.
#[test]
fn unanswered_calls_are_unreachable() {
    let unanswered = [
        LeaseError::Kube(kube::Error::Service("connection reset by peer".into())),
        LeaseError::Kube(kube::Error::ReadEvents(std::io::Error::other("eof"))),
        LeaseError::Kube(kube::Error::LinesCodecMaxLineLengthExceeded),
    ];
    for err in unanswered {
        assert_eq!(
            err.classify(),
            LeaseFailure::Unreachable,
            "{err} never reached the apiserver"
        );
    }
}

/// Failures this build's own configuration decides, before any bytes move. A
/// fresh client reproduces them identically, so none is worth a restart.
#[test]
fn configuration_decided_failures_are_refused() {
    let configuration = [
        LeaseError::Kube(kube::Error::TlsRequired),
        LeaseError::Kube(kube::Error::ProxyProtocolUnsupported {
            proxy_url: "socks4://example.invalid".parse().expect("static test URI"),
        }),
    ];
    for err in configuration {
        assert_eq!(
            err.classify(),
            LeaseFailure::Refused,
            "{err} is decided by our own config, so a fresh client repeats it"
        );
    }
}

/// The counterpart, and the distinction that matters: a failure decided by the
/// *peer's* bytes is `Unreachable`, not `Refused`. `handle_api_errors` reads the
/// body of every 4xx/5xx and returns these when it is not UTF-8 or not
/// parseable — the same code path that yields `Api(code: 0)`. Classifying them as
/// refusals would spare the budget on a garbled error page from something in
/// front of the apiserver, leaving the replica NotReady with nothing to restart
/// it, and would contradict the reasoning applied to `code: 0` one test up.
#[test]
fn failures_decided_by_the_peers_bytes_are_unreachable() {
    let from_peer = [
        LeaseError::Kube(kube::Error::SerdeError(
            serde_json::from_str::<serde_json::Value>("{not json").expect_err("must fail to parse"),
        )),
        LeaseError::Kube(kube::Error::FromUtf8(
            String::from_utf8(vec![0xff, 0xfe]).expect_err("must fail to decode"),
        )),
    ];
    for err in from_peer {
        let category = err.classify();
        assert_eq!(
            category,
            LeaseFailure::Unreachable,
            "{err} is decided by what the peer sent, so it must not be quieter \
             than a call that got no answer"
        );
        assert!(
            category.charges_liveness_budget(),
            "{err} must still spend the budget"
        );
    }
}

/// The catch-all errs toward `Unreachable`, which is the behaviour that existed
/// before any classification: charge the budget, allow the restart. An
/// unrecognised failure must never be *quieter* than that.
///
/// Stated as a property over every category rather than by probing one variant:
/// the claim is that whatever the default resolves to, it spends the budget. A
/// single-variant probe would keep passing if the default moved to another
/// non-charging category.
#[test]
fn no_category_that_the_catch_all_can_reach_is_quieter_than_charging() {
    let via_catch_all = LeaseError::Kube(kube::Error::ReadEvents(std::io::Error::other("eof")));
    let category = via_catch_all.classify();
    assert!(
        category.charges_liveness_budget(),
        "the catch-all resolved to {}, which does not spend the budget — an \
         unrecognised failure would then be quieter than it was before any \
         classification existed",
        category.as_str()
    );
    assert!(
        !category.needs_operator_action(),
        "the catch-all resolved to {}, which reports on sight — an unrecognised \
         transport blip would then log at error every time the category changed",
        category.as_str()
    );
}

#[test]
fn an_unusable_clock_is_its_own_category() {
    assert_eq!(LeaseError::Clock.classify(), LeaseFailure::Clock);
}

/// The two policy questions, pinned per category. Exhaustive by construction:
/// the array is destructured against every variant, so adding one without
/// deciding both answers fails to compile.
#[test]
fn every_category_states_both_policy_answers() {
    // (category, charges the liveness budget, needs a human)
    let policy = [
        (LeaseFailure::Unreachable, true, false),
        (LeaseFailure::Clock, true, true),
        (LeaseFailure::Overloaded, false, false),
        (LeaseFailure::Refused, false, true),
    ];
    for (category, charges, operator) in policy {
        assert_eq!(
            category.charges_liveness_budget(),
            charges,
            "{} charges_liveness_budget",
            category.as_str()
        );
        assert_eq!(
            category.needs_operator_action(),
            operator,
            "{} needs_operator_action",
            category.as_str()
        );
    }
    // The exhaustiveness half: a new variant has to be added here, which forces
    // a decision about both predicates above.
    fn assert_covered(category: LeaseFailure) -> &'static str {
        match category {
            LeaseFailure::Unreachable => "unreachable",
            LeaseFailure::Clock => "clock",
            LeaseFailure::Overloaded => "overloaded",
            LeaseFailure::Refused => "refused",
        }
    }
    for (category, _, _) in policy {
        assert_eq!(assert_covered(category), category.as_str());
    }
}

/// A lost optimistic-concurrency race is not a failure — the store maps it to
/// `Ok(false)`, the same as a peer holding a valid lease. Asserted here because
/// the classifier deliberately has no 409 arm, and a reader has to be able to
/// tell that from an oversight.
#[test]
fn a_conflict_is_not_classified_because_it_is_not_an_error() {
    assert!(super::is_conflict(&kube::Error::Api(Box::new(
        kube::core::Status {
            code: 409,
            ..Default::default()
        }
    ))));
}

fn api_error(code: u16) -> LeaseError {
    LeaseError::Kube(kube::Error::Api(Box::new(kube::core::Status {
        code,
        ..Default::default()
    })))
}
