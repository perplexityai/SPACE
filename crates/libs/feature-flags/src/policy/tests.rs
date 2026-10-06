use super::*;
use crate::{
    SPACE_API_CLIENT_CONCURRENCY_LIMITER_FLAG, SPACE_CLUSTER_API_CLIENT_SIDE_BALANCING_FLAG,
    SPACE_CLUSTER_RATE_LIMITER_FLAG, SPACE_NATIVE_CREATE_POOL_CLAIM_FLAG,
};

const FALLS_BACK_ON: BoolFlag = BoolFlag::new("space-test-guardrail", true);
const FALLS_BACK_OFF: BoolFlag = BoolFlag::new("space-test-rollout", false);

/// The position each reason occupies in [`EVERY_REASON`].
///
/// One arm per variant, and deliberately not an or-pattern: a new
/// [`Unresolved`] variant then has no arm at all, this file stops compiling,
/// and the developer who has to add the arm is already looking at the table.
/// An or-pattern would let a new variant be absorbed by appending one token.
const fn position(unresolved: Unresolved) -> usize {
    match unresolved {
        Unresolved::NoProvider => 0,
        Unresolved::ProviderUnready => 1,
        Unresolved::NoAssignment => 2,
        Unresolved::EvaluationFailed => 3,
    }
}

/// Every reason a read can resolve nothing.
///
/// Hand-written. Nothing forces a new [`Unresolved`] variant into it directly —
/// the forcing function is the missing arm in [`position`], which is a compile
/// error, and [`every_reason_is_listed`] then checks the two agree on
/// membership and order.
const EVERY_REASON: [Unresolved; 4] = [
    Unresolved::NoProvider,
    Unresolved::ProviderUnready,
    Unresolved::NoAssignment,
    Unresolved::EvaluationFailed,
];

/// The table and [`position`] agree, so the loops below really do cover every
/// reason they claim to.
///
/// What this cannot catch, stated because it is the residual: a fifth variant
/// given an arm in `position` but never added here leaves the table short and
/// every loop below quietly narrower. The compile error in `position` is what
/// brings someone to this file; keeping the table in step is then a manual
/// step no test can check.
#[test]
fn every_reason_is_listed() {
    for (index, reason) in EVERY_REASON.into_iter().enumerate() {
        assert_eq!(position(reason), index, "{reason} is listed out of order");
    }
}

/// An operator who redeploys with the env master off does not then have to
/// reason about what the flag backend is saying: nothing it can answer —
/// including a `true` assignment — turns the feature back on.
#[test]
fn env_off_beats_every_provider_answer() {
    for flag in [FALLS_BACK_ON, FALLS_BACK_OFF] {
        assert!(
            !gate_enabled(false, flag, Ok(true)),
            "{flag} with a true assignment"
        );
        assert!(
            !gate_enabled(false, flag, Ok(false)),
            "{flag} with a false assignment"
        );
        for reason in EVERY_REASON {
            assert!(
                !gate_enabled(false, flag, Err(reason)),
                "{flag} on {reason}"
            );
        }
    }
}

/// With the env master on, the assignment decides — and the only direction it
/// can move a feature that the environment already enabled is off.
#[test]
fn env_on_lets_an_assignment_decide() {
    for flag in [FALLS_BACK_ON, FALLS_BACK_OFF] {
        assert!(gate_enabled(true, flag, Ok(true)), "{flag}");
        assert!(!gate_enabled(true, flag, Ok(false)), "{flag}");
    }
}

/// Nothing resolved means the flag's own declaration decides, whatever the
/// reason nothing resolved — including that there was no provider to ask.
///
/// Stated as an operator would: a guardrail the environment turned on stays on
/// however the flag backend fails, and a rollout nobody has been assigned to
/// stays off however it fails — on a cluster with no flag backend at all just
/// as on one whose backend is broken.
///
/// The `NoProvider` half is the load-bearing one, and is asserted rather than
/// assumed. A policy that answered `true` there — on the argument that with no
/// backend the environment is the only authority left — leaves every env-off,
/// assignment and override assertion in this module passing untouched, and
/// enables `FALLS_BACK_OFF` on every deployment that installs no provider,
/// which is the reverse of what declaring `false` asks for.
#[test]
fn nothing_resolved_defers_to_the_declaration_whatever_the_reason() {
    for reason in EVERY_REASON {
        assert!(
            gate_enabled(true, FALLS_BACK_ON, Err(reason)),
            "a guardrail must survive {reason}",
        );
        assert!(
            !gate_enabled(true, FALLS_BACK_OFF, Err(reason)),
            "a rollout must not be enabled by {reason}",
        );
    }
}

/// Each shipped boolean flag gets its own declaration's answer on a deployment
/// that installs no flag backend.
///
/// This replaces a list that had to be kept in step with the `gate_enabled`
/// call sites by hand and could only go stale. There is nothing to keep in
/// step now: the policy has no per-flag and no per-reason branch, so a flag
/// routed through a call site tomorrow gets its declared answer without being
/// named here.
#[test]
fn a_shipped_flag_gets_its_declared_answer_with_no_backend_installed() {
    for (flag, expected) in [
        (SPACE_CLUSTER_RATE_LIMITER_FLAG, true),
        (SPACE_API_CLIENT_CONCURRENCY_LIMITER_FLAG, true),
        (SPACE_CLUSTER_API_CLIENT_SIDE_BALANCING_FLAG, false),
        (SPACE_NATIVE_CREATE_POOL_CLAIM_FLAG, false),
    ] {
        assert_eq!(
            gate_enabled(true, flag, Err(Unresolved::NoProvider)),
            expected,
            "{flag} with the env master on and no provider installed",
        );
    }
}

/// An override is configuration, not a gate: with the env master off a
/// provider-supplied document cannot reach the caller.
///
/// The answer is `Disabled` rather than `Local`, and the distinction is the
/// point — a caller whose override *is* the feature must be able to tell "the
/// operator turned this off" from "the provider had nothing to say", because
/// only the second one leaves local configuration standing.
#[test]
fn env_off_suppresses_a_provider_override() {
    assert_eq!(
        override_allowed(false, Ok("from-the-provider")),
        Override::Disabled,
    );
    assert_eq!(
        override_allowed::<&str>(false, Err(Unresolved::NoAssignment)),
        Override::Disabled,
    );
}

/// With the env master on, an override applies when it resolved and leaves the
/// caller's local configuration in place for every reason it did not.
#[test]
fn env_on_passes_a_resolved_override_through_and_nothing_else() {
    assert_eq!(
        override_allowed(true, Ok("from-the-provider")),
        Override::Resolved("from-the-provider"),
    );
    for reason in EVERY_REASON {
        assert_eq!(
            override_allowed::<&str>(true, Err(reason)),
            Override::Local,
            "override survived {reason}",
        );
    }
}

/// The two states that mean "no document" are distinguishable, and every
/// reason a read resolves nothing lands on `Local` rather than `Disabled`.
///
/// Asserted as an inequality against a provider that answered identically in
/// both cases, so a helper that went back to collapsing them fails here rather
/// than in whichever caller happened to care.
#[test]
fn a_disabled_master_is_not_the_same_answer_as_an_unresolved_read() {
    for reason in EVERY_REASON {
        let disabled = override_allowed::<&str>(false, Err(reason));
        let unresolved = override_allowed::<&str>(true, Err(reason));
        assert_eq!(disabled, Override::Disabled);
        assert_eq!(unresolved, Override::Local);
        assert_ne!(
            disabled, unresolved,
            "collapsed the two answers for {reason}"
        );
    }
    // Including the case an earlier revision argued was safe to collapse: a
    // provider that resolved a document still yields `Disabled` when the
    // master is off, so the document is unreachable rather than merely unused.
    assert_eq!(
        override_allowed(false, Ok("from-the-provider")),
        Override::Disabled,
    );
}

/// `resolved()` is the opt-in collapse, and it maps the three states onto the
/// two its callers already gated down to.
///
/// Each variant is asserted separately: a collapse that returned `Some` for
/// `Disabled` would keep a provider document alive past a master an operator
/// turned off, which is the whole reason the collapse has to be asked for.
#[test]
fn resolved_collapses_disabled_and_local_but_never_invents_a_value() {
    assert_eq!(
        Override::Resolved("from-the-provider").resolved(),
        Some("from-the-provider"),
    );
    assert_eq!(Override::<&str>::Local.resolved(), None);
    assert_eq!(Override::<&str>::Disabled.resolved(), None);
}
