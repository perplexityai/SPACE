use super::*;
use crate::{
    SPACE_API_CLIENT_CONCURRENCY_LIMITER_FLAG, SPACE_CLUSTER_API_CLIENT_SIDE_BALANCING_FLAG,
    SPACE_CLUSTER_RATE_LIMITER_FLAG, SPACE_NATIVE_CREATE_POOL_CLAIM_FLAG,
};

/// A declaration reports the key it was given and the fallback chosen for it,
/// and renders as the key so a log field or a metric tag reads the same as the
/// flag list does.
#[test]
fn a_declaration_carries_its_key_and_its_fallback() {
    const GATE: BoolFlag = BoolFlag::new("space-test-gate", true);
    const COUNT: IntegerFlag = IntegerFlag::new("space-test-count", 9);
    const RATE: FloatFlag = FloatFlag::new("space-test-rate", 0.25);
    const MODE: StringFlag = StringFlag::new("space-test-mode", "off");
    const SHAPE: JsonFlag = JsonFlag::new("space-test-shape");

    assert_eq!(
        (GATE.key(), GATE.when_unresolved()),
        ("space-test-gate", true)
    );
    assert_eq!(
        (COUNT.key(), COUNT.when_unresolved()),
        ("space-test-count", 9)
    );
    assert_eq!(RATE.key(), "space-test-rate");
    // Bit-compared: `clippy::float_cmp` is on repo-wide and the property is
    // that the declared value came back unchanged.
    assert_eq!(RATE.when_unresolved().to_bits(), 0.25_f64.to_bits());
    assert_eq!(
        (MODE.key(), MODE.when_unresolved()),
        ("space-test-mode", "off")
    );
    assert_eq!(SHAPE.key(), "space-test-shape");

    assert_eq!(GATE.to_string(), "space-test-gate");
    assert_eq!(MODE.to_string(), "space-test-mode");
    assert_eq!(SHAPE.to_string(), "space-test-shape");
}

/// The fallbacks the shipped boolean flags declare, pinned as values rather
/// than described in prose.
///
/// Each one is load-bearing in a direction the other flags do not share: the
/// two that fall back *on* are guardrails whose flag exists only to switch
/// them off, so an unreachable provider that flipped them to `false` would
/// silently remove a limiter that the deployment asked for.
/// The one that falls back *off* is a rollout, where the same unreachable
/// provider must not enable anything nobody turned on. Flipping any of these
/// is a deliberate product decision; this test is what makes it impossible to
/// do by accident while editing a nearby line.
#[test]
fn the_shipped_boolean_flags_declare_the_fallbacks_their_features_need() {
    for (flag, expected) in [
        (SPACE_CLUSTER_RATE_LIMITER_FLAG, true),
        (SPACE_API_CLIENT_CONCURRENCY_LIMITER_FLAG, true),
        (SPACE_CLUSTER_API_CLIENT_SIDE_BALANCING_FLAG, false),
        (SPACE_NATIVE_CREATE_POOL_CLAIM_FLAG, false),
    ] {
        assert_eq!(
            flag.when_unresolved(),
            expected,
            "{flag} declares the wrong fallback",
        );
    }
}
