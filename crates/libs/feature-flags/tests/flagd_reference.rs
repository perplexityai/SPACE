//! Guards on the worked flagd definitions in `reference/flagd-space-flags.json`.
//!
//! A reference configuration that has drifted from the code is worse than none:
//! an operator copies it, the flag it names no longer exists or is read as some
//! other type, and the deployment looks configured while the flag sits at its
//! compiled-in default. Nothing else checks this file — it is data, not code, so
//! neither the compiler nor clippy reads it.

use std::collections::{BTreeMap, BTreeSet};

use pplx_space_feature_flags::{
    SPACE_API_CLIENT_CONCURRENCY_LIMITER_FLAG, SPACE_API_CLIENT_CONCURRENCY_QUOTAS_FLAG,
    SPACE_CLUSTER_API_CLIENT_SIDE_BALANCING_FLAG, SPACE_CLUSTER_RATE_LIMITER_FLAG,
    SPACE_NATIVE_CREATE_POOL_CLAIM_FLAG, SPACE_NET_GATEWAY_KNOWN_EGRESS_DOMAINS_FLAG,
};

const REFERENCE: &str = include_str!("../reference/flagd-space-flags.json");

/// The one flag SPACE reads whose key is not a public constant of this crate:
/// the snapshot-retention sweeper keeps it private in the cluster-controller
/// server, which this crate cannot depend on without a cycle.
const RETENTION_CONFIG_FLAG: &str = "space-snapshot-retention-config";

/// Deserializing rather than poking at `serde_json::Value` is itself part of
/// the guard: a definition missing `state`, `defaultVariant`, or `variants`
/// fails to parse here instead of failing at evaluation in a cluster.
#[derive(serde::Deserialize)]
struct Reference {
    flags: BTreeMap<String, FlagDefinition>,
}

#[derive(serde::Deserialize)]
struct FlagDefinition {
    state: String,
    #[serde(rename = "defaultVariant")]
    default_variant: String,
    variants: BTreeMap<String, serde_json::Value>,
    #[serde(default)]
    targeting: serde_json::Value,
}

fn reference() -> Reference {
    serde_json::from_str(REFERENCE).expect("the reference configuration should parse")
}

/// The JSON type a flag's variants must carry, which is the type its call site
/// reads it as. Nothing links the two mechanically — the read lives in another
/// crate — so this table is the link, written by hand.
///
/// Note what that does and does not buy. The guards below compare this table
/// against the reference file, so a flag in one and not the other fails in
/// either direction. A flag in **neither** — declared in the crate root and
/// added to no table — passes, because Rust gives a test no way to enumerate
/// the crate's `Flag` constants and nothing here scans for them. Adding a
/// tenth production flag is therefore a manual step in two places, and
/// forgetting both leaves it silently unconfigurable for anyone deploying the
/// reference.
#[derive(Clone, Copy, Debug)]
enum Shape {
    Bool,
    Object,
}

/// Every flag SPACE reads, with the shape its reader expects.
const FLAGS_READ_BY_SPACE: [(&str, Shape); 7] = [
    (SPACE_CLUSTER_RATE_LIMITER_FLAG.key(), Shape::Bool),
    (SPACE_API_CLIENT_CONCURRENCY_LIMITER_FLAG.key(), Shape::Bool),
    (SPACE_NATIVE_CREATE_POOL_CLAIM_FLAG.key(), Shape::Bool),
    (
        SPACE_CLUSTER_API_CLIENT_SIDE_BALANCING_FLAG.key(),
        Shape::Bool,
    ),
    (
        SPACE_API_CLIENT_CONCURRENCY_QUOTAS_FLAG.key(),
        Shape::Object,
    ),
    (
        SPACE_NET_GATEWAY_KNOWN_EGRESS_DOMAINS_FLAG.key(),
        Shape::Object,
    ),
    (RETENTION_CONFIG_FLAG, Shape::Object),
];

fn flag_keys_read_by_space() -> BTreeSet<&'static str> {
    FLAGS_READ_BY_SPACE.iter().map(|(key, _)| *key).collect()
}

/// The reference defines exactly the flags SPACE reads — no more, no fewer.
///
/// Both directions matter and they catch opposite mistakes. A missing flag
/// means an operator who deploys this file finds one flag unconfigurable. An
/// extra one means a definition outlived the code that read it, which is not
/// hypothetical: `space-cluster-deleted-eviction` was deleted outright in
/// September 2026, and a reference still listing it would have an operator
/// maintaining a flag nothing evaluates.
#[test]
fn the_reference_defines_exactly_the_flags_space_reads() {
    let reference = reference();
    let defined: BTreeSet<&str> = reference.flags.keys().map(String::as_str).collect();
    let expected = flag_keys_read_by_space();

    let missing: Vec<&str> = expected.difference(&defined).copied().collect();
    let extra: Vec<&str> = defined.difference(&expected).copied().collect();

    assert!(
        missing.is_empty(),
        "flags SPACE reads but the reference does not define: {missing:?}"
    );
    assert!(
        extra.is_empty(),
        "flags the reference defines but SPACE never reads: {extra:?}"
    );
}

/// Every flag's `defaultVariant` names a variant it actually declares, and
/// every flag is enabled.
///
/// flagd resolves a flag by variant name, so a `defaultVariant` with no
/// matching variant fails at evaluation rather than at load — the flag reads as
/// an error for every subject, and the deployment only finds out from a
/// non-zero `outcome:error` rate long after the file was written. A flag left
/// `DISABLED` is quieter still: flagd reports it as not found, which is
/// indistinguishable from never having defined it.
#[test]
fn every_default_variant_exists_and_every_flag_is_enabled() {
    for (key, flag) in reference().flags {
        assert!(
            flag.variants.contains_key(&flag.default_variant),
            "{key} defaults to variant {:?}, which it does not declare: {:?}",
            flag.default_variant,
            flag.variants.keys().collect::<Vec<_>>()
        );
        assert_eq!(
            flag.state, "ENABLED",
            "{key} should be ENABLED; flagd reports a disabled flag as not found"
        );
    }
}

/// Each flag's variants carry the JSON type the code reads it as.
///
/// This is what catches the mismatch flagd punishes hardest: a read of the
/// wrong type is a `TypeMismatch` error and the caller silently takes its
/// default, so a boolean written as `"true"` looks like a working definition
/// and behaves like an absent one.
#[test]
fn every_variant_has_the_type_its_reader_expects() {
    let reference = reference();
    for (key, shape) in FLAGS_READ_BY_SPACE {
        let flag = reference
            .flags
            .get(key)
            .unwrap_or_else(|| panic!("{key} should be defined"));
        for (variant, value) in &flag.variants {
            let matches = match shape {
                Shape::Bool => value.is_boolean(),
                // Object, never a bare array: OpenFeature resolves JSON as a
                // structure, so a top-level array cannot be returned at all.
                Shape::Object => value.is_object(),
            };
            assert!(
                matches,
                "{key} variant {variant:?} is {value}, which is not {shape:?} as its reader expects"
            );
        }
    }
}

/// Every fractional rollout in the reference is parked at zero.
///
/// The README tells an operator this file can be served unchanged without
/// changing behavior, and that is the whole reason to hand someone a reference
/// configuration rather than a blank one. A fractional rule is the one
/// construct here that can quietly falsify it. The rollout shapes are present
/// so an operator can see how to ramp, and a shape is one edited digit away
/// from a live ramp on a deployment that copied the example expecting it to
/// be inert.
///
/// So: every weight on a variant other than `defaultVariant` must be zero.
#[test]
fn every_fractional_rollout_is_parked_at_zero() {
    for (key, flag) in reference().flags {
        let Some(buckets) = find_fractional(&flag.targeting) else {
            continue;
        };
        for bucket in buckets {
            let (variant, weight) = match bucket.as_array().map(Vec::as_slice) {
                Some([variant, weight]) => (
                    variant.as_str().unwrap_or_default(),
                    weight.as_u64().unwrap_or_default(),
                ),
                _ => {
                    panic!("{key} has a fractional bucket that is not [variant, weight]: {bucket}")
                }
            };
            if variant != flag.default_variant {
                assert_eq!(
                    weight, 0,
                    "{key} ramps {weight}% onto {variant:?}, which is not its \
                     default variant {:?}; the reference is meant to be servable unchanged",
                    flag.default_variant
                );
            }
        }
    }
}

/// Find a `fractional` rule anywhere in a targeting expression, including
/// nested inside an `if`, which is where a cluster-scoped ramp puts it.
fn find_fractional(targeting: &serde_json::Value) -> Option<&Vec<serde_json::Value>> {
    match targeting {
        serde_json::Value::Object(fields) => {
            if let Some(buckets) = fields
                .get("fractional")
                .and_then(serde_json::Value::as_array)
            {
                return Some(buckets);
            }
            fields.values().find_map(find_fractional)
        }
        serde_json::Value::Array(values) => values.iter().find_map(find_fractional),
        _ => None,
    }
}

/// The known-egress flag is defined in the object form its reader accepts for
/// OpenFeature backends, with the domain list under `domains`.
///
/// This is the one flag whose shape had to change to be expressible at all, so
/// it gets its own assertion rather than relying on "is an object" above: an
/// object with the list under some other key satisfies that check and still
/// reads as no domains at all.
#[test]
fn the_known_egress_flag_uses_the_domains_envelope() {
    let reference = reference();
    let flag = reference
        .flags
        .get(SPACE_NET_GATEWAY_KNOWN_EGRESS_DOMAINS_FLAG.key())
        .expect("the known-egress flag should be defined");

    for (variant, value) in &flag.variants {
        assert!(
            value
                .get("domains")
                .is_some_and(serde_json::Value::is_array),
            "known-egress variant {variant:?} should carry a `domains` array, got {value}"
        );
    }
}
