use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::Node;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

use super::*;
use crate::kubernetes::{
    DEFAULT_SANDBOX_INTERNAL_CONTROL_TOKEN_KEY, DEFAULT_SANDBOX_INTERNAL_SECRET_NAME,
    DEFAULT_SANDBOX_INTERNAL_TOKEN_KEY, DEFAULT_SANDBOX_NODE_GROUP_PLACEMENT,
    DEFAULT_SANDBOX_NODE_SERVICES_READY_AFFINITY, DEFAULT_SANDBOX_POD_IMAGE,
    DEFAULT_SANDBOX_RESTORE_HOST_CPU_PLACEMENT_ENABLED,
    DEFAULT_SANDBOX_RESTORE_SOURCE_NODE_HARD_PIN, DEFAULT_SANDBOX_RUNTIME_CLASS,
    DEFAULT_SANDBOX_SSD_HEADROOM_TIER_AFFINITY,
};

const DISCRIMINATOR_KEY: &str = "space.pplx.net/node-pool";

fn extras() -> BTreeMap<String, String> {
    BTreeMap::from([
        (
            "node.kubernetes.io/space-enabled".to_string(),
            "true".to_string(),
        ),
        ("workload".to_string(), "workers".to_string()),
    ])
}

fn config_values(extra: BTreeMap<String, String>) -> SandboxPodConfigValues {
    SandboxPodConfigValues {
        sandbox_image: DEFAULT_SANDBOX_POD_IMAGE.to_string(),
        runtime_class_name: DEFAULT_SANDBOX_RUNTIME_CLASS.to_string(),
        sandbox_node_selector_key: DISCRIMINATOR_KEY.to_string(),
        sandbox_node_selector_value: "workers".to_string(),
        extra_node_selector_labels: extra,
        internal_secret_name: DEFAULT_SANDBOX_INTERNAL_SECRET_NAME.to_string(),
        internal_token_key: DEFAULT_SANDBOX_INTERNAL_TOKEN_KEY.to_string(),
        internal_control_token_key: DEFAULT_SANDBOX_INTERNAL_CONTROL_TOKEN_KEY.to_string(),
        ssd_headroom_tier_affinity: DEFAULT_SANDBOX_SSD_HEADROOM_TIER_AFFINITY,
        restore_source_node_hard_pin: DEFAULT_SANDBOX_RESTORE_SOURCE_NODE_HARD_PIN,
        node_services_ready_affinity: DEFAULT_SANDBOX_NODE_SERVICES_READY_AFFINITY,
        node_group_placement: DEFAULT_SANDBOX_NODE_GROUP_PLACEMENT,
        restore_host_cpu_placement_enabled: DEFAULT_SANDBOX_RESTORE_HOST_CPU_PLACEMENT_ENABLED,
        host_cpu_compatibility: crate::types::HostCpuCompatibility::default(),
    }
}

fn node_labeled(labels: BTreeMap<String, String>) -> Node {
    Node {
        metadata: ObjectMeta {
            name: Some("node-a".to_string()),
            labels: Some(labels),
            ..Default::default()
        },
        ..Default::default()
    }
}

/// A literal expected map rather than a second call to the composer: the
/// composer is what's under test, so comparing it against itself would pass
/// whatever it produced.
#[test]
fn base_selector_is_the_discriminator_plus_every_extra_label() {
    let selector = compose_base_node_selector(DISCRIMINATOR_KEY, "workers", &extras());

    assert_eq!(
        selector.into_labels(),
        BTreeMap::from([
            (DISCRIMINATOR_KEY.to_string(), "workers".to_string()),
            (
                "node.kubernetes.io/space-enabled".to_string(),
                "true".to_string()
            ),
            ("workload".to_string(), "workers".to_string()),
        ])
    );
}

/// An extra label configured on the discriminator's own key wins.
///
/// Worth pinning because it is the precedence that makes the placement set and
/// the registration/inventory set disjoint rather than nested — those listers
/// use the configured discriminator value, so they would look for `workers`
/// while every sandbox pod asks for `builders`. Nothing rejects the
/// configuration, so the behavior needs to be the documented one.
#[test]
fn an_extra_label_on_the_discriminator_key_overrides_the_configured_value() {
    let extra = BTreeMap::from([(DISCRIMINATOR_KEY.to_string(), "builders".to_string())]);

    let selector = compose_base_node_selector(DISCRIMINATOR_KEY, "workers", &extra);

    assert_eq!(
        selector.into_labels(),
        BTreeMap::from([(DISCRIMINATOR_KEY.to_string(), "builders".to_string())])
    );
}

/// The two entry points forward positional `key`, `value`, `extra` arguments
/// into one composition, so a field mixed up in either forwarder is the whole
/// remaining divergence surface — and it type-checks, because key and value
/// are both `&str`. `build_sandbox_pod` takes the borrowed shape while the
/// substrate's node filters hold the owned one, so a mismatch between them
/// reintroduces exactly the divergence this module exists to prevent.
#[test]
fn owned_and_borrowed_config_shapes_compose_the_same_base_selector() {
    let values = config_values(extras());

    assert_eq!(
        values.base_node_selector(),
        values.for_namespace("space").base_node_selector()
    );
    assert_eq!(
        values.base_node_selector(),
        compose_base_node_selector(DISCRIMINATOR_KEY, "workers", &extras()),
    );
}

#[test]
fn matches_requires_every_entry_to_match_exactly() {
    let selector = config_values(extras()).base_node_selector();
    let labels = selector.clone().into_labels();

    assert!(selector.matches(&node_labeled(labels.clone())));

    // Extra node labels the selector does not mention are irrelevant.
    let mut superset = labels.clone();
    superset.insert("unrelated".to_string(), "value".to_string());
    assert!(selector.matches(&node_labeled(superset)));

    // One missing entry is enough to make the node unschedulable.
    let mut missing = labels.clone();
    missing.remove("workload");
    assert!(!selector.matches(&node_labeled(missing)));

    // A present key with the wrong value is not a match either.
    let mut wrong_value = labels.clone();
    wrong_value.insert("workload".to_string(), "builders".to_string());
    assert!(!selector.matches(&node_labeled(wrong_value)));

    // A Node with no labels at all (`metadata.labels` absent, not empty)...
    assert!(!selector.matches(&Node::default()));
    // ...and one with an empty map, the other way to carry none.
    assert!(!selector.matches(&node_labeled(BTreeMap::new())));
}

/// A selector always carries the discriminator key, whatever the extras.
///
/// Its *value* can be overridden, which
/// `an_extra_label_on_the_discriminator_key_overrides_the_configured_value`
/// pins; the entry itself is always there, and that is what makes an empty
/// selector unreachable through the constructors. That guarantee is the real
/// protection; this asserts the weakest input that could otherwise have
/// defeated it.
#[test]
fn a_selector_is_never_empty_even_with_no_extra_labels() {
    let selector = config_values(BTreeMap::new()).base_node_selector();

    assert_eq!(
        selector.into_labels(),
        BTreeMap::from([(DISCRIMINATOR_KEY.to_string(), "workers".to_string())])
    );
}

/// The emptiness check in `matches` — insurance for an empty selector built
/// from inside this module, and only for the filter exit. `into_labels` has no
/// equivalent and would render `nodeSelector: {}`, so this is not a substitute
/// for the construction guarantee above.
///
/// Without the check `all()` over no entries is vacuously true, so *every* node
/// matches: a label-less one, an empty-labeled one and a fully labeled one
/// alike. No single case is load-bearing; the first evaluated is simply the one
/// that fails.
///
/// Asserted through the public `matches`, where production enters, with no
/// `cfg` on it so it runs in the debug profile CI builds. That is why the check
/// is a plain `if` and not a `debug_assert!`: an assert is compiled out of
/// release — where an empty selector would then match every node, so there is
/// no true property left to assert — and panics before the predicate in debug,
/// where the return value therefore cannot be asserted at all. It would be
/// assertable in neither profile.
#[test]
fn an_empty_selector_matches_no_node() {
    let empty = SandboxNodeSelector(BTreeMap::new());

    for node in [
        Node::default(),
        node_labeled(BTreeMap::new()),
        node_labeled(config_values(extras()).base_node_selector().into_labels()),
    ] {
        assert!(!empty.matches(&node));
    }
}
