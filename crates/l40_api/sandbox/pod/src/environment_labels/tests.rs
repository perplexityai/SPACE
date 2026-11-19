use super::*;

fn reference(digit: char) -> EnvironmentReference {
    format!("sha256:{}", digit.to_string().repeat(64))
        .parse()
        .unwrap()
}

#[test]
fn a_key_is_a_valid_label_key_under_the_prefix() {
    let key = environment_label_key(&reference('a'));
    assert_eq!(key, format!("space.pplx.net/env-sha256-{}", "a".repeat(52)));
    let (_, name) = key.split_once('/').unwrap();
    assert_eq!(name.len(), 63);
    assert!(name.chars().all(|c| c.is_ascii_alphanumeric() || c == '-'));
    assert!(name.ends_with(|c: char| c.is_ascii_alphanumeric()));
    assert!(is_environment_label_key(&key));
}

#[test]
fn distinct_references_get_distinct_keys() {
    assert_ne!(
        environment_label_key(&reference('a')),
        environment_label_key(&reference('b'))
    );
}

#[test]
fn other_space_labels_are_not_environment_labels() {
    for key in [
        "space.pplx.net/template-build-seed",
        "space.pplx.net/ssd-placement-blocked",
        "space.pplx.net/environment",
        "node-restriction.kubernetes.io/space-node-group",
    ] {
        assert!(!is_environment_label_key(key), "{key}");
    }
}

/// A node publishes an environment when its label is set to the value the
/// pod's affinity matches, not merely present.
#[test]
fn a_node_publishes_an_environment_only_through_a_usable_label() {
    let reference = reference('e');
    let key = environment_label_key(&reference);
    let labels = |value: &str| BTreeMap::from([(key.clone(), value.to_string())]);

    assert!(node_publishes_environment(
        &labels(ENVIRONMENT_USABLE_LABEL_VALUE),
        &reference
    ));
    assert!(node_publishes_any_environment(&labels(
        ENVIRONMENT_USABLE_LABEL_VALUE
    )));
    assert!(!node_publishes_environment(&labels("retired"), &reference));
    assert!(!node_publishes_any_environment(&labels("retired")));
    assert!(!node_publishes_any_environment(&BTreeMap::from([(
        "space.pplx.net/node-pool".to_string(),
        ENVIRONMENT_USABLE_LABEL_VALUE.to_string(),
    )])));
    assert_eq!(
        environment_requirement(&reference),
        NodeSelectorRequirement {
            key,
            operator: "In".to_string(),
            values: Some(vec![ENVIRONMENT_USABLE_LABEL_VALUE.to_string()]),
        }
    );
}
