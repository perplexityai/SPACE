use pplx_space_common_core::kubernetes::template_build_presence_label_key;

use super::*;

#[test]
fn template_presence_current_detects_matching_metadata() {
    let seed = template_build_presence_label_key("seed", "sha256:abc");
    let desired = BTreeMap::from([(seed.clone(), "sha256:abc".to_string())]);
    let current = node_watch::NodeMetadata {
        resource_version: "123".to_string(),
        labels: BTreeMap::from([(seed.clone(), TEMPLATE_PRESENT_LABEL_VALUE.to_string())]),
        annotations: BTreeMap::from([(seed, "sha256:abc".to_string())]),
        network_slots_capacity: None,
    };

    assert!(template_presence_current(&current, &desired));
}

#[test]
fn template_presence_current_detects_stale_owned_labels() {
    let seed = template_build_presence_label_key("seed", "sha256:abc");
    let stale = template_build_presence_label_key("stale", "sha256:def");
    let desired = BTreeMap::from([(seed.clone(), "sha256:abc".to_string())]);
    let current = node_watch::NodeMetadata {
        resource_version: "123".to_string(),
        labels: BTreeMap::from([
            (seed.clone(), TEMPLATE_PRESENT_LABEL_VALUE.to_string()),
            (stale.clone(), TEMPLATE_PRESENT_LABEL_VALUE.to_string()),
            (
                "node.kubernetes.io/instance-type".to_string(),
                "metal".to_string(),
            ),
        ]),
        annotations: BTreeMap::from([
            (seed, "sha256:abc".to_string()),
            (stale, "sha256:def".to_string()),
        ]),
        network_slots_capacity: None,
    };

    assert!(!template_presence_current(&current, &desired));
}

#[test]
fn template_presence_current_ignores_stale_third_party_annotations() {
    let seed = template_build_presence_label_key("seed", "sha256:abc");
    let stale = template_build_presence_label_key("stale", "sha256:def");
    let desired = BTreeMap::from([(seed.clone(), "sha256:abc".to_string())]);
    let current = node_watch::NodeMetadata {
        resource_version: "123".to_string(),
        labels: BTreeMap::from([(seed.clone(), TEMPLATE_PRESENT_LABEL_VALUE.to_string())]),
        annotations: BTreeMap::from([
            (seed, "sha256:abc".to_string()),
            (stale, "sha256:def".to_string()),
        ]),
        network_slots_capacity: None,
    };

    assert!(template_presence_current(&current, &desired));
}

#[test]
fn template_presence_current_detects_missing_desired_metadata() {
    let seed = template_build_presence_label_key("seed", "sha256:abc");
    let desired = BTreeMap::from([(seed, "sha256:abc".to_string())]);
    let current = node_watch::NodeMetadata::default();

    assert!(!template_presence_current(&current, &desired));
}

#[test]
fn template_presence_current_detects_mismatched_presence_annotation() {
    let seed = template_build_presence_label_key("seed", "sha256:abc");
    let desired = BTreeMap::from([(seed.clone(), "sha256:abc".to_string())]);
    let current = node_watch::NodeMetadata {
        resource_version: "123".to_string(),
        labels: BTreeMap::from([(seed.clone(), TEMPLATE_PRESENT_LABEL_VALUE.to_string())]),
        annotations: BTreeMap::from([(seed, "sha256:def".to_string())]),
        network_slots_capacity: None,
    };

    assert!(!template_presence_current(&current, &desired));
}

#[test]
fn template_presence_apply_uses_server_side_apply_shape() {
    let seed = template_build_presence_label_key("seed", "sha256:abc");
    let desired = BTreeMap::from([(seed.clone(), "sha256:abc".to_string())]);

    let apply = template_presence_apply("ip-10-0-0-1.ec2.internal", &desired);

    assert_eq!(apply["apiVersion"], "v1");
    assert_eq!(apply["kind"], "Node");
    assert_eq!(apply["metadata"]["name"], "ip-10-0-0-1.ec2.internal");
    assert_eq!(
        apply["metadata"]["labels"][&seed],
        TEMPLATE_PRESENT_LABEL_VALUE
    );
    assert_eq!(apply["metadata"]["annotations"][&seed], "sha256:abc");
}

#[test]
fn template_presence_apply_keeps_labels_and_annotations_in_sync() {
    let seed = template_build_presence_label_key("seed", "sha256:abc");
    let code_runtime = template_build_presence_label_key("code-runtime", "sha256:def");
    let desired = BTreeMap::from([
        (seed.clone(), "sha256:abc".to_string()),
        (code_runtime.clone(), "sha256:def".to_string()),
    ]);

    let apply = template_presence_apply("ip-10-0-0-1.ec2.internal", &desired);

    for (key, content_hash) in desired {
        assert_eq!(
            apply["metadata"]["labels"][&key],
            TEMPLATE_PRESENT_LABEL_VALUE
        );
        assert_eq!(apply["metadata"]["annotations"][&key], content_hash);
    }
}

#[test]
fn template_presence_detects_missing_presence_annotation_for_existing_label() {
    let seed = template_build_presence_label_key("seed", "sha256:abc");
    let desired = BTreeMap::from([(seed.clone(), "sha256:abc".to_string())]);
    let current = node_watch::NodeMetadata {
        resource_version: "123".to_string(),
        labels: BTreeMap::from([(seed, TEMPLATE_PRESENT_LABEL_VALUE.to_string())]),
        annotations: BTreeMap::new(),
        network_slots_capacity: None,
    };

    assert!(!template_presence_current(&current, &desired));
}

#[test]
fn template_presence_detects_missing_presence_label_for_existing_annotation() {
    let seed = template_build_presence_label_key("seed", "sha256:abc");
    let desired = BTreeMap::from([(seed.clone(), "sha256:abc".to_string())]);
    let current = node_watch::NodeMetadata {
        resource_version: "123".to_string(),
        labels: BTreeMap::new(),
        annotations: BTreeMap::from([(seed, "sha256:abc".to_string())]),
        network_slots_capacity: None,
    };

    assert!(!template_presence_current(&current, &desired));
}

#[test]
fn template_presence_patch_params_force_owns_presence_namespace() {
    let params = template_presence_patch_params();

    assert_eq!(
        params.field_manager.as_deref(),
        Some(TEMPLATE_PRESENCE_FIELD_MANAGER)
    );
    assert!(params.force);
}
