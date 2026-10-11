use std::collections::BTreeMap;

use super::*;

fn metadata_with_labels(labels: BTreeMap<String, String>) -> node_watch::NodeMetadata {
    node_watch::NodeMetadata {
        labels,
        ..Default::default()
    }
}

#[test]
fn label_is_current_only_when_it_names_this_host_cpu() {
    let cascade_lake = HostCpu::parse("genuineintel-6-85-7").unwrap();
    let cascade_lake = Some(&cascade_lake);

    assert!(!host_cpu_label_current(
        &metadata_with_labels(BTreeMap::new()),
        cascade_lake
    ));
    assert!(!host_cpu_label_current(
        &metadata_with_labels(BTreeMap::from([(
            HOST_CPU_LABEL.to_string(),
            "genuineintel-6-85-4".to_string(),
        )])),
        cascade_lake
    ));
    assert!(host_cpu_label_current(
        &metadata_with_labels(BTreeMap::from([(
            HOST_CPU_LABEL.to_string(),
            "genuineintel-6-85-7".to_string(),
        )])),
        cascade_lake
    ));
}

#[test]
fn apply_patch_sets_only_the_host_cpu_label() {
    let patch = host_cpu_apply(
        "space-worker-a",
        Some(&HostCpu::parse("genuineintel-6-85-4").unwrap()),
    );

    assert_eq!(
        patch,
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {
                "name": "space-worker-a",
                "labels": {
                    "space.pplx.net/host-cpu": "genuineintel-6-85-4",
                },
            }
        })
    );
}

/// An identity this host can no longer derive must not stay on the Node as
/// if it were still asserted: unknown is current only with no label, and the
/// apply for it sends no labels so server-side apply drops the owned key.
#[test]
fn unknown_identity_wants_the_label_removed() {
    let labeled = metadata_with_labels(BTreeMap::from([(
        HOST_CPU_LABEL.to_string(),
        "genuineintel-6-85-7".to_string(),
    )]));
    assert!(!host_cpu_label_current(&labeled, None));
    assert!(host_cpu_label_current(
        &metadata_with_labels(BTreeMap::new()),
        None
    ));
    assert_eq!(
        host_cpu_apply("space-worker-a", None)["metadata"]["labels"],
        serde_json::json!({})
    );
}

#[test]
fn host_cpu_patch_params_force_owns_the_label() {
    let params = host_cpu_patch_params();

    assert_eq!(
        params.field_manager.as_deref(),
        Some(HOST_CPU_FIELD_MANAGER)
    );
    assert!(params.force);
}
