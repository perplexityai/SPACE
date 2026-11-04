use std::collections::BTreeMap;

use super::*;

fn cache_disk(
    free_bytes: u64,
    total_bytes: u64,
    effect: CacheDiskSchedulingEffect,
) -> CacheDiskStatus {
    let admission_state = match effect {
        CacheDiskSchedulingEffect::Open => {
            pplx_space_node_mgr_client::proto::CacheDiskAdmissionState::Ready
        }
        CacheDiskSchedulingEffect::WarnOnly => {
            pplx_space_node_mgr_client::proto::CacheDiskAdmissionState::Warning
        }
        CacheDiskSchedulingEffect::Full => {
            pplx_space_node_mgr_client::proto::CacheDiskAdmissionState::Full
        }
        CacheDiskSchedulingEffect::Unavailable => {
            pplx_space_node_mgr_client::proto::CacheDiskAdmissionState::Unavailable
        }
    };
    CacheDiskStatus {
        statvfs_total_bytes: total_bytes,
        statvfs_free_bytes: free_bytes,
        statvfs_available_bytes: free_bytes,
        statvfs_available_fraction: if total_bytes == 0 {
            0.0
        } else {
            free_bytes as f64 / total_bytes as f64
        },
        admission_state: admission_state as i32,
        admission_reason: admission_state.as_str_name().to_ascii_lowercase(),
        ..Default::default()
    }
}

fn metadata_with_labels(labels: BTreeMap<String, String>) -> node_watch::NodeMetadata {
    node_watch::NodeMetadata {
        resource_version: "123".to_string(),
        labels,
        annotations: BTreeMap::new(),
        network_slots_capacity: None,
    }
}

#[test]
fn missing_resource_status_advertises_last_resort_tier() {
    // Missing or degenerate cache status must advertise the
    // last-resort tier, not retain a stale high tier that would keep
    // steering placements toward a node whose disk we can't see.
    assert_eq!(desired_tier(None), SsdHeadroomTier::ZERO);
    assert_eq!(
        desired_tier(Some(&cache_disk(0, 0, CacheDiskSchedulingEffect::Open))),
        SsdHeadroomTier::ZERO
    );
    // A healthy filesystem still maps to the top tier.
    assert_eq!(
        desired_tier(Some(&cache_disk(
            900,
            1_000,
            CacheDiskSchedulingEffect::Open
        ))),
        SsdHeadroomTier::MAX
    );
}

#[test]
fn placement_block_follows_node_manager_admission_effect() {
    assert!(!desired_block(Some(&cache_disk(
        100,
        1_000,
        CacheDiskSchedulingEffect::Open
    ))));
    assert!(desired_block(Some(&cache_disk(
        100,
        1_000,
        CacheDiskSchedulingEffect::WarnOnly
    ))));
    assert!(desired_block(Some(&cache_disk(
        50,
        1_000,
        CacheDiskSchedulingEffect::Full
    ))));
    assert!(desired_block(Some(&cache_disk(
        0,
        1_000,
        CacheDiskSchedulingEffect::Unavailable
    ))));
    assert!(desired_block(None));
}

#[test]
fn labels_current_detects_matching_labels() {
    let current = metadata_with_labels(BTreeMap::from([
        (
            SSD_HEADROOM_TIER_LABEL.to_string(),
            SsdHeadroomTier::MAX.to_string(),
        ),
        (SSD_PLACEMENT_BLOCKED_LABEL.to_string(), "false".to_string()),
    ]));

    assert!(placement_labels_current(
        &current,
        SsdHeadroomTier::MAX,
        false
    ));
}

#[test]
fn labels_current_detects_missing_labels() {
    let current = node_watch::NodeMetadata::default();

    assert!(!placement_labels_current(
        &current,
        SsdHeadroomTier::MAX,
        false
    ));
}

#[test]
fn labels_current_detects_changed_block_label() {
    // Tier matches but the block label is stale: not current, so node-ctl
    // must re-apply. Guards the both-labels equality.
    let current = metadata_with_labels(BTreeMap::from([
        (SSD_HEADROOM_TIER_LABEL.to_string(), "0".to_string()),
        (SSD_PLACEMENT_BLOCKED_LABEL.to_string(), "false".to_string()),
    ]));

    assert!(!placement_labels_current(
        &current,
        SsdHeadroomTier::ZERO,
        true
    ));
}

#[test]
fn placement_apply_writes_both_labels() {
    let apply = ssd_placement_apply("ip-10-0-0-1.ec2.internal", SsdHeadroomTier::ZERO, true);

    assert_eq!(apply["apiVersion"], "v1");
    assert_eq!(apply["kind"], "Node");
    assert_eq!(apply["metadata"]["name"], "ip-10-0-0-1.ec2.internal");
    assert_eq!(apply["metadata"]["labels"][SSD_HEADROOM_TIER_LABEL], "0");
    assert_eq!(
        apply["metadata"]["labels"][SSD_PLACEMENT_BLOCKED_LABEL],
        "true"
    );
    // Below the block threshold the label is explicitly "false".
    let unblocked = ssd_placement_apply("n", SsdHeadroomTier::MAX, false);
    assert_eq!(
        unblocked["metadata"]["labels"][SSD_PLACEMENT_BLOCKED_LABEL],
        "false"
    );
}

#[test]
fn tier_patch_params_force_owns_label_namespace() {
    let params = ssd_headroom_tier_patch_params();

    assert_eq!(
        params.field_manager.as_deref(),
        Some(SSD_HEADROOM_TIER_FIELD_MANAGER)
    );
    assert!(params.force);
}
