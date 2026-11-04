use super::*;

fn scheduling_snapshot() -> SchedulingSnapshot {
    SchedulingSnapshot {
        status: NodeSchedulingStatus::Ready,
        reason: "ready".to_string(),
        ready: 1,
        min_ready: 1,
        degraded: 0,
        quarantined: 0,
        total: 1,
        leased: 0,
        max_slots: 1,
        resource_status: None,
    }
}

fn node_mgr_cache_disk(
    admission_state: node_mgr_proto::CacheDiskAdmissionState,
) -> node_mgr_proto::CacheDiskStatus {
    node_mgr_proto::CacheDiskStatus {
        admission_state: admission_state as i32,
        admission_reason: "disk_available_warning".to_string(),
        admission_detail: "disk available fraction below warning threshold".to_string(),
        ..Default::default()
    }
}

#[test]
fn warning_cache_disk_marks_node_full_for_scheduling() {
    let mut snapshot = scheduling_snapshot();
    let cache_disk = node_mgr_cache_disk(node_mgr_proto::CacheDiskAdmissionState::Warning);

    apply_cache_disk_scheduling_effect(&mut snapshot, &cache_disk);

    assert_eq!(snapshot.status, NodeSchedulingStatus::Full);
    assert_eq!(
        snapshot.reason,
        "disk_available_warning: disk available fraction below warning threshold"
    );
}

#[test]
fn cache_disk_status_response_carries_node_manager_resource_status() {
    let node_mgr_status = node_mgr_proto::CacheDiskStatus {
        statvfs_total_bytes: 1_000,
        statvfs_free_bytes: 120,
        statvfs_available_bytes: 90,
        statvfs_available_fraction: 0.09,
        effective_statvfs_available_bytes: 80,
        pending_reservation_bytes: 10,
        admission_state: node_mgr_proto::CacheDiskAdmissionState::Warning as i32,
        admission_reason: "disk_available_warning".to_string(),
        admission_detail: "disk available fraction below warning threshold".to_string(),
        btrfs_metadata_available_bytes: Some(75),
        btrfs_device_unallocated_bytes: Some(200),
    };

    let status = cache_disk_status_from_node_mgr(&node_mgr_status);

    assert_eq!(status.statvfs_total_bytes, 1_000);
    assert_eq!(status.statvfs_free_bytes, 120);
    assert_eq!(status.statvfs_available_bytes, 90);
    assert!((status.statvfs_available_fraction - 0.09).abs() < f64::EPSILON);
    assert_eq!(status.effective_statvfs_available_bytes, 80);
    assert_eq!(status.pending_reservation_bytes, 10);
    assert_eq!(
        status.admission_state,
        ProtoCacheDiskAdmissionState::Warning as i32
    );
    assert_eq!(status.admission_reason, "disk_available_warning");
    assert_eq!(
        status.admission_detail,
        "disk available fraction below warning threshold"
    );
    assert_eq!(status.btrfs_metadata_available_bytes, Some(75));
    assert_eq!(status.btrfs_device_unallocated_bytes, Some(200));
}
