use std::str::FromStr;

use super::*;

fn sandbox_id(value: &str) -> SandboxId {
    SandboxId::from_str(value).unwrap()
}

fn sources(total_mib: u64, available_mib: u64) -> (tempfile::TempDir, MemoryAdmissionSources) {
    let root = tempfile::tempdir().unwrap();
    let meminfo_path = root.path().join("meminfo");
    std::fs::write(
        &meminfo_path,
        format!(
            "MemTotal:       {} kB\nMemAvailable:   {} kB\n",
            total_mib * KIB_PER_MIB,
            available_mib * KIB_PER_MIB
        ),
    )
    .unwrap();
    let cgroup_base = root.path().join("cgroups");
    std::fs::create_dir(&cgroup_base).unwrap();
    let sources = MemoryAdmissionSources::new(meminfo_path, cgroup_base);
    (root, sources)
}

fn write_cgroup(
    sources: &MemoryAdmissionSources,
    sandbox_id: SandboxId,
    current_mib: u64,
    maximum_mib: u64,
) {
    let cgroup_path = sources.firecracker_cgroup_base.join(sandbox_id.to_string());
    std::fs::create_dir(&cgroup_path).unwrap();
    std::fs::write(
        cgroup_path.join("memory.current"),
        (current_mib * BYTES_PER_MIB).to_string(),
    )
    .unwrap();
    std::fs::write(
        cgroup_path.join("memory.max"),
        (maximum_mib * BYTES_PER_MIB).to_string(),
    )
    .unwrap();
}

fn lease(sandbox_id: SandboxId) -> ActiveLeaseMemoryReservation {
    lease_generation(sandbox_id, 1)
}

fn lease_generation(sandbox_id: SandboxId, generation: u64) -> ActiveLeaseMemoryReservation {
    ActiveLeaseMemoryReservation {
        slot_id: 0,
        sandbox_id,
        generation,
        memory_reservation_mib: 8320,
    }
}

#[tokio::test]
async fn low_usage_running_vm_receives_only_configured_growth_reserve() {
    let (_root, sources) = sources(20_000, 20_000);
    let sandbox_id = sandbox_id("019d84bd-280d-7331-99c6-b63377f3a3a2");
    write_cgroup(&sources, sandbox_id, 256, 8320);

    let measurement = MemoryAdmissionConfig { reserve_mib: 1024 }
        .evaluate_new_allocation(8320, &[lease(sandbox_id)], &sources)
        .await
        .unwrap();

    assert_eq!(measurement.existing_growth_reserve_mib, 1024);
}

#[tokio::test]
async fn running_vm_growth_reserve_is_capped_at_remaining_maximum() {
    let (_root, sources) = sources(20_000, 20_000);
    let sandbox_id = sandbox_id("019d84bd-280d-7331-99c6-b63377f3a3a2");
    write_cgroup(&sources, sandbox_id, 8000, 8320);

    let measurement = MemoryAdmissionConfig { reserve_mib: 1024 }
        .evaluate_new_allocation(8320, &[lease(sandbox_id)], &sources)
        .await
        .unwrap();

    assert_eq!(measurement.existing_growth_reserve_mib, 320);
}

#[tokio::test]
async fn invisible_active_lease_is_charged_its_persisted_cap() {
    let (_root, sources) = sources(20_000, 20_000);
    let sandbox_id = sandbox_id("019d84bd-280d-7331-99c6-b63377f3a3a2");

    let measurement = MemoryAdmissionConfig { reserve_mib: 1024 }
        .evaluate_new_allocation(8320, &[lease(sandbox_id)], &sources)
        .await
        .unwrap();

    assert_eq!(measurement.existing_growth_reserve_mib, 8320);
}

#[tokio::test]
async fn lease_committed_after_observation_is_charged_its_persisted_cap() {
    let (_root, sources) = sources(17_663, 17_663);
    let admission = MemoryAdmissionConfig { reserve_mib: 1024 };
    let observation = admission
        .observe_active_leases(&[], &sources)
        .await
        .unwrap();
    let newly_committed = lease(sandbox_id("019d84bd-280d-7331-99c6-b63377f3a3a2"));

    let error = admission
        .check_new_allocation_at_commit(8320, &observation, &[newly_committed])
        .unwrap_err();

    assert!(matches!(
        error,
        MemoryAdmissionError::InsufficientCapacity {
            existing_growth_reserve_mib: 8320,
            ..
        }
    ));
}

#[tokio::test]
async fn replacement_lease_generation_is_charged_after_observation() {
    let (_root, sources) = sources(17_663, 17_663);
    let admission = MemoryAdmissionConfig { reserve_mib: 1024 };
    let sandbox_id = sandbox_id("019d84bd-280d-7331-99c6-b63377f3a3a2");
    write_cgroup(&sources, sandbox_id, 8000, 8320);
    let observation = admission
        .observe_active_leases(&[lease_generation(sandbox_id, 1)], &sources)
        .await
        .unwrap();

    for (slot_id, generation) in [(0, 2), (1, 1)] {
        let replacement = ActiveLeaseMemoryReservation {
            slot_id,
            ..lease_generation(sandbox_id, generation)
        };
        let error = admission
            .check_new_allocation_at_commit(8320, &observation, &[replacement])
            .unwrap_err();
        assert!(matches!(
            error,
            MemoryAdmissionError::InsufficientCapacity {
                existing_growth_reserve_mib: 8640,
                ..
            }
        ));
    }
    let increased = ActiveLeaseMemoryReservation {
        memory_reservation_mib: 9320,
        ..lease_generation(sandbox_id, 1)
    };
    assert_eq!(
        admission
            .check_new_allocation_at_commit(8320, &observation, &[increased])
            .unwrap()
            .existing_growth_reserve_mib,
        1320,
    );
}

#[tokio::test]
async fn exact_capacity_boundary_admits_and_one_mib_over_rejects() {
    let (_root, exact_sources) = sources(10_000, 9344);
    let admission = MemoryAdmissionConfig { reserve_mib: 1024 };

    let exact = admission
        .evaluate_new_allocation(8320, &[], &exact_sources)
        .await
        .unwrap();
    assert_eq!(exact.required_mib, exact.node_memory_total_mib);

    let (_root, over_sources) = sources(10_000, 9343);
    let error = admission
        .evaluate_new_allocation(8320, &[], &over_sources)
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        MemoryAdmissionEvaluationError::Admission(MemoryAdmissionError::InsufficientCapacity {
            required_mib: 10_001,
            ..
        })
    ));
}

#[tokio::test]
async fn malformed_visible_cgroup_falls_back_to_persisted_cap() {
    let (_root, sources) = sources(20_000, 20_000);
    let sandbox_id = sandbox_id("019d84bd-280d-7331-99c6-b63377f3a3a2");
    let cgroup_path = sources.firecracker_cgroup_base.join(sandbox_id.to_string());
    std::fs::create_dir(&cgroup_path).unwrap();
    std::fs::write(cgroup_path.join("memory.current"), "not-a-number\n").unwrap();
    std::fs::write(cgroup_path.join("memory.max"), "8724152320\n").unwrap();

    let measurement = MemoryAdmissionConfig { reserve_mib: 1024 }
        .evaluate_new_allocation(8320, &[lease(sandbox_id)], &sources)
        .await
        .unwrap();

    assert_eq!(measurement.existing_growth_reserve_mib, 8320);
}

#[tokio::test]
async fn unreadable_meminfo_fails_admission_closed() {
    let root = tempfile::tempdir().unwrap();
    let sources = MemoryAdmissionSources::new(
        root.path().join("missing-meminfo"),
        root.path().join("cgroups"),
    );

    let error = MemoryAdmissionConfig { reserve_mib: 1024 }
        .evaluate_new_allocation(8320, &[], &sources)
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        MemoryAdmissionEvaluationError::NodeMemory(NodeMemoryCapacityError::Read { .. })
    ));
}
