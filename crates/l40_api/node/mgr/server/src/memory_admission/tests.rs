use std::str::FromStr;

use super::*;

const HOST: MemoryAdmissionConfig = MemoryAdmissionConfig {
    reserve_mib: 1024,
    parent_cgroup: None,
};

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
    write_vm_cgroup(sources, sandbox_id, current_mib, 0, maximum_mib);
}

/// A VM cgroup whose `memory.current` is `anon_mib + file_mib`.
fn write_vm_cgroup(
    sources: &MemoryAdmissionSources,
    sandbox_id: SandboxId,
    anon_mib: u64,
    file_mib: u64,
    maximum_mib: u64,
) {
    let cgroup_path = sources.firecracker_cgroup_base.join(sandbox_id.to_string());
    std::fs::create_dir(&cgroup_path).unwrap();
    let bytes = |mib: u64| (mib * BYTES_PER_MIB).to_string();
    std::fs::write(
        cgroup_path.join("memory.current"),
        bytes(anon_mib + file_mib),
    )
    .unwrap();
    std::fs::write(cgroup_path.join("memory.max"), bytes(maximum_mib)).unwrap();
    std::fs::write(
        cgroup_path.join("memory.stat"),
        format!(
            "anon {}\nfile {}\nkernel 0\nshmem 0\n",
            bytes(anon_mib),
            bytes(file_mib)
        ),
    )
    .unwrap();
    std::fs::write(cgroup_path.join("memory.events"), "oom 0\noom_kill 0\n").unwrap();
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

    let measurement = HOST
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

    let measurement = HOST
        .evaluate_new_allocation(8320, &[lease(sandbox_id)], &sources)
        .await
        .unwrap();

    assert_eq!(measurement.existing_growth_reserve_mib, 320);
}

#[tokio::test]
async fn invisible_active_lease_is_charged_its_persisted_cap() {
    let (_root, sources) = sources(20_000, 20_000);
    let sandbox_id = sandbox_id("019d84bd-280d-7331-99c6-b63377f3a3a2");

    let measurement = HOST
        .evaluate_new_allocation(8320, &[lease(sandbox_id)], &sources)
        .await
        .unwrap();

    assert_eq!(measurement.existing_growth_reserve_mib, 8320);
}

#[tokio::test]
async fn lease_committed_after_observation_is_charged_its_persisted_cap() {
    let (_root, sources) = sources(17_663, 17_663);
    let admission = HOST;
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
    let admission = HOST;
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
    let admission = HOST;

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

    let measurement = HOST
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

    let error = HOST
        .evaluate_new_allocation(8320, &[], &sources)
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        MemoryAdmissionEvaluationError::NodeMemory(NodeMemoryCapacityError::Read { .. })
    ));
}

fn write_parent_cgroup(
    sources: &MemoryAdmissionSources,
    maximum_mib: u64,
    anon_mib: u64,
    kernel_mib: u64,
    shmem_mib: u64,
    file_mib: u64,
) {
    let base = &sources.firecracker_cgroup_base;
    let bytes = |mib: u64| (mib * BYTES_PER_MIB).to_string();
    std::fs::write(base.join("memory.max"), bytes(maximum_mib)).unwrap();
    std::fs::write(
        base.join("memory.current"),
        bytes(anon_mib + kernel_mib + file_mib),
    )
    .unwrap();
    std::fs::write(
        base.join("memory.stat"),
        format!(
            "anon {}\nfile {}\nkernel {}\nshmem {}\n",
            bytes(anon_mib),
            bytes(file_mib),
            bytes(kernel_mib),
            bytes(shmem_mib)
        ),
    )
    .unwrap();
    std::fs::write(base.join("memory.events"), "oom 0\noom_kill 0\n").unwrap();
}

fn with_parent_cgroup(active_level_mib: u64) -> MemoryAdmissionConfig {
    MemoryAdmissionConfig {
        parent_cgroup: Some(ParentCgroupAdmission {
            active_level_mib,
            file_floor_mib: 1024,
        }),
        ..HOST
    }
}

#[tokio::test]
async fn parent_cgroup_check_refuses_at_the_parent_limit_while_host_passes() {
    let (_root, sources) = sources(100_000, 90_000);
    let sandbox_id = sandbox_id("019d84bd-280d-7331-99c6-b63377f3a3a2");
    write_cgroup(&sources, sandbox_id, 1000, 4000);
    let leases = [lease(sandbox_id)];
    // anon 3500 + kernel 1000 + shmem 500 + growth (2048 - 1000) + reserve
    // 1024 + file floor 1024 + new 4096 = 12192; other file pages are not
    // counted.
    write_parent_cgroup(&sources, 12_192, 3500, 1000, 500, 9000);
    let exact = with_parent_cgroup(2048)
        .evaluate_new_allocation(4096, &leases, &sources)
        .await
        .unwrap();
    assert_eq!(exact.parent_cgroup.unwrap().required_mib, 12_192);
    // The host check is unchanged by the parent check.
    assert_eq!(
        exact,
        MemoryAdmissionMeasurement {
            parent_cgroup: exact.parent_cgroup,
            ..HOST
                .evaluate_new_allocation(4096, &leases, &sources)
                .await
                .unwrap()
        }
    );

    write_parent_cgroup(&sources, 12_191, 3500, 1000, 500, 9000);
    let MemoryAdmissionEvaluationError::Admission(error) = with_parent_cgroup(2048)
        .evaluate_new_allocation(4096, &leases, &sources)
        .await
        .unwrap_err()
    else {
        panic!("expected an admission refusal");
    };
    assert_eq!(error.limit(), MemoryAdmissionLimit::ParentCgroup);
    assert!(matches!(
        error,
        MemoryAdmissionError::ParentCgroupInsufficientCapacity {
            parent_cgroup_booked_growth_mib: 1048,
            file_floor_mib: 1024,
            required_mib: 12_192,
            ..
        }
    ));
}

#[tokio::test]
async fn host_check_still_refuses_while_the_parent_cgroup_passes() {
    let (_root, sources) = sources(10_000, 9343);
    write_parent_cgroup(&sources, 100_000, 0, 0, 0, 0);

    let MemoryAdmissionEvaluationError::Admission(error) = with_parent_cgroup(2048)
        .evaluate_new_allocation(8320, &[], &sources)
        .await
        .unwrap_err()
    else {
        panic!("expected an admission refusal");
    };

    assert_eq!(error.limit(), MemoryAdmissionLimit::Host);
}

#[tokio::test]
async fn parent_cgroup_check_books_each_vm_up_to_its_active_level() {
    let (_root, sources) = sources(100_000, 100_000);
    write_parent_cgroup(&sources, 100_000, 0, 0, 0, 0);
    let small = sandbox_id("019d84bd-280d-7331-99c6-b63377f3a3a2");
    let grown = sandbox_id("019d84bd-280d-7331-99c6-b63377f3a3a3");
    write_cgroup(&sources, small, 500, 4000);
    write_cgroup(&sources, grown, 3000, 4000);
    let leases = [lease(small), lease(grown)];

    for (active_level_mib, booked_mib) in [(2048, 1548), (10_000, 3500 + 1000)] {
        let measurement = with_parent_cgroup(active_level_mib)
            .evaluate_new_allocation(0, &leases, &sources)
            .await
            .unwrap();
        assert_eq!(
            measurement
                .parent_cgroup
                .unwrap()
                .parent_cgroup_booked_growth_mib,
            booked_mib
        );
    }
}

#[tokio::test]
async fn parent_cgroup_check_fails_closed_without_a_parent_limit() {
    let (_root, sources) = sources(100_000, 100_000);
    write_parent_cgroup(&sources, 0, 0, 0, 0, 0);
    let base = &sources.firecracker_cgroup_base;
    std::fs::write(base.join("memory.max"), "max\n").unwrap();

    let error = with_parent_cgroup(2048)
        .evaluate_new_allocation(4096, &[], &sources)
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        MemoryAdmissionEvaluationError::NodeMemory(NodeMemoryCapacityError::ParentCgroupUnlimited)
    ));
}

#[tokio::test]
async fn parent_cgroup_check_books_growth_above_anon_not_file_pages() {
    let (_root, sources) = sources(100_000, 100_000);
    write_parent_cgroup(&sources, 100_000, 0, 0, 0, 0);
    let restored = sandbox_id("019d84bd-280d-7331-99c6-b63377f3a3a2");
    // A restored VM: most of memory.current is its file-backed memory image.
    write_vm_cgroup(&sources, restored, 200, 3800, 4000);

    let measurement = with_parent_cgroup(2048)
        .evaluate_new_allocation(0, &[lease(restored)], &sources)
        .await
        .unwrap();

    assert_eq!(
        measurement
            .parent_cgroup
            .unwrap()
            .parent_cgroup_booked_growth_mib,
        2048 - 200
    );
}

#[tokio::test]
async fn parent_cgroup_probe_rejects_a_parent_the_check_cannot_use() {
    let (_root, sources) = sources(100_000, 100_000);
    let base = &sources.firecracker_cgroup_base;
    write_parent_cgroup(&sources, 100_000, 0, 0, 0, 0);
    probe_parent_cgroup(base).await.unwrap();

    for stat in ["file 0\n", "anon 0\nfile 0\nkernel 0\n"] {
        std::fs::write(base.join("memory.stat"), stat).unwrap();
        assert!(matches!(
            probe_parent_cgroup(base).await,
            Err(NodeMemoryCapacityError::ParentCgroupStatMissing)
        ));
    }
    write_parent_cgroup(&sources, 100_000, 0, 0, 0, 0);
    std::fs::write(base.join("memory.max"), "max\n").unwrap();
    assert!(matches!(
        probe_parent_cgroup(base).await,
        Err(NodeMemoryCapacityError::ParentCgroupUnlimited)
    ));
}
