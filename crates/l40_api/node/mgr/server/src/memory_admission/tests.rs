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
    std::fs::write(
        cgroup_path.join("memory.stat"),
        "active_file 0\ninactive_file 0\n",
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

    let measurement = MemoryAdmissionConfig::default()
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

    let measurement = MemoryAdmissionConfig::default()
        .evaluate_new_allocation(8320, &[lease(sandbox_id)], &sources)
        .await
        .unwrap();

    assert_eq!(measurement.existing_growth_reserve_mib, 320);
}

#[tokio::test]
async fn invisible_active_lease_is_charged_its_persisted_cap() {
    let (_root, sources) = sources(20_000, 20_000);
    let sandbox_id = sandbox_id("019d84bd-280d-7331-99c6-b63377f3a3a2");

    let measurement = MemoryAdmissionConfig::default()
        .evaluate_new_allocation(8320, &[lease(sandbox_id)], &sources)
        .await
        .unwrap();

    assert_eq!(measurement.existing_growth_reserve_mib, 8320);
}

#[tokio::test]
async fn lease_committed_after_observation_is_charged_its_persisted_cap() {
    let (_root, sources) = sources(17_663, 17_663);
    let admission = MemoryAdmissionConfig::default();
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
    let admission = MemoryAdmissionConfig::default();
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
    let admission = MemoryAdmissionConfig::default();

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

    let measurement = MemoryAdmissionConfig::default()
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

    let error = MemoryAdmissionConfig::default()
        .evaluate_new_allocation(8320, &[], &sources)
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        MemoryAdmissionEvaluationError::NodeMemory(NodeMemoryCapacityError::Read { .. })
    ));
}

#[tokio::test]
async fn admittable_count_matches_admission_at_the_boundary() {
    // 100_000 MiB total, 20_000 used, one running VM with 1024 MiB growth
    // reserve, 1024 MiB reserve: 100_000 - 20_000 - 1024 - 1024 = 77_952 MiB
    // free for new demand, i.e. 9 sandboxes of 8320 MiB (74_880), not 10.
    let (_root, sources) = sources(100_000, 80_000);
    let sandbox_id = sandbox_id("019d84bd-280d-7331-99c6-b63377f3a3a2");
    write_cgroup(&sources, sandbox_id, 256, 8320);
    let admission = MemoryAdmissionConfig::default();
    let leases = [lease(sandbox_id)];
    let observation = admission
        .observe_active_leases(&leases, &sources)
        .await
        .unwrap();

    let admittable = admission.admittable_new_sandboxes(8320, &observation, 64);

    assert_eq!(admittable, 9);
    admission
        .check_new_allocation_at_commit(8320 * 9, &observation, &leases)
        .unwrap();
    admission
        .check_new_allocation_at_commit(8320 * 10, &observation, &leases)
        .unwrap_err();
}

#[tokio::test]
async fn admittable_count_is_bounded_by_the_slot_limit() {
    let (_root, sources) = sources(1_000_000, 1_000_000);
    let admission = MemoryAdmissionConfig::default();
    let observation = admission
        .observe_active_leases(&[], &sources)
        .await
        .unwrap();

    assert_eq!(
        admission.admittable_new_sandboxes(8320, &observation, 64),
        64
    );
    assert_eq!(admission.admittable_new_sandboxes(8320, &observation, 0), 0);
}

#[tokio::test]
async fn full_node_admits_no_new_sandbox() {
    // 90_000 MiB used plus 9_000 MiB growth reserve: an 8320 MiB sandbox
    // needs 108_344 MiB of 100_000.
    let (_root, sources) = sources(100_000, 10_000);
    let admission = MemoryAdmissionConfig::default();
    let mut observation = admission
        .observe_active_leases(&[], &sources)
        .await
        .unwrap();
    observation.existing_growth_reserve_mib = 9_000;

    assert_eq!(
        admission.admittable_new_sandboxes(8320, &observation, 64),
        0
    );
}

fn write_parent_cgroup(
    sources: &MemoryAdmissionSources,
    current_mib: u64,
    maximum: &str,
    file_cache_mib: u64,
) {
    let base = &sources.firecracker_cgroup_base;
    std::fs::write(
        base.join("memory.current"),
        (current_mib * BYTES_PER_MIB).to_string(),
    )
    .unwrap();
    std::fs::write(base.join("memory.max"), format!("{maximum}\n")).unwrap();
    std::fs::write(
        base.join("memory.stat"),
        format!(
            "anon 0\nfile {0}\nactive_file {0}\ninactive_file 0\nshmem 0\n",
            file_cache_mib * BYTES_PER_MIB
        ),
    )
    .unwrap();
}

#[tokio::test]
async fn parent_cgroup_limit_refuses_while_host_reports_room() {
    // The host has 60_000 MiB available, but the sandbox parent cgroup is
    // 1_000 MiB below its memory.max.
    let (_root, sources) = sources(100_000, 60_000);
    write_parent_cgroup(&sources, 89_000, &(90_000 * BYTES_PER_MIB).to_string(), 0);

    let error = MemoryAdmissionConfig::default()
        .evaluate_new_allocation(8320, &[], &sources)
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        MemoryAdmissionEvaluationError::Admission(
            MemoryAdmissionError::InsufficientParentCgroupCapacity {
                parent_cgroup_used_mib: 89_000,
                parent_cgroup_max_mib: 90_000,
                required_mib: 98_344,
                ..
            }
        )
    ));
}

#[tokio::test]
async fn parent_cgroup_file_cache_counts_as_reclaimable() {
    let (_root, sources) = sources(100_000, 60_000);
    write_parent_cgroup(
        &sources,
        89_000,
        &(90_000 * BYTES_PER_MIB).to_string(),
        20_000,
    );

    MemoryAdmissionConfig::default()
        .evaluate_new_allocation(8320, &[], &sources)
        .await
        .unwrap();
}

#[tokio::test]
async fn unlimited_parent_cgroup_leaves_only_the_host_check() {
    let (_root, sources) = sources(100_000, 60_000);
    write_parent_cgroup(&sources, 89_000, "max", 0);

    MemoryAdmissionConfig::default()
        .evaluate_new_allocation(8320, &[], &sources)
        .await
        .unwrap();
}

#[tokio::test]
async fn placement_count_follows_the_parent_cgroup_limit() {
    // 30_000 MiB under the parent limit less the 1024 MiB reserve fits three
    // 8320 MiB sandboxes; the host alone would fit seven.
    let (_root, sources) = sources(100_000, 60_000);
    write_parent_cgroup(&sources, 60_000, &(90_000 * BYTES_PER_MIB).to_string(), 0);
    let admission = MemoryAdmissionConfig::default();
    let observation = admission
        .observe_active_leases(&[], &sources)
        .await
        .unwrap();

    assert_eq!(
        admission.admittable_new_sandboxes(8320, &observation, 64),
        3
    );
}

#[tokio::test]
async fn commit_percent_charges_running_sandboxes_up_to_their_committed_share() {
    let (_root, sources) = sources(100_000, 100_000);
    let sandbox_id = sandbox_id("019d84bd-280d-7331-99c6-b63377f3a3a2");
    write_cgroup(&sources, sandbox_id, 256, 8320);

    // 0 keeps the reserve-only growth charge; above it each running sandbox
    // is charged up to its share of memory.max, never past memory.max.
    for (commit_percent, growth_mib) in [(0, 1024), (50, 4160 - 256), (100, 8320 - 256)] {
        let measurement = MemoryAdmissionConfig {
            commit_percent,
            ..MemoryAdmissionConfig::default()
        }
        .evaluate_new_allocation(8320, &[lease(sandbox_id)], &sources)
        .await
        .unwrap();
        assert_eq!(
            measurement.existing_growth_reserve_mib, growth_mib,
            "commit_percent {commit_percent}"
        );
    }
}

#[tokio::test]
async fn unreadable_parent_cgroup_leaves_only_the_host_check() {
    let (_root, sources) = sources(100_000, 60_000);
    write_parent_cgroup(&sources, 89_000, &(90_000 * BYTES_PER_MIB).to_string(), 0);
    std::fs::write(sources.firecracker_cgroup_base.join("memory.stat"), "").unwrap();

    MemoryAdmissionConfig::default()
        .evaluate_new_allocation(8320, &[], &sources)
        .await
        .unwrap();
}

#[tokio::test]
async fn running_sandbox_growth_includes_its_reclaimable_cache() {
    // 4096 MiB anonymous plus 4096 MiB cache in an 8320 MiB sandbox: the
    // cache counts as free, so the sandbox can still grow by 4224 MiB.
    let (_root, sources) = sources(100_000, 100_000);
    let sandbox_id = sandbox_id("019d84bd-280d-7331-99c6-b63377f3a3a2");
    write_cgroup(&sources, sandbox_id, 8192, 8320);
    std::fs::write(
        sources
            .firecracker_cgroup_base
            .join(sandbox_id.to_string())
            .join("memory.stat"),
        format!("active_file {}\ninactive_file 0\n", 4096 * BYTES_PER_MIB),
    )
    .unwrap();

    let measurement = MemoryAdmissionConfig {
        commit_percent: 100,
        ..MemoryAdmissionConfig::default()
    }
    .evaluate_new_allocation(8320, &[lease(sandbox_id)], &sources)
    .await
    .unwrap();

    assert_eq!(measurement.existing_growth_reserve_mib, 8320 - 4096);
}

#[tokio::test]
async fn running_sandbox_without_memory_stat_is_charged_its_full_reservation() {
    let (_root, sources) = sources(100_000, 100_000);
    let sandbox_id = sandbox_id("019d84bd-280d-7331-99c6-b63377f3a3a2");
    write_cgroup(&sources, sandbox_id, 256, 8320);
    std::fs::remove_file(
        sources
            .firecracker_cgroup_base
            .join(sandbox_id.to_string())
            .join("memory.stat"),
    )
    .unwrap();

    let measurement = MemoryAdmissionConfig::default()
        .evaluate_new_allocation(8320, &[lease(sandbox_id)], &sources)
        .await
        .unwrap();

    assert_eq!(measurement.existing_growth_reserve_mib, 8320);
}
