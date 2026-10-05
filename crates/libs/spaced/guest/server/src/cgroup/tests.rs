use super::{
    WORKLOADS_MEMORY_RESERVE_MAX_BYTES, WORKLOADS_PID_RESERVE, has_controller, missing_controllers,
    parse_unified_cgroup, workloads_memory_max_from, workloads_pids_max_from,
};

#[test]
fn io_controller_presence_gates_best_effort_io_weight() {
    // A delegated subtree usually exposes io alongside the required set;
    // there the best-effort io.weight step proceeds.
    assert!(has_controller(
        "cpuset cpu io memory hugetlb pids rdma misc",
        "io"
    ));
    // A guest that delegated only the required controllers has no io, so
    // the io step skips cleanly and the cpu/memory/pids split stays intact.
    assert!(!has_controller("cpu memory pids", "io"));
    assert!(!has_controller("", "io"));
    // Substring guard: a controller whose name merely contains "io"
    // must not be mistaken for the io controller.
    assert!(!has_controller("rdma misc iocost_fake", "io"));
}

#[test]
fn all_required_controllers_present_passes_preflight() {
    // A delegated subtree exposes the full controller set in
    // cgroup.controllers; nothing is missing.
    assert!(missing_controllers("cpuset cpu io memory hugetlb pids rdma misc").is_empty());
    assert!(missing_controllers("cpu memory pids").is_empty());
}

#[test]
fn detects_missing_controllers_without_delegation() {
    // An older seed without Delegate=yes exposes a partial set (or none)
    // here, so the move-then-fail window is predicted before any mutation.
    assert_eq!(missing_controllers("memory pids"), vec!["cpu"]);
    assert_eq!(missing_controllers("cpu"), vec!["memory", "pids"]);
    assert_eq!(missing_controllers(""), vec!["cpu", "memory", "pids"]);
}

#[test]
fn workloads_cap_binds_below_threads_max() {
    // Measured on a 1 GiB SPACE guest: pid_max is the kernel default
    // (32768) but threads-max is far lower (~7831), so the memory-derived
    // ceiling is what a fork bomb hits. The cap must come from
    // threads-max, not pid_max — the pid_max-only basis returned
    // 32768 - reserve, a dead cap well above the ~7831 tasks the kernel
    // will actually create.
    assert_eq!(
        workloads_pids_max_from(32768, 7831),
        Some(7831 - WORKLOADS_PID_RESERVE)
    );
}

#[test]
fn workloads_cap_uses_pid_max_when_it_is_the_lower_ceiling() {
    // Big-memory guest: threads-max exceeds pid_max, so pid_max binds.
    assert_eq!(
        workloads_pids_max_from(32768, 63000),
        Some(32768 - WORKLOADS_PID_RESERVE)
    );
}

#[test]
fn workloads_uncapped_when_ceiling_too_small_for_reserve() {
    // A ceiling at or below twice the reserve would have the reserve
    // claim half or more of it; leave workloads uncapped instead.
    assert_eq!(
        workloads_pids_max_from(WORKLOADS_PID_RESERVE * 2, 7831),
        None
    );
    assert_eq!(workloads_pids_max_from(32768, WORKLOADS_PID_RESERVE), None);
}

#[test]
fn parses_unified_cgroup_path() {
    let content = "0::/system.slice/spaced.service\n";
    assert_eq!(
        parse_unified_cgroup(content),
        Some("/system.slice/spaced.service")
    );
}

#[test]
fn parses_unified_cgroup_from_hybrid_hierarchy() {
    let content = "12:pids:/system.slice/spaced.service\n\
                   1:name=systemd:/system.slice/spaced.service\n\
                   0::/system.slice/spaced.service\n";
    assert_eq!(
        parse_unified_cgroup(content),
        Some("/system.slice/spaced.service")
    );
}

#[test]
fn no_unified_entry_in_pure_v1() {
    let content = "12:pids:/system.slice/spaced.service\n\
                   1:name=systemd:/system.slice/spaced.service\n";
    assert_eq!(parse_unified_cgroup(content), None);
}

#[test]
fn placement_hook_owns_the_descriptor_until_spawn() {
    let file = tempfile::tempfile().unwrap();
    let cgroups = super::GuestCgroups {
        workloads_procs: std::sync::Arc::new(file.try_clone().unwrap()),
    };
    let mut command = tokio::process::Command::new("/bin/sh");
    command.args(["-c", "exit 0"]);
    cgroups.place_spawned_workload(&mut command);
    drop(cgroups);

    // The command must keep the placement handle alive independently of the
    // service that registered the hook. No cgroup privileges are needed to
    // exercise the write and descriptor lifetime.
    let status = command.as_std_mut().status().unwrap();
    assert!(status.success());
    use std::os::unix::fs::FileExt;
    let mut written = [0];
    assert_eq!(file.read_at(&mut written, 0).unwrap(), 1);
    assert_eq!(&written, b"0");
}

const MIB: u64 = 1024 * 1024;

// Workloads keep all but an eighth of a small guest, and all but 128 MiB of a
// larger one, so the guest itself never runs out of memory for them.
#[test]
fn workloads_memory_cap_reserves_an_eighth_up_to_128_mib() {
    assert_eq!(workloads_memory_max_from(512 * MIB), Some(448 * MIB));
    assert_eq!(workloads_memory_max_from(1024 * MIB), Some(896 * MIB));
    assert_eq!(
        workloads_memory_max_from(4096 * MIB),
        Some(4096 * MIB - WORKLOADS_MEMORY_RESERVE_MAX_BYTES)
    );
}

#[test]
fn workloads_memory_uncapped_when_the_guest_is_too_small_to_split() {
    assert_eq!(workloads_memory_max_from(0), None);
    assert_eq!(workloads_memory_max_from(7), None);
}
