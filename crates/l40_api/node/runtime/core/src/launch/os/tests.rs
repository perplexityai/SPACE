use super::*;

#[test]
fn adoption_requires_all_uid_and_gid_credentials() {
    for (status, expected) in [
        (
            "Uid:\t10000 10000 10000 10000\nGid:\t10000 10000 10000 10000\n",
            true,
        ),
        (
            "Uid:\t10000 10000 10000 10000\nGid:\t2000000 2000000 2000000 2000000\n",
            false,
        ),
        (
            "Uid:\t10000 10000 0 10000\nGid:\t10000 10000 10000 10000\n",
            false,
        ),
        ("Uid:\t10000 10000\nGid:\t10000 10000 10000 10000\n", false),
        ("Uid:\t10000 10000 10000 10000\n", false),
    ] {
        assert_eq!(credentials_match(status, 10000, 10000), expected);
    }
}

#[cfg(target_os = "linux")]
#[test]
fn pinned_process_checks_program_and_exact_unified_cgroup() {
    let mut child = std::process::Command::new("sleep")
        .arg("2")
        .spawn()
        .unwrap();
    let process = ProcessHandle::open(child.id()).unwrap();
    let groups = std::fs::read_to_string(format!("/proc/{}/cgroup", child.id())).unwrap();
    let relative = groups
        .lines()
        .find_map(|line| line.strip_prefix("0::"))
        .unwrap();
    let actual = std::path::Path::new("/sys/fs/cgroup").join(relative.trim_start_matches('/'));
    assert_eq!(process.cgroup_matches(&actual).unwrap(), Some(true));
    assert_eq!(
        process.cgroup_matches(&actual.join("sidecar")).unwrap(),
        Some(false)
    );
    assert_eq!(
        process.program_matches("cloud-hypervisor").unwrap(),
        Some(false)
    );
    child.kill().unwrap();
    child.wait().unwrap();
    assert_eq!(process.cgroup_matches(&actual).unwrap(), None);
}
