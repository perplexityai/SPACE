#[test]
fn placement_hook_owns_the_descriptor_until_spawn() {
    use super::ExecutionCgroups;
    for (filesystem, place) in [
        (
            false,
            ExecutionCgroups::place_spawned_workload
                as fn(&ExecutionCgroups, &mut tokio::process::Command),
        ),
        #[cfg(target_os = "linux")]
        (true, ExecutionCgroups::place_filesystem_worker),
    ] {
        let workload_file = tempfile::tempfile().unwrap();
        let filesystem_file = tempfile::tempfile().unwrap();
        let cgroups = ExecutionCgroups::new(
            workload_file.try_clone().unwrap(),
            filesystem_file.try_clone().unwrap(),
        );
        let mut command = tokio::process::Command::new("/bin/sh");
        command.args(["-c", "exit 0"]);
        place(&cgroups, &mut command);
        drop(cgroups);

        // The command must keep the placement handle alive independently of the
        // service that registered the hook. Each hook must use its own leaf.
        let status = command.as_std_mut().status().unwrap();
        assert!(status.success());
        use std::os::unix::fs::FileExt;
        let (selected, other) = if filesystem {
            (filesystem_file, workload_file)
        } else {
            (workload_file, filesystem_file)
        };
        let mut written = [0];
        assert_eq!(selected.read_at(&mut written, 0).unwrap(), 1);
        assert_eq!(&written, b"0");
        assert_eq!(other.read_at(&mut written, 0).unwrap(), 0);
    }
}
