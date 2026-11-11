#[test]
fn placement_hook_owns_the_descriptor_until_spawn() {
    use super::ExecutionCgroups;
    #[cfg_attr(
        not(target_os = "linux"),
        expect(
            clippy::single_element_loop,
            reason = "Filesystem workers are Linux-only; other platforms exercise one hook"
        )
    )]
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

#[cfg(target_os = "linux")]
#[test]
fn placement_is_written_by_the_child_before_exec_can_fail() {
    use std::fs::File;
    use std::io::IoSliceMut;
    use std::os::fd::{AsRawFd, OwnedFd};
    use std::os::unix::net::UnixDatagram;

    use nix::sys::socket::{ControlMessageOwned, MsgFlags, recvmsg, setsockopt, sockopt};

    use super::ExecutionCgroups;

    for place in [
        ExecutionCgroups::place_spawned_workload,
        ExecutionCgroups::place_filesystem_worker,
    ] {
        // The kernel stamps the production placement write with its sender's
        // PID. A parent-side write cannot satisfy this oracle. The real cgroup
        // effect is covered by the privileged embedding boundary probe.
        let (receiver, writer) = UnixDatagram::pair().unwrap();
        setsockopt(&receiver, sockopt::PassCred, &true).unwrap();
        receiver.set_nonblocking(true).unwrap();
        let writer = File::from(OwnedFd::from(writer));
        let cgroups = ExecutionCgroups::new(writer.try_clone().unwrap(), writer);
        let directory = tempfile::tempdir().unwrap();
        let mut command = tokio::process::Command::new(directory.path().join("absent"));
        place(&cgroups, &mut command);
        drop(cgroups);

        // No executable can run and spawn returns no Child. Placement must
        // already have happened in the forked child, even on this failed exec.
        assert_eq!(
            command.as_std_mut().spawn().unwrap_err().kind(),
            std::io::ErrorKind::NotFound
        );
        let mut byte = [0];
        let mut buffers = [IoSliceMut::new(&mut byte)];
        let mut credentials = nix::cmsg_space!(nix::libc::ucred);
        let message = recvmsg::<()>(
            receiver.as_raw_fd(),
            &mut buffers,
            Some(&mut credentials),
            MsgFlags::empty(),
        )
        .expect("placement must precede the failed exec");
        assert_eq!(message.bytes, 1);
        let sender = message
            .cmsgs()
            .unwrap()
            .find_map(|message| match message {
                ControlMessageOwned::ScmCredentials(credentials) => Some(credentials.pid()),
                _ => None,
            })
            .expect("kernel-authenticated sender");
        assert!(sender > 0);
        assert_ne!(
            u32::try_from(sender).unwrap(),
            std::process::id(),
            "placement ran in parent"
        );
        assert_eq!(byte, *b"0");
    }
}
