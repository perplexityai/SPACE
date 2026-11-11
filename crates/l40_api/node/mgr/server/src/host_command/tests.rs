use super::*;

#[test]
fn foreground_command_io_and_failure_paths_are_bounded() {
    let bytes = vec![b'x'; 128 * 1024];
    let copied = output(Command::new("cat").arg("-"), Duration::from_secs(2), &bytes).unwrap();
    assert!(copied.status.success());
    assert_eq!(copied.stdout, bytes);

    let mut input = tempfile::tempfile().unwrap();
    input.write_all(&bytes).unwrap();
    std::io::Seek::rewind(&mut input).unwrap();
    let copied = stream_in_namespace(
        std::process::Command::new("cat").arg("-"),
        Duration::from_secs(2),
        Stdio::from(input),
        Stdio::piped(),
        None,
    )
    .unwrap();
    assert!(copied.status.success());
    assert_eq!(copied.stdout, bytes);

    for script in ["exec yes x", "exec yes x >&2"] {
        // Static test commands only. exec keeps the fixture a single foreground
        // child, matching the host-command runner's contract.
        let error = output(
            Command::new("sh").args(["-c", script]),
            Duration::from_secs(2),
            &[],
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }
    let failure = output(
        Command::new("sh").args(["-c", "printf failure >&2; exit 7"]),
        Duration::from_secs(2),
        &[],
    )
    .unwrap();
    assert_eq!(failure.status.code(), Some(7));
    assert_eq!(failure.stderr, b"failure");

    // Obtain the PID from spawn, not from a file the child may never get time
    // to write on a loaded host. Exercise the same capture/reap path.
    for cancelled in [false, true] {
        let cancel = tokio_util::sync::CancellationToken::new();
        let child = spawn::start(Command::new("sleep").arg("30"), Input::Captured).unwrap();
        let pid = libc::pid_t::try_from(child.id()).unwrap();
        if cancelled {
            cancel.cancel();
        }
        let error = capture(
            child,
            Instant::now(),
            Duration::from_millis(20),
            &[],
            Some(&cancel),
        )
        .unwrap_err();
        assert_eq!(
            error.kind(),
            if cancelled {
                io::ErrorKind::Interrupted
            } else {
                io::ErrorKind::TimedOut
            }
        );
        assert_eq!(
            nix::sys::wait::waitpid(
                nix::unistd::Pid::from_raw(pid),
                Some(nix::sys::wait::WaitPidFlag::WNOHANG)
            ),
            Err(nix::errno::Errno::ECHILD),
            "the timed-out child must already be reaped",
        );
    }
}

#[cfg(target_os = "linux")]
#[test]
fn spawn_preserves_errors_signal_defaults_and_child_ownership() {
    use std::os::unix::process::ExitStatusExt;
    const CLOSED_STDIO: &str = "SPACE_TEST_SPAWN_CLOSED_STDIO";
    const TEST_NAME: &str =
        "host_command::tests::spawn_preserves_errors_signal_defaults_and_child_ownership";
    if std::env::var_os(CLOSED_STDIO).is_some() {
        for fd in 0..=2 {
            nix::unistd::close(fd).unwrap();
        }
        let copied = output(
            Command::new("cat").arg("-"),
            Duration::from_secs(2),
            b"closed stdio",
        )
        .unwrap();
        assert!(copied.status.success());
        assert_eq!(copied.stdout, b"closed stdio");
        // Restore stdio for the test harness after exercising the closed case.
        use std::os::fd::IntoRawFd;
        for expected in 0..=2 {
            let fd = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open("/dev/null")
                .unwrap();
            assert_eq!(fd.into_raw_fd(), expected);
        }
        return;
    }
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", TEST_NAME])
        .env(CLOSED_STDIO, "1")
        .status()
        .unwrap();
    assert!(status.success(), "closed parent stdio must not break spawn");
    let descriptor_count = || std::fs::read_dir("/proc/self/fd").unwrap().count();
    let before = descriptor_count();
    let inaccessible = tempfile::NamedTempFile::new().unwrap();
    // A real non-executable file exercises EACCES independently of PATH.
    for _ in 0..16 {
        for (program, expected) in [
            (
                "/no-such-space-spawn-test-program".as_ref(),
                io::ErrorKind::NotFound,
            ),
            (
                inaccessible.path().as_os_str(),
                io::ErrorKind::PermissionDenied,
            ),
            (
                std::ffi::OsStr::new("bad\0program"),
                io::ErrorKind::InvalidInput,
            ),
        ] {
            let error =
                output(&mut Command::new(program), Duration::from_secs(2), &[]).unwrap_err();
            assert_eq!(error.kind(), expected);
        }
    }
    drop(inaccessible);
    assert_eq!(
        descriptor_count(),
        before,
        "failed spawns must close every pipe"
    );

    for signal in [libc::SIGPIPE, libc::SIGINT, libc::SIGTERM] {
        let script = format!("kill -{signal} $$; exit 9");
        let result = output(
            Command::new("sh").args(["-c", &script]),
            Duration::from_secs(2),
            &[],
        )
        .unwrap();
        assert_eq!(result.status.signal(), Some(signal));
    }
    assert_eq!(
        super::status(Command::new("sh").args(["-c", "exit 11"]))
            .unwrap()
            .code(),
        Some(11)
    );
    let mut child =
        spawn::start(Command::new("sh").args(["-c", "exit 7"]), Input::Captured).unwrap();
    let pid = nix::unistd::Pid::from_raw(child.id().cast_signed());
    let deadline = Instant::now() + Duration::from_secs(2);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(1));
    };
    assert_eq!(status.code(), Some(7));
    assert_eq!(child.try_wait().unwrap(), Some(status));
    assert_eq!(child.wait().unwrap(), status);
    drop(child);
    assert_eq!(
        nix::sys::wait::waitpid(pid, Some(nix::sys::wait::WaitPidFlag::WNOHANG)),
        Err(nix::errno::Errno::ECHILD)
    );
    assert_eq!(descriptor_count(), before);
}
