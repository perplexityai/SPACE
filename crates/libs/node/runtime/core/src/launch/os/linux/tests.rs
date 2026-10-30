use super::{JailerArgs, direct_jailer_command, firecracker_api_socket_path, jailer_args};
use super::{
    ProcessHandle, process_state_from_status, validate_process_pid, validate_raw_process_pid,
};
use crate::cgroup::CgroupLimits;
use std::os::unix::process::ExitStatusExt;

#[test]
fn reject_process_group_pids() {
    validate_process_pid(0).unwrap_err();
    validate_raw_process_pid(0).unwrap_err();
    validate_raw_process_pid(-1).unwrap_err();
}

#[test]
fn reject_own_process_pids() {
    let pid = std::process::id();
    validate_process_pid(pid).unwrap_err();
    validate_raw_process_pid(pid as i32).unwrap_err();
}

#[test]
fn accept_positive_process_pids() {
    let pid = if std::process::id() == 1 { 2 } else { 1 };
    assert_eq!(validate_process_pid(pid).unwrap(), pid as libc::pid_t);
    assert_eq!(
        validate_raw_process_pid(pid as i32).unwrap(),
        pid as libc::pid_t
    );
}

#[test]
fn process_handle_rejects_process_group_pid() {
    assert!(ProcessHandle::open(0).is_err());
}

#[test]
fn process_handle_signals_opened_process() {
    // Include the protocol's upper real-time signal: named-only signal enums
    // would reject it even though the kernel accepts it.
    for signal in [libc::SIGTERM, 64] {
        let mut child = std::process::Command::new("sleep")
            .arg("10")
            .spawn()
            .unwrap();
        let handle = ProcessHandle::open(child.id()).unwrap();

        assert!(handle.signal(signal).unwrap());
        let status = child.wait().unwrap();
        assert_eq!(status.signal(), Some(signal));
        assert!(
            !handle.signal(signal).unwrap(),
            "a reaped process is never signaled again"
        );
        assert_eq!(
            handle.sandbox_ownership("anything").unwrap(),
            crate::launch::ProcessOwnership::Gone,
        );
    }
}

struct KillAndReapChild(std::process::Child);

impl Drop for KillAndReapChild {
    fn drop(&mut self) {
        if self.0.try_wait().is_ok_and(|status| status.is_some()) {
            return;
        }
        if let Err(error) = self.0.kill() {
            tracing::warn!(%error, "failed to kill test child during cleanup");
        }
        if let Err(error) = self.0.wait() {
            tracing::warn!(%error, "failed to reap test child during cleanup");
        }
    }
}

#[test]
fn sandbox_signal_preserves_foreign_process_and_skips_reaped_owner() {
    use std::io::Read;
    use std::os::fd::OwnedFd;
    use std::os::unix::net::UnixStream;
    use std::process::{Command, Stdio};
    use std::time::Duration;

    let sandbox = "019dff0a-611d-7811-ab02-60469a5e65ec";
    let (mut ready, child_ready) = UnixStream::pair().unwrap();
    ready
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    // spawn() may return before /proc exposes the new argv. The child must
    // acknowledge startup before ownership inspection, then block on stdin.
    let mut child = KillAndReapChild(
        Command::new("sh")
            .args(["-c", "printf r; read ignored", sandbox])
            .stdin(Stdio::piped())
            .stdout(Stdio::from(OwnedFd::from(child_ready)))
            .spawn()
            .unwrap(),
    );
    let mut ack = [0];
    ready.read_exact(&mut ack).unwrap();
    assert_eq!(ack, *b"r");
    let handle = ProcessHandle::open(child.0.id()).unwrap();
    assert!(
        !handle
            .signal_if_owned("foreign-sandbox", libc::SIGKILL)
            .unwrap()
    );
    assert!(child.0.try_wait().unwrap().is_none());
    assert!(handle.signal_if_owned(sandbox, libc::SIGTERM).unwrap());
    assert_eq!(child.0.wait().unwrap().signal(), Some(libc::SIGTERM));
    assert!(!handle.signal_if_owned(sandbox, libc::SIGKILL).unwrap());
}

#[test]
fn process_exit_check_survives_signal_interruption() {
    use nix::sys::signal::{SaFlags, SigAction, SigHandler, SigSet, Signal, sigaction};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    // The supervisor's SIGCHLD handler runs whenever a child exits, and poll(2)
    // is never restarted after a handler, whatever SA_RESTART says.
    static HANDLED: AtomicUsize = AtomicUsize::new(0);
    extern "C" fn count_signal(_: libc::c_int) {
        HANDLED.fetch_add(1, Ordering::Relaxed);
    }
    const SIGNALS: usize = 2_000;

    let child = KillAndReapChild(
        std::process::Command::new("sleep")
            .arg("30")
            .spawn()
            .unwrap(),
    );
    let handle = ProcessHandle::open(child.0.id()).unwrap();
    let action = SigAction::new(
        SigHandler::Handler(count_signal),
        SaFlags::empty(),
        SigSet::empty(),
    );
    // SAFETY: the handler only touches an atomic, and the previous action is
    // restored before the test returns.
    let previous = unsafe { sigaction(Signal::SIGCHLD, &action) }.unwrap();
    let process = rustix::process::getpid();
    let thread = rustix::thread::gettid();
    let done = AtomicBool::new(false);
    let deadline = Instant::now() + Duration::from_secs(20);
    let result = std::thread::scope(|scope| {
        scope.spawn(|| {
            while !done.load(Ordering::Relaxed) {
                // SAFETY: tgkill signals only this test's own polling thread.
                unsafe {
                    libc::syscall(
                        libc::SYS_tgkill,
                        process.as_raw_nonzero().get(),
                        thread.as_raw_nonzero().get(),
                        libc::SIGCHLD,
                    );
                }
                std::thread::yield_now();
            }
        });
        let result = (|| {
            while HANDLED.load(Ordering::Relaxed) < SIGNALS {
                if Instant::now() >= deadline {
                    return Err(format!(
                        "only {} signals arrived",
                        HANDLED.load(Ordering::Relaxed)
                    ));
                }
                match handle.has_exited() {
                    Ok(false) => {}
                    other => return Err(format!("live child observed as {other:?}")),
                }
            }
            Ok(())
        })();
        done.store(true, Ordering::Relaxed);
        result
    });
    // SAFETY: restores the action this test replaced.
    unsafe { sigaction(Signal::SIGCHLD, &previous) }.unwrap();
    result.unwrap();
}

#[test]
fn parses_proc_status_state() {
    let status = "Name:\tfirecracker\nState:\tZ (zombie)\nPid:\t123\n";
    assert_eq!(process_state_from_status(status), Some('Z'));

    let status = "Name:\tfirecracker\nState:\tS (sleeping)\nPid:\t123\n";
    assert_eq!(process_state_from_status(status), Some('S'));

    assert_eq!(process_state_from_status("Name:\tfirecracker\n"), None);
}

#[test]
fn jailer_args_keep_existing_launch_contract() {
    let args = jailer_args(JailerArgs {
        sandbox_id: "019dff0a-611d-7811-ab02-60469a5e65ec",
        firecracker_bin: "/space/bin/firecracker",
        chroot_base_dir: "/space/cache/firecracker/jails",
        ns_path: "/var/run/netns/sns-test",
        uid: 200000,
        gid: 200000,
        fc_config_path: "/vm-config.json",
        fc_log_path: "/fc.log",
        fc_metrics_path: "",
        cgroup_limits: &CgroupLimits::default(),
        daemonize: true,
    });

    assert!(
        args.windows(2).any(
            |window| window[0] == "--id" && window[1] == "019dff0a-611d-7811-ab02-60469a5e65ec"
        )
    );
    assert!(
        args.windows(2)
            .any(|window| window[0] == "--exec-file" && window[1] == "/space/bin/firecracker")
    );
    assert!(args.iter().any(|arg| arg == "--daemonize"));
    assert!(
        args.windows(2)
            .any(|window| window[0] == "--config-file" && window[1] == "/vm-config.json")
    );
    assert!(
        args.windows(2)
            .any(|window| window[0] == "--api-sock" && window[1] == "firecracker.sock")
    );

    let console_capture_args = jailer_args(JailerArgs {
        sandbox_id: "019dff0a-611d-7811-ab02-60469a5e65ec",
        firecracker_bin: "/space/bin/firecracker",
        chroot_base_dir: "/space/cache/firecracker/jails",
        ns_path: "/var/run/netns/sns-test",
        uid: 200000,
        gid: 200000,
        fc_config_path: "/vm-config.json",
        fc_log_path: "/fc.log",
        fc_metrics_path: "",
        cgroup_limits: &CgroupLimits::default(),
        daemonize: false,
    });
    assert!(!console_capture_args.iter().any(|arg| arg == "--daemonize"));
}

#[test]
fn direct_jailer_command_runs_jailer_without_systemd_wrapper() {
    let args = vec!["--id".to_string(), "sandbox-a".to_string()];
    let command = direct_jailer_command(
        crate::launch::JailerExecutable::LegacyPath("/space/bin/jailer"),
        &args,
    )
    .unwrap();
    let rendered: Vec<_> = command
        .get_args()
        .map(|arg| arg.to_string_lossy().into_owned())
        .collect();

    assert_eq!(command.get_program().to_string_lossy(), "/space/bin/jailer");
    assert_eq!(rendered, args);
}

#[test]
fn direct_jailer_preserves_runtime_supervisor_scheduling_priority() {
    let parent_nice = rustix::process::getpriority_process(None).expect("read parent priority");
    let cat_path = if std::path::Path::new("/bin/cat").exists() {
        "/bin/cat"
    } else {
        "/usr/bin/cat"
    };
    let mut command = direct_jailer_command(
        crate::launch::JailerExecutable::LegacyPath(cat_path),
        &["/proc/self/stat".to_string()],
    )
    .unwrap();
    let output = command.output().expect("read spawned process stat");
    assert!(output.status.success());
    let stat = String::from_utf8(output.stdout).expect("proc stat is utf8");
    let child_nice: i32 = stat
        .split_ascii_whitespace()
        .nth(18)
        .expect("proc stat exposes nice field")
        .parse()
        .expect("nice field is numeric");

    assert_eq!(
        child_nice, parent_nice,
        "direct jailer should not lower Firecracker CPU scheduling priority"
    );
}

#[test]
fn firecracker_api_socket_path_points_inside_jail_root() {
    let socket_path = firecracker_api_socket_path("/space/cache/firecracker/jails", "sandbox-a");

    assert_eq!(
        socket_path,
        "/space/cache/firecracker/jails/firecracker/sandbox-a/root/firecracker.sock"
    );
}

#[test]
fn sidecar_exec_preserves_pid_arguments_and_explicit_environment() {
    const CHILD: &str = "SPACE_TEST_SIDECAR_EXEC_CHILD";
    const VALUE: &str = "SPACE_TEST_SIDECAR_EXEC_VALUE";
    if let Ok(mode) = std::env::var(CHILD) {
        use nix::sys::signal::{SigSet, SigmaskHow, Signal, pthread_sigmask};
        use std::ffi::CString;
        let mut blocked = SigSet::empty();
        blocked.add(Signal::SIGUSR1);
        pthread_sigmask(SigmaskHow::SIG_BLOCK, Some(&blocked), None).unwrap();
        let args: &[&str] = if mode == "mask" {
            // Inspect the exec target directly: a shell can reset its own mask.
            &["/bin/cat", "/proc/self/status"]
        } else {
            &[
                "/bin/sh",
                "-c",
                "printf 'RESULT:%s:%s:%s' \"$SPACE_TEST_SIDECAR_EXEC_VALUE\" \"$1\" \"$$\"; kill -PIPE $$",
                "sidecar",
                "argument with spaces",
            ]
        };
        let launch = super::PreparedSidecarLaunch {
            program: CString::new(args[0]).unwrap(),
            args: args.iter().map(|arg| CString::new(*arg).unwrap()).collect(),
            env: vec![(
                CString::new(VALUE).unwrap(),
                CString::new("explicit value").unwrap(),
            )],
        };
        launch.exec().unwrap();
        unreachable!("successful exec never returns");
    }
    for mode in ["shell", "mask"] {
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command
            .args(["--exact", "launch::os::linux::tests::sidecar_exec_preserves_pid_arguments_and_explicit_environment", "--nocapture"])
            .env(CHILD, mode)
            .env(VALUE, "inherited value");
        let (pid, output) =
            pplx_space_test_env::run_command(&mut command, std::time::Duration::from_secs(10))
                .unwrap();
        let stdout = String::from_utf8(output.stdout).unwrap();
        if mode == "shell" {
            assert_eq!(output.status.signal(), Some(libc::SIGPIPE));
            assert!(
                stdout.contains(&format!("RESULT:explicit value:argument with spaces:{pid}")),
                "{stdout}"
            );
        } else {
            assert!(output.status.success());
            let mask = stdout
                .lines()
                .find_map(|line| line.strip_prefix("SigBlk:"))
                .unwrap()
                .trim();
            let mask = u64::from_str_radix(mask, 16).unwrap();
            assert_ne!(mask & (1 << (libc::SIGUSR1 - 1)), 0, "{stdout}");
        }
    }
}

#[test]
fn jailer_execution_uses_the_opened_inode_after_path_replacement() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("jailer");
    std::fs::copy("/bin/true", &path).unwrap();
    let pinned = std::fs::File::open(&path).unwrap();
    let mut command = std::process::Command::new(&path);
    command.env_clear();
    super::bind_jailer_descriptor(&mut command, pinned, path.to_str().unwrap(), &[]).unwrap();
    std::fs::rename(&path, directory.path().join("original")).unwrap();
    std::fs::copy("/bin/false", &path).unwrap();
    assert!(command.status().unwrap().success());
    assert!(
        !std::process::Command::new(&path)
            .status()
            .unwrap()
            .success()
    );
}

#[test]
fn jailer_timeout_reports_its_output_without_waiting_for_its_descendants() {
    use std::os::unix::fs::PermissionsExt;
    use std::time::{Duration, Instant};

    /// Kills the background process the fake jailer leaves holding its pipes.
    struct KillRecordedPid(std::path::PathBuf);
    impl Drop for KillRecordedPid {
        fn drop(&mut self) {
            let Ok(pid) = std::fs::read_to_string(&self.0) else {
                return;
            };
            if let Ok(pid) = pid.trim().parse() {
                let _ = nix::sys::signal::kill(
                    nix::unistd::Pid::from_raw(pid),
                    nix::sys::signal::Signal::SIGKILL,
                );
            }
        }
    }

    let directory = tempfile::tempdir().unwrap();
    let jailer = directory.path().join("jailer");
    let background_pid = directory.path().join("background.pid");
    // Dropped at the end of the test, after the elapsed-time check, and
    // before the temporary directory.
    let _background = KillRecordedPid(background_pid.clone());
    // More stdout than the reported tail, then a stage that never finishes,
    // with a background process still holding both pipes open.
    std::fs::write(
        &jailer,
        format!(
            "#!/bin/sh\n\
             printf '%03000d\\n' 0\n\
             echo first-stage-done\n\
             echo second-stage-waiting >&2\n\
             sleep 10 &\n\
             echo $! > '{}'\n\
             exec sleep 10\n",
            background_pid.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&jailer, std::fs::Permissions::from_mode(0o755)).unwrap();

    let started = Instant::now();
    let error = super::run_jailer_command(
        "sandbox-a",
        crate::launch::JailerExecutable::LegacyPath(jailer.to_str().unwrap()),
        "",
        &[],
        Duration::from_millis(300),
    )
    .unwrap_err();

    assert!(started.elapsed() < Duration::from_secs(5), "{error}");
    assert!(
        error.starts_with("jailer for sandbox-a timed out after "),
        "{error}"
    );
    assert!(error.contains("(limit 300ms)"), "{error}");
    assert!(error.contains("first-stage-done"), "{error}");
    assert!(error.contains("second-stage-waiting"), "{error}");
    assert!(
        error.len() < 2 * super::JAILER_OUTPUT_TAIL_BYTES + 200,
        "output tail is unbounded: {} bytes",
        error.len()
    );
}
