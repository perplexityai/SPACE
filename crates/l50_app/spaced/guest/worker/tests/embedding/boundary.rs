//! Caller-owned placement and cleanup, outside the daemon's guest bootstrap.
use std::fs::{File, OpenOptions};
use std::os::fd::{BorrowedFd, RawFd};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::{FutureExt, StreamExt};
use nix::fcntl::{FcntlArg, FdFlag, fcntl};
use nix::sys::wait::{WaitPidFlag, WaitStatus, waitpid};
use nix::unistd::Pid;
use pplx_space_common_core::types::GuestUserName;
use pplx_space_spaced_guest_execution::{
    ExecutionCgroups, ExecutionService, ExecutionServiceConfig, WorkerExecutable,
};
use pplx_space_spaced_guest_protocol::proto::bridge_guest::{
    PtyConfig, SendInputRequest, StartRequest, StatRequest, filesystem_server::Filesystem,
    process_event::Event, process_exit::Outcome, process_server::Process,
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio_util::sync::CancellationToken;

#[path = "boundary/descriptors.rs"]
mod descriptors;

// Run once in the single-threaded test host, before any library work. Launcher
// descriptors remain usable by the host but must not pollute child inventories.
pub(super) fn isolate_launcher_descriptors() {
    for entry in std::fs::read_dir("/proc/self/fd").expect("launcher descriptors") {
        let raw: RawFd = entry
            .expect("launcher descriptor")
            .file_name()
            .to_str()
            .expect("descriptor number")
            .parse()
            .expect("descriptor number");
        if raw < 3 {
            continue;
        }
        // SAFETY: startup is single-threaded and nothing closes descriptors
        // during this enumeration. Borrowing does not take ownership of them.
        let fd = unsafe { BorrowedFd::borrow_raw(raw) };
        let flags =
            FdFlag::from_bits_retain(fcntl(fd, FcntlArg::F_GETFD).expect("descriptor flags"));
        fcntl(fd, FcntlArg::F_SETFD(flags | FdFlag::FD_CLOEXEC))
            .expect("launcher descriptor close-on-exec");
    }
}

fn print_descriptors() {
    for fd in std::fs::read_dir("/proc/self/fd").expect("child descriptors") {
        let target = std::fs::read_link(fd.expect("descriptor").path()).expect("descriptor target");
        println!("{}", target.display());
    }
}

pub(super) fn probe() -> bool {
    match std::env::args().nth(1).as_deref() {
        Some("--descriptor-probe") => {
            print_descriptors();
            true
        }
        Some("--detached-probe") => {
            // A new process group with no inherited output is deliberately outside
            // the completed leader's supervision. The caller owns its containment.
            #[expect(
                clippy::zombie_processes,
                reason = "The test host is a subreaper and waits for this orphan after cgroup teardown"
            )]
            let child = Command::new("/bin/sleep")
                .arg("60")
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .process_group(0)
                .spawn()
                .expect("redirected descendant");
            println!("{}", child.id());
            true
        }
        Some("--unrelated-probe") => {
            print_descriptors();
            println!("ready");
            let mut input = String::new();
            std::io::stdin()
                .read_line(&mut input)
                .expect("caller input");
            assert_eq!(input, "release\n");
            std::process::exit(17);
        }
        _ => false,
    }
}

pub(super) struct Boundary {
    root: PathBuf,
}

impl Boundary {
    pub(super) fn new() -> Self {
        // Opt-in is deliberate: never discover a host cgroup and mutate it.
        // CI supplies the root of a disposable container's private namespace.
        assert_eq!(
            std::env::var("SPACE_TEST_EXECUTION_PRIVATE_CGROUP_NS").as_deref(),
            Ok("1"),
            "set SPACE_TEST_EXECUTION_PRIVATE_CGROUP_NS=1 in a disposable private cgroup namespace",
        );
        assert_eq!(
            std::fs::read_to_string("/proc/self/cgroup").expect("host placement"),
            "0::/\n",
            "run in a private cgroup v2 namespace",
        );
        let root = Path::new("/sys/fs/cgroup").join(format!("embedding-{}", std::process::id()));
        // Exclusive creation means even failure cleanup can only touch our fixture.
        std::fs::create_dir(&root).expect("create owned cgroup");
        let boundary = Self { root };
        for leaf in ["workloads", "filesystem"] {
            std::fs::create_dir(boundary.root.join(leaf)).expect("create leaf cgroup");
        }
        boundary
    }

    fn members(&self, leaf: &str) -> Vec<u32> {
        let mut pids = std::fs::read_to_string(self.root.join(leaf).join("cgroup.procs"))
            .expect("cgroup members")
            .split_whitespace()
            .map(|pid| pid.parse().expect("pid"))
            .collect::<Vec<_>>();
        pids.sort_unstable();
        pids
    }

    fn placement(&self) -> Arc<ExecutionCgroups> {
        let open = |leaf: &str| -> File {
            let handle = OpenOptions::new()
                .write(true)
                .open(self.root.join(leaf).join("cgroup.procs"))
                .expect("placement descriptor");
            assert!(
                FdFlag::from_bits_retain(fcntl(&handle, FcntlArg::F_GETFD).unwrap())
                    .contains(FdFlag::FD_CLOEXEC),
                "caller must supply atomically close-on-exec placement handles"
            );
            handle
        };
        Arc::new(ExecutionCgroups::new(open("workloads"), open("filesystem")))
    }

    fn service(&self) -> ExecutionService {
        ExecutionService::with_cgroups(
            CancellationToken::new(),
            ExecutionServiceConfig {
                default_operation_user: GuestUserName::parse("nobody").expect("account"),
                worker_executable: Some(
                    WorkerExecutable::open(env!("CARGO_BIN_EXE_spaced-filesystem-worker"))
                        .expect("worker"),
                ),
                ..Default::default()
            },
            Some(self.placement()),
        )
        .expect("placed service")
    }

    pub(super) async fn run(&self, child_signals: &mut tokio::signal::unix::Signal) {
        let artifact = tempfile::tempdir().expect("probe directory");
        std::fs::set_permissions(artifact.path(), std::fs::Permissions::from_mode(0o755))
            .expect("searchable probe directory");
        let probe = artifact.path().join("probe");
        tokio::fs::copy(std::env::current_exe().expect("test executable"), &probe)
            .await
            .expect("probe artifact");
        descriptors::during_startup(self, &probe).await;
        let service = self.service();
        service
            .filesystem_service()
            .stat_info(tonic::Request::new(StatRequest {
                path: "/".into(),
                user: None,
            }))
            .await
            .expect("start identity worker");
        let workers = self.members("filesystem");
        assert_eq!(workers.len(), 1, "identity worker must use its own leaf");
        assert!(!workers.contains(&std::process::id()));

        // Keep a PTY live while an independent host spawner execs. Its child must
        // inherit neither the terminal nor writable placement/worker descriptors.
        let mut pty = service
            .start(tonic::Request::new(StartRequest {
                command: vec!["/bin/sh".into(), "-c".into(), "read marker".into()],
                cwd: Some("/".into()),
                pty: Some(PtyConfig { size: None }),
                idempotency_key: Some("placement-pty".into()),
                ..Default::default()
            }))
            .await
            .expect("PTY start")
            .into_inner();
        let started = pty
            .next()
            .await
            .expect("started")
            .expect("started response");
        assert_eq!(
            self.members("workloads"),
            [started.pid.expect("workload pid")],
            "workload placed before publication"
        );
        let mut unrelated = tokio::process::Command::new(&probe)
            .arg("--unrelated-probe")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .expect("unrelated caller child");
        let unrelated_pid = unrelated.id().expect("unrelated pid");
        let mut lines = BufReader::new(unrelated.stdout.take().expect("stdout")).lines();
        loop {
            let target = lines
                .next_line()
                .await
                .expect("descriptor line")
                .expect("ready marker");
            if target == "ready" {
                break;
            }
            descriptors::assert_isolated(&target);
        }
        assert!(!self.members("workloads").contains(&unrelated_pid));
        assert!(!self.members("filesystem").contains(&unrelated_pid));
        service
            .send_input(tonic::Request::new(SendInputRequest {
                process_id: started.process_id,
                data: b"continue\n".to_vec(),
            }))
            .await
            .expect("release PTY child");
        let mut exited = false;
        while let Some(event) = pty.next().await {
            if let Some(Event::Exit(exit)) = event.expect("PTY event").event {
                assert!(matches!(exit.outcome, Some(Outcome::Exited(exit)) if exit.exit_code == 0));
                exited = true;
            }
        }
        assert!(exited);

        // An exited child is still the caller's to reap. Keep its status pending
        // while the service drains its own children.
        while child_signals.recv().now_or_never() == Some(Some(())) {}
        let notifications = super::sigchld::notifications();
        let mut exited_child = Command::new("/bin/sh")
            .args(["-c", "exit 23"])
            .spawn()
            .expect("unrelated exited child");
        let exited_pid = exited_child.id();
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let status = tokio::fs::read_to_string(format!("/proc/{exited_pid}/status"))
                    .await
                    .expect("unreaped caller child");
                if status.lines().any(|line| line.starts_with("State:\tZ")) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("caller child exits before drain");
        tokio::time::timeout(Duration::from_secs(3), child_signals.recv())
            .await
            .expect("Tokio receives SIGCHLD")
            .expect("signal subscription remains open");
        assert!(
            super::sigchld::notifications() > notifications,
            "the caller's preinstalled SIGCHLD handler must still receive notifications"
        );

        // Only this scenario needs orphan adoption. Earlier startup and signal
        // checks must retain the embedding host's ordinary reparenting behavior.
        nix::sys::prctl::set_child_subreaper(true).expect("fixture subreaper");
        let mut events = service
            .start(tonic::Request::new(StartRequest {
                command: vec![probe.display().to_string(), "--detached-probe".into()],
                cwd: Some("/".into()),
                idempotency_key: Some("placement-descendant".into()),
                ..Default::default()
            }))
            .await
            .expect("start leader")
            .into_inner();
        let mut output = Vec::new();
        let mut exited = false;
        while let Some(event) = events.next().await {
            match event.expect("leader event").event {
                Some(Event::Stdout(chunk)) => output.extend(chunk.data),
                Some(Event::Exit(exit)) => {
                    assert!(
                        matches!(exit.outcome, Some(Outcome::Exited(exit)) if exit.exit_code == 0)
                    );
                    exited = true;
                }
                _ => {}
            }
        }
        assert!(exited);
        let descendant: u32 = std::str::from_utf8(&output)
            .expect("pid output")
            .trim()
            .parse()
            .expect("descendant pid");
        assert_eq!(
            self.members("workloads"),
            [descendant],
            "descendant inherits workload placement"
        );
        service.close();
        service.drain().await.expect("leaders and workers drained");
        assert!(self.members("filesystem").is_empty(), "worker exited");
        for pid in workers {
            assert_eq!(
                waitpid(
                    Pid::from_raw(i32::try_from(pid).expect("worker pid")),
                    Some(WaitPidFlag::WNOHANG)
                ),
                Err(nix::errno::Errno::ECHILD),
                "service must reap its worker",
            );
        }
        let descendant = Pid::from_raw(i32::try_from(descendant).expect("pid range"));
        assert_eq!(
            waitpid(descendant, Some(WaitPidFlag::WNOHANG)).expect("adopted descendant"),
            WaitStatus::StillAlive
        );
        assert!(
            unrelated
                .try_wait()
                .expect("caller retains child ownership")
                .is_none()
        );
        assert_eq!(
            tokio::task::spawn_blocking(move || exited_child.wait())
                .await
                .expect("caller reaper task")
                .expect("caller reaps exited child")
                .code(),
            Some(23)
        );
        // Only the caller's containment teardown promises to remove descendants.
        std::fs::write(self.root.join("workloads/cgroup.kill"), b"1").expect("caller teardown");
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                match waitpid(descendant, Some(WaitPidFlag::WNOHANG)).expect("reap descendant") {
                    WaitStatus::StillAlive => tokio::time::sleep(Duration::from_millis(10)).await,
                    WaitStatus::Signaled(_, signal, _) => {
                        assert_eq!(signal as i32, 9);
                        break;
                    }
                    other => panic!("unexpected descendant exit: {other:?}"),
                }
            }
        })
        .await
        .expect("descendant teardown deadline");
        nix::sys::prctl::set_child_subreaper(false).expect("restore ordinary reparenting");
        assert!(self.members("workloads").is_empty());
        assert_eq!(
            std::fs::read_to_string("/proc/self/cgroup").expect("host placement"),
            "0::/\n"
        );
        unrelated
            .stdin
            .take()
            .expect("caller input")
            .write_all(b"release\n")
            .await
            .expect("caller releases unrelated child");
        assert_eq!(
            unrelated
                .wait()
                .await
                .expect("caller reaps unrelated child")
                .code(),
            Some(17)
        );
    }
}

impl Drop for Boundary {
    fn drop(&mut self) {
        // Also clean the exclusively created subtree if an assertion unwinds.
        let _ = std::fs::write(self.root.join("cgroup.kill"), b"1");
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            for leaf in ["workloads", "filesystem"] {
                let _ = std::fs::remove_dir(self.root.join(leaf));
            }
            if std::fs::remove_dir(&self.root).is_ok() {
                break;
            }
            if Instant::now() >= deadline {
                eprintln!("failed to remove owned cgroup {}", self.root.display());
                assert!(std::thread::panicking(), "cgroup cleanup failed");
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}
