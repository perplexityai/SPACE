#![cfg(all(test, target_os = "linux"))]

//! The binary-level handoff contract of `space-nodectl-svc`: real processes
//! contend on the flock service mutex and take over the production socket by
//! atomic rename. Each test runs its instances in its own temporary
//! directory.
//!
//! Every oracle is state the kernel or the service exposes, never a log line:
//! the pid that `SO_PEERCRED` reports for a connection to a socket, the gRPC
//! health status answered on that same connection, the flock an instance
//! holds as listed in `/proc/<pid>/fdinfo`, the socket's inode, and exit
//! statuses. The instances' logs are attached to failures as evidence.
//!
//! Linux only: the oracles read Linux `/proc` and `SO_PEERCRED` semantics,
//! and CI runs the Rust tests on Linux.

use std::os::unix::fs::MetadataExt as _;
use std::os::unix::process::ExitStatusExt as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use hyper_util::rt::TokioIo;
use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;
use tonic::Code;
use tonic::transport::Endpoint;
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_client::HealthClient;

/// Upper bound on how long any awaited condition may take. Generous so a
/// loaded CI runner never fails a healthy handoff; a passing run polls its
/// way out in well under a second.
const WAIT_TIMEOUT: Duration = Duration::from_secs(30);
const POLL_INTERVAL: Duration = Duration::from_millis(10);
/// A probe that cannot finish in this long counts as unavailable.
const PROBE_TIMEOUT: Duration = Duration::from_millis(250);
/// The longest the production socket may go unanswered across a cooperative
/// handoff. The successor polls the mutex every 100 ms, then migrates an
/// already-migrated database and publishes its socket. Local debug-build
/// runs observed 0.10–0.11 s, and at most 0.17 s with every CPU saturated;
/// the margin absorbs a loaded runner.
const MAX_COOPERATIVE_UNAVAILABILITY: Duration = Duration::from_secs(2);

fn nodectl_binary() -> PathBuf {
    std::env::var_os("NEXTEST_BIN_EXE_space-nodectl-svc")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_BIN_EXE_space-nodectl-svc")))
}

/// One simulated node: the production socket, database and service mutex
/// that every instance started from it shares.
struct Node {
    dir: tempfile::TempDir,
    socket: PathBuf,
    db: PathBuf,
    mutex: PathBuf,
    socket_group: String,
}

impl Node {
    fn new() -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        // The production socket's group must be one this test's user can give
        // it without privilege: its own effective group.
        let socket_group = nix::unistd::Group::from_gid(nix::unistd::getegid())
            .expect("look up the effective group")
            .expect("the effective group has a name")
            .name;
        // The shared run directory, which a deployment prepares before node-ctl
        // starts: the production socket and, beside it, the service mutex.
        let run = dir.path().join("run");
        std::fs::create_dir(&run).expect("create the run directory");
        Self {
            socket: run.join("node-ctl.sock"),
            db: dir.path().join("node-ctl.db"),
            mutex: run.join("service.mutex"),
            socket_group,
            dir,
        }
    }

    fn start(&self, name: &str) -> Instance {
        self.start_with(name, &self.db, &[])
    }

    fn start_with(&self, name: &str, db: &Path, extra_args: &[&str]) -> Instance {
        let health = self.dir.path().join(format!("{name}-health.sock"));
        let log = self.dir.path().join(format!("{name}.log"));
        let log_file = std::fs::File::create(&log).expect("create instance log");
        // Taken before the spawn, so the instance's own clock starts later.
        let started_at = Instant::now();
        let child = Command::new(nodectl_binary())
            // Nothing from the developer's or runner's environment reaches the
            // instance; the cluster endpoint stays unset, so facet syncs have
            // nothing to call.
            .env_clear()
            .env("SPACE_NODE_ID", "handoff-test-node")
            .arg("--config")
            .arg(self.dir.path().join("absent-nodectl.toml"))
            .arg("--socket")
            .arg(&self.socket)
            .arg("--db")
            .arg(db)
            .arg("--socket-group")
            .arg(&self.socket_group)
            .arg("--health-socket")
            .arg(&health)
            .args(["--poll-interval-secs", "60"])
            .args(extra_args)
            .stdin(Stdio::null())
            .stdout(log_file.try_clone().expect("clone instance log"))
            .stderr(log_file)
            .spawn()
            .expect("spawn space-nodectl-svc");
        Instance {
            name: name.to_string(),
            child,
            health,
            started_at,
        }
    }

    /// Every instance's log, for a failure message.
    fn evidence(&self) -> String {
        let mut logs = std::fs::read_dir(self.dir.path())
            .map(|entries| {
                entries
                    .filter_map(|entry| entry.ok().map(|entry| entry.path()))
                    .filter(|path| path.extension().is_some_and(|ext| ext == "log"))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        logs.sort();
        logs.iter()
            .map(|path| {
                format!(
                    "--- {} ---\n{}",
                    path.display(),
                    std::fs::read_to_string(path).unwrap_or_default()
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    async fn wait_for<T, F, Fut>(&self, what: &str, mut check: F) -> T
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Option<T>>,
    {
        let deadline = Instant::now() + WAIT_TIMEOUT;
        loop {
            if let Some(value) = check().await {
                return value;
            }
            assert!(
                Instant::now() < deadline,
                "timed out after {WAIT_TIMEOUT:?} waiting for {what}\n{}",
                self.evidence()
            );
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    /// Waits until `instance` answers health checks on the production socket.
    async fn wait_active(&self, instance: &Instance) {
        let pid = instance.pid();
        self.wait_for(
            &format!("{} to serve the production socket", instance.name),
            || async move {
                answering_pid(&self.socket)
                    .await
                    .filter(|served| *served == pid)
            },
        )
        .await;
    }

    /// Waits until `instance` answers health checks on its own health socket,
    /// which it binds before contending for the mutex.
    async fn wait_passive_ready(&self, instance: &Instance) {
        let pid = instance.pid();
        self.wait_for(
            &format!("{} to report ready on its health socket", instance.name),
            || async move {
                answering_pid(&instance.health)
                    .await
                    .filter(|served| *served == pid)
            },
        )
        .await;
    }

    async fn wait_exit(&self, instance: &mut Instance) -> ExitStatus {
        let what = format!("{} to exit", instance.name);
        let child = &mut instance.child;
        self.wait_for(&what, || {
            let status = child.try_wait().expect("poll instance");
            async move { status }
        })
        .await
    }

    fn socket_inode(&self) -> u64 {
        std::fs::symlink_metadata(&self.socket)
            .expect("stat the production socket")
            .ino()
    }

    fn assert_holds_mutex(&self, instance: &Instance, expected: bool) {
        assert_eq!(
            holds_flock(instance.pid(), &self.mutex),
            expected,
            "{} holding the service mutex\n{}",
            instance.name,
            self.evidence()
        );
    }
}

/// A running `space-nodectl-svc`, killed when dropped.
struct Instance {
    name: String,
    child: Child,
    health: PathBuf,
    started_at: Instant,
}

impl Instance {
    fn pid(&self) -> u32 {
        self.child.id()
    }

    fn signal(&self, signal: Signal) {
        kill(self.nix_pid(), signal).expect("signal instance");
    }

    fn nix_pid(&self) -> Pid {
        Pid::from_raw(i32::try_from(self.pid()).expect("pid fits in i32"))
    }

    fn is_running(&mut self) -> bool {
        self.child.try_wait().expect("poll instance").is_none()
    }
}

impl Drop for Instance {
    fn drop(&mut self) {
        if self.is_running() {
            // A stopped process takes SIGKILL too; resuming it first keeps
            // the reap below from depending on that.
            let _ = kill(self.nix_pid(), Signal::SIGCONT);
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

/// The pid of the process answering health checks on `socket`, read from the
/// listening socket's `SO_PEERCRED` on the connection that carries the check,
/// or `None` while nothing answers. A process answers when it reports SERVING
/// or when its rate limiter rejects the check: the production socket limits
/// health checks per peer, and only a running server can reject one.
async fn answering_pid(socket: &Path) -> Option<u32> {
    let stream = tokio::net::UnixStream::connect(socket).await.ok()?;
    let pid = u32::try_from(stream.peer_cred().ok()?.pid()?).ok()?;
    let stream = Arc::new(Mutex::new(Some(stream)));
    let endpoint = Endpoint::from_static("http://localhost");
    let channel = endpoint.connect_with_connector(tower::service_fn(move |_| {
        let stream = stream.lock().expect("probe stream lock").take();
        async move {
            stream
                .map(TokioIo::new)
                .ok_or_else(|| std::io::Error::other("probe connection already used"))
        }
    }));
    let answered = tokio::time::timeout(PROBE_TIMEOUT, async {
        let check = HealthClient::new(channel.await.ok()?)
            .check(HealthCheckRequest::default())
            .await;
        Some(match check {
            Ok(response) => response.into_inner().status() == ServingStatus::Serving,
            Err(status) => status.code() == Code::ResourceExhausted,
        })
    })
    .await
    .ok()??;
    answered.then_some(pid)
}

/// Whether process `pid` holds an exclusive flock on `path`. The kernel lists
/// a lock in the fdinfo of the descriptor whose open file owns it, so a
/// contender that has merely opened the mutex file shows none.
fn holds_flock(pid: u32, path: &Path) -> bool {
    let path = path.canonicalize().expect("canonicalize the mutex path");
    let Ok(fds) = std::fs::read_dir(format!("/proc/{pid}/fd")) else {
        return false;
    };
    fds.filter_map(Result::ok).any(|fd| {
        std::fs::read_link(fd.path()).is_ok_and(|target| target == path)
            && std::fs::read_to_string(format!(
                "/proc/{pid}/fdinfo/{}",
                fd.file_name().to_string_lossy()
            ))
            .is_ok_and(|info| {
                info.lines().any(|line| {
                    line.starts_with("lock:") && line.contains("FLOCK") && line.contains("WRITE")
                })
            })
    })
}

/// One probe of the production socket during a handoff.
struct Sample {
    at: Instant,
    answering_pid: Option<u32>,
}

/// Probes the production socket continuously until stopped.
struct ProbeLoop {
    stop: Arc<AtomicBool>,
    samples: Arc<Mutex<Vec<Sample>>>,
    task: tokio::task::JoinHandle<()>,
}

impl ProbeLoop {
    fn start(socket: &Path) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let samples = Arc::new(Mutex::new(Vec::new()));
        let task = tokio::spawn({
            let socket = socket.to_path_buf();
            let stop = Arc::clone(&stop);
            let samples = Arc::clone(&samples);
            async move {
                while !stop.load(Ordering::SeqCst) {
                    let answering_pid = answering_pid(&socket).await;
                    samples.lock().expect("samples lock").push(Sample {
                        at: Instant::now(),
                        answering_pid,
                    });
                    tokio::time::sleep(POLL_INTERVAL).await;
                }
            }
        });
        Self {
            stop,
            samples,
            task,
        }
    }

    fn saw(&self, pid: u32) -> Option<()> {
        self.samples
            .lock()
            .expect("samples lock")
            .iter()
            .any(|sample| sample.answering_pid == Some(pid))
            .then_some(())
    }

    async fn finish(self) -> Vec<Sample> {
        self.stop.store(true, Ordering::SeqCst);
        self.task.await.expect("probe loop");
        std::mem::take(&mut *self.samples.lock().expect("samples lock"))
    }
}

#[tokio::test]
async fn cooperative_handoff_moves_the_socket_to_the_passive_instance() {
    let node = Node::new();
    let mut a = node.start("a");
    node.wait_active(&a).await;
    let mut b = node.start("b");
    node.wait_passive_ready(&b).await;
    node.assert_holds_mutex(&a, true);
    node.assert_holds_mutex(&b, false);
    node.wait_active(&a).await;
    let predecessor_socket = node.socket_inode();

    let probes = ProbeLoop::start(&node.socket);
    let probing = &probes;
    let (a_pid, b_pid) = (a.pid(), b.pid());
    node.wait_for(
        "the probe loop to reach a",
        || async move { probing.saw(a_pid) },
    )
    .await;
    let sigterm_at = Instant::now();
    a.signal(Signal::SIGTERM);
    node.wait_for("b to serve the production socket", || async move {
        probing.saw(b_pid)
    })
    .await;
    let serving_after = sigterm_at.elapsed();
    let samples = probes.finish().await;

    let a_status = node.wait_exit(&mut a).await;
    assert!(
        a_status.success(),
        "a exited {a_status} on SIGTERM\n{}",
        node.evidence()
    );
    node.assert_holds_mutex(&b, true);
    assert_ne!(
        node.socket_inode(),
        predecessor_socket,
        "b must publish its own socket"
    );
    assert!(b.is_running());

    // Every answer came from a until b's first, and only from b after it.
    let answered = samples
        .iter()
        .filter_map(|sample| sample.answering_pid.map(|pid| (sample.at, pid)))
        .collect::<Vec<_>>();
    let first_b = answered
        .iter()
        .position(|(_, pid)| *pid == b_pid)
        .expect("b answered");
    let (before_b, from_b) = answered.split_at(first_b);
    assert!(
        before_b.iter().all(|(_, pid)| *pid == a_pid)
            && from_b.iter().all(|(_, pid)| *pid == b_pid),
        "the socket's answering pids must go from a ({a_pid}) to b ({b_pid}) exactly once: {:?}",
        answered.iter().map(|(_, pid)| pid).collect::<Vec<_>>()
    );
    let (last_a, _) = before_b.last().expect("a answered before the handoff");
    let (first_b_at, _) = from_b.first().expect("b answered");
    let unavailable = first_b_at.duration_since(*last_a);
    let failed = samples
        .iter()
        .filter(|sample| sample.answering_pid.is_none())
        .count();
    eprintln!(
        "cooperative handoff: SIGTERM to b serving {serving_after:?}; \
         unavailable {unavailable:?} across {failed} failed probes"
    );
    assert!(
        unavailable <= MAX_COOPERATIVE_UNAVAILABILITY,
        "the socket went unanswered for {unavailable:?}, more than \
         {MAX_COOPERATIVE_UNAVAILABILITY:?}\n{}",
        node.evidence()
    );
}

#[tokio::test]
async fn crash_handoff_follows_the_kernel_releasing_the_mutex() {
    let node = Node::new();
    let mut a = node.start("a");
    node.wait_active(&a).await;
    let b = node.start("b");
    node.wait_passive_ready(&b).await;
    node.assert_holds_mutex(&b, false);
    let predecessor_socket = node.socket_inode();

    let sigkill_at = Instant::now();
    a.signal(Signal::SIGKILL);
    node.wait_active(&b).await;
    eprintln!(
        "crash handoff: SIGKILL to b serving {:?}",
        sigkill_at.elapsed()
    );

    let a_status = node.wait_exit(&mut a).await;
    assert_eq!(a_status.signal(), Some(Signal::SIGKILL as i32));
    node.assert_holds_mutex(&b, true);
    assert_ne!(
        node.socket_inode(),
        predecessor_socket,
        "b must replace the dead predecessor's socket"
    );
}

#[tokio::test]
async fn exactly_one_contender_activates_when_the_holder_exits() {
    let node = Node::new();
    let mut a = node.start("a");
    node.wait_active(&a).await;
    let mut b = node.start("b");
    let mut c = node.start("c");
    node.wait_passive_ready(&b).await;
    node.wait_passive_ready(&c).await;
    node.assert_holds_mutex(&b, false);
    node.assert_holds_mutex(&c, false);

    a.signal(Signal::SIGTERM);
    let contenders = [b.pid(), c.pid()];
    let socket = node.socket.as_path();
    let winner = node
        .wait_for(
            "a contender to serve the production socket",
            || async move {
                answering_pid(socket)
                    .await
                    .filter(|pid| contenders.contains(pid))
            },
        )
        .await;
    let a_status = node.wait_exit(&mut a).await;
    assert!(
        a_status.success(),
        "a exited {a_status} on SIGTERM\n{}",
        node.evidence()
    );

    let (winner, loser) = if winner == b.pid() {
        (&mut b, &mut c)
    } else {
        (&mut c, &mut b)
    };
    node.assert_holds_mutex(winner, true);
    node.assert_holds_mutex(loser, false);
    assert!(
        loser.is_running(),
        "{} must stay passive, not exit",
        loser.name
    );
    // The loser stays ready to take over, and the winner keeps the socket.
    node.wait_passive_ready(loser).await;
    node.wait_active(winner).await;
}

#[tokio::test]
async fn passive_instance_exits_cleanly_on_sigterm() {
    let node = Node::new();
    let a = node.start("a");
    node.wait_active(&a).await;
    let active_socket = node.socket_inode();
    // b gets a database path of its own, so the file's absence afterwards
    // shows that a passive instance never opens its database.
    let b_db = node.dir.path().join("b-node-ctl.db");
    let mut b = node.start_with("b", &b_db, &[]);
    node.wait_passive_ready(&b).await;
    node.assert_holds_mutex(&b, false);

    b.signal(Signal::SIGTERM);
    let b_status = node.wait_exit(&mut b).await;
    assert_eq!(
        b_status.code(),
        Some(0),
        "b exited {b_status}\n{}",
        node.evidence()
    );
    assert!(
        !b_db.exists(),
        "a passive instance must not create its database"
    );
    assert_eq!(
        node.socket_inode(),
        active_socket,
        "b must not touch the socket"
    );
    node.wait_active(&a).await;
    node.assert_holds_mutex(&a, true);
}

#[tokio::test]
async fn successor_fails_visibly_behind_a_stuck_predecessor() {
    const MUTEX_BUDGET: Duration = Duration::from_secs(2);

    let node = Node::new();
    let a = node.start("a");
    node.wait_active(&a).await;
    let active_socket = node.socket_inode();
    a.signal(Signal::SIGSTOP);

    let mut b = node.start_with(
        "b",
        &node.db,
        &[
            "--service-mutex-timeout-secs",
            &MUTEX_BUDGET.as_secs().to_string(),
        ],
    );
    // Exiting no sooner than its budget shows the mutex wait decided b's
    // exit, not an earlier startup failure. `started_at` precedes the spawn,
    // so the measured time cannot fall short of b's own.
    let b_status = node.wait_exit(&mut b).await;
    let b_waited = b.started_at.elapsed();
    assert!(
        b_status.code().is_some_and(|code| code != 0),
        "b must exit non-zero behind a stuck predecessor, got {b_status}\n{}",
        node.evidence()
    );
    assert!(
        b_waited >= MUTEX_BUDGET,
        "b exited after {b_waited:?}, before its {MUTEX_BUDGET:?} mutex budget"
    );
    node.assert_holds_mutex(&a, true);
    assert_eq!(
        node.socket_inode(),
        active_socket,
        "b must not publish a socket"
    );

    a.signal(Signal::SIGCONT);
    node.wait_active(&a).await;
}
