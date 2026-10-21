use std::collections::BTreeMap;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::process::CommandExt;

use futures_util::StreamExt;
use pplx_space_spaced_guest_execution::{
    CreateExecutionServiceError, ExecutionService, ExecutionServiceConfig,
    RunFilesystemWorkerError, WorkerExecutable,
};
use pplx_space_spaced_guest_protocol::proto::bridge_guest::{
    PtyConfig, StartRequest, process_event::Event, process_exit::Outcome, process_server::Process,
};
use rustix::thread::{CapabilitySet, CapabilitySets};
use tokio_util::sync::CancellationToken;

type Status = BTreeMap<String, String>;

pub(super) fn status(text: &str) -> Status {
    text.lines()
        .filter_map(|line| line.split_once(':'))
        .filter(|(key, _)| {
            matches!(
                *key,
                "Uid" | "Gid" | "Groups" | "CapInh" | "CapPrm" | "CapEff" | "CapAmb" | "NoNewPrivs"
            )
        })
        .map(|(key, value)| (key.to_owned(), value.trim().to_owned()))
        .collect()
}

pub(super) fn current_status() -> BTreeMap<u32, Status> {
    std::fs::read_dir("/proc/self/task")
        .expect("host threads")
        .filter_map(|entry| {
            let entry = entry.expect("thread");
            let tid = entry
                .file_name()
                .to_string_lossy()
                .parse()
                .expect("thread id");
            match std::fs::read_to_string(entry.path().join("status")) {
                Ok(text) => Some((tid, status(&text))),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => panic!("thread status: {error}"),
            }
        })
        .collect()
}

pub(super) fn assert_host_unchanged(before: &BTreeMap<u32, Status>) {
    let inherited = &before[&std::process::id()];
    for (tid, after) in current_status() {
        assert_eq!(
            &after,
            before.get(&tid).unwrap_or(inherited),
            "host thread {tid} authority must remain unchanged",
        );
    }
}

// Called before the runtime exists. Seed real inheritable and ambient authority
// so clearing them is exercised even when the container starts with empty sets.
pub(super) fn prepare_host() {
    let mut sets = rustix::thread::capabilities(None).expect("host capabilities");
    assert!(sets.permitted.contains(CapabilitySet::NET_RAW));
    sets.inheritable |= CapabilitySet::NET_RAW;
    rustix::thread::set_capabilities(None, sets).expect("host inheritable fixture");
    rustix::thread::configure_capability_in_ambient_set(CapabilitySet::NET_RAW, true)
        .expect("host ambient fixture");
}

pub(super) fn probe() -> bool {
    match std::env::args().nth(1).as_deref() {
        Some("--identity-probe") => {
            print!(
                "{}",
                std::fs::read_to_string("/proc/self/status").expect("child status")
            );
            true
        }
        Some("--threaded-worker-probe") => {
            use std::os::fd::IntoRawFd;
            let (_parent, child) = std::os::unix::net::UnixStream::pair().expect("worker socket");
            let fd = child.into_raw_fd();
            // SAFETY: this probe dispatches before creating any threads.
            unsafe {
                std::env::set_var("SPACE_SPACED_FILESYSTEM_WORKER_FD", fd.to_string());
            }
            let (stop, wait) = std::sync::mpsc::channel::<()>();
            let thread = std::thread::spawn(move || {
                if wait
                    .recv_timeout(std::time::Duration::from_secs(3))
                    .is_err()
                {
                    std::process::exit(90);
                }
            });
            let before = current_status();
            let error = pplx_space_spaced_guest_execution::run_filesystem_worker_from_environment()
                .expect_err("threaded worker must fail before changing credentials");
            assert!(
                matches!(
                    error,
                    RunFilesystemWorkerError::RequiresSingleThreadedStartup
                ),
                "{error}"
            );
            assert_host_unchanged(&before);
            stop.send(()).expect("stop companion thread");
            thread.join().expect("companion thread");
            // The rejected entrypoint has not taken ownership of the channel.
            nix::unistd::close(fd).expect("close worker channel");
            true
        }
        Some("--construction-probe") => {
            if nix::unistd::Uid::effective().is_root() {
                rustix::thread::set_capabilities(
                    None,
                    CapabilitySets {
                        effective: CapabilitySet::empty(),
                        permitted: CapabilitySet::empty(),
                        inheritable: CapabilitySet::empty(),
                    },
                )
                .expect("remove launcher privileges");
            }
            let result = ExecutionService::with_config(
                CancellationToken::new(),
                ExecutionServiceConfig {
                    worker_executable: Some(WorkerExecutable::open("/bin/true").expect("artifact")),
                    ..Default::default()
                },
            );
            assert!(matches!(
                result,
                Err(CreateExecutionServiceError::RestrictedRequiresPrivilege)
            ));
            true
        }
        _ => false,
    }
}

pub(super) fn assert_restricted(status: &Status, account: &nix::unistd::User) {
    for (field, id) in [("Uid", account.uid.as_raw()), ("Gid", account.gid.as_raw())] {
        let ids = status[field]
            .split_whitespace()
            .map(|id| id.parse::<u32>().expect("id"))
            .collect::<Vec<_>>();
        assert_eq!(
            ids,
            vec![id; 4],
            "{field}: real/effective/saved/filesystem identity"
        );
    }
    let name = std::ffi::CString::new(account.name.as_str()).expect("account name");
    let mut expected = nix::unistd::getgrouplist(&name, account.gid)
        .expect("account groups")
        .into_iter()
        .map(|gid| gid.as_raw())
        .collect::<Vec<_>>();
    let mut actual = status["Groups"]
        .split_whitespace()
        .map(|id| id.parse::<u32>().expect("group"))
        .collect::<Vec<_>>();
    expected.sort_unstable();
    expected.dedup();
    actual.sort_unstable();
    actual.dedup();
    assert_eq!(actual, expected, "supplementary groups");
    for field in ["CapInh", "CapPrm", "CapEff", "CapAmb"] {
        assert_eq!(
            u64::from_str_radix(&status[field], 16).expect("capabilities"),
            0,
            "{field}"
        );
    }
    assert_eq!(
        status["NoNewPrivs"], "1",
        "executable privilege gains must be disabled"
    );
}

async fn run_probe(
    service: &ExecutionService,
    executable: &std::path::Path,
    user: &str,
    pty: bool,
) -> Status {
    let mut events = Process::start(
        service,
        tonic::Request::new(StartRequest {
            command: vec![executable.display().to_string(), "--identity-probe".into()],
            cwd: Some("/".into()),
            pty: pty.then_some(PtyConfig { size: None }),
            user: Some(user.into()),
            idempotency_key: Some(format!(
                "identity-{}-{user}-{pty}",
                executable.file_name().expect("name").to_string_lossy()
            )),
            ..Default::default()
        }),
    )
    .await
    .expect("start identity probe")
    .into_inner();
    let mut output = Vec::new();
    let mut exited = false;
    while let Some(event) = events.next().await {
        match event.expect("probe event").event {
            Some(Event::Stdout(chunk)) => output.extend(chunk.data),
            Some(Event::Exit(exit)) => {
                assert!(matches!(exit.outcome, Some(Outcome::Exited(exit)) if exit.exit_code == 0));
                exited = true;
            }
            _ => {}
        }
    }
    assert!(exited);
    status(&String::from_utf8(output).expect("probe status"))
}

pub(super) async fn check_processes(service: &ExecutionService, account: &nix::unistd::User) {
    let directory = tempfile::tempdir().expect("probe fixtures");
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o755))
        .expect("searchable fixture directory");
    let root = nix::unistd::User::from_name("root")
        .expect("root lookup")
        .expect("root");
    for (name, mode) in [("plain", 0o755), ("set-id", 0o6755), ("file-caps", 0o755)] {
        let path = directory.path().join(name);
        std::fs::copy(std::env::current_exe().expect("test executable"), &path)
            .expect("copy probe");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).expect("probe mode");
        if name == "file-caps" {
            // Linux vfs_cap_data revision 2: effective flag, low permitted and
            // inheritable masks, then their high halves. CAP_NET_RAW is bit 13.
            let words = [0x0200_0001_u32, 1 << 13, 0, 0, 0];
            let value = words
                .into_iter()
                .flat_map(u32::to_le_bytes)
                .collect::<Vec<_>>();
            rustix::fs::setxattr(
                &path,
                "security.capability",
                &value,
                rustix::fs::XattrFlags::empty(),
            )
            .expect("file capability fixture");
        }
        if name != "plain" {
            // Prove the fixture can grant privilege; a nosuid mount or an
            // inherited no_new_privs must not turn this into a vacuous pass.
            let output = std::process::Command::new(&path)
                .arg("--identity-probe")
                .uid(account.uid.as_raw())
                .gid(account.gid.as_raw())
                .output()
                .expect("unrestricted probe");
            assert!(output.status.success());
            let state = status(&String::from_utf8(output.stdout).expect("probe status"));
            if name == "set-id" {
                assert_eq!(state["Uid"].split_whitespace().nth(1), Some("0"));
                assert_eq!(state["Gid"].split_whitespace().nth(1), Some("0"));
            } else {
                assert_ne!(
                    u64::from_str_radix(&state["CapEff"], 16).expect("capability mask") & (1 << 13),
                    0
                );
            }
        }
        for pty in [false, true] {
            assert_restricted(
                &run_probe(service, &path, &account.name, pty).await,
                account,
            );
        }
        let error = Process::start(
            service,
            tonic::Request::new(StartRequest {
                command: vec![path.display().to_string(), "--identity-probe".into()],
                user: Some(root.name.clone()),
                cwd: Some("/".into()),
                idempotency_key: Some(format!("reject-root-{name}")),
                ..Default::default()
            }),
        )
        .await
        .err()
        .expect("restricted root process must be rejected");
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        if name == "plain" {
            for user in [account, &root] {
                let result = std::process::Command::new(&path)
                    .arg("--construction-probe")
                    .uid(user.uid.as_raw())
                    .gid(user.gid.as_raw())
                    .status()
                    .expect("construction probe");
                assert!(
                    result.success(),
                    "unprivileged or capability-less launchers must fail"
                );
            }
        }
    }
}

// Entering a searchable leaf must not bypass its private ancestor.
pub(super) async fn check_cwd(service: &ExecutionService, account: &nix::unistd::User) {
    let private = tempfile::tempdir().expect("private ancestor");
    std::fs::set_permissions(private.path(), std::fs::Permissions::from_mode(0o700))
        .expect("private ancestor mode");
    let leaf = private.path().join("public");
    std::fs::create_dir(&leaf).expect("public leaf");
    std::fs::set_permissions(&leaf, std::fs::Permissions::from_mode(0o755)).expect("leaf mode");
    let error = Process::start(
        service,
        tonic::Request::new(StartRequest {
            command: vec!["/bin/pwd".into()],
            cwd: Some(leaf.display().to_string()),
            user: Some(account.name.clone()),
            idempotency_key: Some("private-ancestor".into()),
            ..Default::default()
        }),
    )
    .await
    .err()
    .expect("restricted cwd must respect ancestor permissions");
    assert_eq!(error.code(), tonic::Code::PermissionDenied);
}
