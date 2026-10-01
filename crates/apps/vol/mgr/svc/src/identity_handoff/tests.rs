use super::*;
use std::os::unix::fs::symlink;
use std::os::unix::net::UnixListener;
use std::path::PathBuf;

// Parallel tests spawn children that briefly inherit locked FDs before exec.
const HANDOFF_TIMEOUT: Duration = Duration::from_secs(10);

fn rejected(result: anyhow::Result<()>) -> String {
    let error = format!("{:#}", result.unwrap_err());
    assert!(!error.contains("still owns service.mutex"), "{error}");
    error
}

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        // SAFETY: geteuid takes no arguments and only reads this process's identity.
        assert_eq!(unsafe { libc::geteuid() }, 0, "run handoff tests as root");
        let root =
            std::env::temp_dir().join(format!("space-identity-test-{}", uuid::Uuid::now_v7()));
        let fixture = Self(root);
        for path in ["run/volume-manager", "cache/firecracker/.volmgr-staging"] {
            std::fs::create_dir_all(fixture.0.join(path)).unwrap();
            set_owner(&File::open(fixture.0.join(path)).unwrap(), 950, 960, 0o750).unwrap();
        }
        for (name, bytes) in [
            ("service.mutex", b"mutex".as_slice()),
            ("volume-manager.db", b"database".as_slice()),
            ("volume-manager.db-wal", b"committed-wal".as_slice()),
            ("volume-manager.db-shm", b"shared-memory".as_slice()),
        ] {
            fixture.file(&format!("run/volume-manager/{name}"), bytes);
        }
        fixture.file(
            "cache/firecracker/.volmgr-staging/01980000-0000-7000-8000-000000000001",
            b"queued upload",
        );
        for name in [
            "service.mutex",
            "volume-manager.sock",
            "01980000-0000-7000-8000-000000000001.zst.tmp",
            "01980000-0000-7000-8000-000000000001.enc.tmp",
            "01980000-0000-7000-8000-000000000001.plaintext.tmp",
            "sparse-shadow-source-01980000-0000-7000-8000-000000000001.tmp",
            "source.sparse-shadow-01980000-0000-7000-8000-000000000001.tmp",
        ] {
            fixture.file(
                &format!("cache/firecracker/.volmgr-staging/{name}"),
                b"retained codec temporary",
            );
        }
        fixture
    }
    fn file(&self, path: &str, bytes: &[u8]) {
        std::fs::write(self.0.join(path), bytes).unwrap();
        set_owner(&File::open(self.0.join(path)).unwrap(), 950, 960, 0o600).unwrap();
    }
    fn snapshot(&self) -> Vec<(PathBuf, u64, u32, u32, u32, Vec<u8>)> {
        ["run/volume-manager", "cache/firecracker/.volmgr-staging"]
            .into_iter()
            .flat_map(|dir| std::fs::read_dir(self.0.join(dir)).unwrap())
            .map(|entry| {
                let path = entry.unwrap().path();
                let m = std::fs::metadata(&path).unwrap();
                let bytes = if m.is_file() {
                    std::fs::read(&path).unwrap()
                } else {
                    vec![]
                };
                (path, m.ino(), m.uid(), m.gid(), m.mode(), bytes)
            })
            .collect()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

#[test]
#[ignore = "requires root to exercise service identity handoff"]
fn retained_state_round_trips_without_replacing_mutex_or_bytes() {
    let fresh = Fixture::new();
    std::fs::remove_dir_all(fresh.0.join("run/volume-manager")).unwrap();
    std::fs::remove_dir_all(fresh.0.join("cache/firecracker/.volmgr-staging")).unwrap();
    prepare(&fresh.0, Identity::Separate, HANDOFF_TIMEOUT).unwrap();
    assert_eq!(
        std::fs::metadata(fresh.0.join("run/volume-manager/service.mutex"))
            .unwrap()
            .uid(),
        964
    );
    let f = Fixture::new();
    let _socket = UnixListener::bind(f.0.join("run/volume-manager/volume-manager.sock")).unwrap();
    let before = f.snapshot();
    for (identity, uid, gid) in [
        (Identity::Separate, 964, 964),
        (Identity::Separate, 964, 964),
        (Identity::Legacy, 950, 960),
    ] {
        prepare(&f.0, identity, HANDOFF_TIMEOUT).unwrap();
        for (path, inode, _, _, _, bytes) in &before {
            let m = std::fs::metadata(path).unwrap();
            assert_eq!(m.ino(), *inode, "inode changed: {path:?}");
            if m.is_file() {
                assert_eq!(std::fs::read(path).unwrap(), *bytes);
                assert_eq!(m.uid(), uid);
                let expected_gid = if path.starts_with(f.0.join("cache")) {
                    groups::SPACE_CACHE_RW.gid
                } else {
                    gid
                };
                assert_eq!(m.gid(), expected_gid);
                assert_eq!(
                    m.mode() & 0o777,
                    if path == &f.0.join("run/volume-manager/service.mutex") {
                        0o660
                    } else {
                        0o600
                    }
                );
            }
        }
        if uid == 964 {
            // A crash after transferring all files but before publishing leaf
            // ownership can safely resume using the requested target identity.
            freeze(&File::open(f.0.join("run/volume-manager")).unwrap()).unwrap();
            freeze(&File::open(f.0.join("cache/firecracker/.volmgr-staging")).unwrap()).unwrap();
            prepare(&f.0, Identity::Separate, HANDOFF_TIMEOUT).unwrap();
        }
        let run = std::fs::metadata(f.0.join("run/volume-manager")).unwrap();
        assert_eq!(
            (run.uid(), run.gid(), run.mode() & 0o777),
            (uid, groups::SPACE_SBXCLIENTS.gid, 0o750)
        );
    }
}

#[test]
#[ignore = "requires root to exercise service identity handoff"]
fn abandoned_socket_replacement_does_not_block_handoff_or_restart() {
    use std::os::unix::process::ExitStatusExt;
    const CHILD: &str = "SPACE_SOCKET_BIND_CRASH_TEST";
    if let Some(socket) = std::env::var_os(CHILD) {
        let no_core = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        // SAFETY: no_core is a live rlimit; only this disposable child's limits change.
        assert_eq!(unsafe { libc::setrlimit(libc::RLIMIT_CORE, &no_core) }, 0);
        // Kill only this child on the real binder's publish syscall, after bind
        // and chmod. No sleeps or racing a watcher against rename are needed.
        let jump = |syscall, offset| libc::sock_filter {
            code: (libc::BPF_JMP | libc::BPF_JEQ | libc::BPF_K) as u16,
            jt: offset,
            jf: 0,
            k: u32::try_from(syscall).unwrap(),
        };
        let filter = [
            libc::sock_filter {
                code: (libc::BPF_LD | libc::BPF_W | libc::BPF_ABS) as u16,
                jt: 0,
                jf: 0,
                k: 0,
            },
            jump(libc::SYS_rename, 3),
            jump(libc::SYS_renameat, 2),
            jump(libc::SYS_renameat2, 1),
            libc::sock_filter {
                code: (libc::BPF_RET | libc::BPF_K) as u16,
                jt: 0,
                jf: 0,
                k: libc::SECCOMP_RET_ALLOW,
            },
            libc::sock_filter {
                code: (libc::BPF_RET | libc::BPF_K) as u16,
                jt: 0,
                jf: 0,
                k: libc::SECCOMP_RET_KILL_PROCESS,
            },
        ];
        let program = libc::sock_fprog {
            len: filter.len() as u16,
            filter: filter.as_ptr() as *mut _,
        };
        assert_eq!(
            // SAFETY: PR_SET_NO_NEW_PRIVS accepts these scalar arguments.
            unsafe { libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) },
            0
        );
        assert_eq!(
            // SAFETY: program and its filter remain live while the kernel copies them.
            unsafe { libc::prctl(libc::PR_SET_SECCOMP, libc::SECCOMP_MODE_FILTER, &program) },
            0
        );
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .build()
            .unwrap();
        let _guard = runtime.enter();
        pplx_space_common_svc::bind_unix_socket(
            Path::new(&socket),
            pplx_space_common_svc::UnixSocketOptions::new(0o660),
        )
        .unwrap();
        panic!("socket replacement must reach the injected crash");
    }
    for (source, target) in [
        (Identity::Legacy, Identity::Separate),
        (Identity::Separate, Identity::Separate),
        (Identity::Separate, Identity::Legacy),
        (Identity::Legacy, Identity::Legacy),
    ] {
        let f = Fixture::new();
        prepare(&f.0, source, HANDOFF_TIMEOUT).unwrap();
        let socket = f.0.join("run/volume-manager/volume-manager.sock");
        drop(UnixListener::bind(&socket).unwrap());
        let (uid, gid) = source.ids();
        std::os::unix::fs::chown(&socket, Some(uid), Some(gid)).unwrap();
        let before = f.snapshot();
        let status = std::process::Command::new("setpriv")
            .args([format!("--reuid={uid}"), format!("--regid={gid}"), "--clear-groups".into(), "--".into()])
            .arg(std::env::current_exe().unwrap())
            .args(["--exact", "identity_handoff::tests::abandoned_socket_replacement_does_not_block_handoff_or_restart", "--include-ignored", "--nocapture"])
            .env(CHILD, &socket).status().unwrap();
        assert_eq!(status.signal(), Some(libc::SIGSYS));
        assert_eq!(
            f.snapshot().len(),
            before.len() + 1,
            "real binder must leave an orphan"
        );
        prepare(&f.0, target, HANDOFF_TIMEOUT).unwrap();
        assert_eq!(f.snapshot().len(), before.len());
        for (path, inode, _, _, mode, bytes) in before {
            assert_eq!(std::fs::metadata(&path).unwrap().ino(), inode);
            if mode & libc::S_IFMT == libc::S_IFREG {
                assert_eq!(std::fs::read(path).unwrap(), bytes);
            }
        }
    }
}

#[test]
#[ignore = "requires root to exercise service identity handoff"]
fn hot_sqlite_journal_survives_handoff_and_recovers_committed_rows() {
    const CHILD: &str = "SPACE_SQLITE_HANDOFF_TEST";
    const VERIFY: &str = "SPACE_SQLITE_HANDOFF_VERIFY";
    if let Some(database) = std::env::var_os(CHILD) {
        let conn = rusqlite::Connection::open(database).unwrap();
        if std::env::var_os(VERIFY).is_some() {
            let recovered: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM retained WHERE length(payload) = 4096",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(recovered, 32);
            return;
        }
        conn.execute_batch(
            "PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL; PRAGMA cache_size=2;
             CREATE TABLE retained (id INTEGER PRIMARY KEY, payload BLOB NOT NULL);
             BEGIN IMMEDIATE;",
        )
        .unwrap();
        for id in 0..32 {
            conn.execute("INSERT INTO retained VALUES (?1, zeroblob(4096))", [id])
                .unwrap();
        }
        conn.execute_batch(
            "COMMIT; BEGIN IMMEDIATE; UPDATE retained SET payload = zeroblob(8192);",
        )
        .unwrap();
        // Skip connection/transaction Drop, retaining SQLite's real hot journal
        // after dirty pages have spilled through the deliberately small cache.
        std::process::exit(0);
    }
    for (source, target) in [
        (Identity::Legacy, Identity::Separate),
        (Identity::Separate, Identity::Separate),
        (Identity::Separate, Identity::Legacy),
        (Identity::Legacy, Identity::Legacy),
    ] {
        let f = Fixture::new();
        prepare(&f.0, source, HANDOFF_TIMEOUT).unwrap();
        for name in [
            "volume-manager.db",
            "volume-manager.db-wal",
            "volume-manager.db-shm",
        ] {
            std::fs::remove_file(f.0.join("run/volume-manager").join(name)).unwrap();
        }
        let database = f.0.join("run/volume-manager/volume-manager.db");
        let child = |identity: Identity, verify: bool| {
            let (uid, gid) = identity.ids();
            let mut command = std::process::Command::new("setpriv");
            command.args([format!("--reuid={uid}"), format!("--regid={gid}"), "--clear-groups".into(), "--".into()])
                .arg(std::env::current_exe().unwrap())
                .args(["--exact", "identity_handoff::tests::hot_sqlite_journal_survives_handoff_and_recovers_committed_rows", "--include-ignored", "--nocapture"])
                .env(CHILD, &database);
            if verify {
                command.env(VERIFY, "1");
            }
            assert!(command.status().unwrap().success());
        };
        child(source, false);
        let journal = database.with_file_name("volume-manager.db-journal");
        let journal_bytes = std::fs::read(&journal).unwrap();
        assert!(journal_bytes.len() > 512);
        assert_ne!(&journal_bytes[..8], &[0; 8], "journal header must be hot");
        let before = f.snapshot();
        prepare(&f.0, target, HANDOFF_TIMEOUT).unwrap();
        for (path, inode, _, _, _, bytes) in before {
            let metadata = std::fs::metadata(&path).unwrap();
            assert_eq!(metadata.ino(), inode);
            assert_eq!(metadata.uid(), target.ids().0);
            assert_eq!(std::fs::read(&path).unwrap(), bytes);
        }
        child(target, true);
        assert!(!journal.exists(), "SQLite must finish journal recovery");
    }
}

#[test]
#[ignore = "requires root to exercise service identity handoff"]
fn an_absent_staging_directory_allows_upgrade_and_rollback() {
    let f = Fixture::new();
    set_owner(
        &File::open(f.0.join("cache/firecracker")).unwrap(),
        0,
        952,
        0o2775,
    )
    .unwrap();
    let staging = f.0.join("cache/firecracker/.volmgr-staging");
    let mutex = f.0.join("run/volume-manager/service.mutex");
    let inode = std::fs::metadata(&mutex).unwrap().ino();
    for (identity, uid) in [(Identity::Separate, 964), (Identity::Legacy, 950)] {
        std::fs::remove_dir_all(&staging).unwrap();
        prepare(&f.0, identity, HANDOFF_TIMEOUT).unwrap();
        assert_eq!(std::fs::metadata(&staging).unwrap().uid(), uid);
        let metadata = std::fs::metadata(&mutex).unwrap();
        assert_eq!((metadata.ino(), metadata.uid()), (inode, uid));
    }
}

#[test]
#[ignore = "requires root to exercise service identity handoff"]
fn nonempty_root_owned_staging_must_match_sealed_state() {
    for (gid, mode) in [(952, 0o2775), (952, 0o700), (0, 0o775), (0, 0o750)] {
        let f = Fixture::new();
        let staging = File::open(f.0.join("cache/firecracker/.volmgr-staging")).unwrap();
        set_owner(&staging, 0, gid, mode).unwrap();
        let before = f.snapshot();
        for _ in 0..2 {
            assert!(
                rejected(prepare(&f.0, Identity::Legacy, HANDOFF_TIMEOUT))
                    .contains("root-owned staging directory was not sealed")
            );
            assert_eq!(f.snapshot(), before);
            let metadata = staging.metadata().unwrap();
            assert_eq!(
                (metadata.uid(), metadata.gid(), metadata.mode() & 0o7777),
                (0, gid, mode)
            );
        }
    }
}

#[test]
#[ignore = "requires root to exercise service identity handoff"]
fn a_live_predecessor_blocks_all_ownership_changes() {
    let f = Fixture::new();
    let before = f.snapshot();
    let mutex = File::open(f.0.join("run/volume-manager/service.mutex")).unwrap();
    assert_eq!(
        // SAFETY: mutex owns the descriptor for the duration of the lock.
        unsafe { libc::flock(mutex.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
        0
    );
    let error = prepare(&f.0, Identity::Separate, Duration::ZERO).unwrap_err();
    assert!(format!("{error:#}").contains("still owns service.mutex"));
    assert_eq!(f.snapshot(), before);
    assert_eq!(
        std::fs::metadata(f.0.join("run/volume-manager"))
            .unwrap()
            .uid(),
        950
    );
    drop(mutex);
    prepare(&f.0, Identity::Separate, HANDOFF_TIMEOUT).unwrap();
}

#[test]
#[ignore = "requires root to exercise service identity handoff"]
fn unsafe_retained_entries_fail_without_reowning_external_data() {
    for variant in [
        "symlink",
        "internal-symlink",
        "hardlink",
        "foreign-owner",
        "root-mutex",
        "writable-runtime-parent",
        "writable-node-root",
        "directory",
        "foreign-name",
        "mutex-link",
        "staging-link",
        "staged-socket-owner",
        "staged-socket-regular",
        "staged-socket-name",
        "staged-socket-hardlink",
        "staged-socket-symlink",
    ] {
        let f = Fixture::new();
        let external = f.0.join("unrelated");
        std::fs::write(&external, b"outside state").unwrap();
        set_owner(&File::open(&external).unwrap(), 950, 960, 0o600).unwrap();
        let external_before = std::fs::metadata(&external).unwrap();
        let target = f.0.join("run/volume-manager/volume-manager.db");
        match variant {
            "symlink" => {
                std::fs::remove_file(&target).unwrap();
                symlink(&external, &target).unwrap();
            }
            "internal-symlink" => {
                std::fs::remove_file(&target).unwrap();
                symlink("volume-manager.db-wal", &target).unwrap();
            }
            "hardlink" => {
                std::fs::remove_file(&target).unwrap();
                std::fs::hard_link(&external, &target).unwrap();
                set_owner(&File::open(&external).unwrap(), 950, 960, 0o600).unwrap();
            }
            "writable-node-root" => {
                std::fs::set_permissions(&f.0, Permissions::from_mode(0o777)).unwrap();
            }
            "writable-runtime-parent" => {
                std::fs::set_permissions(f.0.join("run"), Permissions::from_mode(0o777)).unwrap();
            }
            "root-mutex" => {
                set_owner(
                    &File::open(f.0.join("run/volume-manager/service.mutex")).unwrap(),
                    0,
                    0,
                    0o600,
                )
                .unwrap();
            }
            "foreign-owner" => set_owner(&File::open(&target).unwrap(), 955, 955, 0o600).unwrap(),
            "directory" => {
                std::fs::remove_file(&target).unwrap();
                std::fs::create_dir(&target).unwrap();
            }
            "foreign-name" => f.file("run/volume-manager/other-service.db", b"foreign"),
            "mutex-link" => {
                let mutex = f.0.join("run/volume-manager/service.mutex");
                std::fs::remove_file(&mutex).unwrap();
                symlink(&external, mutex).unwrap();
            }
            "staging-link" => {
                let dir = f.0.join("cache/firecracker/.volmgr-staging");
                std::fs::remove_dir_all(&dir).unwrap();
                symlink(f.0.join("run"), dir).unwrap();
            }
            "staged-socket-regular" => {
                f.file("run/volume-manager/.handoff-0123456789", b"retained file")
            }
            "staged-socket-symlink" => symlink(
                &external,
                f.0.join("run/volume-manager/.handoff-0123456789"),
            )
            .unwrap(),
            "staged-socket-owner" | "staged-socket-name" | "staged-socket-hardlink" => {
                let name = if variant == "staged-socket-name" {
                    ".handoff-unrelated"
                } else {
                    ".handoff-0123456789"
                };
                let socket = f.0.join("run/volume-manager").join(name);
                drop(UnixListener::bind(&socket).unwrap());
                let uid = if variant == "staged-socket-owner" {
                    955
                } else {
                    950
                };
                std::os::unix::fs::chown(&socket, Some(uid), Some(960)).unwrap();
                if variant == "staged-socket-hardlink" {
                    std::fs::hard_link(socket, f.0.join("linked-socket")).unwrap();
                }
            }
            _ => unreachable!(),
        }
        let before = std::fs::metadata(&external).unwrap();
        let regular_files = || {
            f.snapshot()
                .into_iter()
                .filter(|(_, _, _, _, mode, _)| mode & libc::S_IFMT == libc::S_IFREG)
                .collect::<Vec<_>>()
        };
        let files_before = regular_files();
        rejected(prepare(&f.0, Identity::Separate, HANDOFF_TIMEOUT));
        assert_eq!(
            regular_files(),
            files_before,
            "changed a file before rejecting {variant}"
        );
        let after = std::fs::metadata(&external).unwrap();
        assert_eq!(
            (after.ino(), after.uid(), after.gid(), after.mode()),
            (before.ino(), before.uid(), before.gid(), before.mode()),
            "changed external file for {variant}"
        );
        assert_eq!(after.ino(), external_before.ino());
        assert_eq!(std::fs::read(&external).unwrap(), b"outside state");
        if variant == "foreign-name" {
            assert_eq!(
                std::fs::metadata(f.0.join("run/volume-manager"))
                    .unwrap()
                    .uid(),
                0
            );
            std::fs::remove_file(f.0.join("run/volume-manager/other-service.db")).unwrap();
            // The sealed directory no longer proves a legacy source. Recover
            // to the matching old identity first; mixed ownership needs review.
            rejected(prepare(&f.0, Identity::Separate, HANDOFF_TIMEOUT));
            prepare(&f.0, Identity::Legacy, HANDOFF_TIMEOUT).unwrap();
            prepare(&f.0, Identity::Separate, HANDOFF_TIMEOUT).unwrap();
            assert_eq!(std::fs::read(&target).unwrap(), b"database");
        }
    }
}

#[test]
#[ignore = "requires root and a private mount namespace; disposable fixtures only"]
fn rejects_bind_mounts_in_private_state() {
    const CHILD: &str = "SPACE_HANDOFF_MOUNT_TEST_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let status = std::process::Command::new("unshare")
            .args(["--mount", "--propagation", "private", "--"])
            .arg(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "identity_handoff::tests::rejects_bind_mounts_in_private_state",
                "--ignored",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .status()
            .unwrap();
        assert!(status.success());
        return;
    }
    for entry in ["directory", "database", "staged-socket"] {
        let f = Fixture::new();
        let external = f.0.join("external");
        let target = if entry == "directory" {
            std::fs::create_dir(&external).unwrap();
            f.0.join("cache/firecracker/.volmgr-staging")
        } else if entry == "staged-socket" {
            drop(UnixListener::bind(&external).unwrap());
            let target = f.0.join("run/volume-manager/.handoff-0123456789");
            drop(UnixListener::bind(&target).unwrap());
            target
        } else {
            std::fs::write(&external, b"foreign mount bytes").unwrap();
            f.0.join("run/volume-manager/volume-manager.db")
        };
        std::os::unix::fs::chown(&external, Some(950), Some(960)).unwrap();
        std::fs::set_permissions(&external, Permissions::from_mode(0o700)).unwrap();
        let source = CString::new(external.as_os_str().as_bytes()).unwrap();
        let destination = CString::new(target.as_os_str().as_bytes()).unwrap();
        assert_eq!(
            // SAFETY: both paths are live C strings naming disposable fixture entries;
            // this child has its own mount namespace and bind mounts need no fstype/data.
            unsafe {
                libc::mount(
                    source.as_ptr(),
                    destination.as_ptr(),
                    std::ptr::null(),
                    libc::MS_BIND,
                    std::ptr::null(),
                )
            },
            0
        );
        let result = prepare(&f.0, Identity::Separate, HANDOFF_TIMEOUT);
        // SAFETY: destination is a live C string naming the fixture mount above.
        assert_eq!(unsafe { libc::umount(destination.as_ptr()) }, 0);
        assert_eq!(
            result
                .unwrap_err()
                .root_cause()
                .downcast_ref::<io::Error>()
                .unwrap()
                .raw_os_error(),
            Some(libc::EXDEV)
        );
        assert_eq!(std::fs::metadata(&external).unwrap().uid(), 950);
    }
}

#[test]
#[ignore = "requires root to exercise service identity handoff"]
fn separated_service_cannot_launder_a_legacy_owned_file_on_restart() {
    for (whole_directory, target_identity) in [
        (false, Identity::Separate),
        (true, Identity::Separate),
        (false, Identity::Legacy),
        (true, Identity::Legacy),
    ] {
        let f = Fixture::new();
        prepare(&f.0, Identity::Separate, HANDOFF_TIMEOUT).unwrap();
        let shared = f.0.join("cache/firecracker");
        set_owner(&File::open(&shared).unwrap(), 0, 952, 0o2775).unwrap();
        let legacy_dir = shared.join("legacy-private");
        std::fs::create_dir(&legacy_dir).unwrap();
        set_owner(
            &File::open(&legacy_dir).unwrap(),
            950,
            952,
            if whole_directory { 0o700 } else { 0o2775 },
        )
        .unwrap();
        f.file(
            "cache/firecracker/legacy-private/secret",
            b"not readable by volume-manager",
        );
        let source = legacy_dir.join("secret");
        let staging = shared.join(".volmgr-staging");
        let as_volume = |program: &str, arguments: &[&Path]| {
            std::process::Command::new("setpriv")
                .args(["--reuid=964", "--regid=964", "--groups=952", "--", program])
                .args(arguments)
                .output()
                .unwrap()
        };
        assert!(!as_volume("cat", &[&source]).status.success());
        let destination = if whole_directory {
            assert!(
                as_volume("mv", &[&staging, &shared.join("saved-staging")])
                    .status
                    .success()
            );
            assert!(as_volume("mv", &[&legacy_dir, &staging]).status.success());
            staging.join("secret")
        } else {
            let destination = staging.join("01980000-0000-7000-8000-000000000002");
            assert!(as_volume("mv", &[&source, &destination]).status.success());
            destination
        };
        rejected(prepare(&f.0, target_identity, HANDOFF_TIMEOUT));
        // Retry the same isolated identity: sealing must not infer legacy access.
        rejected(prepare(&f.0, Identity::Separate, HANDOFF_TIMEOUT));
        assert_eq!(std::fs::metadata(&destination).unwrap().uid(), 950);
    }
}

#[test]
#[ignore = "requires Linux openat2 support and a separately mounted procfs"]
fn rejects_existing_mount_crossings_without_mount_privileges() {
    let root = File::open("/").unwrap();
    let proc = open(root.as_raw_fd(), OsStr::new("proc"), libc::O_PATH, 0, true);
    assert_eq!(proc.unwrap_err().raw_os_error(), Some(libc::EXDEV));
}
