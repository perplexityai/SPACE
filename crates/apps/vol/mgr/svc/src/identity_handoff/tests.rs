use super::*;
use std::os::unix::fs::symlink;
use std::os::unix::net::UnixListener;
use std::path::PathBuf;

// Parallel tests spawn children that briefly inherit locked FDs before exec.
const HANDOFF_TIMEOUT: Duration = Duration::from_secs(10);

fn prepare(root: &Path, identity: Identity, timeout: Duration) -> anyhow::Result<()> {
    super::prepare(root, identity, timeout, None)
}

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
    std::os::unix::fs::chown(
        f.0.join("run/volume-manager/volume-manager.sock"),
        Some(950),
        Some(960),
    )
    .unwrap();
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
        let run = std::fs::metadata(f.0.join("run/volume-manager")).unwrap();
        assert_eq!(
            (run.uid(), run.gid(), run.mode() & 0o777),
            (uid, groups::SPACE_SBXCLIENTS.gid, 0o750)
        );
    }
}

#[test]
#[ignore = "requires root to exercise service identity handoff"]
fn abandoned_socket_replacement_does_not_block_handoff() {
    for (source, target) in [
        (Identity::Legacy, Identity::Separate),
        (Identity::Separate, Identity::Separate),
        (Identity::Separate, Identity::Legacy),
        (Identity::Legacy, Identity::Legacy),
    ] {
        let f = Fixture::new();
        prepare(&f.0, source, HANDOFF_TIMEOUT).unwrap();
        let (uid, gid) = source.ids();
        let socket = f.0.join("run/volume-manager/volume-manager.sock");
        drop(UnixListener::bind(&socket).unwrap());
        std::os::unix::fs::chown(&socket, Some(uid), Some(gid)).unwrap();
        let before = f.snapshot();
        let abandoned = f.0.join("run/volume-manager/.handoff-0123456789");
        drop(UnixListener::bind(&abandoned).unwrap());
        std::os::unix::fs::chown(&abandoned, Some(uid), Some(gid)).unwrap();
        prepare(&f.0, target, HANDOFF_TIMEOUT).unwrap();
        assert!(!abandoned.exists());
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
        0o3775,
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
        let directory_state = || {
            ["run/volume-manager", "cache/firecracker/.volmgr-staging"].map(|path| {
                let metadata = std::fs::symlink_metadata(f.0.join(path)).unwrap();
                (
                    metadata.ino(),
                    metadata.uid(),
                    metadata.gid(),
                    metadata.mode(),
                )
            })
        };
        let directories_before = directory_state();
        rejected(prepare(&f.0, Identity::Separate, HANDOFF_TIMEOUT));
        assert_eq!(
            directory_state(),
            directories_before,
            "changed directory access before rejecting {variant}"
        );
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
            std::fs::remove_file(f.0.join("run/volume-manager/other-service.db")).unwrap();
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
        set_owner(&File::open(&shared).unwrap(), 0, 952, 0o3775).unwrap();
        let untrusted = shared.join("untrusted");
        std::fs::create_dir(&untrusted).unwrap();
        set_owner(&File::open(&untrusted).unwrap(), 964, 952, 0o750).unwrap();
        let legacy_dir = untrusted.join("legacy-private");
        std::fs::create_dir(&legacy_dir).unwrap();
        set_owner(
            &File::open(&legacy_dir).unwrap(),
            950,
            952,
            if whole_directory { 0o770 } else { 0o2775 },
        )
        .unwrap();
        f.file(
            "cache/firecracker/untrusted/legacy-private/secret",
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

type Checkpoint = Box<dyn FnMut(&str)>;

thread_local! {
    static CHECKPOINT: std::cell::RefCell<Option<Checkpoint>> = const { std::cell::RefCell::new(None) };
}

pub(super) fn checkpoint(point: &str) {
    CHECKPOINT.with_borrow_mut(|callback| {
        if let Some(callback) = callback {
            callback(point);
        }
    });
}

fn at_checkpoints<T>(callback: impl FnMut(&str) + 'static, run: impl FnOnce() -> T) -> T {
    CHECKPOINT.with_borrow_mut(|slot| *slot = Some(Box::new(callback)));
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(run));
    CHECKPOINT.with_borrow_mut(|slot| *slot = None);
    result.unwrap_or_else(|error| std::panic::resume_unwind(error))
}

fn crash_child(root: &Path, target: Identity, point: &str) -> std::process::Output {
    std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "identity_handoff::tests::killed_handoffs_resume_or_reverse_without_losing_data",
            "--ignored",
            "--nocapture",
        ])
        .env("SPACE_HANDOFF_CRASH_ROOT", root)
        .env("SPACE_HANDOFF_CRASH_UID", target.ids().0.to_string())
        .env("SPACE_HANDOFF_CRASH_POINT", point)
        .output()
        .unwrap()
}

#[test]
#[ignore = "requires root; terminates only disposable test subprocesses"]
fn killed_handoffs_resume_or_reverse_without_losing_data() {
    use std::os::unix::process::ExitStatusExt;
    if let Some(root) = std::env::var_os("SPACE_HANDOFF_CRASH_ROOT") {
        let identity = Identity::from_uid(
            std::env::var("SPACE_HANDOFF_CRASH_UID")
                .unwrap()
                .parse()
                .unwrap(),
        )
        .unwrap();
        let kill_at = std::env::var("SPACE_HANDOFF_CRASH_POINT").unwrap();
        let points = std::rc::Rc::new(std::cell::Cell::new(0usize));
        let count = points.clone();
        at_checkpoints(
            move |point| {
                let index = count.get();
                count.set(index + 1);
                if kill_at == index.to_string() || kill_at == point {
                    // SAFETY: SIGKILL affects only this disposable subprocess.
                    unsafe {
                        libc::raise(libc::SIGKILL);
                    }
                    unreachable!("SIGKILL must terminate the child");
                }
            },
            || prepare(Path::new(&root), identity, HANDOFF_TIMEOUT),
        )
        .unwrap();
        println!("HANDOFF_POINTS={}", points.get());
        return;
    }
    for (source, target) in [
        (Identity::Legacy, Identity::Separate),
        (Identity::Separate, Identity::Legacy),
    ] {
        for bootstrap in [false, true] {
            let fixture = || {
                let f = Fixture::new();
                prepare(&f.0, source, HANDOFF_TIMEOUT).unwrap();
                if bootstrap {
                    std::fs::remove_dir_all(f.0.join("run/volume-manager")).unwrap();
                    std::fs::remove_dir_all(f.0.join("cache/firecracker/.volmgr-staging")).unwrap();
                } else {
                    let abandoned = f.0.join("run/volume-manager/.handoff-0123456789");
                    drop(UnixListener::bind(&abandoned).unwrap());
                    std::os::unix::fs::chown(
                        &abandoned,
                        Some(source.ids().0),
                        Some(source.ids().1),
                    )
                    .unwrap();
                }
                f
            };
            let baseline = fixture();
            let output = crash_child(&baseline.0, target, "never");
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            let stdout = String::from_utf8(output.stdout).unwrap();
            let points: usize = stdout
                .lines()
                .find_map(|line| line.strip_prefix("HANDOFF_POINTS="))
                .unwrap()
                .parse()
                .unwrap();
            assert!(points > 0, "must exercise actual mutation boundaries");
            for point in 0..points {
                for retry in [source, target] {
                    let f = fixture();
                    let before = if bootstrap { Vec::new() } else { f.snapshot() };
                    let output = crash_child(&f.0, target, &point.to_string());
                    assert_eq!(
                        output.status.signal(),
                        Some(libc::SIGKILL),
                        "point {point}: {}",
                        String::from_utf8_lossy(&output.stderr)
                    );
                    prepare(&f.0, retry, HANDOFF_TIMEOUT).unwrap_or_else(|error| panic!("bootstrap={bootstrap} target={target:?} point={point} retry={retry:?}: {error:#}"));
                    for (path, inode, _, _, mode, bytes) in before {
                        if mode & libc::S_IFMT != libc::S_IFREG {
                            continue;
                        }
                        let metadata = std::fs::metadata(&path).unwrap();
                        assert_eq!(metadata.ino(), inode);
                        assert_eq!(metadata.uid(), retry.ids().0);
                        assert_eq!(std::fs::read(path).unwrap(), bytes);
                    }
                    for path in [
                        "run/volume-manager",
                        "cache/firecracker/.volmgr-staging",
                        "run/volume-manager/service.mutex",
                    ] {
                        assert_eq!(
                            std::fs::metadata(f.0.join(path)).unwrap().uid(),
                            retry.ids().0
                        );
                    }
                    assert!(
                        !f.0.join("run/.volume-manager-identity/pending.json")
                            .exists()
                    );
                }
            }
        }
    }
}

#[test]
#[ignore = "requires root to check administrator-owned installation links"]
fn only_the_trusted_installation_root_may_be_a_symlink() {
    for invalid in [false, true] {
        let f = Fixture::new();
        let link = f.0.join("installation");
        symlink(&f.0, &link).unwrap();
        if invalid {
            std::os::unix::fs::lchown(&link, Some(964), Some(964)).unwrap();
            rejected(prepare(&link, Identity::Separate, HANDOFF_TIMEOUT));
            assert_eq!(
                std::fs::metadata(f.0.join("run/volume-manager"))
                    .unwrap()
                    .uid(),
                950
            );
        } else {
            prepare(&link, Identity::Separate, HANDOFF_TIMEOUT).unwrap();
            assert_eq!(
                std::fs::metadata(f.0.join("run/volume-manager"))
                    .unwrap()
                    .uid(),
                964
            );
        }
    }
}

#[test]
#[ignore = "requires root to exercise interrupted publication"]
fn published_record_never_replays_file_ownership() {
    use std::os::unix::process::ExitStatusExt;
    for point in ["staging published", "runtime published"] {
        let f = Fixture::new();
        assert_eq!(
            crash_child(&f.0, Identity::Separate, point).status.signal(),
            Some(libc::SIGKILL)
        );
        let staging = f.0.join("cache/firecracker/.volmgr-staging");
        let deleted = staging.join("01980000-0000-7000-8000-000000000001");
        std::fs::remove_file(&deleted).unwrap();
        // Model a target service moving a legacy-owned file from a writable
        // shared parent into its published staging directory.
        f.file(
            "cache/firecracker/.volmgr-staging/foreign",
            b"legacy secret",
        );
        let foreign = staging.join("foreign");
        let before = std::fs::metadata(&foreign).unwrap();
        prepare(&f.0, Identity::Separate, HANDOFF_TIMEOUT).unwrap();
        let after = std::fs::metadata(&foreign).unwrap();
        assert_eq!(
            (after.ino(), after.uid(), after.mode()),
            (before.ino(), 950, before.mode())
        );
        assert!(!deleted.exists());
        // A new transaction must reject that foreign file in either direction.
        rejected(prepare(&f.0, Identity::Legacy, HANDOFF_TIMEOUT));
        assert_eq!(std::fs::metadata(foreign).unwrap().uid(), 950);
    }
}

#[test]
#[ignore = "requires root to exercise replacement races"]
fn replacement_between_validation_and_sealing_is_rejected() {
    for point in ["state validated", "owner changed"] {
        let f = Fixture::new();
        let victim = f.0.join("run/volume-manager/volume-manager.db");
        let original = f.0.join("original.db");
        let root = f.0.clone();
        let victim_hook = victim.clone();
        let original_hook = original.clone();
        let mut replaced = false;
        let result = at_checkpoints(
            move |seen| {
                if !replaced && seen == point {
                    replaced = true;
                    std::fs::rename(&victim_hook, &original_hook).unwrap();
                    std::fs::write(&victim_hook, b"substituted").unwrap();
                    std::os::unix::fs::chown(&victim_hook, Some(950), Some(960)).unwrap();
                }
            },
            || prepare(&root, Identity::Separate, HANDOFF_TIMEOUT),
        );
        rejected(result);
        assert!(original.exists(), "checkpoint must execute");
        assert_eq!(std::fs::metadata(&victim).unwrap().uid(), 950);
        for dir in ["run/volume-manager", "cache/firecracker/.volmgr-staging"] {
            assert_eq!(std::fs::metadata(f.0.join(dir)).unwrap().uid(), 950);
        }
    }
}

#[test]
#[ignore = "requires root to exercise missing retained directories"]
fn pending_record_never_recreates_missing_directories() {
    use std::os::unix::process::ExitStatusExt;
    for path in ["run/volume-manager", "cache/firecracker/.volmgr-staging"] {
        let f = Fixture::new();
        assert_eq!(
            crash_child(&f.0, Identity::Separate, "publishing durable")
                .status
                .signal(),
            Some(libc::SIGKILL)
        );
        let original = f.0.join(path);
        std::fs::rename(&original, f.0.join("retained-original")).unwrap();
        rejected(prepare(&f.0, Identity::Separate, HANDOFF_TIMEOUT));
        assert!(!original.exists());
        std::fs::rename(f.0.join("retained-original"), &original).unwrap();
        prepare(&f.0, Identity::Separate, HANDOFF_TIMEOUT).unwrap();
    }
}

#[test]
#[ignore = "requires root and real service credentials"]
fn service_writable_installation_ancestors_cannot_redirect_handoff() {
    for (uid, gid, mode) in [(964, 964, 0o755), (0, 952, 0o775), (1000, 1000, 0o2775)] {
        let f = Fixture::new();
        let parent = f.0.join("administrator-home");
        std::fs::create_dir(&parent).unwrap();
        set_owner(&File::open(&parent).unwrap(), uid, gid, mode).unwrap();
        let nested = parent.join("installation");
        let link = f.0.join("space-link");
        let other = Fixture::new();
        // A root-owned installation link must not trust a mutable target chain.
        symlink(&other.0, &nested).unwrap();
        symlink(&nested, &link).unwrap();
        rejected(prepare(&link, Identity::Separate, HANDOFF_TIMEOUT));
        assert_eq!(
            std::fs::metadata(other.0.join("run/volume-manager/volume-manager.db"))
                .unwrap()
                .uid(),
            950
        );
        std::fs::remove_file(&nested).unwrap();
        std::fs::rename(&other.0, &nested).unwrap();
        let result = prepare(&link, Identity::Separate, HANDOFF_TIMEOUT);
        std::fs::rename(&nested, &other.0).unwrap();
        if uid == 1000 {
            result.unwrap();
        } else {
            rejected(result);
            assert_eq!(
                std::fs::metadata(other.0.join("run/volume-manager/volume-manager.db"))
                    .unwrap()
                    .uid(),
                950
            );
        }
    }
}

#[test]
#[ignore = "requires root and real service credentials"]
fn replacing_published_staging_cannot_grant_legacy_files() {
    use std::os::unix::process::CommandExt;
    for point in ["staging published", "runtime published"] {
        let f = Fixture::new();
        let cache = f.0.join("cache/firecracker");
        set_owner(&File::open(&cache).unwrap(), 0, 952, 0o3775).unwrap();
        let untrusted = cache.join("untrusted");
        std::fs::create_dir(&untrusted).unwrap();
        set_owner(&File::open(&untrusted).unwrap(), 964, 952, 0o750).unwrap();
        let foreign = untrusted.join("foreign");
        std::fs::create_dir(&foreign).unwrap();
        set_owner(&File::open(&foreign).unwrap(), 950, 952, 0o770).unwrap();
        f.file(
            "cache/firecracker/untrusted/foreign/secret",
            b"legacy secret",
        );
        let original_inode = std::fs::metadata(&foreign).unwrap().ino();
        let cache_hook = cache.clone();
        let result = at_checkpoints(
            move |seen| {
                if seen == point {
                    for (from, to) in [
                        (".volmgr-staging", "saved"),
                        ("untrusted/foreign", ".volmgr-staging"),
                    ] {
                        let output = std::process::Command::new("setpriv")
                            .args(["--reuid=964", "--regid=964", "--groups=952", "mv"])
                            .arg(cache_hook.join(from))
                            .arg(cache_hook.join(to))
                            .output()
                            .unwrap();
                        assert!(
                            output.status.success(),
                            "{}",
                            String::from_utf8_lossy(&output.stderr)
                        );
                    }
                }
            },
            || prepare(&f.0, Identity::Separate, HANDOFF_TIMEOUT),
        );
        rejected(result);
        let planted = cache.join(".volmgr-staging");
        let metadata = std::fs::metadata(&planted).unwrap();
        assert_eq!(
            (metadata.ino(), metadata.uid(), metadata.mode() & 0o777),
            (original_inode, 950, 0o770)
        );
        assert!(
            !std::process::Command::new("cat")
                .arg(planted.join("secret"))
                .uid(964)
                .gid(964)
                .output()
                .unwrap()
                .status
                .success()
        );
        for identity in [Identity::Legacy, Identity::Separate] {
            rejected(prepare(&f.0, identity, HANDOFF_TIMEOUT));
        }
    }
}

#[test]
#[ignore = "requires root and access ACL support"]
fn installation_ancestors_reject_service_acl_and_unprotected_world_writes() {
    for (mode, acl, accepted) in [
        (0o1777, false, true),
        (0o777, false, false),
        (0o755, true, false),
    ] {
        let f = Fixture::new();
        let parent = f.0.join("parent");
        std::fs::create_dir(&parent).unwrap();
        set_owner(&File::open(&parent).unwrap(), 1000, 1000, mode).unwrap();
        if acl {
            // Linux POSIX ACL xattr: owner, named user, group, mask, other.
            let mut access = 2u32.to_le_bytes().to_vec();
            for (tag, permissions, id) in [
                (1u16, 7u16, u32::MAX),
                (2, 7, 964),
                (4, 5, u32::MAX),
                (16, 7, u32::MAX),
                (32, 5, u32::MAX),
            ] {
                access.extend(tag.to_le_bytes());
                access.extend(permissions.to_le_bytes());
                access.extend(id.to_le_bytes());
            }
            let directory = File::open(&parent).unwrap();
            assert_eq!(
                // SAFETY: the FD and the complete ACL byte buffer remain live.
                unsafe {
                    libc::fsetxattr(
                        directory.as_raw_fd(),
                        c"system.posix_acl_access".as_ptr(),
                        access.as_ptr().cast(),
                        access.len(),
                        0,
                    )
                },
                0
            );
        }
        let other = Fixture::new();
        let nested = parent.join("installation");
        std::fs::rename(&other.0, &nested).unwrap();
        let result = prepare(&nested, Identity::Separate, HANDOFF_TIMEOUT);
        std::fs::rename(&nested, &other.0).unwrap();
        if accepted {
            result.unwrap();
        } else {
            rejected(result);
        }
        assert_eq!(
            std::fs::metadata(other.0.join("run/volume-manager/volume-manager.db"))
                .unwrap()
                .uid(),
            if accepted { 964 } else { 950 }
        );
    }
}

#[test]
#[ignore = "requires root and real service credentials"]
fn services_cannot_read_or_forge_recovery_records() {
    use std::os::unix::process::ExitStatusExt;
    let f = Fixture::new();
    assert_eq!(
        crash_child(&f.0, Identity::Separate, "publishing durable")
            .status
            .signal(),
        Some(libc::SIGKILL)
    );
    let record = f.0.join("run/.volume-manager-identity/pending.json");
    let original = std::fs::read(&record).unwrap();
    for uid in ["950", "964"] {
        for args in [vec!["cat"], vec!["truncate", "-s", "0"]] {
            let output = std::process::Command::new("setpriv")
                .args(["--reuid", uid, "--regid", uid, "--groups", "951,952,965"])
                .args(args)
                .arg(&record)
                .output()
                .unwrap();
            assert!(!output.status.success());
        }
    }
    assert_eq!(std::fs::read(record).unwrap(), original);
    prepare(&f.0, Identity::Separate, HANDOFF_TIMEOUT).unwrap();
}

#[test]
#[ignore = "requires root and real service credentials"]
fn initial_handoff_rejects_replaceable_staging_parent() {
    for replace_cache in [false, true] {
        let f = Fixture::new();
        let parent = f.0.join(if replace_cache {
            "cache"
        } else {
            "cache/firecracker"
        });
        set_owner(&File::open(&parent).unwrap(), 0, 952, 0o2775).unwrap();
        let foreign = parent.join("private");
        std::fs::create_dir(&foreign).unwrap();
        let staged = if replace_cache {
            set_owner(&File::open(&foreign).unwrap(), 0, 952, 0o3775).unwrap();
            let staged = foreign.join(".volmgr-staging");
            std::fs::create_dir(&staged).unwrap();
            staged
        } else {
            foreign.clone()
        };
        set_owner(&File::open(&staged).unwrap(), 950, 960, 0o700).unwrap();
        let secret = staged.join("secret");
        std::fs::write(&secret, b"legacy secret").unwrap();
        set_owner(&File::open(&secret).unwrap(), 950, 960, 0o600).unwrap();
        let target = if replace_cache {
            "firecracker"
        } else {
            ".volmgr-staging"
        };
        for (from, to) in [(target, "saved"), ("private", target)] {
            assert!(
                std::process::Command::new("setpriv")
                    .args(["--reuid=964", "--regid=964", "--groups=952", "mv"])
                    .arg(parent.join(from))
                    .arg(parent.join(to))
                    .status()
                    .unwrap()
                    .success()
            );
        }
        let result = prepare(&f.0, Identity::Separate, HANDOFF_TIMEOUT);
        let secret = f.0.join("cache/firecracker/.volmgr-staging/secret");
        assert_eq!(std::fs::metadata(&secret).unwrap().uid(), 950);
        assert!(
            !std::process::Command::new("setpriv")
                .args(["--reuid=964", "--regid=964", "--groups=952", "cat"])
                .arg(secret)
                .output()
                .unwrap()
                .status
                .success()
        );
        rejected(result);
    }
}

#[test]
#[ignore = "requires root to check deployment identity under the retained lock"]
fn expected_identity_prevents_implicit_rollback() {
    use std::os::unix::process::ExitStatusExt;
    let f = Fixture::new();
    super::prepare(
        &f.0,
        Identity::Legacy,
        HANDOFF_TIMEOUT,
        Some(Identity::Legacy),
    )
    .unwrap();
    prepare(&f.0, Identity::Separate, HANDOFF_TIMEOUT).unwrap();
    let before = f.snapshot();
    rejected(super::prepare(
        &f.0,
        Identity::Legacy,
        HANDOFF_TIMEOUT,
        Some(Identity::Legacy),
    ));
    assert_eq!(f.snapshot(), before);
    prepare(&f.0, Identity::Legacy, HANDOFF_TIMEOUT).unwrap();
    assert_eq!(
        crash_child(&f.0, Identity::Separate, "journal committed")
            .status
            .signal(),
        Some(libc::SIGKILL)
    );
    rejected(super::prepare(
        &f.0,
        Identity::Legacy,
        HANDOFF_TIMEOUT,
        Some(Identity::Legacy),
    ));
    prepare(&f.0, Identity::Legacy, HANDOFF_TIMEOUT).unwrap();
}

#[test]
#[ignore = "requires root and real service credentials"]
fn initial_handoff_rejects_service_writable_private_state() {
    for area in ["run/volume-manager", "cache/firecracker/.volmgr-staging"] {
        let f = Fixture::new();
        let private = f.0.join(area);
        set_owner(&File::open(&private).unwrap(), 950, 952, 0o2775).unwrap();
        let untrusted = f.0.join("untrusted");
        std::fs::create_dir(&untrusted).unwrap();
        set_owner(&File::open(&untrusted).unwrap(), 964, 964, 0o700).unwrap();
        f.file("untrusted/secret", b"legacy secret");
        let destination = private.join(if area.starts_with("run") {
            "volume-manager.db"
        } else {
            "secret"
        });
        assert!(
            std::process::Command::new("setpriv")
                .args(["--reuid=964", "--regid=964", "--groups=952", "mv"])
                .arg(untrusted.join("secret"))
                .arg(&destination)
                .status()
                .unwrap()
                .success()
        );
        let result = prepare(&f.0, Identity::Separate, HANDOFF_TIMEOUT);
        assert_eq!(std::fs::metadata(&destination).unwrap().uid(), 950);
        rejected(result);
    }
}
