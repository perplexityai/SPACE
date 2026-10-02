use super::*;
use std::os::unix::fs::symlink;
use std::os::unix::net::UnixListener;
use std::path::PathBuf;
use std::process::{Command, Output};

const RUNTIME: &str = "run/volume-manager";
const STAGING: &str = "cache/firecracker/.volmgr-staging";
const DATABASE: &str = "run/volume-manager/volume-manager.db";
const MUTEX: &str = "run/volume-manager/service.mutex";

// Parallel tests spawn children that briefly inherit locked FDs before exec.
const HANDOFF_TIMEOUT: Duration = Duration::from_secs(10);

fn prepare(root: &Path, identity: Identity) -> anyhow::Result<()> {
    super::prepare(root, identity, HANDOFF_TIMEOUT, None)
}

fn rejected(result: anyhow::Result<()>) {
    let error = format!("{:#}", result.unwrap_err());
    assert!(!error.contains("still owns service.mutex"), "{error}");
}

fn metadata(path: impl AsRef<Path>) -> Metadata {
    std::fs::metadata(path).unwrap()
}

fn own(path: impl AsRef<Path>, uid: u32, gid: u32, mode: u32) {
    set_owner(&File::open(path).unwrap(), uid, gid, mode).unwrap();
}

fn attributes(path: impl AsRef<Path>) -> (u64, u32, u32, u32) {
    let m = metadata(path);
    (m.ino(), m.uid(), m.gid(), m.mode())
}

fn as_user(uid: u32) -> Command {
    let mut command = Command::new("setpriv");
    command.args([
        format!("--reuid={uid}"),
        format!("--regid={}", Identity::from_uid(uid).unwrap().ids().1),
        "--groups=951,952,965".into(),
        "--".into(),
    ]);
    command
}

fn test_arguments(name: &str) -> [String; 4] {
    [
        "--exact".into(),
        format!("identity_handoff::tests::{name}"),
        "--ignored".into(),
        "--nocapture".into(),
    ]
}

fn succeeds(command: &mut Command) -> Output {
    let output = command.output().unwrap();
    assert!(
        output.status.success(),
        "{command:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        // SAFETY: geteuid takes no arguments and only reads this process's identity.
        assert_eq!(unsafe { libc::geteuid() }, 0, "run handoff tests as root");
        let root =
            std::env::temp_dir().join(format!("space-identity-test-{}", uuid::Uuid::now_v7()));
        let fixture = Self(root);
        for path in [RUNTIME, STAGING] {
            std::fs::create_dir_all(fixture.0.join(path)).unwrap();
            own(fixture.0.join(path), 950, 960, 0o750);
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
            ".handoff-0123456789",
        ] {
            fixture.file(
                &format!("cache/firecracker/.volmgr-staging/{name}"),
                b"retained staging file",
            );
        }
        fixture
    }
    fn file(&self, path: &str, bytes: &[u8]) {
        std::fs::write(self.0.join(path), bytes).unwrap();
        own(self.0.join(path), 950, 960, 0o600);
    }
    fn snapshot(&self) -> Vec<(PathBuf, u64, u32, u32, u32, Vec<u8>)> {
        [RUNTIME, STAGING]
            .into_iter()
            .flat_map(|dir| std::fs::read_dir(self.0.join(dir)).unwrap())
            .map(|entry| {
                let path = entry.unwrap().path();
                let m = metadata(&path);
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
    for (missing_staging, socket_uid) in [(false, 950), (false, 964), (true, 950)] {
        let f = Fixture::new();
        own(f.0.join("cache/firecracker"), 0, 952, 0o3775);
        let runtime = f.0.join(RUNTIME);
        let staging = f.0.join(STAGING);
        let socket = runtime.join("volume-manager.sock");
        let _listener = UnixListener::bind(&socket).unwrap();
        std::os::unix::fs::chown(&socket, Some(socket_uid), Some(953)).unwrap();
        let socket_before = attributes(&socket);
        for (identity, uid, gid) in [
            (Identity::Legacy, 950, 960),
            (Identity::Separate, 964, 964),
            (Identity::Separate, 964, 964),
            (Identity::Legacy, 950, 960),
        ] {
            let (source_uid, source_gid) =
                Identity::from_uid(metadata(&runtime).uid()).unwrap().ids();
            let before: Vec<_> = f
                .snapshot()
                .into_iter()
                .filter(|(path, ..)| !missing_staging || !path.starts_with(&staging))
                .collect();
            if missing_staging {
                std::fs::remove_dir_all(&staging).unwrap();
            }
            let abandoned = runtime.join(".handoff-0123456789");
            drop(UnixListener::bind(&abandoned).unwrap());
            std::os::unix::fs::chown(&abandoned, Some(source_uid), Some(source_gid)).unwrap();
            prepare(&f.0, identity).unwrap();
            assert!(!abandoned.exists());
            for (path, inode, _, _, _, bytes) in before {
                let m = metadata(&path);
                assert_eq!(m.ino(), inode, "inode changed: {path:?}");
                if m.is_file() {
                    assert_eq!(std::fs::read(&path).unwrap(), bytes);
                    assert_eq!(m.uid(), uid);
                    assert_eq!(m.gid(), if path.starts_with(&staging) { 952 } else { gid });
                    assert_eq!(
                        m.mode() & 0o777,
                        if path == runtime.join("service.mutex") {
                            0o660
                        } else {
                            0o600
                        }
                    );
                }
            }
            for (directory, gid) in [(&staging, 952), (&runtime, 951)] {
                let m = metadata(directory);
                assert_eq!((m.uid(), m.gid(), m.mode() & 0o777), (uid, gid, 0o750));
            }
            assert_eq!(attributes(&socket), socket_before);
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
        prepare(&f.0, source).unwrap();
        for name in [
            "volume-manager.db",
            "volume-manager.db-wal",
            "volume-manager.db-shm",
        ] {
            std::fs::remove_file(f.0.join(RUNTIME).join(name)).unwrap();
        }
        let database = f.0.join(DATABASE);
        let child = |identity: Identity, verify: bool| {
            let (uid, gid) = identity.ids();
            let mut command = Command::new("setpriv");
            command
                .args([
                    format!("--reuid={uid}"),
                    format!("--regid={gid}"),
                    "--clear-groups".into(),
                    "--".into(),
                ])
                .arg(std::env::current_exe().unwrap())
                .args(test_arguments(
                    "hot_sqlite_journal_survives_handoff_and_recovers_committed_rows",
                ))
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
        prepare(&f.0, target).unwrap();
        for (path, inode, _, _, _, bytes) in before {
            let metadata = metadata(&path);
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
fn a_live_predecessor_blocks_all_ownership_changes() {
    let f = Fixture::new();
    let before = f.snapshot();
    let mutex = File::open(f.0.join(MUTEX)).unwrap();
    assert_eq!(
        // SAFETY: mutex owns the descriptor for the duration of the lock.
        unsafe { libc::flock(mutex.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
        0
    );
    let error = super::prepare(&f.0, Identity::Separate, Duration::ZERO, None).unwrap_err();
    assert!(format!("{error:#}").contains("still owns service.mutex"));
    assert_eq!(f.snapshot(), before);
    assert_eq!(metadata(f.0.join(RUNTIME)).uid(), 950);
    drop(mutex);
    prepare(&f.0, Identity::Separate).unwrap();
}

#[test]
#[ignore = "requires root to exercise service identity handoff"]
fn unsafe_retained_entries_fail_without_reowning_external_data() {
    let cases = [
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
        "published-socket-owner",
        "staged-socket-owner",
        "staged-socket-regular",
        "staged-socket-name",
        "staged-socket-hardlink",
        "staged-socket-symlink",
    ]
    .into_iter()
    .map(|case| (case, 0))
    .chain([0o2775, 0o700].map(|mode| ("root-staging", mode)));
    for (variant, mode) in cases {
        let f = Fixture::new();
        let external = f.0.join("unrelated");
        std::fs::write(&external, b"outside state").unwrap();
        own(&external, 950, 960, 0o600);
        let target = f.0.join(DATABASE);
        match variant {
            "root-staging" => own(f.0.join(STAGING), 0, 952, mode),
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
                own(&external, 950, 960, 0o600);
            }
            "writable-node-root" => {
                std::fs::set_permissions(&f.0, Permissions::from_mode(0o777)).unwrap();
            }
            "writable-runtime-parent" => {
                std::fs::set_permissions(f.0.join("run"), Permissions::from_mode(0o777)).unwrap();
            }
            "root-mutex" => {
                own(f.0.join(MUTEX), 0, 0, 0o600);
            }
            "foreign-owner" => own(&target, 955, 955, 0o600),
            "directory" => {
                std::fs::remove_file(&target).unwrap();
                std::fs::create_dir(&target).unwrap();
            }
            "foreign-name" => f.file("run/volume-manager/other-service.db", b"foreign"),
            "mutex-link" => {
                let mutex = f.0.join(MUTEX);
                std::fs::remove_file(&mutex).unwrap();
                symlink(&external, mutex).unwrap();
            }
            "staging-link" => {
                let dir = f.0.join(STAGING);
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
            "published-socket-owner"
            | "staged-socket-owner"
            | "staged-socket-name"
            | "staged-socket-hardlink" => {
                let name = match variant {
                    "published-socket-owner" => "volume-manager.sock",
                    "staged-socket-name" => ".handoff-unrelated",
                    _ => ".handoff-0123456789",
                };
                let socket = f.0.join(RUNTIME).join(name);
                drop(UnixListener::bind(&socket).unwrap());
                let uid = if matches!(variant, "published-socket-owner" | "staged-socket-owner") {
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
        let before = attributes(&external);
        let regular_files = || {
            f.snapshot()
                .into_iter()
                .filter(|(_, _, _, _, mode, _)| mode & libc::S_IFMT == libc::S_IFREG)
                .collect::<Vec<_>>()
        };
        let files_before = regular_files();
        let directory_state = || {
            [RUNTIME, STAGING].map(|path| {
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
        let identity = if variant == "root-staging" {
            Identity::Legacy
        } else {
            Identity::Separate
        };
        for _ in 0..2 {
            rejected(prepare(&f.0, identity));
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
            assert_eq!(
                attributes(&external),
                before,
                "changed external file for {variant}"
            );
            assert_eq!(std::fs::read(&external).unwrap(), b"outside state");
        }
        if variant == "foreign-name" {
            std::fs::remove_file(f.0.join("run/volume-manager/other-service.db")).unwrap();
            prepare(&f.0, Identity::Separate).unwrap();
            assert_eq!(std::fs::read(&target).unwrap(), b"database");
        }
    }
}

#[test]
#[ignore = "requires root and a private mount namespace; disposable fixtures only"]
fn rejects_bind_mounts_in_private_state() {
    const CHILD: &str = "SPACE_HANDOFF_MOUNT_TEST_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let output = succeeds(
            Command::new("unshare")
                .args(["--mount", "--propagation", "private", "--"])
                .arg(std::env::current_exe().unwrap())
                .args(test_arguments("rejects_bind_mounts_in_private_state"))
                .env(CHILD, "1"),
        );
        assert!(
            String::from_utf8(output.stdout)
                .unwrap()
                .lines()
                .any(|line| line == "MOUNTS_CHECKED")
        );
        return;
    }
    for entry in ["directory", "database", "staged-socket"] {
        let f = Fixture::new();
        let external = f.0.join("external");
        let target = if entry == "directory" {
            std::fs::create_dir(&external).unwrap();
            f.0.join(STAGING)
        } else if entry == "staged-socket" {
            drop(UnixListener::bind(&external).unwrap());
            let target = f.0.join("run/volume-manager/.handoff-0123456789");
            drop(UnixListener::bind(&target).unwrap());
            target
        } else {
            std::fs::write(&external, b"foreign mount bytes").unwrap();
            f.0.join(DATABASE)
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
        let result = prepare(&f.0, Identity::Separate);
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
        assert_eq!(metadata(&external).uid(), 950);
    }
    println!("MOUNTS_CHECKED");
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
        prepare(&f.0, Identity::Separate).unwrap();
        let shared = f.0.join("cache/firecracker");
        own(&shared, 0, 952, 0o3775);
        let untrusted = shared.join("untrusted");
        std::fs::create_dir(&untrusted).unwrap();
        own(&untrusted, 964, 952, 0o750);
        let legacy_dir = untrusted.join("legacy-private");
        std::fs::create_dir(&legacy_dir).unwrap();
        own(
            &legacy_dir,
            950,
            952,
            if whole_directory { 0o770 } else { 0o2775 },
        );
        f.file(
            "cache/firecracker/untrusted/legacy-private/secret",
            b"not readable by volume-manager",
        );
        let source = legacy_dir.join("secret");
        let staging = shared.join(".volmgr-staging");
        assert!(
            !as_user(964)
                .arg("cat")
                .arg(&source)
                .status()
                .unwrap()
                .success()
        );
        let destination = if whole_directory {
            succeeds(
                as_user(964)
                    .arg("mv")
                    .arg(&staging)
                    .arg(shared.join("saved-staging")),
            );
            succeeds(as_user(964).arg("mv").arg(&legacy_dir).arg(&staging));
            staging.join("secret")
        } else {
            let destination = staging.join("01980000-0000-7000-8000-000000000002");
            succeeds(as_user(964).arg("mv").arg(&source).arg(&destination));
            destination
        };
        rejected(prepare(&f.0, target_identity));
        // Retry the same isolated identity: sealing must not infer legacy access.
        rejected(prepare(&f.0, Identity::Separate));
        assert_eq!(metadata(&destination).uid(), 950);
    }
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

fn crash_child(root: &Path, target: Identity, point: &str) -> Output {
    Command::new(std::env::current_exe().unwrap())
        .args(test_arguments(
            "killed_handoffs_resume_or_reverse_without_losing_data",
        ))
        .env("SPACE_HANDOFF_CRASH_ROOT", root)
        .env("SPACE_HANDOFF_CRASH_UID", target.ids().0.to_string())
        .env("SPACE_HANDOFF_CRASH_POINT", point)
        .output()
        .unwrap()
}

fn interrupted(point: &str) -> Fixture {
    use std::os::unix::process::ExitStatusExt;
    let fixture = Fixture::new();
    let output = crash_child(&fixture.0, Identity::Separate, point);
    assert_eq!(
        output.status.signal(),
        Some(libc::SIGKILL),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    fixture
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
            || prepare(Path::new(&root), identity),
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
                prepare(&f.0, source).unwrap();
                if bootstrap {
                    std::fs::remove_dir_all(f.0.join(RUNTIME)).unwrap();
                    std::fs::remove_dir_all(f.0.join(STAGING)).unwrap();
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
                    prepare(&f.0, retry).unwrap_or_else(|error| panic!("bootstrap={bootstrap} target={target:?} point={point} retry={retry:?}: {error:#}"));
                    for (path, inode, _, _, mode, bytes) in before {
                        if mode & libc::S_IFMT != libc::S_IFREG {
                            continue;
                        }
                        let metadata = metadata(&path);
                        assert_eq!(metadata.ino(), inode);
                        assert_eq!(metadata.uid(), retry.ids().0);
                        assert_eq!(std::fs::read(path).unwrap(), bytes);
                    }
                    for path in [RUNTIME, STAGING, MUTEX] {
                        assert_eq!(metadata(f.0.join(path)).uid(), retry.ids().0);
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
#[ignore = "requires root to exercise interrupted publication"]
fn published_record_never_replays_file_ownership() {
    for point in ["staging published", "runtime published"] {
        let f = interrupted(point);
        let staging = f.0.join(STAGING);
        let deleted = staging.join("01980000-0000-7000-8000-000000000001");
        std::fs::remove_file(&deleted).unwrap();
        // Model a target service moving a legacy-owned file from a writable
        // shared parent into its published staging directory.
        f.file(
            "cache/firecracker/.volmgr-staging/foreign",
            b"legacy secret",
        );
        let foreign = staging.join("foreign");
        let before = metadata(&foreign);
        prepare(&f.0, Identity::Separate).unwrap();
        let after = metadata(&foreign);
        assert_eq!(
            (after.ino(), after.uid(), after.mode()),
            (before.ino(), 950, before.mode())
        );
        assert!(!deleted.exists());
        // A new transaction must reject that foreign file in either direction.
        rejected(prepare(&f.0, Identity::Legacy));
        assert_eq!(metadata(foreign).uid(), 950);
    }
}

#[test]
#[ignore = "requires root to exercise replacement races"]
fn replacement_between_validation_and_sealing_is_rejected() {
    for point in ["state validated", "owner changed"] {
        let f = Fixture::new();
        let victim = f.0.join(DATABASE);
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
            || prepare(&root, Identity::Separate),
        );
        rejected(result);
        assert!(original.exists(), "checkpoint must execute");
        assert_eq!(metadata(&victim).uid(), 950);
        for dir in [RUNTIME, STAGING] {
            assert_eq!(metadata(f.0.join(dir)).uid(), 950);
        }
    }
}

#[test]
#[ignore = "requires root to exercise missing retained directories"]
fn pending_record_never_recreates_missing_directories() {
    for path in [RUNTIME, STAGING] {
        let f = interrupted("publishing durable");
        let original = f.0.join(path);
        std::fs::rename(&original, f.0.join("retained-original")).unwrap();
        rejected(prepare(&f.0, Identity::Separate));
        assert!(!original.exists());
        std::fs::rename(f.0.join("retained-original"), &original).unwrap();
        prepare(&f.0, Identity::Separate).unwrap();
    }
}

#[test]
#[ignore = "requires root and real service credentials"]
fn replacing_published_staging_cannot_grant_legacy_files() {
    for point in ["staging published", "runtime published"] {
        let f = Fixture::new();
        let cache = f.0.join("cache/firecracker");
        own(&cache, 0, 952, 0o3775);
        let untrusted = cache.join("untrusted");
        std::fs::create_dir(&untrusted).unwrap();
        own(&untrusted, 964, 952, 0o750);
        let foreign = untrusted.join("foreign");
        std::fs::create_dir(&foreign).unwrap();
        own(&foreign, 950, 952, 0o770);
        f.file(
            "cache/firecracker/untrusted/foreign/secret",
            b"legacy secret",
        );
        let original_inode = metadata(&foreign).ino();
        let cache_hook = cache.clone();
        let result = at_checkpoints(
            move |seen| {
                if seen == point {
                    for (from, to) in [
                        (".volmgr-staging", "saved"),
                        ("untrusted/foreign", ".volmgr-staging"),
                    ] {
                        succeeds(
                            as_user(964)
                                .arg("mv")
                                .arg(cache_hook.join(from))
                                .arg(cache_hook.join(to)),
                        );
                    }
                }
            },
            || prepare(&f.0, Identity::Separate),
        );
        rejected(result);
        let planted = cache.join(".volmgr-staging");
        let metadata = metadata(&planted);
        assert_eq!(
            (metadata.ino(), metadata.uid(), metadata.mode() & 0o777),
            (original_inode, 950, 0o770)
        );
        assert!(
            !as_user(964)
                .arg("cat")
                .arg(planted.join("secret"))
                .output()
                .unwrap()
                .status
                .success()
        );
        for identity in [Identity::Legacy, Identity::Separate] {
            rejected(prepare(&f.0, identity));
        }
    }
}

#[test]
#[ignore = "requires root and access ACL support"]
fn installation_trust_checks_ancestors_and_symlink_boundaries() {
    for (uid, gid, mode, acl, allowed) in [
        (0, 0, 0o755, false, true),
        (964, 964, 0o755, false, false),
        (0, 952, 0o775, false, false),
        (1000, 1000, 0o2775, false, true),
        (1000, 1000, 0o1777, false, true),
        (1000, 1000, 0o777, false, false),
        (1000, 1000, 0o755, true, false),
    ] {
        let f = Fixture::new();
        let parent = f.0.join("parent");
        std::fs::create_dir(&parent).unwrap();
        own(&parent, uid, gid, mode);
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
        let mut installation = Fixture::new();
        let nested = parent.join("installation");
        let link = f.0.join("space-link");
        symlink(&nested, &link).unwrap();
        if uid == 0 && allowed {
            symlink(&installation.0, &nested).unwrap();
            rejected(prepare(&link, Identity::Separate));
            assert_eq!(metadata(installation.0.join(DATABASE)).uid(), 950);
            std::fs::remove_file(&nested).unwrap();
        }
        std::fs::rename(&installation.0, &nested).unwrap();
        installation.0 = nested;
        for link_uid in [964, 0] {
            if link_uid != 0 && !(uid == 0 && allowed) {
                continue;
            }
            std::os::unix::fs::lchown(&link, Some(link_uid), Some(0)).unwrap();
            let result = prepare(&link, Identity::Separate);
            let accepted = allowed && link_uid == 0;
            if accepted {
                result.unwrap();
            } else {
                rejected(result);
            }
            assert_eq!(
                metadata(installation.0.join(DATABASE)).uid(),
                if accepted { 964 } else { 950 }
            );
        }
    }
}

#[test]
#[ignore = "requires root and real service credentials"]
fn services_cannot_read_or_forge_recovery_records() {
    const CHILD: &str = "SPACE_HANDOFF_RECORD_ACCESS";
    const CHECKED: &str = "RECORD_ACCESS_CHECKED";
    if let Some(root) = std::env::var_os(CHILD) {
        let root = PathBuf::from(root);
        let uid: u32 = std::env::var("SPACE_HANDOFF_PEER_UID")
            .unwrap()
            .parse()
            .unwrap();
        // SAFETY: geteuid only reads the subprocess identity.
        assert_eq!(unsafe { libc::geteuid() }, uid);
        assert!(matches!(uid, 950 | 964));
        let control = root.join("run/access-control");
        assert_eq!(std::fs::read(&control).unwrap(), b"accessible");
        std::fs::write(&control, b"accessible").unwrap();
        let directory = root.join("run/.volume-manager-identity");
        let record = directory.join("pending.json");
        for result in [
            std::fs::read(&record).map(|_| ()),
            std::fs::OpenOptions::new()
                .write(true)
                .open(&record)
                .map(|_| ()),
            std::fs::write(directory.join("forged.json"), b"forged"),
            std::fs::remove_file(&record),
        ] {
            assert_eq!(result.unwrap_err().raw_os_error(), Some(libc::EACCES));
        }
        println!("{CHECKED}");
        return;
    }
    let f = interrupted("publishing durable");
    let control = f.0.join("run/access-control");
    std::fs::write(&control, b"accessible").unwrap();
    own(&control, 0, 952, 0o660);
    let record = f.0.join("run/.volume-manager-identity/pending.json");
    let original = std::fs::read(&record).unwrap();
    for uid in [950, 964] {
        let output = succeeds(
            as_user(uid)
                .arg(std::env::current_exe().unwrap())
                .args(test_arguments(
                    "services_cannot_read_or_forge_recovery_records",
                ))
                .env(CHILD, &f.0)
                .env("SPACE_HANDOFF_PEER_UID", uid.to_string()),
        );
        assert!(
            String::from_utf8(output.stdout)
                .unwrap()
                .lines()
                .any(|line| line == CHECKED),
            "permission child must execute its checks"
        );
    }
    assert_eq!(std::fs::read(record).unwrap(), original);
    prepare(&f.0, Identity::Separate).unwrap();
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
        own(&parent, 0, 952, 0o2775);
        let foreign = parent.join("private");
        std::fs::create_dir(&foreign).unwrap();
        let staged = if replace_cache {
            own(&foreign, 0, 952, 0o3775);
            let staged = foreign.join(".volmgr-staging");
            std::fs::create_dir(&staged).unwrap();
            staged
        } else {
            foreign.clone()
        };
        own(&staged, 950, 960, 0o700);
        let secret = staged.join("secret");
        std::fs::write(&secret, b"legacy secret").unwrap();
        own(&secret, 950, 960, 0o600);
        let target = if replace_cache {
            "firecracker"
        } else {
            ".volmgr-staging"
        };
        for (from, to) in [(target, "saved"), ("private", target)] {
            succeeds(
                as_user(964)
                    .arg("mv")
                    .arg(parent.join(from))
                    .arg(parent.join(to)),
            );
        }
        let result = prepare(&f.0, Identity::Separate);
        let secret = f.0.join("cache/firecracker/.volmgr-staging/secret");
        assert_eq!(metadata(&secret).uid(), 950);
        assert!(
            !as_user(964)
                .arg("cat")
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
    prepare(&f.0, Identity::Separate).unwrap();
    let before = f.snapshot();
    rejected(super::prepare(
        &f.0,
        Identity::Legacy,
        HANDOFF_TIMEOUT,
        Some(Identity::Legacy),
    ));
    assert_eq!(f.snapshot(), before);
    prepare(&f.0, Identity::Legacy).unwrap();
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
    prepare(&f.0, Identity::Legacy).unwrap();
}

#[test]
#[ignore = "requires root and real service credentials"]
fn initial_handoff_rejects_service_writable_private_state() {
    for area in [RUNTIME, STAGING] {
        let f = Fixture::new();
        let private = f.0.join(area);
        own(&private, 950, 952, 0o2775);
        let untrusted = f.0.join("untrusted");
        std::fs::create_dir(&untrusted).unwrap();
        own(&untrusted, 964, 964, 0o700);
        f.file("untrusted/secret", b"legacy secret");
        let destination = private.join(if area.starts_with("run") {
            "volume-manager.db"
        } else {
            "secret"
        });
        succeeds(
            as_user(964)
                .arg("mv")
                .arg(untrusted.join("secret"))
                .arg(&destination),
        );
        let result = prepare(&f.0, Identity::Separate);
        assert_eq!(metadata(&destination).uid(), 950);
        rejected(result);
    }
}
