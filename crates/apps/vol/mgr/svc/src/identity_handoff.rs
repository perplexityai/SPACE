use std::ffi::{CString, OsStr};
use std::fs::{File, Metadata, Permissions};
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, RawFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::{Context as _, bail, ensure};
use pplx_space_common_core::host::linux::identities::{groups, users};
use pplx_space_common_core::host::linux::paths::{
    SPACE_RUN_DIR, VOLUME_MANAGER_DB, VOLUME_MANAGER_SOCKET, VOLUME_MANAGER_STAGING_DIR,
};
use pplx_space_common_svc::bootstrap::is_unix_socket_staging_name;
use pplx_space_volmgr::run::SERVICE_MUTEX_PATH;

#[derive(Clone, Copy, clap::ValueEnum)]
pub enum Identity {
    Legacy,
    Separate,
}

impl Identity {
    fn ids(self) -> (u32, u32) {
        match self {
            Self::Legacy => (
                users::SPACE_SBXMGR.uid,
                users::SPACE_SBXMGR.primary_group.gid,
            ),
            Self::Separate => (
                users::SPACE_VOLMGR.uid,
                users::SPACE_VOLMGR.primary_group.gid,
            ),
        }
    }
}

#[repr(C)]
struct OpenHow {
    flags: u64,
    mode: u64,
    resolve: u64,
}

fn open(
    parent: RawFd,
    path: &OsStr,
    flags: i32,
    create_mode: u64,
    private: bool,
) -> io::Result<File> {
    let path = CString::new(path.as_bytes())?;
    let how = OpenHow {
        flags: u64::try_from(flags | libc::O_CLOEXEC | libc::O_NOFOLLOW)
            .map_err(io::Error::other)?,
        mode: create_mode,
        // NO_SYMLINKS also rejects magic links. NO_XDEV includes bind mounts;
        // hostPath ancestors may cross mounts, private children may not.
        resolve: 0x04 | if private { 0x01 | 0x08 } else { 0 },
    };
    // SAFETY: path/how are live, correctly sized syscall inputs; no borrowed FD
    // is consumed. The successful return is one newly owned descriptor.
    let fd = unsafe {
        libc::syscall(
            libc::SYS_openat2,
            parent,
            path.as_ptr(),
            &how,
            size_of::<OpenHow>(),
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: successful openat2 returned a new descriptor owned by this call.
    Ok(unsafe { File::from_raw_fd(fd as RawFd) })
}

fn directory(parent: &File, name: &str) -> anyhow::Result<File> {
    let c_name = CString::new(name)?;
    // SAFETY: parent is open and c_name is NUL-terminated. An existing leaf is
    // reopened below with openat2, which refuses links and mount crossings.
    if unsafe { libc::mkdirat(parent.as_raw_fd(), c_name.as_ptr(), 0o700) } < 0 {
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::AlreadyExists {
            return Err(error.into());
        }
    }
    open(
        parent.as_raw_fd(),
        OsStr::new(name),
        libc::O_RDONLY | libc::O_DIRECTORY,
        0,
        true,
    )
    .with_context(|| format!("open private directory {name}"))
}

fn set_owner(file: &File, uid: u32, gid: u32, mode: u32) -> anyhow::Result<()> {
    // SAFETY: file owns a live descriptor and the IDs are canonical identities.
    if unsafe { libc::fchown(file.as_raw_fd(), uid, gid) } < 0 {
        return Err(io::Error::last_os_error().into());
    }
    file.set_permissions(Permissions::from_mode(mode))?;
    Ok(())
}

fn source_owner(directory: &File, target: u32) -> anyhow::Result<u32> {
    let owner = directory.metadata()?.uid();
    if owner == 0 {
        // A sealed leaf carries no proof of its original identity. Inferring
        // legacy ownership here would let a retry launder a planted 950 file.
        return Ok(target);
    }
    ensure!(
        owner == users::SPACE_SBXMGR.uid || owner == users::SPACE_VOLMGR.uid,
        "unexpected private-directory owner {owner}"
    );
    Ok(owner)
}

fn file_owner(metadata: &Metadata, source: u32) -> anyhow::Result<()> {
    ensure!(
        metadata.uid() == source,
        "unexpected retained-file owner {}; expected source identity {source}",
        metadata.uid()
    );
    Ok(())
}

fn freeze(directory: &File) -> anyhow::Result<()> {
    set_owner(directory, 0, 0, 0o700)
}

fn regular(parent: &File, name: &OsStr, source: u32) -> anyhow::Result<File> {
    let file = open(parent.as_raw_fd(), name, libc::O_PATH, 0, true)
        .with_context(|| format!("open retained entry {name:?}"))?;
    let metadata = file.metadata()?;
    ensure!(
        metadata.is_file() && metadata.nlink() == 1,
        "not a single-link regular file: {name:?}"
    );
    file_owner(&metadata, source)?;
    // Reopen the pinned, validated regular inode; opening a retained device
    // before checking its type could have side effects even without a write.
    Ok(File::open(format!("/proc/self/fd/{}", file.as_raw_fd()))?)
}

fn names(directory: &File) -> anyhow::Result<Vec<std::ffi::OsString>> {
    // The proc path names our pinned FD, never an untrusted retained path.
    std::fs::read_dir(format!("/proc/self/fd/{}", directory.as_raw_fd()))?
        .map(|entry| Ok(entry?.file_name()))
        .collect()
}

fn unchanged(parent: &File, name: &str, expected: &File) -> anyhow::Result<()> {
    let observed = open(parent.as_raw_fd(), OsStr::new(name), libc::O_PATH, 0, true)?.metadata()?;
    let expected = expected.metadata()?;
    ensure!(
        (observed.dev(), observed.ino()) == (expected.dev(), expected.ino()),
        "retained path was replaced: {name}"
    );
    Ok(())
}

pub fn prepare(root: &Path, identity: Identity, timeout: Duration) -> anyhow::Result<()> {
    prepare_inner(root, identity, timeout).context(
        "volume-manager identity handoff failed; private directories may be sealed; preserve files and service.mutex for inspection",
    )
}

fn prepare_inner(root: &Path, identity: Identity, timeout: Duration) -> anyhow::Result<()> {
    ensure!(
        // SAFETY: geteuid has no memory or ownership preconditions.
        unsafe { libc::geteuid() } == 0,
        "identity handoff requires root"
    );
    let database_path = Path::new(VOLUME_MANAGER_DB);
    let socket_path = Path::new(VOLUME_MANAGER_SOCKET);
    let mutex_path = Path::new(SERVICE_MUTEX_PATH);
    let staging_path = Path::new(VOLUME_MANAGER_STAGING_DIR);
    let space_path = Path::new(SPACE_RUN_DIR)
        .parent()
        .context("node root path")?;
    let runtime_path = database_path.parent().context("runtime path")?;
    ensure!(
        socket_path.parent() == Some(runtime_path) && mutex_path.parent() == Some(runtime_path),
        "volume-manager retained runtime paths must share one directory"
    );
    let file_name = |path: &'static Path| -> anyhow::Result<&'static str> {
        path.file_name()
            .and_then(OsStr::to_str)
            .context("retained filename")
    };
    let runtime_name = file_name(runtime_path)?;
    let staging_name = file_name(staging_path)?;
    let mutex_name = file_name(mutex_path)?;
    let socket_name = file_name(socket_path)?;
    let database_name = file_name(database_path)?;
    let database_names = [
        database_name.to_owned(),
        format!("{database_name}-wal"),
        format!("{database_name}-shm"),
        format!("{database_name}-journal"),
    ];
    let node_root = open(
        libc::AT_FDCWD,
        root.as_os_str(),
        libc::O_RDONLY | libc::O_DIRECTORY,
        0,
        false,
    )?;
    let root_metadata = node_root.metadata()?;
    ensure!(
        root_metadata.uid() == 0 && root_metadata.mode() & 0o022 == 0,
        "node root must be root-owned and not writable by services"
    );
    let run_parent = open(
        node_root.as_raw_fd(),
        runtime_path
            .parent()
            .context("runtime parent")?
            .strip_prefix(space_path)?
            .as_os_str(),
        libc::O_RDONLY | libc::O_DIRECTORY,
        0,
        false,
    )?;
    let parent_metadata = run_parent.metadata()?;
    ensure!(
        parent_metadata.uid() == 0 && parent_metadata.mode() & 0o022 == 0,
        "runtime parent must be root-owned and not writable by services"
    );
    let cache_parent = open(
        node_root.as_raw_fd(),
        staging_path
            .parent()
            .context("staging parent")?
            .strip_prefix(space_path)?
            .as_os_str(),
        libc::O_RDONLY | libc::O_DIRECTORY,
        0,
        false,
    )?;
    let (uid, gid) = identity.ids();
    let runtime = directory(&run_parent, runtime_name)?;
    let runtime_source = source_owner(&runtime, uid)?;
    let mutex = match open(
        runtime.as_raw_fd(),
        OsStr::new(mutex_name),
        libc::O_RDWR | libc::O_CREAT | libc::O_EXCL | libc::O_NONBLOCK,
        0o600,
        true,
    ) {
        Ok(file) => {
            // Only a newly created inode may be initialized before locking.
            // Never grant access to an existing root-owned file planted here.
            let source_gid = if runtime_source == users::SPACE_SBXMGR.uid {
                users::SPACE_SBXMGR.primary_group.gid
            } else {
                users::SPACE_VOLMGR.primary_group.gid
            };
            set_owner(&file, runtime_source, source_gid, 0o660)?;
            file
        }
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            regular(&runtime, OsStr::new(mutex_name), runtime_source)?
        }
        Err(error) => return Err(error.into()),
    };
    let metadata = mutex.metadata()?;
    ensure!(
        metadata.is_file() && metadata.nlink() == 1,
        "invalid service.mutex"
    );
    file_owner(&metadata, runtime_source)?;
    let deadline = Instant::now() + timeout;
    loop {
        // SAFETY: mutex remains open until the whole handoff completes.
        if unsafe { libc::flock(mutex.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
            break;
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::WouldBlock {
            return Err(error.into());
        }
        ensure!(
            Instant::now() < deadline,
            "old volume-manager still owns service.mutex"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    unchanged(&runtime, mutex_name, &mutex)?;
    freeze(&runtime)?;
    unchanged(&runtime, mutex_name, &mutex)?;
    let staging = directory(&cache_parent, staging_name)?;
    // The cache parent is group-writable, so an attacker can substitute an
    // entire legacy-owned directory there. Only the protected runtime leaf
    // proves the source identity for either private directory.
    let staging_source = runtime_source;
    let staging_owner = source_owner(&staging, uid)?;
    let staging_metadata = staging.metadata()?;
    // Reject before sealing: otherwise a retry could trust the root-owned
    // directory that this failed attempt itself made to look like a handoff.
    if staging_metadata.uid() == 0
        && !(staging_metadata.gid() == 0 && staging_metadata.mode() & 0o7777 == 0o700)
    {
        // A fresh mkdir may inherit the cache's setgid group. Root ownership
        // and no service write make its pinned emptiness stable until sealing.
        ensure!(
            staging_metadata.mode() & 0o022 == 0 && names(&staging)?.is_empty(),
            "nonempty or writable root-owned staging directory was not sealed"
        );
    }
    freeze(&staging)?;
    let staging_names = names(&staging)?;
    let empty_root_staging = staging_metadata.uid() == 0 && staging_names.is_empty();
    // A missing leaf is created root-owned. Only its pinned, sealed emptiness
    // permits using the runtime source instead of inferring interrupted state.
    ensure!(
        staging_owner == staging_source || empty_root_staging,
        "staging owner disagrees with runtime identity"
    );
    let mut runtime_names = names(&runtime)?;
    let mut abandoned_sockets = Vec::new();
    // Validate everything before transferring file ownership. Keep O(1) file
    // descriptors even when a node retains thousands of queued upload sources.
    for name in &runtime_names {
        match name.to_str() {
            Some(name) if name == mutex_name => {}
            Some(name_str) if name_str == socket_name => {
                let socket = open(runtime.as_raw_fd(), name, libc::O_PATH, 0, true)?.metadata()?;
                ensure!(
                    socket.file_type().is_socket() && socket.nlink() == 1,
                    "invalid stale volume socket"
                );
            }
            Some(name_str) if database_names.iter().any(|db| db == name_str) => {
                regular(&runtime, name, runtime_source)?;
            }
            _ if is_unix_socket_staging_name(socket_path, name) => {
                staged_socket(&runtime, name, runtime_source)?;
                abandoned_sockets.push(name.clone());
            }
            _ => bail!("unexpected runtime file: {name:?}"),
        }
    }
    for name in &staging_names {
        // Compression, encryption and sparse comparison leave temporary files
        // beside staged sources. Ownership and FD scope define this boundary,
        // not codec-specific filename conventions.
        regular(&staging, name, staging_source)?;
    }
    unchanged(&run_parent, runtime_name, &runtime)?;
    unchanged(&cache_parent, staging_name, &staging)?;
    // A killed binder cannot clean up its unpublished socket. The same mutex
    // and sealed directory exclude another binder while these are removed.
    for name in abandoned_sockets {
        let socket = staged_socket(&runtime, &name, runtime_source)?;
        unchanged(
            &runtime,
            name.to_str().context("staged socket name")?,
            &socket,
        )?;
        let name = CString::new(name.as_bytes())?;
        // SAFETY: runtime is a pinned directory FD and name is a single component.
        if unsafe { libc::unlinkat(runtime.as_raw_fd(), name.as_ptr(), 0) } < 0 {
            return Err(io::Error::last_os_error().into());
        }
    }
    runtime_names.retain(|name| database_names.iter().any(|db| name == db.as_str()));
    for (directory, entries, file_gid, source) in [
        (&runtime, runtime_names, gid, runtime_source),
        (
            &staging,
            staging_names,
            groups::SPACE_CACHE_RW.gid,
            staging_source,
        ),
    ] {
        for name in entries {
            let file = regular(directory, &name, source)?;
            set_owner(&file, uid, file_gid, 0o600)?;
        }
    }
    unchanged(&run_parent, runtime_name, &runtime)?;
    unchanged(&cache_parent, staging_name, &staging)?;
    unchanged(&runtime, mutex_name, &mutex)?;
    // The mutex inode and every retained byte survive both directions. Stale
    // published socket removal belongs to the daemon after it acquires this same mutex.
    set_owner(&mutex, uid, gid, 0o660)?;
    set_owner(&staging, uid, groups::SPACE_CACHE_RW.gid, 0o750)?;
    set_owner(&runtime, uid, groups::SPACE_SBXCLIENTS.gid, 0o750)?;
    Ok(())
}

fn staged_socket(directory: &File, name: &OsStr, source: u32) -> anyhow::Result<File> {
    let socket = open(directory.as_raw_fd(), name, libc::O_PATH, 0, true)?;
    let metadata = socket.metadata()?;
    ensure!(
        metadata.file_type().is_socket() && metadata.nlink() == 1,
        "invalid staged volume socket"
    );
    file_owner(&metadata, source)?;
    Ok(socket)
}

#[cfg(test)]
mod tests;
