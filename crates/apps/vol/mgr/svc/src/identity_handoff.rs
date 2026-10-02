use std::collections::HashSet;
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

mod journal;

use journal::{Action, Area, Directory, Entry, Inode, Journal, Phase, Record};

#[derive(Clone, Copy, Debug, PartialEq, clap::ValueEnum, serde::Deserialize, serde::Serialize)]
pub enum Identity {
    Legacy,
    Separate,
}

impl Identity {
    fn from_uid(uid: u32) -> anyhow::Result<Self> {
        match uid {
            value if value == users::SPACE_SBXMGR.uid => Ok(Self::Legacy),
            value if value == users::SPACE_VOLMGR.uid => Ok(Self::Separate),
            _ => bail!("unexpected private-directory owner {uid}"),
        }
    }

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
    checkpoint("owner changed");
    file.set_permissions(Permissions::from_mode(mode))?;
    checkpoint("mode changed");
    Ok(())
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

fn lock_exclusive(file: &File, timeout: Duration, message: &str) -> anyhow::Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        // SAFETY: the caller retains the descriptor for the whole operation.
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
            return Ok(());
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::WouldBlock {
            return Err(error.into());
        }
        ensure!(Instant::now() < deadline, "{message}");
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn unlink(parent: &File, name: &OsStr) -> anyhow::Result<()> {
    let name = CString::new(name.as_bytes())?;
    // SAFETY: parent is pinned and the supplied name is one checked component.
    if unsafe { libc::unlinkat(parent.as_raw_fd(), name.as_ptr(), 0) } < 0 {
        return Err(io::Error::last_os_error().into());
    }
    Ok(())
}

fn installation_root(path: &Path) -> anyhow::Result<File> {
    use pplx_space_common_core::host::linux::identities::{ALL_GROUPS, ALL_USERS};
    use std::path::Component;

    fn walk(path: &Path, allow_link: bool) -> anyhow::Result<File> {
        ensure!(path.is_absolute(), "installation path must be absolute");
        let mut parent = File::open("/")?;
        let components: Vec<_> = path.components().skip(1).collect();
        for (index, component) in components.iter().enumerate() {
            let Component::Normal(name) = component else {
                bail!("installation path contains a non-normal component")
            };
            let pm = parent.metadata()?;
            // Administrators may keep an installation beneath their home.
            // Services must not be able to replace any ancestor, including
            // through ACL grants that are invisible in the owning group.
            ensure!(
                !ALL_USERS.iter().any(|u| u.uid == pm.uid()),
                "service-owned installation ancestor"
            );
            // SAFETY: parent is a directory FD and the name is NUL terminated;
            // a zero-sized query reads only the attribute's length.
            let acl = unsafe {
                libc::fgetxattr(
                    parent.as_raw_fd(),
                    c"system.posix_acl_access".as_ptr(),
                    std::ptr::null_mut(),
                    0,
                )
            };
            ensure!(
                acl < 0
                    && matches!(
                        io::Error::last_os_error().raw_os_error(),
                        Some(libc::ENODATA | libc::EOPNOTSUPP)
                    ),
                "installation ancestor has an access ACL or cannot be checked"
            );
            let final_component = index + 1 == components.len();
            // O_PATH|O_NOFOLLOW may pin the final link itself. Intermediate
            // components must be actual directories, never further redirects.
            let child = open(parent.as_raw_fd(), name, libc::O_PATH, 0, false)?;
            let cm = child.metadata()?;
            let service_owned = ALL_USERS.iter().any(|u| u.uid == cm.uid());
            let writable = pm.mode() & 0o002 != 0
                || (pm.mode() & 0o020 != 0 && ALL_GROUPS.iter().any(|g| g.gid == pm.gid()));
            ensure!(
                !service_owned && (!writable || pm.mode() & libc::S_ISVTX != 0),
                "service-writable installation path"
            );
            if cm.is_symlink() {
                ensure!(
                    final_component && allow_link && cm.uid() == 0,
                    "untrusted installation redirect"
                );
                let mut target = vec![0u8; libc::PATH_MAX as usize];
                // SAFETY: the empty name reads this pinned O_PATH symlink;
                // target is writable for the entire advertised length.
                let len = unsafe {
                    libc::readlinkat(
                        child.as_raw_fd(),
                        c"".as_ptr(),
                        target.as_mut_ptr().cast(),
                        target.len(),
                    )
                };
                let len = usize::try_from(len).context("read installation redirect")?;
                ensure!(len < target.len(), "invalid installation redirect");
                target.truncate(len);
                return walk(Path::new(OsStr::from_bytes(&target)), false);
            }
            ensure!(cm.is_dir(), "installation ancestor is not a directory");
            parent = File::open(format!("/proc/self/fd/{}", child.as_raw_fd()))?;
        }
        Ok(parent)
    }
    let root = walk(path, true)?;
    let metadata = root.metadata()?;
    ensure!(
        metadata.is_dir() && metadata.uid() == 0 && metadata.mode() & 0o022 == 0,
        "node root must be root-owned and not writable by services"
    );
    Ok(root)
}

struct State {
    run_parent: File,
    cache_parent: File,
    runtime: File,
    staging: File,
    runtime_name: &'static str,
    staging_name: &'static str,
    mutex_name: &'static str,
    socket_name: &'static str,
    database_names: [String; 4],
}

impl State {
    fn directory(&self, area: Area) -> &File {
        match area {
            Area::Runtime => &self.runtime,
            Area::Staging => &self.staging,
        }
    }

    fn attached(&self) -> anyhow::Result<()> {
        unchanged(&self.run_parent, self.runtime_name, &self.runtime)?;
        unchanged(&self.cache_parent, self.staging_name, &self.staging)
    }

    fn action(&self, area: Area, name: &OsStr) -> anyhow::Result<Action> {
        if area == Area::Staging || self.database_names.iter().any(|db| name == db.as_str()) {
            Ok(Action::OwnFile)
        } else if name == self.mutex_name {
            Ok(Action::OwnMutex)
        } else if name == self.socket_name {
            Ok(Action::KeepSocket)
        } else if is_unix_socket_staging_name(Path::new(self.socket_name), name) {
            Ok(Action::RemoveSocket)
        } else {
            bail!("unexpected runtime file: {name:?}")
        }
    }

    fn plan(&self, target: Identity) -> anyhow::Result<Record> {
        let source = Identity::from_uid(self.runtime.metadata()?.uid())?;
        let staging = self.staging.metadata()?;
        ensure!(
            staging.uid() == source.ids().0
                || (staging.uid() == 0
                    && staging.mode() & 0o022 == 0
                    && names(&self.staging)?.is_empty()),
            "staging owner disagrees with runtime identity; nonempty root-owned staging directory was not sealed by this handoff"
        );
        for directory in [&self.runtime, &self.staging] {
            ensure!(
                directory.metadata()?.mode() & 0o022 == 0,
                "private retained directory was writable by other identities; inspect provenance before migration"
            );
        }
        let mut entries = Vec::new();
        for area in [Area::Runtime, Area::Staging] {
            let directory = self.directory(area);
            let mut names = names(directory)?;
            names.sort();
            for name in names {
                let action = self.action(area, &name)?;
                let file = open(directory.as_raw_fd(), &name, libc::O_PATH, 0, true)?;
                let metadata = file.metadata()?;
                check_type(&metadata, action)?;
                if action == Action::KeepSocket {
                    // The daemon replaces its published socket after taking
                    // this mutex; a completed handoff may retain the old UID.
                    Identity::from_uid(metadata.uid())?;
                } else {
                    file_owner(&metadata, source.ids().0)?;
                }
                entries.push(Entry {
                    area,
                    name: name.as_bytes().to_vec(),
                    inode: Inode::of(&metadata),
                    uid: metadata.uid(),
                    action,
                });
            }
        }
        self.attached()?;
        checkpoint("state validated");
        Ok(Record {
            version: 1,
            source,
            target,
            phase: Phase::Prepared,
            runtime: Directory::of(&self.runtime)?,
            staging: Directory::of(&self.staging)?,
            entries,
        })
    }

    fn directory_inodes(&self, record: &Record) -> anyhow::Result<()> {
        ensure!(
            record.runtime.inode.matches(&self.runtime.metadata()?),
            "recorded runtime directory was replaced"
        );
        ensure!(
            record.staging.inode.matches(&self.staging.metadata()?),
            "recorded staging directory was replaced"
        );
        self.attached()
    }

    fn validate_entries(&self, record: &Record) -> anyhow::Result<()> {
        self.directory_inodes(record)?;
        for area in [Area::Runtime, Area::Staging] {
            let directory = self.directory(area);
            let expected: HashSet<&[u8]> = record
                .entries
                .iter()
                .filter(|entry| entry.area == area)
                .map(|entry| entry.name.as_slice())
                .collect();
            for name in names(directory)? {
                ensure!(
                    expected.contains(name.as_bytes()),
                    "unjournaled retained entry: {name:?}"
                );
            }
        }
        for entry in &record.entries {
            self.checked_entry(record, entry)?;
        }
        Ok(())
    }

    fn checked_entry(&self, record: &Record, entry: &Entry) -> anyhow::Result<Option<File>> {
        let name = entry.name()?;
        ensure!(
            self.action(entry.area, name)? == entry.action,
            "invalid recorded action"
        );
        let file = match open(
            self.directory(entry.area).as_raw_fd(),
            name,
            libc::O_PATH,
            0,
            true,
        ) {
            Ok(file) => file,
            Err(error)
                if error.kind() == io::ErrorKind::NotFound
                    && entry.action == Action::RemoveSocket
                    && record.phase == Phase::Transferring =>
            {
                return Ok(None);
            }
            Err(error) => return Err(error.into()),
        };
        let metadata = file.metadata()?;
        ensure!(
            entry.inode.matches(&metadata),
            "recorded retained inode was replaced: {name:?}"
        );
        check_type(&metadata, entry.action)?;
        let uid = metadata.uid();
        let owned_by_source = uid
            == if entry.action == Action::KeepSocket {
                entry.uid
            } else {
                record.source.ids().0
            };
        let transferred = record.phase == Phase::Transferring
            && matches!(entry.action, Action::OwnFile | Action::OwnMutex)
            && uid == record.target.ids().0;
        ensure!(
            owned_by_source || transferred,
            "unexpected recorded-file owner {uid}: {name:?}"
        );
        Ok(Some(file))
    }

    fn restore_prepared(&self, record: &Record) -> anyhow::Result<()> {
        // Only directory access changed in Prepared. Restore pinned originals,
        // staging first, even if an attacker replaced the path to either leaf.
        for (directory, original) in [
            (&self.staging, &record.staging),
            (&self.runtime, &record.runtime),
        ] {
            let metadata = directory.metadata()?;
            ensure!(
                original.inode.matches(&metadata)
                    && (metadata.uid() == 0 || metadata.uid() == original.uid),
                "cannot restore replaced directory"
            );
            set_owner(directory, original.uid, original.gid, original.mode)?;
            directory.sync_all()?;
        }
        Ok(())
    }

    fn finish(
        &self,
        journal: &Journal,
        record: &mut Record,
        requested: Identity,
        mutex: &File,
    ) -> anyhow::Result<Identity> {
        self.directory_inodes(record)?;
        unchanged(&self.runtime, self.mutex_name, mutex)?;
        let mutex_entry = record
            .entries
            .iter()
            .find(|entry| entry.action == Action::OwnMutex)
            .context("identity record has no service mutex")?;
        ensure!(
            mutex_entry.inode.matches(&mutex.metadata()?),
            "recorded mutex was replaced"
        );
        if record.phase == Phase::Prepared {
            let seal = (|| {
                for (directory, original) in [
                    (&self.runtime, &record.runtime),
                    (&self.staging, &record.staging),
                ] {
                    let metadata = directory.metadata()?;
                    ensure!(
                        metadata.uid() == 0 || metadata.uid() == original.uid,
                        "unexpected directory owner during preparation"
                    );
                }
                self.validate_entries(record)?;
                freeze(&self.runtime)?;
                self.runtime.sync_all()?;
                freeze(&self.staging)?;
                self.staging.sync_all()?;
                self.validate_entries(record)?;
                checkpoint("sealed state validated");
                Ok::<_, anyhow::Error>(())
            })();
            if let Err(error) = seal {
                self.restore_prepared(record)
                    .context("restore directory access after rejected preparation")?;
                journal.clear()?;
                return Err(error);
            }
            record.phase = Phase::Transferring;
            journal.write(record)?;
        }
        if record.phase == Phase::Transferring {
            for directory in [&self.runtime, &self.staging] {
                let metadata = directory.metadata()?;
                ensure!(
                    metadata.uid() == 0 && metadata.gid() == 0 && metadata.mode() & 0o7777 == 0o700,
                    "recorded transfer directories are not sealed"
                );
            }
            self.validate_entries(record)?;
            // Either original identity is recoverable while sealed. Do not
            // expose a failed target merely to get back to the original owner.
            let destination = if requested == record.source {
                record.source
            } else {
                record.target
            };
            let (uid, gid) = destination.ids();
            for entry in &record.entries {
                let Some(file) = self.checked_entry(record, entry)? else {
                    continue;
                };
                match entry.action {
                    Action::OwnFile | Action::OwnMutex => {
                        let file = File::open(format!("/proc/self/fd/{}", file.as_raw_fd()))?;
                        let file_gid = if entry.area == Area::Staging {
                            groups::SPACE_CACHE_RW.gid
                        } else {
                            gid
                        };
                        set_owner(
                            &file,
                            uid,
                            file_gid,
                            if entry.action == Action::OwnMutex {
                                0o660
                            } else {
                                0o600
                            },
                        )?;
                        file.sync_all()?;
                    }
                    Action::RemoveSocket => {
                        unlink(self.directory(entry.area), entry.name()?)?;
                        checkpoint("abandoned socket removed");
                    }
                    Action::KeepSocket => {}
                }
            }
            self.directory_inodes(record)?;
            unchanged(&self.runtime, self.mutex_name, mutex)?;
            self.runtime.sync_all()?;
            self.staging.sync_all()?;
            // This durable phase revokes every per-file mutation permission
            // before fchown makes either directory accessible to the service.
            record.phase = Phase::Publishing(destination);
            journal.write(record)?;
            checkpoint("publishing durable");
        }
        let Phase::Publishing(destination) = record.phase else {
            unreachable!()
        };
        let (uid, _) = destination.ids();
        ensure!(
            mutex.metadata()?.uid() == uid,
            "published mutex owner disagrees with record"
        );
        for (directory, gid, point) in [
            (
                &self.staging,
                groups::SPACE_CACHE_RW.gid,
                "staging published",
            ),
            (
                &self.runtime,
                groups::SPACE_SBXCLIENTS.gid,
                "runtime published",
            ),
        ] {
            self.directory_inodes(record)?;
            let metadata = directory.metadata()?;
            ensure!(
                metadata.uid() == 0 || metadata.uid() == uid,
                "unexpected published directory owner"
            );
            set_owner(directory, uid, gid, 0o750)?;
            directory.sync_all()?;
            checkpoint(point);
        }
        self.directory_inodes(record)?;
        journal.clear()?;
        Ok(destination)
    }
}

fn check_type(metadata: &Metadata, action: Action) -> anyhow::Result<()> {
    let correct_type = match action {
        Action::OwnFile | Action::OwnMutex => metadata.is_file(),
        Action::KeepSocket | Action::RemoveSocket => metadata.file_type().is_socket(),
    };
    ensure!(
        correct_type && metadata.nlink() == 1,
        "retained entry has an unsafe type or link count"
    );
    Ok(())
}

pub fn prepare(
    root: &Path,
    identity: Identity,
    timeout: Duration,
    expected: Option<Identity>,
) -> anyhow::Result<()> {
    prepare_inner(root, identity, timeout, expected).context(
        "volume-manager identity handoff failed; preserve retained files, service.mutex and the root-owned identity recovery record; retry with the required identity",
    )
}

fn prepare_inner(
    root: &Path,
    identity: Identity,
    timeout: Duration,
    expected: Option<Identity>,
) -> anyhow::Result<()> {
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
    let leaf = |path: &'static Path| -> anyhow::Result<&'static str> {
        path.file_name()
            .and_then(OsStr::to_str)
            .context("retained filename")
    };
    let runtime_name = leaf(runtime_path)?;
    let staging_name = leaf(staging_path)?;
    let mutex_name = leaf(mutex_path)?;
    let socket_name = leaf(socket_path)?;
    let database_name = leaf(database_path)?;
    let node_root = installation_root(root)?;
    let parent = |path: &Path| -> anyhow::Result<File> {
        Ok(open(
            node_root.as_raw_fd(),
            path.parent()
                .context("retained parent")?
                .strip_prefix(space_path)?
                .as_os_str(),
            libc::O_RDONLY | libc::O_DIRECTORY,
            0,
            false,
        )?)
    };
    let run_parent = parent(runtime_path)?;
    let metadata = run_parent.metadata()?;
    ensure!(
        metadata.uid() == 0 && metadata.mode() & 0o022 == 0,
        "runtime parent must be root-owned and not writable by services"
    );
    // Both shared ancestors protect root-owned children from replacement.
    // Adding sticky bits to retained state now cannot prove its provenance.
    let mut cache_parent = node_root.try_clone()?;
    for component in staging_path
        .parent()
        .context("staging parent")?
        .strip_prefix(space_path)?
        .components()
    {
        cache_parent = open(
            cache_parent.as_raw_fd(),
            component.as_os_str(),
            libc::O_RDONLY | libc::O_DIRECTORY,
            0,
            false,
        )?;
        let metadata = cache_parent.metadata()?;
        ensure!(
            metadata.uid() == 0
                && (metadata.mode() & 0o022 == 0 || metadata.mode() & libc::S_ISVTX != 0),
            "unsafe shared cache ancestor {component:?}; retained state needs provenance inspection or node replacement, not permission repair"
        );
    }
    let journal = Journal::open(&run_parent, timeout)?;
    let mut pending = journal.read()?;
    let retained_directory = |parent: &File, name: &str| -> anyhow::Result<File> {
        if pending.is_some() {
            Ok(open(
                parent.as_raw_fd(),
                OsStr::new(name),
                libc::O_RDONLY | libc::O_DIRECTORY,
                0,
                true,
            )?)
        } else {
            directory(parent, name)
        }
    };
    let runtime = retained_directory(&run_parent, runtime_name)?;
    checkpoint("runtime opened");
    if pending.is_none() && runtime.metadata()?.uid() == 0 {
        ensure!(
            names(&runtime)?.is_empty(),
            "nonempty root-owned runtime has no identity recovery record"
        );
        let (uid, _) = identity.ids();
        // Publish an empty bootstrap directory's identity before adding its
        // mutex. Every nonempty root-owned runtime then requires a journal.
        set_owner(&runtime, uid, groups::SPACE_SBXCLIENTS.gid, 0o750)?;
        runtime.sync_all()?;
        run_parent.sync_all()?;
    }
    let mutex = match open(
        runtime.as_raw_fd(),
        OsStr::new(mutex_name),
        libc::O_PATH,
        0,
        true,
    ) {
        Ok(file) => {
            let metadata = file.metadata()?;
            check_type(&metadata, Action::OwnMutex)?;
            Identity::from_uid(metadata.uid())?;
            File::open(format!("/proc/self/fd/{}", file.as_raw_fd()))?
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound && pending.is_none() => journal
            .create_mutex(
                &runtime,
                mutex_name,
                Identity::from_uid(runtime.metadata()?.uid())?,
            )?,
        Err(error) => return Err(error.into()),
    };
    lock_exclusive(
        &mutex,
        timeout,
        "old volume-manager still owns service.mutex",
    )?;
    unchanged(&runtime, mutex_name, &mutex)?;
    if let Some(expected) = expected {
        let matches = if let Some(record) = &pending {
            record.source == expected && record.target == expected
        } else {
            runtime.metadata()?.uid() == expected.ids().0
        };
        ensure!(
            matches && identity == expected,
            "retained identity differs from the required deployment identity"
        );
    }
    let staging = retained_directory(&cache_parent, staging_name)?;
    cache_parent.sync_all()?;
    checkpoint("staging opened");
    let state = State {
        run_parent,
        cache_parent,
        runtime,
        staging,
        runtime_name,
        staging_name,
        mutex_name,
        socket_name,
        database_names: [
            database_name.to_owned(),
            format!("{database_name}-wal"),
            format!("{database_name}-shm"),
            format!("{database_name}-journal"),
        ],
    };
    if let Some(record) = pending.as_mut()
        && state.finish(&journal, record, identity, &mutex)? == identity
    {
        return Ok(());
    }
    let mut record = state.plan(identity)?;
    file_owner(&mutex.metadata()?, record.source.ids().0)?;
    journal.write(&record)?;
    state.finish(&journal, &mut record, identity, &mutex)?;
    Ok(())
}

#[cfg(not(test))]
fn checkpoint(_point: &str) {}

#[cfg(test)]
fn checkpoint(point: &str) {
    tests::checkpoint(point);
}

#[cfg(test)]
mod tests;
