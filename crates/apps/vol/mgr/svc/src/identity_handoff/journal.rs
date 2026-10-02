use super::*;
use std::io::Write;

use serde::{Deserialize, Serialize};

const MAX_RECORD_BYTES: u64 = 64 * 1024 * 1024;

const DIRECTORY: &str = ".volume-manager-identity";
const PENDING: &str = "pending.json";
const TEMPORARY: &str = "pending.new";
const MUTEX_TEMPORARY: &str = "mutex.new";

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
pub(super) enum Phase {
    Prepared,
    Transferring,
    Publishing(Identity),
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
pub(super) enum Area {
    Runtime,
    Staging,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
pub(super) enum Action {
    OwnFile,
    OwnMutex,
    KeepSocket,
    RemoveSocket,
}

#[derive(Debug, Deserialize, Serialize)]
pub(super) struct Inode {
    pub dev: u64,
    pub ino: u64,
}

impl Inode {
    pub fn of(metadata: &Metadata) -> Self {
        Self {
            dev: metadata.dev(),
            ino: metadata.ino(),
        }
    }

    pub fn matches(&self, metadata: &Metadata) -> bool {
        (self.dev, self.ino) == (metadata.dev(), metadata.ino())
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub(super) struct Directory {
    pub inode: Inode,
    pub uid: u32,
    pub gid: u32,
    pub mode: u32,
}

impl Directory {
    pub fn of(file: &File) -> anyhow::Result<Self> {
        let metadata = file.metadata()?;
        Ok(Self {
            inode: Inode::of(&metadata),
            uid: metadata.uid(),
            gid: metadata.gid(),
            mode: metadata.mode() & 0o7777,
        })
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub(super) struct Entry {
    pub area: Area,
    pub name: Vec<u8>,
    pub inode: Inode,
    pub uid: u32,
    pub action: Action,
}

impl Entry {
    pub fn name(&self) -> anyhow::Result<&OsStr> {
        ensure!(
            !self.name.is_empty()
                && !self.name.contains(&0)
                && !self.name.contains(&b'/')
                && self.name != b"."
                && self.name != b"..",
            "invalid retained name in identity record"
        );
        Ok(OsStr::from_bytes(&self.name))
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub(super) struct Record {
    pub version: u32,
    pub source: Identity,
    pub target: Identity,
    pub phase: Phase,
    pub runtime: Directory,
    pub staging: Directory,
    pub entries: Vec<Entry>,
}

pub(super) struct Journal {
    directory: File,
    // A separate permanent lock survives atomic replacement of the record.
    _lock: File,
}

impl Journal {
    pub fn open(parent: &File, timeout: Duration) -> anyhow::Result<Self> {
        let directory = directory(parent, DIRECTORY)?;
        let metadata = directory.metadata()?;
        ensure!(
            metadata.uid() == 0 && metadata.mode() & 0o777 == 0o700,
            "identity journal directory must be root-owned 0700"
        );
        parent.sync_all()?;
        let lock = match open(
            directory.as_raw_fd(),
            OsStr::new("lock"),
            libc::O_RDWR | libc::O_CREAT | libc::O_EXCL,
            0o600,
            true,
        ) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                regular(&directory, OsStr::new("lock"), 0)?
            }
            Err(error) => return Err(error.into()),
        };
        ensure!(
            lock.metadata()?.mode() & 0o077 == 0,
            "identity lock is not private"
        );
        lock_exclusive(&lock, timeout, "another identity handoff owns the journal")?;
        let journal = Self {
            directory,
            _lock: lock,
        };
        // These names are reachable only by root and are never published state.
        journal.discard_temporary(TEMPORARY)?;
        journal.discard_temporary(MUTEX_TEMPORARY)?;
        Ok(journal)
    }

    fn discard_temporary(&self, name: &str) -> anyhow::Result<()> {
        match open(
            self.directory.as_raw_fd(),
            OsStr::new(name),
            libc::O_PATH,
            0,
            true,
        ) {
            Ok(file) => {
                let metadata = file.metadata()?;
                ensure!(
                    metadata.is_file() && metadata.nlink() == 1,
                    "invalid journal temporary"
                );
                unlink(&self.directory, OsStr::new(name))?;
                self.directory.sync_all()?;
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }

    pub fn read(&self) -> anyhow::Result<Option<Record>> {
        match open(
            self.directory.as_raw_fd(),
            OsStr::new(PENDING),
            libc::O_PATH,
            0,
            true,
        ) {
            Ok(file) => {
                let metadata = file.metadata()?;
                ensure!(
                    metadata.is_file()
                        && metadata.nlink() == 1
                        && metadata.uid() == 0
                        && metadata.mode() & 0o777 == 0o600
                        && metadata.len() <= MAX_RECORD_BYTES,
                    "invalid identity recovery record"
                );
                let file = File::open(format!("/proc/self/fd/{}", file.as_raw_fd()))?;
                let record: Record = serde_json::from_reader(file)?;
                ensure!(record.version == 1, "unsupported identity recovery record");
                for entry in &record.entries {
                    entry.name()?;
                }
                Ok(Some(record))
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    pub fn write(&self, record: &Record) -> anyhow::Result<()> {
        self.discard_temporary(TEMPORARY)?;
        let mut file = open(
            self.directory.as_raw_fd(),
            OsStr::new(TEMPORARY),
            libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL,
            0o600,
            true,
        )?;
        serde_json::to_writer(&mut file, record)?;
        file.flush()?;
        // Reserve room for the longer phase names before sealing any state.
        let reserve = if record.phase == Phase::Prepared {
            64
        } else {
            0
        };
        ensure!(
            file.metadata()?.len() + reserve <= MAX_RECORD_BYTES,
            "identity recovery record is too large"
        );
        file.sync_all()?;
        checkpoint("journal file synced");
        rename(&self.directory, TEMPORARY, &self.directory, PENDING, 0)?;
        checkpoint("journal renamed");
        self.directory.sync_all()?;
        checkpoint("journal committed");
        Ok(())
    }

    pub fn clear(&self) -> anyhow::Result<()> {
        unlink(&self.directory, OsStr::new(PENDING))?;
        checkpoint("journal unlinked");
        self.directory.sync_all()?;
        checkpoint("journal retired");
        Ok(())
    }

    pub fn create_mutex(
        &self,
        runtime: &File,
        name: &str,
        source: Identity,
    ) -> anyhow::Result<File> {
        ensure!(
            names(runtime)?.is_empty(),
            "retained runtime state has no service mutex"
        );
        let file = open(
            self.directory.as_raw_fd(),
            OsStr::new(MUTEX_TEMPORARY),
            libc::O_RDWR | libc::O_CREAT | libc::O_EXCL,
            0o600,
            true,
        )?;
        checkpoint("mutex created privately");
        let (uid, gid) = source.ids();
        set_owner(&file, uid, gid, 0o660)?;
        lock_exclusive(&file, Duration::ZERO, "new mutex was unexpectedly locked")?;
        file.sync_all()?;
        // Publish the fully initialized inode; a killed creator cannot leave
        // a root-owned service mutex that a later handoff would have to guess at.
        rename(
            &self.directory,
            MUTEX_TEMPORARY,
            runtime,
            name,
            libc::RENAME_NOREPLACE,
        )?;
        checkpoint("mutex published");
        runtime.sync_all()?;
        self.directory.sync_all()?;
        Ok(file)
    }
}

fn rename(from: &File, old: &str, to: &File, new: &str, flags: u32) -> anyhow::Result<()> {
    let old = CString::new(old)?;
    let new = CString::new(new)?;
    // SAFETY: both directory descriptors and C strings remain live for the syscall.
    if unsafe {
        libc::renameat2(
            from.as_raw_fd(),
            old.as_ptr(),
            to.as_raw_fd(),
            new.as_ptr(),
            flags,
        )
    } < 0
    {
        return Err(io::Error::last_os_error().into());
    }
    Ok(())
}
