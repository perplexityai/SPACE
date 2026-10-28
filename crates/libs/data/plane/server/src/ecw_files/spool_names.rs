//! Names for upload spool files, reused rather than unique.
//!
//! Creating and unlinking a uniquely named file leaves a negative dentry for
//! each name. The kernel charges those to the pod's memory cgroup and frees
//! them only under memory pressure, so a pod spooling a steady stream of
//! uploads grows its kernel memory and working set until it restarts. Reusing
//! a name reuses its dentry, which bounds them to the most spool files staged
//! at once.

use std::{
    fs::{File, OpenOptions},
    io,
    os::unix::fs::OpenOptionsExt as _,
    path::{Path, PathBuf},
    sync::{Arc, LazyLock, Mutex, PoisonError},
};

use rand::RngExt as _;

/// One directory's spool file names: a random per-process prefix, so
/// processes sharing the directory don't collide, and slot numbers, each
/// reused once its file is removed.
#[derive(Debug)]
pub(super) struct SpoolNames {
    dir: PathBuf,
    prefix: String,
    slots: Mutex<Slots>,
}

#[derive(Debug, Default)]
struct Slots {
    free: Vec<u64>,
    next: u64,
}

/// Removal logs under the request guard's module, where spool cleanup has
/// always logged.
const LOG_TARGET: &str = concat!(env!("CARGO_CRATE_NAME"), "::ecw_files");

static PROCESS_SPOOL_NAMES: LazyLock<Arc<SpoolNames>> =
    LazyLock::new(|| Arc::new(SpoolNames::in_dir(std::env::temp_dir())));

impl SpoolNames {
    pub(super) fn in_dir(dir: PathBuf) -> Self {
        Self {
            dir,
            prefix: format!("space-ecw-upload-{:08x}", rand::rng().random::<u32>()),
            slots: Mutex::default(),
        }
    }

    /// The process's spool names, in the system temp directory.
    pub(super) fn for_process() -> Arc<Self> {
        Arc::clone(&PROCESS_SPOOL_NAMES)
    }

    /// Creates a spool file under a free name. A name whose file is on disk,
    /// because removing it failed or something else made it, is skipped and
    /// stays out of use: creation never opens an existing file or follows a
    /// link. Blocks on the filesystem.
    pub(super) fn create(self: &Arc<Self>) -> io::Result<(File, SpoolPath)> {
        loop {
            let slot = self.take_slot();
            let path = self.dir.join(format!("{}-{slot}", self.prefix));
            match OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                // Spooled uploads hold user content: owner-only.
                .mode(0o600)
                .open(&path)
            {
                Ok(file) => {
                    let path = SpoolPath {
                        names: Arc::clone(self),
                        slot,
                        path,
                    };
                    return Ok((file, path));
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => {
                    self.free_slot(slot);
                    return Err(error);
                }
            }
        }
    }

    fn take_slot(&self) -> u64 {
        let mut slots = self.slots.lock().unwrap_or_else(PoisonError::into_inner);
        slots.free.pop().unwrap_or_else(|| {
            let slot = slots.next;
            slots.next += 1;
            slot
        })
    }

    fn free_slot(&self, slot: u64) {
        self.slots
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .free
            .push(slot);
    }
}

/// A spool file's path, removed on drop. Its name is reused only once the
/// file is gone.
#[derive(Debug)]
pub(super) struct SpoolPath {
    names: Arc<SpoolNames>,
    slot: u64,
    path: PathBuf,
}

impl SpoolPath {
    pub(super) fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for SpoolPath {
    fn drop(&mut self) {
        let path = self.path.display();
        match std::fs::remove_file(&self.path) {
            Ok(()) => self.names.free_slot(self.slot),
            // Nothing is left under the name, so it can be reused.
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                tracing::debug!(
                    target: LOG_TARGET,
                    %path,
                    "ECW upload spool file was already removed"
                );
                self.names.free_slot(self.slot);
            }
            // The file may still be on disk, so the name stays out of use.
            Err(error) => tracing::warn!(
                target: LOG_TARGET,
                %path,
                %error,
                "failed to remove ECW upload spool file"
            ),
        }
    }
}
