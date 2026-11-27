//! How far a child's setup got between `fork` and `exec`, recorded by the
//! child itself. The kernel reports a missing working directory and a missing
//! executable with the same ENOENT, and the directory can change between the
//! failed spawn and any later look at it, so a failed spawn names the request
//! field to fix from what the child recorded.

use std::ffi::CStr;
use std::io;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use tokio::process::Command;

/// The step of a child's setup that a failed spawn failed at.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SpawnStage {
    /// Before the child exec'd, but not entering its working directory: the
    /// fork, or one of the other hooks.
    Setup,
    /// Entering the working directory.
    WorkingDir,
    /// `exec`: every hook ran.
    Exec,
}

// `SpawnStage::Setup` is 0, which a new mapping holds.
const WORKING_DIR: u8 = 1;
const EXEC: u8 = 2;

/// One start's record of its child's setup: a byte the forked child shares
/// with spaced. It starts at [`SpawnStage::Setup`]; the child's hooks write it
/// before they return, so it is final once `spawn` has returned.
#[derive(Clone)]
pub(crate) struct SpawnProgress(Arc<SharedStage>);

impl SpawnProgress {
    pub(crate) fn new() -> io::Result<Self> {
        SharedStage::new().map(|shared| Self(Arc::new(shared)))
    }

    /// Enter `dir`, recording a failure. Runs in the forked child: a
    /// `chdir` and an atomic store, both async-signal-safe.
    pub(crate) fn enter_working_dir(&self, dir: &CStr) -> io::Result<()> {
        nix::unistd::chdir(dir).map_err(|error| {
            self.0.stage().store(WORKING_DIR, Ordering::Release);
            io::Error::from(error)
        })
    }

    /// Register the hook that records the child reached `exec`. Register it
    /// after every other `pre_exec` hook: hooks run in registration order.
    pub(crate) fn record_exec(&self, command: &mut Command) {
        let progress = self.clone();
        // SAFETY: the hook runs in the forked child before `exec` and only
        // stores to an atomic in memory mapped before the fork.
        unsafe {
            command.pre_exec(move || {
                progress.0.stage().store(EXEC, Ordering::Release);
                Ok(())
            });
        }
    }

    /// Where the child's setup stopped. Read it once `spawn` has returned.
    pub(crate) fn stage(&self) -> SpawnStage {
        match self.0.stage().load(Ordering::Acquire) {
            WORKING_DIR => SpawnStage::WorkingDir,
            EXEC => SpawnStage::Exec,
            _ => SpawnStage::Setup,
        }
    }
}

/// A shared anonymous mapping holding the stage. A forked child writes to
/// the same memory, where it would write to a copy of an ordinary allocation.
struct SharedStage {
    stage: NonNull<AtomicU8>,
}

// SAFETY: the mapping is only accessed through the atomic, and only unmapped
// on drop.
unsafe impl Send for SharedStage {}
// SAFETY: as above.
unsafe impl Sync for SharedStage {}

impl SharedStage {
    fn new() -> io::Result<Self> {
        use rustix::mm::{MapFlags, ProtFlags};
        // SAFETY: a new mapping at an address the kernel picks, so no existing
        // memory is replaced. Anonymous memory starts zeroed.
        let mapping = unsafe {
            rustix::mm::mmap_anonymous(
                std::ptr::null_mut(),
                size_of::<AtomicU8>(),
                ProtFlags::READ | ProtFlags::WRITE,
                MapFlags::SHARED,
            )
        }?;
        let stage = NonNull::new(mapping.cast::<AtomicU8>())
            .ok_or_else(|| io::Error::other("mmap returned a null mapping"))?;
        Ok(Self { stage })
    }

    fn stage(&self) -> &AtomicU8 {
        // SAFETY: the mapping is readable, writable and aligned (page-aligned),
        // and lives until `drop`.
        unsafe { self.stage.as_ref() }
    }
}

impl Drop for SharedStage {
    fn drop(&mut self) {
        // SAFETY: the mapping `new` created, no longer referenced.
        let unmapped =
            unsafe { rustix::mm::munmap(self.stage.as_ptr().cast(), size_of::<AtomicU8>()) };
        if let Err(error) = unmapped {
            tracing::warn!(%error, "failed to unmap a process spawn stage");
        }
    }
}
