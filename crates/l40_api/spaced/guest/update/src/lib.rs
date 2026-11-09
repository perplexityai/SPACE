//! Synchronous, bounded guest-slot operations. Call from ExecStartPre before
//! starting any runtime, or from a tracked blocking task in the serving daemon.
//! The lock spans journal and selector writes; systemd serializes daemon exec.

#![cfg(any(unix, windows))]

mod files;
mod stage;
pub use stage::Staging;

pub fn seed_contract() -> &'static str {
    files::seed_contract()
}

use pplx_space_spaced_update::{
    ArtifactDigest, Attempt, Binary, BundleDescriptor, Journal, JournalState, MAX_METADATA_BYTES,
    Selection, Slot,
};
use std::{
    fs::{self, File},
    io::{self, Read, Write},
    path::{Path, PathBuf},
};
use uuid::Uuid;

#[cfg(unix)]
pub const UPDATE_ROOT: &str = "/var/lib/spaced";
#[cfg(windows)]
pub const UPDATE_ROOT: &str = r"C:\ProgramData\SPACE\agent";

#[cfg(unix)]
use std::{fs::OpenOptions, os::unix::fs::OpenOptionsExt};
#[cfg(windows)]
mod windows;
#[cfg(windows)]
pub use windows::{boot_clock, record_start_deadline, start_deadline};

/// Offline image assembly only. An existing journal or selector is an error;
/// this entry point cannot reset update history on an installed guest.
pub fn initialize_at(root: &Path) -> Result<(), UpdateError> {
    let store = SlotStore::open(root)?;
    let descriptor = store.descriptor(Slot::A)?;
    store.initialize(descriptor.digest()?)
}

#[derive(Debug, thiserror::Error)]
pub enum UpdateError {
    #[error("guest update storage operation failed: {0}")]
    Io(#[from] io::Error),
    #[error(transparent)]
    Descriptor(#[from] pplx_space_spaced_update::DescriptorError),
    #[error(transparent)]
    Journal(#[from] pplx_space_spaced_update::journal::JournalError),
    #[error("guest update metadata is invalid: {0}")]
    Json(#[from] serde_json::Error),
    #[error("guest update metadata or payload exceeds its bound")]
    TooLarge,
    #[error("guest update writer already holds the lock")]
    Busy,
    #[error("guest update payload identity does not match its descriptor")]
    Identity,
    #[error("guest update layout contains an unsafe path or unsupported seed contract")]
    Layout,
    #[error("inactive slot is still referenced by a process")]
    Referenced,
    #[error("guest update needs manager recovery before another mutation")]
    RecoveryRequired,
    #[error("guest update operation is not valid in the current phase")]
    Phase,
}

/// Private root containing exactly A/B. Holding this value holds an OS lock,
/// including across daemon crashes. Never await while holding it.
pub struct SlotStore {
    root: PathBuf,
    _lock: File,
}

impl SlotStore {
    pub fn open(root: &Path) -> Result<Self, UpdateError> {
        files::directory(root)?;
        files::directory(&root.join("slots"))?;
        // flock does not need a writable fd on the guest's local filesystem.
        // An unchanged committed start needs no writes on full/read-only ext4.
        // Recovery transitions still require durable journal/selector writes.
        let lock = match files::open_regular(&root.join("update-lock")) {
            Ok(lock) => lock,
            Err(UpdateError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
                files::create(&root.join("update-lock"), false)?
            }
            Err(error) => return Err(error),
        };
        if !lock.metadata()?.is_file() {
            return Err(UpdateError::Layout);
        }
        match lock.try_lock() {
            Ok(()) => {}
            Err(std::fs::TryLockError::WouldBlock) => return Err(UpdateError::Busy),
            Err(std::fs::TryLockError::Error(error)) => return Err(error.into()),
        }
        Ok(Self {
            root: root.to_owned(),
            _lock: lock,
        })
    }

    pub fn journal(&self) -> Result<Journal, UpdateError> {
        let journal: Journal =
            serde_json::from_slice(&files::read_bounded(&self.root.join("update-state"))?)?;
        journal.validate()?;
        Ok(journal)
    }

    pub fn descriptor(&self, slot: Slot) -> Result<BundleDescriptor, UpdateError> {
        let dir = self.slot_path(slot);
        files::directory(&dir)?;
        Ok(BundleDescriptor::parse(&files::read_bounded(
            &dir.join("artifact.json"),
        )?)?)
    }

    pub fn running_selection(&self) -> Result<Selection, UpdateError> {
        let slot = self.link_slot("current")?;
        Ok(Selection {
            slot,
            digest: self.descriptor(slot)?.digest()?,
        })
    }

    pub fn verify(&self, selection: &Selection) -> Result<BundleDescriptor, UpdateError> {
        let descriptor = self.descriptor(selection.slot)?;
        if descriptor.digest()? != selection.digest
            || !descriptor
                .seed_contracts
                .iter()
                .any(|v| v == files::seed_contract())
        {
            return Err(UpdateError::Identity);
        }
        for binary in Binary::ALL {
            files::verify_binary(
                &self
                    .slot_path(selection.slot)
                    .join(files::binary_name(binary)),
                descriptor.binary(binary),
            )?;
        }
        Ok(descriptor)
    }

    /// The only startup selection path. Malformed/missing metadata falls back
    /// to the verified committed link. I/O errors are retried without replacing
    /// durable authority: an unreadable file may contain a newer commit.
    #[cfg(test)]
    pub fn select(&self, boot: Uuid, now_ms: u64) -> Result<Selection, UpdateError> {
        self.select_with_clock(|| Ok((boot, now_ms)))
    }

    #[cfg(any(test, target_os = "linux", windows))]
    pub fn select_with_clock(
        &self,
        clock: impl FnOnce() -> Result<(Uuid, u64), UpdateError>,
    ) -> Result<Selection, UpdateError> {
        let mut repair_journal = false;
        let mut journal = match self.journal() {
            Ok(journal) => journal,
            Err(UpdateError::Io(error)) if error.kind() != io::ErrorKind::NotFound => {
                return Err(error.into());
            }
            Err(_) => {
                repair_journal = true;
                let slot = self.link_slot("committed")?;
                let descriptor = self.descriptor(slot)?;
                let committed = Selection {
                    slot,
                    digest: descriptor.digest()?,
                };
                Journal {
                    schema: 1,
                    committed,
                    state: JournalState::RecoveryRequired,
                }
            }
        };
        self.verify(&journal.committed)?;
        let previous = journal.clone();
        let (boot, now_ms) = clock()?;
        let selected = journal.select(boot, now_ms)?;
        // Publish attempt consumption before inspecting/executing the candidate.
        // If verification fails, this start fails and the next start rolls back.
        if repair_journal || journal != previous {
            self.write_journal(&journal)?;
        }
        if selected != journal.committed {
            self.verify(&selected)?;
        }
        self.set_link("committed", journal.committed.slot)?;
        self.set_link("current", selected.slot)?;
        Ok(selected)
    }

    pub fn arm(&self, fence: &Attempt) -> Result<(), UpdateError> {
        let mut journal = self.journal()?;
        self.verify(&fence.target)?;
        journal.arm(fence)?;
        self.write_journal(&journal)
    }

    /// A trial gets the actual process incarnation before opening its bridge.
    /// This does not consume another attempt or extend its fixed deadline.
    pub fn adopt_trial(
        &self,
        incarnation: Uuid,
        boot: Uuid,
        now_ms: u64,
    ) -> Result<Option<Attempt>, UpdateError> {
        let mut journal = self.journal()?;
        let JournalState::Trial { attempt, .. } = &mut journal.state else {
            return Ok(None);
        };
        if incarnation.is_nil()
            || attempt.boot != boot
            || now_ms >= attempt.confirmation_deadline_ms
            || self.link_slot("current")? != attempt.target.slot
        {
            return Err(UpdateError::Phase);
        }
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::fs::MetadataExt;
            let running = fs::metadata("/proc/self/exe")?;
            let selected = fs::metadata(self.slot_path(attempt.target.slot).join("spaced-svc"))?;
            if (running.dev(), running.ino()) != (selected.dev(), selected.ino()) {
                return Err(UpdateError::Identity);
            }
        }
        #[cfg(windows)]
        windows::verify_running_binary(
            &self.slot_path(attempt.target.slot).join("spaced-svc.exe"),
        )?;
        attempt.incarnation = incarnation;
        let result = attempt.clone();
        self.write_journal(&journal)?;
        Ok(Some(result))
    }

    pub fn self_test_passed(&self, fence: &Attempt) -> Result<(), UpdateError> {
        let mut journal = self.journal()?;
        journal.self_test_passed(fence)?;
        self.write_journal(&journal)
    }

    pub fn commit(&self, fence: &Attempt, boot: Uuid, now_ms: u64) -> Result<(), UpdateError> {
        let mut journal = self.journal()?;
        let previous = journal.clone();
        journal.commit(fence, boot, now_ms)?;
        if journal != previous {
            self.write_journal(&journal)?;
        }
        self.set_link("committed", journal.committed.slot)?;
        self.set_link("current", journal.committed.slot)
    }

    pub fn abort(&self, fence: &Attempt) -> Result<Journal, UpdateError> {
        let mut journal = self.journal()?;
        match &journal.state {
            JournalState::Committed { attempt } | JournalState::RolledBack { attempt, .. }
                if attempt == fence =>
            {
                return Ok(journal);
            }
            JournalState::Staged { attempt } | JournalState::Armed { attempt }
                if attempt == fence =>
            {
                journal.state = JournalState::RolledBack {
                    attempt: attempt.clone(),
                    progress: pplx_space_spaced_update::TrialProgress::Starting,
                };
            }
            JournalState::Trial { attempt, progress } if attempt == fence => {
                journal.state = JournalState::RolledBack {
                    attempt: attempt.clone(),
                    progress: *progress,
                };
            }
            _ => return Err(UpdateError::Phase),
        }
        // The serving trial must exit after cancellation. Do not change current
        // here: the selector chooses the fallback only after systemd reaps it.
        self.write_journal(&journal)?;
        Ok(journal)
    }

    pub fn restart(
        &self,
        restart: pplx_space_spaced_update::journal::Restart,
    ) -> Result<bool, UpdateError> {
        let mut journal = self.journal()?;
        match &journal.state {
            JournalState::Restarting { restart: previous }
            | JournalState::Restarted { restart: previous }
                if previous == &restart =>
            {
                return Ok(false);
            }
            JournalState::Stable
            | JournalState::Committed { .. }
            | JournalState::RolledBack { .. }
            | JournalState::Restarted { .. } => {}
            _ => return Err(UpdateError::Phase),
        }
        journal.state = JournalState::Restarting { restart };
        self.write_journal(&journal)?;
        Ok(true)
    }

    /// Seed assembly uses an already populated slot A; never bootstraps a live
    /// legacy guest or overwrites an existing installation.
    pub fn initialize(&self, digest: ArtifactDigest) -> Result<(), UpdateError> {
        for name in ["update-state", "current", "committed"] {
            match fs::symlink_metadata(self.root.join(name)) {
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
                Ok(_) => return Err(UpdateError::Layout),
            }
        }
        let selection = Selection {
            slot: Slot::A,
            digest,
        };
        self.verify(&selection)?;
        self.write_journal(&Journal::stable(selection))?;
        self.set_link("committed", Slot::A)?;
        self.set_link("current", Slot::A)
    }

    pub(crate) fn slot_path(&self, slot: Slot) -> PathBuf {
        self.root.join("slots").join(slot.name())
    }

    #[cfg(unix)]
    fn link_slot(&self, name: &str) -> Result<Slot, UpdateError> {
        match fs::read_link(self.root.join(name))?.to_str() {
            Some("slots/a") => Ok(Slot::A),
            Some("slots/b") => Ok(Slot::B),
            _ => Err(UpdateError::Layout),
        }
    }

    #[cfg(unix)]
    fn set_link(&self, name: &str, slot: Slot) -> Result<(), UpdateError> {
        if self.link_slot(name).is_ok_and(|existing| existing == slot) {
            return Ok(());
        }
        let temp = self.root.join(format!(".{name}.next"));
        files::remove_file_if_present(&temp)?;
        std::os::unix::fs::symlink(slot.selector(), &temp)?;
        fs::rename(&temp, self.root.join(name))?;
        File::open(&self.root)?.sync_all()?;
        Ok(())
    }

    pub(crate) fn write_journal(&self, journal: &Journal) -> Result<(), UpdateError> {
        journal.validate()?;
        let bytes = serde_json::to_vec(journal)?;
        if bytes.len() > MAX_METADATA_BYTES {
            return Err(UpdateError::TooLarge);
        }
        let temp = self.root.join(".update-state.next");
        files::remove_file_if_present(&temp)?;
        let mut file = files::create(&temp, false)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        drop(file);
        files::replace(&temp, &self.root.join("update-state"))?;
        files::sync_directory(&self.root)?;
        Ok(())
    }
}

/// Fixed seed ABI. A failed selector deliberately fails ExecStartPre; it must
/// never fall through into daemon startup with an unverified current selector.
#[cfg(target_os = "linux")]
pub fn select_installed() -> Result<(), UpdateError> {
    SlotStore::open(Path::new(UPDATE_ROOT))?.select_with_clock(boot_clock)?;
    Ok(())
}

#[cfg(target_os = "linux")]
pub fn boot_clock() -> Result<(Uuid, u64), UpdateError> {
    let boot = fs::read_to_string("/proc/sys/kernel/random/boot_id")?
        .trim()
        .parse::<Uuid>()
        .map_err(|_| UpdateError::Layout)?;
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: ts is writable and valid for the duration of this syscall.
    if unsafe { libc::clock_gettime(libc::CLOCK_BOOTTIME, &mut ts) } != 0 {
        return Err(io::Error::last_os_error().into());
    }
    let nanos = u64::try_from(ts.tv_nsec).map_err(|_| UpdateError::Layout)?;
    let millis = u64::try_from(ts.tv_sec)
        .map_err(|_| UpdateError::Layout)?
        .checked_mul(1000)
        .and_then(|v| v.checked_add(nanos / 1_000_000))
        .ok_or(UpdateError::Layout)?;
    Ok((boot, millis))
}

#[cfg(test)]
mod tests;
