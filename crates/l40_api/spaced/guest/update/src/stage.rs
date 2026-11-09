use super::*;
#[cfg(target_os = "linux")]
use std::os::unix::fs::MetadataExt;
use std::os::unix::fs::PermissionsExt;

/// One bounded streaming writer. Dropping it abandons only inactive partials;
/// the committed bundle and journal stay authoritative. A caller must bound the
/// transfer lifetime; serving code owns it on a tracked blocking worker.
pub struct Staging {
    store: SlotStore,
    descriptor: BundleDescriptor,
    attempt: Attempt,
    files: [File; 2],
    written: [u64; 2],
    replay: bool,
}

impl SlotStore {
    pub fn begin_stage(
        self,
        descriptor: BundleDescriptor,
        attempt: Attempt,
    ) -> Result<Staging, UpdateError> {
        descriptor.validate()?;
        let mut journal = self.journal()?;
        if descriptor.architecture != self.descriptor(journal.committed.slot)?.architecture
            || !descriptor
                .seed_contracts
                .iter()
                .any(|contract| contract == pplx_space_spaced_update::SEED_CONTRACT)
        {
            return Err(UpdateError::Identity);
        }
        match &journal.state {
            JournalState::Restarted { restart } if restart.operation == attempt.operation => {
                return Err(UpdateError::Phase);
            }
            JournalState::RolledBack {
                attempt: consumed,
                progress,
            } if consumed.operation == attempt.operation => {
                // The manager may retry AwaitingHost once under the same
                // operation. Stage RPC checks the serving boot/incarnation;
                // require a fresh fallback observation here, not a replay of
                // the original fence (whose incarnation adoption changed).
                if *progress != pplx_space_spaced_update::TrialProgress::AwaitingHost
                    || consumed.execution != attempt.execution
                    || consumed.target != attempt.target
                    || consumed.incarnation == attempt.incarnation
                    || (consumed.boot == attempt.boot
                        && attempt.confirmation_deadline_ms <= consumed.confirmation_deadline_ms)
                {
                    return Err(UpdateError::Phase);
                }
            }
            JournalState::Stable
            | JournalState::Committed { .. }
            | JournalState::RolledBack { .. }
            | JournalState::Restarted { .. }
            // Staging grants no activation authority. A new fenced host
            // operation may replace inactive bytes left by stop/cancellation;
            // the old receipt is retired before replacement bytes are removed.
            | JournalState::Staged { .. } => {}
            JournalState::RecoveryRequired => return Err(UpdateError::RecoveryRequired),
            _ => return Err(UpdateError::Phase),
        }
        if attempt.target.slot != journal.committed.slot.other()
            || descriptor.digest()? != attempt.target.digest
        {
            return Err(UpdateError::Identity);
        }
        if self.link_slot("current")? != journal.committed.slot
            || self.link_slot("committed")? != journal.committed.slot
        {
            return Err(UpdateError::Phase);
        }
        if let JournalState::Staged { attempt: prior } = &journal.state
            && prior.operation == attempt.operation
        {
            if prior != &attempt {
                return Err(UpdateError::Phase);
            }
            self.verify(&attempt.target)?;
            let path = self.slot_path(attempt.target.slot);
            let service = files::open_regular(&path.join(Binary::Service.file_name()))?;
            let cli = files::open_regular(&path.join(Binary::Cli.file_name()))?;
            return Ok(Staging {
                store: self,
                descriptor,
                attempt,
                files: [service, cli],
                written: [0, 0],
                replay: true,
            });
        }
        let staged = Journal {
            schema: 1,
            committed: journal.committed.clone(),
            state: JournalState::Staged {
                attempt: attempt.clone(),
            },
        };
        staged.validate()?;
        if matches!(journal.state, JournalState::Staged { .. }) {
            journal.state = JournalState::Stable;
            self.write_journal(&journal)?;
        }
        let path = self.slot_path(attempt.target.slot);
        match fs::symlink_metadata(&path) {
            Ok(_) => {
                files::directory(&path)?;
                ensure_unreferenced(&path)?;
                fs::remove_dir_all(&path)?;
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        // Reserve space for the whole transfer plus journal/selector repair.
        ensure_capacity(
            &self.root,
            descriptor.service.length + descriptor.cli.length,
        )?;
        fs::create_dir(&path)?;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700))?;
        File::open(self.root.join("slots"))?.sync_all()?;
        let create = |binary: Binary| -> Result<File, UpdateError> {
            Ok(OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o700)
                .open(path.join(binary.file_name()))?)
        };
        let service = create(Binary::Service)?;
        let cli = create(Binary::Cli)?;
        // No Staged journal until both payloads have been verified and flushed.
        Ok(Staging {
            store: self,
            descriptor,
            attempt,
            files: [service, cli],
            written: [0, 0],
            replay: false,
        })
    }
}

impl Staging {
    pub fn write(&mut self, binary: Binary, offset: u64, bytes: &[u8]) -> Result<(), UpdateError> {
        let [service, cli] = &mut self.files;
        let [service_written, cli_written] = &mut self.written;
        let (file, written) = match binary {
            Binary::Service => (service, service_written),
            Binary::Cli => (cli, cli_written),
        };
        if bytes.is_empty() || bytes.len() > 64 * 1024 || offset != *written {
            return Err(UpdateError::Layout);
        }
        let end = offset
            .checked_add(bytes.len() as u64)
            .ok_or(UpdateError::TooLarge)?;
        if end > self.descriptor.binary(binary).length {
            return Err(UpdateError::TooLarge);
        }
        if self.replay {
            // Compare a retransmission against the already verified bytes;
            // an interrupted retry must not truncate or replace that slot.
            let mut existing = vec![0; bytes.len()];
            file.read_exact(&mut existing)?;
            if existing != bytes {
                return Err(UpdateError::Identity);
            }
        } else {
            file.write_all(bytes)?;
        }
        *written = end;
        Ok(())
    }

    pub fn finish(self) -> Result<Attempt, UpdateError> {
        for ((binary, file), written) in Binary::ALL.into_iter().zip(&self.files).zip(self.written)
        {
            if written != self.descriptor.binary(binary).length {
                return Err(UpdateError::Identity);
            }
            if !self.replay {
                file.sync_all()?;
            }
        }
        if self.replay {
            return Ok(self.attempt);
        }
        let path = self.store.slot_path(self.attempt.target.slot);
        let mut descriptor = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path.join("artifact.json"))?;
        descriptor.write_all(&self.descriptor.canonical_bytes()?)?;
        descriptor.sync_all()?;
        File::open(&path)?.sync_all()?;
        self.store.verify(&self.attempt.target)?;
        let mut journal = self.store.journal()?;
        journal.state = JournalState::Staged {
            attempt: self.attempt.clone(),
        };
        self.store.write_journal(&journal)?;
        Ok(self.attempt)
    }
}

#[cfg(target_os = "linux")]
fn ensure_capacity(root: &Path, bytes: u64) -> Result<(), UpdateError> {
    use std::os::unix::ffi::OsStrExt;
    let root =
        std::ffi::CString::new(root.as_os_str().as_bytes()).map_err(|_| UpdateError::Layout)?;
    // SAFETY: statvfs is a C structure of integer fields; zero is valid for each.
    let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
    // SAFETY: root is NUL-terminated and stat is writable for this syscall.
    if unsafe { libc::statvfs(root.as_ptr(), &mut stat) } != 0 {
        return Err(io::Error::last_os_error().into());
    }
    if stat.f_bavail.saturating_mul(stat.f_frsize) < bytes.saturating_add(16 * 1024 * 1024)
        || (stat.f_files != 0 && stat.f_favail < 16)
    {
        return Err(io::Error::from_raw_os_error(libc::ENOSPC).into());
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn ensure_capacity(_: &Path, _: u64) -> Result<(), UpdateError> {
    Err(UpdateError::Layout)
}

#[cfg(target_os = "linux")]
fn process_gone(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::NotFound || error.raw_os_error() == Some(libc::ESRCH)
}

#[cfg(target_os = "linux")]
fn ensure_unreferenced(slot: &Path) -> Result<(), UpdateError> {
    // Inode identity also catches hardlinks and deleted mappings. Fail closed
    // on inaccessible live processes or excessive procfs traversal.
    let mut identities = Vec::new();
    for binary in Binary::ALL {
        match fs::symlink_metadata(slot.join(binary.file_name())) {
            Ok(meta) if meta.is_file() => identities.push((meta.dev(), meta.ino())),
            Ok(_) => return Err(UpdateError::Layout),
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
    }
    if identities.is_empty() {
        return Ok(());
    }
    let mut visited = 0usize;
    for entry in fs::read_dir("/proc")? {
        let entry = entry?;
        if !entry
            .file_name()
            .as_encoded_bytes()
            .iter()
            .all(u8::is_ascii_digit)
        {
            continue;
        }
        visited += 1;
        if visited > 65536 {
            return Err(UpdateError::Referenced);
        }
        let maps = match File::open(entry.path().join("maps")) {
            Ok(file) => file,
            Err(e) if process_gone(&e) => continue,
            Err(e) => return Err(e.into()),
        };
        let mut text = Vec::new();
        match maps.take(64 * 1024 * 1024 + 1).read_to_end(&mut text) {
            Ok(_) => {}
            Err(error) if process_gone(&error) => continue,
            Err(error) => return Err(error.into()),
        }
        if text.len() > 64 * 1024 * 1024 {
            return Err(UpdateError::Referenced);
        }
        ensure_maps_unreferenced(&text, &identities)?;
        let fds = match fs::read_dir(entry.path().join("fd")) {
            Ok(fds) => fds,
            Err(e) if process_gone(&e) => continue,
            Err(e) => return Err(e.into()),
        };
        ensure_fds_unreferenced(fds, &identities, &mut 0)?;
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn ensure_maps_unreferenced(text: &[u8], identities: &[(u64, u64)]) -> Result<(), UpdateError> {
    for line in text
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        // Only device/inode fields are ASCII; mapped filenames need not be UTF-8.
        let mut fields = line
            .split(u8::is_ascii_whitespace)
            .filter(|field| !field.is_empty());
        let device = fields
            .nth(3)
            .and_then(|field| std::str::from_utf8(field).ok())
            .ok_or(UpdateError::Layout)?;
        let inode = fields
            .next()
            .and_then(|v| std::str::from_utf8(v).ok()?.parse::<u64>().ok())
            .ok_or(UpdateError::Layout)?;
        let (major, minor) = device.split_once(':').ok_or(UpdateError::Layout)?;
        let major = u32::from_str_radix(major, 16).map_err(|_| UpdateError::Layout)?;
        let minor = u32::from_str_radix(minor, 16).map_err(|_| UpdateError::Layout)?;
        if identities.iter().any(|(dev, ino)| {
            *ino == inode && libc::major(*dev) == major && libc::minor(*dev) == minor
        }) {
            return Err(UpdateError::Referenced);
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn ensure_fds_unreferenced(
    fds: impl IntoIterator<Item = io::Result<fs::DirEntry>>,
    identities: &[(u64, u64)],
    visited: &mut usize,
) -> Result<(), UpdateError> {
    for fd in fds {
        *visited += 1;
        if *visited > 65536 {
            return Err(UpdateError::Referenced);
        }
        let fd = match fd {
            Ok(fd) => fd,
            Err(error) if process_gone(&error) => break,
            Err(error) => return Err(error.into()),
        };
        match fs::metadata(fd.path()) {
            Ok(meta) if identities.contains(&(meta.dev(), meta.ino())) => {
                return Err(UpdateError::Referenced);
            }
            Ok(_) => {}
            Err(e) if process_gone(&e) => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

#[cfg(all(test, target_os = "linux"))]
mod tests;

#[cfg(not(target_os = "linux"))]
fn ensure_unreferenced(_: &Path) -> Result<(), UpdateError> {
    Err(UpdateError::Layout)
}
