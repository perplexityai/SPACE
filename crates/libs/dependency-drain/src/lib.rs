//! Cross-process admission gate for replacing a service dependency.
//!
//! A participant holds the capability lock for its active lifetime. Every new
//! work item briefly shares the admission gate while joining the participant's
//! active set; the first active item also shares the work gate until the set is
//! empty. A deployment process closes admission exclusively, then takes the
//! work gate exclusively to wait for admitted work before replacing the
//! dependency. It keeps both gates closed until the replacement is healthy.
//! Kernel lock release makes both sides fail safe across process death.
//!
//! It is its own crate because the mechanism needs only `flock` and the
//! filesystem, so a service can use it without the rest of common-svc.

use std::fs::OpenOptions;
use std::os::unix::fs::OpenOptionsExt as _;
use std::path::{Path, PathBuf};

use nix::errno::Errno;
use nix::fcntl::{Flock, FlockArg};
use thiserror::Error;

#[derive(Debug)]
pub struct DependencyDrainParticipant {
    _capability: Flock<std::fs::File>,
    admission_gate_path: PathBuf,
    work_gate_path: PathBuf,
}

#[derive(Debug)]
pub struct DependencyWorkLease {
    _lock: Flock<std::fs::File>,
}

#[derive(Debug)]
pub struct DependencyAdmissionLease {
    _lock: Flock<std::fs::File>,
}

impl DependencyDrainParticipant {
    /// Registers one active process as a participant in the dependency-drain
    /// protocol. A second active participant is an invariant violation.
    pub fn register(
        capability_path: impl AsRef<Path>,
        admission_gate_path: impl AsRef<Path>,
        work_gate_path: impl AsRef<Path>,
    ) -> Result<Self, DependencyDrainError> {
        let capability_path = capability_path.as_ref();
        let capability_file = open_lock_file(capability_path)?;
        let capability = Flock::lock(capability_file, FlockArg::LockExclusiveNonblock).map_err(
            |(_, source)| {
                if source == Errno::EWOULDBLOCK {
                    DependencyDrainError::ParticipantAlreadyActive {
                        path: capability_path.to_path_buf(),
                    }
                } else {
                    DependencyDrainError::Lock {
                        path: capability_path.to_path_buf(),
                        source: std::io::Error::from_raw_os_error(source as i32),
                    }
                }
            },
        )?;
        Ok(Self {
            _capability: capability,
            admission_gate_path: admission_gate_path.as_ref().to_path_buf(),
            work_gate_path: work_gate_path.as_ref().to_path_buf(),
        })
    }

    /// Crosses the short-lived admission gate. Every new work item must hold
    /// this while joining the in-process active set; `None` means a deployment
    /// has closed admission.
    pub fn try_acquire_admission(
        &self,
    ) -> Result<Option<DependencyAdmissionLease>, DependencyDrainError> {
        let admission_file = open_lock_file(&self.admission_gate_path)?;
        match Flock::lock(admission_file, FlockArg::LockSharedNonblock) {
            Ok(lock) => Ok(Some(DependencyAdmissionLease { _lock: lock })),
            Err((_, Errno::EWOULDBLOCK)) => Ok(None),
            Err((_, source)) => Err(DependencyDrainError::Lock {
                path: self.admission_gate_path.clone(),
                source: std::io::Error::from_raw_os_error(source as i32),
            }),
        }
    }

    /// Acquires the long-lived shared work gate for the first active work item.
    /// The admission lease proves a deployment cannot hold the exclusive work
    /// gate concurrently.
    pub fn try_acquire_work(
        &self,
        _admission: &DependencyAdmissionLease,
    ) -> Result<Option<DependencyWorkLease>, DependencyDrainError> {
        let file = open_lock_file(&self.work_gate_path)?;
        match Flock::lock(file, FlockArg::LockSharedNonblock) {
            Ok(lock) => Ok(Some(DependencyWorkLease { _lock: lock })),
            Err((_, Errno::EWOULDBLOCK)) => Ok(None),
            Err((_, source)) => Err(DependencyDrainError::Lock {
                path: self.work_gate_path.clone(),
                source: std::io::Error::from_raw_os_error(source as i32),
            }),
        }
    }
}

fn open_lock_file(path: &Path) -> Result<std::fs::File, DependencyDrainError> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .map_err(|source| DependencyDrainError::Open {
            path: path.to_path_buf(),
            source,
        })?;
    if !file
        .metadata()
        .map_err(|source| DependencyDrainError::Open {
            path: path.to_path_buf(),
            source,
        })?
        .is_file()
    {
        return Err(DependencyDrainError::NotRegularFile {
            path: path.to_path_buf(),
        });
    }
    Ok(file)
}

#[derive(Debug, Error)]
pub enum DependencyDrainError {
    #[error("failed to open dependency drain lock {}: {source}", path.display())]
    Open {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("dependency drain lock is not a regular file: {}", path.display())]
    NotRegularFile { path: PathBuf },
    #[error("another dependency drain participant already holds {}", path.display())]
    ParticipantAlreadyActive { path: PathBuf },
    #[error("failed to lock dependency drain file {}: {source}", path.display())]
    Lock {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

#[cfg(test)]
mod tests;
