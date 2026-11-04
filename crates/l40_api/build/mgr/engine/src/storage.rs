use std::path::{Path, PathBuf};

use pplx_space_common_core::host::disk::{DiskStatus, statvfs_status};

pub(crate) trait StorageProbe: Send + Sync {
    fn status(&self, root: &Path) -> Option<DiskStatus>;
}

pub(crate) struct StatvfsStorageProbe;

impl StorageProbe for StatvfsStorageProbe {
    fn status(&self, root: &Path) -> Option<DiskStatus> {
        statvfs_status(root.to_str()?)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StorageCheck {
    Admission,
    Active,
}

pub(crate) fn check_available(
    root: &Path,
    probe: &dyn StorageProbe,
    required_available_bytes: u64,
    check: StorageCheck,
) -> Result<DiskStatus, StorageAdmissionError> {
    let status = probe
        .status(root)
        .ok_or_else(|| StorageAdmissionError::Unavailable {
            path: root.to_path_buf(),
        })?;
    if status.available_bytes >= required_available_bytes {
        return Ok(status);
    }
    match check {
        StorageCheck::Admission => Err(StorageAdmissionError::AdmissionRejected {
            available_bytes: status.available_bytes,
            required_available_bytes,
        }),
        StorageCheck::Active => Err(StorageAdmissionError::HeadroomExhausted {
            available_bytes: status.available_bytes,
            required_available_bytes,
        }),
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StorageAdmissionError {
    #[error("storage status is unavailable for {path}")]
    Unavailable { path: PathBuf },
    #[error(
        "storage admission requires {required_available_bytes} available bytes but found {available_bytes}"
    )]
    AdmissionRejected {
        available_bytes: u64,
        required_available_bytes: u64,
    },
    #[error(
        "storage has {available_bytes} available bytes, below the required {required_available_bytes} bytes"
    )]
    HeadroomExhausted {
        available_bytes: u64,
        required_available_bytes: u64,
    },
}

pub(crate) fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(default)
}
