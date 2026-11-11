use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

use pplx_space_common_core::host::disk_admission::DEFAULT_MIN_AVAILABLE_BYTES;

use crate::storage::{
    StatvfsStorageProbe, StorageAdmissionError, StorageCheck, StorageProbe, check_available,
    env_u64,
};

const ENV_MIN_AVAILABLE_BYTES: &str = "SPACE_BUILDER_BLOB_MIN_AVAILABLE_BYTES";

#[derive(Clone)]
pub(crate) struct BlobStorageAdmission {
    root: PathBuf,
    min_available_bytes: u64,
    pending_bytes: Arc<Mutex<u64>>,
    probe: Arc<dyn StorageProbe>,
}

impl BlobStorageAdmission {
    pub(crate) fn from_env(root: PathBuf) -> Self {
        Self {
            root,
            min_available_bytes: env_u64(ENV_MIN_AVAILABLE_BYTES, DEFAULT_MIN_AVAILABLE_BYTES),
            pending_bytes: Arc::new(Mutex::new(0)),
            probe: Arc::new(StatvfsStorageProbe),
        }
    }

    #[cfg(test)]
    pub(crate) fn for_test(
        root: PathBuf,
        min_available_bytes: u64,
        probe: Arc<dyn StorageProbe>,
    ) -> Self {
        Self {
            root,
            min_available_bytes,
            pending_bytes: Arc::new(Mutex::new(0)),
            probe,
        }
    }

    pub(crate) fn reserve(&self, bytes: u64) -> Result<BlobStorageReservation, BlobStorageError> {
        let mut pending_bytes = self
            .pending_bytes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let reserved_bytes = pending_bytes.saturating_add(bytes);
        let required_available_bytes = self.min_available_bytes.saturating_add(reserved_bytes);
        check_available(
            &self.root,
            self.probe.as_ref(),
            required_available_bytes,
            StorageCheck::Admission,
        )?;
        *pending_bytes = reserved_bytes;
        Ok(BlobStorageReservation {
            root: self.root.clone(),
            min_available_bytes: self.min_available_bytes,
            pending_bytes: Arc::clone(&self.pending_bytes),
            probe: Arc::clone(&self.probe),
            remaining_bytes: bytes,
        })
    }
}

pub(crate) struct BlobStorageReservation {
    root: PathBuf,
    min_available_bytes: u64,
    pending_bytes: Arc<Mutex<u64>>,
    probe: Arc<dyn StorageProbe>,
    remaining_bytes: u64,
}

impl BlobStorageReservation {
    pub(crate) fn record_written(&mut self, bytes: u64) -> Result<(), BlobStorageError> {
        if bytes > self.remaining_bytes {
            return Err(BlobStorageError::ReservationExceeded {
                written_bytes: bytes,
                remaining_bytes: self.remaining_bytes,
            });
        }
        self.remaining_bytes -= bytes;
        let pending_bytes = {
            let mut pending_bytes = self
                .pending_bytes
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            *pending_bytes -= bytes;
            *pending_bytes
        };
        let required_available_bytes = self.min_available_bytes.saturating_add(pending_bytes);
        check_available(
            &self.root,
            self.probe.as_ref(),
            required_available_bytes,
            StorageCheck::Active,
        )?;
        Ok(())
    }
}

impl Drop for BlobStorageReservation {
    fn drop(&mut self) {
        let mut pending_bytes = self
            .pending_bytes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *pending_bytes -= self.remaining_bytes;
    }
}

#[derive(Debug, thiserror::Error)]
pub enum BlobStorageError {
    #[error(transparent)]
    Admission(#[from] StorageAdmissionError),
    #[error(
        "blob upload wrote {written_bytes} bytes with only {remaining_bytes} reserved bytes remaining"
    )]
    ReservationExceeded {
        written_bytes: u64,
        remaining_bytes: u64,
    },
}

#[cfg(test)]
mod tests;
