use std::path::PathBuf;
use std::time::Duration;

use crate::storage::{StorageAdmissionError, StorageCheck, StorageProbe, check_available};

const GIB: u64 = 1024 * 1024 * 1024;
const CONTAINERD_ROOT: &str = "/var/lib/containerd";

pub(super) const DEFAULT_MAX_IMAGE_BYTES: u64 = 32 * GIB;
pub(super) const DEFAULT_MIN_AVAILABLE_BYTES: u64 = 20 * GIB;
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Clone, Debug)]
pub(super) struct OciStorageConfig {
    pub(super) root: PathBuf,
    pub(super) max_image_bytes: u64,
    pub(super) min_available_bytes: u64,
    pub(super) poll_interval: Duration,
}

impl OciStorageConfig {
    pub(super) fn with_defaults() -> Self {
        Self {
            root: PathBuf::from(CONTAINERD_ROOT),
            max_image_bytes: DEFAULT_MAX_IMAGE_BYTES,
            min_available_bytes: DEFAULT_MIN_AVAILABLE_BYTES,
            poll_interval: DEFAULT_POLL_INTERVAL,
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct OciPullBudget {
    config: OciStorageConfig,
    initial_available_bytes: u64,
}

impl OciPullBudget {
    pub(super) fn admit(
        config: OciStorageConfig,
        probe: &dyn StorageProbe,
    ) -> Result<Self, OciStorageError> {
        let required_available_bytes = config
            .min_available_bytes
            .saturating_add(config.max_image_bytes);
        let status = check_available(
            &config.root,
            probe,
            required_available_bytes,
            StorageCheck::Admission,
        )?;
        Ok(Self {
            config,
            initial_available_bytes: status.available_bytes,
        })
    }

    pub(super) fn poll_interval(&self) -> Duration {
        self.config.poll_interval
    }

    pub(super) fn check(&self, probe: &dyn StorageProbe) -> Result<(), OciStorageError> {
        let status = check_available(
            &self.config.root,
            probe,
            self.config.min_available_bytes,
            StorageCheck::Active,
        )?;
        let consumed_bytes = self
            .initial_available_bytes
            .saturating_sub(status.available_bytes);
        if consumed_bytes > self.config.max_image_bytes {
            return Err(OciStorageError::ImageBudgetExceeded {
                consumed_bytes,
                max_image_bytes: self.config.max_image_bytes,
            });
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum OciStorageError {
    #[error(transparent)]
    Admission(#[from] StorageAdmissionError),
    #[error(
        "OCI pull consumed {consumed_bytes} containerd bytes, above the {max_image_bytes} byte image budget"
    )]
    ImageBudgetExceeded {
        consumed_bytes: u64,
        max_image_bytes: u64,
    },
}
