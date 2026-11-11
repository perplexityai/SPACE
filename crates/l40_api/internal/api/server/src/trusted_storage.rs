//! Trusted origins for newly registered template releases.

use std::collections::BTreeSet;

use pplx_space_common_core::env::{Environment, ProcessEnvironment};
use pplx_space_common_core::types::StorageUri;

pub(super) const TRUSTED_BUCKETS_ENV: &str = "SPACE_TRUSTED_TEMPLATE_STORAGE_BUCKETS";

#[derive(Debug, thiserror::Error)]
pub(super) enum StoragePolicyConfigError {
    #[error("{TRUSTED_BUCKETS_ENV} must contain UTF-8 bucket names")]
    InvalidEnvironment(#[source] std::env::VarError),
}

#[derive(Clone, Debug, Default)]
pub(super) struct TrustedTemplateStorageBuckets(BTreeSet<String>);

#[derive(Debug, thiserror::Error)]
pub(super) enum StorageOriginRejection {
    #[error("endpoint overrides are not allowed for template storage")]
    EndpointOverride,
    #[error("bucket is not trusted for template storage")]
    UntrustedBucket,
}

impl TrustedTemplateStorageBuckets {
    pub(super) fn from_buckets(buckets: impl IntoIterator<Item = impl AsRef<str>>) -> Self {
        Self(
            buckets
                .into_iter()
                .map(|bucket| bucket.as_ref().trim().to_owned())
                .filter(|bucket| !bucket.is_empty())
                .collect(),
        )
    }

    pub(super) fn from_env() -> Result<Self, StoragePolicyConfigError> {
        Self::from_environment(&ProcessEnvironment)
    }

    fn from_environment(env: &impl Environment) -> Result<Self, StoragePolicyConfigError> {
        match env.var(TRUSTED_BUCKETS_ENV) {
            Ok(value) => Ok(Self::from_buckets(value.split(','))),
            Err(std::env::VarError::NotPresent) => Ok(Self::default()),
            Err(error) => Err(StoragePolicyConfigError::InvalidEnvironment(error)),
        }
    }

    pub(super) fn check(&self, uri: &StorageUri) -> Result<(), StorageOriginRejection> {
        // Unconfigured deployments retain compatibility with local S3 endpoints.
        if self.0.is_empty() {
            return Ok(());
        }
        // A trusted bucket with a caller-selected endpoint still redirects pulls.
        if uri.endpoint.is_some() {
            return Err(StorageOriginRejection::EndpointOverride);
        }
        if !self.0.contains(&uri.bucket) {
            return Err(StorageOriginRejection::UntrustedBucket);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
