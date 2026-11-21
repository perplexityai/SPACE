//! Which object-store origins a process trusts for template artifacts.

use std::collections::BTreeSet;

use crate::StorageUri;

/// The variable that lists the trusted buckets, comma-separated. The
/// internal API checks a release's storage locations against it when the
/// release registers, and the cluster controller checks them against it
/// again before it signs a request to one.
pub const TRUSTED_TEMPLATE_STORAGE_BUCKETS_ENV: &str = "SPACE_TRUSTED_TEMPLATE_STORAGE_BUCKETS";

/// Exact S3 bucket names trusted for template artifacts. An empty set trusts
/// every origin, endpoint overrides included, which keeps local
/// S3-compatible storage working where no policy is configured.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TrustedStorageBuckets(BTreeSet<String>);

/// Why [`TrustedStorageBuckets::check`] refused a location.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum StorageOriginRejection {
    #[error("endpoint overrides are not allowed for template storage")]
    EndpointOverride,
    #[error("bucket is not trusted for template storage")]
    UntrustedBucket,
}

impl TrustedStorageBuckets {
    /// The named buckets, trimmed, with empty names dropped.
    pub fn from_buckets(buckets: impl IntoIterator<Item = impl AsRef<str>>) -> Self {
        Self(
            buckets
                .into_iter()
                .map(|bucket| bucket.as_ref().trim().to_owned())
                .filter(|bucket| !bucket.is_empty())
                .collect(),
        )
    }

    /// The buckets of a comma-separated list, as
    /// [`TRUSTED_TEMPLATE_STORAGE_BUCKETS_ENV`] carries them.
    pub fn from_list(list: &str) -> Self {
        Self::from_buckets(list.split(','))
    }

    /// Admit `uri` if the policy is unconfigured, or if it names a trusted
    /// bucket and no endpoint override.
    pub fn check(&self, uri: &StorageUri) -> Result<(), StorageOriginRejection> {
        // Unconfigured deployments retain compatibility with local S3 endpoints.
        if self.0.is_empty() {
            return Ok(());
        }
        // A configured list also refuses every endpoint override.
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
