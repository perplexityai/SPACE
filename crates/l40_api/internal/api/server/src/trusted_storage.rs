//! Trusted origins for newly registered template releases.

use pplx_space_env_vars::{Environment, ProcessEnvironment};

pub(super) use pplx_space_artifact_model::TRUSTED_TEMPLATE_STORAGE_BUCKETS_ENV as TRUSTED_BUCKETS_ENV;
pub(super) use pplx_space_artifact_model::TrustedStorageBuckets as TrustedTemplateStorageBuckets;

#[derive(Debug, thiserror::Error)]
pub(super) enum StoragePolicyConfigError {
    #[error("{TRUSTED_BUCKETS_ENV} must contain UTF-8 bucket names")]
    InvalidEnvironment(#[source] std::env::VarError),
}

pub(super) fn from_env() -> Result<TrustedTemplateStorageBuckets, StoragePolicyConfigError> {
    from_environment(&ProcessEnvironment)
}

fn from_environment(
    env: &impl Environment,
) -> Result<TrustedTemplateStorageBuckets, StoragePolicyConfigError> {
    match env.var(TRUSTED_BUCKETS_ENV) {
        Ok(value) => Ok(TrustedTemplateStorageBuckets::from_list(&value)),
        Err(std::env::VarError::NotPresent) => Ok(TrustedTemplateStorageBuckets::default()),
        Err(error) => Err(StoragePolicyConfigError::InvalidEnvironment(error)),
    }
}

#[cfg(test)]
mod tests;
