pub use pplx_space_common_svc::federated_identity::{
    DEFAULT_FEDERATED_IDENTITY_ISSUER, FederatedIdentityPolicy, FederatedIdentityPolicyError,
};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FederatedIdentityConfig {
    pub enabled: bool,
    pub policy: FederatedIdentityPolicy,
}

impl FederatedIdentityConfig {
    pub fn from_env() -> Result<Self, FederatedIdentityPolicyError> {
        Ok(Self {
            enabled: cfg!(feature = "federated-identity"),
            policy: FederatedIdentityPolicy::from_env()?,
        })
    }

    pub fn disabled() -> Self {
        Self {
            enabled: false,
            policy: FederatedIdentityPolicy::default(),
        }
    }
}

#[cfg(test)]
mod tests;
