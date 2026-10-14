//! Validated credential values bound to a pulled policy.

use std::collections::{BTreeMap, BTreeSet};

use pplx_space_common_core::types::CredentialReferenceId;
use pplx_space_credential_injection_core::proto::ResolvedNetworkCredential;
use pplx_space_net_common::NetworkPolicyV1;

#[derive(Default)]
pub struct NetworkCredentialSnapshot {
    values: BTreeMap<CredentialReferenceId, zeroize::Zeroizing<Vec<u8>>>,
}

impl std::fmt::Debug for NetworkCredentialSnapshot {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NetworkCredentialSnapshot")
            .field("credential_count", &self.values.len())
            .finish()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum InvalidNetworkCredentialSnapshot {
    #[error("credential authority returned a malformed reference ID")]
    MalformedReferenceId,
    #[error("credential authority returned an unauthorized reference ID")]
    UnauthorizedReferenceId,
    #[error("credential authority returned a duplicate reference ID")]
    DuplicateReferenceId,
}

impl NetworkCredentialSnapshot {
    pub fn from_wire(
        credentials: Vec<ResolvedNetworkCredential>,
        policy: &NetworkPolicyV1,
    ) -> Result<Self, InvalidNetworkCredentialSnapshot> {
        let authorized: BTreeSet<_> = policy
            .network_credential_rules
            .iter()
            .flat_map(|rule| rule.credential_header_bindings.iter())
            .map(|binding| CredentialReferenceId::from_uuid(binding.credential_reference_id))
            .collect();
        let mut values = BTreeMap::new();
        for mut credential in credentials {
            let reference_id = credential
                .credential_reference_id
                .parse::<CredentialReferenceId>()
                .map_err(|_| InvalidNetworkCredentialSnapshot::MalformedReferenceId)?;
            if reference_id.to_string() != credential.credential_reference_id {
                return Err(InvalidNetworkCredentialSnapshot::MalformedReferenceId);
            }
            if !authorized.contains(&reference_id) {
                return Err(InvalidNetworkCredentialSnapshot::UnauthorizedReferenceId);
            }
            if values
                .insert(
                    reference_id,
                    zeroize::Zeroizing::new(std::mem::take(&mut credential.value)),
                )
                .is_some()
            {
                return Err(InvalidNetworkCredentialSnapshot::DuplicateReferenceId);
            }
        }
        Ok(Self { values })
    }

    pub(crate) fn value(&self, reference_id: CredentialReferenceId) -> Option<&[u8]> {
        self.values.get(&reference_id).map(AsRef::as_ref)
    }
}

#[cfg(test)]
mod tests;
