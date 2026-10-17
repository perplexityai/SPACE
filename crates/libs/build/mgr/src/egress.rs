//! Operator-configured egress policy for build VMs.

use pplx_space_net_common::{
    EgressDestinationV1, IpCidr, NetworkPolicyV1, NetworkPolicyValidationError,
};

/// Comma-separated CIDRs build steps must not reach.
const ENV_EGRESS_DENY_CIDRS: &str = "SPACE_BUILDER_EGRESS_DENY_CIDRS";
/// Comma-separated CIDRs inside the denied ranges that build steps may still
/// reach, such as package mirrors and registries.
const ENV_EGRESS_ALLOW_CIDRS: &str = "SPACE_BUILDER_EGRESS_ALLOW_CIDRS";

#[derive(Debug, thiserror::Error)]
pub enum BuildVmEgressPolicyError {
    #[error("{name} is not valid Unicode")]
    NotUnicode { name: &'static str },
    #[error("{name} entry '{value}' is not a CIDR")]
    InvalidCidr {
        name: &'static str,
        value: String,
        #[source]
        source: NetworkPolicyValidationError,
    },
}

/// Build VMs allow all egress except the configured deny ranges, less the
/// configured allow ranges. Explicit denies take precedence over explicit
/// allows in a policy, so the allow ranges are removed from the deny list
/// instead of being listed as allows. Platform hard-denies and the guest
/// firewall's drops of the RFC 1918 ranges
/// (`docker/space/nftables/sandbox-pre-tproxy-guardrails.json`) apply before
/// this policy, so allow ranges cannot reopen those addresses.
pub fn build_vm_egress_policy_from_env() -> Result<NetworkPolicyV1, BuildVmEgressPolicyError> {
    let deny = env_cidrs(ENV_EGRESS_DENY_CIDRS)?;
    let allow = env_cidrs(ENV_EGRESS_ALLOW_CIDRS)?;
    Ok(build_vm_egress_policy(deny, &allow))
}

fn build_vm_egress_policy(deny: Vec<IpCidr>, allow: &[IpCidr]) -> NetworkPolicyV1 {
    let mut policy = NetworkPolicyV1::allow_all();
    policy.egress.deny = allow
        .iter()
        .fold(deny, |deny, &allowed| {
            deny.into_iter()
                .flat_map(|cidr| cidr.without(allowed))
                .collect()
        })
        .into_iter()
        .map(EgressDestinationV1::Cidr)
        .collect();
    policy
}

fn env_cidrs(name: &'static str) -> Result<Vec<IpCidr>, BuildVmEgressPolicyError> {
    let value = match std::env::var(name) {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => return Ok(Vec::new()),
        Err(std::env::VarError::NotUnicode(_)) => {
            return Err(BuildVmEgressPolicyError::NotUnicode { name });
        }
    };
    parse_cidrs(name, &value)
}

fn parse_cidrs(name: &'static str, value: &str) -> Result<Vec<IpCidr>, BuildVmEgressPolicyError> {
    value
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(|entry| {
            entry
                .parse()
                .map_err(|source| BuildVmEgressPolicyError::InvalidCidr {
                    name,
                    value: entry.to_string(),
                    source,
                })
        })
        .collect()
}

#[cfg(test)]
mod tests;
