//! Guest boot configuration belongs to runtime adapters. Lifecycle callers
//! provide resources and policy, not a provider's JSON keys or device model.
use pplx_space_common_core::types::{
    Arch, BalloonConfig, GuestOperatingSystem, GuestPlatform, RateLimiter, RuntimeKind, VcpuCount,
};
use std::num::{NonZeroU32, NonZeroU64};

mod cloud_hypervisor;
mod firecracker;

/// Resources for a catalog-qualified guest boot. The source selects direct
/// kernel boot or firmware without making the lifecycle invent a kernel.
pub struct GuestBoot<'a> {
    pub guest: GuestPlatform,
    pub cpus: VcpuCount,
    pub memory_mib: NonZeroU64,
    pub guest_cid: NonZeroU32,
    pub tap: &'a str,
    pub mac: &'a str,
    pub disk_rate: Option<RateLimiter>,
    pub network_rate: Option<RateLimiter>,
    pub balloon: Option<BalloonConfig>,
    pub huge_pages: bool,
    pub console: bool,
    pub builder: bool,
    pub source: BootSource,
}

pub use pplx_space_execution_catalog::guest::{BootSource, GuestProfile, GuestShutdown};

#[derive(Debug, thiserror::Error)]
pub enum BootConfigError {
    #[error("runtime {0} has no boot adapter")]
    UnsupportedRuntime(RuntimeKind),
    #[error("unqualified boot source for guest {0:?}")]
    Guest(GuestPlatform),
    #[error("{feature} is not qualified for runtime {runtime}")]
    UnsupportedFeature {
        runtime: RuntimeKind,
        feature: &'static str,
    },
    #[error("guest memory size overflows the runtime's byte count")]
    MemoryOverflow,
    #[error("invalid runtime boot configuration: {0}")]
    Invalid(&'static str),
    #[error("serialize runtime boot configuration: {0}")]
    Json(#[from] serde_json::Error),
}

/// Check resource policy before allocating a network slot; rendering repeats
/// this boundary for callers outside sandbox-manager.
pub fn validate_resources(
    runtime: RuntimeKind,
    balloon: bool,
    huge_pages: bool,
) -> Result<(), BootConfigError> {
    if runtime == RuntimeKind::CloudHypervisor {
        for (enabled, feature) in [(balloon, "ballooning"), (huge_pages, "huge pages")] {
            if enabled {
                return Err(BootConfigError::UnsupportedFeature { runtime, feature });
            }
        }
    }
    Ok(())
}

pub fn configuration(
    runtime: RuntimeKind,
    input: &GuestBoot<'_>,
) -> Result<String, BootConfigError> {
    match (input.source, input.guest.os, input.guest.architecture) {
        (BootSource::LinuxDirect, GuestOperatingSystem::Linux, _) => {}
        (BootSource::Uefi, _, Arch::X86_64)
            if runtime == RuntimeKind::CloudHypervisor && !input.builder => {}
        _ => return Err(BootConfigError::Guest(input.guest)),
    }
    let value = match runtime {
        RuntimeKind::Firecracker => firecracker::configuration(input),
        RuntimeKind::CloudHypervisor => cloud_hypervisor::configuration(input)?,
        RuntimeKind::Gvisor => return Err(BootConfigError::UnsupportedRuntime(runtime)),
    };
    Ok(serde_json::to_string(&value)?)
}

// Changes apply only on cold boot. Rebuild templates and base snapshots to
// roll out new boot arguments to workloads restored from memory state.
fn command_line(input: &GuestBoot<'_>, devices: &str, uart: &str, console: &str) -> String {
    use pplx_space_common_core::guest::linux::network::PRIMARY_INTERFACE;
    use pplx_space_common_core::guest::network::{
        GATEWAY as GATEWAY_IPV4_ADDR, GUEST as IPV4_ADDR, NETMASK as IPV4_NETMASK,
    };
    let build = if input.builder {
        format!(
            " {}",
            pplx_space_common_core::types::TEMPLATE_RUNTIME_BUILD_MASK
        )
    } else {
        String::new()
    };
    let clock = if input.guest.architecture == Arch::X86_64 {
        " clocksource=kvm-clock"
    } else {
        ""
    };
    let console = if input.console {
        format!(" console={console} systemd.log_target=console systemd.log_level=info")
    } else {
        String::new()
    };
    format!(
        "reboot=k panic=1 {devices} rw root=/dev/vda{build}{clock} transparent_hugepage=never{uart}{console} ip={IPV4_ADDR}::{GATEWAY_IPV4_ADDR}:{IPV4_NETMASK}:instance:{PRIMARY_INTERFACE}:off"
    )
}

#[cfg(test)]
pub(crate) mod tests;

// The two REST protocols use the same token-bucket schema and millisecond units.
// Queue topology remains an adapter policy, not part of this encoding.
pub(crate) fn token_buckets(limiter: &RateLimiter) -> serde_json::Value {
    use pplx_space_common_core::types::TokenBucket;
    let bucket = |bucket: TokenBucket| serde_json::json!({ "size": bucket.size, "refill_time": bucket.refill_time_ms });
    serde_json::json!({ "bandwidth": bucket(limiter.bandwidth), "ops": bucket(limiter.operations) })
}

fn object<const N: usize>(
    fields: [(&str, serde_json::Value); N],
) -> serde_json::Map<String, serde_json::Value> {
    fields
        .into_iter()
        .map(|(key, value)| (key.to_owned(), value))
        .collect()
}

/// Parsed, admitted input. Control adapters cannot accept unvalidated JSON.
#[derive(Debug)]
pub struct StandaloneBoot(cloud_hypervisor::Configuration);
impl StandaloneBoot {
    /// Encode the admitted device model for its runtime control protocol.
    pub fn encode(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(&self.0)
    }
}

/// Parse the standalone adapter's privileged boot input before process launch.
pub fn validate_standalone(
    runtime: RuntimeKind,
    configuration: &str,
    source: BootSource,
) -> Result<StandaloneBoot, BootConfigError> {
    match runtime {
        RuntimeKind::CloudHypervisor => {
            cloud_hypervisor::parse(configuration, source).map(StandaloneBoot)
        }
        _ => Err(BootConfigError::UnsupportedRuntime(runtime)),
    }
}

/// Persisted CH machine state uses SPACE's ordinary machine-state and memory
/// artifact pair. Configuration remains a parsed device model; the opaque VMM
/// state is consumed only by the contained, pinned runtime.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct CloudHypervisorState {
    version: StateEncoding,
    configuration: cloud_hypervisor::Configuration,
    state: serde_json::Value,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
enum StateEncoding {
    #[serde(rename = "cloud-hypervisor-54-v1")]
    CloudHypervisor54V1,
}

impl StandaloneBoot {
    /// Apply the target node's admitted and clamped disk budget before restore.
    pub fn set_disk_rate(&mut self, rate: RateLimiter) {
        self.0.set_disk_rate(rate);
    }

    pub fn memory_bytes(&self) -> u64 {
        self.0.memory_bytes()
    }

    pub fn capture_state(&self, state: serde_json::Value) -> Result<Vec<u8>, BootConfigError> {
        self.0.require_state_access()?;
        let configuration = self.0.clone();
        Ok(serde_json::to_vec(&CloudHypervisorState {
            version: StateEncoding::CloudHypervisor54V1,
            configuration,
            state,
        })?)
    }

    /// Decode within the caller's bounded read, then bind host resources to the
    /// admitted target. Snapshot bytes cannot supply additional device paths.
    pub fn restore_state(
        &self,
        bytes: &[u8],
    ) -> Result<(StandaloneBoot, Vec<u8>), BootConfigError> {
        let mut snapshot: CloudHypervisorState = serde_json::from_slice(bytes)?;
        snapshot.configuration.restore_on(&self.0)?;
        Ok((
            StandaloneBoot(snapshot.configuration),
            serde_json::to_vec(&snapshot.state)?,
        ))
    }
}
