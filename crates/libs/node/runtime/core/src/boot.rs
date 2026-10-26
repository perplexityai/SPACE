//! Guest boot configuration belongs to runtime adapters. Lifecycle callers
//! provide resources and policy, not a provider's JSON keys or device model.
use pplx_space_common_core::types::{
    Arch, BalloonConfig, GuestOperatingSystem, GuestPlatform, RateLimiter, RuntimeKind, VcpuCount,
};
use std::num::{NonZeroU32, NonZeroU64};

mod cloud_hypervisor;
mod firecracker;

/// Inputs for the Linux direct-boot adapters. Firmware and library-backed
/// runtimes can introduce their own boot input without changing lifecycle intent.
pub struct LinuxBoot<'a> {
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
}

#[derive(Debug, thiserror::Error)]
pub enum BootConfigError {
    #[error("runtime {0} has no Linux direct-boot adapter")]
    UnsupportedRuntime(RuntimeKind),
    #[error("Linux direct boot cannot boot guest {0:?}")]
    Guest(GuestPlatform),
    #[error("{feature} is not qualified for runtime {runtime}")]
    UnsupportedFeature {
        runtime: RuntimeKind,
        feature: &'static str,
    },
    #[error("guest memory size overflows the runtime's byte count")]
    MemoryOverflow,
    #[error("serialize runtime boot configuration: {0}")]
    Json(#[from] serde_json::Error),
}

pub fn configuration(
    runtime: RuntimeKind,
    input: &LinuxBoot<'_>,
) -> Result<String, BootConfigError> {
    if input.guest.os != GuestOperatingSystem::Linux {
        return Err(BootConfigError::Guest(input.guest));
    }
    let value = match runtime {
        RuntimeKind::Firecracker => firecracker::configuration(input),
        RuntimeKind::CloudHypervisor => cloud_hypervisor::configuration(input)?,
        RuntimeKind::Gvisor => return Err(BootConfigError::UnsupportedRuntime(runtime)),
    };
    Ok(serde_json::to_string(&value)?)
}

fn command_line(input: &LinuxBoot<'_>, devices: &str, uart: &str, console: &str) -> String {
    use pplx_space_common_core::guest::linux::network::{
        GATEWAY_IPV4_ADDR, IPV4_ADDR, IPV4_NETMASK, PRIMARY_INTERFACE,
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
mod tests;

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
