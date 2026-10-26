//! Guest boot configuration belongs to runtime adapters. Lifecycle callers
//! provide resources and policy, not a provider's JSON keys or device model.
use pplx_space_common_core::types::{
    Arch, ArtifactComponentKind, ArtifactComponentMetadata, BalloonConfig, ExecutionMetadata,
    GuestOperatingSystem, GuestPlatform, RateLimiter, RuntimeKind, VcpuCount,
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
    pub initramfs: bool,
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
    #[error("invalid runtime boot configuration: {0}")]
    Invalid(&'static str),
    #[error("device memory admission: {0}")]
    DeviceMemory(#[from] crate::catalog::DeviceProfileError),
    #[error("invalid assigned device: {0}")]
    Device(#[from] crate::vfio::VfioError),
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
    if input.initramfs && runtime != RuntimeKind::CloudHypervisor {
        return Err(BootConfigError::UnsupportedFeature {
            runtime,
            feature: "authenticated initramfs",
        });
    }
    let value = match runtime {
        RuntimeKind::Firecracker => firecracker::configuration(input),
        RuntimeKind::CloudHypervisor => cloud_hypervisor::configuration(input)?,
        RuntimeKind::Gvisor => return Err(BootConfigError::UnsupportedRuntime(runtime)),
    };
    Ok(serde_json::to_string(&value)?)
}

/// Inject only node-owned assignments after validating the disk's boot config.
/// The caller must validate durable ownership and profile/environment compatibility;
/// the contained launcher independently validates the live IOMMU boundary.
pub fn with_exclusive_devices(
    runtime: RuntimeKind,
    configuration: &str,
    initramfs: bool,
    devices: &pplx_space_execution_common::devices::DeviceExecution,
    resources: crate::catalog::DeviceResourceLimits,
    reserved_memory_mib: u64,
) -> Result<String, BootConfigError> {
    validate_standalone(runtime, configuration, initramfs)?;
    let mut value: serde_json::Value = serde_json::from_str(configuration)?;
    let guest_bytes = value
        .pointer("/memory/size")
        .and_then(serde_json::Value::as_u64)
        .ok_or(BootConfigError::Invalid("missing guest memory size"))?;
    if guest_bytes % (1024 * 1024) != 0 {
        return Err(BootConfigError::Invalid("guest memory must be a whole MiB"));
    }
    resources.check_guest_memory(guest_bytes / (1024 * 1024), reserved_memory_mib)?;
    let mut assigned = Vec::new();
    for assignment in devices.assignments() {
        for (index, locator) in assignment.host_device_ids().iter().enumerate() {
            let address = crate::vfio::PciAddress::from_locator(locator.as_str())?;
            assigned.push(serde_json::json!({
                "id": format!("device-{}-{index}", assignment.lease_id()),
                "path": address.sysfs_path(),
            }));
        }
    }
    value
        .as_object_mut()
        .ok_or(BootConfigError::Invalid("expected boot object"))?
        .insert("devices".into(), serde_json::json!(assigned));
    Ok(serde_json::to_string(&value)?)
}

/// The adapter accepts one catalog-bound initramfs, never a caller-selected path.
pub fn initramfs_component(
    execution: &ExecutionMetadata,
) -> Result<Option<&ArtifactComponentMetadata>, BootConfigError> {
    let mut inputs = execution
        .components()
        .iter()
        .filter(|component| component.kind == ArtifactComponentKind::Initrd);
    let Some(component) = inputs.next() else {
        return Ok(None);
    };
    if execution.runtime().kind != RuntimeKind::CloudHypervisor
        || component.role.as_ref().map(|role| role.as_str()) != Some("initramfs")
        || component.digest.is_none()
        || inputs.next().is_some()
    {
        return Err(BootConfigError::Invalid("unqualified initramfs component"));
    }
    Ok(Some(component))
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
    // Module-bearing boot bundles must not make template-controlled modules
    // loadable merely because the selected kernel permits optional enforcement.
    let modules = if input.initramfs {
        " module.sig_enforce=1"
    } else {
        ""
    };
    format!(
        "reboot=k panic=1 {devices} rw root=/dev/vda{build}{clock} transparent_hugepage=never{uart}{console}{modules} ip={IPV4_ADDR}::{GATEWAY_IPV4_ADDR}:{IPV4_NETMASK}:instance:{PRIMARY_INTERFACE}:off"
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

/// Validate the standalone adapter's privileged boot input before process launch.
pub fn validate_standalone(
    runtime: RuntimeKind,
    configuration: &str,
    initramfs: bool,
) -> Result<(), BootConfigError> {
    match runtime {
        RuntimeKind::CloudHypervisor => cloud_hypervisor::validate(configuration, initramfs),
        _ => Err(BootConfigError::UnsupportedRuntime(runtime)),
    }
}
