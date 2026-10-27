use super::{BootConfigError, LinuxBoot, object};
use pplx_space_common_core::types::{Arch, RuntimeKind};
use serde_json::{Value, json};

pub(super) fn configuration(input: &LinuxBoot<'_>) -> Result<Value, BootConfigError> {
    if input.balloon.is_some() {
        return Err(BootConfigError::UnsupportedFeature {
            runtime: RuntimeKind::CloudHypervisor,
            feature: "ballooning",
        });
    }
    if input.huge_pages {
        return Err(BootConfigError::UnsupportedFeature {
            runtime: RuntimeKind::CloudHypervisor,
            feature: "huge pages",
        });
    }
    let bytes = input
        .memory_mib
        .get()
        .checked_mul(1024 * 1024)
        .ok_or(BootConfigError::MemoryOverflow)?;
    let mut disk = object([
        ("id", json!("rootfs")),
        ("path", json!("/rootfs.ext4")),
        ("readonly", json!(false)),
        ("image_type", json!("Raw")),
        ("backing_files", json!(false)),
    ]);
    let mut net = object([
        ("id", json!("eth0")),
        ("tap", json!(input.tap)),
        ("mac", json!(input.mac)),
        ("num_queues", json!(2)),
    ]);
    // CH applies this configuration to independent RX/TX buckets per queue
    // pair. Keep the default single queue pair to retain the assigned budget.
    if let Some(rate) = input.disk_rate {
        disk.insert("rate_limiter_config".into(), super::token_buckets(&rate));
    }
    if let Some(rate) = input.network_rate {
        net.insert("rate_limiter_config".into(), super::token_buckets(&rate));
    }
    // Use the platform UART for early boot diagnostics, before virtio drivers
    // initialize. The guest must name the device actually exposed by the VMM.
    let console = match input.guest.architecture {
        Arch::Aarch64 => "ttyAMA0",
        Arch::X86_64 => "ttyS0",
    };
    let rendered = json!({
        "cpus": { "boot_vcpus": input.cpus.get(), "max_vcpus": input.cpus.get() },
        "memory": { "size": bytes, "shared": false, "hugepages": false, "thp": false },
        "payload": { "kernel": "/vmlinux.bin", "cmdline": super::command_line(input, "net.ifnames=0", "", console) },
        "disks": [disk], "net": [net],
        "vsock": { "cid": input.guest_cid.get(), "socket": "/v.sock" },
        "serial": { "mode": if input.console { "Tty" } else { "Off" } },
        "console": { "mode": "Off" },
        "landlock_enable": true
    });
    let parsed: Configuration = serde_json::from_value(rendered)?;
    parsed.validate()?;
    Ok(serde_json::to_value(parsed)?)
}

/// The only admitted standalone device model. Both the renderer and the
/// privileged consumer use this codec; unknown fields fail at every level.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Configuration {
    cpus: Cpus,
    memory: Memory,
    payload: Payload,
    disks: [Disk; 1],
    net: [Net; 1],
    vsock: Vsock,
    serial: Console,
    console: Console,
    landlock_enable: bool,
}
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Cpus {
    boot_vcpus: std::num::NonZeroU32,
    max_vcpus: std::num::NonZeroU32,
}
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Memory {
    size: std::num::NonZeroU64,
    shared: bool,
    hugepages: bool,
    thp: bool,
}
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Payload {
    kernel: String,
    cmdline: String,
}
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Disk {
    id: String,
    path: String,
    readonly: bool,
    image_type: ImageType,
    backing_files: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    rate_limiter_config: Option<Limiter>,
}
#[derive(Debug, serde::Serialize, serde::Deserialize)]
enum ImageType {
    Raw,
}
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Net {
    id: String,
    tap: pplx_space_common_core::types::LinuxInterfaceName,
    mac: pplx_space_common_core::types::MacAddress,
    num_queues: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    rate_limiter_config: Option<Limiter>,
}
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Limiter {
    bandwidth: Bucket,
    ops: Bucket,
}
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Bucket {
    size: u64,
    refill_time: u64,
}
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Vsock {
    cid: pplx_space_common_core::types::VsockCid,
    socket: String,
}
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Console {
    mode: ConsoleMode,
}
#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
enum ConsoleMode {
    Off,
    Tty,
}

impl Configuration {
    fn validate(&self) -> Result<(), BootConfigError> {
        if !self.landlock_enable
            || self.payload.kernel != "/vmlinux.bin"
            || self.disks[0].path != "/rootfs.ext4"
            || self.vsock.socket != "/v.sock"
            || self.memory.shared
            || self.memory.hugepages
            || self.memory.thp
            || self.disks[0].backing_files
            || self.disks[0].readonly
            || self.net[0].num_queues != 2
            || self.console.mode != ConsoleMode::Off
            || self.cpus.boot_vcpus != self.cpus.max_vcpus
        {
            return Err(BootConfigError::Invalid("unqualified boot device model"));
        }
        Ok(())
    }
}

pub(super) fn parse(configuration: &str) -> Result<Configuration, BootConfigError> {
    let parsed: Configuration = serde_json::from_str(configuration)?;
    parsed.validate()?;
    Ok(parsed)
}
