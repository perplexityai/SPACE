use super::{BootConfigError, BootSource, GuestBoot, object};
use pplx_space_common_core::types::{Arch, RuntimeKind};
use serde_json::{Value, json};

pub(super) fn configuration(input: &GuestBoot<'_>) -> Result<Value, BootConfigError> {
    super::validate_resources(
        RuntimeKind::CloudHypervisor,
        input.balloon.is_some(),
        input.huge_pages,
    )?;
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
    let payload = match input.source {
        BootSource::LinuxDirect => object([
            ("kernel", json!("/vmlinux.bin")),
            (
                "cmdline",
                json!(super::command_line(input, "net.ifnames=0", "", console)),
            ),
        ]),
        BootSource::Uefi => object([("firmware", json!("/firmware.fd"))]),
    };
    let mut cpus = object([
        ("boot_vcpus", json!(input.cpus.get())),
        ("max_vcpus", json!(input.cpus.get())),
    ]);
    if input.source == BootSource::Uefi {
        cpus.insert("kvm_hyperv".into(), json!(true));
    }
    let rendered = json!({
        "cpus": cpus,
        "memory": { "size": bytes, "shared": false, "hugepages": false, "thp": false },
        "payload": payload,
        "disks": [disk], "net": [net],
        "vsock": { "cid": input.guest_cid.get(), "socket": "/v.sock" },
        "serial": if input.source == BootSource::Uefi {
            json!({ "mode": "Socket", "socket": "/serial.sock" })
        } else { json!({ "mode": if input.console { "Tty" } else { "Off" } }) },
        "console": { "mode": "Off" },
        "landlock_enable": true,
        "landlock_rules": [{"path": "/state", "access": "rw"}]
    });
    let parsed: Configuration = serde_json::from_value(rendered)?;
    parsed.validate(input.source)?;
    Ok(serde_json::to_value(parsed)?)
}

/// The only admitted standalone device model. Both the renderer and the
/// privileged consumer use this codec; unknown fields fail at every level.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    landlock_rules: Option<[StateAccess; 1]>,
}
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Cpus {
    #[serde(skip_serializing_if = "Option::is_none")]
    kvm_hyperv: Option<bool>,
    boot_vcpus: std::num::NonZeroU32,
    max_vcpus: std::num::NonZeroU32,
}
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Memory {
    size: std::num::NonZeroU64,
    shared: bool,
    hugepages: bool,
    thp: bool,
}
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
enum Payload {
    Linux(LinuxPayload),
    Uefi(FirmwarePayload),
}
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct FirmwarePayload {
    firmware: String,
}
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct LinuxPayload {
    kernel: String,
    cmdline: String,
}
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
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
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
enum ImageType {
    Raw,
}
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Net {
    id: String,
    tap: pplx_space_common_core::types::LinuxInterfaceName,
    mac: pplx_space_common_core::types::MacAddress,
    num_queues: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    rate_limiter_config: Option<Limiter>,
}
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Limiter {
    bandwidth: Bucket,
    ops: Bucket,
}
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Bucket {
    size: u64,
    refill_time: u64,
}
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Vsock {
    #[serde(default)]
    id: VsockId,
    cid: pplx_space_common_core::types::VsockCid,
    socket: String,
}
// CH restores a virtio device by ID. An omitted ID is auto-generated from
// the saved counter, creating a new device instead of restoring the old one.
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
enum VsockId {
    #[default]
    #[serde(rename = "space-vsock")]
    Space,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Console {
    #[serde(skip_serializing_if = "Option::is_none")]
    socket: Option<String>,
    mode: ConsoleMode,
}
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
enum ConsoleMode {
    Socket,
    Off,
    Tty,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct StateAccess {
    path: StateRoot,
    access: StatePermissions,
}
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
enum StateRoot {
    #[serde(rename = "/state")]
    State,
}
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
enum StatePermissions {
    #[serde(rename = "rw")]
    ReadWrite,
}

impl Configuration {
    pub(super) fn memory_bytes(&self) -> u64 {
        self.memory.size.get()
    }

    pub(super) fn require_state_access(&self) -> Result<(), BootConfigError> {
        if self.landlock_rules.is_none() {
            return Err(BootConfigError::Invalid(
                "runtime has no admitted state workspace",
            ));
        }
        Ok(())
    }

    pub(super) fn set_disk_rate(&mut self, rate: pplx_space_common_core::types::RateLimiter) {
        self.disks[0].rate_limiter_config = Some(Limiter {
            bandwidth: Bucket {
                size: rate.bandwidth.size,
                refill_time: rate.bandwidth.refill_time_ms,
            },
            ops: Bucket {
                size: rate.operations.size,
                refill_time: rate.operations.refill_time_ms,
            },
        });
    }

    pub(super) fn restore_on(&mut self, target: &Self) -> Result<(), BootConfigError> {
        let source = match &target.payload {
            Payload::Linux(_) => BootSource::LinuxDirect,
            Payload::Uefi(_) => BootSource::Uefi,
        };
        self.validate(source)?;
        target.validate(source)?;
        self.require_state_access()?;
        target.require_state_access()?;
        if self.cpus.boot_vcpus != target.cpus.boot_vcpus || self.memory.size != target.memory.size
        {
            return Err(BootConfigError::Invalid(
                "snapshot resources differ from restore admission",
            ));
        }
        // Guest MAC and CID are resident device state. Host locators and policy
        // belong to this execution; never inherit another execution's limits.
        self.net[0].tap = target.net[0].tap.clone();
        self.net[0].rate_limiter_config = target.net[0].rate_limiter_config.clone();
        self.disks[0].rate_limiter_config = target.disks[0].rate_limiter_config.clone();
        Ok(())
    }

    fn validate(&self, source: BootSource) -> Result<(), BootConfigError> {
        match (&self.payload, source) {
            (Payload::Linux(payload), BootSource::LinuxDirect) => {
                if payload.kernel != "/vmlinux.bin"
                    || self.cpus.kvm_hyperv.is_some()
                    || self.serial.socket.is_some()
                    || !matches!(self.serial.mode, ConsoleMode::Off | ConsoleMode::Tty)
                {
                    return Err(BootConfigError::Invalid(
                        "payload differs from the execution contract",
                    ));
                }
            }
            (Payload::Uefi(payload), BootSource::Uefi) => {
                if payload.firmware != "/firmware.fd"
                    || self.cpus.kvm_hyperv != Some(true)
                    || self.serial.mode != ConsoleMode::Socket
                    || self.serial.socket.as_deref() != Some("/serial.sock")
                {
                    return Err(BootConfigError::Invalid(
                        "payload differs from the execution contract",
                    ));
                }
            }
            _ => {
                return Err(BootConfigError::Invalid(
                    "payload differs from the execution contract",
                ));
            }
        }
        if !self.landlock_enable
            || self.disks[0].id != "rootfs"
            || self.net[0].id != "eth0"
            || self.disks[0].path != "/rootfs.ext4"
            || self.vsock.socket != "/v.sock"
            || self.memory.shared
            || self.memory.hugepages
            || self.memory.thp
            || self.disks[0].backing_files
            || self.disks[0].readonly
            || self.net[0].num_queues != 2
            || self.console.socket.is_some()
            || self.console.mode != ConsoleMode::Off
            || self.cpus.boot_vcpus != self.cpus.max_vcpus
        {
            return Err(BootConfigError::Invalid("unqualified boot device model"));
        }
        Ok(())
    }
}

pub(super) fn parse(
    configuration: &str,
    source: BootSource,
) -> Result<Configuration, BootConfigError> {
    let parsed: Configuration = serde_json::from_str(configuration)?;
    parsed.validate(source)?;
    Ok(parsed)
}
