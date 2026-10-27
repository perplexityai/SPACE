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
    Ok(json!({
        "cpus": { "boot_vcpus": input.cpus.get(), "max_vcpus": input.cpus.get() },
        "memory": { "size": bytes, "shared": false, "hugepages": false, "thp": false },
        "payload": { "kernel": "/vmlinux.bin", "cmdline": super::command_line(input, "net.ifnames=0", "", console) },
        "disks": [disk], "net": [net],
        "vsock": { "cid": input.guest_cid.get(), "socket": "/v.sock" },
        "serial": { "mode": if input.console { "Tty" } else { "Off" } },
        "console": { "mode": "Off" },
        "landlock_enable": true
    }))
}
