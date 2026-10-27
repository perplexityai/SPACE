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

/// Node-manager admits only the device model this adapter constructs. The
/// control API accepts host paths and additional devices, so tenant-authored
/// configuration must never expand this model or disable its confinement.
pub(super) fn validate(configuration: &str) -> Result<(), BootConfigError> {
    let value: Value = serde_json::from_str(configuration)?;
    let fields = value
        .as_object()
        .ok_or(BootConfigError::Invalid("expected boot object"))?;
    const ALLOWED: &[&str] = &[
        "cpus",
        "memory",
        "payload",
        "disks",
        "net",
        "vsock",
        "serial",
        "console",
        "landlock_enable",
    ];
    if fields.keys().any(|key| !ALLOWED.contains(&key.as_str()))
        || value.get("landlock_enable") != Some(&json!(true))
        || value.pointer("/payload/kernel") != Some(&json!("/vmlinux.bin"))
        || value.pointer("/disks/0/path") != Some(&json!("/rootfs.ext4"))
        || value.pointer("/vsock/socket") != Some(&json!("/v.sock"))
        || value.get("disks").and_then(Value::as_array).map(Vec::len) != Some(1)
        || value.get("net").and_then(Value::as_array).map(Vec::len) != Some(1)
    {
        return Err(BootConfigError::Invalid(
            "unsupported standalone device model",
        ));
    }
    for (name, allowed) in [
        ("payload", &["kernel", "cmdline"][..]),
        ("cpus", &["boot_vcpus", "max_vcpus"][..]),
        ("memory", &["size", "shared", "hugepages", "thp"][..]),
        ("vsock", &["cid", "socket"][..]),
        ("serial", &["mode"][..]),
        ("console", &["mode"][..]),
    ] {
        let object = value
            .get(name)
            .and_then(Value::as_object)
            .ok_or(BootConfigError::Invalid("missing device configuration"))?;
        if object.keys().any(|key| !allowed.contains(&key.as_str())) {
            return Err(BootConfigError::Invalid("unqualified device option"));
        }
    }
    for (name, allowed) in [
        (
            "disks",
            &[
                "id",
                "path",
                "readonly",
                "image_type",
                "backing_files",
                "rate_limiter_config",
            ][..],
        ),
        (
            "net",
            &["id", "tap", "mac", "num_queues", "rate_limiter_config"][..],
        ),
    ] {
        let object = value
            .get(name)
            .and_then(Value::as_array)
            .and_then(|v| v.first())
            .and_then(Value::as_object)
            .ok_or(BootConfigError::Invalid("missing boot device"))?;
        if object.keys().any(|key| !allowed.contains(&key.as_str())) {
            return Err(BootConfigError::Invalid("unqualified boot device option"));
        }
    }
    if value.pointer("/memory/shared") != Some(&json!(false))
        || value.pointer("/memory/hugepages") != Some(&json!(false))
        || value.pointer("/disks/0/backing_files") != Some(&json!(false))
        || value.pointer("/disks/0/image_type") != Some(&json!("Raw"))
        || value.pointer("/net/0/num_queues") != Some(&json!(2))
    {
        return Err(BootConfigError::Invalid("unqualified boot resource option"));
    }
    Ok(())
}
