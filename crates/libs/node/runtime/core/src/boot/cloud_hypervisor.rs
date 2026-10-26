use super::{BootConfigError, BootSource, GuestBoot, object};
use pplx_space_common_core::types::{Arch, RuntimeKind};
use serde_json::{Value, json};

pub(super) fn configuration(input: &GuestBoot<'_>) -> Result<Value, BootConfigError> {
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
    let payload = match input.source {
        BootSource::LinuxDirect { initramfs } => {
            let mut payload = object([
                ("kernel", json!("/vmlinux.bin")),
                (
                    "cmdline",
                    json!(super::command_line(input, "net.ifnames=0", "", console)),
                ),
            ]);
            if initramfs {
                payload.insert("initramfs".into(), json!("/initrd.img"));
            }
            payload
        }
        BootSource::Uefi => object([("firmware", json!("/firmware.fd"))]),
    };
    let mut cpus = object([
        ("boot_vcpus", json!(input.cpus.get())),
        ("max_vcpus", json!(input.cpus.get())),
    ]);
    if input.source == BootSource::Uefi {
        cpus.insert("kvm_hyperv".into(), json!(true));
    }
    Ok(json!({
        "cpus": cpus,
        "memory": { "size": bytes, "shared": false, "hugepages": false, "thp": false },
        "payload": payload,
        "disks": [disk], "net": [net],
        "vsock": { "cid": input.guest_cid.get(), "socket": "/v.sock" },
        "serial": if input.source == BootSource::Uefi {
            json!({ "mode": "Socket", "socket": "/serial.sock" })
        } else { json!({ "mode": if input.console { "Tty" } else { "Off" } }) },
        "console": { "mode": "Off" },
        "landlock_enable": true
    }))
}

/// Node-manager admits only the device model this adapter constructs. The
/// control API accepts host paths and additional devices, so tenant-authored
/// configuration must never expand this model or disable its confinement.
pub(super) fn validate(configuration: &str, source: BootSource) -> Result<(), BootConfigError> {
    let value: Value = serde_json::from_str(configuration)?;
    let fields = value
        .as_object()
        .ok_or(BootConfigError::Invalid("expected boot object"))?;
    let initramfs = source.has_initramfs();
    let expected_initramfs = initramfs.then(|| json!("/initrd.img"));
    if value.pointer("/payload/initramfs") != expected_initramfs.as_ref() {
        return Err(BootConfigError::Invalid(
            "initramfs differs from the execution contract",
        ));
    }
    if initramfs
        && !value
            .pointer("/payload/cmdline")
            .and_then(Value::as_str)
            .is_some_and(|args| {
                args.split_whitespace()
                    .filter(|arg| arg.starts_with("module.sig_enforce="))
                    .eq(["module.sig_enforce=1"])
            })
    {
        return Err(BootConfigError::Invalid(
            "initramfs requires module signature enforcement",
        ));
    }
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
        || value.pointer("/disks/0/path") != Some(&json!("/rootfs.ext4"))
        || value.pointer("/vsock/socket") != Some(&json!("/v.sock"))
        || value.get("disks").and_then(Value::as_array).map(Vec::len) != Some(1)
        || value.get("net").and_then(Value::as_array).map(Vec::len) != Some(1)
    {
        return Err(BootConfigError::Invalid(
            "unsupported standalone device model",
        ));
    }
    let valid_serial = if source == BootSource::Uefi {
        value.get("serial") == Some(&json!({ "mode": "Socket", "socket": "/serial.sock" }))
    } else {
        value.pointer("/serial/socket").is_none()
            && matches!(
                value.pointer("/serial/mode").and_then(Value::as_str),
                Some("Tty" | "Off")
            )
    };
    if !valid_serial {
        return Err(BootConfigError::Invalid("unqualified serial endpoint"));
    }
    let valid_payload = match source {
        BootSource::LinuxDirect { .. } => {
            value.pointer("/payload/kernel") == Some(&json!("/vmlinux.bin"))
                && value.pointer("/cpus/kvm_hyperv").is_none()
        }
        BootSource::Uefi => {
            value.pointer("/payload/firmware") == Some(&json!("/firmware.fd"))
                && value.pointer("/cpus/kvm_hyperv") == Some(&json!(true))
        }
    };
    if !valid_payload {
        return Err(BootConfigError::Invalid(
            "payload differs from the execution contract",
        ));
    }
    for (name, allowed) in [
        (
            "payload",
            if source == BootSource::Uefi {
                &["firmware"][..]
            } else {
                &["kernel", "cmdline", "initramfs"][..]
            },
        ),
        ("cpus", &["boot_vcpus", "max_vcpus", "kvm_hyperv"][..]),
        ("memory", &["size", "shared", "hugepages", "thp"][..]),
        ("vsock", &["cid", "socket"][..]),
        ("serial", &["mode", "socket"][..]),
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
