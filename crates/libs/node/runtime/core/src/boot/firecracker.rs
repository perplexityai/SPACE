use super::{GuestBoot, object};
use serde_json::{Value, json};

pub(super) fn configuration(input: &GuestBoot<'_>) -> Value {
    let uart = format!(" 8250.nr_uarts={}", u8::from(input.console));
    let mut drive = object([
        ("drive_id", json!("rootfs")),
        ("path_on_host", json!("/rootfs.ext4")),
        ("is_root_device", json!(true)),
        ("is_read_only", json!(false)),
    ]);
    if let Some(rate) = input.disk_rate {
        drive.insert("rate_limiter".into(), super::token_buckets(&rate));
    }
    let mut net = object([
        (
            "iface_id",
            json!(pplx_space_common_core::guest::linux::network::PRIMARY_INTERFACE),
        ),
        ("guest_mac", json!(input.mac)),
        ("host_dev_name", json!(input.tap)),
    ]);
    if let Some(rate) = input.network_rate {
        net.insert("rx_rate_limiter".into(), super::token_buckets(&rate));
        net.insert("tx_rate_limiter".into(), super::token_buckets(&rate));
    }
    let mut machine = object([
        ("vcpu_count", json!(input.cpus.get())),
        ("mem_size_mib", json!(input.memory_mib.get())),
        ("smt", json!(false)),
    ]);
    if input.huge_pages {
        machine.insert("huge_pages".into(), json!("2M"));
    }
    let mut config = object([
        (
            "boot-source",
            json!({"kernel_image_path": "/vmlinux.bin", "boot_args": super::command_line(input, "pci=off", &uart, "ttyS0")}),
        ),
        ("drives", json!([drive])),
        ("machine-config", json!(machine)),
        ("network-interfaces", json!([net])),
        (
            "vsock",
            json!({"guest_cid": input.guest_cid.get(), "uds_path": "/v.sock"}),
        ),
    ]);
    if let Some(balloon) = input.balloon {
        config.insert("balloon".into(), crate::fc_api::balloon_json(balloon));
    }
    Value::Object(config)
}
