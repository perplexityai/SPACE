use super::*;
use pplx_space_common_core::types::{BalloonDevice, TokenBucket};
use serde_json::{Value, json};

pub(crate) fn input() -> LinuxBoot<'static> {
    LinuxBoot {
        guest: GuestPlatform {
            os: GuestOperatingSystem::Linux,
            architecture: Arch::Aarch64,
        },
        cpus: VcpuCount::try_from_u32(2).unwrap(),
        memory_mib: NonZeroU64::new(1024).unwrap(),
        guest_cid: NonZeroU32::new(3).unwrap(),
        tap: "tap0",
        mac: "02:00:00:00:00:01",
        disk_rate: Some(RateLimiter {
            bandwidth: TokenBucket {
                size: 1_048_576,
                refill_time_ms: 1000,
            },
            operations: TokenBucket {
                size: 100,
                refill_time_ms: 1000,
            },
        }),
        network_rate: None,
        balloon: None,
        huge_pages: false,
        console: false,
        builder: false,
    }
}

#[test]
fn cold_boot_adapters_preserve_resources_and_choose_their_device_model() {
    for runtime in [RuntimeKind::Firecracker, RuntimeKind::CloudHypervisor] {
        let mut input = input();
        input.network_rate = input.disk_rate;
        let config: Value = serde_json::from_str(&configuration(runtime, &input).unwrap()).unwrap();
        let (args, memory, drive) = match runtime {
            RuntimeKind::Firecracker => {
                assert_eq!(config["machine-config"]["vcpu_count"], 2);
                assert_eq!(config["drives"][0]["path_on_host"], "/rootfs.ext4");
                assert_eq!(config["vsock"]["uds_path"], "/v.sock");
                assert_eq!(config["network-interfaces"][0]["host_dev_name"], "tap0");
                (
                    &config["boot-source"]["boot_args"],
                    config["machine-config"]["mem_size_mib"].as_u64().unwrap() * 1024 * 1024,
                    &config["drives"][0]["rate_limiter"],
                )
            }
            RuntimeKind::CloudHypervisor => {
                assert_eq!(config["cpus"]["boot_vcpus"], 2);
                assert_eq!(config["disks"][0]["path"], "/rootfs.ext4");
                assert_eq!(config["vsock"]["socket"], "/v.sock");
                assert_eq!(config["net"][0]["tap"], "tap0");
                assert_eq!(config["landlock_enable"], true);
                assert_eq!(
                    config["net"][0]["rate_limiter_config"],
                    json!({
                        "bandwidth": {"size": 1_048_576, "refill_time": 1000},
                        "ops": {"size": 100, "refill_time": 1000},
                    })
                );
                (
                    &config["payload"]["cmdline"],
                    config["memory"]["size"].as_u64().unwrap(),
                    &config["disks"][0]["rate_limiter_config"],
                )
            }
            RuntimeKind::Gvisor => unreachable!(),
        };
        assert_eq!(memory, 1_073_741_824);
        assert_eq!(
            config["vsock"]["cid"]
                .as_u64()
                .or(config["vsock"]["guest_cid"].as_u64()),
            Some(3)
        );
        assert_eq!(
            drive,
            &json!({ "bandwidth": { "size": 1_048_576, "refill_time": 1000 }, "ops": { "size": 100, "refill_time": 1000 } })
        );
        let args = args.as_str().unwrap();
        assert!(args.contains("root=/dev/vda"));
        assert!(
            !args.contains("clocksource=kvm-clock"),
            "ARM guest must choose its own clock"
        );
        assert_eq!(
            args.contains("pci=off"),
            runtime == RuntimeKind::Firecracker
        );
        assert_eq!(
            args.contains("net.ifnames=0"),
            runtime == RuntimeKind::CloudHypervisor
        );
        assert!(!args.contains("console="));
    }
}

#[test]
fn standalone_console_matches_the_guest_platform_uart() {
    for (architecture, device) in [(Arch::Aarch64, "ttyAMA0"), (Arch::X86_64, "ttyS0")] {
        let mut boot = input();
        boot.guest.architecture = architecture;
        boot.console = true;
        let text = configuration(RuntimeKind::CloudHypervisor, &boot).unwrap();
        let config: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(config["serial"]["mode"], "Tty");
        assert_eq!(config["console"]["mode"], "Off");
        assert!(
            config["payload"]["cmdline"]
                .as_str()
                .unwrap()
                .contains(&format!("console={device}"))
        );
    }
}

#[test]
fn firecracker_retains_balloon_network_and_console_configuration() {
    let mut input = input();
    input.guest.architecture = Arch::X86_64;
    input.console = true;
    input.builder = true;
    input.huge_pages = true;
    input.network_rate = input.disk_rate;
    for device in [
        BalloonDevice::FreePageHinting,
        BalloonDevice::FreePageReporting,
    ] {
        input.balloon = Some(BalloonConfig::new(device, 1));
        let config: Value =
            serde_json::from_str(&configuration(RuntimeKind::Firecracker, &input).unwrap())
                .unwrap();
        let args = config["boot-source"]["boot_args"].as_str().unwrap();
        assert!(args.contains("8250.nr_uarts=1 console=ttyS0"));
        assert!(args.contains("clocksource=kvm-clock"));
        assert!(args.contains(pplx_space_common_core::types::TEMPLATE_RUNTIME_BUILD_MASK));
        assert_eq!(config["machine-config"]["huge_pages"], "2M");
        assert_eq!(
            config["balloon"]["free_page_hinting"],
            device == BalloonDevice::FreePageHinting
        );
        assert_eq!(
            config["balloon"]["free_page_reporting"],
            device == BalloonDevice::FreePageReporting
        );
        assert_eq!(
            config["network-interfaces"][0]["rx_rate_limiter"],
            config["drives"][0]["rate_limiter"]
        );
        assert_eq!(
            config["network-interfaces"][0]["tx_rate_limiter"],
            config["drives"][0]["rate_limiter"]
        );
    }
}

#[test]
fn standalone_admission_rejects_extra_devices_and_host_paths() {
    let text = configuration(RuntimeKind::CloudHypervisor, &input()).unwrap();
    validate_standalone(RuntimeKind::CloudHypervisor, &text).unwrap();
    let original: Value = serde_json::from_str(&text).unwrap();
    for (pointer, replacement) in [
        ("/payload/kernel", json!("/etc/shadow")),
        ("/vsock/socket", json!("/outside/socket")),
        ("/disks/0/path", json!("/dev/host")),
        ("/landlock_enable", json!(false)),
        ("/memory/shared", json!(true)),
        ("/disks/0/backing_files", json!(true)),
        ("/net/0/num_queues", json!(8)),
    ] {
        let mut config = original.clone();
        *config.pointer_mut(pointer).unwrap() = replacement;
        assert!(
            validate_standalone(RuntimeKind::CloudHypervisor, &config.to_string()).is_err(),
            "{pointer}"
        );
    }
    let mut extra = original;
    extra["devices"] = json!([{ "path": "/dev/vfio/3" }]);
    validate_standalone(RuntimeKind::CloudHypervisor, &extra.to_string()).unwrap_err();
}

#[test]
fn standalone_refuses_unqualified_memory_devices() {
    for balloon in [false, true] {
        let mut boot = input();
        if balloon {
            boot.balloon = Some(BalloonConfig::new(BalloonDevice::FreePageHinting, 1));
        } else {
            boot.huge_pages = true;
        }
        assert!(matches!(
            configuration(RuntimeKind::CloudHypervisor, &boot),
            Err(BootConfigError::UnsupportedFeature { .. })
        ));
    }
}

#[test]
fn persisted_ch_state_rebinds_host_resources_but_preserves_guest_identity() {
    let source = validate_standalone(
        RuntimeKind::CloudHypervisor,
        &configuration(RuntimeKind::CloudHypervisor, &input()).unwrap(),
    )
    .unwrap();
    let state = json!({"id":"vm", "snapshots":{}, "snapshot_data":{"sample":[1,2,3]}});
    let encoded = source.capture_state(state.clone()).unwrap();
    let mut target_input = input();
    target_input.tap = "tap9";
    target_input.mac = "02:00:00:00:00:09";
    target_input.guest_cid = NonZeroU32::new(12).unwrap();
    target_input.disk_rate = None;
    let mut target = validate_standalone(
        RuntimeKind::CloudHypervisor,
        &configuration(RuntimeKind::CloudHypervisor, &target_input).unwrap(),
    )
    .unwrap();
    let rate = pplx_space_common_core::types::RateLimiter {
        bandwidth: pplx_space_common_core::types::TokenBucket {
            size: 123456,
            refill_time_ms: 1000,
        },
        operations: pplx_space_common_core::types::TokenBucket {
            size: 123,
            refill_time_ms: 1000,
        },
    };
    target.set_disk_rate(rate);
    let (restored, restored_state) = target.restore_state(&encoded).unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&restored_state).unwrap(),
        state
    );
    let config: Value = serde_json::from_slice(&restored.encode().unwrap()).unwrap();
    assert_eq!(config["net"][0]["tap"], "tap9");
    assert_eq!(config["net"][0]["mac"], "02:00:00:00:00:01");
    assert_eq!(config["vsock"]["cid"], 3);
    assert_eq!(config["vsock"]["id"], "space-vsock");
    assert_eq!(
        config["disks"][0]["rate_limiter_config"]["bandwidth"]["size"],
        123456
    );
    assert_eq!(
        config["disks"][0]["rate_limiter_config"]["ops"]["size"],
        123
    );
    assert_eq!(config["payload"]["kernel"], "/vmlinux.bin");
    assert_eq!(
        config["landlock_rules"],
        json!([{"path":"/state","access":"rw"}])
    );
    // A restored execution remains capturable without reverting its guest identity.
    restored.capture_state(state).unwrap();
}

#[test]
fn persisted_ch_state_refuses_unknown_formats_devices_and_resource_mismatch() {
    let boot = validate_standalone(
        RuntimeKind::CloudHypervisor,
        &configuration(RuntimeKind::CloudHypervisor, &input()).unwrap(),
    )
    .unwrap();
    let baseline: Value = serde_json::from_slice(&boot.capture_state(json!({})).unwrap()).unwrap();
    for (pointer, value) in [
        ("/version", json!("future-format")),
        ("/configuration/disks/0/path", json!("/etc/shadow")),
        ("/configuration/landlock_enable", json!(false)),
        ("/configuration/landlock_rules/0/path", json!("/")),
        ("/configuration/cpus/boot_vcpus", json!(8)),
        (
            "/configuration/memory/size",
            json!(8 * 1024 * 1024 * 1024u64),
        ),
    ] {
        let mut invalid = baseline.clone();
        *invalid.pointer_mut(pointer).unwrap() = value;
        assert!(
            boot.restore_state(&serde_json::to_vec(&invalid).unwrap())
                .is_err(),
            "{pointer}"
        );
    }
    let mut unknown_device = baseline;
    unknown_device["configuration"]["devices"] = json!([{"path":"/sys/devices/example"}]);
    boot.restore_state(&serde_json::to_vec(&unknown_device).unwrap())
        .unwrap_err();
}
