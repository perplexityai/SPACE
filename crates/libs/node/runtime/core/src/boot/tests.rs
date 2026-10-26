use super::*;
use pplx_space_common_core::types::{BalloonDevice, TokenBucket};
use serde_json::{Value, json};

fn input() -> GuestBoot<'static> {
    GuestBoot {
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
        source: BootSource::LinuxDirect { initramfs: false },
    }
}

#[test]
fn cold_boot_adapters_preserve_resources_and_choose_their_device_model() {
    for runtime in [RuntimeKind::Firecracker, RuntimeKind::CloudHypervisor] {
        let config: Value =
            serde_json::from_str(&configuration(runtime, &input()).unwrap()).unwrap();
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
    validate_standalone(
        RuntimeKind::CloudHypervisor,
        &text,
        BootSource::LinuxDirect { initramfs: false },
    )
    .unwrap();
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
            validate_standalone(
                RuntimeKind::CloudHypervisor,
                &config.to_string(),
                BootSource::LinuxDirect { initramfs: false }
            )
            .is_err(),
            "{pointer}"
        );
    }
    let mut extra = original;
    extra["devices"] = json!([{ "path": "/dev/vfio/3" }]);
    assert!(
        validate_standalone(
            RuntimeKind::CloudHypervisor,
            &extra.to_string(),
            BootSource::LinuxDirect { initramfs: false }
        )
        .is_err()
    );
}

#[test]
fn initramfs_configuration_must_match_the_resolved_boot_contract() {
    use pplx_space_common_core::types::{Digest, RuntimeIdentity};
    let component = ArtifactComponentMetadata {
        kind: ArtifactComponentKind::Initrd,
        role: Some("initramfs".parse().unwrap()),
        version: Some("1".parse().unwrap()),
        digest: Some(Digest::sha256([42; 32])),
        format: Some("cpio".parse().unwrap()),
    };
    for (runtime, role, digest, accepted) in [
        (
            RuntimeKind::CloudHypervisor,
            "initramfs",
            component.digest,
            true,
        ),
        (
            RuntimeKind::CloudHypervisor,
            "boot",
            component.digest,
            false,
        ),
        (RuntimeKind::CloudHypervisor, "initramfs", None, false),
        (
            RuntimeKind::Firecracker,
            "initramfs",
            component.digest,
            false,
        ),
    ] {
        let execution = ExecutionMetadata::new(
            RuntimeIdentity {
                kind: runtime,
                version: "1".parse().unwrap(),
            },
            input().guest,
            None,
            vec![ArtifactComponentMetadata {
                role: Some(role.parse().unwrap()),
                digest,
                ..component.clone()
            }],
        )
        .unwrap();
        let selected = initramfs_component(&execution);
        if accepted {
            assert_eq!(selected.unwrap(), Some(&component));
        } else {
            assert!(matches!(selected, Err(BootConfigError::Invalid(_))));
        }
    }
    for initramfs in [false, true] {
        let mut boot = input();
        boot.source = BootSource::LinuxDirect { initramfs };
        let text = configuration(RuntimeKind::CloudHypervisor, &boot).unwrap();
        validate_standalone(
            RuntimeKind::CloudHypervisor,
            &text,
            BootSource::LinuxDirect { initramfs },
        )
        .unwrap();
        assert!(
            validate_standalone(
                RuntimeKind::CloudHypervisor,
                &text,
                BootSource::LinuxDirect {
                    initramfs: !initramfs
                }
            )
            .is_err()
        );
        let mut config: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(
            config["payload"]["cmdline"]
                .as_str()
                .unwrap()
                .split_whitespace()
                .any(|argument| argument == "module.sig_enforce=1"),
            initramfs,
        );
        if initramfs {
            for args in [
                "root=/dev/vda",
                "module.sig_enforce=0",
                "module.sig_enforce=1 module.sig_enforce=0",
            ] {
                let mut changed = config.clone();
                changed["payload"]["cmdline"] = json!(args);
                assert!(
                    validate_standalone(
                        RuntimeKind::CloudHypervisor,
                        &changed.to_string(),
                        BootSource::LinuxDirect { initramfs: true }
                    )
                    .is_err()
                );
            }
        }
        for path in [json!("/etc/shadow"), json!("../initrd.img"), Value::Null] {
            config["payload"]["initramfs"] = path;
            assert!(
                validate_standalone(
                    RuntimeKind::CloudHypervisor,
                    &config.to_string(),
                    BootSource::LinuxDirect { initramfs }
                )
                .is_err()
            );
        }
        if initramfs {
            assert!(matches!(
                configuration(RuntimeKind::Firecracker, &boot),
                Err(BootConfigError::UnsupportedFeature { .. })
            ));
        }
    }
}

#[test]
fn exclusive_devices_are_injected_from_assignments_without_accepting_configured_host_paths() {
    use pplx_space_common_core::types::{Digest, SandboxActivationAttemptId, SandboxId};
    use pplx_space_execution_common::{devices::decode_execution, proto};
    let input_devices = |locator: &str| proto::DeviceExecutionInput {
        sandbox_id: SandboxId::new().to_string(),
        requirements_digest: Digest::sha256([1; 32]).to_string(),
        assignments: vec![proto::ExclusiveDeviceAssignment {
            lease_id: SandboxActivationAttemptId::new().to_string(),
            parent_id: SandboxActivationAttemptId::new().to_string(),
            profile: "compute".into(),
            host_device_ids: vec![locator.into()],
        }],
    };
    let (_, devices) = decode_execution("", Some(&input_devices("pci:0000:02:00.0"))).unwrap();
    let devices = devices.unwrap();
    let runtime = RuntimeKind::CloudHypervisor;
    let base = configuration(runtime, &input()).unwrap();
    let resources = crate::catalog::DeviceResourceLimits::for_test(1024, 256);
    let injected = with_exclusive_devices(
        runtime,
        &base,
        BootSource::LinuxDirect { initramfs: false },
        &devices,
        resources,
        1280,
    )
    .unwrap();
    with_exclusive_devices(
        runtime,
        &base,
        BootSource::LinuxDirect { initramfs: false },
        &devices,
        resources,
        1279,
    )
    .unwrap_err();
    let mut injected: Value = serde_json::from_str(&injected).unwrap();
    assert_eq!(
        injected["devices"][0]["path"],
        "/sys/bus/pci/devices/0000:02:00.0"
    );
    assert_eq!(injected["devices"].as_array().unwrap().len(), 1);
    assert_eq!(injected["landlock_enable"], true);
    injected.as_object_mut().unwrap().remove("devices");
    assert_eq!(injected, serde_json::from_str::<Value>(&base).unwrap());
    for value in [
        json!({"devices":[{"path":"/unassigned/device"}]}),
        json!({"landlock_enable":false}),
        json!({"memory":{"size":1073741824,"shared":false,"hugepages":true,"thp":false}}),
    ] {
        let mut supplied: Value = serde_json::from_str(&base).unwrap();
        for (key, value) in value.as_object().unwrap() {
            supplied[key] = value.clone();
        }
        with_exclusive_devices(
            runtime,
            &supplied.to_string(),
            BootSource::LinuxDirect { initramfs: false },
            &devices,
            resources,
            1280,
        )
        .unwrap_err();
    }
    let (_, non_pci) = decode_execution("", Some(&input_devices("mdev:opaque"))).unwrap();
    with_exclusive_devices(
        runtime,
        &base,
        BootSource::LinuxDirect { initramfs: false },
        &non_pci.unwrap(),
        resources,
        1280,
    )
    .unwrap_err();
    with_exclusive_devices(
        RuntimeKind::Firecracker,
        &base,
        BootSource::LinuxDirect { initramfs: false },
        &devices,
        resources,
        1280,
    )
    .unwrap_err();
}

#[test]
fn firmware_boot_keeps_linux_inputs_out_of_the_device_model() {
    let mut boot = input();
    boot.guest = GuestPlatform {
        os: GuestOperatingSystem::Windows,
        architecture: Arch::X86_64,
    };
    boot.source = BootSource::Uefi;
    let text = configuration(RuntimeKind::CloudHypervisor, &boot).unwrap();
    validate_standalone(RuntimeKind::CloudHypervisor, &text, BootSource::Uefi).unwrap();
    let config: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(config["payload"], json!({"firmware": "/firmware.fd"}));
    assert_eq!(config["cpus"]["kvm_hyperv"], true);
    for (field, value) in [
        ("kernel", json!("/vmlinux.bin")),
        ("cmdline", json!("root=/dev/vda")),
        ("initramfs", json!("/initrd.img")),
        ("firmware", json!("/etc/shadow")),
    ] {
        let mut changed = config.clone();
        changed["payload"][field] = value;
        validate_standalone(
            RuntimeKind::CloudHypervisor,
            &changed.to_string(),
            BootSource::Uefi,
        )
        .unwrap_err();
    }
    validate_standalone(
        RuntimeKind::CloudHypervisor,
        &text,
        BootSource::LinuxDirect { initramfs: false },
    )
    .unwrap_err();
    configuration(RuntimeKind::Firecracker, &boot).unwrap_err();
    boot.builder = true;
    configuration(RuntimeKind::CloudHypervisor, &boot).unwrap_err();
    boot.builder = false;
    boot.guest.os = GuestOperatingSystem::Linux;
    configuration(RuntimeKind::CloudHypervisor, &boot).unwrap_err();
}
