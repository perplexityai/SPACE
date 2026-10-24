use super::*;
use pplx_space_common_core::types::{BalloonDevice, TokenBucket};
use serde_json::{Value, json};

fn input() -> LinuxBoot<'static> {
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
