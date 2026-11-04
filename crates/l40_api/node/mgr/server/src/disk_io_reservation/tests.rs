use super::*;
use std::io::{Read as _, Write as _};
use std::net::TcpListener;
use std::sync::atomic::{AtomicUsize, Ordering};

struct FixedInstanceTypeProvider {
    instance_type: Result<&'static str, &'static str>,
    calls: AtomicUsize,
}

#[tonic::async_trait]
impl AwsInstanceTypeProvider for FixedInstanceTypeProvider {
    async fn instance_type(&self) -> Result<String, String> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.instance_type.map(str::to_owned).map_err(str::to_owned)
    }
}

#[derive(Default)]
struct RecordingWriter {
    writes: Vec<(BlockDevice, String)>,
    fail_after: Option<usize>,
}

impl IoMaxWriter for RecordingWriter {
    fn write(&mut self, device: BlockDevice, value: &str) -> Result<(), String> {
        if self.fail_after == Some(self.writes.len()) {
            return Err("injected io.max write failure".into());
        }
        self.writes.push((device, value.to_string()));
        Ok(())
    }
}

fn enabled_config() -> DiskIoReservationConfig {
    DiskIoReservationConfig {
        enabled: Some(true),
        target_path: PathBuf::from("/space/cache"),
        capacity_profile: Some("aws-r6id-metal".into()),
        sandbox_capacity_percent: Some(70),
        read_weight_percent: Some(50),
        write_weight_percent: Some(50),
    }
}

#[tokio::test]
async fn enabled_reservation_derives_capacity_profile_from_instance_type() {
    for (instance_type, expected_profile) in [
        ("m5d.metal", "aws-m5d-metal"),
        ("r6id.metal", "aws-r6id-metal"),
    ] {
        let provider = FixedInstanceTypeProvider {
            instance_type: Ok(instance_type),
            calls: AtomicUsize::new(0),
        };
        let mut config = enabled_config();
        config.capacity_profile = None;

        config.resolve_capacity_profile(&provider).await.unwrap();

        assert_eq!(config.capacity_profile.as_deref(), Some(expected_profile));
        assert_eq!(provider.calls.load(Ordering::Relaxed), 1);
    }
}

#[tokio::test]
async fn explicit_capacity_profile_override_skips_instance_type_lookup() {
    let provider = FixedInstanceTypeProvider {
        instance_type: Err("must not be called"),
        calls: AtomicUsize::new(0),
    };
    let mut config = enabled_config();

    config.resolve_capacity_profile(&provider).await.unwrap();

    assert_eq!(config.capacity_profile.as_deref(), Some("aws-r6id-metal"));
    assert_eq!(provider.calls.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn unknown_instance_type_fails_closed_without_selecting_a_profile() {
    let provider = FixedInstanceTypeProvider {
        instance_type: Ok("unknown.metal"),
        calls: AtomicUsize::new(0),
    };
    let mut config = enabled_config();
    config.capacity_profile = None;

    let error = config
        .resolve_capacity_profile(&provider)
        .await
        .unwrap_err();

    assert!(error.contains("unsupported disk_io_reservation AWS instance type"));
    assert!(config.capacity_profile.is_none());
}

fn spawn_imdsv2_server(instance_type: &'static str) -> (String, std::thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let handle = std::thread::spawn(move || {
        let read_request = |stream: &mut std::net::TcpStream| {
            let mut request = Vec::new();
            let mut buffer = [0_u8; 1024];
            while !request.ends_with(b"\r\n\r\n") {
                let read = stream.read(&mut buffer).unwrap();
                assert_ne!(read, 0, "request ended before the HTTP headers");
                request.extend_from_slice(&buffer[..read]);
            }
            String::from_utf8(request).unwrap()
        };

        let (mut token_stream, _) = listener.accept().unwrap();
        let token_request = read_request(&mut token_stream);
        assert!(token_request.starts_with("PUT /latest/api/token HTTP/1.1\r\n"));
        assert!(
            token_request
                .to_ascii_lowercase()
                .contains("x-aws-ec2-metadata-token-ttl-seconds: 60\r\n")
        );
        token_stream
            .write_all(
                b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\nConnection: close\r\n\r\ntest-token",
            )
            .unwrap();

        let (mut metadata_stream, _) = listener.accept().unwrap();
        let metadata_request = read_request(&mut metadata_stream);
        assert!(metadata_request.starts_with("GET /latest/meta-data/instance-type HTTP/1.1\r\n"));
        assert!(
            metadata_request
                .to_ascii_lowercase()
                .contains("x-aws-ec2-metadata-token: test-token\r\n")
        );
        write!(
            metadata_stream,
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{instance_type}",
            instance_type.len()
        )
        .unwrap();
    });
    (endpoint, handle)
}

#[tokio::test]
async fn imds_provider_uses_v2_token_to_read_instance_type() {
    let (endpoint, server) = spawn_imdsv2_server("m5d.metal");
    let provider = AwsImdsInstanceTypeProvider::for_endpoint(endpoint).unwrap();

    assert_eq!(provider.instance_type().await.unwrap(), "m5d.metal");
    server.join().unwrap();
}

fn devices() -> Vec<BlockDevice> {
    vec![
        BlockDevice {
            major: 259,
            minor: 0,
        },
        BlockDevice {
            major: 259,
            minor: 1,
        },
        BlockDevice {
            major: 259,
            minor: 2,
        },
        BlockDevice {
            major: 259,
            minor: 3,
        },
    ]
}

#[test]
fn absent_configuration_does_not_resolve_devices_or_write_cgroup() {
    let config = DiskIoReservationConfig {
        target_path: PathBuf::from("/path/that/does/not/exist"),
        ..DiskIoReservationConfig::default()
    };

    let reservation = apply_parent_disk_io_reservation(&config).unwrap();

    assert!(matches!(reservation, ParentDiskIoReservation::Unconfigured));
}

#[test]
fn aggregate_budget_is_divided_without_exceeding_requested_limit() {
    let mut writer = RecordingWriter::default();
    let reservation = apply_to_devices(&enabled_config(), devices(), &mut writer).unwrap();

    assert_eq!(
        writer.writes,
        vec![
            (
                BlockDevice {
                    major: 259,
                    minor: 0
                },
                "rbps=768606208 wbps=383778816 riops=187833 wiops=93916".into()
            ),
            (
                BlockDevice {
                    major: 259,
                    minor: 1
                },
                "rbps=768606208 wbps=383778816 riops=187833 wiops=93916".into()
            ),
            (
                BlockDevice {
                    major: 259,
                    minor: 2
                },
                "rbps=768606208 wbps=383778816 riops=187833 wiops=93916".into()
            ),
            (
                BlockDevice {
                    major: 259,
                    minor: 3
                },
                "rbps=768606208 wbps=383778816 riops=187833 wiops=93916".into()
            )
        ]
    );
    let ParentDiskIoReservation::Enabled {
        effective_aggregate_limits,
        ..
    } = reservation
    else {
        panic!("expected enabled reservation");
    };
    assert_eq!(
        effective_aggregate_limits,
        DiskIoLimits {
            read_iops: 751_332,
            write_iops: 375_664,
            read_bandwidth_mib_per_sec: 2_932,
            write_bandwidth_mib_per_sec: 1_464,
        }
    );
}

#[test]
fn disabled_configuration_removes_limits_from_resolved_devices() {
    let config = DiskIoReservationConfig {
        enabled: Some(false),
        ..DiskIoReservationConfig::default()
    };
    let mut writer = RecordingWriter::default();

    let reservation = apply_to_devices(&config, devices(), &mut writer).unwrap();

    assert!(matches!(
        reservation,
        ParentDiskIoReservation::Disabled { .. }
    ));
    assert_eq!(writer.writes.len(), 4);
    assert!(
        writer
            .writes
            .iter()
            .all(|(_, value)| value == "rbps=max wbps=max riops=max wiops=max")
    );
}

#[test]
fn enabled_configuration_rejects_incomplete_or_invalid_relative_policy_before_writing() {
    let mut writer = RecordingWriter::default();
    let mut missing = enabled_config();
    missing.capacity_profile = None;
    assert!(
        apply_to_devices(&missing, devices(), &mut writer)
            .unwrap_err()
            .contains("capacity_profile is required")
    );

    let mut invalid = enabled_config();
    invalid.read_weight_percent = Some(60);
    assert!(
        apply_to_devices(&invalid, devices(), &mut writer)
            .unwrap_err()
            .contains("sum to 100")
    );
    assert!(writer.writes.is_empty());
}

#[test]
fn capacity_profile_rejects_an_unexpected_leaf_device_count() {
    let config = enabled_config();
    let mut writer = RecordingWriter::default();
    let mut mismatched_devices = devices();
    mismatched_devices.pop();

    let error = apply_to_devices(&config, mismatched_devices, &mut writer).unwrap_err();

    assert!(error.contains("expects 4 leaf devices, but 3 resolved"));
    assert!(writer.writes.is_empty());
}

#[test]
fn unsupported_capacity_profile_fails_closed() {
    let mut config = enabled_config();
    config.capacity_profile = Some("unknown".into());
    let error = apply_to_devices(&config, devices(), &mut RecordingWriter::default()).unwrap_err();
    assert!(error.contains("unsupported disk_io_reservation.capacity_profile"));
}

#[test]
fn any_cgroup_write_failure_aborts_enablement() {
    let mut writer = RecordingWriter {
        fail_after: Some(1),
        ..RecordingWriter::default()
    };
    let error = apply_to_devices(&enabled_config(), devices(), &mut writer).unwrap_err();
    assert_eq!(error, "injected io.max write failure");
    assert_eq!(writer.writes.len(), 1);
}

#[cfg(target_os = "linux")]
#[test]
fn btrfs_parser_returns_every_member_device() {
    let output = r#"Label: 'cache'  uuid: deadbeef
        Total devices 2 FS bytes used 4096
        devid    1 size 100000 used 4096 path /dev/nvme1n1
        devid    2 size 100000 used 4096 path /dev/nvme2n1
    "#;
    assert_eq!(
        linux::parse_btrfs_device_paths(output).unwrap(),
        vec![PathBuf::from("/dev/nvme1n1"), PathBuf::from("/dev/nvme2n1")]
    );
}

#[cfg(target_os = "linux")]
#[test]
fn leaf_resolution_unwraps_and_deduplicates_stacked_devices() {
    let root = tempfile::tempdir().unwrap();
    for device in ["253:0", "259:0", "259:1"] {
        std::fs::create_dir_all(root.path().join(device).join("slaves")).unwrap();
    }
    for (name, value) in [("nvme0n1", "259:0"), ("nvme1n1", "259:1")] {
        let slave = root.path().join("253:0/slaves").join(name);
        std::fs::create_dir_all(&slave).unwrap();
        std::fs::write(slave.join("dev"), value).unwrap();
    }

    let mut leaves = BTreeSet::new();
    linux::expand_leaf_devices(
        BlockDevice {
            major: 253,
            minor: 0,
        },
        root.path(),
        &mut BTreeSet::new(),
        &mut leaves,
    )
    .unwrap();

    assert_eq!(
        leaves,
        BTreeSet::from([
            BlockDevice {
                major: 259,
                minor: 0
            },
            BlockDevice {
                major: 259,
                minor: 1
            }
        ])
    );
}
