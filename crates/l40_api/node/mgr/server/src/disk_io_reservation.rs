//! Aggregate sandbox disk-I/O protection for node-local management services.
//!
//! Per-sandbox Firecracker rate limiters protect sandboxes from each other only
//! while their configured limits fit on the host. This module caps the parent
//! `/sys/fs/cgroup/space` cgroup so all sandbox processes together leave a
//! measured I/O reservation for node-local management processes outside that
//! cgroup.

use std::fmt;
use std::path::PathBuf;
use std::time::Duration;

#[cfg(target_os = "linux")]
use std::collections::BTreeSet;
#[cfg(target_os = "linux")]
use std::path::Path;

use serde::Deserialize;

const DEFAULT_TARGET_PATH: &str = "/space/cache";
const AWS_IMDS_ENDPOINT: &str = "http://169.254.169.254";
const AWS_IMDS_TIMEOUT: Duration = Duration::from_secs(2);
#[cfg(target_os = "linux")]
const CGROUP_IO_MAX_PATH: &str = "/sys/fs/cgroup/space/io.max";
#[cfg(target_os = "linux")]
const SYS_DEV_BLOCK_PATH: &str = "/sys/dev/block";
#[cfg(any(target_os = "linux", test))]
const MIB_BYTES: u64 = 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DiskIoLimits {
    pub read_iops: u64,
    pub write_iops: u64,
    pub read_bandwidth_mib_per_sec: u64,
    pub write_bandwidth_mib_per_sec: u64,
}

#[cfg(any(target_os = "linux", test))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DiskIoCapacityProfile {
    name: &'static str,
    expected_device_count: usize,
    capacity: DiskIoLimits,
}

#[cfg(any(target_os = "linux", test))]
struct ResolvedDiskIoPolicy {
    profile: DiskIoCapacityProfile,
    sandbox_capacity_percent: u64,
    read_weight_percent: u64,
    write_weight_percent: u64,
    limits: DiskIoLimits,
}

#[cfg(any(target_os = "linux", test))]
impl DiskIoCapacityProfile {
    fn named(name: &str) -> Result<Self, String> {
        // AWS publishes aggregate 4 KiB queue-depth-saturation IOPS for these
        // instance-store shapes. Bandwidth is the corresponding conservative
        // 4 KiB throughput, rounded down to whole MiB/s.
        // https://docs.aws.amazon.com/ec2/latest/instancetypes/mo.html
        match name {
            "aws-r6id-metal" => Ok(Self {
                name: "aws-r6id-metal",
                expected_device_count: 4,
                capacity: DiskIoLimits {
                    read_iops: 2_146_664,
                    write_iops: 1_073_336,
                    read_bandwidth_mib_per_sec: 8_385,
                    write_bandwidth_mib_per_sec: 4_192,
                },
            }),
            "aws-m5d-metal" => Ok(Self {
                name: "aws-m5d-metal",
                expected_device_count: 4,
                capacity: DiskIoLimits {
                    read_iops: 1_400_000,
                    write_iops: 680_000,
                    read_bandwidth_mib_per_sec: 5_468,
                    write_bandwidth_mib_per_sec: 2_656,
                },
            }),
            _ => Err(format!(
                "unsupported disk_io_reservation.capacity_profile {name:?}"
            )),
        }
    }

    fn for_aws_instance_type(instance_type: &str) -> Result<Self, String> {
        match instance_type {
            "m5d.metal" => Self::named("aws-m5d-metal"),
            "r6id.metal" => Self::named("aws-r6id-metal"),
            _ => Err(format!(
                "unsupported disk_io_reservation AWS instance type {instance_type:?}"
            )),
        }
    }
}

#[tonic::async_trait]
pub trait AwsInstanceTypeProvider: Send + Sync {
    async fn instance_type(&self) -> Result<String, String>;
}

pub struct AwsImdsInstanceTypeProvider {
    client: reqwest::Client,
    endpoint: String,
}

impl AwsImdsInstanceTypeProvider {
    pub fn new() -> Result<Self, String> {
        Self::build(AWS_IMDS_ENDPOINT.to_owned())
    }

    fn build(endpoint: String) -> Result<Self, String> {
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(AWS_IMDS_TIMEOUT)
            .build()
            .map_err(|error| format!("failed to build AWS IMDS client: {error}"))?;
        Ok(Self { client, endpoint })
    }

    #[cfg(test)]
    fn for_endpoint(endpoint: String) -> Result<Self, String> {
        Self::build(endpoint)
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.endpoint.trim_end_matches('/'))
    }
}

#[tonic::async_trait]
impl AwsInstanceTypeProvider for AwsImdsInstanceTypeProvider {
    async fn instance_type(&self) -> Result<String, String> {
        let token_response = self
            .client
            .put(self.url("/latest/api/token"))
            .header("x-aws-ec2-metadata-token-ttl-seconds", "60")
            .send()
            .await
            .map_err(|error| format!("failed to request AWS IMDSv2 token: {error}"))?
            .error_for_status()
            .map_err(|error| format!("AWS IMDSv2 token request failed: {error}"))?;
        let token = token_response
            .text()
            .await
            .map_err(|error| format!("failed to read AWS IMDSv2 token: {error}"))?;
        if token.is_empty() {
            return Err("AWS IMDSv2 returned an empty token".into());
        }

        let instance_type_response = self
            .client
            .get(self.url("/latest/meta-data/instance-type"))
            .header("x-aws-ec2-metadata-token", token)
            .send()
            .await
            .map_err(|error| format!("failed to request AWS instance type from IMDSv2: {error}"))?
            .error_for_status()
            .map_err(|error| format!("AWS IMDSv2 instance-type request failed: {error}"))?;
        let instance_type = instance_type_response
            .text()
            .await
            .map_err(|error| format!("failed to read AWS instance type from IMDSv2: {error}"))?;
        let instance_type = instance_type.trim();
        if instance_type.is_empty() {
            return Err("AWS IMDSv2 returned an empty instance type".into());
        }
        Ok(instance_type.to_owned())
    }
}

#[cfg(any(target_os = "linux", test))]
impl DiskIoLimits {
    fn checked_bandwidth_bytes(self) -> Result<(u64, u64), String> {
        let read = self
            .read_bandwidth_mib_per_sec
            .checked_mul(MIB_BYTES)
            .ok_or_else(|| "sandbox read bandwidth overflows bytes per second".to_string())?;
        let write = self
            .write_bandwidth_mib_per_sec
            .checked_mul(MIB_BYTES)
            .ok_or_else(|| "sandbox write bandwidth overflows bytes per second".to_string())?;
        Ok((read, write))
    }

    fn validate(self) -> Result<Self, String> {
        if self.read_iops == 0
            || self.write_iops == 0
            || self.read_bandwidth_mib_per_sec == 0
            || self.write_bandwidth_mib_per_sec == 0
        {
            return Err("enabled disk I/O reservation limits must all be greater than 0".into());
        }
        self.checked_bandwidth_bytes()?;
        Ok(self)
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DiskIoReservationConfig {
    /// `None` preserves the pre-feature behavior without touching `io.max`.
    /// `Some(false)` explicitly removes limits for the currently resolved
    /// backing devices, providing a rollback path after enablement.
    pub enabled: Option<bool>,
    pub target_path: PathBuf,
    /// Optional override for non-EC2 tests and emergency operation. Enabled
    /// EC2 nodes normally derive this from their IMDS instance type.
    pub capacity_profile: Option<String>,
    /// Total normalized hardware capacity available to sandbox reads and
    /// writes together. The remainder stays outside the sandbox parent cgroup.
    pub sandbox_capacity_percent: Option<u64>,
    /// Share of the sandbox budget assigned to reads. Read and write weights
    /// must sum to 100 so independently enforceable io.max limits cannot
    /// consume more than the total normalized sandbox budget under mixed I/O.
    pub read_weight_percent: Option<u64>,
    pub write_weight_percent: Option<u64>,
}

impl Default for DiskIoReservationConfig {
    fn default() -> Self {
        Self {
            enabled: None,
            target_path: PathBuf::from(DEFAULT_TARGET_PATH),
            capacity_profile: None,
            sandbox_capacity_percent: None,
            read_weight_percent: None,
            write_weight_percent: None,
        }
    }
}

#[cfg(any(target_os = "linux", test))]
impl DiskIoReservationConfig {
    pub async fn resolve_capacity_profile(
        &mut self,
        provider: &dyn AwsInstanceTypeProvider,
    ) -> Result<(), String> {
        if self.enabled != Some(true) || self.capacity_profile.is_some() {
            return Ok(());
        }
        let instance_type = provider.instance_type().await?;
        let profile = DiskIoCapacityProfile::for_aws_instance_type(instance_type.trim())?;
        self.capacity_profile = Some(profile.name.to_owned());
        Ok(())
    }

    fn resolve_policy(&self) -> Result<ResolvedDiskIoPolicy, String> {
        let missing =
            |field: &str| format!("disk_io_reservation.{field} is required when enabled is true");
        let profile = DiskIoCapacityProfile::named(
            self.capacity_profile
                .as_deref()
                .ok_or_else(|| missing("capacity_profile"))?,
        )?;
        let sandbox_percent = self
            .sandbox_capacity_percent
            .ok_or_else(|| missing("sandbox_capacity_percent"))?;
        let read_weight = self
            .read_weight_percent
            .ok_or_else(|| missing("read_weight_percent"))?;
        let write_weight = self
            .write_weight_percent
            .ok_or_else(|| missing("write_weight_percent"))?;
        if !(1..=100).contains(&sandbox_percent) {
            return Err("disk_io_reservation.sandbox_capacity_percent must be in 1..=100".into());
        }
        if read_weight == 0
            || write_weight == 0
            || read_weight.checked_add(write_weight) != Some(100)
        {
            return Err(
                "disk_io_reservation read_weight_percent and write_weight_percent must both be greater than 0 and sum to 100"
                    .into(),
            );
        }
        let weighted = |capacity: u64, weight: u64| -> Result<u64, String> {
            let value = u128::from(capacity)
                .checked_mul(u128::from(sandbox_percent))
                .and_then(|value| value.checked_mul(u128::from(weight)))
                .ok_or_else(|| "disk I/O capacity calculation overflowed".to_string())?
                / 10_000;
            u64::try_from(value)
                .map_err(|_| "disk I/O capacity calculation does not fit in u64".to_string())
        };
        let limits = DiskIoLimits {
            read_iops: weighted(profile.capacity.read_iops, read_weight)?,
            write_iops: weighted(profile.capacity.write_iops, write_weight)?,
            read_bandwidth_mib_per_sec: weighted(
                profile.capacity.read_bandwidth_mib_per_sec,
                read_weight,
            )?,
            write_bandwidth_mib_per_sec: weighted(
                profile.capacity.write_bandwidth_mib_per_sec,
                write_weight,
            )?,
        }
        .validate()?;
        Ok(ResolvedDiskIoPolicy {
            profile,
            sandbox_capacity_percent: sandbox_percent,
            read_weight_percent: read_weight,
            write_weight_percent: write_weight,
            limits,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct BlockDevice {
    pub major: u64,
    pub minor: u64,
}

impl fmt::Display for BlockDevice {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}:{}", self.major, self.minor)
    }
}

#[derive(Debug)]
pub enum ParentDiskIoReservation {
    Unconfigured,
    Disabled {
        target_path: PathBuf,
        devices: Vec<BlockDevice>,
    },
    Enabled {
        target_path: PathBuf,
        capacity_profile: &'static str,
        hardware_capacity: DiskIoLimits,
        sandbox_capacity_percent: u64,
        read_weight_percent: u64,
        write_weight_percent: u64,
        devices: Vec<BlockDevice>,
        effective_aggregate_limits: DiskIoLimits,
        per_device_limits: DiskIoLimits,
    },
}

impl ParentDiskIoReservation {
    pub fn mode(&self) -> &'static str {
        match self {
            Self::Unconfigured => "unconfigured",
            Self::Disabled { .. } => "disabled",
            Self::Enabled { .. } => "enabled",
        }
    }

    pub fn device_count(&self) -> usize {
        match self {
            Self::Unconfigured => 0,
            Self::Disabled { devices, .. } | Self::Enabled { devices, .. } => devices.len(),
        }
    }
}

pub fn apply_parent_disk_io_reservation(
    config: &DiskIoReservationConfig,
) -> Result<ParentDiskIoReservation, String> {
    if config.enabled.is_none() {
        let reservation = ParentDiskIoReservation::Unconfigured;
        emit_metrics(&reservation);
        return Ok(reservation);
    }

    #[cfg(not(target_os = "linux"))]
    {
        let _ = config;
        Err("disk I/O reservation is supported only on Linux".into())
    }

    #[cfg(target_os = "linux")]
    {
        let devices = linux::resolve_leaf_devices(&config.target_path)?;
        let mut writer = FsIoMaxWriter::new(PathBuf::from(CGROUP_IO_MAX_PATH));
        let reservation = apply_to_devices(config, devices, &mut writer)?;
        emit_metrics(&reservation);
        Ok(reservation)
    }
}

#[cfg(any(target_os = "linux", test))]
trait IoMaxWriter {
    fn write(&mut self, device: BlockDevice, value: &str) -> Result<(), String>;
}

#[cfg(target_os = "linux")]
struct FsIoMaxWriter {
    path: PathBuf,
}

#[cfg(target_os = "linux")]
impl FsIoMaxWriter {
    fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

#[cfg(target_os = "linux")]
impl IoMaxWriter for FsIoMaxWriter {
    fn write(&mut self, device: BlockDevice, value: &str) -> Result<(), String> {
        std::fs::write(&self.path, format!("{device} {value}\n")).map_err(|error| {
            format!(
                "failed to write disk I/O limit for device {device} to {}: {error}",
                self.path.display()
            )
        })
    }
}

#[cfg(any(target_os = "linux", test))]
fn apply_to_devices(
    config: &DiskIoReservationConfig,
    devices: Vec<BlockDevice>,
    writer: &mut dyn IoMaxWriter,
) -> Result<ParentDiskIoReservation, String> {
    if devices.is_empty() {
        return Err(format!(
            "no backing block devices resolved for {}",
            config.target_path.display()
        ));
    }

    if config.enabled == Some(false) {
        for device in &devices {
            writer.write(*device, "rbps=max wbps=max riops=max wiops=max")?;
        }
        return Ok(ParentDiskIoReservation::Disabled {
            target_path: config.target_path.clone(),
            devices,
        });
    }

    let policy = config.resolve_policy()?;
    let profile = policy.profile;
    if devices.len() != profile.expected_device_count {
        return Err(format!(
            "disk I/O capacity profile {} expects {} leaf devices, but {} resolved for {}",
            profile.name,
            profile.expected_device_count,
            devices.len(),
            config.target_path.display()
        ));
    }
    let device_count = u64::try_from(devices.len())
        .map_err(|_| "backing block device count does not fit in u64".to_string())?;
    let per_device = DiskIoLimits {
        read_iops: policy.limits.read_iops / device_count,
        write_iops: policy.limits.write_iops / device_count,
        read_bandwidth_mib_per_sec: policy.limits.read_bandwidth_mib_per_sec / device_count,
        write_bandwidth_mib_per_sec: policy.limits.write_bandwidth_mib_per_sec / device_count,
    }
    .validate()
    .map_err(|error| {
        format!("aggregate disk I/O budget is too small for {device_count} devices: {error}")
    })?;
    let (read_bps, write_bps) = per_device.checked_bandwidth_bytes()?;

    for device in &devices {
        writer.write(
            *device,
            &format!(
                "rbps={read_bps} wbps={write_bps} riops={} wiops={}",
                per_device.read_iops, per_device.write_iops
            ),
        )?;
    }

    let effective_aggregate_limits = DiskIoLimits {
        read_iops: per_device.read_iops * device_count,
        write_iops: per_device.write_iops * device_count,
        read_bandwidth_mib_per_sec: per_device.read_bandwidth_mib_per_sec * device_count,
        write_bandwidth_mib_per_sec: per_device.write_bandwidth_mib_per_sec * device_count,
    };
    Ok(ParentDiskIoReservation::Enabled {
        target_path: config.target_path.clone(),
        capacity_profile: profile.name,
        hardware_capacity: profile.capacity,
        sandbox_capacity_percent: policy.sandbox_capacity_percent,
        read_weight_percent: policy.read_weight_percent,
        write_weight_percent: policy.write_weight_percent,
        devices,
        effective_aggregate_limits,
        per_device_limits: per_device,
    })
}

fn emit_metrics(reservation: &ParentDiskIoReservation) {
    use pplx_space_metrics::node_disk;

    node_disk::sandbox_io_reservation_configured(!matches!(
        reservation,
        ParentDiskIoReservation::Unconfigured
    ));
    node_disk::sandbox_io_reservation_enabled(matches!(
        reservation,
        ParentDiskIoReservation::Enabled { .. }
    ));
    node_disk::sandbox_io_reservation_device_count(reservation.device_count() as u64);
    if let ParentDiskIoReservation::Enabled {
        effective_aggregate_limits,
        ..
    } = reservation
    {
        node_disk::sandbox_io_read_iops_max(effective_aggregate_limits.read_iops);
        node_disk::sandbox_io_write_iops_max(effective_aggregate_limits.write_iops);
        node_disk::sandbox_io_read_bandwidth_mib_per_sec_max(
            effective_aggregate_limits.read_bandwidth_mib_per_sec,
        );
        node_disk::sandbox_io_write_bandwidth_mib_per_sec_max(
            effective_aggregate_limits.write_bandwidth_mib_per_sec,
        );
    } else {
        node_disk::sandbox_io_read_iops_max(0);
        node_disk::sandbox_io_write_iops_max(0);
        node_disk::sandbox_io_read_bandwidth_mib_per_sec_max(0);
        node_disk::sandbox_io_write_bandwidth_mib_per_sec_max(0);
    }
}

#[cfg(target_os = "linux")]
mod linux {
    use super::*;
    use crate::host_command::Command;
    use std::os::unix::fs::{FileTypeExt as _, MetadataExt as _};

    pub(super) fn resolve_leaf_devices(target_path: &Path) -> Result<Vec<BlockDevice>, String> {
        let source_devices = resolve_filesystem_devices(target_path)?;
        let mut leaves = BTreeSet::new();
        let mut visiting = BTreeSet::new();
        for device in source_devices {
            expand_leaf_devices(
                device,
                Path::new(SYS_DEV_BLOCK_PATH),
                &mut visiting,
                &mut leaves,
            )?;
        }
        Ok(leaves.into_iter().collect())
    }

    fn resolve_filesystem_devices(target_path: &Path) -> Result<Vec<BlockDevice>, String> {
        let statfs = nix::sys::statfs::statfs(target_path).map_err(|error| {
            format!(
                "failed to inspect filesystem for {}: {error}",
                target_path.display()
            )
        })?;
        if statfs.filesystem_type() == nix::sys::statfs::BTRFS_SUPER_MAGIC {
            return resolve_btrfs_devices(target_path);
        }

        let metadata = std::fs::metadata(target_path).map_err(|error| {
            format!(
                "failed to inspect disk I/O reservation target {}: {error}",
                target_path.display()
            )
        })?;
        Ok(vec![block_device_from_dev_t(metadata.dev())])
    }

    fn resolve_btrfs_devices(target_path: &Path) -> Result<Vec<BlockDevice>, String> {
        let output = crate::host_command::output(
            Command::new("btrfs")
                .args(["filesystem", "show", "--raw"])
                .arg(target_path),
            crate::host_command::PROBE_BUDGET,
            &[],
        )
        .map_err(|error| format!("failed to execute btrfs filesystem show: {error}"))?;
        if !output.status.success() {
            return Err(format!(
                "btrfs filesystem show failed for {}: {}",
                target_path.display(),
                String::from_utf8_lossy(&output.stderr).trim()
            ));
        }
        let paths = parse_btrfs_device_paths(&String::from_utf8_lossy(&output.stdout))?;
        paths
            .into_iter()
            .map(|path| {
                let metadata = std::fs::metadata(&path).map_err(|error| {
                    format!("failed to inspect btrfs device {}: {error}", path.display())
                })?;
                if !metadata.file_type().is_block_device() {
                    return Err(format!(
                        "btrfs member {} is not a block device",
                        path.display()
                    ));
                }
                Ok(block_device_from_dev_t(metadata.rdev()))
            })
            .collect()
    }

    fn block_device_from_dev_t(device: libc::dev_t) -> BlockDevice {
        BlockDevice {
            major: nix::sys::stat::major(device),
            minor: nix::sys::stat::minor(device),
        }
    }

    pub(super) fn parse_btrfs_device_paths(output: &str) -> Result<Vec<PathBuf>, String> {
        let paths: Vec<_> = output
            .lines()
            .filter_map(|line| {
                let trimmed = line.trim_start();
                if !trimmed.starts_with("devid ") {
                    return None;
                }
                trimmed
                    .rsplit_once(" path ")
                    .map(|(_, path)| PathBuf::from(path.trim()))
            })
            .filter(|path| !path.as_os_str().is_empty())
            .collect();
        if paths.is_empty() {
            return Err("btrfs filesystem show returned no member devices".into());
        }
        Ok(paths)
    }

    pub(super) fn expand_leaf_devices(
        device: BlockDevice,
        sys_dev_block: &Path,
        visiting: &mut BTreeSet<BlockDevice>,
        leaves: &mut BTreeSet<BlockDevice>,
    ) -> Result<(), String> {
        if !visiting.insert(device) {
            return Err(format!(
                "cycle detected in block-device topology at {device}"
            ));
        }
        let device_path = sys_dev_block.join(device.to_string());
        if !device_path.exists() {
            return Err(format!(
                "block device {device} is absent from {}",
                sys_dev_block.display()
            ));
        }
        let slaves_path = device_path.join("slaves");
        let mut children = Vec::new();
        if slaves_path.exists() {
            for entry in std::fs::read_dir(&slaves_path)
                .map_err(|error| format!("failed to read {}: {error}", slaves_path.display()))?
            {
                let entry = entry.map_err(|error| {
                    format!("failed to read entry in {}: {error}", slaves_path.display())
                })?;
                let dev_value =
                    std::fs::read_to_string(entry.path().join("dev")).map_err(|error| {
                        format!(
                            "failed to resolve slave {}: {error}",
                            entry.path().display()
                        )
                    })?;
                children.push(parse_block_device(dev_value.trim())?);
            }
        }
        if children.is_empty() {
            leaves.insert(device);
        } else {
            for child in children {
                expand_leaf_devices(child, sys_dev_block, visiting, leaves)?;
            }
        }
        visiting.remove(&device);
        Ok(())
    }

    fn parse_block_device(value: &str) -> Result<BlockDevice, String> {
        let (major, minor) = value
            .split_once(':')
            .ok_or_else(|| format!("invalid block device identifier {value:?}"))?;
        Ok(BlockDevice {
            major: major
                .parse()
                .map_err(|_| format!("invalid block device major in {value:?}"))?,
            minor: minor
                .parse()
                .map_err(|_| format!("invalid block device minor in {value:?}"))?,
        })
    }
}

#[cfg(test)]
mod tests;
