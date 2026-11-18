use std::fs::File;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{FileTypeExt, MetadataExt};
use std::path::Path;

use nix::fcntl::{OFlag, open};
use nix::sys::stat::Mode;
use pplx_space_execution_common::proto::BlockVolumeBinding;
use pplx_space_execution_spec::{BlockVolumeSizeGiB, GuestBlockMountPath, GuestDataDrive};
use tonic::Status;

fn unsafe_device(error: impl std::fmt::Display) -> Status {
    Status::failed_precondition(format!(
        "retained block-device verification failed: {error}"
    ))
}

pub(super) fn validate_binding(binding: &BlockVolumeBinding) -> Result<(), Status> {
    if binding.topology_version != GuestDataDrive::TOPOLOGY_VERSION || binding.fence.is_none() {
        return Err(unsafe_device("unsupported or unfenced drive topology"));
    }
    uuid::Uuid::parse_str(&binding.filesystem_uuid).map_err(unsafe_device)?;
    binding.capacity().map_err(unsafe_device)?;
    GuestBlockMountPath::try_from(binding.mount.clone()).map_err(unsafe_device)?;
    serial(&binding.aws_volume_id)?;
    Ok(())
}

fn serial(volume: &str) -> Result<String, Status> {
    let id = volume
        .strip_prefix("vol-")
        .ok_or_else(|| unsafe_device("invalid EBS volume ID"))?;
    if id.len() != 17
        || !id
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(unsafe_device("invalid EBS volume ID"));
    }
    Ok(format!("vol{id}"))
}

/// Pin the whole EBS namespace, never a partition, pathname alias, instance
/// store or arbitrary caller device. Re-identify through the opened inode's
/// major/minor, rather than trusting a name observed before the open.
pub(super) fn pin(volume: &str, capacity: BlockVolumeSizeGiB) -> Result<File, Status> {
    let expected = serial(volume)?;
    let mut found = None;
    for entry in std::fs::read_dir("/sys/class/block").map_err(unsafe_device)? {
        let entry = entry.map_err(unsafe_device)?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if !name.starts_with("nvme") || !name.ends_with("n1") || name.contains('p') {
            continue;
        }
        let candidate = Path::new("/dev").join(name);
        let file = match open(
            &candidate,
            OFlag::O_RDONLY | OFlag::O_NONBLOCK | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
            Mode::empty(),
        ) {
            Ok(fd) => File::from(fd),
            Err(nix::errno::Errno::ENOENT | nix::errno::Errno::ENXIO) => continue,
            Err(error) => return Err(unsafe_device(error)),
        };
        let metadata = file.metadata().map_err(unsafe_device)?;
        if !metadata.file_type().is_block_device() {
            return Err(unsafe_device("namespace is not a block device"));
        }
        let sys = Path::new("/sys/dev/block").join(format!(
            "{}:{}",
            nix::sys::stat::major(metadata.rdev()),
            nix::sys::stat::minor(metadata.rdev())
        ));
        let observed = std::fs::read_to_string(sys.join("device/serial")).map_err(unsafe_device)?;
        if observed.trim() != expected {
            continue;
        }
        let model = std::fs::read_to_string(sys.join("device/model")).map_err(unsafe_device)?;
        let sectors: u64 = std::fs::read_to_string(sys.join("size"))
            .map_err(unsafe_device)?
            .trim()
            .parse()
            .map_err(unsafe_device)?;
        if model.trim() != "Amazon Elastic Block Store"
            || sectors.checked_mul(512) != Some(u64::from(capacity.get()) * 1024 * 1024 * 1024)
            || sys.join("partition").try_exists().map_err(unsafe_device)?
        {
            return Err(unsafe_device(
                "EBS model, whole-disk geometry or size mismatch",
            ));
        }
        if found.replace(file).is_some() {
            return Err(unsafe_device("ambiguous EBS serial"));
        }
    }
    found.ok_or_else(|| Status::unavailable("attached EBS namespace is not visible yet"))
}

pub(super) fn format_fresh(binding: &BlockVolumeBinding) -> Result<(), Status> {
    validate_binding(binding)?;
    if binding.initialization_ready
        || binding.initialization_claim.is_none()
        || binding.disk_generation != 0
    {
        return Err(unsafe_device(
            "format requires a fresh durable initialization claim",
        ));
    }
    let file = pin(
        &binding.aws_volume_id,
        binding.capacity().map_err(unsafe_device)?,
    )?;
    // The child opens the parent's retained descriptor, not a replaceable /dev
    // name. The parent holds this file and mutation ownership through reap.
    let pinned = format!("/proc/{}/fd/{}", std::process::id(), file.as_raw_fd());
    let output = crate::host_command::output(
        crate::host_command::Command::new("mkfs.ext4").args([
            "-U",
            &binding.filesystem_uuid,
            "-L",
            "space-data",
            "-E",
            "nodiscard,lazy_itable_init=0,lazy_journal_init=0",
            &pinned,
        ]),
        crate::host_command::MUTATION_BUDGET,
        &[],
    )
    .map_err(unsafe_device)?;
    if !output.status.success() {
        return Err(unsafe_device("fresh ext4 initialization did not complete"));
    }
    Ok(())
}

pub(super) fn bind(binding: &BlockVolumeBinding, jail: &str) -> Result<(), Status> {
    validate_binding(binding)?;
    let source = pin(
        &binding.aws_volume_id,
        binding.capacity().map_err(unsafe_device)?,
    )?;
    let root = super::super::trusted_paths::open_directory(Path::new(jail))?;
    let directory =
        super::super::trusted_paths::open_bind_directory(&root, "block-volumes".as_ref())?;
    let target = super::super::trusted_paths::open_bind_target(&directory, "data0".as_ref())?;
    super::super::trusted_paths::bind_pinned_files(&source, &target)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pplx_space_execution_common::proto::BlockVolumeFence;

    #[test]
    fn block_volume_device_identity_rejects_aliases_and_noncanonical_ids() {
        assert_eq!(
            serial("vol-0123456789abcdef0").unwrap(),
            "vol0123456789abcdef0"
        );
        for invalid in [
            "/dev/nvme1n1",
            "vol-0123456789abcdef0/../other",
            "vol-0123456789ABCDEF0",
            "vol-0123456789abcdef0\n",
            "vol-01234567",
        ] {
            assert_eq!(
                serial(invalid).unwrap_err().code(),
                tonic::Code::FailedPrecondition
            );
        }
    }

    #[test]
    fn block_volume_format_refuses_exposed_or_unclaimed_disks_before_host_io() {
        let fresh = BlockVolumeBinding {
            fence: Some(BlockVolumeFence::default()),
            aws_volume_id: "vol-0123456789abcdef0".into(),
            filesystem_uuid: uuid::Uuid::now_v7().to_string(),
            mount: "/data".into(),
            topology_version: GuestDataDrive::TOPOLOGY_VERSION,
            ..Default::default()
        };
        validate_binding(&fresh).unwrap();
        for refused in [
            fresh.clone(),
            BlockVolumeBinding {
                initialization_claim: Some(uuid::Uuid::now_v7().to_string()),
                initialization_ready: true,
                ..fresh.clone()
            },
            BlockVolumeBinding {
                initialization_claim: Some(uuid::Uuid::now_v7().to_string()),
                disk_generation: 1,
                ..fresh
            },
        ] {
            assert_eq!(
                format_fresh(&refused).unwrap_err().code(),
                tonic::Code::FailedPrecondition
            );
        }
    }
}
