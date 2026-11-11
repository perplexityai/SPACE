//! Restore an immutable encrypted COW under an independent sandbox key.
//!
//! Only kernel dm-crypt mappings see plaintext. The destination stays unnamed
//! until the complete sparse payload is flushed, and a retry never overwrites
//! a published child COW. Callers own the sandbox device/cleanup locks.

use super::{
    DmCryptError, ReleaseDevice, blockdev_sectors, dm_exists, luks_format_args, run, run_with_stdin,
};
use crate::device_backing::{self, Backing, LoopDevice};
use pplx_space_common_core::types::SandboxId;
use std::fs::File;
use std::os::unix::fs::{FileExt, MetadataExt};
use std::path::Path;

/// Both passphrases come from credential-manager after the same-org assertion.
/// Formatting the destination, rather than replacing a cloned LUKS keyslot,
/// gives the child a distinct volume key as well as a distinct passphrase.
pub(crate) async fn stage(
    sandbox_id: SandboxId,
    source_path: &Path,
    target_path: &Path,
    source_dek: &[u8],
    target_dek: &[u8],
) -> Result<(), DmCryptError> {
    let source_name = format!("sbxroot-{sandbox_id}-fork-source");
    let target_name = format!("sbxroot-{sandbox_id}-fork-target");
    cleanup(&source_name, &target_name).await?;
    let source_path = source_path.to_owned();
    let target_path = target_path.to_owned();
    let name = target_path
        .file_name()
        .ok_or_else(|| invalid("missing COW name"))?
        .to_owned();
    let backings = device_backing::blocking(move || {
        if Backing::open(&target_path, false)?.is_some() {
            return Ok(None);
        }
        let source = Backing::require(Backing::open(&source_path, false)?)?;
        let target = Backing::create_unpublished(&target_path)?;
        let metadata = source.file.metadata()?;
        let size = metadata.len();
        // Bound work even if a staged object has malformed lengths.
        if size
            > super::cow_backing_size_bytes(
                pplx_space_common_core::sandbox_storage::MAX_SANDBOX_ROOTFS_BYTES,
            )
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "fork COW exceeds maximum rootfs capacity",
            ));
        }
        let disk = nix::sys::statvfs::fstatvfs(&target.file)?;
        let free = u128::from(disk.blocks_available()) * u128::from(disk.fragment_size());
        let required =
            u128::from(metadata.blocks()) * 512 + u128::from(super::MIN_FREE_BYTES_FOR_SETUP);
        if free < required {
            return Err(std::io::Error::new(
                std::io::ErrorKind::StorageFull,
                "insufficient disk for encrypted COW re-encryption",
            ));
        }
        target.resize(size)?;
        Ok(Some((source, target)))
    })
    .await?;
    let Some((source, target)) = backings else {
        return Ok(());
    };
    let (source, source_loop, target, target_loop) = device_backing::blocking(move || {
        let source_loop = LoopDevice::attach(&source, true)?;
        let target_loop = LoopDevice::attach(&target, false)?;
        Ok((source, source_loop, target, target_loop))
    })
    .await?;

    let result = async {
        let source_device = source_loop.path().to_string_lossy();
        let target_device = target_loop.path().to_string_lossy();
        let source_layout = layout(&source_device).await?;
        run_with_stdin(
            "cryptsetup",
            &[
                "open",
                "--readonly",
                "--key-file=-",
                &source_device,
                &source_name,
            ],
            source_dek,
        )
        .await?;
        let sector_size = source_layout.sector_bytes.to_string();
        let mut format_args = luks_format_args(&target_device);
        format_args.extend_from_slice(&["--sector-size", &sector_size]);
        run_with_stdin("cryptsetup", &format_args, target_dek).await?;
        let target_layout = layout(&target_device).await?;
        if source_layout.offset != target_layout.offset
            || source_layout.sector_bytes != target_layout.sector_bytes
        {
            return Err(invalid("fork requires matching LUKS payload geometry"));
        }
        run_with_stdin(
            "cryptsetup",
            &["open", "--key-file=-", &target_device, &target_name],
            target_dek,
        )
        .await?;
        let source_mapper = format!("/dev/mapper/{source_name}");
        let target_mapper = format!("/dev/mapper/{target_name}");
        let source_sectors = blockdev_sectors(Path::new(&source_mapper)).await?;
        if source_sectors != blockdev_sectors(Path::new(&target_mapper)).await? {
            return Err(invalid("fork COW mapper sizes differ"));
        }
        let source_file = source.file.try_clone().map_err(device_backing::error)?;
        device_backing::blocking(move || {
            let input = File::open(source_mapper)?;
            let output = File::options().write(true).open(target_mapper)?;
            copy_payload(
                &source_file,
                &input,
                &output,
                source_layout,
                source_sectors * super::SECTOR_BYTES,
            )?;
            output.sync_all()
        })
        .await
    }
    .await;
    // Closing flushes crypto I/O before publication; both loop fds remain
    // pinned through cleanup, including failures and failed source unlocks.
    let closed = cleanup(&source_name, &target_name).await;
    if let (Err(error), Err(cleanup_error)) = (&result, &closed) {
        tracing::warn!(%sandbox_id, %error, %cleanup_error, "failed encrypted fork requires device cleanup retry");
    }
    result?;
    closed?;
    device_backing::blocking(move || target.publish(&name)).await
}

pub(super) async fn cleanup(source: &str, target: &str) -> Result<(), DmCryptError> {
    let mut first_error = None;
    for name in [target, source] {
        let result = async {
            if dm_exists(name).await? {
                ReleaseDevice::Crypt(name).run().await?;
            }
            Ok(())
        }
        .await;
        if let Err(error) = result {
            first_error.get_or_insert(error);
        }
    }
    first_error.map_or(Ok(()), Err)
}

#[derive(Debug, Clone, Copy)]
struct Layout {
    offset: u64,
    sector_bytes: u64,
}

async fn layout(device: &str) -> Result<Layout, DmCryptError> {
    let bytes = run("cryptsetup", &["luksDump", "--dump-json-metadata", device]).await?;
    Layout::try_from(bytes.as_slice())
        .map_err(|error| invalid(&format!("unsupported fork COW layout: {error}")))
}

#[derive(Debug, thiserror::Error)]
enum LayoutError {
    #[error("invalid LUKS metadata: {0}")]
    Json(#[from] serde_json::Error),
    #[error("requires one ordinary dynamic crypt segment with sector-aligned payload")]
    Unsupported,
}

impl TryFrom<&[u8]> for Layout {
    type Error = LayoutError;
    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        #[derive(serde::Deserialize)]
        struct Metadata {
            segments: std::collections::BTreeMap<String, Segment>,
        }
        #[derive(serde::Deserialize)]
        struct Segment {
            #[serde(rename = "type")]
            kind: SegmentKind,
            offset: String,
            size: SegmentSize,
            sector_size: u64,
            #[serde(default)]
            flags: Vec<String>,
        }
        #[derive(serde::Deserialize)]
        enum SegmentKind {
            #[serde(rename = "crypt")]
            Crypt,
        }
        #[derive(serde::Deserialize)]
        enum SegmentSize {
            #[serde(rename = "dynamic")]
            Dynamic,
        }
        let metadata: Metadata = serde_json::from_slice(bytes)?;
        if metadata.segments.len() != 1 {
            return Err(LayoutError::Unsupported);
        }
        let segment = metadata.segments.get("0").ok_or(LayoutError::Unsupported)?;
        let offset = segment
            .offset
            .parse::<u64>()
            .map_err(|_| LayoutError::Unsupported)?;
        let SegmentKind::Crypt = segment.kind;
        let SegmentSize::Dynamic = segment.size;
        if !segment.flags.is_empty()
            || !matches!(segment.sector_size, 512 | 4096)
            || offset == 0
            || offset % segment.sector_size != 0
        {
            return Err(LayoutError::Unsupported);
        }
        Ok(Self {
            offset,
            sector_bytes: segment.sector_size,
        })
    }
}

fn invalid(message: &str) -> DmCryptError {
    DmCryptError::InvalidParams(message.into())
}

/// Ciphertext holes do not denote plaintext zeros: they denote unused COW
/// space. Preserve them, copying only allocated ciphertext sectors through
/// the mappings. Copying the full logical mapper would materialize every hole.
fn copy_payload(
    backing: &File,
    input: &File,
    output: &File,
    layout: Layout,
    length: u64,
) -> std::io::Result<()> {
    use rustix::fs::{SeekFrom, seek};
    let end = layout
        .offset
        .checked_add(length)
        .ok_or(std::io::ErrorKind::InvalidData)?;
    if end > backing.metadata()?.len() {
        return Err(std::io::ErrorKind::UnexpectedEof.into());
    }
    let mut at = layout.offset;
    let deadline = std::time::Instant::now() + crate::host_command::MUTATION_BUDGET;
    let mut buffer = zeroize::Zeroizing::new(vec![0; 1024 * 1024]);
    while at < end {
        let data = match seek(backing, SeekFrom::Data(at)) {
            Ok(data) => data,
            Err(rustix::io::Errno::NXIO) => break,
            Err(error) => return Err(error.into()),
        };
        if data >= end {
            break;
        }
        let hole = seek(backing, SeekFrom::Hole(data))?.min(end);
        let mut position = data.max(layout.offset) / layout.sector_bytes * layout.sector_bytes;
        let limit = hole.div_ceil(layout.sector_bytes) * layout.sector_bytes;
        if hole <= data || limit > end {
            return Err(std::io::ErrorKind::InvalidData.into());
        }
        while position < limit {
            if std::time::Instant::now() >= deadline {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "encrypted COW re-encryption exceeded its copy budget",
                ));
            }
            let count = (limit - position).min(buffer.len() as u64) as usize;
            let logical = position - layout.offset;
            let bytes = buffer
                .get_mut(..count)
                .ok_or(std::io::ErrorKind::InvalidInput)?;
            input.read_exact_at(bytes, logical)?;
            output.write_all_at(bytes, logical)?;
            position += count as u64;
        }
        at = hole;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
