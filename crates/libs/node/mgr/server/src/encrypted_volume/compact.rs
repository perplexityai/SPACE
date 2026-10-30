//! Compaction of an encrypted suspend volume that has grown far past its
//! contents.
//!
//! A restored BYOK sandbox's Firecracker writes its next Diff snapshot into
//! the memory file it was loaded from, so each re-suspend rewrites that file in
//! place. On btrfs with `compress-force`, every rewrite lands in new extents,
//! and a compressed extent stays allocated until all of it has been
//! overwritten, so the filesystem's used bytes grow on every cycle. The sparse
//! `.luks` backing grows with them and never gives a block back, because
//! nothing passes discards through dm-crypt. The memory file also fragments
//! across the volume, which makes each later restore slower to fault the
//! guest's memory back in.
//!
//! The volume records its used bytes right after it was last freshly written
//! in a small file at its root ([`BASELINE_FILE`]), so the record travels with
//! the published artifact. It only schedules compaction: a guest that rewrites
//! it can make its own suspends compact more or less often, nothing else. Once
//! the used bytes reach [`GROWTH_RATIO`] times the baseline,
//! [`build_compacted_copy`] copies the volume's files into a freshly
//! formatted sibling volume under the same key and seals it, and the caller
//! publishes the sibling's backing instead of the volume's. The files keep
//! their bytes, so the next restore loads the same memory base; the live
//! volume is left as it is.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use pplx_space_common_core::types::SandboxId;

use super::{
    IdempotentArm, PrepareParams, VOLUME_HEADROOM_BYTES, device_backing, idempotent_arm,
    is_mounted, mapper_identity, mapper_name, mount_device_numbers_of, prepare, release,
};
use crate::rootfs_dmcrypt::{DmCryptError, run};

/// Compact once the filesystem uses this many times its baseline. Below it,
/// the copy would cost more than the restore saves.
const GROWTH_RATIO: u64 = 2;

/// And only when compacting frees at least this much, so a small volume
/// isn't recopied for a few megabytes.
const MIN_RECLAIM_BYTES: u64 = 256 * 1024 * 1024;

/// Upper bound on the copy, which runs inside the suspend's staging. A copy
/// that would take longer is abandoned and the volume is published as it is.
const COPY_BUDGET: Duration = Duration::from_secs(45);

const COPY_CHUNK_BYTES: usize = 1024 * 1024;

/// The file at a volume's root that records its baseline in decimal bytes.
pub const BASELINE_FILE: &str = ".compaction-baseline";

/// The used bytes a volume records as its baseline, if it records one that
/// parses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BaselineState {
    Bytes(u64),
    Absent,
}

/// What a suspend's staging should do with its volume.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    /// Publish the volume as it is.
    Keep,
    /// Record the current used bytes as the baseline, then publish the volume.
    RecordBaseline,
    /// Record the current used bytes as the baseline, so a failed or retried
    /// compaction is not attempted again until the volume doubles once more,
    /// then publish a compacted copy.
    Compact,
}

/// Decide for a volume using `used` bytes. `fresh` is a volume formatted for
/// this capture; a volume with no baseline that is not fresh has an unknown
/// history and is compacted once if it is large enough, which also gives it a
/// true baseline.
pub fn decide(used: u64, baseline: BaselineState, fresh: bool) -> Decision {
    match baseline {
        _ if fresh => Decision::RecordBaseline,
        BaselineState::Absent if used >= MIN_RECLAIM_BYTES => Decision::Compact,
        BaselineState::Absent => Decision::RecordBaseline,
        BaselineState::Bytes(baseline)
            if used >= baseline.saturating_mul(GROWTH_RATIO)
                && used.saturating_sub(baseline) >= MIN_RECLAIM_BYTES =>
        {
            Decision::Compact
        }
        BaselineState::Bytes(_) => Decision::Keep,
    }
}

/// The volume's recorded baseline. A missing, unreadable, oversized or
/// malformed record is absent; a symlink in its place is refused.
pub async fn read_baseline(mount_dir: &Path) -> Result<BaselineState, DmCryptError> {
    let path = mount_dir.join(BASELINE_FILE);
    device_backing::blocking(move || {
        use std::io::Read;
        use std::os::unix::fs::OpenOptionsExt;
        let file = match std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(nix::libc::O_NOFOLLOW | nix::libc::O_NONBLOCK)
            .open(&path)
        {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(BaselineState::Absent);
            }
            Err(error) => return Err(error),
        };
        if !file.metadata()?.file_type().is_file() {
            return Ok(BaselineState::Absent);
        }
        let mut contents = String::new();
        file.take(32).read_to_string(&mut contents)?;
        Ok(contents
            .trim()
            .parse()
            .map_or(BaselineState::Absent, BaselineState::Bytes))
    })
    .await
}

/// Record `used` as the baseline of the writable volume at `mount_dir`.
pub async fn record_baseline(mount_dir: &Path, used: u64) -> Result<(), DmCryptError> {
    let path = mount_dir.join(BASELINE_FILE);
    device_backing::blocking(move || {
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .custom_flags(nix::libc::O_NOFOLLOW)
            .open(&path)?;
        if !file.metadata()?.file_type().is_file() {
            return Err(std::io::Error::other(
                "baseline record is not a regular file",
            ));
        }
        file.write_all(used.to_string().as_bytes())?;
        file.sync_all()
    })
    .await
}

/// Whether the volume's own filesystem is positively mounted at `mount_dir`.
pub async fn is_volume_mounted(
    sandbox_id: &SandboxId,
    volume_name: &str,
    mount_dir: &Path,
) -> Result<bool, DmCryptError> {
    if !is_mounted(mount_dir).await? {
        return Ok(false);
    }
    let identity = mapper_identity(&mapper_name(sandbox_id, volume_name)).await;
    Ok(
        idempotent_arm(&identity, mount_device_numbers_of(mount_dir).await?)
            == IdempotentArm::Accept,
    )
}

/// Flush the filesystem at `mount_dir` and read its used bytes.
pub async fn used_bytes(mount_dir: &Path) -> Result<u64, DmCryptError> {
    let point = mount_dir.to_string_lossy();
    run("sync", &["-f", point.as_ref()]).await?;
    let mount = mount_dir.to_owned();
    device_backing::blocking(move || {
        let fs = nix::sys::statvfs::statvfs(&mount)?;
        let used = u128::from(fs.blocks()).saturating_sub(u128::from(fs.blocks_free()))
            * u128::from(fs.fragment_size());
        Ok(u64::try_from(used).unwrap_or(u64::MAX))
    })
    .await
}

/// A sealed compacted copy of a volume, ready to publish in its place.
#[derive(Debug)]
pub struct CompactedCopy {
    /// The sealed sibling backing. The caller publishes it and then removes
    /// it with [`discard`].
    pub backing_path: PathBuf,
    /// The copy's used bytes, which it records as its baseline.
    pub used: u64,
}

/// Inputs for [`build_compacted_copy`].
pub struct CompactParams<'a> {
    pub sandbox_id: SandboxId,
    pub volume_name: &'a str,
    pub backing_path: &'a Path,
    /// The volume's mount, held read-only by the caller for the copy.
    pub mount_dir: &'a Path,
    /// The volume's DEK; the copy is formatted under the same key.
    pub dek: &'a [u8],
}

/// The sibling volume a compaction builds next to `volume_name`.
struct Sibling {
    volume_name: String,
    backing_path: PathBuf,
    mount_dir: PathBuf,
}

fn sibling(volume_name: &str, backing_path: &Path, mount_dir: &Path) -> Sibling {
    let volume_name = format!("{volume_name}-compact");
    Sibling {
        backing_path: backing_path.with_file_name(format!("{volume_name}.luks")),
        mount_dir: mount_dir.with_file_name(format!("{volume_name}-vol")),
        volume_name,
    }
}

/// Copy the volume's files into a freshly formatted sibling under the same
/// key, record the copy's baseline in it, and seal it. Any failure discards
/// the sibling.
pub async fn build_compacted_copy(
    params: CompactParams<'_>,
) -> Result<CompactedCopy, DmCryptError> {
    let CompactParams {
        sandbox_id,
        volume_name,
        backing_path,
        mount_dir,
        dek,
    } = params;
    let sibling = sibling(volume_name, backing_path, mount_dir);
    match fill_sibling(&sandbox_id, &sibling, backing_path, mount_dir, dek).await {
        Ok(used) => Ok(CompactedCopy {
            backing_path: sibling.backing_path,
            used,
        }),
        Err(error) => {
            discard_sibling(&sandbox_id, &sibling).await;
            Err(error)
        }
    }
}

async fn fill_sibling(
    sandbox_id: &SandboxId,
    sibling: &Sibling,
    backing_path: &Path,
    mount_dir: &Path,
    dek: &[u8],
) -> Result<u64, DmCryptError> {
    // Same logical size as the original: its backing was sized from it.
    let length = {
        let backing = backing_path.to_owned();
        device_backing::blocking(move || Ok(std::fs::metadata(&backing)?.len())).await?
    };
    prepare(PrepareParams {
        sandbox_id: *sandbox_id,
        volume_name: sibling.volume_name.clone(),
        backing_path: sibling.backing_path.clone(),
        restore_source: None,
        mount_dir: sibling.mount_dir.clone(),
        size_bytes: length.saturating_sub(VOLUME_HEADROOM_BYTES).max(1),
        dek,
        fresh: true,
    })
    .await?;
    let source = mount_dir.to_owned();
    let target = sibling.mount_dir.clone();
    let deadline = Instant::now() + COPY_BUDGET;
    device_backing::blocking(move || copy_contents(&source, &target, deadline)).await?;
    let used = used_bytes(&sibling.mount_dir).await?;
    record_baseline(&sibling.mount_dir, used).await?;
    release(
        sandbox_id,
        &sibling.volume_name,
        &sibling.backing_path,
        &sibling.mount_dir,
    )
    .await?;
    Ok(used)
}

/// Remove a published or abandoned compacted copy. Best-effort.
pub async fn discard(
    sandbox_id: &SandboxId,
    volume_name: &str,
    backing_path: &Path,
    mount_dir: &Path,
) {
    discard_sibling(sandbox_id, &sibling(volume_name, backing_path, mount_dir)).await;
}

/// Best-effort: seal and delete a sibling that won't be used further.
async fn discard_sibling(sandbox_id: &SandboxId, sibling: &Sibling) {
    if let Err(error) = release(
        sandbox_id,
        &sibling.volume_name,
        &sibling.backing_path,
        &sibling.mount_dir,
    )
    .await
    {
        tracing::warn!(%sandbox_id, %error, "failed to release a compacted volume copy");
    }
    let path = sibling.backing_path.clone();
    if let Err(error) = device_backing::blocking(move || match std::fs::remove_file(&path) {
        Err(error) if error.kind() != std::io::ErrorKind::NotFound => Err(error),
        _ => Ok(()),
    })
    .await
    {
        tracing::warn!(%sandbox_id, %error, "failed to remove a compacted volume copy");
    }
}

/// Bytes allocated to a backing file on the node's disk.
pub async fn allocated_bytes(path: &Path) -> Result<u64, DmCryptError> {
    let path = path.to_owned();
    device_backing::blocking(move || {
        use std::os::unix::fs::MetadataExt;
        Ok(std::fs::metadata(&path)?.blocks() * 512)
    })
    .await
}

/// Copy every regular file directly under `source` into the empty `target`,
/// keeping holes, length, mode and ownership, and mirror the directory's own
/// mode and ownership. Any other kind of entry fails the copy: the volume
/// holds only what Firecracker wrote.
pub(super) fn copy_contents(
    source: &Path,
    target: &Path,
    deadline: Instant,
) -> std::io::Result<()> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};

    for entry in std::fs::read_dir(source)? {
        let entry = entry?;
        let name = entry.file_name();
        let from = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(nix::libc::O_NOFOLLOW | nix::libc::O_NONBLOCK)
            .open(entry.path())?;
        let metadata = from.metadata()?;
        if !metadata.file_type().is_file() {
            return Err(std::io::Error::other(format!(
                "unexpected entry {name:?} in encrypted volume"
            )));
        }
        let to = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(nix::libc::O_NOFOLLOW)
            .open(target.join(&name))?;
        copy_sparse(&from, &to, metadata.len(), deadline)?;
        std::os::unix::fs::fchown(&to, Some(metadata.uid()), Some(metadata.gid()))?;
        to.set_permissions(std::fs::Permissions::from_mode(metadata.mode() & 0o7777))?;
        to.sync_all()?;
    }
    let directory = std::fs::metadata(source)?;
    std::os::unix::fs::chown(target, Some(directory.uid()), Some(directory.gid()))?;
    std::fs::set_permissions(
        target,
        std::fs::Permissions::from_mode(directory.mode() & 0o7777),
    )?;
    std::fs::File::open(target)?.sync_all()
}

/// Copy `length` bytes of `from` into the empty `to`, writing only the ranges
/// that hold data so holes stay holes.
fn copy_sparse(
    from: &std::fs::File,
    to: &std::fs::File,
    length: u64,
    deadline: Instant,
) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;

    let mut buffer = vec![0; COPY_CHUNK_BYTES];
    for (start, end) in data_ranges(from, length)? {
        let mut offset = start;
        while offset < end {
            if Instant::now() >= deadline {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "encrypted volume compaction exceeded its copy budget",
                ));
            }
            let want = (end - offset).min(COPY_CHUNK_BYTES as u64) as usize;
            let chunk = buffer
                .get_mut(..want)
                .ok_or_else(|| std::io::Error::other("copy chunk exceeds its buffer"))?;
            let read = from.read_at(chunk, offset)?;
            let data = chunk
                .get(..read)
                .filter(|data| !data.is_empty())
                .ok_or(std::io::ErrorKind::UnexpectedEof)?;
            to.write_all_at(data, offset)?;
            offset += read as u64;
        }
    }
    to.set_len(length)
}

/// The `[start, end)` ranges of `file` that hold data.
#[cfg(target_os = "linux")]
fn data_ranges(file: &std::fs::File, length: u64) -> std::io::Result<Vec<(u64, u64)>> {
    use nix::unistd::{Whence, lseek};

    let seek = |offset: u64, whence| -> std::io::Result<Option<u64>> {
        let offset = i64::try_from(offset).map_err(std::io::Error::other)?;
        match lseek(file, offset, whence) {
            Ok(found) => Ok(Some(u64::try_from(found).map_err(std::io::Error::other)?)),
            Err(nix::errno::Errno::ENXIO) => Ok(None),
            Err(error) => Err(error.into()),
        }
    };
    let mut ranges = Vec::new();
    let mut offset = 0;
    while offset < length {
        let Some(start) = seek(offset, Whence::SeekData)? else {
            break;
        };
        let end = seek(start, Whence::SeekHole)?.unwrap_or(length).min(length);
        if end <= start {
            break;
        }
        ranges.push((start, end));
        offset = end;
    }
    Ok(ranges)
}

#[cfg(not(target_os = "linux"))]
fn data_ranges(_file: &std::fs::File, length: u64) -> std::io::Result<Vec<(u64, u64)>> {
    Ok(vec![(0, length)])
}

#[cfg(test)]
mod tests;
