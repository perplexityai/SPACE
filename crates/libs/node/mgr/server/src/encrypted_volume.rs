//! Per-sandbox encrypted data volume: btrfs(zstd) over dm-crypt.
//!
//! A general-purpose encrypted, compressed scratch volume for a sandbox. A
//! caller writes ordinary files into the returned directory; this module makes
//! that directory a btrfs filesystem mounted on a dm-crypt device, so every
//! byte written lands **compressed then encrypted** on the node disk from the
//! first write — never plaintext, not even transiently. Each volume has a
//! `volume_name` (unique per sandbox), so a sandbox can hold several.
//!
//! Today's only consumer is the suspend snapshot capture
//! (`volume_name="snapshot"`), into which Firecracker writes a suspend's VM
//! state + memory dump; the volume is equally usable for other BYOK data.
//!
//! # Layering — compression ABOVE encryption (enforced by the kernel stack)
//!
//! ```text
//!   Firecracker write (vmstate / memory)
//!          │
//!          ▼  btrfs mount with compress-force=zstd      ← compresses the
//!          │                                              plaintext here
//!          ▼  /dev/mapper/<id>-snap  (dm-crypt, AES)     ← encrypts the
//!          │                                              already-compressed
//!          ▼  snapshot.luks backing file                   bytes
//!          (compressed-then-encrypted on disk)
//! ```
//!
//! Because the filesystem sits *above* the block device, a write flows down
//! through zstd first and dm-crypt second — compression always precedes
//! encryption, which is the only correct order (you cannot compress
//! ciphertext). `compress-force` (not the default `compress`) disables btrfs's
//! per-extent compressibility heuristic so the guarantee holds even for spans
//! the heuristic would misjudge.
//!
//! # Key reuse and lifecycle
//!
//! The DEK is the sandbox's existing volume DEK — the same key the live rootfs
//! COW uses, persisted once in `sandbox_volume_encryption` and unwrapped on
//! demand here. No separate key issuance. The volume is a **transient capture
//! buffer**: the durable artifact is the S3 snapshot (with its own wrap in
//! `snapshot_storage_location_encrypted_fields`), so this volume is always
//! freshly formatted on prepare and torn down after the upload (or, on the
//! restore side, after the artifacts have been consumed).

use std::path::{Path, PathBuf};

use pplx_space_common_core::types::SandboxId;

use crate::rootfs_dmcrypt::{
    DmCryptError, ReleaseDevice, crypt_mapper_numbers, luks_format_args, parse_device_numbers, run,
    run_with_stdin, verify_kernel_support,
};

use crate::device_backing::{self, Backing, LoopDevice};

#[cfg(any(target_os = "linux", test))]
mod mount_identity;

/// btrfs needs a floor of real space for its metadata even when the backing
/// file is sparse; 256 MiB clears the historical ~109 MiB minimum with room
/// for the LUKS2 header and metadata.
const MIN_VOLUME_BYTES: u64 = 256 * 1024 * 1024;

/// Headroom over the requested logical size, for the LUKS2 header + btrfs
/// metadata. Sparse, so generous costs nothing until written.
const VOLUME_HEADROOM_BYTES: u64 = 64 * 1024 * 1024;

/// How long [`release`] waits, after the lazy unmount, for the kernel to drop
/// the detached btrfs's hold on the mapping before closing it. That hold is
/// usually gone within milliseconds, but a transient reference outside
/// node-manager can keep it for several seconds, past `cryptsetup close`'s own
/// remove retries. Bounded so a holder that never lets go still reaches the
/// close and its existing failure path instead of pinning the sandbox's device
/// lock and a lifecycle permit.
const MAPPER_IDLE_BUDGET: std::time::Duration = std::time::Duration::from_secs(10);
const MAPPER_IDLE_POLL: std::time::Duration = std::time::Duration::from_millis(100);

/// A mounted encrypted volume. `mount_dir` is where the caller reads/writes the
/// (transparently compressed + encrypted) files.
#[derive(Debug, Clone)]
pub struct PreparedEncryptedVolume {
    /// Directory the btrfs filesystem is mounted at.
    pub mount_dir: PathBuf,
}

/// dm name of an encrypted volume's dm-crypt device. Includes the per-sandbox
/// `volume_name` so a sandbox can hold several volumes without collision. The
/// `sbxvol-` prefix keeps it greppable in `dmsetup ls` for per-sandbox cleanup.
fn mapper_name(sandbox_id: &SandboxId, volume_name: &str) -> String {
    format!("sbxvol-{sandbox_id}-{volume_name}")
}

/// Sparse backing size for the volume: the requested logical size
/// (caller derives it from VM memory + state + slack) plus header/metadata
/// headroom, floored at the btrfs minimum. Pure; unit-tested.
fn volume_backing_size_bytes(requested_bytes: u64) -> u64 {
    (requested_bytes + VOLUME_HEADROOM_BYTES).max(MIN_VOLUME_BYTES)
}

async fn read_mountinfo() -> Result<String, DmCryptError> {
    tokio::fs::read_to_string("/proc/self/mountinfo")
        .await
        .map_err(|source| DmCryptError::Io {
            cmd: "read /proc/self/mountinfo".to_string(),
            source,
        })
}

fn mountpoint_is_present(mountinfo: &str, dir: &Path) -> bool {
    let dir = dir.to_string_lossy();
    mountinfo.lines().any(|line| {
        line.split(" - ")
            .next()
            .and_then(|head| head.split_whitespace().nth(4))
            == Some(dir.as_ref())
    })
}

/// Is `dir` currently a mount point? Probe failures are unknown, never absent.
async fn is_mounted(dir: &Path) -> Result<bool, DmCryptError> {
    Ok(mountpoint_is_present(&read_mountinfo().await?, dir))
}

/// Detach every mount stacked at `dir`, re-reading mountinfo after each
/// detach so a shadowed older mount cannot survive underneath the visible one.
async fn unmount_all_at_path(dir: &Path) -> Result<(), DmCryptError> {
    loop {
        let info = read_mountinfo().await?;
        if !mountpoint_is_present(&info, dir) {
            return Ok(());
        }
        ReleaseDevice::DetachMount(&dir.to_string_lossy())
            .run()
            .await?;
    }
}

/// Mountpoints whose mount source is `device`, from a mountinfo dump,
/// latest-mounted first so unmounting in order unwinds binds before the
/// origin mount.
/// Matching is by device identity — mountinfo's `major:minor` field — because
/// the recorded source string may be spelled `/dev/dm-N`; the `/dev/mapper`
/// path match remains as a fallback when the numbers could not be resolved.
pub(crate) fn mountpoints_of_device(
    mountinfo: &str,
    device: &str,
    majmin: Option<(u32, u32)>,
) -> Vec<String> {
    // btrfs uses an anonymous superblock device. Preserve the kernel dm-N
    // source alias even when there is no udev-created node to stat.
    let dm_source = majmin
        .filter(|_| device.starts_with("/dev/mapper/"))
        .map(|(_, minor)| format!("/dev/dm-{minor}"));
    let majmin = majmin.map(|(major, minor)| format!("{major}:{minor}"));
    let mut points: Vec<String> = mountinfo
        .lines()
        .filter_map(|line| {
            let (head, tail) = line.split_once(" - ")?;
            let mut fields = head.split_whitespace();
            let numbers = fields.nth(2)?;
            let mount_point = fields.nth(1)?;
            let source = tail.split_whitespace().nth(1)?;
            (majmin.as_deref() == Some(numbers)
                || dm_source.as_deref() == Some(source)
                || source == device)
                .then(|| mount_point.to_string())
        })
        .collect();
    points.reverse();
    points
}

/// major:minor of the visible mount at `dir` (mountinfo field 3).
fn mount_device_numbers(mountinfo: &str, dir: &str) -> Option<(u32, u32)> {
    mountinfo
        .lines()
        .filter_map(|line| {
            let head = line.split(" - ").next()?;
            let mut fields = head.split_whitespace();
            let numbers = fields.nth(2)?;
            let mount_point = fields.nth(1)?;
            (mount_point == dir).then(|| parse_device_numbers(numbers))?
        })
        .next_back()
}

/// Resolved identity of a dm mapper name. `Absent` (the name has no device —
/// e.g. `force_free_mapper` renamed it away) is deliberately distinct from
/// `Error` (the probe itself failed): a surviving mount over an absent mapper
/// is stale and must be rebuilt, whereas an unresolved probe must never
/// trigger destruction.
enum MapperIdentity {
    Present((u32, u32)),
    Absent,
    Error(DmCryptError),
}

/// Classify a `crypt_mapper_numbers` probe, distinguishing "does not exist"
/// (absent) from any other failure (error). Pure for testing.
fn classify_mapper_probe(probe: Result<(u32, u32), DmCryptError>) -> MapperIdentity {
    match probe {
        Ok(numbers) => MapperIdentity::Present(numbers),
        Err(DmCryptError::Command { stderr, .. })
            if stderr.to_lowercase().contains("does not exist") =>
        {
            MapperIdentity::Absent
        }
        Err(error) => MapperIdentity::Error(error),
    }
}

async fn mapper_identity(mapper: &str) -> MapperIdentity {
    classify_mapper_probe(crypt_mapper_numbers(mapper).await)
}

/// Resolve the backing block device, not btrfs's anonymous superblock ID.
/// Missing or ambiguous evidence must stop preparation before backing replacement.
async fn mount_device_numbers_of(mount_dir: &Path) -> Result<Option<(u32, u32)>, DmCryptError> {
    let info = read_mountinfo().await?;
    let numbers = mount_device_numbers(&info, &mount_dir.to_string_lossy());
    let Some(_numbers @ (0, _)) = numbers else {
        return Ok(numbers);
    };
    #[cfg(target_os = "linux")]
    {
        let expected_mount_id = info
            .lines()
            .filter_map(|line| {
                let mut fields = line.split_whitespace();
                let id = fields.next()?.parse::<u64>().ok()?;
                (fields.nth(3)? == mount_dir.to_string_lossy()).then_some(id)
            })
            .next_back()
            .ok_or_else(|| DmCryptError::Io {
                cmd: "read mounted btrfs identity".into(),
                source: std::io::Error::other("mount ID is missing"),
            })?;
        let mount_dir = mount_dir.to_owned();
        tokio::task::spawn_blocking(move || {
            mount_identity::btrfs_device(&mount_dir, expected_mount_id)
        })
        .await
        .map_err(|error| DmCryptError::Io {
            cmd: "resolve mounted btrfs device".into(),
            source: std::io::Error::other(error),
        })?
        .map(Some)
        .map_err(|source| DmCryptError::Io {
            cmd: "resolve mounted btrfs device".into(),
            source,
        })
    }
    #[cfg(not(target_os = "linux"))]
    Err(DmCryptError::Io {
        cmd: format!(
            "resolve mounted btrfs device for {}:{}",
            _numbers.0, _numbers.1
        ),
        source: std::io::Error::from(std::io::ErrorKind::Unsupported),
    })
}

/// Outcome of the idempotent re-entry check. `Accept` requires POSITIVE proof
/// the mount is present and on this mapper; anything unknown is `Retry`
/// (never accept an unverified mount — Firecracker would write plaintext into
/// an unmounted dir — and never destroy on an unproven probe).
#[derive(Debug, PartialEq, Eq)]
enum IdempotentArm {
    Accept,
    Rebuild,
    Retry,
}

fn idempotent_arm(identity: &MapperIdentity, mount_num: Option<(u32, u32)>) -> IdempotentArm {
    match (identity, mount_num) {
        // Anonymous filesystem numbers carry no block-device ownership proof.
        (_, Some((0, _))) => IdempotentArm::Retry,
        // Positively ours and still mounted → hand it back.
        (MapperIdentity::Present(mapper_num), Some(mount_num)) if *mapper_num == mount_num => {
            IdempotentArm::Accept
        }
        // Positively on a different device → stale, rebuild.
        (MapperIdentity::Present(_), Some(_)) => IdempotentArm::Rebuild,
        // Was mounted at the guard check but its device vanished/unresolved by
        // now (TOCTOU): unknown, never accept an unverified mount.
        (MapperIdentity::Present(_), None) => IdempotentArm::Retry,
        // The mapper name is gone (NotFound): whatever is here is stale.
        (MapperIdentity::Absent, _) => IdempotentArm::Rebuild,
        // Probe error: unknown, retry.
        (MapperIdentity::Error(_), _) => IdempotentArm::Retry,
    }
}

fn mountpoints_for_mapper_identity(
    mountinfo: &str,
    mapper: &str,
    identity: MapperIdentity,
) -> Result<Vec<String>, DmCryptError> {
    let majmin = match identity {
        MapperIdentity::Present(numbers) => Some(numbers),
        // An absent name can still have a mountinfo entry spelled with the old
        // mapper path; retain that non-destructive cleanup fallback.
        MapperIdentity::Absent => None,
        MapperIdentity::Error(error) => return Err(error),
    };
    Ok(mountpoints_of_device(
        mountinfo,
        &format!("/dev/mapper/{mapper}"),
        majmin,
    ))
}

/// The mapping's backing loop device, from `dmsetup deps -o devname` output
/// like `1 dependencies : (loop2)`.
fn loop_dep(deps: &str) -> Option<String> {
    let name = deps.split('(').nth(1)?.split(')').next()?;
    (!name.is_empty()).then(|| format!("/dev/{name}"))
}

async fn mapper_mountpoints(mapper: &str) -> Result<Vec<String>, DmCryptError> {
    // Fail closed: without either the mount table or a trustworthy identity
    // for a present mapper, `/dev/dm-N` mounts cannot be enumerated safely.
    let info = read_mountinfo().await?;
    mountpoints_for_mapper_identity(&info, mapper, mapper_identity(mapper).await)
}

/// Lazily unmount every mountpoint of the mapper device (the canonical dir
/// AND any jail bind — enumerated, never hardcoded). `-l` detaches even under
/// an open fd (a cancelled capture's Firecracker); the device reference then
/// drains when the holder exits.
async fn unmount_mapper_mounts(mapper: &str) -> Result<(), DmCryptError> {
    let points = mapper_mountpoints(mapper).await?;
    for point in points {
        ReleaseDevice::DetachMount(&point).run().await?;
    }
    Ok(())
}

/// The mapping's device-mapper open count.
async fn mapper_open_count(mapper: &str) -> Result<u64, DmCryptError> {
    let out = run(
        "dmsetup",
        &["info", "-c", "--noheadings", "-o", "open", mapper],
    )
    .await?;
    let out = String::from_utf8_lossy(&out);
    out.trim().parse().map_err(|_| DmCryptError::Command {
        cmd: format!("dmsetup info -c --noheadings -o open {mapper}"),
        stderr: format!("unparseable open count: {:?}", out.trim()),
    })
}

/// Poll `open_count` until it reports zero, `budget` elapses, or the probe
/// fails. Returns whether the device was observed idle. Never an error: the
/// close that follows reports the device's real state either way.
async fn wait_until_unopened<F, Fut>(
    mapper: &str,
    mut open_count: F,
    budget: std::time::Duration,
    poll: std::time::Duration,
) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<u64, DmCryptError>>,
{
    let started = tokio::time::Instant::now();
    loop {
        match open_count().await {
            Ok(0) => return true,
            Ok(open) if started.elapsed() >= budget => {
                tracing::warn!(
                    mapper,
                    open,
                    ?budget,
                    "encrypted-volume mapping still open after unmount"
                );
                return false;
            }
            Ok(_) => tokio::time::sleep(poll).await,
            Err(error) => {
                tracing::warn!(mapper, %error, "encrypted-volume open-count probe failed");
                return false;
            }
        }
    }
}

/// Positively confirm `dir` is no longer a mountpoint (absent from mountinfo).
/// An unreadable mountinfo is unknown, not confirmation — returns Err.
async fn confirm_not_mounted(dir: &Path) -> Result<(), DmCryptError> {
    let info = read_mountinfo().await?;
    if mountpoint_is_present(&info, dir) {
        return Err(DmCryptError::Command {
            cmd: format!("confirm unmounted {}", dir.display()),
            stderr: "mountpoint still present after unmount".to_string(),
        });
    }
    Ok(())
}

/// Free a mapping whose regular close is refused by an open holder: rename it
/// out of the way (the name is reusable immediately), arm a deferred remove,
/// and arm loop autoclear. `--noudevsync` because state-changing dmsetup ops
/// hang on udev cookies on these hosts.
async fn force_free_mapper(mapper: &str) {
    let loop_dev = match run("dmsetup", &["deps", "-o", "devname", mapper]).await {
        Ok(out) => loop_dep(&String::from_utf8_lossy(&out)),
        Err(_) => None,
    };
    let leak = format!(
        "{mapper}-leak-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |t| t.as_secs())
    );
    for args in [
        ["rename", "--noudevsync", mapper, &leak],
        ["remove", "--deferred", "--noudevsync", &leak],
    ] {
        if let Err(error) = run("dmsetup", &args).await {
            tracing::warn!(mapper, %error, "encrypted-volume force-free step failed");
        }
    }
    if let Some(loop_dev) = loop_dev {
        let _ = ReleaseDevice::Loop(&loop_dev).run().await;
    }
}

/// A mapping already holds this volume's name: if it is debris (its backing
/// file deleted or replaced — a deadline-cancelled capture leaves exactly
/// this), unwind it in stack order and report `true` so the open can retry.
/// Never unwinding a live stack rests on node-manager's canonical per-sandbox
/// device-stack lock: nothing else can be mid-build under this name while this
/// runs. `None` means the pinned canonical leaf was confirmed absent, never
/// that a backing probe failed. `false` requires a positively matching inode.
async fn reconcile_stale_mapper(
    mapper: &str,
    backing: Option<&std::fs::File>,
) -> Result<bool, DmCryptError> {
    let deps = run("dmsetup", &["deps", "-o", "devname", mapper]).await?;
    let loop_dev =
        loop_dep(&String::from_utf8_lossy(&deps)).ok_or_else(|| DmCryptError::Command {
            cmd: format!("dmsetup deps -o devname {mapper}"),
            stderr: "mapper has no identifiable backing loop device".into(),
        })?;
    if let Some(backing) = backing {
        let backing = backing.try_clone().map_err(device_backing::error)?;
        let device = PathBuf::from(loop_dev);
        if device_backing::blocking(move || device_backing::matches_loop(&device, &backing)).await?
        {
            return Ok(false);
        }
    }
    tracing::warn!(mapper, "reconciling stale encrypted-volume mapping");
    // A renamed mapper makes any surviving `/dev/dm-N` mounts undiscoverable
    // by this name. Do not close, force-free, or retry until every mountpoint
    // was successfully detached.
    unmount_mapper_mounts(mapper).await?;
    if ReleaseDevice::Crypt(mapper).run().await.is_err() {
        force_free_mapper(mapper).await;
    }
    Ok(true)
}

/// Inputs for [`prepare`].
pub struct PrepareParams<'a> {
    pub sandbox_id: SandboxId,
    /// Logical volume name, unique per sandbox (e.g. "snapshot").
    pub volume_name: String,
    /// Encrypted (LUKS2) backing file for the volume.
    pub backing_path: PathBuf,
    /// Restore path: an already-formatted backing (the downloaded
    /// `<volume_name>.luks`) to stage at `backing_path` before opening, so
    /// the opened volume is always backed by the canonical path — a later
    /// re-suspend then seals and stages the file it restored from.
    /// Staged only when the volume is not already mounted (the idempotent
    /// re-entry must never clobber a live volume's backing). Mutually
    /// exclusive with `fresh`.
    pub restore_source: Option<PathBuf>,
    /// Directory to mount the btrfs filesystem at.
    pub mount_dir: PathBuf,
    /// Logical size in bytes the volume should hold.
    pub size_bytes: u64,
    /// Plaintext DEK (the sandbox's volume DEK), borrowed from a `Zeroizing`
    /// buffer the caller owns. Fed to cryptsetup over stdin.
    pub dek: &'a [u8],
    /// `true` to format a fresh volume (recreate + LUKS-format + mkfs.btrfs);
    /// `false` to open + mount an already-formatted `backing_path`.
    pub fresh: bool,
}

/// Format (or open) and mount an encrypted volume, returning its mount
/// directory. Idempotent: if the volume is already mounted this returns it. On
/// any failure after a device is created, the partial stack is unwound before
/// the error is returned.
pub async fn prepare(params: PrepareParams<'_>) -> Result<PreparedEncryptedVolume, DmCryptError> {
    let PrepareParams {
        sandbox_id,
        volume_name,
        backing_path,
        restore_source,
        mount_dir,
        size_bytes,
        dek,
        fresh,
    } = params;

    if size_bytes == 0 {
        return Err(DmCryptError::InvalidParams(
            "size_bytes must be non-zero".to_string(),
        ));
    }
    if dek.is_empty() {
        return Err(DmCryptError::InvalidParams("empty DEK".to_string()));
    }
    if fresh && restore_source.is_some() {
        return Err(DmCryptError::InvalidParams(
            "restore_source is only valid when opening (fresh = false)".to_string(),
        ));
    }

    let mapper = mapper_name(&sandbox_id, &volume_name);

    // Idempotent re-entry, keyed on device identity (not path spelling).
    // Ordered before the restore staging so a retried prepare can never
    // overwrite the backing file under a live volume.
    if is_mounted(&mount_dir).await? {
        let identity = mapper_identity(&mapper).await;
        let mount_num = mount_device_numbers_of(&mount_dir).await?;
        match idempotent_arm(&identity, mount_num) {
            // Accept ONLY on positive proof the mount is present and ours.
            IdempotentArm::Accept => return Ok(PreparedEncryptedVolume { mount_dir }),
            // Unknown (probe error, or the mount vanished mid-check): never
            // accept an unverified mount, never destroy — fail retryably.
            IdempotentArm::Retry => {
                return Err(match identity {
                    MapperIdentity::Error(error) => error,
                    _ => DmCryptError::Command {
                        cmd: format!("prepare {mapper}"),
                        stderr: "mount device could not be confirmed on re-entry".to_string(),
                    },
                });
            }
            // Positively foreign or a gone mapper name: detach the stale mount
            // before the fresh build. Peel every layer: the visible mount can
            // shadow an older stale mount at the same path.
            IdempotentArm::Rebuild => {
                unmount_all_at_path(&mount_dir).await?;
            }
        }
    }

    // Version-skew recovery: an older prepare may have detached the canonical
    // btrfs mount while leaving this healthy mapper (and Firecracker's jail
    // bind) alive. If the mapper still points at the exact canonical backing,
    // re-mount it instead of attempting a second `cryptsetup open`. Never use
    // this recovery for a fresh format or while a restore source still needs
    // staging; either case could overwrite a live volume.
    match mapper_identity(&mapper).await {
        MapperIdentity::Present(_) => {
            let path = backing_path.clone();
            let backing = device_backing::blocking(move || Backing::open(&path, false)).await?;
            let reconciled =
                reconcile_stale_mapper(&mapper, backing.as_ref().map(|b| &b.file)).await?;
            if !reconciled {
                if !fresh && restore_source.is_none() {
                    mount_mapper(&mapper, &mount_dir).await?;
                    return Ok(PreparedEncryptedVolume { mount_dir });
                }
                return Err(DmCryptError::Command {
                    cmd: format!("prepare {mapper}"),
                    stderr:
                        "existing mapper still owns the current backing; refusing to replace it"
                            .to_string(),
                });
            }
        }
        MapperIdentity::Absent => {}
        MapperIdentity::Error(error) => return Err(error),
    }

    // Restore: lay the downloaded backing down at the canonical path before
    // opening it. Symlink-safe descriptor-relative copy — the source is
    // caller-supplied.
    if let Some(source) = &restore_source {
        let root = PathBuf::from(
            pplx_space_common_core::host::linux::paths::sandbox_cache_dir(&sandbox_id.to_string()),
        );
        let source = source.clone();
        let staged_backing = backing_path.clone();
        tokio::task::spawn_blocking(move || {
            crate::contained_copy::stage_file_contained(&root, &source, &staged_backing)
        })
        .await
        // A JoinError is a panic in the staging task — a server fault, not a
        // caller mistake, so it must map to an Internal-class error (retryable)
        // rather than InvalidParams.
        .map_err(|error| DmCryptError::Io {
            cmd: "restore staging task".to_string(),
            source: std::io::Error::other(error),
        })?
        .map_err(|error| match error {
            e @ crate::contained_copy::ContainedCopyError::Escape { .. } => {
                DmCryptError::InvalidParams(e.to_string())
            }
            crate::contained_copy::ContainedCopyError::Io { op, path, source } => {
                DmCryptError::Io {
                    cmd: format!("{op} {}", path.display()),
                    source,
                }
            }
        })?;
    }

    verify_kernel_support().await?;

    match build_volume(&mapper, &backing_path, &mount_dir, size_bytes, dek, fresh).await {
        Ok(()) => Ok(PreparedEncryptedVolume { mount_dir }),
        Err(e) => {
            let _ = release(&sandbox_id, &volume_name, &mount_dir).await;
            Err(e)
        }
    }
}

/// Ordered build steps, factored out so [`prepare`] can wrap them in a single
/// unwind-on-error.
async fn build_volume(
    mapper: &str,
    backing_path: &Path,
    mount_dir: &Path,
    size_bytes: u64,
    dek: &[u8],
    fresh: bool,
) -> Result<(), DmCryptError> {
    let path = backing_path.to_owned();
    let (backing, loop_device) = device_backing::blocking(move || {
        let backing = if fresh {
            let backing = Backing::replace(&path)?;
            backing.resize(volume_backing_size_bytes(size_bytes))?;
            backing
        } else {
            Backing::require(Backing::open(&path, true)?)?
        };
        let device = LoopDevice::attach(&backing, false)?;
        Ok((backing, device))
    })
    .await?;
    let loop_path = loop_device.path().to_string_lossy();
    if fresh {
        run_with_stdin("cryptsetup", &luks_format_args(&loop_path), dek).await?;
    }

    let mapper_dev = format!("/dev/mapper/{mapper}");
    let open_args = ["open", "--key-file=-", loop_path.as_ref(), mapper];
    if let Err(error) = run_with_stdin("cryptsetup", &open_args, dek).await {
        // A deadline-cancelled capture leaves its mapping (and mounts) behind;
        // reconcile confirmed debris and retry once, else propagate.
        let reconciled = if matches!(
            &error, DmCryptError::Command { stderr, .. } if stderr.contains("already exists")
        ) {
            reconcile_stale_mapper(mapper, Some(&backing.file)).await?
        } else {
            false
        };
        if !reconciled {
            return Err(error);
        }
        run_with_stdin("cryptsetup", &open_args, dek).await?;
    }

    if fresh {
        run("mkfs.btrfs", &["-q", &mapper_dev]).await?;
    }

    mount_mapper(mapper, mount_dir).await
}

async fn mount_mapper(mapper: &str, mount_dir: &Path) -> Result<(), DmCryptError> {
    let mapper_dev = format!("/dev/mapper/{mapper}");
    let target = mount_dir.to_owned();
    device_backing::blocking(move || device_backing::mount(Path::new(&mapper_dev), &target))
        .await?;
    // A cache peer may have renamed the parent after it was pinned. Do not
    // hand back a path which no longer names this newly mounted filesystem.
    if idempotent_arm(
        &mapper_identity(mapper).await,
        mount_device_numbers_of(mount_dir).await?,
    ) != IdempotentArm::Accept
    {
        return Err(DmCryptError::InvalidParams(
            "mounted volume moved before publication".into(),
        ));
    }

    Ok(())
}

/// Tear down an encrypted volume: unmount then close the dm-crypt device.
/// Best-effort and idempotent. Leaves the backing file for the caller to
/// delete (sandbox cleanup / dir reclaim removes it).
pub async fn release(
    sandbox_id: &SandboxId,
    volume_name: &str,
    mount_dir: &Path,
) -> Result<(), DmCryptError> {
    let mapper = mapper_name(sandbox_id, volume_name);
    // Release is the seal-safety gate: it may report Ok ONLY after positively
    // confirming every mountpoint is gone AND the mapper is closed. Anything
    // unconfirmed is retryable, never a success the caller stages as sealed.
    //
    // Durability gate: flush the inner btrfs to the encrypted backing device
    // BEFORE detaching or closing it. `unmount_mapper_mounts` uses a lazy
    // `umount -l` so a cancelled capture's lingering Firecracker fd cannot wedge
    // the seal, but a lazy unmount does NOT wait for writeback. Without an
    // explicit sync the `cryptsetup close` / loop detach below can race ahead of
    // btrfs flushing Firecracker's just-written vmstate+memory, sealing a
    // snapshot that loads but resumes into a dead guest (an in-guest
    // spaced-bridge vsock failure on the next restore). A synchronous `umount`
    // used to provide this flush implicitly; restore it explicitly. A single
    // verified mountpoint is sufficient because syncfs flushes the whole
    // filesystem. Fail closed on any sync error: without positive durability
    // proof this volume must not be detached, closed, or staged as sealed.
    // No mapper mount means there is no mounted inner filesystem to flush and
    // preserves idempotent release after an earlier successful teardown.
    let points = mapper_mountpoints(&mapper).await?;
    if let Some(point) = points.first() {
        run("sync", &["-f", point]).await?;
    }
    // Stack order: unmount every mountpoint (canonical dir + any jail bind),
    // propagating any unmount/mountinfo failure.
    unmount_mapper_mounts(&mapper).await?;
    match mapper_identity(&mapper).await {
        // Already closed.
        MapperIdentity::Absent => {}
        MapperIdentity::Present(_) => {
            // `umount -l` returns once the mounts are detached, not once the
            // filesystem is gone; closing while it still holds the mapping
            // fails a seal whose data `sync -f` already made durable.
            wait_until_unopened(
                &mapper,
                || mapper_open_count(&mapper),
                MAPPER_IDLE_BUDGET,
                MAPPER_IDLE_POLL,
            )
            .await;
            if let Err(error) = ReleaseDevice::Crypt(&mapper).run().await {
                // An open fd (a cancelled capture's Firecracker) can hold the
                // device. Free the NAME so the next prepare isn't wedged, but
                // still fail: the volume was not sealed.
                tracing::warn!(%mapper, %error, "encrypted-volume close refused; force-freeing mapping");
                force_free_mapper(&mapper).await;
                return Err(error);
            }
        }
        // Unknown probe: never claim the mapper closed.
        MapperIdentity::Error(error) => return Err(error),
    }
    // Positive postcondition before wiping or reporting success: the mount is
    // gone. (Wiping is guarded by the same confirmation, so it can never
    // delete into a live encrypted mount.)
    confirm_not_mounted(mount_dir).await?;
    let path = mount_dir.to_owned();
    device_backing::blocking(move || device_backing::remove_empty_directory(&path)).await?;
    Ok(())
}

#[cfg(test)]
mod tests;
