//! Per-sandbox encrypted live rootfs: dm-crypt over a dm-snapshot COW.
//!
//! Goal: every byte the guest writes to its rootfs lands **encrypted** on the
//! node disk, from the first write, **without** giving up the near-instant
//! reflink cold start or btrfs-send diff snapshots — and with **zero**
//! overhead for non-BYOK sandboxes (the caller simply never invokes this
//! module for them).
//!
//! # Layering
//!
//! ```text
//!   template snapshot/rootfs.ext4   (plaintext, shared via btrfs reflink)
//!          │  reflink (caller, ~ms) → instance/origin.ext4
//!          │  losetup → /dev/loopN          ← dm-snapshot ORIGIN (read-only:
//!          │                                   dm-snapshot never writes here,
//!          │                                   so its blocks stay reflink-
//!          │                                   shared with the template)
//!   instance/cow.luks   (sparse, per-sandbox)
//!          │  cryptsetup open (per-sandbox DEK)
//!          │  → /dev/mapper/<id>-cow         ← dm-snapshot COW store; every
//!          │                                   guest write is routed here and
//!          │                                   dm-crypt encrypts it before it
//!          │                                   touches cow.luks on disk
//!          └──── dmsetup create snapshot <origin> <cow> P <chunk> ───────────┐
//!                                                                            │
//!                       /dev/mapper/<id>   ← composite block device          │
//!                                            (Firecracker path_on_host)  ◄────┘
//! ```
//!
//! Reads of blocks the guest never wrote pass straight through the origin
//! (plaintext template — non-secret); reads of written blocks come back
//! through dm-crypt. Suspend captures `cow.luks` (already ciphertext); a btrfs
//! send/receive diff of it is naturally encrypted and small.
//!
//! # The fresh-vs-restore branch (load-bearing)
//!
//! dm-snapshot keeps its exception-store metadata in the first chunk of the
//! COW device. A freshly `luksFormat`-ed mapper returns *random-looking*
//! decrypted bytes there, which the kernel reads as "existing but corrupt
//! metadata" and refuses to activate (`device-mapper: persistent snapshot:
//! Invalid or corrupt snapshot`). So on a **fresh** create we zero the first
//! [`FRESH_COW_ZERO_PREFIX_BYTES`] of the *decrypted* mapper — the "this is a
//! brand-new snapshot" signal. On **restore** the COW already holds valid
//! metadata and the remapped exception blocks; zeroing it would orphan every
//! remapped block and silently corrupt the rootfs, so we MUST NOT zero.
//!
//! # Home
//!
//! This module lives in node-manager — the privileged node daemon — so the
//! unprivileged sandbox-manager can delegate the losetup / cryptsetup /
//! dmsetup work over the node-manager socket, mirroring the btrfs
//! delegation RPCs.
//!
//! # Guardrails (this module is deliberately defensive)
//!
//! * Kernel capability is verified before any device is touched.
//! * COW size is **derived** from the rootfs size — there is no operator knob
//!   to mis-set, and the guest filesystem (sized to the rootfs) cannot write
//!   more than the COW can hold, so the COW cannot overflow by construction.
//! * Backing file is sparse; a free-space floor is checked up front so setup
//!   can't half-succeed on a full disk.
//! * Mapper/loop names are derived from the sandbox id; prepare is idempotent
//!   (re-entry returns the existing device) and never double-opens.
//! * Failures mid-prepare attempt ordered unwind. Failed probes or teardown
//!   retain backing files and report the failure for retry/repair.
//! * The plaintext DEK is fed to cryptsetup over stdin (never written to a
//!   key-file on disk) and the caller holds it in a `Zeroizing` buffer.

use std::path::{Path, PathBuf};
use std::process::Command;

use crate::device_backing::{self, Backing, LoopDevice};
use pplx_space_common_core::types::SandboxId;

/// Bytes of the *decrypted* COW mapper zeroed before a FRESH dm-snapshot is
/// created over it, so the kernel sees a clean exception store rather than
/// LUKS header noise. Never written on restore. 4 MiB comfortably covers the
/// dm-snapshot persistent-store header + first metadata area.
const FRESH_COW_ZERO_PREFIX_BYTES: u64 = 4 * 1024 * 1024;

/// dm-snapshot chunk size in 512-byte sectors. 8 sectors = 4 KiB = the ext4
/// block size, so one guest block write dirties exactly one COW chunk with no
/// read-modify-write amplification.
const SNAPSHOT_CHUNK_SECTORS: u64 = 8;

/// Fixed additive headroom for the COW backing file on top of the rootfs
/// size: covers the LUKS2 header (up to ~16 MiB) plus slack. Combined with the
/// proportional term in [`cow_backing_size_bytes`] it guarantees the decrypted
/// COW can hold a full-rootfs worth of changed chunks plus dm-snapshot
/// metadata even in the pathological "every block rewritten" case.
const COW_HEADROOM_BYTES: u64 = 64 * 1024 * 1024;

/// Minimum free bytes required on the COW's filesystem before setup. The
/// backing file is sparse so this is only the floor needed for the LUKS
/// format + zero-prefix writes to land, not the worst-case divergence.
pub(crate) const MIN_FREE_BYTES_FOR_SETUP: u64 = 128 * 1024 * 1024;

/// `cryptsetup` KDF flags for a LUKS2 volume whose passphrase is a
/// full-entropy random DEK (issued by credmgr), not a human password.
///
/// LUKS2's default KDF is argon2id, which is deliberately *memory-hard*:
/// `luksFormat`/`luksOpen` allocate ~0.5–1 GiB of scratch to make brute-forcing
/// weak human passwords expensive. Our passphrase is a 256-bit random key, so
/// brute force is already infeasible and the memory-hardness buys nothing —
/// while a ~700 MiB spike per format/open OOM-kills the memory-limited
/// node-manager (it runs cryptsetup in its own cgroup). pbkdf2 at cryptsetup's
/// minimum iteration count derives the key in a few MiB. `luksOpen` reads these
/// params from the header, so pinning them at format time makes both format and
/// every subsequent open cheap.
pub(crate) const LUKS_DEK_KDF_ARGS: &[&str] =
    &["--pbkdf", "pbkdf2", "--pbkdf-force-iterations", "1000"];

/// Build the `cryptsetup luksFormat` argv for a DEK-keyed LUKS2 volume backed
/// by `backing`. Threads in [`LUKS_DEK_KDF_ARGS`] so the memory-hard default
/// KDF is never used. The DEK is fed on stdin (`--key-file=-`).
pub(crate) fn luks_format_args(backing: &str) -> Vec<&str> {
    let mut args = vec!["luksFormat", "--batch-mode", "--type", "luks2"];
    args.extend_from_slice(LUKS_DEK_KDF_ARGS);
    args.extend_from_slice(&["--key-file=-", backing]);
    args
}

/// Errors from preparing or releasing an encrypted rootfs. One variant per
/// failure class so the caller (sbx-mgr) can decide retry vs. fail-closed:
/// `KernelUnsupported` and `InvalidParams` are permanent, `InsufficientDisk`
/// is operator-actionable, and `Command`/`Busy`/`Io` are the transient-ish rest.
#[derive(Debug, thiserror::Error)]
pub enum DmCryptError {
    /// The running kernel can't provide dm-snapshot and/or dm-crypt.
    #[error("kernel does not support required device-mapper targets: {0}")]
    KernelUnsupported(String),
    /// A caller-supplied parameter is unusable (zero/again misaligned size,
    /// missing origin file, …). Re-asking won't help.
    #[error("invalid rootfs-crypto parameters: {0}")]
    InvalidParams(String),
    /// Not enough free space on the COW's filesystem to set up safely.
    #[error("insufficient disk for encrypted rootfs setup: {0}")]
    InsufficientDisk(String),
    /// A `losetup`/`cryptsetup`/`dmsetup`/`blockdev`/`dd` invocation failed.
    #[error("command `{cmd}` failed: {stderr}")]
    Command { cmd: String, stderr: String },
    /// `cryptsetup close` refused because the device is still in use, as
    /// reported by its exit status rather than its message.
    #[error("command `{cmd}` failed: {stderr}")]
    Busy { cmd: String, stderr: String },
    /// A spawn / I/O failure invoking a tool or writing the key to stdin.
    #[error("io running `{cmd}`: {source}")]
    Io {
        cmd: String,
        #[source]
        source: std::io::Error,
    },
}

/// A live encrypted rootfs device stack. `mapper_path` is what Firecracker
/// gets as the rootfs drive's `path_on_host`.
#[derive(Debug, Clone)]
pub struct PreparedRootfs {
    /// `/dev/mapper/<composite>` — the dm-snapshot composite device.
    pub mapper_path: PathBuf,
}

/// dm name of the composite snapshot device Firecracker mounts.
fn composite_name(sandbox_id: &SandboxId) -> String {
    format!("sbxroot-{sandbox_id}")
}

/// dm name of the dm-crypt COW device backing the snapshot's writes.
fn cow_mapper_name(sandbox_id: &SandboxId) -> String {
    format!("sbxroot-{sandbox_id}-cow")
}

/// Size of the sparse COW backing file. Derived purely from the rootfs size —
/// no operator knob. The proportional term (1/16) over-covers dm-snapshot's
/// per-chunk metadata (~1/256 of the data) several times over, and
/// [`COW_HEADROOM_BYTES`] covers the LUKS2 header; together they guarantee the
/// decrypted COW can absorb a full rewrite of every rootfs block. Sparse, so
/// the generous virtual size costs nothing until actually written.
fn cow_backing_size_bytes(rootfs_size_bytes: u64) -> u64 {
    rootfs_size_bytes + rootfs_size_bytes / 16 + COW_HEADROOM_BYTES
}

/// Build the `dmsetup create` table line for the snapshot composite.
/// `origin_sectors` is the live size of the origin loop device (queried at
/// runtime, not computed, so it's exact). `P` = persistent store (survives
/// reboot/reopen — required for the restore path).
fn snapshot_table(origin_sectors: u64, origin_dev: &str, cow_dev: &str) -> String {
    format!("0 {origin_sectors} snapshot {origin_dev} {cow_dev} P {SNAPSHOT_CHUNK_SECTORS}")
}

/// Foreground device commands run off-runtime and retain the awaiting
/// operation's ownership until termination and reaping have completed.
pub(crate) async fn run(cmd: &str, args: &[&str]) -> Result<Vec<u8>, DmCryptError> {
    run_captured(cmd, args, &[], crate::host_command::MUTATION_BUDGET).await
}

pub(crate) async fn run_with_stdin(
    cmd: &str,
    args: &[&str],
    stdin_bytes: &[u8],
) -> Result<(), DmCryptError> {
    run_captured(cmd, args, stdin_bytes, crate::host_command::MUTATION_BUDGET)
        .await
        .map(|_| ())
}

async fn run_captured(
    cmd: &str,
    args: &[&str],
    stdin_bytes: &[u8],
    budget: std::time::Duration,
) -> Result<Vec<u8>, DmCryptError> {
    let label = format!("{cmd} {}", args.join(" "));
    let mut command = Command::new(cmd);
    command.args(args);
    let input = zeroize::Zeroizing::new(stdin_bytes.to_vec());
    tokio::task::spawn_blocking(move || checked_output(&mut command, &input, budget))
        .await
        .map_err(|error| DmCryptError::Io {
            cmd: label,
            source: std::io::Error::other(error),
        })?
}

pub(crate) fn checked_output(
    command: &mut Command,
    input: &[u8],
    budget: std::time::Duration,
) -> Result<Vec<u8>, DmCryptError> {
    checked_output_with(command, input, budget, crate::host_command::output)
}

pub(crate) fn checked_output_with(
    command: &mut Command,
    input: &[u8],
    budget: std::time::Duration,
    output: impl FnOnce(
        &mut Command,
        std::time::Duration,
        &[u8],
    ) -> std::io::Result<std::process::Output>,
) -> Result<Vec<u8>, DmCryptError> {
    checked_output_classified(command, input, budget, output, None)
}

/// `cryptsetup`'s documented exit status for "device already exists or device
/// is busy". A close cannot produce "already exists", so for a close it means
/// busy.
const CRYPTSETUP_EXIT_BUSY: i32 = 5;

/// [`checked_output_with`], reporting a failure whose exit status is
/// `busy_exit` as [`DmCryptError::Busy`].
fn checked_output_classified(
    command: &mut Command,
    input: &[u8],
    budget: std::time::Duration,
    output: impl FnOnce(
        &mut Command,
        std::time::Duration,
        &[u8],
    ) -> std::io::Result<std::process::Output>,
    busy_exit: Option<i32>,
) -> Result<Vec<u8>, DmCryptError> {
    let label = format!(
        "{} {}",
        command.get_program().to_string_lossy(),
        command
            .get_args()
            .map(|arg| arg.to_string_lossy())
            .collect::<Vec<_>>()
            .join(" ")
    );
    let out = output(command, budget, input).map_err(|source| DmCryptError::Io {
        cmd: label.clone(),
        source,
    })?;
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr).trim().to_string();
        return Err(if busy_exit.is_some() && out.status.code() == busy_exit {
            DmCryptError::Busy { cmd: label, stderr }
        } else {
            DmCryptError::Command { cmd: label, stderr }
        });
    }
    Ok(out.stdout)
}

/// Individual release steps shared by normal unwind and discovered orphan
/// stacks. Callers own discovery, dependency order, durability and retention;
/// neither a lazy detach nor a command error proves the whole stack is gone.
#[derive(Debug)]
pub(crate) enum ReleaseDevice<'a> {
    Snapshot(&'a str),
    Crypt(&'a str),
    Loop(&'a str),
    Unmount(&'a str),
    DetachMount(&'a str),
}

impl ReleaseDevice<'_> {
    fn command(&self) -> (&'static str, Vec<&str>) {
        match self {
            Self::Snapshot(name) => ("dmsetup", vec!["remove", "--retry", name]),
            Self::Crypt(name) => ("cryptsetup", vec!["close", name]),
            Self::Loop(path) => ("losetup", vec!["-d", path]),
            Self::Unmount(path) => ("umount", vec![path]),
            Self::DetachMount(path) => ("umount", vec!["-l", path]),
        }
    }

    fn busy_exit(&self) -> Option<i32> {
        matches!(self, Self::Crypt(_)).then_some(CRYPTSETUP_EXIT_BUSY)
    }

    pub(crate) fn blocking_with(
        self,
        output: impl FnOnce(
            &mut Command,
            std::time::Duration,
            &[u8],
        ) -> std::io::Result<std::process::Output>,
    ) -> Result<(), DmCryptError> {
        let busy_exit = self.busy_exit();
        let (program, args) = self.command();
        let mut command = Command::new(program);
        command.args(args);
        checked_output_classified(
            &mut command,
            &[],
            crate::host_command::MUTATION_BUDGET,
            output,
            busy_exit,
        )
        .map(|_| ())
    }

    pub(crate) async fn run(self) -> Result<(), DmCryptError> {
        let busy_exit = self.busy_exit();
        let (program, args) = self.command();
        let label = format!("{program} {}", args.join(" "));
        let mut command = Command::new(program);
        command.args(args);
        tokio::task::spawn_blocking(move || {
            checked_output_classified(
                &mut command,
                &[],
                crate::host_command::MUTATION_BUDGET,
                crate::host_command::output,
                busy_exit,
            )
        })
        .await
        .map_err(|error| DmCryptError::Io {
            cmd: label,
            source: std::io::Error::other(error),
        })?
        .map(|_| ())
    }
}

/// Verify the kernel can provide both device-mapper targets we need,
/// loading their modules if necessary. `dmsetup targets` lists every
/// registered target; checking up front turns a late, cryptic activation
/// failure into a clear permanent error.
pub async fn verify_kernel_support() -> Result<(), DmCryptError> {
    let out = run("dmsetup", &["targets"]).await?;
    let listed = String::from_utf8_lossy(&out);
    let has = |t: &str| {
        listed
            .lines()
            .any(|l| l.split_whitespace().next() == Some(t))
    };
    // Neither target appears until its module is loaded; nudge both
    // best-effort and re-check below. This needs the kmod binary in the
    // image and the host's /lib/modules mounted into the container (the
    // BYOK-gated chart shape provides both) — worker kernels don't load
    // dm_crypt/dm_snapshot at boot, and cryptsetup's own auto-load has the
    // same in-container requirements.
    for (target, module) in [("snapshot", "dm_snapshot"), ("crypt", "dm_crypt")] {
        if !has(target)
            && let Err(error) = run("modprobe", &[module]).await
        {
            tracing::warn!(
                module,
                %error,
                "best-effort modprobe failed; the kernel-support re-check below decides"
            );
        }
    }
    let out = run("dmsetup", &["targets"]).await?;
    let listed = String::from_utf8_lossy(&out);
    let has = |t: &str| {
        listed
            .lines()
            .any(|l| l.split_whitespace().next() == Some(t))
    };
    let mut missing = Vec::new();
    if !has("crypt") {
        missing.push("crypt");
    }
    if !has("snapshot") {
        missing.push("snapshot");
    }
    if missing.is_empty() {
        Ok(())
    } else {
        Err(DmCryptError::KernelUnsupported(format!(
            "missing dm targets: {}",
            missing.join(", ")
        )))
    }
}

/// Does a dm device with this name currently exist?
pub(crate) async fn dm_exists(name: &str) -> Result<bool, DmCryptError> {
    // A successful inventory with no matching name proves absence. A failed
    // single-device lookup cannot distinguish absence from an ioctl failure.
    let out = run(
        "dmsetup",
        &["info", "--columns", "--noheadings", "-o", "name"],
    )
    .await?;
    Ok(String::from_utf8_lossy(&out)
        .lines()
        .any(|line| line.trim() == name))
}

/// Tear down the encrypted-rootfs device stack for a sandbox: composite
/// snapshot → dm-crypt COW → origin loops, in dependency order. Idempotent for
/// confirmed absence; probe and teardown errors propagate. It doubles
/// as the failure-unwind for [`prepare`] and the teardown for suspend/delete.
/// Leaves the backing files (`origin.ext4`, `cow.luks`) on disk; the caller
/// decides whether to keep them (suspend) or delete them (sandbox delete).
pub async fn release(sandbox_id: &SandboxId, origin_path: &Path) -> Result<(), DmCryptError> {
    let composite = composite_name(sandbox_id);
    let cow = cow_mapper_name(sandbox_id);

    if dm_exists(&composite).await? {
        ReleaseDevice::Snapshot(&composite).run().await?;
    }
    if dm_exists(&cow).await? {
        ReleaseDevice::Crypt(&cow).run().await?;
    }
    for loopdev in loops_for_file(origin_path).await? {
        ReleaseDevice::Loop(&loopdev.path().to_string_lossy())
            .run()
            .await?;
    }
    Ok(())
}

/// Inputs for [`prepare`].
pub struct PrepareParams<'a> {
    pub sandbox_id: SandboxId,
    /// Plaintext, reflinked-from-template rootfs file — the dm-snapshot origin.
    pub origin_path: PathBuf,
    /// Per-sandbox encrypted COW backing file. Created here on a fresh create;
    /// on restore it is either the kept canonical COW (opened in place) or the
    /// destination [`restore_cow_source`] is staged into before opening.
    pub cow_path: PathBuf,
    /// Restore path: the downloaded `cow.luks` to stage at `cow_path` before
    /// opening. `None` on a fresh create and on a same-node restore (the COW
    /// was kept in place). Staged only when the composite is not already live,
    /// so a retried restore/launch against a running rootfs reuses the live
    /// stack instead of clobbering its backing — the same re-entry invariant
    /// the data-volume primitive enforces around its mount check.
    pub restore_cow_source: Option<PathBuf>,
    /// Logical rootfs size in bytes; the COW is sized from this.
    pub rootfs_size_bytes: u64,
    /// Plaintext DEK used as the LUKS passphrase. Held by the caller in a
    /// `Zeroizing` buffer; this module only borrows it and feeds it to
    /// cryptsetup over stdin.
    pub dek: &'a [u8],
}

/// Bring up the encrypted rootfs device stack and return the composite mapper
/// path for Firecracker. Idempotent: if the composite already exists this
/// returns it without touching anything. On any failure after a device is
/// created, the partial stack is unwound before the error is returned.
pub async fn prepare(params: PrepareParams<'_>) -> Result<PreparedRootfs, DmCryptError> {
    let PrepareParams {
        sandbox_id,
        origin_path,
        cow_path,
        restore_cow_source,
        rootfs_size_bytes,
        dek,
    } = params;

    // rootfs_size_bytes is an optional cross-check now — the COW is sized
    // from the measured origin device (see build_stack). 0 means "derive from
    // the origin"; a non-zero value must at least be sector-aligned to be a
    // believable size.
    if rootfs_size_bytes != 0 && rootfs_size_bytes % SECTOR_BYTES != 0 {
        return Err(DmCryptError::InvalidParams(format!(
            "rootfs_size_bytes must be 0 or a multiple of {SECTOR_BYTES}, got {rootfs_size_bytes}"
        )));
    }
    if dek.is_empty() {
        return Err(DmCryptError::InvalidParams("empty DEK".to_string()));
    }
    let (origin, origin_bytes) = {
        let path = origin_path.clone();
        device_backing::blocking(move || {
            let origin = Backing::require(Backing::open(&path, false)?)?;
            let size = origin.file.metadata()?.len();
            Ok((origin, size))
        })
        .await?
    };
    pplx_space_common_core::sandbox_storage::validate_rootfs_bytes(origin_bytes)
        .map_err(|error| DmCryptError::InvalidParams(error.to_string()))?;

    let composite = composite_name(&sandbox_id);
    let mapper_path = PathBuf::from(format!("/dev/mapper/{composite}"));

    verify_kernel_support().await?;

    // A live stack wins before staging or creating any backing file.
    if dm_exists(&composite).await? {
        return reuse_live_composite(&composite, origin, mapper_path).await;
    }
    if let Some(source) = &restore_cow_source {
        stage_restore_cow(&sandbox_id, source, &cow_path).await?;
    }
    // Remove only the partial stack, preserving any committed COW bytes.
    release(&sandbox_id, &origin_path).await?;
    // Run the fallible build; on any error unwind whatever we created.
    match build_stack(&sandbox_id, origin, cow_path, rootfs_size_bytes, dek).await {
        Ok(()) => Ok(PreparedRootfs { mapper_path }),
        Err(e) => {
            // Best-effort unwind; surface the original error, not the cleanup's.
            if let Err(cleanup) = release(&sandbox_id, &origin_path).await {
                tracing::warn!(%sandbox_id, %cleanup, "encrypted rootfs unwind failed; retaining backing files for repair");
            }
            Err(e)
        }
    }
}

/// Re-entry must match the actual composite origin, not merely find some loop
/// attached to a pathname which a cache peer can replace.
async fn reuse_live_composite(
    composite: &str,
    origin: Backing,
    mapper_path: PathBuf,
) -> Result<PreparedRootfs, DmCryptError> {
    let table = run("dmsetup", &["table", composite]).await?;
    let table = String::from_utf8_lossy(&table);
    let origin_device = table
        .split_whitespace()
        .nth(3)
        .and_then(parse_device_numbers)
        .ok_or_else(|| {
            DmCryptError::InvalidParams("composite origin is not a device number".into())
        })?;
    if origin_device.0 != 7 {
        return Err(DmCryptError::InvalidParams(
            "composite origin is not a loop device".into(),
        ));
    }
    let device = PathBuf::from(format!("/dev/loop{}", origin_device.1));
    let matches =
        device_backing::blocking(move || device_backing::matches_loop(&device, &origin.file))
            .await?;
    if !matches {
        return Err(DmCryptError::InvalidParams(
            "live composite owns a different origin inode".into(),
        ));
    }
    Ok(PreparedRootfs { mapper_path })
}

/// Stage the downloaded restore COW to its canonical path with a symlink-safe,
/// atomic, descriptor-relative copy. Runs on the blocking pool. The source is
/// caller-supplied, so containment is enforced against the sandbox cache dir.
async fn stage_restore_cow(
    sandbox_id: &SandboxId,
    source: &Path,
    cow_path: &Path,
) -> Result<(), DmCryptError> {
    let root = PathBuf::from(
        pplx_space_common_core::host::linux::paths::sandbox_cache_dir(&sandbox_id.to_string()),
    );
    let source = source.to_path_buf();
    let cow_path = cow_path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        crate::contained_copy::stage_file_contained(&root, &source, &cow_path)
    })
    .await
    // A JoinError is a staging-task panic — a server fault, so map it to an
    // Internal-class error (retryable), not a caller InvalidParams.
    .map_err(|error| DmCryptError::Io {
        cmd: "restore cow staging task".to_string(),
        source: std::io::Error::other(error),
    })?
    .map(drop)
    .map_err(|error| match error {
        e @ crate::contained_copy::ContainedCopyError::Escape { .. } => {
            DmCryptError::InvalidParams(e.to_string())
        }
        crate::contained_copy::ContainedCopyError::Io { op, path, source } => DmCryptError::Io {
            cmd: format!("{op} {}", path.display()),
            source,
        },
    })
}

/// The ordered device-build steps, factored out so [`prepare`] can wrap them
/// in a single unwind-on-error.
async fn build_stack(
    sandbox_id: &SandboxId,
    origin: Backing,
    cow_path: PathBuf,
    rootfs_size_bytes: u64,
    dek: &[u8],
) -> Result<(), DmCryptError> {
    let cow = cow_mapper_name(sandbox_id);
    let composite = composite_name(sandbox_id);
    let cow_mapper = format!("/dev/mapper/{cow}");

    // Attach the (plaintext) origin as a loop device FIRST — dm-snapshot's
    // origin must be a block device, and we size the COW from the origin's
    // *measured* size, not from the caller's `rootfs_size_bytes`. That makes
    // COW capacity ≥ the exact block device the guest sees, so the COW cannot
    // overflow no matter how a caller computed the size (the guest can't
    // address past the device, and dm-snapshot stores one exception per
    // chunk, so total COW use is bounded by the device size).
    let origin_loop = device_backing::blocking(move || LoopDevice::attach(&origin, true)).await?;
    let origin_sectors = blockdev_sectors(origin_loop.path()).await?;
    let origin_bytes = origin_sectors.saturating_mul(SECTOR_BYTES);
    pplx_space_common_core::sandbox_storage::validate_rootfs_bytes(origin_bytes)
        .map_err(|error| DmCryptError::InvalidParams(error.to_string()))?;

    // `rootfs_size_bytes` is now only a sanity cross-check: the measured
    // origin is authoritative for sizing. A material disagreement means the
    // caller's view of the rootfs is stale, worth a log line but not fatal.
    if rootfs_size_bytes != 0 && rootfs_size_bytes != origin_bytes {
        tracing::warn!(
            rootfs_size_bytes,
            origin_bytes,
            "rootfs_size_bytes disagrees with measured origin; sizing COW from measured origin"
        );
    }

    let cow_name = cow_path
        .file_name()
        .ok_or_else(|| device_backing::error(std::io::ErrorKind::InvalidInput.into()))?
        .to_owned();
    let (cow_backing, cow_loop, fresh) = device_backing::blocking(move || {
        let (cow_backing, fresh) = match Backing::open(&cow_path, true)? {
            Some(file) => (file, false),
            None => (Backing::create_unpublished(&cow_path)?, true),
        };
        if fresh {
            cow_backing.resize(cow_backing_size_bytes(origin_bytes))?;
        }
        let cow_loop = LoopDevice::attach(&cow_backing, false)?;
        Ok((cow_backing, cow_loop, fresh))
    })
    .await?;
    let cow_device = cow_loop.path().to_string_lossy();
    if fresh {
        run_with_stdin("cryptsetup", &luks_format_args(&cow_device), dek).await?;
    } else {
        // Existing invalid or unreadable data is never authority to reformat.
        run("cryptsetup", &["isLuks", &cow_device]).await?;
    }
    run_with_stdin(
        "cryptsetup",
        &["open", "--key-file=-", &cow_device, &cow],
        dek,
    )
    .await?;

    // FRESH ONLY: zero the COW metadata header so dm-snapshot sees a clean
    // exception store. NEVER on restore — see module docs.
    if fresh {
        run(
            "dd",
            &[
                "if=/dev/zero",
                &format!("of={cow_mapper}"),
                "bs=1M",
                &format!("count={}", FRESH_COW_ZERO_PREFIX_BYTES / (1024 * 1024)),
                "conv=fsync",
            ],
        )
        .await?;
        // Only a complete LUKS container with an initialized COW header may
        // select the reopen path on retry. Publish before creating the
        // composite, so every device that can carry guest writes has durable
        // backing. Existing COWs are never reformatted or republished.
        device_backing::blocking(move || cow_backing.publish(&cow_name)).await?;
    }

    // Build the composite snapshot device over (origin, cow). `origin_sectors`
    // was measured above (and used to size the COW), so device and COW are
    // guaranteed consistent.
    let table = snapshot_table(
        origin_sectors,
        &origin_loop.path().to_string_lossy(),
        &cow_mapper,
    );
    run("dmsetup", &["create", &composite, "--table", &table]).await?;
    // `dmsetup create` loads the kernel device but, on a host with no running
    // udev daemon (containers, minimal nodes), does not materialize the
    // `/dev/mapper/<name>` node — so the downstream bind-mount into the jail
    // fails with "special device does not exist" even though the device is
    // ACTIVE in `dmsetup ls`. (cryptsetup creates its own node on `open`,
    // which is why the COW mapper appears but the composite does not.)
    // `mknodes` materializes the node and is idempotent + udev-independent.
    run("dmsetup", &["mknodes", &composite]).await?;

    Ok(())
}

/// Sector count of a block device via `blockdev --getsz` (always 512-byte
/// sectors regardless of physical sector size).
async fn blockdev_sectors(dev: &Path) -> Result<u64, DmCryptError> {
    let out = run("blockdev", &["--getsz", &dev.to_string_lossy()]).await?;
    String::from_utf8_lossy(&out)
        .trim()
        .parse::<u64>()
        .map_err(|e| DmCryptError::Command {
            cmd: format!("blockdev --getsz {dev:?}"),
            stderr: format!("unparseable sector count: {e}"),
        })
}

/// 512-byte sector size used by dm/blockdev.
const SECTOR_BYTES: u64 = 512;

#[cfg(test)]
mod tests;

/// Backing file for a warm-pool member's plaintext COW. Deliberately a
/// different name from [`ROOTFS_COW_FILE`]'s `cow.luks`: the pinned COW probe decides
/// format-vs-open from on-disk state, and a plaintext file at the encrypted
/// path would read as "never formatted" and be silently LUKS-formatted by a
/// later prepare, destroying the member's exception store.
///
/// Presence contract: this file survives on disk only once its exception
/// store has been activated by the kernel — a fresh create's failure unwind
/// deletes it. Its presence therefore means "this member has committed COW
/// state", which is what sbx-mgr's degrade decision and its
/// suspend/snapshot veto for plaintext composite sandboxes key on.
pub(crate) const POOL_COW_FILE: &str = "cow.plain";

/// First bytes of dm-snapshot's persistent exception-store header: the
/// little-endian on-disk encoding of the kernel's `SNAP_MAGIC` (0x70416e53),
/// which reads as `SnAp`. The kernel writes it when it activates a fresh
/// (all-zero) store, so a `cow.plain` carrying it holds live exception
/// metadata.
const SNAPSHOT_STORE_MAGIC: [u8; 4] = *b"SnAp";

/// Whether a COW backing file's first bytes carry a dm-snapshot persistent
/// store header. This is the plaintext analogue of the pinned COW probe's
/// format-vs-open decision: a store the kernel has activated MUST be opened
/// as-is — re-zeroing its header would orphan every remapped chunk and
/// silently revert the guest to the template.
fn has_persistent_snapshot_store(prefix: &[u8]) -> bool {
    prefix
        .get(..SNAPSHOT_STORE_MAGIC.len())
        .is_some_and(|head| head == SNAPSHOT_STORE_MAGIC)
}

/// All loop devices currently backing `file` (`losetup -j`). A file can be
/// attached more than once — e.g. a crashed prepare retried before anything
/// detached its first loop — and teardown must see every attachment, not
/// just the first output line.
async fn loops_for_file(file: &Path) -> Result<Vec<LoopDevice>, DmCryptError> {
    use std::os::fd::AsRawFd;
    let path = file.to_owned();
    let Some(backing) = device_backing::blocking(move || Backing::open(&path, false)).await? else {
        return Ok(Vec::new());
    };
    // The query is only a candidate index; kernel inode identity authorizes
    // detach, and the returned block fd prevents reuse until detach completes.
    let pinned = format!(
        "/proc/{}/fd/{}",
        std::process::id(),
        backing.file.as_raw_fd()
    );
    let out = run("losetup", &["-j", &pinned]).await?;
    // Lines look like: `/dev/loop5: [2049]:123 (/path/origin.ext4)`
    let candidates: Vec<_> = String::from_utf8_lossy(&out)
        .lines()
        .filter_map(|line| {
            let dev = line.split(':').next()?.trim();
            (!dev.is_empty()).then(|| PathBuf::from(dev))
        })
        .collect();
    device_backing::blocking(move || {
        candidates
            .into_iter()
            .filter_map(|device| LoopDevice::matching(&device, &backing.file).transpose())
            .collect()
    })
    .await
}

/// Tear down the plaintext-COW composite stack: composite snapshot, then
/// every loop attached to the COW and origin backing files, in dependency
/// order. Best-effort against absent devices and idempotent, mirroring
/// [`release`] for the encrypted stack.
///
/// `remove_cow` additionally deletes `cow.plain`. That is correct ONLY for a
/// fresh create's unwind, where the store was never activated: per the
/// [`POOL_COW_FILE`] presence contract, an activated store's file must
/// survive its device stack.
async fn release_plaintext(
    sandbox_id: &SandboxId,
    origin_path: &Path,
    cow_path: &Path,
    remove_cow: bool,
) -> Result<(), DmCryptError> {
    let composite = composite_name(sandbox_id);
    if dm_exists(&composite).await? {
        ReleaseDevice::Snapshot(&composite).run().await?;
    }
    for lo in loops_for_file(cow_path).await? {
        ReleaseDevice::Loop(&lo.path().to_string_lossy())
            .run()
            .await?;
    }
    for lo in loops_for_file(origin_path).await? {
        ReleaseDevice::Loop(&lo.path().to_string_lossy())
            .run()
            .await?;
    }
    if remove_cow {
        let path = cow_path.to_owned();
        device_backing::blocking(move || device_backing::unlink(&path)).await?;
    }
    Ok(())
}

/// Bring up the dm-snapshot composite over a **plaintext** COW: the shape a
/// warm-pool member boots on before it has an owner.
///
/// The plaintext COW is a bare loop device rather than a dm-crypt mapper.
/// Encryption is immutable for a sandbox; no in-place conversion is supported.
///
/// Idempotent: a live composite is handed back as-is, and a persisted COW
/// (stop→start, node-manager restart) is opened rather than re-initialized.
/// Any mid-build failure unwinds every device created, plus the COW file
/// itself when its store was never activated.
///
/// `encrypted_cow_path` is the sandbox's `cow.luks` location, checked only to
/// refuse the call: an encrypted sandbox restarting down the plaintext arm
/// means the caller's encryption view of the sandbox is stale, and
/// building a fresh plaintext COW here would orphan the encrypted one —
/// silently downgrading the sandbox to plaintext-at-rest.
pub async fn prepare_plaintext_snapshot(
    sandbox_id: &SandboxId,
    origin_path: &Path,
    cow_path: &Path,
    encrypted_cow_path: &Path,
) -> Result<PreparedRootfs, DmCryptError> {
    let origin = {
        let path = origin_path.to_owned();
        device_backing::blocking(move || Backing::require(Backing::open(&path, false)?)).await?
    };
    // Fail closed on the canonical encrypted COW's PRESENCE, not on
    // `cryptsetup isLuks` succeeding: per the re-home's staging protocol the
    // canonical name exists only once a re-home committed, and a probe that
    // treats an I/O or tooling failure as "no encrypted COW" would build a
    // plaintext COW over the committed one — exactly the orphaning this
    // guard exists to prevent.
    match tokio::fs::try_exists(encrypted_cow_path).await {
        Ok(false) => {}
        Ok(true) => {
            return Err(DmCryptError::InvalidParams(format!(
                "sandbox already has an encrypted COW at {encrypted_cow_path:?}; \
                 refusing a plaintext prepare that would orphan it"
            )));
        }
        Err(source) => {
            return Err(DmCryptError::Io {
                cmd: format!("stat {}", encrypted_cow_path.display()),
                source,
            });
        }
    }
    let composite = composite_name(sandbox_id);
    let mapper_path = PathBuf::from(format!("/dev/mapper/{composite}"));

    verify_kernel_support().await?;
    if dm_exists(&composite).await? {
        return reuse_live_composite(&composite, origin, mapper_path).await;
    }

    release_plaintext(sandbox_id, origin_path, cow_path, false).await?;
    let (cow, fresh) = {
        let path = cow_path.to_owned();
        device_backing::blocking(move || {
            use std::os::unix::fs::FileExt;
            let cow = match Backing::open(&path, true)? {
                Some(file) => file,
                None => Backing::create(&path)?,
            };
            let mut prefix = [0; 4];
            let length = cow.file.read_at(&mut prefix, 0)?;
            let fresh = length != prefix.len() || !has_persistent_snapshot_store(&prefix);
            Ok((cow, fresh))
        })
        .await?
    };

    match build_plaintext_stack(sandbox_id, origin, cow, fresh).await {
        Ok(()) => Ok(PreparedRootfs { mapper_path }),
        Err(e) => {
            // Best-effort unwind; surface the original error, not the
            // cleanup's — but a failed unwind is exactly the loop/mapper leak
            // the delete-time sweep can no longer find once the COW file is
            // gone, so it gets its own log line rather than a silent discard.
            if let Err(cleanup) = release_plaintext(sandbox_id, origin_path, cow_path, fresh).await
            {
                tracing::warn!(
                    %sandbox_id,
                    error = %cleanup,
                    "plaintext snapshot rootfs unwind failed; devices may be leaked"
                );
            }
            Err(e)
        }
    }
}

/// The ordered plaintext-stack build steps, factored out so
/// [`prepare_plaintext_snapshot`] can wrap them in a single unwind-on-error
/// (the same shape [`prepare`] gives [`build_stack`]).
async fn build_plaintext_stack(
    sandbox_id: &SandboxId,
    origin: Backing,
    cow: Backing,
    fresh: bool,
) -> Result<(), DmCryptError> {
    let composite = composite_name(sandbox_id);

    let origin_loop = device_backing::blocking(move || LoopDevice::attach(&origin, true)).await?;
    let origin_sectors = blockdev_sectors(origin_loop.path()).await?;
    let origin_bytes = origin_sectors.saturating_mul(SECTOR_BYTES);
    let cow_loop = device_backing::blocking(move || {
        if fresh {
            cow.resize(cow_backing_size_bytes(origin_bytes))?;
        }
        LoopDevice::attach(&cow, false)
    })
    .await?;

    // FRESH ONLY: a fresh exception store must read as zeroes or dm-snapshot
    // rejects it as corrupt metadata (a recycled backing file could hold
    // anything). On open, the store header IS the data — zeroing it would
    // orphan every remapped chunk.
    if fresh {
        run_bounded(
            "dd",
            &[
                "if=/dev/zero",
                &format!("of={}", cow_loop.path().to_string_lossy()),
                "bs=1M",
                &format!("count={}", FRESH_COW_ZERO_PREFIX_BYTES / (1024 * 1024)),
                "conv=fsync",
            ],
            FREEZE_COMMAND_BUDGET,
        )
        .await?;
    }

    let table = snapshot_table(
        origin_sectors,
        &origin_loop.path().to_string_lossy(),
        &cow_loop.path().to_string_lossy(),
    );
    run("dmsetup", &["create", &composite, "--table", &table]).await?;
    run_bounded("dmsetup", &["mknodes", &composite], FREEZE_COMMAND_BUDGET).await?;

    Ok(())
}

/// Parse a dm `major:minor` device reference — the form `dmsetup table` and
/// `dmsetup info -c -o major,minor` print — into numbers. Numeric, not
/// string, comparison: column padding and leading zeros must not make one
/// device look like two.
pub(crate) fn parse_device_numbers(raw: &str) -> Option<(u32, u32)> {
    let (major, minor) = raw.trim().split_once(':')?;
    Some((major.trim().parse().ok()?, minor.trim().parse().ok()?))
}

const FREEZE_COMMAND_BUDGET: std::time::Duration = crate::host_command::MUTATION_BUDGET;

async fn run_bounded(
    cmd: &str,
    args: &[&str],
    budget: std::time::Duration,
) -> Result<Vec<u8>, DmCryptError> {
    run_captured(cmd, args, &[], budget).await
}

/// The crypt mapper's `major:minor` — the comparison key the idempotency and
/// commit probes match against the live table's COW slot. A probe failure is
/// an ERROR, never "no mapper": callers make destructive decisions on the
/// answer, and collapsing a failed `dmsetup info` into a mismatch would let
/// them unwind the backing of a committed live table.
pub(crate) async fn crypt_mapper_numbers(cow: &str) -> Result<(u32, u32), DmCryptError> {
    let out = run(
        "dmsetup",
        &[
            "info",
            "-c",
            "--noheadings",
            "--separator",
            ":",
            "-o",
            "major,minor",
            cow,
        ],
    )
    .await?;
    let text = String::from_utf8_lossy(&out);
    parse_device_numbers(&text).ok_or_else(|| {
        DmCryptError::InvalidParams(format!(
            "unparseable device numbers for mapper {cow}: {}",
            text.trim()
        ))
    })
}
