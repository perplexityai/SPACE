//! Filesystem-level snapshot artifacts for the Full ext4 codec.
//!
//! An nspawn guest has no VM RAM to checkpoint, so the durable state *is* the
//! rootfs. The guest boots a directory tree (`instance/root`); at snapshot time
//! `CreateSnapshot` rebuilds `rootfs.ext4` from that tree (`mke2fs -d`) and
//! materializes the Full-codec artifact set — `rootfs.ext4` plus placeholder
//! `vmstate` / `memory` files — which sandbox-manager uploads through the real
//! volume-manager → the local object store. Restore reflinks `rootfs.ext4` back and extracts
//! it (`debugfs rdump`) into the tree for a cold boot.

use std::path::Path;

/// Copy `src` to `dst`, preferring a reflink (O(1) `CoW`) and falling back to a
/// byte copy on filesystems without reflink support.
pub fn reflink_or_copy(src: &Path, dst: &Path) -> std::io::Result<()> {
    if let Some(parent) = dst.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let reflinked = std::process::Command::new("cp")
        .arg("--reflink=auto")
        .arg("-f")
        .arg(src)
        .arg(dst)
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok_and(|s| s.success());
    if reflinked {
        return Ok(());
    }
    std::fs::copy(src, dst)?;
    Ok(())
}

/// Extract an ext4 image into a directory tree without loop-mounting it.
///
/// `systemd-nspawn --image=` needs udev + the host /run hierarchy to set up a
/// loop device, which kind pods don't provide, so we boot `--directory=` from an
/// extracted tree instead. `debugfs -R "rdump / <dir>"` reads the ext4 contents
/// directly (no mount, no loop) preserving ownership, modes, and symlinks.
pub fn extract_ext4_to_dir(image: &Path, dir: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)?;
    let status = std::process::Command::new("debugfs")
        .arg("-R")
        .arg(format!("rdump / {}", dir.display()))
        .arg(image)
        .status()?;
    if !status.success() {
        return Err(std::io::Error::other(format!(
            "debugfs rdump of {} failed: {status}",
            image.display()
        )));
    }
    Ok(())
}

/// Build an ext4 image from a directory tree without loop-mounting it.
///
/// `mke2fs -d <dir>` populates the filesystem image directly from the tree, so
/// no root mount / loop device is needed — the counterpart to
/// [`extract_ext4_to_dir`] for capturing the live rootfs at snapshot time.
pub fn build_ext4_from_dir(dir: &Path, image: &Path, size_mib: u64) -> std::io::Result<()> {
    if let Some(parent) = image.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let _ = std::fs::remove_file(image);
    let status = std::process::Command::new("mke2fs")
        .arg("-t")
        .arg("ext4")
        .arg("-d")
        .arg(dir)
        .arg("-F")
        .arg("-q")
        .arg(image)
        .arg(format!("{size_mib}m"))
        .status()?;
    if !status.success() {
        return Err(std::io::Error::other(format!(
            "mke2fs -d {} -> {} failed: {status}",
            dir.display(),
            image.display()
        )));
    }
    Ok(())
}

/// Write the Full-codec artifact set into `dest_dir`:
/// `rootfs.ext4` (rebuilt from the live rootfs tree to capture current state),
/// `vmstate`, `memory`.
///
/// `vmstate` / `memory` are placeholders — the local runtime restores at the
/// filesystem level (cold boot from the restored rootfs), so a Full restore
/// needs only the rootfs and never loads these. They must be **non-empty**
/// though: sandbox-manager's restore rejects a zero-length `memory` file
/// (`<path> is empty`) and derives a memory-MiB from its size, so we write a
/// tiny non-empty marker (rounds up to 1 MiB, well under the guest max).
const SNAPSHOT_PLACEHOLDER: &[u8] =
    b"pplx-space-local-runtime: no VM memory snapshot (fs-level restore)\n";

pub fn write_full_snapshot(
    instance_rootdir: &Path,
    instance_rootfs: &Path,
    dest_dir: &Path,
    vmstate_name: &str,
    memory_name: &str,
    size_mib: u64,
) -> std::io::Result<()> {
    std::fs::create_dir_all(dest_dir)?;
    // Refresh the instance ext4 from the live rootfs tree so it mirrors current
    // guest state. In the Firecracker model `instance/rootfs.ext4` *is* the live
    // VM disk and is what the suspend upload stages; here the live writes land in
    // `instance/root`, so rebuild the image before it (or our dest copy) is read.
    build_ext4_from_dir(instance_rootdir, instance_rootfs, size_mib)?;
    reflink_or_copy(instance_rootfs, &dest_dir.join("rootfs.ext4"))?;
    std::fs::write(dest_dir.join(vmstate_name), SNAPSHOT_PLACEHOLDER)?;
    std::fs::write(dest_dir.join(memory_name), SNAPSHOT_PLACEHOLDER)?;
    Ok(())
}
