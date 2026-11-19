//! Occupancy and `statvfs` probes for a filesystem.
//!
//! [`DiskUsage`] is a coarse legacy occupancy view derived from `f_bfree`.
//! It is useful for gauges and broad placement bias, but it is not btrfs
//! logical referenced data and must not be used as an allocation proof.
//! Admission uses [`DiskStatus::available_bytes`] (`f_bavail`) plus btrfs
//! pressure checks instead.

/// A coarse `statvfs` occupancy snapshot. Tests construct this directly;
/// production derives it with [`DiskStatus::usage`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DiskUsage {
    pub used_bytes: u64,
    pub total_bytes: u64,
}

/// A `statvfs` snapshot of a filesystem's total, free, and unprivileged
/// available bytes. `free_bytes` comes from `f_bfree`; `available_bytes`
/// comes from `f_bavail` and is the conservative admission signal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DiskStatus {
    pub total_bytes: u64,
    pub free_bytes: u64,
    pub available_bytes: u64,
}

impl DiskStatus {
    pub fn usage(self) -> DiskUsage {
        DiskUsage {
            used_bytes: self.total_bytes.saturating_sub(self.free_bytes),
            total_bytes: self.total_bytes,
        }
    }

    /// `f_bavail / total` in `[0.0, 1.0]`; 0 on a degenerate filesystem.
    pub fn available_fraction(self) -> f64 {
        if self.total_bytes == 0 {
            return 0.0;
        }
        (self.available_bytes as f64 / self.total_bytes as f64).clamp(0.0, 1.0)
    }
}

impl DiskUsage {
    /// `used / total` in `[0.0, 1.0]`; 0 on an empty/degenerate fs.
    pub fn fraction(self) -> f64 {
        if self.total_bytes == 0 {
            return 0.0;
        }
        (self.used_bytes as f64 / self.total_bytes as f64).clamp(0.0, 1.0)
    }

    /// `free / total` in `[0.0, 1.0]`; 0 on an empty/degenerate fs, so a
    /// broken or unmounted filesystem never advertises headroom.
    pub fn free_fraction(self) -> f64 {
        if self.total_bytes == 0 {
            return 0.0;
        }
        let free_bytes = self.total_bytes.saturating_sub(self.used_bytes);
        (free_bytes as f64 / self.total_bytes as f64).clamp(0.0, 1.0)
    }

    /// Bytes that must be freed to bring usage below
    /// `target_fraction` of the filesystem. 0 when already below.
    pub fn bytes_over_target(self, target_fraction: f64) -> u64 {
        let target_bytes_f64 = self.total_bytes as f64 * target_fraction.clamp(0.0, 1.0);
        #[expect(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            reason = "target_fraction is clamped into 0..=1, and Rust has no checked f64-to-u64 conversion"
        )]
        let target_bytes = target_bytes_f64 as u64;
        self.used_bytes.saturating_sub(target_bytes)
    }
}

#[cfg(unix)]
pub fn statvfs_usage(path: &str) -> Option<DiskUsage> {
    statvfs_status(path).map(DiskStatus::usage)
}

#[cfg(unix)]
pub fn statvfs_status(path: &str) -> Option<DiskStatus> {
    let vfs = nix::sys::statvfs::statvfs(path).ok()?;
    let frsize = statvfs_field_to_u64(vfs.fragment_size())?;
    let total = statvfs_field_to_u64(vfs.blocks())?.saturating_mul(frsize);
    let free = statvfs_field_to_u64(vfs.blocks_free())?.saturating_mul(frsize);
    let available = statvfs_field_to_u64(vfs.blocks_available())?.saturating_mul(frsize);
    Some(DiskStatus {
        total_bytes: total,
        free_bytes: free,
        available_bytes: available,
    })
}

#[cfg(unix)]
fn statvfs_field_to_u64<T>(value: T) -> Option<u64>
where
    u64: TryFrom<T>,
{
    u64::try_from(value).ok()
}

#[cfg(test)]
mod tests;
