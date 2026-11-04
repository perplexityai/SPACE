use super::*;

/// A census that cannot fit must fail, never publish a truncated total/list.
/// The budget is shared by nested walks, including ignored directory entries.
pub(super) struct DirectoryBudget {
    remaining: usize,
    started: Instant,
}

impl Default for DirectoryBudget {
    fn default() -> Self {
        Self {
            remaining: 16_384,
            started: Instant::now(),
        }
    }
}

impl DirectoryBudget {
    pub(super) fn directories(&mut self, path: &Path) -> std::io::Result<Vec<PathBuf>> {
        self.check()?;
        let entries = match std::fs::read_dir(path) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(error),
        };
        let mut paths = Vec::new();
        for entry in entries {
            self.check()?;
            self.remaining -= 1;
            let entry = entry?;
            // Do not walk symlinked cache roots outside the census tree.
            if entry.file_type()?.is_dir() {
                paths.push(entry.path());
            }
        }
        Ok(paths)
    }

    fn check(&self) -> std::io::Result<()> {
        if self.remaining == 0 || self.started.elapsed() >= Duration::from_secs(2) {
            return Err(std::io::Error::other("directory inventory budget exceeded"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;

pub(super) fn read_pid_from_symlink(path: &str) -> Option<u32> {
    std::fs::read_link(path).ok().and_then(|target| {
        target
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(|s| s.parse::<u32>().ok())
            .filter(|pid| launch::validate_process_pid(*pid).is_ok())
    })
}

pub(super) struct BtrfsDiskUsage {
    pub(super) exclusive_bytes: u64,
    pub(super) total_bytes: u64,
}

// Extent accounting is a measurement, not a cheap host readiness probe.
pub(super) const DISK_MEASUREMENT_BUDGET: Duration = Duration::from_secs(30);

/// Get exclusive and total bytes for a path using `btrfs filesystem du -s --raw`.
///
/// Returns `Ok(None)` when the path is gone by the time btrfs checks it.
pub(super) fn btrfs_du_exclusive(
    path: &str,
    budget: Duration,
) -> Result<Option<BtrfsDiskUsage>, String> {
    let output = crate::host_command::output(
        crate::host_command::Command::new("btrfs").args(["filesystem", "du", "-s", "--raw", path]),
        budget.min(DISK_MEASUREMENT_BUDGET),
        &[],
    )
    .map_err(|e| format!("run btrfs filesystem du: {e}"))?;
    if !output.status.success() {
        let error = String::from_utf8_lossy(&output.stderr).trim().to_string();
        return match std::path::Path::new(path).try_exists() {
            Ok(false) => Ok(None),
            Ok(true) => Err(error),
            Err(exists_error) => Err(format!("{error}; check path existence: {exists_error}")),
        };
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let line = stdout
        .lines()
        .nth(1)
        .ok_or_else(|| "missing btrfs du data row".to_string())?;
    let mut cols = line.split_whitespace();
    let total_bytes = cols
        .next()
        .ok_or_else(|| "missing total bytes column".to_string())?
        .parse::<u64>()
        .map_err(|e| format!("parse total bytes: {e}"))?;
    let exclusive_bytes = cols
        .next()
        .ok_or_else(|| "missing exclusive bytes column".to_string())?
        .parse::<u64>()
        .map_err(|e| format!("parse exclusive bytes: {e}"))?;
    Ok(Some(BtrfsDiskUsage {
        exclusive_bytes,
        total_bytes,
    }))
}
