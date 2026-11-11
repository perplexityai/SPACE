//! `/proc/<pid>/exe` readlink targets and content-addressed install names.

use std::path::{Path, PathBuf};

/// Marker the kernel appends to a `/proc/<pid>/exe` readlink target
/// when the backing inode was unlinked while the process kept running.
const EXE_DELETED_MARKER: &str = " (deleted)";

/// Strip a trailing kernel ` (deleted)` marker from a `/proc/<pid>/exe`
/// readlink target. The marker appears when a binary is replaced in place
/// while a process still runs it; a content-addressed install that writes a
/// fresh file and repoints a symlink never produces it for the running
/// inode. Conservative: only the literal trailing marker goes.
pub fn strip_exe_deleted_marker(path: &Path) -> PathBuf {
    if let Some(s) = path.to_str()
        && let Some(stripped) = s.strip_suffix(EXE_DELETED_MARKER)
    {
        return PathBuf::from(stripped);
    }
    path.to_path_buf()
}

/// Whether a raw `/proc/<pid>/exe` readlink target carries the kernel's
/// trailing ` (deleted)` marker — the backing inode was unlinked while
/// the process kept running.
pub fn exe_link_was_deleted(path: &Path) -> bool {
    path.to_str()
        .is_some_and(|s| s.ends_with(EXE_DELETED_MARKER))
}

/// True when `file_name` is `binary`'s content-addressed install name,
/// `<binary>-<sha256[:8]>` (exactly 8 lowercase hex).
pub fn is_content_addressed_binary_file_name(file_name: &str, binary: &str) -> bool {
    let Some(digest) = file_name
        .strip_prefix(binary)
        .and_then(|rest| rest.strip_prefix('-'))
    else {
        return false;
    };
    digest.len() == 8 && digest.chars().all(|c| matches!(c, '0'..='9' | 'a'..='f'))
}

#[cfg(test)]
mod tests;
