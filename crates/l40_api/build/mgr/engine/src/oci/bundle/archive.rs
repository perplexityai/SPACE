use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Stdio;

use super::dependencies::bundle_entries;
use super::install::{prepare_target_directory, replace_target_directory};
use super::{
    GuestBundleError, GuestBundleOperation, REQUIRED_PATHS, normalize_relative_path, path_arg,
    privileged_command, run_status,
};

pub struct GuestBundle {
    archive: tempfile::NamedTempFile,
    managed_directories: Vec<PathBuf>,
}

impl GuestBundle {
    pub fn capture(seed_root: &Path, cache_dir: &Path) -> Result<Self, GuestBundleError> {
        Self::capture_with_privilege(seed_root, cache_dir, true)
    }

    pub(super) fn capture_with_privilege(
        seed_root: &Path,
        cache_dir: &Path,
        as_root: bool,
    ) -> Result<Self, GuestBundleError> {
        for relative in REQUIRED_PATHS {
            let path = seed_root.join(relative);
            if !path.exists() && !path.is_symlink() {
                return Err(GuestBundleError::MissingPath { path });
            }
        }

        let entries = bundle_entries(seed_root, as_root)?;
        let mut managed_directories = Vec::new();
        for entry in &entries {
            let path = seed_root.join(entry);
            let metadata = fs::symlink_metadata(&path).map_err(|source| GuestBundleError::Io {
                operation: GuestBundleOperation::Inspect,
                path,
                source,
            })?;
            if metadata.is_dir() && !metadata.file_type().is_symlink() {
                managed_directories.push(entry.clone());
            }
        }
        let archive = tempfile::Builder::new()
            .prefix("space-guest-bundle-")
            .suffix(".tar")
            .tempfile_in(cache_dir)
            .map_err(|source| GuestBundleError::Io {
                operation: GuestBundleOperation::Archive,
                path: cache_dir.to_path_buf(),
                source,
            })?;
        let output = archive.reopen().map_err(|source| GuestBundleError::Io {
            operation: GuestBundleOperation::Archive,
            path: archive.path().to_path_buf(),
            source,
        })?;
        let mut command = privileged_command("tar", as_root);
        command.args(["--create", "--file=-"]);
        if as_root {
            command.args([
                "--numeric-owner",
                "--xattrs",
                "--xattrs-include=*",
                "--acls",
            ]);
        }
        command.arg("--directory");
        command.arg(seed_root).args(entries).stdout(output);
        run_status(GuestBundleOperation::Archive, &mut command)?;

        Ok(Self {
            archive,
            managed_directories,
        })
    }

    pub fn extract(&self, target_root: &Path) -> Result<(), GuestBundleError> {
        self.extract_with_privilege(target_root, true)
    }

    pub(super) fn extract_with_privilege(
        &self,
        target_root: &Path,
        as_root: bool,
    ) -> Result<(), GuestBundleError> {
        super::install::validate_target_root(target_root)?;
        for directory in &self.managed_directories {
            replace_target_directory(
                target_root,
                directory,
                GuestBundleOperation::Extract,
                as_root,
            )?;
        }
        for directory in self.archive_directories()? {
            prepare_target_directory(
                target_root,
                &directory,
                GuestBundleOperation::Extract,
                as_root,
            )?;
        }

        let mut command = privileged_command("tar", as_root);
        command.args([
            "--extract",
            "--file",
            path_arg(self.archive.path()).as_str(),
            "--directory",
            path_arg(target_root).as_str(),
        ]);
        #[cfg(target_os = "linux")]
        command.arg("--keep-directory-symlink");
        if as_root {
            command.args([
                "--overwrite",
                "--same-owner",
                "--preserve-permissions",
                "--numeric-owner",
                "--xattrs",
                "--xattrs-include=*",
                "--acls",
            ]);
        }
        command.stdin(Stdio::null());
        run_status(GuestBundleOperation::Extract, &mut command)
    }

    fn archive_directories(&self) -> Result<BTreeSet<PathBuf>, GuestBundleError> {
        let file = self
            .archive
            .reopen()
            .map_err(|source| GuestBundleError::Io {
                operation: GuestBundleOperation::Extract,
                path: self.archive.path().to_path_buf(),
                source,
            })?;
        let mut archive = tar::Archive::new(file);
        let entries = archive.entries().map_err(|source| GuestBundleError::Io {
            operation: GuestBundleOperation::Extract,
            path: self.archive.path().to_path_buf(),
            source,
        })?;
        let mut directories = BTreeSet::new();
        for entry in entries {
            let entry = entry.map_err(|source| GuestBundleError::Io {
                operation: GuestBundleOperation::Extract,
                path: self.archive.path().to_path_buf(),
                source,
            })?;
            let is_directory = entry.header().entry_type().is_dir();
            let path = entry.path().map_err(|source| GuestBundleError::Io {
                operation: GuestBundleOperation::Extract,
                path: self.archive.path().to_path_buf(),
                source,
            })?;
            let path = normalize_relative_path(&path)?;
            if is_directory {
                directories.insert(path.clone());
            }
            let mut parent = path.parent();
            while let Some(path) = parent.filter(|path| !path.as_os_str().is_empty()) {
                directories.insert(path.to_path_buf());
                parent = path.parent();
            }
        }
        Ok(directories)
    }
}
