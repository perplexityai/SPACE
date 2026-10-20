use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use oci_spec::distribution::Reference as OciReference;

use super::bundle::{GuestBundle, GuestBundleError, install_current_guest};
use super::containerd::{OciBuildError, OciMountRequest, mount};
use crate::command_as_root;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RootfsAssemblyOperation {
    CreateWorkspace,
    MountSeed,
    UnmountSeed,
    InitializeRootfs,
    FormatRootfs,
    MountRootfs,
    CopyOciRootfs,
    UnmountRootfs,
    CheckRootfs,
}

impl RootfsAssemblyOperation {
    const fn as_str(self) -> &'static str {
        match self {
            Self::CreateWorkspace => "create_workspace",
            Self::MountSeed => "mount_seed",
            Self::UnmountSeed => "unmount_seed",
            Self::InitializeRootfs => "initialize_rootfs",
            Self::FormatRootfs => "format_rootfs",
            Self::MountRootfs => "mount_rootfs",
            Self::CopyOciRootfs => "copy_oci_rootfs",
            Self::UnmountRootfs => "unmount_rootfs",
            Self::CheckRootfs => "check_rootfs",
        }
    }
}

impl std::fmt::Display for RootfsAssemblyOperation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RootfsAssemblyError {
    #[error(transparent)]
    Bundle(#[from] GuestBundleError),
    #[error("{operation} failed for {path}: {source}")]
    Io {
        operation: RootfsAssemblyOperation,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("{operation} exited unsuccessfully with code {code:?}")]
    Command {
        operation: RootfsAssemblyOperation,
        code: Option<i32>,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum OciImageAssemblyError {
    #[error(transparent)]
    Containerd(#[from] OciBuildError),
    #[error(transparent)]
    Rootfs(#[from] RootfsAssemblyError),
    #[error("rootfs assembly task failed: {0}")]
    Task(#[source] tokio::task::JoinError),
}

impl OciImageAssemblyError {
    /// The bounded cause metrics report for a failed image assembly.
    pub fn error_kind(&self) -> crate::TemplateBuildErrorKind {
        match self {
            Self::Containerd(error) => error.error_kind(),
            Self::Rootfs(_) => crate::TemplateBuildErrorKind::Unpack,
            Self::Task(_) => crate::TemplateBuildErrorKind::Other,
        }
    }
}

pub struct OciImageAssemblyRequest {
    pub seed_rootfs: PathBuf,
    pub target_rootfs: PathBuf,
    pub target_rootfs_size_mib: u32,
    pub reference: OciReference,
    pub cache_dir: PathBuf,
    pub guest_bin_dir: PathBuf,
    pub bundle_version: String,
}

struct RootfsAssemblyRequest<'a> {
    pub seed_rootfs: &'a Path,
    pub target_rootfs: &'a Path,
    pub target_rootfs_size_mib: u32,
    pub oci_rootfs: &'a Path,
    pub cache_dir: &'a Path,
    pub guest_bin_dir: &'a Path,
    pub bundle_version: &'a str,
}

fn assemble(request: RootfsAssemblyRequest<'_>) -> Result<(), RootfsAssemblyError> {
    let workspace = tempfile::Builder::new()
        .prefix("space-oci-assembly-")
        .tempdir_in(request.cache_dir)
        .map_err(|source| RootfsAssemblyError::Io {
            operation: RootfsAssemblyOperation::CreateWorkspace,
            path: request.cache_dir.to_path_buf(),
            source,
        })?;
    let seed_mount = workspace.path().join("seed");
    let target_mount = workspace.path().join("target");
    fs::create_dir(&seed_mount).map_err(|source| RootfsAssemblyError::Io {
        operation: RootfsAssemblyOperation::CreateWorkspace,
        path: seed_mount.clone(),
        source,
    })?;
    fs::create_dir(&target_mount).map_err(|source| RootfsAssemblyError::Io {
        operation: RootfsAssemblyOperation::CreateWorkspace,
        path: target_mount.clone(),
        source,
    })?;

    let bundle = with_mounted_ext4(
        request.seed_rootfs,
        &seed_mount,
        true,
        RootfsAssemblyOperation::MountSeed,
        RootfsAssemblyOperation::UnmountSeed,
        |root| GuestBundle::capture(root, workspace.path()).map_err(RootfsAssemblyError::from),
    )?;

    initialize_rootfs(request.target_rootfs, request.target_rootfs_size_mib)?;
    format_ext4(request.target_rootfs)?;
    with_mounted_ext4(
        request.target_rootfs,
        &target_mount,
        false,
        RootfsAssemblyOperation::MountRootfs,
        RootfsAssemblyOperation::UnmountRootfs,
        |root| {
            copy_tree(request.oci_rootfs, root)?;
            bundle.extract(root)?;
            install_current_guest(root, request.guest_bin_dir, request.bundle_version)?;
            Ok(())
        },
    )?;
    check_ext4(request.target_rootfs)
}

pub async fn assemble_from_image(
    request: OciImageAssemblyRequest,
) -> Result<(), OciImageAssemblyError> {
    let oci_rootfs = mount(OciMountRequest {
        cache_dir: request.cache_dir.clone(),
        reference: request.reference,
    })
    .await?;
    let oci_rootfs_path = oci_rootfs.path().to_path_buf();
    let assembly_result = tokio::task::spawn_blocking(move || {
        assemble(RootfsAssemblyRequest {
            seed_rootfs: &request.seed_rootfs,
            target_rootfs: &request.target_rootfs,
            target_rootfs_size_mib: request.target_rootfs_size_mib,
            oci_rootfs: &oci_rootfs_path,
            cache_dir: &request.cache_dir,
            guest_bin_dir: &request.guest_bin_dir,
            bundle_version: &request.bundle_version,
        })
    })
    .await
    .map_err(OciImageAssemblyError::Task)
    .and_then(|result| result.map_err(OciImageAssemblyError::Rootfs));
    let cleanup_result = oci_rootfs.unmount().await;

    match assembly_result {
        Err(error) => {
            if let Err(cleanup_error) = cleanup_result {
                tracing::warn!(error = %cleanup_error, "OCI image unmount also failed");
            }
            Err(error)
        }
        Ok(()) => {
            cleanup_result?;
            Ok(())
        }
    }
}

fn initialize_rootfs(path: &Path, size_mib: u32) -> Result<(), RootfsAssemblyError> {
    let parent = path.parent().ok_or_else(|| RootfsAssemblyError::Io {
        operation: RootfsAssemblyOperation::InitializeRootfs,
        path: path.to_path_buf(),
        source: std::io::Error::new(std::io::ErrorKind::InvalidInput, "rootfs has no parent"),
    })?;
    fs::create_dir_all(parent).map_err(|source| RootfsAssemblyError::Io {
        operation: RootfsAssemblyOperation::InitializeRootfs,
        path: parent.to_path_buf(),
        source,
    })?;
    let file = fs::File::create(path).map_err(|source| RootfsAssemblyError::Io {
        operation: RootfsAssemblyOperation::InitializeRootfs,
        path: path.to_path_buf(),
        source,
    })?;
    file.set_len(u64::from(size_mib) * 1024 * 1024)
        .map_err(|source| RootfsAssemblyError::Io {
            operation: RootfsAssemblyOperation::InitializeRootfs,
            path: path.to_path_buf(),
            source,
        })
}

fn with_mounted_ext4<T>(
    image: &Path,
    mountpoint: &Path,
    read_only: bool,
    mount_operation: RootfsAssemblyOperation,
    unmount_operation: RootfsAssemblyOperation,
    operation: impl FnOnce(&Path) -> Result<T, RootfsAssemblyError>,
) -> Result<T, RootfsAssemblyError> {
    let mut mount = command_as_root("mount");
    mount
        .arg("--options")
        .arg(if read_only { "loop,ro" } else { "loop,rw" })
        .arg(image)
        .arg(mountpoint);
    run_status(mount_operation, &mut mount, &[])?;

    let result = operation(mountpoint);
    let mut unmount = command_as_root("umount");
    unmount.arg(mountpoint);
    let unmount_result = run_status(unmount_operation, &mut unmount, &[]);
    match result {
        Err(error) => {
            if let Err(unmount_error) = unmount_result {
                tracing::warn!(error = %unmount_error, "rootfs unmount also failed");
            }
            Err(error)
        }
        Ok(value) => {
            unmount_result?;
            Ok(value)
        }
    }
}

fn format_ext4(image: &Path) -> Result<(), RootfsAssemblyError> {
    let mut command = command_as_root("mkfs.ext4");
    command.args([
        "-F",
        "-O",
        "dir_nlink,large_dir,inline_data",
        "-J",
        "size=4",
        "-m",
        "1",
    ]);
    command.arg(image);
    run_status(RootfsAssemblyOperation::FormatRootfs, &mut command, &[])
}

fn copy_tree(source: &Path, target: &Path) -> Result<(), RootfsAssemblyError> {
    let mut archive = command_as_root("tar");
    archive.args([
        "--create",
        "--file=-",
        "--numeric-owner",
        "--xattrs",
        "--xattrs-include=*",
        "--acls",
        "--sparse",
        "--one-file-system",
        "--directory",
    ]);
    archive
        .arg(source)
        .arg(".")
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    let mut archive = archive.spawn().map_err(|error| RootfsAssemblyError::Io {
        operation: RootfsAssemblyOperation::CopyOciRootfs,
        path: source.to_path_buf(),
        source: error,
    })?;
    let archive_stdout = archive.stdout.take().expect("OCI archive stdout is piped");

    let mut extract = command_as_root("tar");
    extract.args([
        "--extract",
        "--file=-",
        "--same-owner",
        "--preserve-permissions",
        "--numeric-owner",
        "--xattrs",
        "--xattrs-include=*",
        "--acls",
        "--overwrite",
        "--directory",
    ]);
    extract
        .arg(target)
        .stdin(Stdio::from(archive_stdout))
        .stdout(Stdio::inherit())
        .stderr(Stdio::null());
    let extract_status = match extract.status() {
        Ok(status) => status,
        Err(source) => {
            let _ = archive.kill();
            let _ = archive.wait();
            return Err(RootfsAssemblyError::Io {
                operation: RootfsAssemblyOperation::CopyOciRootfs,
                path: target.to_path_buf(),
                source,
            });
        }
    };
    drop(extract);
    let archive_status = archive.wait().map_err(|error| RootfsAssemblyError::Io {
        operation: RootfsAssemblyOperation::CopyOciRootfs,
        path: source.to_path_buf(),
        source: error,
    })?;
    if !extract_status.success() {
        return Err(RootfsAssemblyError::Command {
            operation: RootfsAssemblyOperation::CopyOciRootfs,
            code: extract_status.code(),
        });
    }
    if !archive_status.success() {
        return Err(RootfsAssemblyError::Command {
            operation: RootfsAssemblyOperation::CopyOciRootfs,
            code: archive_status.code(),
        });
    }
    Ok(())
}

fn check_ext4(image: &Path) -> Result<(), RootfsAssemblyError> {
    let mut command = command_as_root("e2fsck");
    command.args(["-fy"]).arg(image);
    run_status(RootfsAssemblyOperation::CheckRootfs, &mut command, &[1])
}

fn run_status(
    operation: RootfsAssemblyOperation,
    command: &mut Command,
    accepted_codes: &[i32],
) -> Result<(), RootfsAssemblyError> {
    let status = command.status().map_err(|source| RootfsAssemblyError::Io {
        operation,
        path: PathBuf::new(),
        source,
    })?;
    if status.success()
        || status
            .code()
            .is_some_and(|code| accepted_codes.contains(&code))
    {
        Ok(())
    } else {
        Err(RootfsAssemblyError::Command {
            operation,
            code: status.code(),
        })
    }
}

#[cfg(test)]
mod tests;
