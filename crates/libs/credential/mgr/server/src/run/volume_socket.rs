use std::fs;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{FileTypeExt, MetadataExt};
use std::os::unix::net::{SocketAddr, UnixListener};
use std::path::{Component, Path};

use pplx_space_common_svc::core::host::linux::identities::{gids, group_names, users};
use pplx_space_common_svc::{SocketGroup, UnixSocketOptions, apply_unix_socket_permissions};

pub(super) fn options() -> UnixSocketOptions {
    UnixSocketOptions::new(0o660)
        .with_group(SocketGroup::Required(group_names::SPACE_VOLCREDCLIENTS))
}

fn trusted_parent(path: &Path, allow_installation_link: bool) -> io::Result<()> {
    if path
        .components()
        .any(|part| matches!(part, Component::ParentDir))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "directory path contains parent traversal",
        ));
    }
    for ancestor in path.ancestors().collect::<Vec<_>>().into_iter().rev() {
        let metadata = fs::symlink_metadata(ancestor)?;
        // The installation root may be an administrator-provisioned redirect;
        // descendants must not redirect the credential directory elsewhere.
        if allow_installation_link
            && ancestor == Path::new("/space")
            && metadata.is_symlink()
            && metadata.uid() == 0
        {
            // Validate the unresolved target: canonicalization could hide a
            // replaceable intermediate link in an otherwise protected path.
            trusted_parent(&Path::new("/").join(fs::read_link(ancestor)?), false)?;
        } else if !metadata.is_dir() || metadata.uid() != 0 || metadata.mode() & 0o022 != 0 {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!(
                    "{} must be a root-owned directory without group or other write access",
                    ancestor.display()
                ),
            ));
        }
    }
    Ok(())
}

pub(super) fn preflight(path: &Path, standard_socket: &Path) -> io::Result<()> {
    if !path.is_absolute()
        || path
            .components()
            .any(|part| matches!(part, Component::ParentDir))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "volume socket path must be absolute without parent traversal",
        ));
    }
    // A short probe name must not hide an invalid production pathname.
    SocketAddr::from_pathname(path)?;
    let name = path.file_name().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "volume socket path must name a file",
        )
    })?;
    let directory = path.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "volume socket requires a directory",
        )
    })?;
    let parent = directory.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "volume socket requires a dedicated directory",
        )
    })?;
    trusted_parent(parent, true)?;
    let metadata = fs::symlink_metadata(directory)?;
    if !metadata.is_dir()
        || metadata.uid() != users::SPACE_SBXMGR.uid
        || metadata.gid() != gids::SPACE_VOLCREDCLIENTS
        || metadata.mode() & 0o7777 != 0o750
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "{} must be a directory owned by {}:{} with mode 0750 (found {}:{} {:o})",
                directory.display(),
                users::SPACE_SBXMGR.uid,
                gids::SPACE_VOLCREDCLIENTS,
                metadata.uid(),
                metadata.gid(),
                metadata.mode() & 0o7777,
            ),
        ));
    }
    // Both binders replace existing sockets. Compare the directory identities
    // before either bind, including when the socket itself does not exist yet.
    if standard_socket.file_name() == Some(name) {
        let standard_parent = standard_socket
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        match fs::metadata(standard_parent) {
            Ok(standard)
                if standard.dev() == metadata.dev() && standard.ino() == metadata.ino() =>
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "standard and volume credential sockets name the same filesystem entry",
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_socket() => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
        Ok(_) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "existing volume socket path is not a socket",
            ));
        }
    }

    for _ in 0..64 {
        let nonce = uuid::Uuid::now_v7().simple().to_string();
        let length = name.as_bytes().len().min(nonce.len());
        let prefix = if length > 3 { ".p-" } else { "" };
        let probe = directory.join(format!(
            "{prefix}{}",
            nonce
                .chars()
                .rev()
                .take(length - prefix.len())
                .collect::<String>()
        ));
        if probe == path {
            continue;
        }
        // The shared production binder can replace an existing socket. A
        // probe must own a fresh name before applying permissions or removing it.
        let listener = match UnixListener::bind(&probe) {
            Ok(listener) => listener,
            Err(error) if error.kind() == io::ErrorKind::AddrInUse => continue,
            Err(error) => return Err(error),
        };
        let result = apply_unix_socket_permissions(&probe, options())
            .map_err(io::Error::other)
            .and_then(|()| {
                let metadata = fs::symlink_metadata(&probe)?;
                if metadata.uid() != users::SPACE_SBXMGR.uid
                    || metadata.gid() != gids::SPACE_VOLCREDCLIENTS
                    || metadata.mode() & 0o7777 != 0o660
                {
                    return Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        "probe socket identity or permissions do not match the volume listener",
                    ));
                }
                Ok(())
            });
        let cleanup = fs::remove_file(&probe);
        drop(listener);
        if let Err(error) = cleanup {
            if let Err(source) = result {
                tracing::warn!(%source, "volume socket probe failed before cleanup also failed");
            }
            return Err(io::Error::new(
                error.kind(),
                format!("remove volume socket probe {}: {error}", probe.display()),
            ));
        }
        return result;
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "could not allocate a unique volume socket probe",
    ))
}

#[cfg(all(test, target_os = "linux"))]
mod tests;
