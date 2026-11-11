use super::*;
use aws_lc_rs::digest::{Context, SHA256};
use pplx_space_spaced_update::BinaryIdentity;

pub(crate) fn directory(path: &Path) -> Result<(), UpdateError> {
    let metadata = fs::symlink_metadata(path)?;
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        if metadata.file_attributes()
            & windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT
            != 0
        {
            return Err(UpdateError::Layout);
        }
    }
    if !metadata.is_dir() {
        return Err(UpdateError::Layout);
    }
    Ok(())
}

#[cfg(unix)]
pub(crate) fn open_regular(path: &Path) -> Result<File, UpdateError> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    if !file.metadata()?.is_file() {
        return Err(UpdateError::Layout);
    }
    Ok(file)
}

pub(crate) fn read_bounded(path: &Path) -> Result<Vec<u8>, UpdateError> {
    let mut bytes = Vec::new();
    open_regular(path)?
        .take((MAX_METADATA_BYTES + 1) as u64)
        .read_to_end(&mut bytes)?;
    if bytes.len() > MAX_METADATA_BYTES {
        return Err(UpdateError::TooLarge);
    }
    Ok(bytes)
}

pub(crate) fn remove_file_if_present(path: &Path) -> Result<(), UpdateError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
    }
}

pub(crate) fn verify_binary(path: &Path, expected: &BinaryIdentity) -> Result<(), UpdateError> {
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    let mut file = open_regular(path)?;
    let metadata = file.metadata()?;
    if metadata.len() != expected.length {
        return Err(UpdateError::Identity);
    }
    #[cfg(unix)]
    if metadata.permissions().mode() & 0o111 == 0 {
        return Err(UpdateError::Identity);
    }
    let mut context = Context::new(&SHA256);
    let mut buffer = [0u8; 64 * 1024];
    let mut remaining = expected.length;
    while remaining > 0 {
        let limit = usize::try_from(remaining.min(buffer.len() as u64))
            .map_err(|_| UpdateError::TooLarge)?;
        let read = file.read(buffer.get_mut(..limit).ok_or(UpdateError::TooLarge)?)?;
        if read == 0 {
            return Err(UpdateError::Identity);
        }
        context.update(buffer.get(..read).ok_or(UpdateError::TooLarge)?);
        remaining -= read as u64;
    }
    if file.read(&mut buffer[..1])? != 0 {
        return Err(UpdateError::Identity);
    }
    let mut bytes = [0; 32];
    bytes.copy_from_slice(context.finish().as_ref());
    let digest = ArtifactDigest::try_from(pplx_space_content_digest::Digest::sha256(bytes))?;
    if digest != expected.digest {
        return Err(UpdateError::Identity);
    }
    Ok(())
}

#[cfg(unix)]
pub(crate) fn create(path: &Path, executable: bool) -> Result<File, UpdateError> {
    Ok(OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(if executable { 0o700 } else { 0o600 })
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?)
}
#[cfg(unix)]
pub(crate) fn replace(from: &Path, to: &Path) -> Result<(), UpdateError> {
    fs::rename(from, to)?;
    Ok(())
}
#[cfg(unix)]
pub(crate) fn sync_directory(path: &Path) -> Result<(), UpdateError> {
    File::open(path)?.sync_all()?;
    Ok(())
}
pub(crate) fn binary_name(binary: Binary) -> &'static str {
    PLATFORM.binary_name(binary)
}
#[cfg(windows)]
pub(crate) use super::windows::{create, open_regular, replace, sync_directory};
