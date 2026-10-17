//! Opt-in backing for newly materialized templates. Ready cache entries are immutable.

use std::fs::File;
use std::io;
use std::path::Path;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum RootfsBackingPolicy {
    /// Preserve the filesystem's normal allocation policy.
    #[default]
    Inherit,
    NoCow,
}

#[derive(Debug, thiserror::Error)]
#[error("SPACE_TEMPLATE_ROOTFS_BACKING_POLICY must be inherit or nocow")]
pub struct RootfsBackingConfigError;

impl std::str::FromStr for RootfsBackingPolicy {
    type Err = RootfsBackingConfigError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "inherit" => Ok(Self::Inherit),
            "nocow" => Ok(Self::NoCow),
            _ => Err(RootfsBackingConfigError),
        }
    }
}

impl std::fmt::Display for RootfsBackingPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl RootfsBackingPolicy {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Inherit => "inherit",
            Self::NoCow => "nocow",
        }
    }

    pub(crate) fn from_env() -> Result<Self, RootfsBackingConfigError> {
        match std::env::var("SPACE_TEMPLATE_ROOTFS_BACKING_POLICY") {
            Ok(value) => value.parse(),
            Err(std::env::VarError::NotPresent) => Ok(Self::default()),
            Err(std::env::VarError::NotUnicode(_)) => Err(RootfsBackingConfigError),
        }
    }

    /// Apply before the first write: btrfs cannot change data checksum policy
    /// on an already populated inode. Never use this on a published parent.
    pub(crate) fn prepare_empty(self, file: &File) -> io::Result<()> {
        match self {
            Self::Inherit => Ok(()),
            Self::NoCow => pplx_space_common_io::reflink::set_nocow_on_empty(file),
        }
    }

    /// Check before serving pulls, rather than retrying every artifact location
    /// for a cache filesystem that cannot honor the configured policy.
    pub(crate) fn validate_cache(self, root: &Path) -> io::Result<()> {
        match self {
            Self::Inherit => Ok(()),
            Self::NoCow => {
                pplx_space_common_io::reflink::validate_nocow_filesystem(&File::open(root)?)
            }
        }
    }

    /// Legacy raw artifacts have already been downloaded. Rewrite once in the
    /// unpublished staging directory, not once per guest or in a ready cache.
    pub(crate) fn prepare_raw(self, path: &Path) -> io::Result<()> {
        if self == Self::Inherit {
            return Ok(());
        }
        let mut source = File::open(path)?;
        pplx_space_common_io::reflink::validate_nocow_filesystem(&source)?;
        #[cfg(target_os = "linux")]
        if rustix::fs::ioctl_getflags(&source)?.contains(rustix::fs::IFlags::NOCOW) {
            return Ok(());
        }
        let parent = path.parent().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "rootfs has no parent directory",
            )
        })?;
        let temporary = tempfile::NamedTempFile::new_in(parent)?;
        self.prepare_empty(temporary.as_file())?;
        let mut writer = pplx_space_common_io::sparse::SparseFileWriter::new(
            temporary.as_file().try_clone()?,
            64 * 1024,
        )?;
        io::copy(&mut source, &mut writer)?;
        let output = writer.finish()?;
        output.set_permissions(source.metadata()?.permissions())?;
        // Cache artifacts are revalidated before registration. Preserve the
        // existing atomic-publication contract without adding per-create fsync.
        temporary.persist(path).map_err(|error| error.error)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
