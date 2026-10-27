//! Verified chunks are extents of completed, read-only template images. A
//! persistent lease prevents every participating deletion path from changing
//! the source while a pinned descriptor is used by assembly.

use pplx_space_common_core::{
    host::btrfs::{ReadonlySubvolumeIdentity, readonly_subvolume_identity},
    types::{Digest, TEMPLATE_ROOTFS_FILE, TemplateName},
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::{fs::File, io, path::Path};

pub mod store;

#[derive(Debug, thiserror::Error)]
pub enum LeaseError {
    #[error("verified-source database mutex poisoned")]
    Lock,
    #[error("verified-source registry: {0}")]
    Store(#[from] rusqlite::Error),
}

pub struct LeaseSet {
    database: Arc<Mutex<rusqlite::Connection>>,
    selection: store::Selection,
}

impl LeaseSet {
    pub fn acquire(
        database: Arc<Mutex<rusqlite::Connection>>,
        manifest: &super::delivery::AuthenticatedManifest,
    ) -> Result<Arc<Self>, LeaseError> {
        let selection = store::select_and_lease(
            &mut *database.lock().map_err(|_| LeaseError::Lock)?,
            manifest,
        )?;
        Ok(Arc::new(Self {
            database,
            selection,
        }))
    }

    pub fn pin(self: &Arc<Self>, root: &Path) -> Result<BTreeMap<Digest, LeasedChunk>, LeaseError> {
        let mut pinned = BTreeMap::<uuid::Uuid, Arc<PinnedSource>>::new();
        let mut invalid = BTreeSet::new();
        let mut chunks = BTreeMap::new();
        for candidate in &self.selection.candidates {
            if chunks.contains_key(&candidate.digest) || invalid.contains(&candidate.source_id) {
                continue;
            }
            let source = if let Some(source) = pinned.get(&candidate.source_id) {
                Arc::clone(source)
            } else {
                let opened = PinnedSource::open(root, &candidate.template, candidate.rootfs)
                    .and_then(|source| {
                        source.validate(candidate.identity)?;
                        Ok(Arc::new(source))
                    });
                match opened {
                    Ok(source) => {
                        pinned.insert(candidate.source_id, Arc::clone(&source));
                        source
                    }
                    Err(error) => {
                        tracing::warn!(source_id = %candidate.source_id, %error, "excluding unavailable or changed verified source");
                        invalid.insert(candidate.source_id);
                        store::invalidate(
                            &*self.database.lock().map_err(|_| LeaseError::Lock)?,
                            candidate.source_id,
                        )?;
                        continue;
                    }
                }
            };
            if candidate.length == 0
                || candidate
                    .offset
                    .checked_add(candidate.length)
                    .is_none_or(|end| end > source.identity.length)
            {
                invalid.insert(candidate.source_id);
                store::invalidate(
                    &*self.database.lock().map_err(|_| LeaseError::Lock)?,
                    candidate.source_id,
                )?;
                continue;
            }
            chunks.insert(
                candidate.digest,
                LeasedChunk {
                    source,
                    offset: candidate.offset,
                    length: candidate.length,
                    _lease: Arc::clone(self),
                },
            );
        }
        Ok(chunks)
    }
}

impl Drop for LeaseSet {
    fn drop(&mut self) {
        let result = self
            .database
            .lock()
            .map_err(|_| LeaseError::Lock)
            .and_then(|connection| {
                store::release(&connection, self.selection.lease_id).map_err(LeaseError::from)
            });
        if let Err(error) = result {
            // Retaining a failed-to-release durable lease prevents deletion;
            // exclusive startup can clear it after every old reader is gone.
            tracing::error!(lease_id = %self.selection.lease_id, %error, "could not release verified-source lease");
        }
    }
}

pub struct LeasedChunk {
    source: Arc<PinnedSource>,
    offset: u64,
    length: u64,
    _lease: Arc<LeaseSet>,
}

impl LeasedChunk {
    pub const fn length(&self) -> u64 {
        self.length
    }
    pub fn copy_to(&self, output: &File, offset: u64) -> io::Result<bool> {
        self.source.validate(self.source.identity)?;
        let cloned = pplx_space_common_io::reflink::copy_range_on_descriptors(
            self.source.image(),
            self.offset,
            output,
            offset,
            self.length,
        )?;
        self.source.validate(self.source.identity)?;
        Ok(cloned)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SourceIdentity {
    pub subvolume: ReadonlySubvolumeIdentity,
    pub device: u64,
    pub inode: u64,
    pub length: u64,
    pub ctime_seconds: i64,
    pub ctime_nanos: i64,
}

/// Both handles stay open until the assembly releases its source lease. The
/// comparison uses kernel identity and immutable backing, not path timestamps.
pub struct PinnedSource {
    root: std::path::PathBuf,
    name: TemplateName,
    digest: Digest,
    directory: File,
    image: File,
    identity: SourceIdentity,
}

impl PinnedSource {
    pub fn open(root: &Path, name: &TemplateName, digest: Digest) -> io::Result<Self> {
        use rustix::fs::{Mode, OFlags, open, openat};
        let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
        let root_path = root.to_path_buf();
        let root = open(root, flags, Mode::empty())?;
        let template = openat(&root, name.as_str(), flags, Mode::empty())?;
        let directory = File::from(openat(&template, digest.to_string(), flags, Mode::empty())?);
        let image = File::from(openat(
            &directory,
            TEMPLATE_ROOTFS_FILE,
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )?);
        let identity = identify(&directory, &image)?;
        Ok(Self {
            root: root_path,
            name: name.clone(),
            digest,
            directory,
            image,
            identity,
        })
    }

    pub const fn identity(&self) -> SourceIdentity {
        self.identity
    }
    pub fn image(&self) -> &File {
        &self.image
    }

    pub fn validate(&self, expected: SourceIdentity) -> io::Result<()> {
        if expected != self.identity || identify(&self.directory, &self.image)? != expected {
            return Err(io::Error::other(
                "verified source identity or immutable backing changed",
            ));
        }
        Ok(())
    }

    /// Admission holds the owning SQLite mutex while comparing the canonical
    /// path as well as the pinned handles. A concurrent import cannot publish
    /// a registry entry for a directory it has just replaced.
    pub fn validate_admission_path(&self) -> io::Result<()> {
        self.validate(self.identity)?;
        Self::open(&self.root, &self.name, self.digest)?.validate(self.identity)
    }
}

fn identify(directory: &File, image: &File) -> io::Result<SourceIdentity> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let subvolume = readonly_subvolume_identity(directory)?;
        let metadata = image.metadata()?;
        if !metadata.is_file()
            || metadata.dev() != directory.metadata()?.dev()
            || metadata.len() == 0
        {
            return Err(io::Error::other(
                "rootfs must be a nonempty regular file inside its read-only subvolume",
            ));
        }
        Ok(SourceIdentity {
            subvolume,
            device: metadata.dev(),
            inode: metadata.ino(),
            length: metadata.len(),
            ctime_seconds: metadata.ctime(),
            ctime_nanos: metadata.ctime_nsec(),
        })
    }
    #[cfg(not(unix))]
    {
        let _ = (directory, image);
        Err(io::ErrorKind::Unsupported.into())
    }
}
