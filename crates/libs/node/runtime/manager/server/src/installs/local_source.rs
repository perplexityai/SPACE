//! Read-only deployment feeds. Bytes still pass bounded staging, catalog hashes
//! and independent node-manager publication; a feed never grants execution trust.
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use pplx_space_common_core::types::{Arch, RuntimeIdentity, RuntimeKind};
use tokio::io::AsyncReadExt as _;

use super::publisher::PinSource;
use super::release_source::{
    ReleaseChunks, ReleaseLocation, ReleasePayload, ReleaseSource, ReleaseSourceError,
};
use super::source::{
    ByteChunks, KernelLocation, KernelSource, RuntimeLine, SourceError, SourceKernel,
    VerifiableKernel,
};

/// An absolute local path encoded as a host-local file URI in durable jobs.
/// Keep this deliberately narrower than general URLs: no authority, escapes,
/// query, fragment, traversal or platform-dependent normalization.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct LocalSourcePath(PathBuf);

#[derive(Debug, thiserror::Error)]
#[error(
    "local artifact source must be an absolute path without traversal, URL escapes or whitespace: {0}"
)]
pub struct LocalSourcePathError(String);

impl LocalSourcePath {
    pub fn new(path: PathBuf) -> Result<Self, LocalSourcePathError> {
        let invalid = || LocalSourcePathError(path.display().to_string());
        let text = path.to_str().ok_or_else(invalid)?;
        if !text.starts_with('/')
            || text.starts_with("//")
            || text
                .bytes()
                .any(|b| b.is_ascii_whitespace() || b.is_ascii_control() || b"%?#\\".contains(&b))
            || text.split('/').any(|part| matches!(part, "." | ".."))
            || path
                .components()
                .any(|part| !matches!(part, Component::RootDir | Component::Normal(_)))
        {
            return Err(invalid());
        }
        Ok(Self(path))
    }
    pub fn as_path(&self) -> &Path {
        &self.0
    }
    fn join(&self, path: impl AsRef<Path>) -> Self {
        Self(self.0.join(path))
    }
}
impl std::str::FromStr for LocalSourcePath {
    type Err = LocalSourcePathError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let path = value
            .strip_prefix("file://")
            .ok_or_else(|| LocalSourcePathError(value.into()))?;
        Self::new(path.into())
    }
}
impl std::fmt::Display for LocalSourcePath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "file://{}", self.0.display())
    }
}

/// Operator-selected delivery root for one supported runtime. This is a source,
/// never the managed store or an execution path.
#[derive(Clone, Debug)]
pub struct LocalArtifactFeed {
    runtime: RuntimeKind,
    root: LocalSourcePath,
}

#[derive(Debug, thiserror::Error)]
pub enum LocalArtifactFeedError {
    #[error("expected <runtime>=<absolute-directory>")]
    Shape,
    #[error("local artifact delivery is not supported for runtime {0}")]
    Runtime(String),
    #[error(transparent)]
    Path(#[from] LocalSourcePathError),
}

impl std::str::FromStr for LocalArtifactFeed {
    type Err = LocalArtifactFeedError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (runtime, root) = value.split_once('=').ok_or(LocalArtifactFeedError::Shape)?;
        let kind = RuntimeKind::parse(runtime)
            .map_err(|_| LocalArtifactFeedError::Runtime(runtime.into()))?;
        if !matches!(
            kind,
            RuntimeKind::Firecracker | RuntimeKind::CloudHypervisor
        ) {
            return Err(LocalArtifactFeedError::Runtime(runtime.into()));
        }
        Ok(Self {
            runtime: kind,
            root: LocalSourcePath::new(root.into())?,
        })
    }
}
impl LocalArtifactFeed {
    pub fn runtime(&self) -> RuntimeKind {
        self.runtime
    }
}

pub(crate) struct LocalArtifactSource {
    runtime: RuntimeKind,
    root: LocalSourcePath,
    pins: Arc<dyn PinSource>,
}
impl LocalArtifactSource {
    pub(crate) fn new(feed: LocalArtifactFeed, pins: Arc<dyn PinSource>) -> Self {
        Self {
            runtime: feed.runtime,
            root: feed.root,
            pins,
        }
    }
    fn kernel_path(&self, line: RuntimeLine, arch: Arch, version: &str) -> LocalSourcePath {
        self.root.join(format!(
            "kernels/{}/{}/{arch}/{version}/vmlinux.bin",
            line.runtime, line.release
        ))
    }
    async fn read(&self, location: &LocalSourcePath) -> std::io::Result<tokio::fs::File> {
        let root = self.root.clone();
        let location = location.clone();
        tokio::task::spawn_blocking(move || open_regular(root.as_path(), location.as_path()))
            .await
            .map_err(std::io::Error::other)?
            .map(tokio::fs::File::from_std)
    }
}

// Open beneath one pinned root. No source member may traverse a symlink or
// block on a FIFO. The configured root itself is an operator-selected location.
fn open_regular(root: &Path, path: &Path) -> std::io::Result<std::fs::File> {
    use rustix::fs::{Mode, OFlags, openat};
    let relative = path.strip_prefix(root).map_err(std::io::Error::other)?;
    let mut parts = relative.components().peekable();
    let mut directory = std::fs::File::open(root)?;
    while let Some(Component::Normal(part)) = parts.next() {
        let flags = OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK;
        let file: std::fs::File = openat(
            &directory,
            part,
            if parts.peek().is_some() {
                flags | OFlags::DIRECTORY
            } else {
                flags
            },
            Mode::empty(),
        )?
        .into();
        if parts.peek().is_none() {
            if !file.metadata()?.is_file() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "source is not a regular file",
                ));
            }
            return Ok(file);
        }
        directory = file;
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        "source is not a child file",
    ))
}

#[async_trait::async_trait]
impl ReleaseSource for LocalArtifactSource {
    fn runtime(&self) -> RuntimeKind {
        self.runtime
    }
    fn root(&self) -> ReleaseLocation {
        ReleaseLocation::Local(self.root.clone())
    }
    fn archive(&self, release: &RuntimeIdentity, arch: Arch) -> ReleaseLocation {
        ReleaseLocation::Local(self.root.join(format!(
            "releases/{}/{}/{arch}/payload",
            release.kind, release.version
        )))
    }
    fn payload(&self, release: &RuntimeIdentity, architecture: Arch) -> ReleasePayload {
        super::release_source::release_payload(release, architecture)
    }
    async fn open(&self, archive: &ReleaseLocation) -> Result<ReleaseChunks, ReleaseSourceError> {
        let ReleaseLocation::Local(path) = archive else {
            return Err(ReleaseSourceError::Foreign {
                location: archive.clone(),
            });
        };
        if !path.as_path().starts_with(
            self.root
                .as_path()
                .join("releases")
                .join(self.runtime.as_str()),
        ) {
            return Err(ReleaseSourceError::Foreign {
                location: archive.clone(),
            });
        }
        let file = self
            .read(path)
            .await
            .map_err(|source| ReleaseSourceError::Fetch {
                location: archive.clone(),
                source: Box::new(source),
            })?;
        let location = archive.clone();
        Ok(Box::pin(futures_util::stream::try_unfold(
            file,
            move |mut file| {
                let location = location.clone();
                async move {
                    let mut bytes = vec![0; super::staging::WRITE_BATCH_BYTES];
                    let size = file.read(&mut bytes).await.map_err(|source| {
                        ReleaseSourceError::Fetch {
                            location,
                            source: Box::new(source),
                        }
                    })?;
                    bytes.truncate(size);
                    Ok((size != 0).then(|| (bytes::Bytes::from(bytes), file)))
                }
            },
        )))
    }
}

#[async_trait::async_trait]
impl KernelSource for LocalArtifactSource {
    fn root(&self) -> KernelLocation {
        KernelLocation::Local(self.root.clone())
    }
    fn runtime(&self) -> RuntimeKind {
        self.runtime
    }
    async fn list(
        &self,
        line: RuntimeLine,
        architecture: Arch,
    ) -> Result<Vec<SourceKernel>, SourceError> {
        if line.runtime != self.runtime {
            return Ok(Vec::new());
        }
        let pins = self.pins.clone();
        let root = self.root.clone();
        let location = KernelSource::root(self);
        tokio::task::spawn_blocking(move || {
            let catalog = pins.catalog().map_err(|source| SourceError::List {
                location: Box::new(location.clone()),
                source: Box::new(source),
            })?;
            let mut found = Vec::new();
            for pin in catalog
                .all_kernels(architecture)
                .filter(|pin| pin.runtime == line.runtime)
            {
                let parsed_line =
                    pin.runtime_line
                        .as_str()
                        .parse()
                        .map_err(|source| SourceError::List {
                            location: Box::new(location.clone()),
                            source: Box::new(source),
                        })?;
                if line.release != parsed_line {
                    continue;
                }
                let version = pin
                    .version
                    .as_str()
                    .parse()
                    .map_err(|source| SourceError::List {
                        location: Box::new(location.clone()),
                        source: Box::new(source),
                    })?;
                let path = root.join(format!(
                    "kernels/{}/{}/{architecture}/{}/vmlinux.bin",
                    line.runtime, line.release, pin.version
                ));
                match open_regular(root.as_path(), path.as_path()) {
                    Ok(_) => found.push(SourceKernel {
                        version,
                        line,
                        architecture,
                        location: KernelLocation::Local(path),
                        size: pin.size,
                        md5: None,
                    }),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(source) => {
                        return Err(SourceError::List {
                            location: Box::new(KernelLocation::Local(path)),
                            source: Box::new(source),
                        });
                    }
                }
            }
            Ok(found)
        })
        .await
        .map_err(|source| SourceError::List {
            location: Box::new(KernelSource::root(self)),
            source: Box::new(source),
        })?
    }
    async fn open(&self, kernel: &VerifiableKernel) -> Result<ByteChunks, SourceError> {
        let expected = KernelLocation::Local(self.kernel_path(
            kernel.line,
            kernel.architecture,
            kernel.version.as_str(),
        ));
        if kernel.line.runtime != self.runtime || kernel.location != expected {
            return Err(SourceError::Foreign {
                location: Box::new(kernel.location.clone()),
            });
        }
        let KernelLocation::Local(path) = &expected else {
            return Err(SourceError::Foreign {
                location: Box::new(expected),
            });
        };
        let file = self
            .read(path)
            .await
            .map_err(|source| SourceError::Download {
                location: Box::new(expected.clone()),
                source: Box::new(source),
            })?;
        Ok(Box::pin(futures_util::stream::try_unfold(
            file,
            move |mut file| {
                let location = expected.clone();
                async move {
                    let mut bytes = vec![0; super::staging::WRITE_BATCH_BYTES];
                    let size =
                        file.read(&mut bytes)
                            .await
                            .map_err(|source| SourceError::Download {
                                location: Box::new(location),
                                source: Box::new(source),
                            })?;
                    bytes.truncate(size);
                    Ok((size != 0).then(|| (bytes::Bytes::from(bytes), file)))
                }
            },
        )))
    }
}

#[cfg(test)]
mod tests;
