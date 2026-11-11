//! In-memory kernel and release sources, and their fixtures, for tests.

use super::release_source::ReleasePayload;

use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures_util::StreamExt as _;
use md5::{Digest as _, Md5};
use pplx_space_common_core::types::{
    Arch, Digest, RuntimeIdentity, RuntimeKind, RuntimeVersion, StorageUri,
};
use sha2::Sha256;

use std::os::unix::fs::MetadataExt as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use pplx_space_node_runtime_core::artifact_store::{
    ArtifactStore, PublishOutcome, RemoveOutcome, StagedName,
};
use pplx_space_node_runtime_core::catalog::{
    CatalogError, PinnedFile, PinnedKernel, PinnedRelease, RuntimeCatalog,
};

use super::publisher::{ArtifactPublisher, NodeArtifactRef, PinSource, Published, PublisherError};
use super::release_source::{ReleaseChunks, ReleaseSource, ReleaseSourceError, ReleaseUrl};

use super::source::{
    ByteChunks, ContentMd5, KernelLocation, KernelSource, KernelSources, RuntimeLine, SourceError,
    SourceKernel, VerifiableKernel,
};

/// How the fake source serves a kernel's bytes.
#[derive(Clone, Debug)]
pub(crate) enum Body {
    /// All of these bytes, in two chunks.
    Bytes(Vec<u8>),
    /// These bytes, then a transport error.
    FailAfter(Vec<u8>),
}

#[derive(Debug)]
struct Offered {
    kernel: SourceKernel,
    // Catalog evidence does not change when a transfer serves different bytes.
    digest: Digest,
    body: Body,
}

#[derive(Debug)]
pub(crate) struct FakeSource {
    root: StorageUri,
    offered: Mutex<Vec<Offered>>,
    opens: AtomicUsize,
}

impl Default for FakeSource {
    fn default() -> Self {
        Self {
            root: "s3://fake/firecracker-ci?region=us-east-1"
                .parse()
                .expect("fake source root is valid"),
            offered: Mutex::default(),
            opens: AtomicUsize::default(),
        }
    }
}

impl FakeSource {
    /// Offer `bytes` as `version` for `line`, listed with their true size
    /// and MD5.
    pub(crate) fn offer(&self, line: &str, version: &str, bytes: &[u8]) -> SourceKernel {
        self.offer_with(
            line,
            version,
            bytes.len() as u64,
            Some(md5_of(bytes)),
            Body::Bytes(bytes.to_vec()),
        )
    }

    /// Offer a kernel listed with `size` and `md5`, served as `body`.
    pub(crate) fn offer_with(
        &self,
        line: &str,
        version: &str,
        size: u64,
        md5: Option<ContentMd5>,
        body: Body,
    ) -> SourceKernel {
        let line = fc_line(line);
        let architecture =
            super::source::host_arch().expect("tests run on a supported architecture");
        let kernel = SourceKernel {
            version: version.parse().expect("test version is valid"),
            line,
            architecture,
            location: StorageUri {
                key_prefix: format!(
                    "firecracker-ci/v{}/{architecture}/vmlinux-{version}",
                    line.release
                ),
                ..self.root.clone()
            }
            .into(),
            size,
            md5,
        };
        let bytes = match &body {
            Body::Bytes(bytes) | Body::FailAfter(bytes) => bytes,
        };
        let digest = Digest::sha256(Sha256::digest(bytes).into());
        self.offered
            .lock()
            .expect("fake source lock")
            .push(Offered {
                kernel: kernel.clone(),
                digest,
                body,
            });
        kernel
    }

    /// Serve `kernel`'s bytes as `body` from now on.
    pub(crate) fn serve(&self, kernel: &SourceKernel, body: Body) {
        for offered in self.offered.lock().expect("fake source lock").iter_mut() {
            if offered.kernel.location == kernel.location {
                offered.body = body.clone();
            }
        }
    }

    /// How many downloads were started.
    pub(crate) fn opens(&self) -> usize {
        self.opens.load(Ordering::SeqCst)
    }

    /// Pins for each version's latest offer. Changing a transfer with `serve`
    /// does not replace these independently captured bytes.
    pub(crate) fn pins(&self) -> Vec<PinnedKernel> {
        let offered = self.offered.lock().expect("fake source lock");
        let mut pins: Vec<PinnedKernel> = Vec::new();
        for offer in offered.iter().rev() {
            let kernel = &offer.kernel;
            if pins.iter().any(|pin| {
                pin.version == *kernel.version.artifact_version()
                    && pin.architecture == kernel.architecture
            }) {
                continue;
            }
            pins.push(PinnedKernel {
                version: kernel.version.artifact_version().clone(),
                runtime: kernel.line.runtime,
                runtime_line: RuntimeVersion::parse(kernel.line.release.to_string())
                    .expect("a test line is a runtime version"),
                architecture: kernel.architecture,
                size: kernel.size,
                sha256: offer.digest,
            });
        }
        pins
    }
}

#[async_trait::async_trait]
impl KernelSource for FakeSource {
    fn root(&self) -> KernelLocation {
        self.root.clone().into()
    }

    fn runtime(&self) -> RuntimeKind {
        RuntimeKind::Firecracker
    }

    async fn list(
        &self,
        line: RuntimeLine,
        architecture: Arch,
    ) -> Result<Vec<SourceKernel>, SourceError> {
        Ok(self
            .offered
            .lock()
            .expect("fake source lock")
            .iter()
            .filter(|offered| {
                offered.kernel.line == line && offered.kernel.architecture == architecture
            })
            .map(|offered| offered.kernel.clone())
            .collect())
    }

    async fn open(&self, kernel: &VerifiableKernel) -> Result<ByteChunks, SourceError> {
        self.opens.fetch_add(1, Ordering::SeqCst);
        let body = self
            .offered
            .lock()
            .expect("fake source lock")
            .iter()
            .find(|offered| offered.kernel.location == kernel.location)
            .map(|offered| offered.body.clone())
            .expect("the fake source opens only what it offered");
        let chunks = |bytes: Vec<u8>| {
            let middle = bytes.len() / 2;
            let (first, second) = bytes.split_at(middle);
            vec![
                Ok(bytes::Bytes::copy_from_slice(first)),
                Ok(bytes::Bytes::copy_from_slice(second)),
            ]
        };
        Ok(match body {
            Body::Bytes(bytes) => Box::pin(futures_util::stream::iter(chunks(bytes))),
            Body::FailAfter(bytes) => {
                let location = kernel.location.clone();
                Box::pin(futures_util::stream::iter(chunks(bytes)).chain(
                    futures_util::stream::once(async move {
                        Err(SourceError::Download {
                            location: Box::new(location),
                            source: "connection reset".into(),
                        })
                    }),
                ))
            }
        })
    }
}

pub(crate) fn md5_of(bytes: &[u8]) -> ContentMd5 {
    ContentMd5::new(Md5::digest(bytes).into())
}

/// A kernel image the runtime manager accepts on this host, padded to `len`
/// bytes: an ELF64 `vmlinux` header on x86_64, the Linux `Image` header on
/// aarch64, as Firecracker's published kernels are.
pub(crate) fn boot_kernel(len: usize, fill: u8) -> Vec<u8> {
    let mut image = vec![fill; len.max(64)];
    let Some(header) = image.get_mut(..64) else {
        return image;
    };
    header.fill(0);
    match super::source::host_arch() {
        Some(Arch::Aarch64) => {
            header[..2].copy_from_slice(b"MZ");
            header[56..60].copy_from_slice(b"ARMd");
        }
        _ => {
            header[..4].copy_from_slice(b"\x7fELF");
            header[4] = 2; // ELF64
            header[5] = 1; // little endian
            header[6] = 1; // version
            header[16..18].copy_from_slice(&2u16.to_le_bytes()); // executable
            header[18..20].copy_from_slice(&62u16.to_le_bytes()); // x86_64
            header[20..24].copy_from_slice(&1u32.to_le_bytes());
            header[52..54].copy_from_slice(&64u16.to_le_bytes());
        }
    }
    image
}

/// A boot image of `len` bytes whose banner names `release`.
pub(crate) fn banner_kernel(len: usize, release: &str) -> Vec<u8> {
    let mut image = boot_kernel(len, 0);
    let banner = format!("Linux version {release} (builder@host) #1 SMP\n");
    image.splice(64..64 + banner.len(), banner.bytes());
    image
}

/// The Firecracker line `release` names, such as `1.15`.
pub(crate) fn fc_line(release: &str) -> RuntimeLine {
    RuntimeLine::new(
        RuntimeKind::Firecracker,
        release.parse().expect("test line is valid"),
    )
}

/// An in-memory Firecracker release source: archives laid out as
/// Firecracker's GitHub releases are, each with a published checksum.
pub(crate) struct FakeReleaseSource {
    root: ReleaseUrl,
    archives: Mutex<Vec<(ReleaseUrl, Vec<u8>)>>,
    opens: AtomicUsize,
}

impl Default for FakeReleaseSource {
    fn default() -> Self {
        Self {
            root: "https://releases.example.test/download"
                .parse()
                .expect("fake release root is valid"),
            archives: Mutex::default(),
            opens: AtomicUsize::default(),
        }
    }
}

impl FakeReleaseSource {
    /// Serve `archive` as `version`'s for this host.
    pub(crate) fn publish(&self, version: &str, archive: Vec<u8>) {
        let location = self.archive(&fc_release(version), host());
        self.archives
            .lock()
            .expect("fake release source lock")
            .push((location, archive));
    }

    pub(crate) fn opens(&self) -> usize {
        self.opens.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl ReleaseSource for FakeReleaseSource {
    fn runtime(&self) -> RuntimeKind {
        RuntimeKind::Firecracker
    }

    fn root(&self) -> &ReleaseUrl {
        &self.root
    }

    fn archive(&self, release: &RuntimeIdentity, architecture: Arch) -> ReleaseUrl {
        let version = release.version.as_str();
        self.root
            .join(&format!("v{version}"))
            .join(&format!("firecracker-v{version}-{architecture}.tgz"))
    }

    fn payload(&self, release: &RuntimeIdentity, architecture: Arch) -> ReleasePayload {
        super::release_source::firecracker_payload(release, architecture)
    }

    async fn open(&self, archive: &ReleaseUrl) -> Result<ReleaseChunks, ReleaseSourceError> {
        self.opens.fetch_add(1, Ordering::SeqCst);
        let bytes = self
            .archives
            .lock()
            .expect("fake release source lock")
            .iter()
            .find(|(location, ..)| location == archive)
            .map(|(_, bytes)| bytes.clone())
            .expect("the fake release source opens only what it published");
        let middle = bytes.len() / 2;
        let (first, second) = bytes.split_at(middle);
        Ok(Box::pin(futures_util::stream::iter(vec![
            Ok(bytes::Bytes::copy_from_slice(first)),
            Ok(bytes::Bytes::copy_from_slice(second)),
        ])))
    }
}

fn host() -> Arch {
    super::source::host_arch().expect("tests run on a supported architecture")
}

/// Firecracker release `version`, such as `1.16.2`.
pub(crate) fn fc_release(version: &str) -> RuntimeIdentity {
    RuntimeIdentity {
        kind: RuntimeKind::Firecracker,
        version: RuntimeVersion::parse(version).expect("test release is valid"),
    }
}

/// An ELF64 executable header for `architecture`, padded with `fill`.
pub(crate) fn executable(architecture: Arch, fill: u8) -> Vec<u8> {
    let mut binary = vec![fill; 256];
    binary[..64].fill(0);
    binary[..4].copy_from_slice(b"\x7fELF");
    binary[4] = 2; // ELF64
    binary[5] = 1; // little endian
    binary[6] = 1; // version
    binary[16..18].copy_from_slice(&2u16.to_le_bytes()); // executable
    let machine: u16 = match architecture {
        Arch::X86_64 => 62,
        Arch::Aarch64 => 183,
    };
    binary[18..20].copy_from_slice(&machine.to_le_bytes());
    binary[20..24].copy_from_slice(&1u32.to_le_bytes());
    binary[52..54].copy_from_slice(&64u16.to_le_bytes());
    binary
}

/// One archive member: a regular file, or a symlink to the given target.
#[derive(Clone)]
pub(crate) enum Member {
    File(Vec<u8>),
    Link(&'static str),
}

/// A gzip-compressed tar archive holding `members`.
pub(crate) fn tgz(members: &[(&str, Member)]) -> Vec<u8> {
    let encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    let mut builder = tar::Builder::new(encoder);
    for (path, member) in members {
        let mut header = tar::Header::new_gnu();
        match member {
            Member::File(bytes) => {
                header.set_entry_type(tar::EntryType::Regular);
                header.set_size(bytes.len() as u64);
                header.set_mode(0o755);
                builder
                    .append_data(&mut header, path, bytes.as_slice())
                    .expect("append a test member");
            }
            Member::Link(target) => {
                header.set_entry_type(tar::EntryType::Symlink);
                header.set_size(0);
                builder
                    .append_link(&mut header, path, target)
                    .expect("append a test link");
            }
        }
    }
    builder
        .into_inner()
        .and_then(flate2::write::GzEncoder::finish)
        .expect("finish a test archive")
}

/// Release `version`'s archive as Firecracker publishes it for this host,
/// with binaries made from `fill`.
pub(crate) fn release_archive(version: &str, fill: u8) -> Vec<u8> {
    let architecture = host();
    let member = |binary: &str| {
        format!("release-v{version}-{architecture}/{binary}-v{version}-{architecture}")
    };
    tgz(&[
        (
            &member("firecracker"),
            Member::File(executable(architecture, fill)),
        ),
        (
            &member("jailer"),
            Member::File(executable(architecture, fill.wrapping_add(1))),
        ),
        (
            &format!("release-v{version}-{architecture}/SHA256SUMS"),
            Member::File(b"not unpacked\n".to_vec()),
        ),
    ])
}

/// Pins for tests: every kernel the fake source offers is pinned by the
/// bytes it serves, and releases as a test pins them.
pub(crate) struct TestPins {
    kernels: Arc<FakeSource>,
    releases: Mutex<Vec<PinnedRelease>>,
}

impl TestPins {
    pub(crate) fn new(kernels: Arc<FakeSource>) -> Arc<Self> {
        Arc::new(Self {
            kernels,
            releases: Mutex::default(),
        })
    }

    pub(crate) fn pin_release(&self, pin: PinnedRelease) {
        let mut releases = self.releases.lock().expect("test pins lock");
        releases.retain(|pinned| {
            pinned.release != pin.release || pinned.architecture != pin.architecture
        });
        releases.push(pin);
    }
}

impl PinSource for TestPins {
    fn catalog(&self) -> Result<RuntimeCatalog, CatalogError> {
        Ok(RuntimeCatalog::from_pins(
            self.releases
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone(),
            self.kernels.pins(),
        ))
    }
}

/// A publisher that does what node-manager does, in-process: the store's
/// own publication and removal, held to the pins, with the test user as the
/// store's owner.
pub(crate) struct LocalPublisher {
    store: ArtifactStore,
    pins: Arc<dyn PinSource>,
}

impl LocalPublisher {
    pub(crate) fn new(
        releases_root: &Path,
        kernels_root: &Path,
        staging: &Path,
        pins: Arc<dyn PinSource>,
    ) -> Arc<Self> {
        let owner = std::fs::metadata(kernels_root)
            .expect("the kernel root exists")
            .uid();
        Arc::new(Self {
            store: ArtifactStore::new(releases_root, kernels_root, staging, 1 << 30)
                .owned_by(owner),
            pins,
        })
    }
}

fn refused(error: &(dyn std::error::Error + 'static), transient: bool) -> PublisherError {
    if transient {
        PublisherError::Unavailable(error.to_string().into())
    } else {
        PublisherError::Refused(Box::new(tonic::Status::failed_precondition(
            super::describe(error),
        )))
    }
}

#[async_trait::async_trait]
impl ArtifactPublisher for LocalPublisher {
    async fn publish(
        &self,
        artifact: &NodeArtifactRef,
        staged: &StagedName,
    ) -> Result<Published, PublisherError> {
        let catalog = self
            .pins
            .catalog()
            .map_err(|error| refused(&error, false))?;
        let architecture = host();
        let publication = match artifact {
            NodeArtifactRef::Boot(identity) => {
                let pin = catalog
                    .boot_artifact(identity, architecture)
                    .ok_or_else(|| {
                        PublisherError::Refused(Box::new(tonic::Status::failed_precondition(
                            "no boot pins",
                        )))
                    })?;
                self.store.publish_boot_artifact(pin, staged)
            }
            NodeArtifactRef::Release(release, digest) => {
                let pin = catalog
                    .release(release, architecture)
                    .filter(|pin| pin.digest() == *digest)
                    .ok_or_else(|| {
                        PublisherError::Refused(Box::new(tonic::Status::failed_precondition(
                            "no pins",
                        )))
                    })?;
                self.store.publish_release(pin, staged)
            }
            NodeArtifactRef::Kernel(version, digest) => {
                let pin = catalog
                    .kernel(version.artifact_version(), architecture)
                    .filter(|pin| pin.sha256 == *digest)
                    .ok_or_else(|| {
                        PublisherError::Refused(Box::new(tonic::Status::failed_precondition(
                            "no pins",
                        )))
                    })?;
                self.store.publish_kernel(pin, staged)
            }
        }
        .map_err(|error| refused(&error, error.is_transient()))?;
        Ok(Published {
            path: publication.path,
            digest: publication.digest,
            already_present: publication.outcome == PublishOutcome::AlreadyPresent,
        })
    }

    async fn remove(&self, artifact: &NodeArtifactRef) -> Result<bool, PublisherError> {
        let removed = match artifact {
            NodeArtifactRef::Boot(identity) => self.store.remove_boot_artifact(identity),
            NodeArtifactRef::Release(release, digest) => {
                self.store.remove_release(release, *digest)
            }
            NodeArtifactRef::Kernel(version, digest) => self
                .store
                .remove_kernel(version.artifact_version(), *digest),
        }
        .map_err(|error| refused(&error, error.is_transient()))?;
        Ok(removed == RemoveOutcome::Removed)
    }
}

/// Release `version`'s pins for [`release_archive`]`(version, fill)`.
pub(crate) fn release_pin(version: &str, fill: u8) -> PinnedRelease {
    let architecture = host();
    let sha256 = |bytes: &[u8]| Digest::sha256(Sha256::digest(bytes).into());
    PinnedRelease {
        release: fc_release(version),
        architecture,
        archive_sha256: sha256(&release_archive(version, fill)),
        binaries: vec![
            PinnedFile {
                path: "bin/firecracker",
                sha256: sha256(&executable(architecture, fill)),
            },
            PinnedFile {
                path: "bin/jailer",
                sha256: sha256(&executable(architecture, fill.wrapping_add(1))),
            },
        ],
    }
}

/// Whether `staging` holds no job's directory: only the empty namespace
/// directories, `<runtime>/<kind>/`, a job leaves behind.
pub(crate) fn staging_holds_no_job(staging: &Path) -> bool {
    let entries = |dir: &Path| -> Vec<PathBuf> {
        std::fs::read_dir(dir)
            .expect("list staging")
            .map(|entry| entry.expect("staging entry").path())
            .collect()
    };
    entries(staging).iter().all(|runtime| {
        runtime.is_dir()
            && entries(runtime)
                .iter()
                .all(|kind| kind.is_dir() && entries(kind).is_empty())
    })
}

pub(crate) struct FakeExecutableSource {
    pub(crate) location: super::release_source::ReleaseUrl,
    pub(crate) bytes: Vec<u8>,
}

#[async_trait::async_trait]
impl ReleaseSource for FakeExecutableSource {
    fn runtime(&self) -> RuntimeKind {
        RuntimeKind::CloudHypervisor
    }

    fn root(&self) -> &super::release_source::ReleaseUrl {
        &self.location
    }

    fn archive(&self, _: &RuntimeIdentity, _: Arch) -> super::release_source::ReleaseUrl {
        self.location.clone()
    }

    fn payload(&self, _: &RuntimeIdentity, _: Arch) -> ReleasePayload {
        ReleasePayload::Executable
    }

    async fn open(
        &self,
        _: &super::release_source::ReleaseUrl,
    ) -> Result<super::release_source::ReleaseChunks, ReleaseSourceError> {
        Ok(Box::pin(futures_util::stream::iter([Ok(
            bytes::Bytes::copy_from_slice(&self.bytes),
        )])))
    }
}

pub(crate) fn kernel_sources(source: Arc<dyn KernelSource>) -> KernelSources {
    let mut sources = KernelSources::default();
    sources.insert(source).unwrap();
    sources
}
