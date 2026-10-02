//! An in-memory kernel source and kernel fixtures for tests.

use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures_util::StreamExt as _;
use md5::{Digest as _, Md5};
use pplx_space_common_core::types::{Arch, RuntimeKind, StorageUri};

use super::source::{
    ByteChunks, ContentMd5, KernelSource, RuntimeLine, SourceError, SourceKernel, VerifiableKernel,
};

/// How the fake source serves a kernel's bytes.
#[derive(Clone, Debug)]
pub(crate) enum Body {
    /// All of these bytes, in two chunks.
    Bytes(Vec<u8>),
    /// These bytes, then nothing ever again.
    Stall(Vec<u8>),
    /// These bytes, then a transport error.
    FailAfter(Vec<u8>),
}

#[derive(Debug)]
struct Offered {
    kernel: SourceKernel,
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
            },
            size,
            md5,
        };
        self.offered
            .lock()
            .expect("fake source lock")
            .push(Offered {
                kernel: kernel.clone(),
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
}

#[async_trait::async_trait]
impl KernelSource for FakeSource {
    fn root(&self) -> &StorageUri {
        &self.root
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
            Body::Stall(bytes) => Box::pin(
                futures_util::stream::iter(chunks(bytes)).chain(futures_util::stream::pending()),
            ),
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
