//! Reviewed upstream kernel release assets. Listing is a compiled manifest;
//! installing never follows a moving "latest" release or grants execution policy.
use super::{
    github::{GitHubReleaseSource, GitHubRuntime},
    release_source::{ReleaseSource, ReleaseUrl},
    source::*,
};
use futures_util::TryStreamExt;
use pplx_space_common_core::types::{Arch, RuntimeKind};

const BASE: &str = "https://github.com/cloud-hypervisor/linux/releases/download";
const RELEASE: &str = "ch-release-v7.2.8-20261009";

pub(crate) struct CloudHypervisorKernels {
    transport: GitHubReleaseSource,
    kernels: Vec<SourceKernel>,
}

#[derive(Debug, thiserror::Error)]
pub enum KernelSourceBuildError {
    #[error(transparent)]
    Url(#[from] super::release_source::ReleaseUrlParseError),
    #[error(transparent)]
    Version(#[from] KernelVersionParseError),
    #[error(transparent)]
    Transport(#[from] super::github::GitHubReleaseSourceError),
}
impl CloudHypervisorKernels {
    pub(crate) fn new() -> Result<Self, KernelSourceBuildError> {
        let base: ReleaseUrl = BASE.parse()?;
        Ok(Self {
            kernels: [
                (Arch::Aarch64, "Image-arm64", 24_177_152),
                (Arch::X86_64, "vmlinux-x86_64", 48_205_848),
            ]
            .into_iter()
            .map(|(architecture, asset, size)| {
                Ok(SourceKernel {
                    version: "7.2.8+".parse()?,
                    line: RuntimeLine::new(RuntimeKind::CloudHypervisor, ReleaseLine::new(54, 0)),
                    architecture,
                    location: KernelLocation::Https(base.join(&format!("{RELEASE}/{asset}"))),
                    size,
                    md5: None,
                })
            })
            .collect::<Result<_, KernelVersionParseError>>()?,
            transport: GitHubReleaseSource::new(GitHubRuntime::CloudHypervisor, base)?,
        })
    }
}

#[async_trait::async_trait]
impl KernelSource for CloudHypervisorKernels {
    fn root(&self) -> KernelLocation {
        KernelLocation::Https(self.transport.root().clone())
    }
    fn runtime(&self) -> RuntimeKind {
        RuntimeKind::CloudHypervisor
    }
    async fn list(
        &self,
        line: RuntimeLine,
        architecture: Arch,
    ) -> Result<Vec<SourceKernel>, SourceError> {
        Ok(self
            .kernels
            .iter()
            .filter(|kernel| kernel.line == line && kernel.architecture == architecture)
            .cloned()
            .collect())
    }
    async fn open(&self, kernel: &VerifiableKernel) -> Result<ByteChunks, SourceError> {
        let source = self
            .kernels
            .iter()
            .find(|source| {
                source.line == kernel.line
                    && source.architecture == kernel.architecture
                    && source.version == kernel.version
                    && source.location == kernel.location
                    && source.size == kernel.size
            })
            .ok_or_else(|| SourceError::Foreign {
                location: Box::new(kernel.location.clone()),
            })?;
        let KernelLocation::Https(location) = &source.location else {
            return Err(SourceError::Foreign {
                location: Box::new(source.location.clone()),
            });
        };
        Ok(Box::pin(
            self.transport
                .open(location)
                .await?
                .map_err(SourceError::Https),
        ))
    }
}

#[cfg(test)]
mod tests;
