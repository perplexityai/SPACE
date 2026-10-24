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
const ASSET: &str = "ch-release-v7.2.8-20261009/Image-arm64";

pub(crate) struct CloudHypervisorKernels {
    transport: GitHubReleaseSource,
    location: ReleaseUrl,
    version: KernelVersion,
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
            location: base.join(ASSET),
            version: "7.2.8+".parse()?,
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
        if line != RuntimeLine::new(self.runtime(), ReleaseLine::new(54, 0))
            || architecture != Arch::Aarch64
        {
            return Ok(Vec::new());
        }
        Ok(vec![SourceKernel {
            version: self.version.clone(),
            line,
            architecture,
            location: KernelLocation::Https(self.location.clone()),
            size: 24_177_152,
            md5: None,
        }])
    }
    async fn open(&self, kernel: &VerifiableKernel) -> Result<ByteChunks, SourceError> {
        if kernel.location != KernelLocation::Https(self.location.clone())
            || kernel.line != RuntimeLine::new(self.runtime(), ReleaseLine::new(54, 0))
            || kernel.architecture != Arch::Aarch64
            || kernel.version != self.version
        {
            return Err(SourceError::Foreign {
                location: Box::new(kernel.location.clone()),
            });
        }
        Ok(Box::pin(
            self.transport
                .open(&self.location)
                .await?
                .map_err(SourceError::Https),
        ))
    }
}
