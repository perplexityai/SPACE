//! Firecracker's release archives, read from its GitHub releases.

use std::time::Duration;

use pplx_space_common_core::types::{Arch, RuntimeIdentity, RuntimeKind};

use super::release_source::{ReleaseChunks, ReleaseSource, ReleaseSourceError, ReleaseUrl};

/// Bound on an archive's first response, connecting and redirects included.
/// The archive's body that follows is bounded per chunk by the caller.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// GitHub answers a release download with a redirect to its object storage.
const MAX_REDIRECTS: usize = 5;

pub(crate) struct GitHubReleaseSource {
    /// The release download base, such as
    /// [`super::release_source::FIRECRACKER_RELEASE_SOURCE`].
    base: ReleaseUrl,
    client: reqwest::Client,
}

#[derive(Debug, thiserror::Error)]
#[error("cannot build the release download client")]
pub struct GitHubReleaseSourceError(#[source] reqwest::Error);

impl GitHubReleaseSource {
    pub(crate) fn new(base: ReleaseUrl) -> Result<Self, GitHubReleaseSourceError> {
        // Redirects stay on HTTPS: `holds` vets only the first location.
        let client = reqwest::Client::builder()
            .https_only(true)
            .redirect(reqwest::redirect::Policy::limited(MAX_REDIRECTS))
            .connect_timeout(CONNECT_TIMEOUT)
            .user_agent(concat!("space-rtmgr/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(GitHubReleaseSourceError)?;
        Ok(Self { base, client })
    }

    fn holds(&self, archive: &ReleaseUrl) -> bool {
        archive
            .as_str()
            .strip_prefix(self.base.as_str().trim_end_matches('/'))
            .is_some_and(|rest| rest.starts_with('/'))
    }

    /// The response's status and headers; the caller bounds the wait.
    async fn send(&self, location: &ReleaseUrl) -> Result<reqwest::Response, ReleaseSourceError> {
        let fetch = |source: reqwest::Error| ReleaseSourceError::Fetch {
            location: location.clone(),
            source: Box::new(source),
        };
        self.client
            .get(location.as_str())
            .send()
            .await
            .map_err(fetch)?
            .error_for_status()
            .map_err(fetch)
    }
}

#[async_trait::async_trait]
impl ReleaseSource for GitHubReleaseSource {
    fn runtime(&self) -> RuntimeKind {
        RuntimeKind::Firecracker
    }

    fn root(&self) -> &ReleaseUrl {
        &self.base
    }

    fn archive(&self, release: &RuntimeIdentity, architecture: Arch) -> ReleaseUrl {
        let version = release.version.as_str();
        self.base.join(&format!("v{version}")).join(&format!(
            "firecracker-v{version}-{}.tgz",
            architecture.as_str()
        ))
    }

    fn archive_member(
        &self,
        release: &RuntimeIdentity,
        architecture: Arch,
        binary: &str,
    ) -> String {
        let (version, architecture) = (release.version.as_str(), architecture.as_str());
        format!("release-v{version}-{architecture}/{binary}-v{version}-{architecture}")
    }

    async fn open(&self, archive: &ReleaseUrl) -> Result<ReleaseChunks, ReleaseSourceError> {
        if !self.holds(archive) {
            return Err(ReleaseSourceError::Foreign {
                location: archive.clone(),
            });
        }
        let response = tokio::time::timeout(REQUEST_TIMEOUT, self.send(archive))
            .await
            .map_err(|_| ReleaseSourceError::TimedOut {
                location: archive.clone(),
                after: REQUEST_TIMEOUT,
            })??;
        let location = archive.clone();
        Ok(Box::pin(futures_util::stream::try_unfold(
            response,
            move |mut response| {
                let location = location.clone();
                async move {
                    let chunk =
                        response
                            .chunk()
                            .await
                            .map_err(|source| ReleaseSourceError::Fetch {
                                location,
                                source: Box::new(source),
                            })?;
                    Ok(chunk.map(|chunk| (chunk, response)))
                }
            },
        )))
    }
}

#[cfg(test)]
mod tests;
