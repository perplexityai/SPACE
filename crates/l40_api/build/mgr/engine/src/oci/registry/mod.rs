use async_trait::async_trait;
use oci_spec::distribution::Reference as OciReference;
use pplx_space_common_core::types::{Arch, Digest, DigestParseError};
use reqwest::StatusCode;

mod client;
#[cfg(feature = "aws-ecr")]
mod ecr;
mod ecr_auth;
mod manifest;

#[cfg(feature = "aws-ecr")]
pub use ecr::EcrConfigurationError;

const DOCKER_HUB_REGISTRY: &str = "docker.io";

#[derive(Debug, thiserror::Error)]
pub(super) enum RegistryError {
    #[error("OCI image configuration exceeds the 4 MiB limit")]
    ImageConfigTooLarge,
    #[error("OCI manifest contains alternate download URLs")]
    AlternateDownloadUrls,
    #[cfg(not(feature = "aws-ecr"))]
    #[error("OCI image registry is not supported")]
    UnsupportedRegistry,
    #[error("failed to configure the OCI registry client")]
    ClientConfiguration {
        #[source]
        source: reqwest::Error,
    },
    #[error("Docker Hub authentication request failed")]
    AuthenticationRequest {
        #[source]
        source: reqwest::Error,
    },
    #[error("Docker Hub authentication was rejected with status {status}")]
    AuthenticationRejected { status: StatusCode },
    #[error("Docker Hub authentication response was too large")]
    AuthenticationResponseTooLarge,
    #[error("Docker Hub authentication response was invalid")]
    InvalidAuthenticationResponse {
        #[source]
        source: serde_json::Error,
    },
    #[error("Docker Hub returned an empty authentication token")]
    EmptyAuthenticationToken,
    #[error("OCI registry manifest request failed")]
    ManifestRequest {
        #[source]
        source: reqwest::Error,
    },
    #[error("OCI registry manifest request was rejected with status {status}")]
    ManifestRejected { status: StatusCode },
    #[error("OCI registry manifest response was too large")]
    ManifestResponseTooLarge,
    #[error("OCI registry manifest digest is missing")]
    MissingManifestDigest,
    #[error("OCI registry manifest digest is invalid")]
    InvalidManifestDigest {
        #[source]
        source: DigestParseError,
    },
    #[error("OCI registry manifest body does not match its digest")]
    ManifestDigestMismatch,
    #[error("OCI registry manifest document is invalid")]
    InvalidManifest {
        #[source]
        source: serde_json::Error,
    },
    #[error("OCI image does not contain exactly one matching Linux platform")]
    MissingOrAmbiguousPlatform,
    #[error("OCI image index nesting exceeds the supported depth")]
    IndexNestingTooDeep,
    #[cfg(feature = "aws-ecr")]
    #[error(transparent)]
    EcrConfiguration(#[from] EcrConfigurationError),
    #[cfg(feature = "aws-ecr")]
    #[error("ECR repository is not allowed on this builder")]
    EcrRepositoryNotAllowed,
    #[cfg(feature = "aws-ecr")]
    #[error("ECR authentication request failed")]
    EcrAuthenticationRequest {
        #[source]
        source: Box<
            aws_sdk_ecr::error::SdkError<
                aws_sdk_ecr::operation::get_authorization_token::GetAuthorizationTokenError,
            >,
        >,
    },
    #[cfg(feature = "aws-ecr")]
    #[error("ECR returned no authorization token for the requested registry")]
    EmptyEcrAuthenticationToken,
    #[error("failed to create the ECR authentication directory {path}")]
    EcrAuthDirectory {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to serialize ECR authentication")]
    EcrAuthSerialization {
        #[source]
        source: toml::ser::Error,
    },
    #[error("failed to write ECR authentication file {path}")]
    EcrAuthFile {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
}

pub(super) struct ManifestPayload {
    pub(super) digest: Digest,
    pub(super) body: Vec<u8>,
}

#[async_trait]
pub(super) trait ManifestSource: Send + Sync {
    async fn fetch(
        &self,
        repository: &str,
        manifest: &str,
    ) -> Result<ManifestPayload, RegistryError>;
}

pub(super) struct RegistrySession {
    manifest_source: client::RegistryManifestSource,
    ecr_pull_auth: Option<ecr_auth::EcrPullAuth>,
}

impl RegistrySession {
    fn new(
        manifest_source: client::RegistryManifestSource,
        ecr_pull_auth: Option<ecr_auth::EcrPullAuth>,
    ) -> Self {
        Self {
            manifest_source,
            ecr_pull_auth,
        }
    }

    pub(super) fn manifest_source(&self) -> &dyn ManifestSource {
        &self.manifest_source
    }

    pub(super) fn ecr_pull_auth(&self) -> Option<&ecr_auth::EcrPullAuth> {
        self.ecr_pull_auth.as_ref()
    }
}

pub(super) async fn session_for(
    reference: &OciReference,
) -> Result<RegistrySession, RegistryError> {
    if reference.registry() == DOCKER_HUB_REGISTRY {
        let source = client::RegistryManifestSource::docker_hub(reference.repository()).await?;
        return Ok(RegistrySession::new(source, None));
    }
    #[cfg(feature = "aws-ecr")]
    {
        return ecr::session_for(reference).await;
    }
    #[cfg(not(feature = "aws-ecr"))]
    Err(RegistryError::UnsupportedRegistry)
}

pub(super) use ecr_auth::{EcrPullAuth, ecr_auth_root};

#[cfg(feature = "aws-ecr")]
pub(super) fn validate_configuration() -> Result<(), EcrConfigurationError> {
    ecr::validate_configuration()
}

#[derive(Debug)]
pub(super) struct ResolvedImage {
    pub reference: OciReference,
    pub config_digest: Digest,
}

pub(super) async fn resolve_with_source(
    reference: &OciReference,
    arch: Arch,
    source: &dyn ManifestSource,
) -> Result<ResolvedImage, RegistryError> {
    manifest::resolve(reference, arch, source).await
}

#[cfg(test)]
mod tests;
