use serde::Deserialize;
use sha2::{Digest as _, Sha256};
use tracing::Instrument as _;

use super::{Digest, ManifestPayload, ManifestSource, RegistryError, StatusCode};

const DOCKER_HUB_MANIFESTS_URL: &str = "https://registry-1.docker.io/v2";
const DOCKER_HUB_TOKEN_URL: &str = "https://auth.docker.io/token";
const DOCKER_HUB_TOKEN_SERVICE: &str = "registry.docker.io";
const DOCKER_CONTENT_DIGEST: &str = "docker-content-digest";
const ACCEPT_MANIFESTS: &str = concat!(
    "application/vnd.oci.image.index.v1+json,",
    "application/vnd.oci.image.manifest.v1+json,",
    "application/vnd.docker.distribution.manifest.list.v2+json,",
    "application/vnd.docker.distribution.manifest.v2+json"
);
const MAX_RESPONSE_BYTES: usize = 4 * 1024 * 1024;

pub(super) struct RegistryManifestSource {
    client: reqwest::Client,
    manifests_url: String,
    server_address: String,
    authorization: RegistryAuthorization,
}

enum RegistryAuthorization {
    #[cfg(feature = "aws-ecr")]
    Basic(String),
    Bearer(String),
}

#[derive(Deserialize)]
pub(super) struct TokenResponse {
    token: Option<String>,
    access_token: Option<String>,
}

impl TokenResponse {
    pub(super) fn into_token(self) -> Option<String> {
        self.token
            .filter(|token| !token.is_empty())
            .or_else(|| self.access_token.filter(|token| !token.is_empty()))
    }
}

enum ResponseBodyError {
    Request(reqwest::Error),
    TooLarge,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RegistryOperation {
    Authenticate,
    FetchManifest,
}

impl RegistryOperation {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Authenticate => "docker_registry.authenticate",
            Self::FetchManifest => "docker_registry.fetch_manifest",
        }
    }
}

impl std::fmt::Display for RegistryOperation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl RegistryManifestSource {
    pub(super) async fn docker_hub(repository: &str) -> Result<Self, RegistryError> {
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .https_only(true)
            .connect_timeout(std::time::Duration::from_secs(10))
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|source| RegistryError::ClientConfiguration { source })?;
        let operation = RegistryOperation::Authenticate;
        let span = registry_client_span(operation, "auth.docker.io");
        let result = async {
            let scope = format!("repository:{repository}:pull");
            let response = client
                .get(DOCKER_HUB_TOKEN_URL)
                .query(&[
                    ("service", DOCKER_HUB_TOKEN_SERVICE),
                    ("scope", scope.as_str()),
                ])
                .send()
                .await
                .map_err(|source| RegistryError::AuthenticationRequest { source })?;
            record_http_status(response.status());
            if !response.status().is_success() {
                return Err(RegistryError::AuthenticationRejected {
                    status: response.status(),
                });
            }
            let body = read_bounded(response).await.map_err(|error| match error {
                ResponseBodyError::Request(source) => {
                    RegistryError::AuthenticationRequest { source }
                }
                ResponseBodyError::TooLarge => RegistryError::AuthenticationResponseTooLarge,
            })?;
            let response: TokenResponse = serde_json::from_slice(&body)
                .map_err(|source| RegistryError::InvalidAuthenticationResponse { source })?;
            let token = response
                .into_token()
                .ok_or(RegistryError::EmptyAuthenticationToken)?;
            Ok(Self {
                client,
                manifests_url: DOCKER_HUB_MANIFESTS_URL.to_string(),
                server_address: "registry-1.docker.io".to_string(),
                authorization: RegistryAuthorization::Bearer(token),
            })
        }
        .instrument(span.clone())
        .await;
        record_registry_result(&span, &result);
        result
    }

    #[cfg(feature = "aws-ecr")]
    pub(super) fn ecr(registry: &str, authorization_token: String) -> Result<Self, RegistryError> {
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .https_only(true)
            .connect_timeout(std::time::Duration::from_secs(10))
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|source| RegistryError::ClientConfiguration { source })?;
        Ok(Self {
            client,
            manifests_url: format!("https://{registry}/v2"),
            server_address: registry.to_string(),
            authorization: RegistryAuthorization::Basic(authorization_token),
        })
    }
}

#[async_trait::async_trait]
impl ManifestSource for RegistryManifestSource {
    async fn fetch(
        &self,
        repository: &str,
        manifest: &str,
    ) -> Result<ManifestPayload, RegistryError> {
        let operation = RegistryOperation::FetchManifest;
        let span = registry_client_span(operation, &self.server_address);
        let result = async {
            let request = self
                .client
                .get(format!(
                    "{}/{repository}/manifests/{manifest}",
                    self.manifests_url
                ))
                .header(reqwest::header::ACCEPT, ACCEPT_MANIFESTS);
            let request = match &self.authorization {
                #[cfg(feature = "aws-ecr")]
                RegistryAuthorization::Basic(token) => {
                    request.header(reqwest::header::AUTHORIZATION, format!("Basic {token}"))
                }
                RegistryAuthorization::Bearer(token) => request.bearer_auth(token),
            };
            let response = request
                .send()
                .await
                .map_err(|source| RegistryError::ManifestRequest { source })?;
            record_http_status(response.status());
            if !response.status().is_success() {
                return Err(RegistryError::ManifestRejected {
                    status: response.status(),
                });
            }
            let digest_header = response.headers().get(DOCKER_CONTENT_DIGEST).cloned();
            let body = read_bounded(response).await.map_err(|error| match error {
                ResponseBodyError::Request(source) => RegistryError::ManifestRequest { source },
                ResponseBodyError::TooLarge => RegistryError::ManifestResponseTooLarge,
            })?;
            verified_manifest_payload(digest_header.as_ref(), body)
        }
        .instrument(span.clone())
        .await;
        record_registry_result(&span, &result);
        result
    }
}

pub(super) fn verified_manifest_payload(
    digest_header: Option<&reqwest::header::HeaderValue>,
    body: Vec<u8>,
) -> Result<ManifestPayload, RegistryError> {
    let actual = Digest::sha256(Sha256::digest(&body).into());
    // ECR omits this header, so treat it as an additional integrity check when present.
    if let Some(digest_header) = digest_header {
        let digest = digest_header
            .to_str()
            .map_err(|_| RegistryError::MissingManifestDigest)?
            .parse::<Digest>()
            .map_err(|source| RegistryError::InvalidManifestDigest { source })?;
        if digest != actual {
            return Err(RegistryError::ManifestDigestMismatch);
        }
    }
    Ok(ManifestPayload {
        digest: actual,
        body,
    })
}

fn registry_client_span(operation: RegistryOperation, server_address: &str) -> tracing::Span {
    tracing::info_span!(
        "http.client",
        otel.kind = "client",
        otel.name = operation.as_str(),
        otel.status_code = tracing::field::Empty,
        otel.status_description = tracing::field::Empty,
        "operation.name" = operation.as_str(),
        http.request.method = "GET",
        http.response.status_code = tracing::field::Empty,
        server.address = server_address,
        server.port = 443,
        url.scheme = "https",
    )
}

fn record_http_status(status: StatusCode) {
    tracing::Span::current().record("http.response.status_code", i64::from(status.as_u16()));
}

fn record_registry_result<T>(span: &tracing::Span, result: &Result<T, RegistryError>) {
    match result {
        Ok(_) => {
            span.record("otel.status_code", "OK");
        }
        Err(error) => {
            span.record("otel.status_code", "ERROR");
            span.record("otel.status_description", error.to_string().as_str());
        }
    }
}

async fn read_bounded(mut response: reqwest::Response) -> Result<Vec<u8>, ResponseBodyError> {
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(ResponseBodyError::Request)? {
        if body.len().saturating_add(chunk.len()) > MAX_RESPONSE_BYTES {
            return Err(ResponseBodyError::TooLarge);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}
