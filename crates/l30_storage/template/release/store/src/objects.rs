//! A release's `metadata.json` in object storage: the shared one its build
//! was published with, and a twin's own, under the build's
//! `environments/<environment>/` key.

use std::sync::OnceLock;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use aws_credential_types::Credentials;
use aws_credential_types::provider::{ProvideCredentials, SharedCredentialsProvider};
use bytes::Bytes;
use pplx_space_artifact_model::{
    StorageOriginRejection, StorageUri, StorageUriParseError, TemplateArtifact,
    TrustedStorageBuckets, template_environment_metadata_relative_key,
};
use pplx_space_execution_spec::EnvironmentReference;
use pplx_space_object_store::{
    ObjectCredentials, ObjectCredentialsError, ObjectStore, ObjectStoreError, ObjectTarget,
    S3ObjectStore,
};

/// Reading or writing a release's metadata failed.
#[derive(Debug, thiserror::Error)]
pub enum ObjectAccessError {
    /// A storage location didn't parse. Neither the connection string nor
    /// the parser's error, which repeats it, is kept: a connection string can
    /// carry endpoint credentials. `reason` names what was wrong.
    #[error("a storage location is not an object-store URI: {reason}")]
    Location { reason: &'static str },
    /// The location overrides the endpoint with one that isn't `https://`,
    /// and the client wasn't built to allow that. The endpoint isn't in the
    /// message: an endpoint URL can carry credentials.
    #[error("storage location in bucket {bucket:?} names a plaintext endpoint")]
    PlaintextEndpoint { bucket: String },
    /// The client's trusted-origin policy refuses the location: its bucket
    /// isn't trusted, or it overrides the endpoint. The endpoint isn't in the
    /// message, for the same reason.
    #[error("storage location in bucket {bucket:?} is refused: {reason}")]
    UntrustedOrigin {
        bucket: String,
        reason: StorageOriginRejection,
    },
    /// The location's key prefix isn't the release's own build directory
    /// (`<prefix>/templates/<name>/<hash>`), so a twin's metadata written
    /// under it would land in another build's directory, or in one that
    /// every build of the template shares.
    #[error("storage location in bucket {bucket:?} isn't the release's build directory")]
    NotReleaseDirectory { bucket: String },
    #[error(transparent)]
    Credentials(#[from] ObjectCredentialsError),
    #[error(transparent)]
    Store(#[from] ObjectStoreError),
}

/// Where a release's build lives in object storage: one of its storage
/// locations, whose key prefix is the build directory
/// (`<prefix>/templates/<name>/<hash>`).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TwinMetadataTarget {
    location: StorageUri,
    /// Whether the key prefix is the release's own build directory. Only
    /// then does the target take a twin's metadata.
    release_directory: bool,
}

impl TwinMetadataTarget {
    /// Parse one of the storage connection strings of the release of
    /// `template_name` and `hash`.
    ///
    /// A location outside the release's build directory still parses, so its
    /// shared metadata can be read, but the twin-metadata key is refused for
    /// it.
    ///
    /// An endpoint override must be an `http://` or `https://` URL with a
    /// host, no query or fragment, and no `@`; the URI may carry no `#`
    /// fragment; and the bucket and region may carry only identifier
    /// characters. Otherwise credentials or tokens in the connection string
    /// would reach the target's `Debug`, this crate's errors, or the object
    /// store's errors, which quote the endpoint.
    pub fn parse(
        connection_string: &str,
        template_name: &str,
        hash: &str,
    ) -> Result<Self, ObjectAccessError> {
        if connection_string.contains('#') {
            return Err(ObjectAccessError::Location {
                reason: "storage location carries a fragment",
            });
        }
        let location =
            StorageUri::parse(connection_string).map_err(|error| ObjectAccessError::Location {
                reason: parse_failure_reason(&error),
            })?;
        if let Some((_, query)) = connection_string.split_once('?') {
            validate_unique_query_keys(query)
                .map_err(|reason| ObjectAccessError::Location { reason })?;
        }
        if location.bucket.contains('@') {
            return Err(ObjectAccessError::Location {
                reason: "the bucket carries credentials",
            });
        }
        if !is_valid_bucket(&location.bucket) {
            return Err(ObjectAccessError::Location {
                reason: "invalid bucket name",
            });
        }
        if !is_valid_region(&location.region) {
            return Err(ObjectAccessError::Location {
                reason: "invalid region",
            });
        }
        if let Some(endpoint) = location.endpoint.as_deref() {
            validate_endpoint(endpoint).map_err(|reason| ObjectAccessError::Location { reason })?;
        }
        let release_directory = is_release_directory(&location.key_prefix, template_name, hash);
        Ok(Self {
            location,
            release_directory,
        })
    }

    /// The bucket the build is in.
    pub fn bucket(&self) -> &str {
        &self.location.bucket
    }

    /// The key of the build's shared `metadata.json`.
    pub fn shared_metadata_key(&self) -> String {
        self.join(TemplateArtifact::Metadata.file_name())
    }

    /// The key of the build's `manifest.json`.
    pub(crate) fn manifest_key(&self) -> String {
        self.join(TemplateArtifact::Manifest.file_name())
    }

    /// The key of the `metadata.json` of the build's twin bound to
    /// `environment`. Refused with [`ObjectAccessError::NotReleaseDirectory`]
    /// unless the location is the release's own build directory.
    pub(crate) fn twin_metadata_key(
        &self,
        environment: EnvironmentReference,
    ) -> Result<String, ObjectAccessError> {
        if !self.release_directory {
            return Err(ObjectAccessError::NotReleaseDirectory {
                bucket: self.location.bucket.clone(),
            });
        }
        Ok(self.join(&template_environment_metadata_relative_key(environment)))
    }

    fn join(&self, relative: &str) -> String {
        if self.location.key_prefix.is_empty() {
            relative.to_owned()
        } else {
            format!("{}/{relative}", self.location.key_prefix)
        }
    }

    /// Whether the location overrides the endpoint with one that isn't
    /// `https://`. A location without an override uses the region's AWS
    /// endpoint, which is HTTPS.
    fn has_plaintext_endpoint(&self) -> bool {
        self.location.endpoint.as_deref().is_some_and(|endpoint| {
            !endpoint
                .get(..8)
                .is_some_and(|scheme| scheme.eq_ignore_ascii_case("https://"))
        })
    }

    fn object<'a>(&'a self, key: &'a str) -> ObjectTarget<'a> {
        ObjectTarget {
            bucket: &self.location.bucket,
            key,
            region: &self.location.region,
            endpoint: self.location.endpoint.as_deref(),
            path_style: self.location.path_style,
        }
    }
}

/// Whether `key_prefix` ends in `templates/<template_name>/<hash>`, with no
/// empty or dot segments anywhere in it. A SHA-256 digest is stored with or
/// without its `sha256:` prefix, and its build directory may be spelled either
/// way; any other digest matches only its own spelling.
fn is_release_directory(key_prefix: &str, template_name: &str, hash: &str) -> bool {
    if key_prefix
        .split('/')
        .any(|segment| segment.is_empty() || segment == "." || segment == "..")
    {
        return false;
    }
    let mut segments = key_prefix.rsplit('/');
    let (Some(directory), Some(name), Some(templates)) =
        (segments.next(), segments.next(), segments.next())
    else {
        return false;
    };
    templates == "templates" && name == template_name && same_build_hash(directory, hash)
}

/// The 64-character lowercase hex body of a bare or `sha256:`-prefixed SHA-256
/// digest, or `None` for any other string.
pub(crate) fn sha256_hex(hash: &str) -> Option<&str> {
    let hex = hash.strip_prefix("sha256:").unwrap_or(hash);
    (hex.len() == 64
        && hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)))
    .then_some(hex)
}

fn same_build_hash(left: &str, right: &str) -> bool {
    match (sha256_hex(left), sha256_hex(right)) {
        (Some(left), Some(right)) => left == right,
        _ => !left.is_empty() && left == right,
    }
}

fn is_valid_bucket(bucket: &str) -> bool {
    !bucket.is_empty()
        && bucket
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
}

fn is_valid_region(region: &str) -> bool {
    !region.is_empty()
        && region
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

/// Reject empty query segments and duplicate query keys before
/// [`StorageUri::parse`] folds them with last-write-wins, so an earlier
/// unvalidated parameter cannot ride along in the raw connection string.
fn validate_unique_query_keys(query: &str) -> Result<(), &'static str> {
    let mut seen_region = false;
    let mut seen_endpoint = false;
    let mut seen_path_style = false;
    for pair in query.split('&') {
        if pair.is_empty() {
            return Err("malformed query parameter");
        }
        let Some((key, _)) = pair.split_once('=') else {
            return Err("malformed query parameter");
        };
        let slot = match key {
            "region" => &mut seen_region,
            "endpoint" => &mut seen_endpoint,
            "path_style" => &mut seen_path_style,
            _ => continue,
        };
        if *slot {
            return Err("duplicate query parameter");
        }
        *slot = true;
    }
    Ok(())
}

/// Check that an endpoint override is an `http://` or `https://` URL with a
/// host, no query or fragment, and no `@`.
fn validate_endpoint(endpoint: &str) -> Result<(), &'static str> {
    if endpoint.contains('@') {
        return Err("the endpoint carries credentials");
    }
    if endpoint.contains('#') {
        return Err("endpoint URL carries a fragment");
    }
    let uri = endpoint
        .parse::<http::Uri>()
        .map_err(|_| "malformed endpoint URL")?;
    let Some(scheme) = uri.scheme_str() else {
        return Err("endpoint URL has no scheme");
    };
    if !scheme.eq_ignore_ascii_case("http") && !scheme.eq_ignore_ascii_case("https") {
        return Err("endpoint URL scheme isn't http or https");
    }
    if uri.host().is_none_or(str::is_empty) {
        return Err("endpoint URL has no host");
    }
    if uri.query().is_some() {
        return Err("endpoint URL carries a query string");
    }
    Ok(())
}

/// What was wrong with a connection string, without the string itself.
fn parse_failure_reason(error: &StorageUriParseError) -> &'static str {
    match error {
        StorageUriParseError::NotS3(_) => "not an s3:// URI",
        StorageUriParseError::Empty(_) => "empty s3:// URI",
        StorageUriParseError::MissingBucket(_) => "missing bucket",
        StorageUriParseError::MissingRegion(_) => "missing region",
        StorageUriParseError::MalformedQuery { .. } => "malformed query parameter",
        StorageUriParseError::InvalidPathStyle { .. } => "path_style isn't true or false",
        StorageUriParseError::UnsupportedQuery { .. } => "unsupported query parameter",
    }
}

/// The release metadata objects a twin needs: read the build's shared
/// `metadata.json`, write a twin's own. Never writes the shared one.
#[async_trait]
pub trait ReleaseObjects: Send + Sync + 'static {
    /// The bytes of the build's shared `metadata.json`.
    async fn read_metadata(
        &self,
        target: &TwinMetadataTarget,
    ) -> Result<Vec<u8>, ObjectAccessError>;

    /// The bytes of the build's `manifest.json`.
    async fn read_manifest(
        &self,
        target: &TwinMetadataTarget,
    ) -> Result<Vec<u8>, ObjectAccessError>;

    /// Write `document` as the `metadata.json` of the build's twin bound to
    /// `environment`, replacing any document already there. Refused with
    /// [`ObjectAccessError::NotReleaseDirectory`] when `target` isn't the
    /// release's own build directory.
    ///
    /// Call this only before the twin is published: check that no release of
    /// the build is in the environment's partition first. A published twin's
    /// metadata is immutable, and a different document would no longer match
    /// the `execution_metadata` its row records. The write replaces rather
    /// than refusing an existing object because an object with no twin row is
    /// unpublished: a run that wrote it and failed before inserting the twin
    /// leaves it behind, possibly stale, and the retry has to replace it.
    ///
    /// The check and the write aren't atomic, so the caller must also be the
    /// only writer for the build and environment until the twin's row
    /// commits, for example by serializing upgrades per build and
    /// environment. Two writers that both see an empty partition race
    /// otherwise: one publishes its twin, and the other's later write
    /// replaces that twin's metadata.
    async fn write_twin_metadata(
        &self,
        target: &TwinMetadataTarget,
        environment: EnvironmentReference,
        document: Bytes,
    ) -> Result<(), ObjectAccessError>;
}

/// [`ReleaseObjects`] on S3, signed with the process's default AWS credential
/// chain (on EKS, the pod's workload identity). The chain is loaded once, on
/// first use. Its provider doesn't cache, so the credentials it returns are
/// kept until `CREDENTIAL_REFRESH_MARGIN` before they expire, rather than
/// fetched for every object. A caller that never asks builds no client and
/// loads no configuration.
///
/// A registrant picks a release's storage locations, endpoint included, and
/// these requests carry the process's own credentials: the access key ID, the
/// session token and a signature. So a client from [`S3ReleaseObjects::new`]
/// fails every call on a location whose endpoint isn't `https://` with
/// [`ObjectAccessError::PlaintextEndpoint`], before loading any credentials.
/// Local S3-compatible storage served over plain HTTP needs
/// [`S3ReleaseObjects::allowing_plaintext_endpoints`], which an operator must
/// opt into and which allows endpoint overrides too.
///
/// A client built with [`S3ReleaseObjects::trusting`] uses only the
/// locations the deployment's trusted buckets admit. With no trusted buckets,
/// a location that overrides the endpoint is refused unless the client was
/// built with [`S3ReleaseObjects::allowing_endpoint_overrides`], an operator
/// opt-in for S3-compatible storage at an `https://` endpoint.
#[derive(Default)]
pub struct S3ReleaseObjects {
    allow_plaintext_endpoints: bool,
    allow_endpoint_overrides: bool,
    trusted: TrustedStorageBuckets,
    store: OnceLock<S3ObjectStore>,
    provider: tokio::sync::OnceCell<SharedCredentialsProvider>,
    credentials: tokio::sync::Mutex<Option<Credentials>>,
}

/// How long before they expire cached credentials are replaced: room for an
/// object call that starts just before.
const CREDENTIAL_REFRESH_MARGIN: Duration = Duration::from_secs(5 * 60);

impl S3ReleaseObjects {
    /// A client that refuses plaintext endpoints and, until trusted buckets
    /// or [`Self::allowing_endpoint_overrides`] say otherwise, every endpoint
    /// override.
    pub fn new() -> Self {
        Self::default()
    }

    /// A client that also uses `http://` endpoints, for local S3-compatible
    /// storage. A plaintext endpoint is always an override, so this allows
    /// endpoint overrides too. Only for an operator's explicit opt-in.
    pub fn allowing_plaintext_endpoints() -> Self {
        Self {
            allow_plaintext_endpoints: true,
            allow_endpoint_overrides: true,
            ..Self::default()
        }
    }

    /// This client, also using a location's `https://` endpoint override
    /// while no trusted buckets are configured, for S3-compatible storage.
    /// Only for an operator's explicit opt-in.
    #[must_use]
    pub fn allowing_endpoint_overrides(self) -> Self {
        Self {
            allow_endpoint_overrides: true,
            ..self
        }
    }

    /// This client, refusing every location `trusted` refuses, before
    /// loading any credentials. An empty policy refuses none.
    #[must_use]
    pub fn trusting(self, trusted: TrustedStorageBuckets) -> Self {
        Self { trusted, ..self }
    }

    /// Refuse `target` before any credentials are loaded when the trusted
    /// buckets refuse it, when its endpoint is plaintext and this client
    /// wasn't built to allow that, or when it overrides the endpoint, no
    /// trusted buckets are configured to vet it (a configured list refuses
    /// every override itself), and this client wasn't built to allow that.
    pub(crate) fn admit(&self, target: &TwinMetadataTarget) -> Result<(), ObjectAccessError> {
        if let Err(reason) = self.trusted.check(&target.location) {
            return Err(ObjectAccessError::UntrustedOrigin {
                bucket: target.location.bucket.clone(),
                reason,
            });
        }
        if !self.allow_plaintext_endpoints && target.has_plaintext_endpoint() {
            return Err(ObjectAccessError::PlaintextEndpoint {
                bucket: target.location.bucket.clone(),
            });
        }
        if !self.allow_endpoint_overrides && target.location.endpoint.is_some() {
            return Err(ObjectAccessError::UntrustedOrigin {
                bucket: target.location.bucket.clone(),
                reason: StorageOriginRejection::EndpointOverride,
            });
        }
        Ok(())
    }

    async fn parts(&self) -> Result<(&S3ObjectStore, ObjectCredentials), ObjectAccessError> {
        let credentials = self.credentials().await?;
        let credentials = ObjectCredentials {
            access_key_id: credentials.access_key_id().to_owned(),
            secret_access_key: credentials.secret_access_key().to_owned(),
            session_token: credentials.session_token().map(str::to_owned),
        };
        Ok((self.store.get_or_init(S3ObjectStore::new), credentials))
    }

    async fn credentials(&self) -> Result<Credentials, ObjectAccessError> {
        let mut cached = self.credentials.lock().await;
        if let Some(credentials) = cached.as_ref()
            && credentials.expiry().is_none_or(|expiry| {
                expiry
                    .duration_since(SystemTime::now())
                    .is_ok_and(|left| left > CREDENTIAL_REFRESH_MARGIN)
            })
        {
            return Ok(credentials.clone());
        }
        let provider = self
            .provider
            .get_or_try_init(|| async {
                aws_config::load_defaults(aws_config::BehaviorVersion::latest())
                    .await
                    .credentials_provider()
                    .ok_or(ObjectCredentialsError::NoProvider)
            })
            .await?;
        let credentials = provider
            .provide_credentials()
            .await
            .map_err(ObjectCredentialsError::from)?;
        *cached = Some(credentials.clone());
        Ok(credentials)
    }
}

#[async_trait]
impl ReleaseObjects for S3ReleaseObjects {
    async fn read_metadata(
        &self,
        target: &TwinMetadataTarget,
    ) -> Result<Vec<u8>, ObjectAccessError> {
        self.admit(target)?;
        let (store, credentials) = self.parts().await?;
        read_release_metadata(store, &credentials, target).await
    }

    async fn read_manifest(
        &self,
        target: &TwinMetadataTarget,
    ) -> Result<Vec<u8>, ObjectAccessError> {
        self.admit(target)?;
        let (store, credentials) = self.parts().await?;
        let key = target.manifest_key();
        Ok(store.get_bytes(&credentials, target.object(&key)).await?)
    }

    async fn write_twin_metadata(
        &self,
        target: &TwinMetadataTarget,
        environment: EnvironmentReference,
        document: Bytes,
    ) -> Result<(), ObjectAccessError> {
        self.admit(target)?;
        target.twin_metadata_key(environment)?;
        let (store, credentials) = self.parts().await?;
        write_twin_metadata(store, &credentials, target, environment, document).await
    }
}

pub(crate) async fn read_release_metadata(
    store: &dyn ObjectStore,
    credentials: &ObjectCredentials,
    target: &TwinMetadataTarget,
) -> Result<Vec<u8>, ObjectAccessError> {
    let key = target.shared_metadata_key();
    Ok(store.get_bytes(credentials, target.object(&key)).await?)
}

pub(crate) async fn write_twin_metadata(
    store: &dyn ObjectStore,
    credentials: &ObjectCredentials,
    target: &TwinMetadataTarget,
    environment: EnvironmentReference,
    document: Bytes,
) -> Result<(), ObjectAccessError> {
    let key = target.twin_metadata_key(environment)?;
    Ok(store
        .put_bytes(credentials, target.object(&key), document)
        .await?)
}
