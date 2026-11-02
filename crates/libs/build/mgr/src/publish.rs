use std::{
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use bytes::Bytes;
use pplx_space_common_core::host::linux::paths::template_build_dir;
use pplx_space_common_core::types::{
    AuthIdParseError, ContentHash, OrgId, RUNTIME_FIRECRACKER_ALIAS, StorageUri,
    SystemPublicationGrantId, TEMPLATE_ROOTFS_ZSTD_CODEC, TEMPLATE_ROOTFS_ZSTD_FILE,
    TEMPLATE_ROOTFS_ZSTD_LEVEL, TemplateArtifact, TemplateName, TenantPublicationGrantId,
    template_object_key, template_object_prefix,
};
use pplx_space_common_svc::page_cache;
use pplx_space_node_api_client::cluster::{
    AuthenticatedClusterApiClient, ConnectClusterApiError,
    connect_from_env as connect_cluster_api_from_env,
};
use pplx_space_node_api_client::proto::{
    CreateSystemTemplatePublicationGrantRequest, FacetRef, FindTemplateReleaseByHashRequest,
    MarkFacetDirtyRequest, ReconcileFacet, RegisterTemplateReleaseRequest, RegisterTemplateRequest,
    facet_ref,
};
use pplx_space_object_store::{
    ObjectCredentials, ObjectStore, ObjectStoreError, ObjectTarget, S3ObjectStore,
};
use serde::{Deserialize, Serialize};
use tonic::Code;

use crate::object_store::{
    CredentialExchangeError, ObjectStoreConfig, ObjectStoreConfigError, StorageAccess,
    object_store_credentials,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TemplateOrg {
    org_id: OrgId,
}

impl TemplateOrg {
    pub const fn new(org_id: OrgId) -> Self {
        Self { org_id }
    }

    pub fn parse(org_id: &str) -> Result<Self, AuthIdParseError> {
        Ok(Self {
            org_id: org_id.parse()?,
        })
    }

    pub const fn org_id(self) -> OrgId {
        self.org_id
    }
}

/// The ownership and the specific publication authorization persisted with
/// a build.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TemplatePublication {
    Tenant {
        org: TemplateOrg,
        grant: Option<TenantPublicationGrantId>,
    },
    System(Option<SystemPublicationGrantId>),
}

impl TemplatePublication {
    pub const fn org(self) -> Option<TemplateOrg> {
        match self {
            Self::Tenant { org, .. } => Some(org),
            Self::System(_) => None,
        }
    }

    pub const fn system_grant(self) -> Option<SystemPublicationGrantId> {
        match self {
            Self::Tenant { .. } => None,
            Self::System(grant) => grant,
        }
    }

    pub const fn tenant_grant(self) -> Option<TenantPublicationGrantId> {
        match self {
            Self::Tenant { grant, .. } => grant,
            Self::System(_) => None,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PublishError {
    #[error("firmware guest templates are local-only and cannot be published")]
    LocalOnlyGuestImage,
    #[error("authorize operator system build: {0}")]
    AuthorizeSystemBuild(#[source] tonic::Status),
    #[error("invalid system publication grant returned by ClusterApi: {0}")]
    InvalidSystemGrant(#[source] AuthIdParseError),
    #[error(transparent)]
    InvalidTemplateName(#[from] pplx_space_common_core::types::TemplateParseError),
    #[error(transparent)]
    InvalidContentHash(#[from] pplx_space_common_core::types::DigestParseError),
    #[error(transparent)]
    ObjectStoreConfig(#[from] ObjectStoreConfigError),
    #[error("required build artifact is missing: {path}")]
    MissingArtifact { path: PathBuf },
    #[error("compressed template rootfs was not ready after {}s: {path}", timeout.as_secs())]
    CompressedRootfsNotReady { path: PathBuf, timeout: Duration },
    #[error("failed to read {path}: {source}")]
    ReadArtifact {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid artifact {path}: {source}")]
    InvalidArtifact {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("manifest {path} does not vouch for execution metadata: {source}")]
    ManifestExecutionMetadata {
        path: PathBuf,
        #[source]
        source: pplx_space_common_core::types::TemplateManifestSchemaError,
    },
    #[error(
        "manifest {path} carries no execution metadata; registering would create a \
         permanently metadata-less release"
    )]
    MissingExecutionMetadata { path: PathBuf },
    #[error(transparent)]
    CredentialExchange(#[from] CredentialExchangeError),
    #[error("object store upload failed for {artifact}: {source}")]
    ObjectStore {
        artifact: String,
        #[source]
        source: ObjectStoreError,
    },
    #[error("cluster api connect failed: {0}")]
    ClusterApiConnect(#[from] pplx_space_node_api_client::cluster::ConnectClusterApiError),
    #[error("register template failed: {0}")]
    RegisterTemplate(#[source] tonic::Status),
    #[error("register template release failed: {0}")]
    RegisterTemplateRelease(#[source] tonic::Status),
    #[error("find template release failed: {0}")]
    FindTemplateRelease(#[source] tonic::Status),
}

impl PublishError {
    /// The bounded cause metrics report for a failed registration or
    /// publish.
    pub fn error_kind(&self) -> crate::TemplateBuildErrorKind {
        match self {
            Self::CompressedRootfsNotReady { .. } => crate::TemplateBuildErrorKind::Timeout,
            Self::AuthorizeSystemBuild(_)
            | Self::LocalOnlyGuestImage
            | Self::InvalidSystemGrant(_)
            | Self::InvalidTemplateName(_)
            | Self::InvalidContentHash(_)
            | Self::ObjectStoreConfig(_)
            | Self::MissingArtifact { .. }
            | Self::ReadArtifact { .. }
            | Self::InvalidArtifact { .. }
            | Self::ManifestExecutionMetadata { .. }
            | Self::MissingExecutionMetadata { .. }
            | Self::CredentialExchange(_)
            | Self::ObjectStore { .. }
            | Self::ClusterApiConnect(_)
            | Self::RegisterTemplate(_)
            | Self::RegisterTemplateRelease(_)
            | Self::FindTemplateRelease(_) => crate::TemplateBuildErrorKind::Publish,
        }
    }
}

/// Called by the direct operator CLI using its own ClusterApi credential,
/// before submitting to build-manager. Only an unconfigured cluster is local-only;
/// missing or insufficient operator authority fails before the build is queued.
pub async fn authorize_operator_system_build(
    template_id: &str,
) -> Result<Option<SystemPublicationGrantId>, PublishError> {
    let template = TemplateName::parse(template_id.to_owned())?;
    let mut client = match connect_cluster_api_from_env().await {
        Ok(client) => client,
        Err(ConnectClusterApiError::MissingEndpoint) => return Ok(None),
        Err(error) => return Err(PublishError::ClusterApiConnect(error)),
    };
    let response = client
        .create_system_template_publication_grant(CreateSystemTemplatePublicationGrantRequest {
            template_name: template.to_string(),
        })
        .await
        .map_err(PublishError::AuthorizeSystemBuild)?
        .into_inner();
    response
        .grant_id
        .parse()
        .map(Some)
        .map_err(PublishError::InvalidSystemGrant)
}

pub async fn register_build_template(
    template_id: &str,
    publication: TemplatePublication,
) -> Result<(), PublishError> {
    let template = TemplateName::parse(template_id.to_string())?;
    let mut cluster_api = match connect_cluster_api_from_env().await {
        Ok(client) => client,
        Err(ConnectClusterApiError::MissingEndpoint | ConnectClusterApiError::MissingToken(_)) => {
            tracing::info!(
                template_name_length = template.as_str().len(),
                "ClusterApi is not configured; leaving build local-only"
            );
            return Ok(());
        }
        Err(error) => return Err(PublishError::ClusterApiConnect(error)),
    };
    ensure_cluster_template(&mut cluster_api, &template, publication).await
}

pub async fn publish_build(
    template_id: &str,
    content_hash: &str,
    publish_tags: &[String],
    publication: TemplatePublication,
) -> Result<(), PublishError> {
    let template = TemplateName::parse(template_id.to_string())?;
    let content_hash = content_hash.parse::<ContentHash>()?;
    let build_dir = PathBuf::from(template_build_dir(
        template.as_str(),
        &content_hash.to_string(),
    ));
    let mut cluster_api = match connect_cluster_api_from_env().await {
        Ok(client) => client,
        Err(ConnectClusterApiError::MissingEndpoint | ConnectClusterApiError::MissingToken(_)) => {
            tracing::info!(
                template_name_length = template.as_str().len(),
                hash = %content_hash,
                "ClusterApi is not configured; leaving build local-only"
            );
            return Ok(());
        }
        Err(error) => return Err(PublishError::ClusterApiConnect(error)),
    };

    publish_build_from_dir(
        &mut cluster_api,
        &build_dir,
        &template,
        content_hash,
        publish_tags,
        publication,
    )
    .await?;
    mark_local_template_inventory_dirty(&template).await;
    Ok(())
}

async fn publish_build_from_dir(
    cluster_api: &mut AuthenticatedClusterApiClient,
    build_dir: &Path,
    template: &TemplateName,
    content_hash: ContentHash,
    publish_tags: &[String],
    publication: TemplatePublication,
) -> Result<(), PublishError> {
    let release_tags = release_tags(content_hash, publish_tags);
    let execution = load_build_execution_metadata(build_dir).await?;

    // Register the template identity before anything that depends on it. A
    // failed upload may leave a template with no release, but never a release
    // that points at partial objects.
    ensure_cluster_template(cluster_api, template, publication).await?;
    // A registered release is immutable: Credential Manager refuses write
    // credentials for a release prefix that already has a storage location.
    // A rebuild that reproduced an already-published hash therefore uploads
    // nothing and re-registers its tags only: neither a location it did not
    // upload to nor execution metadata from a manifest that never left here.
    // That holds whether or not a cold store is configured.
    let recorded_locations =
        recorded_release_locations(cluster_api, template, content_hash, publication.org()).await?;

    let Some(object_store) = ObjectStoreConfig::from_env_optional()? else {
        let registration = release_registration(recorded_locations.is_some(), None, execution);
        register_cluster_release(
            cluster_api,
            template,
            content_hash,
            &release_tags,
            registration.storage_locations,
            publication,
            registration.execution,
        )
        .await?;
        tracing::info!(
            template = template.as_str(),
            hash = %content_hash,
            already_published = recorded_locations.is_some(),
            "cold store is not configured; registered release without storage location"
        );
        return Ok(());
    };
    let storage_uri = build_storage_uri(&object_store, template, content_hash);
    let published = if let Some(recorded) = &recorded_locations {
        if recorded.contains(&storage_uri) {
            tracing::info!(
                template = template.as_str(),
                hash = %content_hash,
                "release is already published; skipping artifact upload"
            );
        } else {
            tracing::warn!(
                template = template.as_str(),
                hash = %content_hash,
                recorded_locations = recorded.len(),
                "release is already published elsewhere; skipping artifact upload and \
                 registering tags only, no location at the current object store"
            );
        }
        true
    } else {
        let publication_prefix =
            template_object_prefix(&object_store.key_prefix, template, content_hash);
        let vend = acquire_publication_credentials(build_dir, || {
            object_store_credentials(
                &object_store.bucket,
                &publication_prefix,
                true,
                crate::object_store::CredentialSubject::Template(template),
                StorageAccess::Write,
            )
        })
        .await?;
        match vend {
            Publication::Upload(creds) => {
                let store = S3ObjectStore::new();
                upload_build_artifacts(
                    &store,
                    &creds,
                    &object_store,
                    template,
                    content_hash,
                    build_dir,
                )
                .await?;
                false
            }
            Publication::AlreadyPublished => {
                // Another publisher registered this hash after the lookup, or
                // the lookup could not see the release. The artifacts are
                // published either way; only the tags remain to register.
                tracing::info!(
                    template = template.as_str(),
                    hash = %content_hash,
                    "credential manager reports the release as already published; \
                     skipping artifact upload and registering tags only"
                );
                true
            }
        }
    };
    let registration = release_registration(published, Some(&storage_uri), execution);

    register_cluster_release(
        cluster_api,
        template,
        content_hash,
        &release_tags,
        registration.storage_locations,
        publication,
        registration.execution,
    )
    .await?;
    Ok(())
}

/// The storage locations the cluster already records for this release, when
/// an earlier publication uploaded and registered its artifacts. `None` when
/// the release has no recorded location, is unregistered, or is not visible to
/// this builder (`NotFound`). With a cold store configured, these cases require
/// a credential exchange before deciding whether to upload or register tags only.
async fn recorded_release_locations(
    cluster_api: &mut AuthenticatedClusterApiClient,
    template: &TemplateName,
    content_hash: ContentHash,
    template_org: Option<TemplateOrg>,
) -> Result<Option<Vec<String>>, PublishError> {
    let request = FindTemplateReleaseByHashRequest {
        generic_only: false,
        template_name: template.as_str().to_string(),
        hash: content_hash.to_string(),
        caller_org_id: template_org.map(|org| org.org_id().as_uuid().to_string()),
    };
    match cluster_api.find_template_release_by_hash(request).await {
        Ok(response) => {
            let locations: Vec<String> = response
                .into_inner()
                .storage_locations
                .into_iter()
                .map(|location| location.connection_string)
                .collect();
            Ok((!locations.is_empty()).then_some(locations))
        }
        Err(status) if status.code() == Code::NotFound => Ok(None),
        Err(status) => Err(PublishError::FindTemplateRelease(status)),
    }
}

/// What a publication registers besides its tags.
struct ReleaseRegistration {
    storage_locations: Vec<String>,
    execution: Option<pplx_space_execution_common::proto::ExecutionMetadata>,
}

/// A release that is already published, whether the lookup said so or
/// Credential Manager refused the write, was not uploaded again, so it
/// registers neither a location, even when the current object store differs
/// from every recorded one, nor execution metadata: the local manifest
/// describes artifacts that never left this builder, and the published
/// release's metadata must keep describing what is in the bucket. A first
/// publication registers the location it uploaded to, if a cold store is
/// configured, and its manifest's execution metadata.
fn release_registration(
    published: bool,
    uploaded_to: Option<&str>,
    execution: pplx_space_execution_common::proto::ExecutionMetadata,
) -> ReleaseRegistration {
    if published {
        ReleaseRegistration {
            storage_locations: Vec::new(),
            execution: None,
        }
    } else {
        ReleaseRegistration {
            storage_locations: uploaded_to.map(str::to_string).into_iter().collect(),
            execution: Some(execution),
        }
    }
}

/// The write vend's outcome for a release the publication lookup did not
/// report as published.
#[derive(Debug)]
enum Publication {
    /// Credentials for uploading the artifacts.
    Upload(ObjectCredentials),
    /// Credential Manager refused the write because the release already has
    /// a storage location: another publisher registered this hash after the
    /// lookup, or the lookup could not see the release. The artifacts are
    /// published either way and only tags remain to register.
    AlreadyPublished,
}

/// Acquire write credentials once the artifacts are ready, treating Credential
/// Manager's machine-readable already-published refusal as a publication
/// outcome rather than a failure. Every other error, including unrelated
/// `PERMISSION_DENIED` refusals, still fails the publication.
async fn acquire_publication_credentials<Acquire, AcquireFuture>(
    build_dir: &Path,
    acquire: Acquire,
) -> Result<Publication, PublishError>
where
    Acquire: FnOnce() -> AcquireFuture,
    AcquireFuture: std::future::Future<Output = Result<ObjectCredentials, CredentialExchangeError>>,
{
    match acquire_after_artifacts_ready(build_dir, acquire).await {
        Ok(credentials) => Ok(Publication::Upload(credentials)),
        Err(PublishError::CredentialExchange(error)) if error.is_release_already_published() => {
            Ok(Publication::AlreadyPublished)
        }
        Err(error) => Err(error),
    }
}

async fn mark_local_template_inventory_dirty(template: &TemplateName) {
    let request = MarkFacetDirtyRequest {
        refs: vec![FacetRef {
            facet: ReconcileFacet::TemplateInventory as i32,
            element: Some(facet_ref::Element::Template(template.as_str().to_string())),
        }],
    };
    match pplx_space_node_api_client::connect_node_ctl().await {
        Ok(mut client) => {
            if let Err(error) = client.mark_facet_dirty(request).await {
                tracing::warn!(
                    template = template.as_str(),
                    %error,
                    "failed to mark local template inventory dirty after build publish"
                );
            }
        }
        Err(error) => {
            tracing::debug!(
                template = template.as_str(),
                %error,
                "node-ctl unavailable; local template inventory will publish on the next periodic tick"
            );
        }
    }
}

/// Backoff schedule for transient cluster-api registration failures. The
/// total span covers several kubelet volume-sync periods so a stale projected
/// token gets a real chance to rotate before the build is declared failed.
/// Latency contract: a persistently failing registration spends at most the
/// sum of this schedule sleeping (~3.5 minutes) plus one 30-second client RPC
/// timeout per attempt — well inside the 6-hour build job timeout, and the
/// worker keeps heartbeating the job lease throughout.
const REGISTER_RETRY_BACKOFF: [Duration; 4] = [
    Duration::from_secs(10),
    Duration::from_secs(30),
    Duration::from_secs(60),
    Duration::from_secs(120),
];

/// Retry pacing for one cluster-api registration call: walks
/// [`REGISTER_RETRY_BACKOFF`] once, yielding a delay for each failure that
/// [`RegisterRequest::retry_is_safe`] classifies as replayable.
struct RegisterRetrySchedule {
    backoff: std::slice::Iter<'static, Duration>,
}

impl RegisterRetrySchedule {
    fn new() -> Self {
        Self {
            backoff: REGISTER_RETRY_BACKOFF.iter(),
        }
    }

    /// Delay to sleep before the next attempt, or `None` when the failure is
    /// permanent or the schedule is exhausted and the status should surface.
    fn next_delay(
        &mut self,
        request: &RegisterRequest,
        status: &tonic::Status,
    ) -> Option<Duration> {
        if !request.retry_is_safe(status) {
            return None;
        }
        self.backoff.next().copied()
    }
}

/// One cluster-api registration RPC, dispatched by request type so the retry
/// loop below stays a single plain `async fn` (an `AsyncFnMut`-based helper
/// trips rustc's "`Send` is not general enough" false positive under the
/// callers' `async_trait` bounds).
enum RegisterRequest {
    Template(RegisterTemplateRequest),
    Release(Box<RegisterTemplateReleaseRequest>),
}

impl RegisterRequest {
    const fn operation(&self) -> &'static str {
        match self {
            Self::Template(_) => "RegisterTemplate",
            Self::Release(_) => "RegisterTemplateRelease",
        }
    }

    /// Authentication refusal and the server's explicit no-commit marker are
    /// safe to replay. Generic Unavailable can follow a committed tag write;
    /// retrying it could overwrite a newer build's tag.
    fn retry_is_safe(&self, status: &tonic::Status) -> bool {
        status.code() == Code::Unauthenticated
            || pplx_space_node_api_client::cluster::PublicationNotCommitted::from_status(status)
                .is_some()
    }
}

async fn register_with_transient_retry(
    cluster_api: &mut AuthenticatedClusterApiClient,
    template: &TemplateName,
    request: RegisterRequest,
) -> Result<(), tonic::Status> {
    register_with_retry_schedule(cluster_api, template, request, RegisterRetrySchedule::new()).await
}

async fn register_with_retry_schedule(
    cluster_api: &mut AuthenticatedClusterApiClient,
    template: &TemplateName,
    request: RegisterRequest,
    mut schedule: RegisterRetrySchedule,
) -> Result<(), tonic::Status> {
    loop {
        let result = match &request {
            RegisterRequest::Template(inner) => cluster_api
                .register_template(
                    pplx_space_common_svc::auth::build::publication_request(inner.clone())
                        .map_err(|error| tonic::Status::failed_precondition(error.to_string()))?,
                )
                .await
                .map(|_| ()),
            RegisterRequest::Release(inner) => cluster_api
                .register_template_release(
                    pplx_space_common_svc::auth::build::publication_request(inner.as_ref().clone())
                        .map_err(|error| tonic::Status::failed_precondition(error.to_string()))?,
                )
                .await
                .map(|_| ()),
        };
        let status = match result {
            Ok(()) => return Ok(()),
            Err(status) => status,
        };
        let Some(delay) = schedule.next_delay(&request, &status) else {
            return Err(status);
        };
        tracing::warn!(
            template = template.as_str(),
            operation = request.operation(),
            delay_secs = delay.as_secs(),
            error = %status,
            "transient cluster-api registration failure; retrying"
        );
        tokio::time::sleep(delay).await;
    }
}

async fn ensure_cluster_template(
    cluster_api: &mut AuthenticatedClusterApiClient,
    template: &TemplateName,
    publication: TemplatePublication,
) -> Result<(), PublishError> {
    let request = RegisterRequest::Template(template_registration_request(template, publication));
    match register_with_transient_retry(cluster_api, template, request).await {
        Ok(()) => Ok(()),
        Err(status) if status.code() == Code::AlreadyExists => Ok(()),
        Err(status) => Err(PublishError::RegisterTemplate(status)),
    }
}

fn template_registration_request(
    template: &TemplateName,
    publication: TemplatePublication,
) -> RegisterTemplateRequest {
    RegisterTemplateRequest {
        system_publication_grant_id: publication.system_grant().map(|grant| grant.to_string()),
        tenant_publication_grant_id: publication.tenant_grant().map(|grant| grant.to_string()),
        name: template.as_str().to_string(),
        description: None,
        org_id: publication.org().map(|owner| owner.org_id().to_string()),
    }
}

async fn register_cluster_release(
    cluster_api: &mut AuthenticatedClusterApiClient,
    template: &TemplateName,
    content_hash: ContentHash,
    tags: &[String],
    storage_connection_strings: Vec<String>,
    publication: TemplatePublication,
    execution: Option<pplx_space_execution_common::proto::ExecutionMetadata>,
) -> Result<(), PublishError> {
    let request = RegisterRequest::Release(Box::new(release_registration_request(
        template,
        content_hash,
        tags,
        storage_connection_strings,
        publication,
        execution,
    )));
    register_with_transient_retry(cluster_api, template, request)
        .await
        .map_err(PublishError::RegisterTemplateRelease)
}

fn release_registration_request(
    template: &TemplateName,
    content_hash: ContentHash,
    tags: &[String],
    storage_connection_strings: Vec<String>,
    publication: TemplatePublication,
    execution: Option<pplx_space_execution_common::proto::ExecutionMetadata>,
) -> RegisterTemplateReleaseRequest {
    RegisterTemplateReleaseRequest {
        system_publication_grant_id: publication.system_grant().map(|grant| grant.to_string()),
        tenant_publication_grant_id: publication.tenant_grant().map(|grant| grant.to_string()),
        template_name: template.as_str().to_string(),
        runtime: RUNTIME_FIRECRACKER_ALIAS.to_string(),
        hash: content_hash.to_string(),
        tags: tags.to_vec(),
        storage_connection_strings,
        org_id: publication.org().map(|owner| owner.org_id().to_string()),
        execution,
    }
}

/// Loads the build's execution metadata for attachment to the release
/// registration, from the digest-stable `manifest.json` — not `metadata.json`.
/// A same-rootfs rebuild can refresh `metadata.json` while template-manager
/// retains the hash-addressed manifest, and registration is first-write-wins,
/// so the value frozen here must be the one the manifest (and therefore the
/// node inventory publish) carries. The schema-aware accessor rejects legacy
/// and unsupported manifest shapes instead of trusting a field the schema
/// does not vouch for.
///
/// An unusable manifest fails the publish: registration is the only writer
/// of release execution metadata (node publication does not backfill it), so
/// registering without it would create a permanently metadata-less release.
/// The strictness is consistent with the upload path, which already fails on
/// an unreadable manifest, and with the grpcurl registrants, whose shared
/// converter refuses the same shapes.
async fn load_build_execution_metadata(
    build_dir: &Path,
) -> Result<pplx_space_execution_common::proto::ExecutionMetadata, PublishError> {
    let manifest_path = build_dir.join(TemplateArtifact::Manifest.file_name());
    let bytes =
        tokio::fs::read(&manifest_path)
            .await
            .map_err(|source| PublishError::ReadArtifact {
                path: manifest_path.clone(),
                source,
            })?;
    let manifest = serde_json::from_slice::<pplx_space_common_core::types::TemplateManifest>(
        &bytes,
    )
    .map_err(|source| PublishError::InvalidArtifact {
        path: manifest_path.clone(),
        source,
    })?;
    let execution = manifest.execution_metadata().map_err(|source| {
        PublishError::ManifestExecutionMetadata {
            path: manifest_path.clone(),
            source,
        }
    })?;
    if manifest.guest_image.is_some() {
        return Err(PublishError::LocalOnlyGuestImage);
    }
    let execution = execution.ok_or(PublishError::MissingExecutionMetadata {
        path: manifest_path,
    })?;
    Ok(pplx_space_execution_common::proto::ExecutionMetadata::from(
        execution,
    ))
}

async fn upload_build_artifacts(
    store: &impl ObjectStore,
    creds: &ObjectCredentials,
    object_store: &ObjectStoreConfig,
    template: &TemplateName,
    content_hash: ContentHash,
    build_dir: &Path,
) -> Result<(), PublishError> {
    let metadata_path = artifact_path(build_dir, TemplateArtifact::Metadata)?;
    let manifest_path = artifact_path(build_dir, TemplateArtifact::Manifest)?;
    let compressed_rootfs_path = artifact_path(build_dir, TemplateArtifact::RootfsZstd)?;

    let metadata_key = template_object_key(
        &object_store.key_prefix,
        template,
        content_hash,
        TemplateArtifact::Metadata,
    );
    let manifest_key = template_object_key(
        &object_store.key_prefix,
        template,
        content_hash,
        TemplateArtifact::Manifest,
    );

    let metadata_bytes =
        tokio::fs::read(&metadata_path)
            .await
            .map_err(|source| PublishError::ReadArtifact {
                path: metadata_path.clone(),
                source,
            })?;
    store
        .put_bytes(
            creds,
            ObjectTarget {
                bucket: &object_store.bucket,
                key: &metadata_key,
                region: &object_store.region,
                endpoint: object_store.endpoint.as_deref(),
                path_style: object_store.path_style,
            },
            Bytes::from(metadata_bytes),
        )
        .await
        .map_err(|source| PublishError::ObjectStore {
            artifact: TemplateArtifact::Metadata.file_name().to_string(),
            source,
        })?;

    let manifest_bytes =
        tokio::fs::read(&manifest_path)
            .await
            .map_err(|source| PublishError::ReadArtifact {
                path: manifest_path.clone(),
                source,
            })?;
    store
        .put_bytes(
            creds,
            ObjectTarget {
                bucket: &object_store.bucket,
                key: &manifest_key,
                region: &object_store.region,
                endpoint: object_store.endpoint.as_deref(),
                path_style: object_store.path_style,
            },
            Bytes::from(manifest_bytes),
        )
        .await
        .map_err(|source| PublishError::ObjectStore {
            artifact: TemplateArtifact::Manifest.file_name().to_string(),
            source,
        })?;

    let compressed_rootfs_key = template_object_key(
        &object_store.key_prefix,
        template,
        content_hash,
        TemplateArtifact::RootfsZstd,
    );
    store
        .put_file(
            creds,
            ObjectTarget {
                bucket: &object_store.bucket,
                key: &compressed_rootfs_key,
                region: &object_store.region,
                endpoint: object_store.endpoint.as_deref(),
                path_style: object_store.path_style,
            },
            &compressed_rootfs_path,
        )
        .await
        .map_err(|source| PublishError::ObjectStore {
            artifact: TemplateArtifact::RootfsZstd.file_name().to_string(),
            source,
        })?;
    page_cache::advise_path_dontneed(
        &compressed_rootfs_path,
        "publish compressed template rootfs input",
    );

    Ok(())
}

const COMPRESSED_ROOTFS_READY_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const COMPRESSED_ROOTFS_READY_POLL: Duration = Duration::from_secs(1);

/// Compression can legitimately consume much of a scoped credential's
/// lifetime. Centralizing the ordering makes it impossible for the publish
/// path to acquire a lease before the readiness wait by accident.
async fn acquire_after_artifacts_ready<T, Acquire, AcquireFuture>(
    build_dir: &Path,
    acquire: Acquire,
) -> Result<T, PublishError>
where
    Acquire: FnOnce() -> AcquireFuture,
    AcquireFuture: std::future::Future<Output = Result<T, CredentialExchangeError>>,
{
    wait_for_compressed_rootfs_ready(build_dir).await?;
    acquire().await.map_err(PublishError::from)
}

async fn wait_for_compressed_rootfs_ready(build_dir: &Path) -> Result<(), PublishError> {
    let started = Instant::now();
    let compressed_rootfs_path = build_dir.join(TemplateArtifact::RootfsZstd.file_name());
    loop {
        if compressed_rootfs_ready(build_dir).await? {
            return Ok(());
        }
        if started.elapsed() >= COMPRESSED_ROOTFS_READY_TIMEOUT {
            return Err(PublishError::CompressedRootfsNotReady {
                path: compressed_rootfs_path,
                timeout: COMPRESSED_ROOTFS_READY_TIMEOUT,
            });
        }
        tokio::time::sleep(COMPRESSED_ROOTFS_READY_POLL).await;
    }
}

async fn compressed_rootfs_ready(build_dir: &Path) -> Result<bool, PublishError> {
    let compressed_rootfs_path = build_dir.join(TemplateArtifact::RootfsZstd.file_name());
    if !compressed_rootfs_path.is_file() {
        return Ok(false);
    }

    let manifest_path = build_dir.join(TemplateArtifact::Manifest.file_name());
    let manifest_bytes =
        tokio::fs::read(&manifest_path)
            .await
            .map_err(|source| PublishError::ReadArtifact {
                path: manifest_path.clone(),
                source,
            })?;
    let manifest: serde_json::Value =
        serde_json::from_slice(&manifest_bytes).map_err(|source| {
            PublishError::InvalidArtifact {
                path: manifest_path.clone(),
                source,
            }
        })?;
    let Some(entry) = manifest
        .get("files")
        .and_then(serde_json::Value::as_object)
        .and_then(|files| files.get(TEMPLATE_ROOTFS_ZSTD_FILE))
        .and_then(serde_json::Value::as_object)
    else {
        return Ok(false);
    };

    Ok(entry
        .get("digest")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|digest| !digest.is_empty())
        && entry
            .get("size")
            .and_then(serde_json::Value::as_u64)
            .is_some()
        && entry
            .get("compression")
            .and_then(serde_json::Value::as_object)
            .is_some_and(|compression| {
                compression.get("codec").and_then(serde_json::Value::as_str)
                    == Some(TEMPLATE_ROOTFS_ZSTD_CODEC)
                    && compression.get("level").and_then(serde_json::Value::as_i64)
                        == Some(i64::from(TEMPLATE_ROOTFS_ZSTD_LEVEL))
            }))
}

fn artifact_path(build_dir: &Path, artifact: TemplateArtifact) -> Result<PathBuf, PublishError> {
    let path = build_dir.join(artifact.file_name());
    if path.is_file() {
        Ok(path)
    } else {
        Err(PublishError::MissingArtifact { path })
    }
}

fn build_storage_uri(
    object_store: &ObjectStoreConfig,
    template: &TemplateName,
    content_hash: ContentHash,
) -> String {
    StorageUri {
        bucket: object_store.bucket.clone(),
        key_prefix: template_object_prefix(&object_store.key_prefix, template, content_hash),
        region: object_store.region.clone(),
        endpoint: object_store.endpoint.clone(),
        path_style: object_store.path_style,
    }
    .to_string()
}

fn release_tags(content_hash: ContentHash, publish_tags: &[String]) -> Vec<String> {
    let mut tags = vec![content_hash.to_string()];
    for tag in publish_tags {
        let trimmed = tag.trim();
        if !trimmed.is_empty() && !tags.iter().any(|existing| existing == trimmed) {
            tags.push(trimmed.to_string());
        }
    }
    tags
}

#[cfg(test)]
mod tests;
