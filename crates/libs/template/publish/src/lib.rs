//! `space-template-publish` — makes a cluster's pinned template releases
//! resolvable, so a freshly provisioned cluster can create a sandbox.
//!
//! A cluster whose Helm releases are all green still cannot create a sandbox
//! until `ClusterApi.FindTemplateRelease("seed", "latest")` resolves. That
//! needs two things per template: the builder's artifacts in *this* cluster's
//! object store, and the release rows that point at them. Registration state is
//! per-cluster database rows, so sharing another cluster's bucket supplies
//! bytes and never rows.
//!
//! For each `--template <name>@<content-hash>` pin, in the order given:
//!
//! 1. ask `FindTemplateRelease` for `latest`. If it resolves, this template is
//!    already published — **stop here**. Nothing is copied and nothing is
//!    registered, which is what makes a second run against a live cluster a
//!    no-op and what stops a pin from dragging `latest` backwards over a newer
//!    release a builder has since published;
//! 2. otherwise put the builder's three artifacts in the destination bucket,
//!    copying each from the source bucket server-side when the destination
//!    lacks it. The bytes never pass through this process;
//! 3. verify the destination `manifest.json` names this template and this
//!    content hash — the artifacts stay exactly as the builder authored them,
//!    and the pin is checked against them rather than trusted;
//! 4. register the template (tolerating one that already exists) and the
//!    release, tagged `latest` plus its own digest, with a self-describing
//!    storage URI derived from the destination bucket.
//!
//! Everything goes through the real ClusterApi RPCs. Nothing here writes
//! Postgres directly and nothing is staged onto a worker: each worker's
//! template-manager prefetch loop resolves the tags it tracks and pulls the
//! digests it is missing, so registering the release is the whole job.
//!
//! This crate is the whole job. The `space-template-publish` binary is a thin
//! `main` that parses flags, initializes logging, and calls [`run`], so a
//! deployment that needs private wiring can supply its own `main` against this
//! library without copying any of the publisher.

use std::str::FromStr;

use pplx_space_common_core::types::{
    ContentHash, DEFAULT_TEMPLATE_TAG, RUNTIME_FIRECRACKER_ALIAS, StorageUri, TemplateArtifact,
    TemplateManifest, TemplateName, TemplateReference, TemplateReferenceParseError,
    TemplateSelector, template_object_key, template_object_prefix,
};
use pplx_space_execution_common::proto::ExecutionMetadata;
use pplx_space_node_api_client::cluster::{
    AuthenticatedClusterApiClient, ConnectClusterApiError,
    connect_from_env as connect_cluster_api_from_env,
};
use pplx_space_node_api_client::proto::{
    FindTemplateReleaseRequest, RegisterTemplateReleaseRequest, RegisterTemplateRequest,
};
use pplx_space_object_store::{
    ObjectCredentials, ObjectCredentialsError, ObjectStore, ObjectStoreError, ObjectTarget,
    S3ObjectStore,
};
use tokio::sync::OnceCell;
use tonic::Code;
use tracing::{info, warn};

/// Name the job logs under, shared with the binary's startup banner.
pub const SERVICE_NAME: &str = "space-template-publish";

/// One `(template, content hash)` pin from a cluster's publish list.
///
/// Spelled `<name>@<content-hash>` on the command line, which is exactly
/// [`TemplateReference`]'s digest form — so parsing goes through that shared
/// grammar rather than a second one, and this type only adds the constraint the
/// grammar deliberately does not have: a pin must name one immutable build, so a
/// tag or an abbreviated digest is rejected.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TemplatePin {
    template: TemplateName,
    content_hash: ContentHash,
}

#[derive(Debug, thiserror::Error)]
pub enum TemplatePinParseError {
    #[error("template pin '{pin}' is not a valid template reference: {source}")]
    Reference {
        pin: String,
        #[source]
        source: TemplateReferenceParseError,
    },
    #[error(
        "template pin '{pin}' must name a full content hash ('<name>@sha256:<64 hex>'); a tag or \
         abbreviated digest would let the artifacts a pin resolves to change underneath it"
    )]
    NotAFullDigest { pin: String },
}

impl FromStr for TemplatePin {
    type Err = TemplatePinParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let reference: TemplateReference =
            value
                .parse()
                .map_err(|source| TemplatePinParseError::Reference {
                    pin: value.to_string(),
                    source,
                })?;
        let TemplateSelector::ArtifactDigest(digest) = reference.selector() else {
            return Err(TemplatePinParseError::NotAFullDigest {
                pin: value.to_string(),
            });
        };
        Ok(Self {
            content_hash: digest.content_hash(),
            template: reference.name().clone(),
        })
    }
}

impl std::fmt::Display for TemplatePin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}@{}", self.template.as_str(), self.content_hash)
    }
}

/// What one pin did, so the run's log reads as a per-template ledger.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PinOutcome {
    /// `latest` already resolved. Nothing was copied, nothing registered.
    AlreadyPublished,
    /// Every artifact was already in the destination bucket; only the release
    /// rows were missing.
    Registered,
    /// Artifacts were copied from the source bucket, then registered.
    CopiedAndRegistered,
}

impl PinOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::AlreadyPublished => "already_published",
            Self::Registered => "registered",
            Self::CopiedAndRegistered => "copied_and_registered",
        }
    }
}

impl std::fmt::Display for PinOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Every way a publish run fails. Named per step so the Job's exit reason
/// identifies the pin and the artifact an operator has to look at.
#[derive(Debug, thiserror::Error)]
pub enum RunTemplatePublishError {
    #[error("cluster api connect failed: {0}")]
    ClusterApiConnect(#[from] ConnectClusterApiError),
    #[error(transparent)]
    Credentials(#[from] ObjectCredentialsError),
    #[error("{template}: looking up the latest release failed: {source}")]
    FindRelease {
        template: TemplateName,
        #[source]
        source: tonic::Status,
    },
    #[error("{template}: object store operation on '{key}' failed: {source}")]
    ObjectStore {
        template: TemplateName,
        key: String,
        #[source]
        source: ObjectStoreError,
    },
    #[error("{template}: manifest.json at '{key}' is not valid JSON: {source}")]
    ManifestMalformed {
        template: TemplateName,
        key: String,
        #[source]
        source: serde_json::Error,
    },
    #[error(
        "{template}: manifest.json at '{key}' describes {manifest_name}@{manifest_hash}, not the pinned release"
    )]
    ManifestMismatch {
        template: TemplateName,
        key: String,
        manifest_name: String,
        manifest_hash: String,
    },
    #[error(
        "{template}: manifest.json at '{key}' carries no content hash, so the pin cannot be \
         checked against the artifacts it claims"
    )]
    ManifestUnstamped { template: TemplateName, key: String },
    #[error("{template}: RegisterTemplate failed: {source}")]
    RegisterTemplate {
        template: TemplateName,
        #[source]
        source: tonic::Status,
    },
    #[error("{template}: RegisterTemplateRelease failed: {source}")]
    RegisterTemplateRelease {
        template: TemplateName,
        #[source]
        source: tonic::Status,
    },
}

/// Where a cluster's template artifacts live, and where they come from.
///
/// There is deliberately no key-prefix knob. Keys are always
/// `templates/<name>/<hash>/<file>`, because that is the one prefix the IAM
/// policies authorize — `builder_template_artifacts_publish` and
/// `template_artifacts_pull` are both scoped to `<bucket>/templates/*`. A prefix
/// input would be a setting whose only reachable value is the default, and whose
/// every other value fails at runtime with `AccessDenied`.
#[derive(Clone, Debug, Eq, PartialEq)]
struct BucketLocation {
    bucket: String,
    region: String,
}

impl BucketLocation {
    fn target<'a>(&'a self, key: &'a str) -> ObjectTarget<'a> {
        ObjectTarget {
            bucket: &self.bucket,
            key,
            region: &self.region,
            // Real S3 only: a custom endpoint and path-style addressing are the
            // local kind object store's business, and the kind stack has its own
            // publish Job.
            endpoint: None,
            path_style: false,
        }
    }

    /// The self-describing connection string a release in this location gets.
    fn storage_uri(&self, pin: &TemplatePin) -> String {
        StorageUri {
            bucket: self.bucket.clone(),
            key_prefix: template_object_prefix("", &pin.template, pin.content_hash),
            region: self.region.clone(),
            endpoint: None,
            path_style: false,
        }
        .to_string()
    }
}

/// Everything one publish run needs: the binary's flags, parsed.
///
/// The source bucket and region stay optional here rather than being resolved
/// by the caller, so the fallback to the destination — the shape every cluster
/// but one runs in — is the library's behavior and is covered by its tests.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TemplatePublishRunConfig {
    /// Pins to settle, in the order given, so a template may be listed after
    /// the one it derives from.
    pub templates: Vec<TemplatePin>,

    /// Destination bucket: this cluster's own template-artifacts bucket.
    pub bucket: String,

    /// Destination bucket region.
    pub region: String,

    /// Bucket already holding the pinned builds. `None` means `bucket`, which
    /// makes the copy step a no-op for a cluster that already has its own
    /// artifacts.
    pub source_bucket: Option<String>,

    /// Source bucket region. `None` means `region`.
    pub source_region: Option<String>,
}

impl TemplatePublishRunConfig {
    fn destination(&self) -> BucketLocation {
        BucketLocation {
            bucket: self.bucket.clone(),
            region: self.region.clone(),
        }
    }

    fn source(&self) -> BucketLocation {
        BucketLocation {
            bucket: self
                .source_bucket
                .clone()
                .unwrap_or_else(|| self.bucket.clone()),
            region: self
                .source_region
                .clone()
                .unwrap_or_else(|| self.region.clone()),
        }
    }
}

/// Settles every pin in `config`, in order: publishing what is missing and
/// leaving alone what already resolves.
pub async fn run(config: TemplatePublishRunConfig) -> Result<(), RunTemplatePublishError> {
    let destination = config.destination();
    let source = config.source();
    let mut cluster_api = connect_cluster_api_from_env().await?;
    // Resolved on the first object-store call rather than up front: the steady
    // state is that every pin already resolves, and that run touches S3 not at
    // all — so it should not pay an STS round-trip, nor fail on a cluster whose
    // workload identity is broken while it has nothing to publish.
    let credentials = Credentials::default();
    let store = S3ObjectStore::new();

    info!(
        pins = config.templates.len(),
        destination_bucket = destination.bucket,
        source_bucket = source.bucket,
        "publishing pinned template releases"
    );

    for pin in &config.templates {
        let outcome = publish_pin(
            &mut cluster_api,
            &store,
            &credentials,
            &destination,
            &source,
            pin,
        )
        .await?;
        info!(
            template = pin.template.as_str(),
            content_hash = %pin.content_hash,
            %outcome,
            "template pin settled"
        );
    }

    Ok(())
}

/// One process-lifetime snapshot of the AWS default credential chain, resolved
/// on first use.
#[derive(Default)]
struct Credentials(OnceCell<ObjectCredentials>);

impl Credentials {
    async fn get(&self) -> Result<&ObjectCredentials, RunTemplatePublishError> {
        self.0
            .get_or_try_init(ObjectCredentials::from_default_chain)
            .await
            .map_err(RunTemplatePublishError::from)
    }
}

async fn publish_pin(
    cluster_api: &mut AuthenticatedClusterApiClient,
    store: &impl ObjectStore,
    credentials: &Credentials,
    destination: &BucketLocation,
    source: &BucketLocation,
    pin: &TemplatePin,
) -> Result<PinOutcome, RunTemplatePublishError> {
    if let Some(resolved) = resolved_latest_hash(cluster_api, &pin.template).await? {
        // Deliberately indifferent to whether the resolved hash matches the
        // pin: this is a bootstrap, and re-pointing `latest` at an older build
        // is a rollback an operator asks for through the builder, never a side
        // effect of a routine deploy.
        info!(
            template = pin.template.as_str(),
            pinned = %pin.content_hash,
            resolved,
            "latest already resolves; leaving this template alone"
        );
        return Ok(PinOutcome::AlreadyPublished);
    }

    let copied = ensure_artifacts(store, credentials, destination, source, pin).await?;
    let execution = verify_manifest(store, credentials, destination, pin).await?;

    // Re-checked immediately before writing the tag. Copying an artifact set can
    // take minutes, and `RegisterTemplateRelease` tag writes are
    // last-writer-wins, so a builder that published this template's first
    // `latest` during the copy would otherwise be overwritten by the pin — the
    // exact rollback the floor rule exists to prevent.
    //
    // This narrows the window to one RPC round-trip rather than closing it.
    // Closing it needs the tag write to be conditional server-side (a
    // tag-if-absent mode on `RegisterTemplateRelease`), which would also make
    // the guarantee hold for every publisher instead of just this one.
    if let Some(resolved) = resolved_latest_hash(cluster_api, &pin.template).await? {
        info!(
            template = pin.template.as_str(),
            pinned = %pin.content_hash,
            resolved,
            "latest was published while artifacts were being copied; leaving the tag alone"
        );
        return Ok(PinOutcome::AlreadyPublished);
    }
    register(cluster_api, destination, pin, execution).await?;

    Ok(if copied {
        PinOutcome::CopiedAndRegistered
    } else {
        PinOutcome::Registered
    })
}

/// Content hash `latest` currently resolves to, or `None` when the template has
/// no `latest` release yet.
async fn resolved_latest_hash(
    cluster_api: &mut AuthenticatedClusterApiClient,
    template: &TemplateName,
) -> Result<Option<String>, RunTemplatePublishError> {
    let request = FindTemplateReleaseRequest {
        caller_org_id: None,
        template_name: template.as_str().to_string(),
        runtime: RUNTIME_FIRECRACKER_ALIAS.to_string(),
        tag: DEFAULT_TEMPLATE_TAG.to_string(),
    };
    match cluster_api.find_template_release(request).await {
        // An OK response carrying no release is a protocol the server does not
        // document (it answers NOT_FOUND instead), so treat it as absent rather
        // than as a skip. Registration is an upsert, so publishing something
        // already published costs nothing; skipping something that is not
        // published leaves the cluster unable to create a sandbox.
        Ok(response) => Ok(response.into_inner().release.map(|release| release.hash)),
        Err(status) if status.code() == Code::NotFound => Ok(None),
        Err(source) => Err(RunTemplatePublishError::FindRelease {
            template: template.clone(),
            source,
        }),
    }
}

/// Puts every published artifact in the destination bucket, copying from the
/// source bucket server-side where the destination is missing or truncated.
/// Returns whether any copy ran.
async fn ensure_artifacts(
    store: &impl ObjectStore,
    credentials: &Credentials,
    destination: &BucketLocation,
    source: &BucketLocation,
    pin: &TemplatePin,
) -> Result<bool, RunTemplatePublishError> {
    let mut copied = false;
    for artifact in TemplateArtifact::PUBLISHED {
        // One key: the layout is fixed, so a given pin's artifact sits at the
        // same key in either bucket and only the bucket differs.
        let key = template_object_key("", &pin.template, pin.content_hash, artifact);

        let credentials = credentials.get().await?;
        let present = store
            .head(credentials, destination.target(&key))
            .await
            .map_err(|source| RunTemplatePublishError::ObjectStore {
                template: pin.template.clone(),
                key: key.clone(),
                source,
            })?;

        // A cluster with no separate source reads its own bucket, so both keys
        // address one object: there is nothing to compare it against and a copy
        // would be a self-copy. Its presence still has to be confirmed, though —
        // the builder uploads `manifest.json` *before* the rootfs, so a publish
        // that died in between leaves a manifest that verifies against a rootfs
        // that is not there. Registering that yields a `latest` no worker can
        // pull, and every later run skips it because `latest` now resolves.
        let self_sourced = destination.bucket == source.bucket;
        if self_sourced {
            if present.is_none() {
                return Err(RunTemplatePublishError::ObjectStore {
                    template: pin.template.clone(),
                    key: key.clone(),
                    source: ObjectStoreError::NotFound {
                        key: format!("{}/{key}", destination.bucket),
                    },
                });
            }
            continue;
        }

        let upstream = store
            .head(credentials, source.target(&key))
            .await
            .map_err(|source| RunTemplatePublishError::ObjectStore {
                template: pin.template.clone(),
                key: key.clone(),
                source,
            })?;

        let upstream_size = match (present, upstream) {
            // A complete copy already. Sizes are the check available without
            // reading either object; the manifest verification below is what
            // actually pins the contents.
            (Some(here), Some(there)) if here.size == there.size => continue,
            // Nothing upstream to copy from, but the destination already holds
            // it. A cluster can reach this state legitimately: its own builder
            // published the release and the source bucket never had it.
            (Some(_), None) => {
                info!(
                    template = pin.template.as_str(),
                    key, "artifact present in the destination and absent upstream; keeping it"
                );
                continue;
            }
            // Nothing to copy from. Named as the source so the operator checks
            // the pin against the source bucket, not the destination.
            (_, None) => {
                return Err(RunTemplatePublishError::ObjectStore {
                    template: pin.template.clone(),
                    key: key.clone(),
                    source: ObjectStoreError::NotFound {
                        key: format!("{}/{key}", source.bucket),
                    },
                });
            }
            (_, Some(there)) => there.size,
        };

        info!(
            template = pin.template.as_str(),
            source_bucket = source.bucket,
            bucket = destination.bucket,
            key,
            "copying artifact into the destination bucket"
        );
        store
            .copy_object(
                credentials,
                source.target(&key),
                destination.target(&key),
                upstream_size,
            )
            .await
            .map_err(|error| RunTemplatePublishError::ObjectStore {
                template: pin.template.clone(),
                key: key.clone(),
                source: error,
            })?;
        copied = true;
    }
    Ok(copied)
}

/// Reads the destination `manifest.json`, checks it describes the pinned release
/// — so a wrong hash in a cluster's publish list fails here instead of producing
/// a release row that points at somebody else's bytes — and returns the build's
/// execution metadata for the registration to carry.
///
/// The metadata comes from the manifest rather than anywhere else for the same
/// reason the builder takes it from there: the manifest is hash-addressed, so
/// the value frozen at registration is the one the content hash implies.
/// Registration is first-write-wins on `(template_name, hash)`, so a release
/// registered without it keeps that gap.
async fn verify_manifest(
    store: &impl ObjectStore,
    credentials: &Credentials,
    destination: &BucketLocation,
    pin: &TemplatePin,
) -> Result<Option<ExecutionMetadata>, RunTemplatePublishError> {
    let key = template_object_key(
        "",
        &pin.template,
        pin.content_hash,
        TemplateArtifact::Manifest,
    );
    let bytes = store
        .get_bytes(credentials.get().await?, destination.target(&key))
        .await
        .map_err(|source| RunTemplatePublishError::ObjectStore {
            template: pin.template.clone(),
            key: key.clone(),
            source,
        })?;
    let manifest: TemplateManifest = serde_json::from_slice(&bytes).map_err(|source| {
        RunTemplatePublishError::ManifestMalformed {
            template: pin.template.clone(),
            key: key.clone(),
            source,
        }
    })?;

    match manifest_verdict(&manifest, pin) {
        ManifestVerdict::Matches => Ok(execution_metadata(&manifest, &pin.template, &key)),
        // Rejected rather than waved through with a warning. An unverifiable pin
        // is the one thing this check exists to prevent, and template-manager's
        // own recovery path rejects a manifest whose content hash disagrees with
        // the digest directory it sits under — so accepting one here registers a
        // release the node side will not accept.
        ManifestVerdict::Unstamped => Err(RunTemplatePublishError::ManifestUnstamped {
            template: pin.template.clone(),
            key,
        }),
        ManifestVerdict::Mismatch => Err(RunTemplatePublishError::ManifestMismatch {
            template: pin.template.clone(),
            key,
            manifest_name: manifest.name.clone(),
            manifest_hash: manifest.content_hash.clone(),
        }),
    }
}

/// Execution metadata to attach to the release, or `None` when the manifest
/// carries none it can vouch for.
///
/// A schema-invalid or legacy manifest is reported and dropped rather than
/// failing the publish: the release itself is still correct and pullable, and
/// the node side treats absent execution metadata as a value to fill in later.
fn execution_metadata(
    manifest: &TemplateManifest,
    template: &TemplateName,
    key: &str,
) -> Option<ExecutionMetadata> {
    match manifest.execution_metadata() {
        Ok(execution) => execution.map(ExecutionMetadata::from),
        Err(error) => {
            warn!(
                template = template.as_str(),
                key,
                %error,
                "manifest execution metadata failed schema validation; registering the release without it"
            );
            None
        }
    }
}

/// Outcome of checking a manifest against the pin that claims it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ManifestVerdict {
    Matches,
    /// The manifest predates normalization and carries no content hash, so
    /// there is nothing to compare.
    Unstamped,
    Mismatch,
}

fn manifest_verdict(manifest: &TemplateManifest, pin: &TemplatePin) -> ManifestVerdict {
    if manifest.content_hash.is_empty() {
        return ManifestVerdict::Unstamped;
    }
    // Parsed rather than string-compared: a malformed digest in the manifest is
    // a mismatch, not an inequality that happens to read the same way.
    let hash_matches = manifest
        .content_hash
        .parse::<ContentHash>()
        .is_ok_and(|hash| hash == pin.content_hash);
    // An empty name is the same pre-normalization gap as an empty hash, so it
    // is not evidence of a mismatch on its own.
    let name_matches = manifest.name.is_empty() || manifest.name == pin.template.as_str();
    if hash_matches && name_matches {
        ManifestVerdict::Matches
    } else {
        ManifestVerdict::Mismatch
    }
}

async fn register(
    cluster_api: &mut AuthenticatedClusterApiClient,
    destination: &BucketLocation,
    pin: &TemplatePin,
    execution: Option<ExecutionMetadata>,
) -> Result<(), RunTemplatePublishError> {
    match cluster_api
        .register_template(RegisterTemplateRequest {
            system_publication_grant_id: None,
            name: pin.template.as_str().to_string(),
            description: None,
            org_id: None,
        })
        .await
    {
        Ok(_) => {}
        Err(status) if status.code() == Code::AlreadyExists => {}
        Err(source) => {
            return Err(RunTemplatePublishError::RegisterTemplate {
                template: pin.template.clone(),
                source,
            });
        }
    }

    let storage_uri = destination.storage_uri(pin);
    info!(
        template = pin.template.as_str(),
        content_hash = %pin.content_hash,
        storage_uri,
        "registering release"
    );
    cluster_api
        .register_template_release(RegisterTemplateReleaseRequest {
            system_publication_grant_id: None,
            template_name: pin.template.as_str().to_string(),
            runtime: RUNTIME_FIRECRACKER_ALIAS.to_string(),
            hash: pin.content_hash.to_string(),
            // Its own digest alongside `latest`, matching what the builder
            // attaches, so the release is addressable by hash without relying
            // on a mutable tag.
            tags: vec![
                pin.content_hash.to_string(),
                DEFAULT_TEMPLATE_TAG.to_string(),
            ],
            storage_connection_strings: vec![storage_uri],
            org_id: None,
            // Carried so a bootstrapped release is indistinguishable from a
            // builder-published one. Registration is first-write-wins, so a
            // release registered without it never gains it later.
            execution,
        })
        .await
        .map(|_| ())
        .map_err(|source| RunTemplatePublishError::RegisterTemplateRelease {
            template: pin.template.clone(),
            source,
        })
}

#[cfg(test)]
mod tests;
