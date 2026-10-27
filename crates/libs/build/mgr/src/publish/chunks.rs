//! Experimental pack publication uses the builder's ordinary credential and
//! authorized release-registration paths, with a separate durable catalog.

use super::*;
use pplx_space_common_core::types::{Digest, TEMPLATE_ROOTFS_FILE, template_chunks::ChunkSize};
use pplx_space_node_api_client::{
    chunk_representation::ChunkRepresentation, proto::TemplateChunkRepresentation,
};
use pplx_space_template_mgr::chunked_rootfs::publisher::{
    self, Namespace, ObjectSink, UploadError, UploadReceipt, store::Catalog,
};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("chunk publication configuration: {0}")]
    Config(&'static str),
    #[error("open publisher catalog: {0}")]
    Catalog(#[from] pplx_space_sqlite::SqliteError),
    #[error("publication I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("publish chunks: {0}")]
    Publish(#[from] publisher::PublishError),
    #[error("publisher task: {0}")]
    Task(#[from] tokio::task::JoinError),
    #[error("publisher registration receipt: {0}")]
    Receipt(#[from] rusqlite::Error),
    #[error("publisher representation: {0}")]
    Representation(#[from] pplx_space_node_api_client::chunk_representation::RepresentationError),
}

pub(super) struct Config {
    sizes: Vec<ChunkSize>,
    catalog: PathBuf,
}

impl Config {
    pub(super) fn from_env(template: &TemplateName) -> Result<Option<Self>, Error> {
        let sizes = match std::env::var("SPACE_TEMPLATE_CHUNK_SIZES") {
            Ok(value) => value,
            Err(std::env::VarError::NotPresent) => return Ok(None),
            Err(_) => return Err(Error::Config("chunk sizes are not valid Unicode")),
        };
        let prefix: pplx_space_common_core::types::template_chunks::TemplatePrefix =
            std::env::var("SPACE_TEMPLATE_EXPERIMENTAL_TEMPLATE_PREFIX")
                .map_err(|_| Error::Config("an explicit experimental template prefix is required"))?
                .parse()
                .map_err(|_| Error::Config("invalid experimental template prefix"))?;
        if !prefix.contains(template.as_str()) {
            return Ok(None);
        }
        let mut parsed = Vec::new();
        for size in sizes.split(',') {
            let size = size
                .parse::<u64>()
                .map_err(|_| Error::Config("sizes must be comma-separated byte counts"))?
                .try_into()
                .map_err(|_| Error::Config("unsupported chunk size"))?;
            if parsed.contains(&size) || parsed.len() == 5 {
                return Err(Error::Config("sizes must be unique and at most five"));
            }
            parsed.push(size);
        }
        let catalog = std::env::var_os("SPACE_TEMPLATE_CHUNK_PUBLISHER_DB")
            .map(PathBuf::from)
            .filter(|path| path.is_absolute())
            .ok_or(Error::Config(
                "an absolute SPACE_TEMPLATE_CHUNK_PUBLISHER_DB path is required",
            ))?;
        Ok(Some(Self {
            sizes: parsed,
            catalog,
        }))
    }
}

struct Sink {
    runtime: tokio::runtime::Handle,
    storage: StorageUri,
    template: TemplateName,
    store: S3ObjectStore,
}

impl ObjectSink for Sink {
    fn namespace(&self) -> Namespace {
        Namespace::new(&self.storage)
    }
    fn put(
        &mut self,
        object: &str,
        source: &Path,
        digest: Digest,
    ) -> Result<UploadReceipt, UploadError> {
        self.runtime.block_on(async {
            let key = format!("{}/{}", self.storage.key_prefix, object);
            // Exact-object credentials carry append-only conditions for this
            // namespace. Refresh per object instead of retaining expired STS
            // credentials across a long compression/publication operation.
            let creds = object_store_credentials(
                &self.storage.bucket,
                &key,
                false,
                crate::object_store::CredentialSubject::Template(&self.template),
                StorageAccess::ReadWrite,
            )
            .await
            .map_err(std::io::Error::other)?;
            let target = ObjectTarget {
                bucket: &self.storage.bucket,
                key: &key,
                region: &self.storage.region,
                endpoint: self.storage.endpoint.as_deref(),
                path_style: self.storage.path_style,
            };
            let receipt = self
                .store
                .put_immutable_file(&creds, target, source, digest)
                .await
                .map_err(std::io::Error::other)?;
            Ok(UploadReceipt {
                bytes: receipt.bytes,
                version_id: receipt.version_id,
            })
        })
    }
}

pub(super) struct Pending {
    catalog: Catalog,
    namespace: Namespace,
    pub(super) representations: Vec<TemplateChunkRepresentation>,
}

impl Pending {
    pub(super) async fn acknowledge(mut self) -> Result<(), Error> {
        tokio::task::spawn_blocking(move || {
            let digests = self
                .representations
                .iter()
                .map(|value| {
                    value
                        .manifest_digest
                        .parse::<Digest>()
                        .map_err(|_| Error::Config("invalid generated manifest digest"))
                })
                .collect::<Result<Vec<_>, _>>()?;
            self.catalog
                .acknowledge_registration(self.namespace.as_str(), &digests)?;
            Ok::<_, Error>(())
        })
        .await?
    }
}

pub(super) async fn publish(
    config: Config,
    storage: &ObjectStoreConfig,
    template: &TemplateName,
    hash: ContentHash,
    build_dir: &Path,
) -> Result<Pending, Error> {
    let mut catalog = Catalog::open(&config.catalog).await?;
    let release_prefix = template_object_prefix(&storage.key_prefix, template, hash);
    let (template_prefix, _) = release_prefix
        .rsplit_once('/')
        .ok_or(Error::Config("invalid release prefix"))?;
    let storage = StorageUri {
        bucket: storage.bucket.clone(),
        key_prefix: format!("{template_prefix}/chunks/v1"),
        region: storage.region.clone(),
        endpoint: storage.endpoint.clone(),
        path_style: storage.path_style,
    };
    let mut sink = Sink {
        runtime: tokio::runtime::Handle::current(),
        template: template.clone(),
        storage,
        store: S3ObjectStore::new(),
    };
    let source = build_dir.join(TEMPLATE_ROOTFS_FILE);
    let staging_parent = config
        .catalog
        .parent()
        .ok_or(Error::Config("catalog has no parent"))?
        .to_path_buf();
    tokio::task::spawn_blocking(move || {
        let scratch = tempfile::Builder::new()
            .prefix("chunk-publication-")
            .tempdir_in(staging_parent)?;
        let mut representations = Vec::new();
        for size in config.sizes {
            let uploaded = catalog.publish(
                std::fs::File::open(&source)?,
                &mut sink,
                scratch.path(),
                size,
                hash.digest(),
            )?;
            let representation = TemplateChunkRepresentation {
                format_version: 1,
                chunk_size_bytes: size.bytes(),
                index_digest: uploaded.binding.index_digest.to_string(),
                manifest_digest: uploaded.binding.manifest_digest.to_string(),
                manifest_bytes: uploaded.manifest_bytes.len() as u64,
                image_blake3: uploaded.image_blake3.to_string(),
                storage_prefix: sink.storage.to_string(),
            };
            ChunkRepresentation::try_from(representation.clone())?;
            tracing::info!(chunk_size_bytes = size.bytes(), statistics = ?uploaded.statistics,
                "uploaded immutable template chunk representation");
            representations.push(representation);
        }
        Ok(Pending {
            namespace: sink.namespace(),
            catalog,
            representations,
        })
    })
    .await?
}

/// An ambiguous registration reply is resolved through authorized exact-hash
/// lookup. Do not replay tag changes that might overwrite a newer publication.
pub(super) async fn confirm(
    cluster_api: &mut AuthenticatedClusterApiClient,
    template: &TemplateName,
    hash: ContentHash,
    publication: TemplatePublication,
    expected: &[TemplateChunkRepresentation],
) -> bool {
    let org = publication.org().map(|owner| owner.org_id().to_string());
    let response = match cluster_api
        .find_template_release_by_hash(FindTemplateReleaseByHashRequest {
            template_name: template.to_string(),
            hash: hash.to_string(),
            caller_org_id: org.clone(),
            generic_only: false,
        })
        .await
    {
        Ok(response) => response.into_inner(),
        Err(_) => return false,
    };
    if response.authorized_org_id != org {
        return false;
    }
    let Some(release) = response.release else {
        return false;
    };
    release.template_name == template.as_str()
        && release.hash == hash.to_string()
        && release.runtime == RUNTIME_FIRECRACKER_ALIAS
        && expected
            .iter()
            .all(|item| release.chunk_representations.contains(item))
}
