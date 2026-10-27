//! Explicit experimental delivery through the existing template pull service.
//! Both modes require durable read-only backing before publishing readiness.

use super::*;
use crate::chunked_rootfs::{
    ChunkSize, assemble, cache,
    delivery::{self, AuthenticatedManifest, Binding},
};
use pplx_space_common_core::types::{
    Digest, ObjectByteRange, TEMPLATE_ROOTFS_ZSTD_FILE, TemplateManifest, TemplateMetadata,
};
use pplx_space_node_api_client::chunk_representation::ChunkRepresentation;
use std::fs::File;

#[derive(Clone, Copy, Debug)]
pub(crate) enum Selection {
    Flat(ChunkSize),
    Chunks(ChunkSize, assemble::Tuning),
}

#[derive(Debug, thiserror::Error)]
#[error(
    "invalid experimental template delivery configuration: require flat:<chunk bytes> or chunks:<chunk bytes> and an explicit template prefix; range concurrency must be 1..16, request target bytes 1..32 MiB, scheduling coupled/pipeline/parallel-frames/overlap-reuse/combined"
)]
pub(crate) struct ConfigError;

#[derive(Clone, Debug)]
pub(crate) struct Config {
    selection: Selection,
    prefix: pplx_space_common_core::types::template_chunks::TemplatePrefix,
}

impl Config {
    #[cfg(test)]
    pub(crate) fn for_test(prefix: &str) -> Self {
        Self {
            selection: Selection::Chunks(ChunkSize::Mib4, assemble::Tuning::default()),
            prefix: prefix.parse().unwrap(),
        }
    }

    pub(crate) fn from_env() -> Result<Option<Self>, ConfigError> {
        let selection = match std::env::var("SPACE_TEMPLATE_EXPERIMENTAL_DELIVERY") {
            Ok(value) => value,
            Err(std::env::VarError::NotPresent) => return Ok(None),
            Err(_) => return Err(ConfigError),
        };
        let prefix = std::env::var("SPACE_TEMPLATE_EXPERIMENTAL_TEMPLATE_PREFIX")
            .map_err(|_| ConfigError)?;
        let concurrency = match std::env::var("SPACE_TEMPLATE_EXPERIMENTAL_RANGE_CONCURRENCY") {
            Ok(value) => Some(value),
            Err(std::env::VarError::NotPresent) => None,
            Err(_) => return Err(ConfigError),
        };
        let optional = |name| match std::env::var(name) {
            Ok(value) => Ok(Some(value)),
            Err(std::env::VarError::NotPresent) => Ok(None),
            Err(_) => Err(ConfigError),
        };
        let request_target = optional("SPACE_TEMPLATE_EXPERIMENTAL_REQUEST_TARGET_BYTES")?;
        let scheduling = optional("SPACE_TEMPLATE_EXPERIMENTAL_SCHEDULING")?;
        Self::from_values(
            &selection,
            &prefix,
            concurrency.as_deref(),
            request_target.as_deref(),
            scheduling.as_deref(),
        )
        .map(Some)
    }
    fn from_values(
        selection: &str,
        prefix: &str,
        concurrency: Option<&str>,
        request_target: Option<&str>,
        scheduling: Option<&str>,
    ) -> Result<Self, ConfigError> {
        let tuning = assemble::Tuning::from_values(concurrency, request_target, scheduling)
            .map_err(|_| ConfigError)?;
        let selection = match selection.parse()? {
            Selection::Chunks(size, _) => Selection::Chunks(size, tuning),
            flat @ Selection::Flat(_) => flat,
        };
        Ok(Self {
            selection,
            prefix: prefix.parse().map_err(|_| ConfigError)?,
        })
    }
    pub(crate) fn for_template(&self, name: &str) -> Option<Selection> {
        self.prefix.contains(name).then_some(self.selection)
    }
}

impl Selection {
    fn size(self) -> ChunkSize {
        match self {
            Self::Flat(size) | Self::Chunks(size, _) => size,
        }
    }
    fn label(self) -> &'static str {
        match self {
            Self::Flat(_) => "flat-full-blake3",
            Self::Chunks(_, _) => "chunks-verified-cache",
        }
    }
}

impl std::str::FromStr for Selection {
    type Err = ConfigError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (mode, size) = value.split_once(':').ok_or(ConfigError)?;
        let size = size
            .parse::<u64>()
            .map_err(|_| ConfigError)?
            .try_into()
            .map_err(|_| ConfigError)?;
        match mode {
            "flat" => Ok(Self::Flat(size)),
            "chunks" => Ok(Self::Chunks(size, assemble::Tuning::default())),
            _ => Err(ConfigError),
        }
    }
}

#[cfg(test)]
mod tests;

fn io_status(error: impl std::fmt::Display) -> Status {
    Status::internal(format!("experimental template I/O: {error}"))
}
fn integrity(error: impl std::fmt::Display) -> Status {
    Status::data_loss(format!("experimental template integrity: {error}"))
}
fn transfer(error: PullError) -> Status {
    match error {
        PullError::Failure(reason) => Status::unavailable(reason.to_string()),
        PullError::Retryable(status) | PullError::Failed(status) => status,
        PullError::HashMismatch { expected, actual } => {
            integrity(format!("expected {expected}, observed {actual}"))
        }
    }
}

fn assembly_status(error: assemble::AssemblyError) -> Status {
    match error {
        assemble::AssemblyError::Transfer(status) => status,
        assemble::AssemblyError::Io(error) => io_status(error),
        assemble::AssemblyError::Task(error) => io_status(error),
        assemble::AssemblyError::Chunk(error) => integrity(error),
        assemble::AssemblyError::Invalid(message) => integrity(message),
    }
}

struct VolumeRanges {
    client: VolmgrClient,
    subject: SubjectId,
    storage: StorageUri,
}

impl VolumeRanges {
    async fn get(
        &self,
        file: &str,
        range: ObjectByteRange,
        destination: &Path,
    ) -> Result<u64, Status> {
        let mut client = self.client.clone();
        client
            .pull_file(PullFileRequest {
                subject_id: self.subject.to_string(),
                s3_uri: build_object_uri(&self.storage, file),
                local_path: destination.to_string_lossy().into_owned(),
                raw: true,
                object_range: Some(pplx_space_volmgr_client::proto::ObjectByteRange {
                    offset: range.offset(),
                    length: range.length(),
                    object_size: range.object_size(),
                    version_id: range.version_id().map(str::to_string),
                }),
                file_mode: Some(TEMPLATE_CACHE_FILE_MODE),
                existing_file_policy: ExistingFilePolicy::LegacySkipPresent as i32,
                priority: RESTORE_PRIORITY,
                snapshot_artifact: SnapshotArtifact::Unspecified as i32,
                transfer_scope_id: None,
            })
            .await
            .map(|response| response.into_inner().bytes_downloaded)
    }
}

#[async_trait::async_trait]
impl assemble::RangeFetcher for VolumeRanges {
    async fn fetch(
        &self,
        object: Digest,
        range: ObjectByteRange,
        destination: &Path,
    ) -> Result<u64, Status> {
        // Range scratch directories are shared only with the node-local volume
        // manager via the existing template-cache group, not world-writable.
        set_template_cache_permissions(
            destination
                .parent()
                .ok_or_else(|| io_status("range parent"))?,
            TEMPLATE_CACHE_DIR_MODE,
        )
        .map_err(io_status)?;
        self.get(&delivery::object_name(object), range, destination)
            .await
    }
}

pub(super) async fn pull(
    db: Arc<Mutex<rusqlite::Connection>>,
    name: &str,
    release: &ResolvedRelease,
    selection: Selection,
    policy: RootfsBackingPolicy,
    tags: &[String],
) -> Result<PullPath, Status> {
    let template = TemplateName::parse(name.to_string()).map_err(integrity)?;
    let digest = release.hash.parse::<Digest>().map_err(integrity)?;
    let representation = release
        .chunks
        .iter()
        .find(|item| item.chunk_size() == selection.size())
        .ok_or_else(|| {
            Status::failed_precondition(
                "selected chunk representation is absent; refusing delivery fallback",
            )
        })?;
    if local_build_exists(&db, name, &release.hash) {
        return Ok(PullPath::DbHit);
    }
    {
        let conn = db.lock().map_err(|_| io_status("database mutex"))?;
        if crate::db::is_build_marked_deleting(&conn, name, &release.hash).map_err(io_status)? {
            return Err(Status::unavailable("template deletion is in progress"));
        }
    }
    // Recovery does not infer verification from filenames. An uncommitted
    // image is rebuilt from authenticated data, without admitting orphan bytes.
    let directory = Path::new(TEMPLATE_DIR).join(name).join(&release.hash);
    if directory.exists() {
        cleanup(&db, name, &release.hash, &directory).await?;
    }
    let parent = directory
        .parent()
        .ok_or_else(|| io_status("template parent"))?;
    std::fs::create_dir_all(parent).map_err(io_status)?;
    set_template_cache_permissions(parent, TEMPLATE_CACHE_DIR_MODE).map_err(io_status)?;
    let scratch = tempfile::Builder::new()
        .prefix(".chunk-transfer-")
        .tempdir_in(parent)
        .map_err(io_status)?;
    set_template_cache_permissions(scratch.path(), TEMPLATE_CACHE_DIR_MODE).map_err(io_status)?;
    // A fresh subvolume is mandatory for both benchmark paths. Failure is
    // reported before downloading; there is no writable-directory fallback.
    crate::btrfs_via_nodemgr::create_template_subvolume(name, &release.hash)
        .await
        .map_err(io_status)?;
    let result = materialize(
        Arc::clone(&db),
        &template,
        digest,
        release,
        representation,
        selection,
        policy,
        &directory,
        scratch.path(),
        tags,
    )
    .await;
    if result.is_err() {
        // The owner flight stays occupied until cleanup has finished. No Drop
        // task can wake later and remove a successful subsequent attempt.
        if let Err(error) = cleanup(&db, name, &release.hash, &directory).await {
            warn!(template = name, %error, "failed experimental pull retained for guarded recovery");
        }
    }
    result.map(|()| PullPath::FreshPull)
}

pub(super) async fn cleanup(
    db: &Arc<Mutex<rusqlite::Connection>>,
    name: &str,
    hash: &str,
    directory: &Path,
) -> Result<(), Status> {
    {
        let conn = db.lock().map_err(|_| io_status("database mutex"))?;
        if !cache::store::begin_retirement(&conn, name, hash).map_err(io_status)? {
            return Err(Status::unavailable("source has active reader leases"));
        }
    }
    if directory.exists() {
        crate::btrfs_via_nodemgr::delete_template_subvolume(name, hash)
            .await
            .map_err(io_status)?;
    }
    let conn = db.lock().map_err(|_| io_status("database mutex"))?;
    crate::db::delete_build(&conn, name, hash).map_err(io_status)?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn materialize(
    db: Arc<Mutex<rusqlite::Connection>>,
    name: &TemplateName,
    digest: Digest,
    release: &ResolvedRelease,
    representation: &ChunkRepresentation,
    selection: Selection,
    policy: RootfsBackingPolicy,
    directory: &Path,
    scratch: &Path,
    tags: &[String],
) -> Result<(), Status> {
    let started = Instant::now();
    let client = pplx_space_volmgr_client::connect(VOLMGR_SOCKET)
        .await
        .map_err(io_status)?;
    let subject = SubjectId::Template(name.clone());
    let storage = release
        .storage_uris
        .first()
        .ok_or_else(|| Status::failed_precondition("release has no flat artifact location"))?;
    let storage = StorageUri::parse(storage).map_err(integrity)?;
    let metadata_path = scratch.join("metadata.json");
    let manifest_path = scratch.join("manifest.json");
    let pulls = [
        (&metadata_path, "metadata.json"),
        (&manifest_path, "manifest.json"),
    ]
    .map(|(path, file)| {
        pull_one(
            client.clone(),
            &subject,
            build_object_uri(&storage, file),
            path.to_string_lossy().into_owned(),
            PullOptions::control("metadata"),
            tags,
        )
    });
    // Finish both RPCs before considering cleanup, including when one fails.
    let mut control_wire_bytes = 0;
    for result in futures_util::future::join_all(pulls).await {
        control_wire_bytes += result.map_err(transfer)?;
    }
    let metadata: TemplateMetadata =
        serde_json::from_slice(&tokio::fs::read(&metadata_path).await.map_err(io_status)?)
            .map_err(integrity)?;
    let mut manifest: TemplateManifest =
        serde_json::from_slice(&tokio::fs::read(&manifest_path).await.map_err(io_status)?)
            .map_err(integrity)?;
    manifest.validate_filenames().map_err(integrity)?;
    if (!manifest.bootable_artifact.is_empty()
        && manifest.bootable_artifact != TEMPLATE_ROOTFS_FILE)
        || manifest
            .files
            .get(TEMPLATE_ROOTFS_FILE)
            .is_none_or(|entry| entry.digest != release.hash)
    {
        return Err(integrity(
            "flat manifest does not identify the authorized rootfs",
        ));
    }
    // Auxiliary template files retain the ordinary pull/decompression contract.
    let auxiliary_plan: Vec<_> =
        build_manifest_pull_plan(&manifest, &storage, &scratch.to_string_lossy())
            .into_iter()
            .filter(|entry| {
                entry.manifest_key != TEMPLATE_ROOTFS_FILE
                    && entry.manifest_key != TEMPLATE_ROOTFS_ZSTD_FILE
            })
            .collect();
    let pulls = auxiliary_plan.iter().map(|entry| {
        pull_one(
            client.clone(),
            &subject,
            entry.s3_uri.clone(),
            entry.local_path.clone(),
            PullOptions::bulk(entry.phase, entry.raw),
            tags,
        )
    });
    for result in futures_util::future::join_all(pulls).await {
        control_wire_bytes += result.map_err(transfer)?;
    }
    for target in build_decompress_targets(&manifest, &scratch.to_string_lossy()) {
        if target.uncompressed_name == TEMPLATE_ROOTFS_FILE {
            continue;
        }
        check_manifest_digests_for_target(&target, TEMPLATE_ROOTFS_FILE, &release.hash)
            .map_err(transfer)?;
        let expected = target.expected_compressed_digest;
        let stats = tokio::task::spawn_blocking(move || {
            crate::rootfs_zstd::decompress_rootfs(
                Path::new(&target.compressed_path),
                Path::new(&target.uncompressed_path),
                Some(&target.expected_uncompressed_digest),
                RootfsBackingPolicy::Inherit,
            )
        })
        .await
        .map_err(io_status)?
        .map_err(integrity)?;
        if stats.compressed_hash != expected {
            return Err(integrity("auxiliary compressed digest"));
        }
    }
    let rootfs = directory.join(TEMPLATE_ROOTFS_FILE);
    let mut admission = None;
    let mut stats;
    let logical_bytes;
    match selection {
        Selection::Flat(_) => {
            let compressed = manifest
                .files
                .get(TEMPLATE_ROOTFS_ZSTD_FILE)
                .ok_or_else(|| {
                    Status::failed_precondition("flat benchmark requires published zstd rootfs")
                })?;
            let compressed_digest = compressed.digest.clone();
            let input = scratch.join(TEMPLATE_ROOTFS_ZSTD_FILE);
            let bytes = pull_one(
                client.clone(),
                &subject,
                build_object_uri(&storage, TEMPLATE_ROOTFS_ZSTD_FILE),
                input.to_string_lossy().into_owned(),
                PullOptions::bulk("rootfs", true),
                tags,
            )
            .await
            .map_err(transfer)?;
            let expected = representation.image_blake3().to_string();
            let destination = rootfs.clone();
            let result = tokio::task::spawn_blocking(move || {
                crate::rootfs_zstd::decompress_rootfs(&input, &destination, Some(&expected), policy)
            })
            .await
            .map_err(io_status)?
            .map_err(integrity)?;
            if result.compressed_hash != compressed_digest {
                return Err(integrity("flat compressed digest"));
            }
            logical_bytes = result.rootfs_size;
            stats = assemble::Stats {
                downloaded_bytes: bytes,
                downloaded_logical_bytes: logical_bytes,
                requests: 1,
                ..Default::default()
            };
        }
        Selection::Chunks(_, tuning) => {
            let ranges = VolumeRanges {
                client,
                subject,
                storage: representation.storage_prefix().parsed().clone(),
            };
            let path = scratch.join("delivery.json");
            let size = representation.manifest_bytes();
            ranges
                .get(
                    &representation.manifest_name(),
                    ObjectByteRange::new(0, size, size, None).map_err(integrity)?,
                    &path,
                )
                .await?;
            let binding = Binding {
                rootfs_digest: digest,
                index_digest: representation.index_digest(),
                manifest_digest: representation.manifest_digest(),
            };
            let delivery = Arc::new(
                AuthenticatedManifest::from_bytes(
                    &tokio::fs::read(path).await.map_err(io_status)?,
                    binding,
                )
                .map_err(integrity)?,
            );
            if delivery.image_blake3() != representation.image_blake3()
                || delivery.index().chunk_size() != selection.size()
            {
                return Err(integrity("delivery disagrees with release commitments"));
            }
            let leased = cache::LeaseSet::acquire(Arc::clone(&db), &delivery).map_err(io_status)?;
            let local = leased.pin(Path::new(TEMPLATE_DIR)).map_err(io_status)?;
            drop(leased);
            let output = File::options()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&rootfs)
                .map_err(io_status)?;
            policy.prepare_empty(&output).map_err(io_status)?;
            logical_bytes = delivery.index().logical_bytes();
            stats = assemble::assemble_with_tuning(
                Arc::clone(&delivery),
                local,
                Arc::new(output),
                scratch,
                &ranges,
                tuning,
            )
            .await
            .map_err(assembly_status)?;
            admission = Some((binding, delivery));
        }
    }
    stats.downloaded_bytes += control_wire_bytes;
    stats.requests += 2 + auxiliary_plan.len() as u64;
    if matches!(selection, Selection::Chunks(_, _)) {
        stats.downloaded_bytes += representation.manifest_bytes();
        stats.requests += 1;
    }
    // The local manifest describes present files. The flat transfer cache and
    // chunk transfer cache both live outside the final immutable subvolume.
    manifest.files.remove(TEMPLATE_ROOTFS_ZSTD_FILE);
    let mut auxiliary_files: Vec<_> = manifest
        .files
        .keys()
        .filter(|file| file.as_str() != TEMPLATE_ROOTFS_FILE)
        .map(|filename| (scratch.join(filename), directory.join(filename)))
        .collect();
    auxiliary_files.push((metadata_path.clone(), directory.join("metadata.json")));
    tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        for (source, destination) in auxiliary_files {
            pplx_space_common_io::reflink::copy_file(&source, &destination)?;
        }
        Ok(())
    })
    .await
    .map_err(io_status)?
    .map_err(io_status)?;
    manifest = crate::service::normalize_template_manifest(
        manifest,
        name.as_str(),
        &release.hash,
        &metadata,
        logical_bytes,
        None,
    )
    .map_err(integrity)?;
    let manifest_json = serde_json::to_string_pretty(&manifest).map_err(integrity)?;
    tokio::fs::write(directory.join("manifest.json"), &manifest_json)
        .await
        .map_err(io_status)?;
    normalize_template_cache_tree(directory).map_err(io_status)?;
    let durable_directory = directory.to_path_buf();
    tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        for entry in std::fs::read_dir(&durable_directory)? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                File::open(entry.path())?.sync_all()?;
            }
        }
        File::open(&durable_directory)?.sync_all()?;
        File::open(
            durable_directory
                .parent()
                .ok_or_else(|| std::io::Error::other("parent"))?,
        )?
        .sync_all()
    })
    .await
    .map_err(io_status)?
    .map_err(io_status)?;
    crate::btrfs_via_nodemgr::finalize_template_subvolume(name.as_str(), &release.hash)
        .await
        .map_err(io_status)?;
    // Persist the read-only subvolume transition before the SQLite commit.
    let sync_directory = directory.to_path_buf();
    tokio::task::spawn_blocking(move || File::open(sync_directory)?.sync_all())
        .await
        .map_err(io_status)?
        .map_err(io_status)?;
    let source =
        cache::PinnedSource::open(Path::new(TEMPLATE_DIR), name, digest).map_err(integrity)?;
    let build = crate::db::BuildUpsert {
        name: name.as_str(),
        content_hash: &release.hash,
        build_date: &manifest.build_date,
        manifest_json: &manifest_json,
        runtime: &manifest.runtime,
        arch: &manifest.arch,
        artifact_size_bytes: logical_bytes,
        state: "ready",
    };
    {
        let conn = db.lock().map_err(|_| io_status("database mutex"))?;
        let committed = if let Some((binding, manifest)) = admission {
            crate::db::upsert_verified_build(&conn, &build, binding, &manifest, &source)
                .map_err(io_status)?
                .is_some()
        } else {
            let transaction = conn.unchecked_transaction().map_err(io_status)?;
            let written =
                crate::db::upsert_build_in_transaction(&transaction, &build).map_err(io_status)?;
            transaction.commit().map_err(io_status)?;
            written
        };
        if !committed {
            return Err(Status::unavailable("template was retired during assembly"));
        }
    }
    info!(template = name.as_str(), content_hash = %digest, mode = selection.label(), chunk_size_bytes = selection.size().bytes(),
        elapsed_ms = started.elapsed().as_millis() as u64, statistics = %serde_json::to_string(&stats).map_err(io_status)?,
        "experimental template delivery committed durable read-only image");
    crate::node_state_sync::mark_template_dirty(&db, name.as_str());
    Ok(())
}
