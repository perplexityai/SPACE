use super::*;

use pplx_space_common_svc::page_cache;

/// RAII guard that removes a key from the importing_templates set on drop.
pub(super) struct ImportGuard {
    pub(super) set: Arc<Mutex<std::collections::HashSet<String>>>,
    pub(super) key: String,
}

impl Drop for ImportGuard {
    fn drop(&mut self) {
        if let Ok(mut importing) = self.set.lock() {
            importing.remove(&self.key);
        }
    }
}

type PoolSandboxListFn = dyn Fn(
        pplx_space_sbx_mgr_client::GrpcRetryPolicy,
    ) -> futures_util::future::BoxFuture<'static, Vec<sbx_proto::SandboxListElement>>
    + Send
    + Sync;

/// Lists the node's sandboxes for the pool counts `ListTemplates` and
/// `PoolStatus` report.
#[derive(Clone)]
pub(super) struct PoolSandboxLister(Arc<PoolSandboxListFn>);

impl PoolSandboxLister {
    pub(super) fn new(
        list: impl Fn(
            pplx_space_sbx_mgr_client::GrpcRetryPolicy,
        )
            -> futures_util::future::BoxFuture<'static, Vec<sbx_proto::SandboxListElement>>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        Self(Arc::new(list))
    }

    /// Pages through sandbox-manager. An unreachable or failing
    /// sandbox-manager reads as no sandboxes, so the counts report zero
    /// instead of failing the reply.
    pub(super) fn sandbox_manager() -> Self {
        Self::new(|retry_policy| {
            Box::pin(async move {
                let Ok(mut client) =
                    pplx_space_sbx_mgr_client::connect_with_retry_policy(retry_policy).await
                else {
                    return Vec::new();
                };
                pplx_space_sbx_mgr_client::list_all_sandboxes(&mut client)
                    .await
                    .unwrap_or_default()
            })
        })
    }
}

impl TemplateManagerService {
    /// The sandboxes pool counts are computed from, or none when the caller
    /// asked to omit the counts.
    pub(super) async fn pool_count_sandboxes(
        &self,
        omit_sandbox_counts: bool,
    ) -> Vec<sbx_proto::SandboxListElement> {
        if omit_sandbox_counts {
            return Vec::new();
        }
        (self.pool_sandbox_lister.0)(self.sbx_connect_retry_policy).await
    }
}

pub(super) fn content_hash_algorithm(hash: &str) -> Result<HashAlg, Status> {
    hash.parse::<Digest>()
        .map(Digest::algorithm)
        .map_err(|error| Status::invalid_argument(format!("invalid content hash: {error}")))
}

pub(super) async fn compute_template_file_hash(
    path: PathBuf,
    algorithm: HashAlg,
    context: &'static str,
    read_mode: TemplateReadMode,
) -> Result<String, Status> {
    let display_path = path.display().to_string();
    tokio::task::spawn_blocking(move || compute_content_hash(&display_path, algorithm, read_mode))
        .await
        .map_err(|e| Status::internal(format!("spawn_blocking: {e}")))?
        .map_err(|e| Status::internal(format!("{context} {}: {e}", path.display())))
}

/// Hash a file using the requested algorithm, including its canonical prefix.
pub(super) fn compute_content_hash(
    rootfs_path: &str,
    algorithm: HashAlg,
    read_mode: TemplateReadMode,
) -> std::io::Result<String> {
    use std::io::Read;

    let mut file = read_mode.reader(std::fs::File::open(rootfs_path)?)?;
    let mut hasher = crate::content_hash::ContentHasher::new(algorithm);
    let mut buf = vec![0u8; 512 * 1024]; // 512KB read buffer
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        let bytes = buf.get(..n).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "read byte count exceeded content hash buffer",
            )
        })?;
        hasher.update(bytes);
    }

    page_cache::advise_dontneed(
        file.file(),
        Path::new(rootfs_path),
        "hash template file input",
    );
    Ok(hasher.finish().to_string())
}

/// Compute a hash of snapshot.base + vmstate.base in a snapshot dir.
pub(super) fn compute_snapshot_hash(snap_dir: &str) -> std::io::Result<String> {
    use sha2::Digest;
    use std::io::Read;

    let mut hasher = sha2::Sha256::new();

    for name in ["snapshot.base", "vmstate.base"] {
        let path = format!("{snap_dir}/{name}");
        let mut file = std::fs::File::open(&path)?;
        let len = file.metadata()?.len();
        hasher.update(name.as_bytes());
        hasher.update(len.to_le_bytes());

        let mut buf = vec![0u8; 512 * 1024];
        loop {
            let n = file.read(&mut buf)?;
            if n == 0 {
                break;
            }
            let bytes = buf.get(..n).ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "read byte count exceeded snapshot hash buffer",
                )
            })?;
            hasher.update(bytes);
        }
    }

    let result = hasher.finalize();
    Ok(result.iter().map(|b| format!("{b:02x}")).collect())
}

/// Builds a `GetTemplateResponse` from a local SQLite lookup. Returns
/// `Status::not_found` when the build row isn't there. Used both for
/// the initial lookup in [`TemplateManagerService::get_template`] and
/// for the re-lookup after an auto-pull from the configured volume.
pub(super) async fn build_get_template_response(
    template_root: PathBuf,
    db: Arc<Mutex<rusqlite::Connection>>,
    template_id: String,
    req_hash: String,
    req_tag: String,
    snapshot_selector: Option<(u32, u32)>,
    policy_snapshot_memory_mib: Option<u32>,
) -> Result<crate::proto::GetTemplateResponse, Status> {
    tokio::task::spawn_blocking(move || {
        let conn = db
            .lock()
            .map_err(|e| Status::internal(format!("db mutex poisoned: {e}")))?;

        let (name, content_hash) =
            TemplateManagerService::resolve_template(&conn, &template_id, &req_hash, &req_tag)?;

        let build = crate::db::get_build(&conn, &name, &content_hash)
            .map_err(|e| Status::internal(format!("db get_build: {e}")))?
            .ok_or_else(|| Status::not_found(format!("build not found: {name}@{content_hash}")))?;

        let snapshots = crate::db::list_snapshots(&conn, &name, &content_hash)
            .map_err(|e| Status::internal(format!("db list_snapshots: {e}")))?;

        let settings = crate::db::get_template_settings(&conn, &name)
            .map_err(|e| Status::internal(format!("db get_template_settings: {e}")))?;
        let matched_snapshot = if let Some((vcpu, memory_mib)) = snapshot_selector {
            crate::db::get_snapshot(&conn, &name, &content_hash, vcpu, memory_mib)
                .map_err(|e| Status::internal(format!("db get_snapshot: {e}")))?
        } else if let Some(settings) = settings
            .as_ref()
            .filter(|settings| settings.base_snapshot_policy_set_at.is_some())
        {
            let vcpu = settings.snapshot_vcpu;
            let memory_mib = policy_snapshot_memory_mib.unwrap_or(settings.snapshot_memory_mib);
            crate::db::get_snapshot(&conn, &name, &content_hash, vcpu, memory_mib)
                .map_err(|e| Status::internal(format!("db get_snapshot: {e}")))?
        } else {
            snapshots.first().map(|s| crate::db::SnapshotRow {
                name: s.name.clone(),
                content_hash: s.content_hash.clone(),
                vcpu: s.vcpu,
                memory_mib: s.memory_mib,
                snapshot_hash: s.snapshot_hash.clone(),
                built_at: s.built_at.clone(),
            })
        };

        let template_dir_path = build_artifacts_dir(&template_root, &name, &content_hash)
            .to_string_lossy()
            .into_owned();
        let snapshot_result = matched_snapshot.as_ref().map(|snap| {
            (
                snapshot_artifacts_dir(
                    &template_root,
                    &name,
                    &content_hash,
                    snap.vcpu,
                    snap.memory_mib,
                )
                .to_string_lossy()
                .into_owned(),
                snap.snapshot_hash.clone(),
                snap.vcpu,
                snap.memory_mib,
            )
        });

        let available_snapshots: Vec<crate::proto::SnapshotInfo> = snapshots
            .into_iter()
            .map(|s| crate::proto::SnapshotInfo {
                vcpu: s.vcpu,
                memory_mib: s.memory_mib,
                snapshot_hash: s.snapshot_hash,
            })
            .collect();
        let default_auto_lifecycle = crate::db::ensure_template_settings(&conn, &name)
            .map_err(|e| Status::internal(format!("db ensure_template_settings: {e}")))?;
        let default_network_policy = crate::db::ensure_template_network_policy(&conn, &name)
            .map_err(|e| Status::internal(format!("db ensure_template_settings: {e}")))?;
        let default_network_policy_generation = settings
            .as_ref()
            .map(|settings| settings.default_network_policy_generation)
            .unwrap_or(0);
        let default_network_policy_authority = settings
            .as_ref()
            .map(|settings| settings.default_network_policy_authority)
            .unwrap_or(crate::db::PolicyAuthority::Cluster);
        let default_auto_lifecycle_authority = settings
            .as_ref()
            .map(|settings| settings.default_auto_lifecycle_authority)
            .unwrap_or(crate::db::PolicyAuthority::Cluster);
        drop(conn);

        crate::rootfs_cache::verify_rootfs_from_metadata(&template_dir_path).map_err(|error| {
            Status::failed_precondition(format!(
                "template rootfs is unavailable for {name}@{content_hash}: {error}"
            ))
        })?;

        Ok::<_, Status>(crate::proto::GetTemplateResponse {
            authorized_org_id: None,
            template: Some(crate::proto::TemplateRef {
                name: build.name,
                content_hash: Some(build.content_hash),
                tag: None,
            }),
            build_date: build.build_date,
            artifact_size_bytes: build.artifact_size_bytes,
            state: build.state,
            runtime: build.runtime,
            arch: build.arch,
            manifest_json: build.manifest_json,
            template_dir: template_dir_path,
            snapshot_dir: snapshot_result.as_ref().map(|(dir, _, _, _)| dir.clone()),
            snapshot_hash: snapshot_result.as_ref().map(|(_, hash, _, _)| hash.clone()),
            snapshot_vcpu: snapshot_result.as_ref().map(|(_, _, vcpu, _)| *vcpu),
            snapshot_memory_mib: snapshot_result
                .as_ref()
                .map(|(_, _, _, memory_mib)| *memory_mib),
            available_snapshots,
            default_auto_lifecycle: Some(common_policy_to_proto(&default_auto_lifecycle)),
            network_policy: Some(common_network_policy_to_proto(&default_network_policy)),
            network_policy_generation: default_network_policy_generation,
            network_policy_authority: default_network_policy_authority.as_proto_i32(),
            auto_lifecycle_authority: default_auto_lifecycle_authority.as_proto_i32(),
        })
    })
    .await
    .map_err(|e| Status::internal(format!("spawn_blocking: {e}")))?
}

/// Renders a "what was the request shape" hint for `not_found` errors,
/// disambiguating between hash-only / tag-only / "no selector" cases.
pub(super) fn describe_lookup(req_hash: &str, req_tag: &str) -> String {
    if !req_hash.is_empty() {
        format!("content_hash={req_hash}")
    } else if !req_tag.is_empty() {
        format!("tag={req_tag}")
    } else {
        "tag=latest (default)".to_string()
    }
}

pub(super) fn get_template_metric_tags(name: &str, req_hash: &str, req_tag: &str) -> Vec<String> {
    // The `GetTemplate` cache-hit/miss path is always caller-driven; the
    // prefetch loop never goes through this code, so the trigger is
    // hard-coded to `on_demand` here.
    pplx_space_metrics::template::request_tags(
        name,
        req_hash,
        req_tag,
        crate::volume::PullTrigger::OnDemand.as_tag(),
    )
}

pub(super) fn record_fetch_error(name: &str, req_hash: &str, req_tag: &str, reason: &'static str) {
    pplx_space_metrics::template::fetch_error(name, req_hash, req_tag, reason);
}
