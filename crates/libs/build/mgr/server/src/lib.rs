use std::collections::HashSet;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use futures_util::{FutureExt, TryStream, TryStreamExt};
use prost::Message;

use pplx_space_build_mgr::{
    BuildError, BuildOutcome, BuildPhase, PublishError, SystemPublicationGrantId, TemplateBuilder,
    TemplateOrg, TemplatePublication, blob, publish_build,
    recipe::{CopyPayload, Recipe, Step},
    register_build_template,
};
use pplx_space_build_mgr_client::BLOB_UPLOAD_LIMIT_BYTES;
use pplx_space_build_mgr_client::proto::build_manager_server::BuildManager;
use pplx_space_build_mgr_client::proto::{
    BuildState, GetBuildStatusRequest, GetBuildStatusResponse, StartBuildRequest,
    StartBuildResponse, UploadBlobChunk, UploadBlobPhase, UploadBlobResponse,
};
use pplx_space_common_jobs::{
    BackgroundWorker, Job, JobError, JobExecutor, LeaseConfig, WorkerConfig,
};
use rusqlite::{Connection, params};
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};
use tracing::Instrument as _;

pub mod jobs;
pub mod run;
use jobs::{BuildJobPayload, BuildJobResult, BuildTerminalState};
pub use run::{
    BuildMgrRunConfig, DEFAULT_DRAIN_TIMEOUT_SECONDS, DEFAULT_LISTEN, RunBuildMgrError,
    SERVICE_NAME, run,
};

#[derive(Clone)]
pub struct BuildManagerDaemonService {
    db: Arc<Mutex<Connection>>,
    worker: BackgroundWorker<BuildExecutor>,
    draining: Arc<AtomicBool>,
    blob_store: blob::BlobStore,
}

const BUILD_JOB_TIMEOUT: Duration = Duration::from_secs(6 * 60 * 60);
const BUILD_JOB_MAX_ATTEMPTS: i32 = 1;
const BUILD_JOB_LEASE_TTL: Duration = Duration::from_secs(90);
const BUILD_JOB_LEASE_HEARTBEAT: Duration = Duration::from_secs(15);
const BUILD_JOB_POLL_INTERVAL: Duration = Duration::from_secs(1);
const EXPIRED_LEASE_REAPER_INTERVAL: Duration = Duration::from_secs(15);
const BLOB_REAPER_INTERVAL: Duration = Duration::from_secs(15 * 60);
const INTERRUPTED_BUILD_ERROR: &str =
    "build-manager exited while build was running; resubmit the build";

#[derive(Clone)]
struct BuildExecutor {
    db: Arc<Mutex<Connection>>,
    backend: Arc<dyn BuildBackend>,
    blob_store: blob::BlobStore,
}

impl BuildManagerDaemonService {
    pub fn new(db_path: &str, max_concurrent_builds: usize) -> Result<Self, NewServiceError> {
        #[cfg(feature = "aws-ecr")]
        pplx_space_build_mgr::oci::validate_registry_configuration()
            .map_err(NewServiceError::OciRegistryConfiguration)?;
        let conn = open_db(db_path).map_err(NewServiceError::OpenDb)?;
        let blob_store = blob::BlobStore::from_env().map_err(NewServiceError::BlobStore)?;
        Ok(Self::from_backend(
            Arc::new(Mutex::new(conn)),
            max_concurrent_builds,
            Arc::new(RuntimeBuildBackend),
            blob_store,
        ))
    }

    fn from_backend(
        db: Arc<Mutex<Connection>>,
        max_concurrent_builds: usize,
        backend: Arc<dyn BuildBackend>,
        blob_store: blob::BlobStore,
    ) -> Self {
        let max_concurrent_builds = max_concurrent_builds.max(1);
        let executor = BuildExecutor {
            db: Arc::clone(&db),
            backend,
            blob_store: blob_store.clone(),
        };
        let worker = BackgroundWorker::new(
            Arc::clone(&db),
            executor,
            WorkerConfig::new(
                max_concurrent_builds,
                BUILD_JOB_TIMEOUT,
                BUILD_JOB_MAX_ATTEMPTS,
            )
            .with_lease(build_job_lease_config()),
        );
        Self {
            db,
            worker,
            draining: Arc::new(AtomicBool::new(false)),
            blob_store,
        }
    }

    pub fn recover_jobs(&self) -> Vec<pplx_space_common_jobs::JobId> {
        self.mark_expired_build_leases_failed();
        self.worker.recover_and_log("build-manager.template_build")
    }

    pub async fn run_queue_driver(self, shutdown_token: CancellationToken) {
        self.worker.run_queue_driver(shutdown_token).await;
    }

    pub async fn run_queue_metrics(self, shutdown_token: CancellationToken) {
        self.worker.run_queue_metrics(shutdown_token).await;
    }

    pub async fn run_terminal_job_pruner(self, shutdown_token: CancellationToken) {
        self.worker
            .run_terminal_job_pruner("build-manager.template_build", shutdown_token)
            .await;
    }

    pub async fn run_expired_lease_reaper(self, shutdown_token: CancellationToken) {
        let mut interval = tokio::time::interval(EXPIRED_LEASE_REAPER_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    self.mark_expired_build_leases_failed();
                }
                _ = shutdown_token.cancelled() => return,
            }
        }
    }

    pub async fn run_blob_reaper(self, shutdown_token: CancellationToken) {
        let mut interval = tokio::time::interval(BLOB_REAPER_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let blob_store = self.blob_store.clone();
                    match tokio::task::spawn_blocking(move || blob_store.prune_local_blobs()).await {
                        Ok(Ok(pruned)) if pruned.files > 0 => tracing::info!(
                            files = pruned.files,
                            bytes = pruned.bytes,
                            "pruned expired local build input blobs"
                        ),
                        Ok(Ok(_)) => {}
                        Ok(Err(error)) => tracing::warn!(%error, "failed to prune local build input blobs"),
                        Err(error) => tracing::warn!(%error, "local build input blob reaper task failed"),
                    }
                }
                _ = shutdown_token.cancelled() => return,
            }
        }
    }

    pub fn mark_expired_build_leases_failed(&self) -> usize {
        let expired = {
            let conn = self.db.lock().expect("build-manager db mutex poisoned");
            jobs::fail_expired_running_builds(&conn, INTERRUPTED_BUILD_ERROR).unwrap_or_else(
                |error| {
                    tracing::error!(%error, "failed to mark expired build leases failed");
                    Vec::new()
                },
            )
        };
        if !expired.is_empty() {
            tracing::warn!(
                count = expired.len(),
                "marked build jobs with expired leases failed"
            );
            // The owning process is gone; reap its staging output so the
            // multi-GB rootfs copy doesn't wait for the 24h startup backstop.
            for (job_id, subject_id) in &expired {
                if let Some(template_id) = subject_id.as_deref() {
                    pplx_space_build_mgr::remove_build_staging_dir(template_id, job_id);
                }
            }
        }
        expired.len()
    }

    pub fn start_draining(&self) {
        self.draining.store(true, Ordering::SeqCst);
    }

    pub fn running_build_count(&self) -> usize {
        let conn = self.db.lock().expect("build-manager db mutex poisoned");
        count_active_build_jobs(&conn).unwrap_or_else(|error| {
            tracing::error!(%error, "failed to count active build jobs");
            0
        })
    }

    pub fn local_running_build_count(&self) -> usize {
        let conn = self.db.lock().expect("build-manager db mutex poisoned");
        count_running_build_jobs_by_lease_owner(&conn, self.worker_id()).unwrap_or_else(|error| {
            tracing::error!(%error, "failed to count local running build jobs");
            0
        })
    }

    pub async fn wait_for_running_builds(&self, timeout: Duration) -> bool {
        self.drain_local_running_builds(timeout).await;
        self.local_running_build_count() == 0
    }

    async fn drain_local_running_builds(&self, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        let mut last_log: Option<Instant> = None;

        loop {
            let active = self.local_running_build_count();
            pplx_space_metrics::jobs::drain_outstanding(active as u64);

            if active == 0 {
                return;
            }

            if Instant::now() >= deadline {
                tracing::error!(
                    active_jobs = active,
                    "SIGTERM drain timeout reached while local build jobs are still running"
                );
                return;
            }

            let should_log = last_log.is_none_or(|t| t.elapsed() >= Duration::from_secs(5));
            if should_log {
                tracing::warn!(
                    active_jobs = active,
                    "SIGTERM received; waiting for local build job(s) to complete before exiting"
                );
                last_log = Some(Instant::now());
            }

            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    fn worker_id(&self) -> &str {
        self.worker
            .lease_worker_id()
            .expect("build-manager worker is configured with leases")
    }
}

#[derive(Debug, thiserror::Error)]
pub enum NewServiceError {
    #[cfg(feature = "aws-ecr")]
    #[error("configure OCI registries: {0}")]
    OciRegistryConfiguration(#[source] pplx_space_build_mgr::oci::EcrConfigurationError),
    #[error("open build-manager db: {0}")]
    OpenDb(#[source] rusqlite::Error),
    #[error("configure build input storage: {0}")]
    BlobStore(#[source] blob::BlobError),
}

#[async_trait]
trait BuildBackend: Send + Sync {
    async fn ensure_cluster_template(
        &self,
        template_id: &str,
        publication: TemplatePublication,
    ) -> Result<(), PublishError>;

    async fn build(
        &self,
        recipe_label: &str,
        recipe: pplx_space_build_mgr::PreparedRecipe,
        template_id: &str,
        job_id: &pplx_space_common_jobs::JobId,
        template_org: Option<TemplateOrg>,
    ) -> Result<BuildOutcome, BuildError>;

    async fn publish(
        &self,
        template_id: &str,
        content_hash: &str,
        publish_tags: &[String],
        publication: TemplatePublication,
    ) -> Result<(), PublishError>;

    /// Deletes the node's superseded local builds of the template, keeping
    /// `keep_hash`. Only called when no sibling job for the template is
    /// active (see the executor's prune gate).
    async fn prune_replaced_builds(&self, template_id: &str, keep_hash: &str);
}

struct RuntimeBuildBackend;

#[async_trait]
impl BuildBackend for RuntimeBuildBackend {
    async fn ensure_cluster_template(
        &self,
        template_id: &str,
        publication: TemplatePublication,
    ) -> Result<(), PublishError> {
        register_build_template(template_id, publication).await
    }

    async fn build(
        &self,
        recipe_label: &str,
        recipe: pplx_space_build_mgr::PreparedRecipe,
        template_id: &str,
        job_id: &pplx_space_common_jobs::JobId,
        template_org: Option<TemplateOrg>,
    ) -> Result<BuildOutcome, BuildError> {
        TemplateBuilder::new()
            .with_caller_org(template_org.map(TemplateOrg::org_id))
            .build_prepared(recipe_label, recipe, template_id, job_id)
            .await
    }

    async fn publish(
        &self,
        template_id: &str,
        content_hash: &str,
        publish_tags: &[String],
        publication: TemplatePublication,
    ) -> Result<(), PublishError> {
        publish_build(template_id, content_hash, publish_tags, publication).await
    }

    async fn prune_replaced_builds(&self, template_id: &str, keep_hash: &str) {
        pplx_space_build_mgr::prune_replaced_template_builds(template_id, keep_hash).await;
    }
}

fn parse_template_org(org_id: Option<String>) -> Result<Option<TemplateOrg>, Status> {
    let Some(org_id) = org_id else {
        // Operator CLI/system recipe builds may use trusted local bases without an org.
        // Organization-owned HTTP builds must supply their authenticated org before this RPC.
        return Ok(None);
    };
    if org_id.trim().is_empty() {
        return Err(Status::invalid_argument("org_id must not be empty"));
    }
    TemplateOrg::parse(org_id.trim())
        .map(Some)
        .map_err(|error| Status::invalid_argument(format!("invalid template org_id: {error}")))
}

#[async_trait]
impl JobExecutor for BuildExecutor {
    async fn execute(
        &self,
        job: &Job,
        _cancellation: tokio_util::sync::CancellationToken,
    ) -> Result<i64, JobError> {
        let payload: BuildJobPayload = job.payload_as()?;
        let proto_recipe =
            pplx_space_build_mgr_client::proto::Recipe::decode(payload.recipe_proto.as_slice())
                .map_err(|error| {
                    JobError::permanent(format!("malformed recipe payload: {error}"))
                })?;
        let recipe: Recipe = proto_recipe
            .try_into()
            .map_err(|error| JobError::permanent(format!("invalid recipe payload: {error}")))?;
        let recipe = pplx_space_build_mgr::PreparedRecipe::new(recipe)
            .map_err(|error| JobError::permanent(error.to_string()))?;
        let mut materialized = HashSet::new();
        for step in &recipe.recipe().steps {
            let Step::Copy {
                payload: CopyPayload::BlobSha256(sha256),
                ..
            } = &step.step
            else {
                continue;
            };
            if materialized.insert(sha256) {
                self.blob_store
                    .ensure_local(sha256)
                    .await
                    .map_err(|error| classify_blob_materialization_error(sha256, error))?;
            }
        }

        let job_id = job.job_id;
        let template_id = payload.template_id.clone();
        let terminal_template_id = template_id.clone();
        let recipe_label = payload.recipe_label.clone();
        let publish_tags = payload.publish_tags.clone();
        let template_org = payload.template_org;
        let publication = match template_org {
            Some(org) => TemplatePublication::Tenant(org),
            None => TemplatePublication::System(payload.system_publication_grant_id),
        };
        let build_span = tracing::info_span!(
            "template.build.job",
            otel.kind = "internal",
            otel.name = "template.build.job",
            "operation.name" = "template.build.job",
            job_id = %job_id,
            template_id = %template_id,
            recipe = %recipe_label,
            publish_tags_count = publish_tags.len(),
        );

        let backend = Arc::clone(&self.backend);
        let build_result = async move {
            let started_at = Instant::now();
            tracing::info!(
                job_id = %job_id,
                template_id = %template_id,
                recipe = %recipe_label,
                "build job started"
            );

            let build_result = if let Err(error) = backend
                .ensure_cluster_template(&template_id, publication)
                .await
            {
                BuildJobResult {
                    template_id: template_id.clone(),
                    state: BuildTerminalState::PublishFailed,
                    content_hash: None,
                    error: Some(error.to_string()),
                }
            } else {
                let build_backend = Arc::clone(&backend);
                let runtime = tokio::runtime::Handle::current();
                let build_template_id = template_id.clone();
                let build_recipe_label = recipe_label.clone();
                let build_job_id = job_id;
                let blocking_span = tracing::Span::current();
                let result = tokio::task::spawn_blocking(move || {
                    let build_future = build_backend
                        .build(
                            &build_recipe_label,
                            recipe,
                            &build_template_id,
                            &build_job_id,
                            template_org,
                        )
                        .instrument(blocking_span);
                    runtime.block_on(AssertUnwindSafe(build_future).catch_unwind())
                })
                .await;

                match result {
                Ok(Ok(Ok(outcome))) => match outcome.content_hash {
                    Some(hash) => {
                        let publish_started_at = Instant::now();
                        let publish_span = tracing::info_span!(
                            "template.build.phase",
                            otel.kind = "internal",
                            otel.name = "template.build.phase",
                            "operation.name" = "template.build.phase",
                            template_id = %template_id,
                            phase = %BuildPhase::Publish,
                        );
                        let publish_result = async {
                            tracing::info!(
                                template_id = %template_id,
                                phase = %BuildPhase::Publish,
                                "build phase started"
                            );
                            let result = backend
                                .publish(&template_id, &hash, &publish_tags, publication)
                                .await;
                            let step_result = pplx_space_metrics::template::TemplateBuildStepResult::from_ok(result.is_ok());
                            let result_tag = step_result.as_str();
                            pplx_space_metrics::template::build_step_completed(
                                &template_id,
                                BuildPhase::Publish.as_str(),
                                step_result,
                                publish_started_at,
                            );
                            let duration_ms = publish_started_at.elapsed().as_millis() as u64;
                            match &result {
                                Ok(()) => tracing::info!(
                                    template_id = %template_id,
                                    phase = %BuildPhase::Publish,
                                    duration_ms,
                                    result = result_tag,
                                    "build phase completed"
                                ),
                                Err(error) => tracing::warn!(
                                    template_id = %template_id,
                                    phase = %BuildPhase::Publish,
                                    duration_ms,
                                    result = result_tag,
                                    error = %error,
                                    "build phase failed"
                                ),
                            }
                            result
                        }
                        .instrument(publish_span)
                        .await;
                        match publish_result {
                            Ok(()) => BuildJobResult {
                                template_id: template_id.clone(),
                                state: BuildTerminalState::Succeeded,
                                content_hash: Some(hash),
                                error: None,
                            },
                            Err(error) => BuildJobResult {
                                template_id: template_id.clone(),
                                state: BuildTerminalState::PublishFailed,
                                content_hash: Some(hash),
                                error: Some(error.to_string()),
                            },
                        }
                    }
                    None => BuildJobResult {
                        template_id: template_id.clone(),
                        state: BuildTerminalState::Failed,
                        content_hash: None,
                        error: Some(String::from(
                            "build completed but template-manager registration did not return a content hash",
                        )),
                    },
                },
                Ok(Ok(Err(error))) => BuildJobResult {
                    template_id: template_id.clone(),
                    state: BuildTerminalState::Failed,
                    content_hash: None,
                    error: Some(error.to_string()),
                },
                Ok(Err(_)) => BuildJobResult {
                    template_id: template_id.clone(),
                    state: BuildTerminalState::Failed,
                    content_hash: None,
                    error: Some(String::from("build task panicked")),
                },
                Err(error) => BuildJobResult {
                    template_id: template_id.clone(),
                    state: BuildTerminalState::Failed,
                    content_hash: None,
                    error: Some(format!("build task failed: {error}")),
                },
                }
            };

            let result = build_result.state.as_metric_result();
            pplx_space_metrics::template::build_completed(&template_id, result, started_at);
            let result = result.as_str();
            let duration_ms = started_at.elapsed().as_millis() as u64;
            match &build_result.error {
                Some(error) => tracing::warn!(
                    job_id = %job_id,
                    template_id = %template_id,
                    duration_ms,
                    result,
                    error = %error,
                    "build job completed with failure"
                ),
                None => tracing::info!(
                    job_id = %job_id,
                    template_id = %template_id,
                    duration_ms,
                    result,
                    content_hash = build_result.content_hash.as_deref().unwrap_or(""),
                    "build job completed"
                ),
            }
            build_result
        }
        .instrument(build_span)
        .await;

        let stored_result = match job.lease_owner.as_deref() {
            Some(owner) => {
                jobs::store_build_result_if_lease_owner(&self.db, &job.job_id, owner, &build_result)
            }
            None => jobs::store_build_result(&self.db, &job.job_id, &build_result).map(|_| true),
        }
        .map_err(|error| JobError::permanent(format!("store build result: {error}")))?;
        if !stored_result {
            tracing::warn!(
                job_id = %job.job_id,
                template_id = %terminal_template_id,
                "skipped build result write because lease ownership was lost"
            );
            return Ok(0);
        }
        self.prune_replaced_builds_if_no_siblings(&job.job_id, &build_result)
            .await;
        Ok(0)
    }
}

fn classify_blob_materialization_error(sha256: &str, error: blob::BlobError) -> JobError {
    let message = format!("materialize build input {sha256}: {error}");
    if error.is_retriable() {
        JobError::retriable(message)
    } else {
        JobError::permanent(message)
    }
}

fn blob_upload_status(error: blob::BlobError) -> Status {
    let message = format!("store blob: {error}");
    match error {
        blob::BlobError::Storage(_) => Status::resource_exhausted(message),
        _ => Status::internal(message),
    }
}

impl BuildExecutor {
    /// Deletes this node's superseded local builds of the template after a
    /// fully successful build+publish. Skipped while any sibling job for the
    /// same template is pending or running: a sibling that registered its
    /// build but is still publishing would otherwise lose its artifacts to
    /// our prune. If template-manager has no cache budget, eviction cannot
    /// provide this cleanup for builder nodes.
    async fn prune_replaced_builds_if_no_siblings(
        &self,
        job_id: &pplx_space_common_jobs::JobId,
        result: &BuildJobResult,
    ) {
        if result.state != BuildTerminalState::Succeeded {
            return;
        }
        let Some(content_hash) = result.content_hash.as_deref() else {
            return;
        };
        let active_siblings = {
            let conn = self.db.lock().expect("build-manager db mutex poisoned");
            match jobs::count_other_active_builds_for_subject(&conn, &result.template_id, job_id) {
                Ok(count) => count,
                Err(error) => {
                    tracing::warn!(
                        job_id = %job_id,
                        template_id = %result.template_id,
                        %error,
                        "skipping local build prune; could not count sibling build jobs"
                    );
                    return;
                }
            }
        };
        if active_siblings > 0 {
            tracing::info!(
                job_id = %job_id,
                template_id = %result.template_id,
                active_siblings,
                "skipping local build prune while sibling builds are active"
            );
            return;
        }
        self.backend
            .prune_replaced_builds(&result.template_id, content_hash)
            .await;
    }
}

fn open_db(path: &str) -> rusqlite::Result<Connection> {
    let conn = Connection::open(path)?;
    conn.busy_timeout(Duration::from_secs(30))?;
    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
         PRAGMA synchronous=NORMAL;
         PRAGMA foreign_keys=ON;",
    )?;
    Ok(conn)
}

fn count_active_build_jobs(conn: &Connection) -> rusqlite::Result<usize> {
    conn.query_row(
        "SELECT COUNT(*) FROM jobs
         WHERE kind = ?1 AND status IN ('pending', 'running')",
        params![jobs::BUILD_JOB_KIND],
        |row| row.get::<_, i64>(0),
    )
    .map(jobs::sqlite_count_to_usize)
}

fn count_running_build_jobs_by_lease_owner(
    conn: &Connection,
    lease_owner: &str,
) -> rusqlite::Result<usize> {
    conn.query_row(
        "SELECT COUNT(*) FROM jobs
         WHERE kind = ?1
           AND status = 'running'
           AND lease_owner = ?2",
        params![jobs::BUILD_JOB_KIND, lease_owner],
        |row| row.get::<_, i64>(0),
    )
    .map(jobs::sqlite_count_to_usize)
}

fn build_job_lease_config() -> LeaseConfig {
    LeaseConfig::new(format!(
        "space-build-manager-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default()
    ))
    .with_lease_ttl(BUILD_JOB_LEASE_TTL)
    .with_heartbeat_interval(BUILD_JOB_LEASE_HEARTBEAT)
    .with_poll_interval(BUILD_JOB_POLL_INTERVAL)
    .with_expired_lease_requeue(false)
}

#[async_trait::async_trait]
impl BuildManager for BuildManagerDaemonService {
    async fn start_build(
        &self,
        request: Request<StartBuildRequest>,
    ) -> Result<Response<StartBuildResponse>, Status> {
        pplx_space_common_svc::auth::build::authorize_template_build(
            &request,
            request.get_ref().org_id.as_deref(),
        )?;
        let StartBuildRequest {
            system_publication_grant_id,
            recipe,
            template_id,
            options,
            client,
            org_id,
        } = request.into_inner();
        let inline = recipe.ok_or_else(|| Status::invalid_argument("recipe is required"))?;
        let recipe_proto = inline.clone();
        let _: Recipe = inline
            .clone()
            .try_into()
            .map_err(|error| Status::invalid_argument(format!("invalid recipe: {error}")))?;
        let client = client.unwrap_or_default();
        let recipe_label = if client.source_path.is_empty() {
            format!("<inline:{template_id}>")
        } else {
            client.source_path
        };
        let options = options.unwrap_or_default();
        let template_org = parse_template_org(org_id)?;
        let system_publication_grant_id = system_publication_grant_id
            .as_deref()
            .map(str::parse::<SystemPublicationGrantId>)
            .transpose()
            .map_err(|_| Status::invalid_argument("invalid system publication grant ID"))?;
        if template_org.is_some() && system_publication_grant_id.is_some() {
            return Err(Status::invalid_argument(
                "tenant builds cannot carry a system publication grant",
            ));
        }
        if template_id.is_empty() {
            return Err(Status::invalid_argument("template_id is required"));
        }

        if self.draining.load(Ordering::SeqCst) {
            return Err(Status::unavailable(
                pplx_space_build_mgr_client::BUILD_MANAGER_DRAINING_MESSAGE,
            ));
        }
        let active_builds = {
            let conn = self.db.lock().expect("build-manager db mutex poisoned");
            count_active_build_jobs(&conn)
                .map_err(|error| Status::internal(format!("count active build jobs: {error}")))?
        };
        let payload = BuildJobPayload {
            recipe_proto: recipe_proto.encode_to_vec(),
            recipe_label,
            template_id: template_id.clone(),
            force: options.force,
            publish_tags: options.publish_tags,
            template_org,
            system_publication_grant_id,
        };
        // Every submission gets its own job (no dedup key); the worker's
        // serialized-subjects lease claim queues same-template jobs FIFO
        // instead of rejecting them.
        let job_id =
            self.worker
                .submit_json(jobs::BUILD_JOB_KIND, &payload, Some(&template_id), None)?;
        tracing::info!(
            job_id = %job_id,
            template_id = %template_id,
            active_builds = active_builds + 1,
            "build job submitted"
        );

        Ok(Response::new(StartBuildResponse {
            job_id: job_id.to_string(),
        }))
    }

    async fn get_build_status(
        &self,
        request: Request<GetBuildStatusRequest>,
    ) -> Result<Response<GetBuildStatusResponse>, Status> {
        let request = request.into_inner();
        let job_id = request
            .job_id
            .parse::<pplx_space_common_jobs::JobId>()
            .map_err(|error| Status::invalid_argument(format!("job_id: {error}")))?;
        let job = self
            .worker
            .get_job(&job_id)?
            .ok_or_else(|| Status::not_found("build job not found"))?;

        let payload = job.payload_as::<BuildJobPayload>().map_err(|error| {
            Status::internal(format!(
                "malformed persisted build job: {}",
                error.message()
            ))
        })?;
        let result = {
            let conn = self.db.lock().expect("build-manager db mutex poisoned");
            jobs::get_build_result(&conn, &job_id)
                .map_err(|error| Status::internal(format!("read build result: {error}")))?
        };

        let (state, content_hash, error) = match (job.status.as_str(), result) {
            ("pending" | "running", _) => (BuildState::Running, String::new(), String::new()),
            ("completed", Some(result)) => (
                result.state.as_build_state(),
                result.content_hash.unwrap_or_default(),
                result.error.unwrap_or_default(),
            ),
            ("completed", None) => (
                BuildState::Failed,
                String::new(),
                "build job completed without persisted result".to_string(),
            ),
            ("failed", maybe_result) => {
                let result = maybe_result.unwrap_or(BuildJobResult {
                    template_id: payload.template_id.clone(),
                    state: BuildTerminalState::Failed,
                    content_hash: None,
                    error: job.error.clone(),
                });
                (
                    result.state.as_build_state(),
                    result.content_hash.unwrap_or_default(),
                    result
                        .error
                        .or(job.error.clone())
                        .unwrap_or_else(|| "build job failed".to_string()),
                )
            }
            (other, _) => {
                return Err(Status::internal(format!(
                    "unknown persisted build job status: {other}"
                )));
            }
        };

        Ok(Response::new(GetBuildStatusResponse {
            template_id: payload.template_id,
            state: state as i32,
            template_version: String::new(),
            content_hash,
            error,
        }))
    }

    async fn upload_blob(
        &self,
        request: Request<tonic::Streaming<UploadBlobChunk>>,
    ) -> Result<Response<UploadBlobResponse>, Status> {
        let stored = self.store_blob_stream(request.into_inner()).await?;
        Ok(Response::new(UploadBlobResponse {
            sha256: stored.sha256,
            size_bytes: stored.size_bytes,
        }))
    }
}

impl BuildManagerDaemonService {
    async fn store_blob_stream<S>(&self, mut stream: S) -> Result<blob::StoredBlob, Status>
    where
        S: TryStream<Ok = UploadBlobChunk, Error = Status> + Unpin,
    {
        let mut upload = None;
        let mut committed = false;
        while let Some(chunk) = stream
            .try_next()
            .await
            .map_err(|err| Status::invalid_argument(format!("read blob chunk: {err}")))?
        {
            if committed {
                return Err(Status::invalid_argument(
                    "blob upload continued after commit",
                ));
            }
            match UploadBlobPhase::try_from(chunk.phase) {
                Ok(UploadBlobPhase::Data) => {}
                Ok(UploadBlobPhase::Commit) => {
                    if !chunk.data.is_empty() {
                        return Err(Status::invalid_argument(
                            "blob upload commit must not contain data",
                        ));
                    }
                    committed = true;
                    continue;
                }
                Ok(UploadBlobPhase::Unspecified) => {
                    return Err(Status::invalid_argument(
                        "blob upload chunk must set an explicit DATA or COMMIT phase",
                    ));
                }
                Err(_) => return Err(Status::invalid_argument("unknown blob upload phase")),
            }
            if chunk.data.is_empty() {
                continue;
            }
            let next_size = upload
                .as_ref()
                .map_or(0, |upload: &blob::BlobUpload| upload.size_bytes())
                + chunk.data.len() as u64;
            if next_size > BLOB_UPLOAD_LIMIT_BYTES {
                return Err(Status::resource_exhausted(format!(
                    "blob upload exceeds {} bytes",
                    BLOB_UPLOAD_LIMIT_BYTES
                )));
            }
            if upload.is_none() {
                upload = Some(
                    self.blob_store
                        .begin_reserved_upload(BLOB_UPLOAD_LIMIT_BYTES)
                        .map_err(blob_upload_status)?,
                );
            }
            let upload = upload.as_mut().expect("blob upload initialized");
            upload
                .write_chunk(&chunk.data)
                .map_err(blob_upload_status)?;
        }
        if !committed {
            return Err(Status::invalid_argument("blob upload ended before commit"));
        }
        let upload = match upload {
            Some(upload) => upload,
            None => self
                .blob_store
                .begin_reserved_upload(0)
                .map_err(blob_upload_status)?,
        };
        self.blob_store
            .finish_upload(upload)
            .await
            .map_err(|err| Status::internal(format!("store blob: {err}")))
    }
}

#[cfg(test)]
mod tests;
