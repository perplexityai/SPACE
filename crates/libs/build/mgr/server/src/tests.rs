use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};

use futures_util::stream::iter;
use pplx_space_build_mgr_client::proto::{
    BuildOptions, ClientContext, Recipe as ProtoRecipe, UploadBlobChunk, UploadBlobPhase, recipe,
};
use tonic::Code;

use super::*;

use pplx_space_test_env::TestResult;

mod auth_rpc;

#[cfg(feature = "aws-ecr")]
#[test]
fn invalid_ecr_configuration_prevents_service_startup() {
    const ECR_REPOSITORIES_ENV: &str = "SPACE_BUILDER_ECR_REPOSITORIES";

    if !pplx_space_test_env::enter_test_process(&[(
        ECR_REPOSITORIES_ENV,
        Some(std::ffi::OsStr::new("not JSON")),
    )]) {
        return;
    }
    let tempdir = tempfile::tempdir().expect("create startup fixture");
    let db_path = tempdir.path().join("build-manager.db");

    let error = match BuildManagerDaemonService::new(db_path.to_str().expect("UTF-8 path"), 1) {
        Ok(_) => panic!("invalid ECR configuration should prevent startup"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        NewServiceError::OciRegistryConfiguration(_)
    ));
}

type PublishCall = (String, String, Vec<String>, TemplatePublication);

impl BuildManagerDaemonService {
    pub(crate) fn with_backend(
        max_concurrent_builds: usize,
        backend: Arc<dyn BuildBackend>,
    ) -> Result<Self, NewServiceError> {
        let conn = open_in_memory_db().map_err(NewServiceError::OpenDb)?;
        Ok(Self::from_backend(
            Arc::new(Mutex::new(conn)),
            max_concurrent_builds,
            backend,
            blob::BlobStore::local(),
        ))
    }

    pub(crate) fn with_backend_and_db_path(
        db_path: &str,
        max_concurrent_builds: usize,
        backend: Arc<dyn BuildBackend>,
    ) -> Result<Self, NewServiceError> {
        let mut conn = open_db(db_path).map_err(NewServiceError::OpenDb)?;
        initialize_schema(&mut conn);
        Ok(Self::from_backend(
            Arc::new(Mutex::new(conn)),
            max_concurrent_builds,
            backend,
            blob::BlobStore::local(),
        ))
    }
}

fn open_in_memory_db() -> rusqlite::Result<Connection> {
    let mut conn = Connection::open_in_memory()?;
    initialize_schema(&mut conn);
    Ok(conn)
}

pub(super) fn initialize_schema(conn: &mut Connection) {
    if conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name = 'jobs')",
            [],
            |row| row.get::<_, bool>(0),
        )
        .unwrap()
    {
        return;
    }
    let service = sqlx::migrate!("../../../../../db/build-manager");
    let common = sqlx::migrate!("../../../../../db/common");
    for migration in common
        .iter()
        .chain(service.iter())
        .filter(|migration| migration.migration_type.is_up_migration())
    {
        conn.execute_batch(migration.sql.as_ref()).unwrap();
    }
}

#[derive(Default)]
struct MockBuildBackend {
    build_results: Mutex<VecDeque<Result<BuildOutcome, BuildError>>>,
    build_calls: Mutex<Vec<String>>,
    environment_calls: Mutex<Vec<Option<pplx_space_common_core::types::EnvironmentReference>>>,
    cluster_template_results: Mutex<VecDeque<Result<(), PublishError>>>,
    cluster_template_calls: Mutex<Vec<(String, TemplatePublication)>>,
    publish_results: Mutex<VecDeque<Result<(), PublishError>>>,
    publish_calls: Mutex<Vec<PublishCall>>,
    prune_calls: Mutex<Vec<(String, String)>>,
}

#[derive(Default)]
struct BlockingBuildBackend {
    started: std::sync::atomic::AtomicUsize,
    released: AtomicBool,
    release: tokio::sync::Notify,
}

impl BlockingBuildBackend {
    async fn wait_started(&self) {
        for _ in 0..100 {
            if self.started_count() >= 1 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("blocking build backend did not start");
    }

    fn started_count(&self) -> usize {
        self.started.load(Ordering::SeqCst)
    }

    fn release(&self) {
        self.released.store(true, Ordering::SeqCst);
        self.release.notify_waiters();
    }
}

impl MockBuildBackend {
    fn with_build_result(result: Result<BuildOutcome, BuildError>) -> Arc<Self> {
        Arc::new(Self {
            build_results: Mutex::new(VecDeque::from([result])),
            ..Self::default()
        })
    }

    fn push_publish_result(self: &Arc<Self>, result: Result<(), PublishError>) {
        self.publish_results.lock().unwrap().push_back(result);
    }

    fn push_cluster_template_result(self: &Arc<Self>, result: Result<(), PublishError>) {
        self.cluster_template_results
            .lock()
            .unwrap()
            .push_back(result);
    }
}

#[async_trait]
impl BuildBackend for MockBuildBackend {
    async fn ensure_cluster_template(
        &self,
        template_id: &str,
        publication: TemplatePublication,
    ) -> Result<(), PublishError> {
        self.cluster_template_calls
            .lock()
            .unwrap()
            .push((template_id.to_string(), publication));
        self.cluster_template_results
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(Ok(()))
    }

    async fn prune_replaced_builds(&self, template_id: &str, keep_hash: &str) {
        self.prune_calls
            .lock()
            .unwrap()
            .push((template_id.to_string(), keep_hash.to_string()));
    }

    async fn build(
        &self,
        _recipe_label: &str,
        _recipe: pplx_space_build_mgr::PreparedRecipe,
        template_id: &str,
        _job_id: &pplx_space_common_jobs::JobId,
        template_org: Option<TemplateOrg>,
        environment: Option<pplx_space_common_core::types::EnvironmentReference>,
    ) -> Result<BuildOutcome, BuildError> {
        assert!(
            self.cluster_template_calls
                .lock()
                .unwrap()
                .iter().any(|(name, publication)| name == template_id && publication.org() == template_org),
            "the persisted caller org must reach both template registration and base materialization"
        );
        self.environment_calls.lock().unwrap().push(environment);
        self.build_calls
            .lock()
            .unwrap()
            .push(template_id.to_string());
        self.build_results
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| Ok(BuildOutcome { content_hash: None }))
    }

    async fn publish(
        &self,
        template_id: &str,
        content_hash: &str,
        publish_tags: &[String],
        publication: TemplatePublication,
    ) -> Result<(), PublishError> {
        self.publish_calls.lock().unwrap().push((
            template_id.to_string(),
            content_hash.to_string(),
            publish_tags.to_vec(),
            publication,
        ));
        self.publish_results
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(Ok(()))
    }
}

#[async_trait]
impl BuildBackend for BlockingBuildBackend {
    async fn ensure_cluster_template(
        &self,
        _template_id: &str,
        _publication: TemplatePublication,
    ) -> Result<(), PublishError> {
        Ok(())
    }

    async fn prune_replaced_builds(&self, _template_id: &str, _keep_hash: &str) {}

    async fn build(
        &self,
        _recipe_label: &str,
        _recipe: pplx_space_build_mgr::PreparedRecipe,
        _template_id: &str,
        _job_id: &pplx_space_common_jobs::JobId,
        _template_org: Option<TemplateOrg>,
        _environment: Option<pplx_space_common_core::types::EnvironmentReference>,
    ) -> Result<BuildOutcome, BuildError> {
        self.started.fetch_add(1, Ordering::SeqCst);
        while !self.released.load(Ordering::SeqCst) {
            self.release.notified().await;
        }
        Ok(BuildOutcome {
            content_hash: Some(
                "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
                    .to_string(),
            ),
        })
    }

    async fn publish(
        &self,
        _template_id: &str,
        _content_hash: &str,
        _publish_tags: &[String],
        _publication: TemplatePublication,
    ) -> Result<(), PublishError> {
        Ok(())
    }
}

async fn wait_for_terminal_status(
    service: &BuildManagerDaemonService,
    job_id: &str,
) -> Result<GetBuildStatusResponse, tonic::Status> {
    for _ in 0..50 {
        let status = service
            .get_build_status(Request::new(GetBuildStatusRequest {
                job_id: job_id.to_string(),
            }))
            .await?
            .into_inner();
        let state = BuildState::try_from(status.state).expect("known build state");
        if matches!(
            state,
            BuildState::Succeeded | BuildState::Failed | BuildState::PublishFailed
        ) {
            return Ok(status);
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("build job did not reach a terminal state");
}

#[tokio::test]
async fn stalled_empty_data_marker_does_not_start_blob_upload() -> TestResult {
    let tempdir = tempfile::tempdir()?;
    let blob_dir = tempdir.path().join("blobs");
    let service = BuildManagerDaemonService::from_backend(
        Arc::new(Mutex::new(open_in_memory_db()?)),
        1,
        Arc::new(MockBuildBackend::default()),
        blob::BlobStore::local_in(blob_dir.clone()),
    );
    let waiting_for_more_data = Arc::new(tokio::sync::Notify::new());
    let stream_wait = Arc::clone(&waiting_for_more_data);
    let mut poll_count = 0;
    let stream = futures_util::stream::poll_fn(move |_| {
        poll_count += 1;
        match poll_count {
            1 => std::task::Poll::Ready(Some(Ok(UploadBlobChunk {
                data: Vec::new(),
                phase: UploadBlobPhase::Data as i32,
            }))),
            2 => {
                stream_wait.notify_one();
                std::task::Poll::Pending
            }
            _ => std::task::Poll::Pending,
        }
    });
    let upload = tokio::spawn(async move { service.store_blob_stream(stream).await });

    waiting_for_more_data.notified().await;

    assert!(!blob_dir.exists());
    upload.abort();
    assert!(matches!(upload.await, Err(ref error) if error.is_cancelled()));
    Ok(())
}

#[tokio::test]
async fn interrupted_explicit_blob_upload_is_not_published() -> TestResult {
    let tempdir = tempfile::tempdir()?;
    let blob_dir = tempdir.path().join("blobs");
    let service = BuildManagerDaemonService::from_backend(
        Arc::new(Mutex::new(open_in_memory_db()?)),
        1,
        Arc::new(MockBuildBackend::default()),
        blob::BlobStore::local_in(blob_dir.clone()),
    );
    let stream = iter([Ok(UploadBlobChunk {
        data: b"partial upload".to_vec(),
        phase: UploadBlobPhase::Data as i32,
    })]);

    let result = service.store_blob_stream(stream).await;

    assert!(matches!(result, Err(ref error) if error.code() == Code::InvalidArgument));
    assert_eq!(std::fs::read_dir(blob_dir)?.count(), 0);
    Ok(())
}

#[tokio::test]
async fn committed_blob_upload_is_published() -> TestResult {
    let tempdir = tempfile::tempdir()?;
    let blob_dir = tempdir.path().join("blobs");
    let service = BuildManagerDaemonService::from_backend(
        Arc::new(Mutex::new(open_in_memory_db()?)),
        1,
        Arc::new(MockBuildBackend::default()),
        blob::BlobStore::local_in(blob_dir.clone()),
    );
    let chunks = [
        UploadBlobChunk {
            data: b"complete upload".to_vec(),
            phase: UploadBlobPhase::Data as i32,
        },
        UploadBlobChunk {
            data: Vec::new(),
            phase: UploadBlobPhase::Commit as i32,
        },
    ];

    let stored = service
        .store_blob_stream(iter(chunks.into_iter().map(Ok)))
        .await?;

    assert_eq!(stored.size_bytes, 15);
    assert_eq!(std::fs::read_dir(blob_dir)?.count(), 1);
    Ok(())
}

#[tokio::test]
async fn unspecified_blob_upload_phase_is_rejected() -> TestResult {
    let tempdir = tempfile::tempdir()?;
    let blob_dir = tempdir.path().join("blobs");
    let service = BuildManagerDaemonService::from_backend(
        Arc::new(Mutex::new(open_in_memory_db()?)),
        1,
        Arc::new(MockBuildBackend::default()),
        blob::BlobStore::local_in(blob_dir.clone()),
    );
    let chunks = [
        UploadBlobChunk {
            data: b"implicit commit".to_vec(),
            phase: UploadBlobPhase::Unspecified as i32,
        },
        UploadBlobChunk {
            data: Vec::new(),
            phase: UploadBlobPhase::Commit as i32,
        },
    ];

    let result = service
        .store_blob_stream(iter(chunks.into_iter().map(Ok)))
        .await;

    assert!(matches!(result, Err(ref error) if error.code() == Code::InvalidArgument));
    assert!(!blob_dir.exists() || std::fs::read_dir(&blob_dir)?.count() == 0);
    Ok(())
}

async fn wait_for_inflight_count(service: &BuildManagerDaemonService, expected: usize) {
    for _ in 0..50 {
        if service.worker.inflight() == expected {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(service.worker.inflight(), expected);
}

fn test_recipe() -> ProtoRecipe {
    ProtoRecipe {
        metadata: Some(recipe::Metadata { description: None }),
        build_config: Some(recipe::BuildConfig::default()),
        origin: Some(recipe::Origin {
            kind: Some(recipe::origin::Kind::FromTemplate(
                recipe::origin::FromTemplate {
                    base: "seed".to_string(),
                },
            )),
        }),
        steps: vec![],
    }
}

fn authorized_request<T>(body: T) -> Request<T> {
    let mut request = Request::new(body);
    request.extensions_mut().insert(
        pplx_space_common_svc::auth::build::BuildAuthenticatedCaller::trusted_by_configuration(),
    );
    request
}

fn start_build_request(
    recipe: Option<ProtoRecipe>,
    template_id: &str,
    publish_tags: Vec<String>,
) -> StartBuildRequest {
    StartBuildRequest {
        environment: None,
        system_publication_grant_id: None,
        tenant_publication_grant_id: None,
        recipe,
        template_id: template_id.to_string(),
        options: Some(BuildOptions {
            force: false,
            publish_tags,
        }),
        client: Some(ClientContext {
            source_path: "/tmp/recipe.toml".to_string(),
            cli_version: String::new(),
            hostname: String::new(),
        }),
        org_id: None,
    }
}

fn build_job_payload(template_id: &str) -> BuildJobPayload {
    BuildJobPayload {
        recipe_proto: test_recipe().encode_to_vec(),
        recipe_label: "/tmp/recipe.toml".to_string(),
        template_id: template_id.to_string(),
        force: false,
        publish_tags: vec![],
        template_org: None,
        system_publication_grant_id: None,
        tenant_publication_grant_id: None,
    }
}

#[tokio::test(flavor = "current_thread")]
async fn prune_runs_after_success_when_no_sibling_active() -> TestResult {
    let backend = MockBuildBackend::with_build_result(Ok(BuildOutcome {
        content_hash: Some(
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
        ),
    }));
    backend.push_publish_result(Ok(()));
    let service = BuildManagerDaemonService::with_backend(1, backend.clone())?;

    let response = service
        .start_build(authorized_request(start_build_request(
            Some(test_recipe()),
            "seed",
            vec![],
        )))
        .await?
        .into_inner();
    let status = wait_for_terminal_status(&service, &response.job_id).await?;
    assert_eq!(
        BuildState::try_from(status.state).expect("known build state"),
        BuildState::Succeeded
    );

    assert_eq!(
        backend.prune_calls.lock().unwrap().as_slice(),
        &[(
            "seed".to_string(),
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
        )]
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn prune_skipped_while_sibling_same_template_job_active() -> TestResult {
    let backend = MockBuildBackend::with_build_result(Ok(BuildOutcome {
        content_hash: Some(
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
        ),
    }));
    backend.push_publish_result(Ok(()));
    let service = BuildManagerDaemonService::with_backend(2, backend.clone())?;
    // A queued sibling build for the same template: it may later register
    // and publish an artifact the prune would delete, so prune must wait
    // for a build that finishes with no siblings in flight.
    {
        let conn = service.db.lock().expect("test db lock");
        conn.execute(
            "INSERT INTO jobs (job_id, kind, payload, subject_id, status)
             VALUES ('sibling-job', ?1, '{}', 'seed', 'pending')",
            params![jobs::BUILD_JOB_KIND],
        )?;
    }

    let response = service
        .start_build(authorized_request(start_build_request(
            Some(test_recipe()),
            "seed",
            vec![],
        )))
        .await?
        .into_inner();
    let status = wait_for_terminal_status(&service, &response.job_id).await?;
    assert_eq!(
        BuildState::try_from(status.state).expect("known build state"),
        BuildState::Succeeded
    );

    assert!(
        backend.prune_calls.lock().unwrap().is_empty(),
        "prune must be skipped while a same-template sibling job is active"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn build_defaults_to_publish_enabled() -> TestResult {
    let backend = MockBuildBackend::with_build_result(Ok(BuildOutcome {
        content_hash: Some(
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
        ),
    }));
    backend.push_publish_result(Ok(()));
    let service = BuildManagerDaemonService::with_backend(1, backend.clone())?;

    let response = service
        .start_build(authorized_request(start_build_request(
            Some(test_recipe()),
            "seed",
            vec!["latest".to_string()],
        )))
        .await?
        .into_inner();

    let status = wait_for_terminal_status(&service, &response.job_id).await?;
    assert_eq!(
        BuildState::try_from(status.state).expect("known build state"),
        BuildState::Succeeded
    );
    assert_eq!(
        backend.publish_calls.lock().unwrap().as_slice(),
        &[(
            "seed".to_string(),
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            vec!["latest".to_string()],
            TemplatePublication::System(None),
        )]
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn build_publish_carries_publication_authority() -> TestResult {
    let org = TemplateOrg::parse("018f0000-0000-7000-8000-000000000002")?;
    let grant = SystemPublicationGrantId::new();
    for publication in [
        TemplatePublication::Tenant { org, grant: None },
        TemplatePublication::Tenant {
            org,
            grant: Some(TenantPublicationGrantId::new()),
        },
        TemplatePublication::System(Some(grant)),
    ] {
        let hash = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let backend = MockBuildBackend::with_build_result(Ok(BuildOutcome {
            content_hash: Some(hash.to_string()),
        }));
        let service = BuildManagerDaemonService::with_backend(1, backend.clone())?;
        let mut request = start_build_request(
            Some(test_recipe()),
            "scope-output",
            vec!["latest".to_string()],
        );
        request.org_id = publication.org().map(|org| org.org_id().to_string());
        request.system_publication_grant_id = publication.system_grant().map(|id| id.to_string());
        request.tenant_publication_grant_id = publication.tenant_grant().map(|id| id.to_string());
        let response = service
            .start_build(authorized_request(request))
            .await?
            .into_inner();
        let status = wait_for_terminal_status(&service, &response.job_id).await?;
        assert_eq!(BuildState::try_from(status.state)?, BuildState::Succeeded);
        assert_eq!(
            backend.cluster_template_calls.lock().unwrap().as_slice(),
            &[("scope-output".to_string(), publication)]
        );
        assert_eq!(
            backend.publish_calls.lock().unwrap().as_slice(),
            &[(
                "scope-output".to_string(),
                hash.to_string(),
                vec!["latest".to_string()],
                publication
            )]
        );
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn template_registration_failure_stops_before_local_build() -> TestResult {
    let backend = MockBuildBackend::with_build_result(Ok(BuildOutcome {
        content_hash: Some(
            "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string(),
        ),
    }));
    backend.push_cluster_template_result(Err(PublishError::RegisterTemplate(
        Status::permission_denied("template belongs to a different organization"),
    )));
    let service = BuildManagerDaemonService::with_backend(1, backend.clone())?;

    let response = service
        .start_build(authorized_request(start_build_request(
            Some(test_recipe()),
            "customer-template",
            vec!["latest".to_string()],
        )))
        .await?
        .into_inner();

    let status = wait_for_terminal_status(&service, &response.job_id).await?;

    assert_eq!(
        BuildState::try_from(status.state).expect("known build state"),
        BuildState::PublishFailed
    );
    assert!(backend.build_calls.lock().unwrap().is_empty());
    assert!(backend.publish_calls.lock().unwrap().is_empty());
    assert!(
        status
            .error
            .contains("template belongs to a different organization")
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn build_rejects_missing_recipe() -> TestResult {
    let backend = MockBuildBackend::default();
    let service = BuildManagerDaemonService::with_backend(1, Arc::new(backend))?;

    let error = service
        .start_build(authorized_request(start_build_request(
            None,
            "seed",
            vec![],
        )))
        .await
        .expect_err("missing recipe should be invalid");

    assert_eq!(error.code(), Code::InvalidArgument);
    assert!(error.message().contains("recipe is required"));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn build_materializes_blob_inputs_before_calling_backend() -> TestResult {
    let sha256 = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
    assert!(!blob::blob_path(sha256)?.exists());
    let backend = MockBuildBackend::with_build_result(Ok(BuildOutcome {
        content_hash: Some(
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
        ),
    }));
    let service = BuildManagerDaemonService::with_backend(1, backend)?;
    let mut recipe = test_recipe();
    recipe.steps.push(recipe::Step {
        display_name: None,
        env_refs: Vec::new(),
        body: Some(recipe::step::Body::Copy(recipe::Copy {
            payload: Some(recipe::copy::Payload::BlobSha256(sha256.to_string())),
            dest: "/tmp/input".to_string(),
            source_path: None,
        })),
    });

    let response = service
        .start_build(authorized_request(start_build_request(
            Some(recipe),
            "seed",
            vec![],
        )))
        .await?
        .into_inner();
    let status = wait_for_terminal_status(&service, &response.job_id).await?;

    assert_eq!(
        BuildState::try_from(status.state).expect("known build state"),
        BuildState::Failed
    );
    assert!(status.error.contains("is not available locally"));
    Ok(())
}

#[test]
fn transient_blob_materialization_error_is_retriable() {
    let error = blob::BlobError::CredentialExchange(
        pplx_space_build_mgr::object_store::CredentialExchangeError::Exchange(
            tonic::Status::unavailable("credmgr unavailable"),
        ),
    );

    let error = classify_blob_materialization_error(&"a".repeat(64), error);

    assert!(matches!(error, JobError::Retriable(_)));
}

#[test]
fn missing_blob_materialization_error_is_permanent() {
    let sha256 = "a".repeat(64);
    let error = blob::BlobError::NotShared {
        sha256: sha256.clone(),
    };

    let error = classify_blob_materialization_error(&sha256, error);

    assert!(matches!(error, JobError::Permanent(_)));
}

#[tokio::test(flavor = "current_thread")]
async fn publish_failure_is_distinct_terminal_state_and_keeps_hash() -> TestResult {
    let backend = MockBuildBackend::with_build_result(Ok(BuildOutcome {
        content_hash: Some(
            "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".to_string(),
        ),
    }));
    backend.push_publish_result(Err(PublishError::RegisterTemplateRelease(
        tonic::Status::new(Code::AlreadyExists, "tag 'latest' already exists"),
    )));
    let service = BuildManagerDaemonService::with_backend(1, backend)?;

    let response = service
        .start_build(authorized_request(start_build_request(
            Some(test_recipe()),
            "seed",
            vec!["latest".to_string()],
        )))
        .await?
        .into_inner();

    let status = wait_for_terminal_status(&service, &response.job_id).await?;
    assert_eq!(
        BuildState::try_from(status.state).expect("known build state"),
        BuildState::PublishFailed
    );
    assert_eq!(
        status.content_hash,
        "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
    );
    assert_eq!(
        status.error,
        PublishError::RegisterTemplateRelease(tonic::Status::new(
            Code::AlreadyExists,
            "tag 'latest' already exists",
        ))
        .to_string()
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn build_failure_preserves_registration_error_message() -> TestResult {
    let backend = MockBuildBackend::with_build_result(Err(BuildError::Message(
        "template-manager registration failed: invalid metadata".to_string(),
    )));
    let service = BuildManagerDaemonService::with_backend(1, backend)?;

    let response = service
        .start_build(authorized_request(start_build_request(
            Some(test_recipe()),
            "seed",
            vec!["latest".to_string()],
        )))
        .await?
        .into_inner();

    let status = wait_for_terminal_status(&service, &response.job_id).await?;
    assert_eq!(
        BuildState::try_from(status.state).expect("known build state"),
        BuildState::Failed
    );
    assert_eq!(
        status.error,
        "template-manager registration failed: invalid metadata"
    );
    assert!(status.content_hash.is_empty());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn submitted_job_status_survives_service_recreation() -> TestResult {
    let tempdir = tempfile::tempdir()?;
    let db_path = tempdir.path().join("build-manager.db");
    let db_path = db_path.to_string_lossy();
    let blocking_backend = Arc::new(BlockingBuildBackend::default());
    let service =
        BuildManagerDaemonService::with_backend_and_db_path(&db_path, 1, blocking_backend.clone())?;

    let response = service
        .start_build(authorized_request(start_build_request(
            Some(test_recipe()),
            "seed",
            vec![],
        )))
        .await?
        .into_inner();

    let recreated = BuildManagerDaemonService::with_backend_and_db_path(
        &db_path,
        1,
        Arc::new(MockBuildBackend::default()),
    )?;
    let status = recreated
        .get_build_status(Request::new(GetBuildStatusRequest {
            job_id: response.job_id.clone(),
        }))
        .await?
        .into_inner();

    assert_eq!(status.template_id, "seed");
    assert_eq!(
        BuildState::try_from(status.state).expect("known build state"),
        BuildState::Running
    );
    blocking_backend.release();
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn terminal_job_status_survives_service_recreation() -> TestResult {
    let tempdir = tempfile::tempdir()?;
    let db_path = tempdir.path().join("build-manager.db");
    let db_path = db_path.to_string_lossy();
    let backend = MockBuildBackend::with_build_result(Ok(BuildOutcome {
        content_hash: Some(
            "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee".to_string(),
        ),
    }));
    let service =
        BuildManagerDaemonService::with_backend_and_db_path(&db_path, 1, backend.clone())?;

    let response = service
        .start_build(authorized_request(start_build_request(
            Some(test_recipe()),
            "seed",
            vec![],
        )))
        .await?
        .into_inner();
    let status = wait_for_terminal_status(&service, &response.job_id).await?;
    assert_eq!(
        BuildState::try_from(status.state).expect("known build state"),
        BuildState::Succeeded
    );

    let recreated = BuildManagerDaemonService::with_backend_and_db_path(
        &db_path,
        1,
        Arc::new(MockBuildBackend::default()),
    )?;
    let status = recreated
        .get_build_status(Request::new(GetBuildStatusRequest {
            job_id: response.job_id,
        }))
        .await?
        .into_inner();

    assert_eq!(
        BuildState::try_from(status.state).expect("known build state"),
        BuildState::Succeeded
    );
    assert_eq!(
        status.content_hash,
        "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn expired_running_job_lease_is_marked_failed() -> TestResult {
    let backend = MockBuildBackend::with_build_result(Ok(BuildOutcome {
        content_hash: Some(
            "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff".to_string(),
        ),
    }));
    let service = BuildManagerDaemonService::with_backend(1, backend)?;
    let job_id = pplx_space_common_jobs::JobId::new();
    let payload = serde_json::to_string(&build_job_payload("seed"))?;
    {
        let conn = service.db.lock().unwrap();
        conn.execute(
            "INSERT INTO jobs (
                job_id, kind, status, payload, subject_id, dedup_key,
                max_attempts, lease_owner, lease_expires_at
             )
             VALUES (?1, ?2, 'running', ?3, ?4, ?4, 1, 'dead-worker',
                     datetime('now', '-1 second'))",
            params![job_id, jobs::BUILD_JOB_KIND, payload, "seed"],
        )?;
    }

    assert_eq!(service.mark_expired_build_leases_failed(), 1);
    let status = service
        .get_build_status(Request::new(GetBuildStatusRequest {
            job_id: job_id.to_string(),
        }))
        .await?
        .into_inner();

    assert_eq!(
        BuildState::try_from(status.state).expect("known build state"),
        BuildState::Failed
    );
    assert_eq!(
        status.error,
        "build-manager exited while build was running; resubmit the build"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn same_template_builds_run_in_parallel_on_one_service() -> TestResult {
    let backend = Arc::new(BlockingBuildBackend::default());
    let service = BuildManagerDaemonService::with_backend(2, backend.clone())?;
    let shutdown = CancellationToken::new();
    let driver = tokio::spawn(service.clone().run_queue_driver(shutdown.clone()));

    let first = service
        .start_build(authorized_request(start_build_request(
            Some(test_recipe()),
            "seed",
            vec![],
        )))
        .await?
        .into_inner();
    let second = service
        .start_build(authorized_request(start_build_request(
            Some(test_recipe()),
            "seed",
            vec![],
        )))
        .await?
        .into_inner();
    assert_ne!(first.job_id, second.job_id);
    assert_eq!(service.running_build_count(), 2);

    // Same-template builds no longer serialize: both execute while the
    // first is still blocked (job-scoped staging makes this safe).
    for _ in 0..100 {
        if backend.started_count() >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(backend.started_count(), 2);

    backend.release();
    let first_status = service
        .worker
        .wait_for_job(&first.job_id.parse().unwrap(), Duration::from_secs(10))
        .await?;
    let second_status = service
        .worker
        .wait_for_job(&second.job_id.parse().unwrap(), Duration::from_secs(10))
        .await?;
    assert_eq!(first_status.status, "completed");
    assert_eq!(second_status.status, "completed");
    assert_eq!(backend.started_count(), 2);

    shutdown.cancel();
    let _ = driver.await;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn same_template_builds_run_concurrently_across_service_instances() -> TestResult {
    let tempdir = tempfile::tempdir()?;
    let db_path = tempdir.path().join("build-manager.db");
    let db_path = db_path.to_string_lossy();
    let backend1 = Arc::new(BlockingBuildBackend::default());
    let backend2 = Arc::new(BlockingBuildBackend::default());
    let service1 =
        BuildManagerDaemonService::with_backend_and_db_path(&db_path, 2, backend1.clone())?;
    let service2 =
        BuildManagerDaemonService::with_backend_and_db_path(&db_path, 2, backend2.clone())?;
    let shutdown = CancellationToken::new();
    let driver1 = tokio::spawn(service1.clone().run_queue_driver(shutdown.clone()));
    let driver2 = tokio::spawn(service2.clone().run_queue_driver(shutdown.clone()));

    let first = service1
        .start_build(authorized_request(start_build_request(
            Some(test_recipe()),
            "seed",
            vec![],
        )))
        .await?
        .into_inner();
    let second = service2
        .start_build(authorized_request(start_build_request(
            Some(test_recipe()),
            "seed",
            vec![],
        )))
        .await?
        .into_inner();
    assert_eq!(service1.running_build_count(), 2);

    // Both pods execute same-template builds simultaneously; neither
    // waits for the other (job-scoped staging isolates their output).
    for _ in 0..100 {
        if backend1.started_count() + backend2.started_count() >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(backend1.started_count() + backend2.started_count(), 2);

    backend1.release();
    backend2.release();
    let first_status = service1
        .worker
        .wait_for_job(&first.job_id.parse().unwrap(), Duration::from_secs(10))
        .await?;
    let second_status = service2
        .worker
        .wait_for_job(&second.job_id.parse().unwrap(), Duration::from_secs(10))
        .await?;
    assert_eq!(first_status.status, "completed");
    assert_eq!(second_status.status, "completed");

    shutdown.cancel();
    let _ = driver1.await;
    let _ = driver2.await;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn cloned_service_shares_inflight_count() -> TestResult {
    let backend = Arc::new(BlockingBuildBackend::default());
    let service = BuildManagerDaemonService::with_backend(1, backend.clone())?;
    let cloned_service = service.clone();

    let response = service
        .start_build(authorized_request(start_build_request(
            Some(test_recipe()),
            "seed",
            vec![],
        )))
        .await?
        .into_inner();

    backend.wait_started().await;
    assert_eq!(service.worker.inflight(), 1);
    assert_eq!(cloned_service.worker.inflight(), 1);

    backend.release();
    let status = wait_for_terminal_status(&service, &response.job_id).await?;
    wait_for_inflight_count(&cloned_service, 0).await;
    assert_eq!(
        BuildState::try_from(status.state).expect("known build state"),
        BuildState::Succeeded
    );
    assert_eq!(service.worker.inflight(), 0);
    assert_eq!(cloned_service.worker.inflight(), 0);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn distinct_template_builds_run_on_different_service_instances() -> TestResult {
    let tempdir = tempfile::tempdir()?;
    let db_path = tempdir.path().join("build-manager.db");
    let db_path = db_path.to_string_lossy();
    let backend1 = Arc::new(BlockingBuildBackend::default());
    let backend2 = Arc::new(BlockingBuildBackend::default());
    let service1 =
        BuildManagerDaemonService::with_backend_and_db_path(&db_path, 1, backend1.clone())?;
    let service2 =
        BuildManagerDaemonService::with_backend_and_db_path(&db_path, 1, backend2.clone())?;

    let first = service1
        .start_build(authorized_request(start_build_request(
            Some(test_recipe()),
            "seed",
            vec![],
        )))
        .await?
        .into_inner();
    let second = service2
        .start_build(authorized_request(start_build_request(
            Some(test_recipe()),
            "code-runtime",
            vec![],
        )))
        .await?
        .into_inner();

    backend1.wait_started().await;
    backend2.wait_started().await;
    assert_eq!(service1.running_build_count(), 2);

    backend1.release();
    backend2.release();
    let first = wait_for_terminal_status(&service1, &first.job_id).await?;
    let second = wait_for_terminal_status(&service2, &second.job_id).await?;
    assert_eq!(
        BuildState::try_from(first.state).expect("known build state"),
        BuildState::Succeeded
    );
    assert_eq!(
        BuildState::try_from(second.state).expect("known build state"),
        BuildState::Succeeded
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn stale_lease_owner_cannot_write_build_result() -> TestResult {
    let backend = Arc::new(BlockingBuildBackend::default());
    let service = BuildManagerDaemonService::with_backend(1, backend.clone())?;

    let response = service
        .start_build(authorized_request(start_build_request(
            Some(test_recipe()),
            "seed",
            vec![],
        )))
        .await?
        .into_inner();
    backend.wait_started().await;
    {
        let conn = service.db.lock().unwrap();
        conn.execute(
            "UPDATE jobs SET lease_owner = 'other-worker' WHERE job_id = ?1",
            params![response.job_id],
        )?;
    }

    backend.release();
    tokio::time::sleep(Duration::from_millis(100)).await;
    {
        let conn = service.db.lock().unwrap();
        let result_count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM build_job_results WHERE job_id = ?1",
            params![response.job_id],
            |row| row.get(0),
        )?;
        assert_eq!(result_count, 0);
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn draining_rejects_new_builds_and_waits_for_running_builds() -> TestResult {
    let backend = Arc::new(BlockingBuildBackend::default());
    let service = BuildManagerDaemonService::with_backend(1, backend.clone())?;

    let response = service
        .start_build(authorized_request(start_build_request(
            Some(test_recipe()),
            "seed",
            vec![],
        )))
        .await?
        .into_inner();
    assert_eq!(service.running_build_count(), 1);
    backend.wait_started().await;

    service.start_draining();
    let error = service
        .start_build(authorized_request(start_build_request(
            Some(test_recipe()),
            "code-runtime",
            vec![],
        )))
        .await
        .expect_err("draining service should reject new builds");
    assert_eq!(error.code(), Code::Unavailable);
    assert!(
        !service
            .wait_for_running_builds(Duration::from_millis(10))
            .await
    );

    backend.release();
    assert!(
        service
            .wait_for_running_builds(Duration::from_secs(1))
            .await
    );
    let status = wait_for_terminal_status(&service, &response.job_id).await?;
    assert_eq!(
        BuildState::try_from(status.state).expect("known build state"),
        BuildState::Succeeded
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn drain_ignores_sibling_pod_running_builds() -> TestResult {
    let tempdir = tempfile::tempdir()?;
    let db_path = tempdir.path().join("build-manager.db");
    let db_path = db_path.to_string_lossy();
    let backend1 = Arc::new(BlockingBuildBackend::default());
    let backend2 = Arc::new(BlockingBuildBackend::default());
    let service1 =
        BuildManagerDaemonService::with_backend_and_db_path(&db_path, 1, backend1.clone())?;
    let service2 =
        BuildManagerDaemonService::with_backend_and_db_path(&db_path, 1, backend2.clone())?;

    let first = service1
        .start_build(authorized_request(start_build_request(
            Some(test_recipe()),
            "seed",
            vec![],
        )))
        .await?
        .into_inner();
    let second = service2
        .start_build(authorized_request(start_build_request(
            Some(test_recipe()),
            "code-runtime",
            vec![],
        )))
        .await?
        .into_inner();
    backend1.wait_started().await;
    backend2.wait_started().await;

    service1.start_draining();
    backend1.release();

    let drained = service1
        .wait_for_running_builds(Duration::from_secs(1))
        .await;
    assert_eq!(service1.running_build_count(), 1);

    backend2.release();
    let first = wait_for_terminal_status(&service1, &first.job_id).await?;
    let second = wait_for_terminal_status(&service2, &second.job_id).await?;
    assert!(drained);
    assert_eq!(
        BuildState::try_from(first.state).expect("known build state"),
        BuildState::Succeeded
    );
    assert_eq!(
        BuildState::try_from(second.state).expect("known build state"),
        BuildState::Succeeded
    );
    Ok(())
}

#[tokio::test]
async fn unauthenticated_owner_claim_does_not_enqueue_a_build() -> TestResult {
    let backend = MockBuildBackend::with_build_result(Ok(BuildOutcome {
        content_hash: Some(
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
        ),
    }));
    let service = BuildManagerDaemonService::with_backend(1, backend.clone())?;

    for org_id in [None, Some("018f0000-0000-7000-8000-000000000002")] {
        let mut body = start_build_request(
            Some(test_recipe()),
            "victim-template",
            vec!["latest".to_string()],
        );
        body.org_id = org_id.map(str::to_string);
        let mut request = Request::new(body);
        request
            .metadata_mut()
            .insert("authorization", "Bearer shared-token".parse()?);

        let error = service.start_build(request).await.unwrap_err();

        assert_eq!(error.code(), Code::PermissionDenied);
        assert_eq!(count_active_build_jobs(&service.db.lock().unwrap())?, 0);
        assert!(backend.publish_calls.lock().unwrap().is_empty());
    }
    Ok(())
}

#[tokio::test]
async fn missing_listener_identity_prevents_durable_build_work() -> TestResult {
    if !pplx_space_test_env::enter_test_process(&[
        ("SPACE_INTERNAL_RPC_INSECURE_DEV", None),
        ("SPACE_INTERNAL_RPC_TLS_CERT", None),
        ("SPACE_INTERNAL_RPC_TLS_KEY", None),
    ]) {
        return Ok(());
    }
    let directory = tempfile::tempdir()?;
    let db_path = directory.path().join("new/builds.db");
    let config = crate::run::BuildMgrRunConfig {
        listen: "127.0.0.1:0".parse()?,
        max_concurrent_builds: 1,
        db_path: db_path.to_str().unwrap().into(),
        drain_timeout: Duration::ZERO,
    };
    assert!(matches!(
        crate::run::run(config, CancellationToken::new()).await,
        Err(crate::run::RunBuildMgrError::InternalTransport(
            pplx_space_common_grpc::internal_transport::TransportError::Missing(
                "SPACE_INTERNAL_RPC_TLS_CERT"
            )
        ))
    ));
    assert!(!db_path.parent().unwrap().exists());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn environment_preflight_precedes_blob_and_publication_effects() -> TestResult {
    let export = "SPACE_BUILD_EXPORT_REVIEW_MISSING_EXPORT";
    if !pplx_space_test_env::enter_test_process(&[(export, None)]) {
        return Ok(());
    }
    for with_blob in [true, false] {
        let backend = MockBuildBackend::with_build_result(Ok(BuildOutcome { content_hash: None }));
        let service = BuildManagerDaemonService::with_backend(1, backend.clone())?;
        let mut recipe = test_recipe();
        recipe.steps.push(recipe::Step {
            env_refs: vec!["REVIEW_MISSING_EXPORT".into()],
            body: Some(recipe::step::Body::Run(recipe::Run {
                command: "true".into(),
                user: None,
            })),
            ..Default::default()
        });
        if with_blob {
            recipe.steps.push(recipe::Step {
                display_name: None,
                env_refs: vec![],
                body: Some(recipe::step::Body::Copy(recipe::Copy {
                    payload: Some(recipe::copy::Payload::BlobSha256("e".repeat(64))),
                    dest: "/tmp/input".into(),
                    source_path: None,
                })),
            });
        }
        let response = service
            .start_build(authorized_request(start_build_request(
                Some(recipe),
                "preflight",
                vec![],
            )))
            .await?
            .into_inner();
        let status = wait_for_terminal_status(&service, &response.job_id).await?;
        assert!(status.error.contains("REVIEW_MISSING_EXPORT"), "{status:?}");
        assert!(backend.cluster_template_calls.lock().unwrap().is_empty());
        assert!(backend.build_calls.lock().unwrap().is_empty());
        assert!(backend.publish_calls.lock().unwrap().is_empty());
    }
    Ok(())
}

#[tokio::test]
async fn selected_build_survives_queue_and_old_wire_readers_reject_before_enqueue() -> TestResult {
    // The old decoder knows recipe field 1, but not the additive selected input.
    #[derive(prost::Message)]
    struct OldStartBuild {
        #[prost(message, optional, tag = "1")]
        recipe: Option<ProtoRecipe>,
    }
    let reference =
        "sha256:959f1181ac69f0dde7125fbc81be6fcd75b64e9e06e7a393f067b96eb6fb2ac4".parse()?;
    let backend = MockBuildBackend::with_build_result(Ok(BuildOutcome { content_hash: None }));
    let service = BuildManagerDaemonService::with_backend(1, backend.clone())?;
    let mut request = start_build_request(Some(test_recipe()), "benchmark-c", vec![]);
    pplx_space_build_mgr_client::environment::bind(&mut request, reference);
    let old = OldStartBuild::decode(request.encode_to_vec().as_slice())?;
    assert!(old.recipe.is_none());
    let error = service
        .start_build(authorized_request(start_build_request(
            old.recipe,
            "old",
            vec![],
        )))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::InvalidArgument);
    assert_eq!(count_active_build_jobs(&service.db.lock().unwrap())?, 0);
    let mut contradictory = request.clone();
    contradictory.recipe = Some(test_recipe());
    assert_eq!(
        service
            .start_build(authorized_request(contradictory))
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    let response = service
        .start_build(authorized_request(request))
        .await?
        .into_inner();
    assert_eq!(response.environment, Some(reference.to_string()));
    wait_for_terminal_status(&service, &response.job_id).await?;
    assert_eq!(
        *backend.environment_calls.lock().unwrap(),
        vec![Some(reference)]
    );
    let job = service.worker.get_job(&response.job_id.parse()?)?.unwrap();
    let (payload, binding) = jobs::decode_payload(&job)?;
    assert_eq!(binding, Some(reference));
    assert_eq!(payload.template_id, "benchmark-c");
    assert!(
        job.payload_as::<BuildJobPayload>().is_err(),
        "downgraded worker must not silently execute an unbound build"
    );
    let mut unknown = job.clone();
    unknown.payload = unknown.payload.replace("environment_v1", "environment_v2");
    jobs::decode_payload(&unknown).unwrap_err();
    // Flat jobs accept additive fields without acquiring selection semantics.
    let mut additive = serde_json::to_value(&payload)?;
    additive["future_display_label"] = serde_json::json!("ordinary build");
    let mut legacy = job.clone();
    legacy.payload = serde_json::to_string(&additive)?;
    let (decoded, reference) = jobs::decode_payload(&legacy)?;
    assert_eq!(decoded.template_id, payload.template_id);
    assert!(reference.is_none());
    // A future or malformed envelope cannot fall back to the legacy decoder,
    // even if it supplies all legacy fields as well.
    let mut ambiguous = serde_json::to_value(payload)?;
    ambiguous["schema"] = serde_json::json!("environment_v2");
    unknown.payload = serde_json::to_string(&ambiguous)?;
    jobs::decode_payload(&unknown).unwrap_err();
    Ok(())
}
