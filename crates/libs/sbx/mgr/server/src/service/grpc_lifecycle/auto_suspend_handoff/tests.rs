use super::*;
use crate::snapshot_upload::{AutoSuspendHandoff, SnapshotMeta, SnapshotUploadConfig};
use pplx_space_common_core::types::{
    AutoLifecycleConditionSet, AutoLifecycleConditionSetId, AutoLifecycleStage, SnapshotArtifact,
};

struct Fixture {
    service: SandboxManagerService,
    payload: SnapshotUploadJobs,
    job_id: JobId,
    _scratch: tempfile::TempDir,
}

impl Fixture {
    fn new() -> Self {
        let scratch = tempfile::tempdir().unwrap();
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new().to_string();
        let policy = AutoLifecyclePolicy {
            wake: Default::default(),
            condition_sets: std::collections::BTreeMap::from([(
                AutoLifecycleConditionSetId::AccessIdle,
                AutoLifecycleConditionSet::access_idle(),
            )]),
            stages: vec![AutoLifecycleStage {
                condition_set: AutoLifecycleConditionSetId::AccessIdle,
                at: 30,
                action: AutoLifecycleIdleAction::Suspend,
            }],
        }
        .normalized();
        crate::service::tests::service_tests::insert_test_sandbox_with_policy(
            &conn,
            &sandbox_id,
            SandboxState::Paused,
            &policy,
        );
        let state = initial_runtime_state(&policy, now_secs() - 400);
        crate::db::update_auto_lifecycle_state(&conn, &sandbox_id, &state).unwrap();
        let row = crate::db::read::sandbox(&conn, &sandbox_id)
            .unwrap()
            .unwrap();
        let due_at = due_stage(&policy, &state, now_secs()).unwrap().3;
        let artifact_id = LocalArtifactId::new();
        crate::service::suspend_snapshot_publication::write_full_suspend_artifact(
            scratch.path(),
            &sandbox_id,
            artifact_id,
            b"immutable test capture",
        );
        let meta: SnapshotMeta = serde_json::from_slice(
            &std::fs::read(
                scratch
                    .path()
                    .join(crate::snapshot_upload::SNAPSHOT_METADATA_FILENAME),
            )
            .unwrap(),
        )
        .unwrap();
        let config = SnapshotUploadConfig {
            bucket: "test-bucket".to_owned(),
            key_prefix: "test".to_owned(),
            region: "us-east-1".to_owned(),
            endpoint: Some("http://object-store.test".to_owned()),
            path_style: true,
        };
        let mut payload = crate::snapshot_upload::prepare_suspend_snapshot(
            &meta,
            &uuid::Uuid::now_v7().to_string(),
            scratch.path(),
            &config,
            ArtifactKind::Full,
            pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat::Full,
            None,
        )
        .unwrap();
        payload.auto_suspend = Some(AutoSuspendHandoff::new(&row, due_at));
        let job_id = JobId::new();
        pplx_space_common_jobs::db::insert_job(
            conn.raw(),
            pplx_space_common_jobs::db::InsertJob {
                job_id: &job_id,
                kind: crate::snapshot_upload_jobs::SNAPSHOT_UPLOAD_JOB_KIND,
                payload: &serde_json::to_string(&payload).unwrap(),
                subject_id: Some(&sandbox_id),
                dedup_key: Some(&artifact_id.to_string()),
                max_attempts: 1,
                traceparent: None,
                priority: Default::default(),
            },
        )
        .unwrap();
        let service = SandboxManagerService::new_with_options(
            conn,
            SandboxManagerServiceOptions {
                sandbox_runtime_live: Arc::new(|_| true),
                ..SandboxManagerServiceOptions::for_test_in(scratch.path())
            },
        );
        Self {
            service,
            payload,
            job_id,
            _scratch: scratch,
        }
    }

    fn row(&self) -> crate::db::SandboxRow {
        self.service
            .lookup_sandbox(&self.payload.meta.sandbox_id)
            .unwrap()
    }
}

#[tokio::test]
async fn transient_handoff_stays_due_without_recapture_or_slow_retry() {
    let fixture = Fixture::new();
    let failure = node_local_backup_status(ArtifactBackupSubmissionError::Admission(
        crate::snapshot_upload::SnapshotSubmissionError::Admission {
            artifact: SnapshotArtifact::Memory,
            source: Status::unavailable("handoff"),
        },
    ));
    assert!(is_transient_backup_handoff(&failure));
    assert!(
        !failure
            .metadata()
            .contains_key(AUTO_LIFECYCLE_SUSPEND_BACKUP_FENCE_FAILED_METADATA)
    );
    let due_at = fixture.payload.auto_suspend.as_ref().unwrap().due_at;
    for attempt in 0..24 {
        let before = now_secs();
        fixture
            .service
            .defer_auto_suspend_handoff(&fixture.row(), &failure)
            .unwrap();
        let row = fixture.row();
        if attempt == 2 {
            crate::db::update_auto_lifecycle_with_state(
                &fixture.service.db.lock().unwrap(),
                &row.id,
                &row.auto_lifecycle,
                &row.auto_lifecycle_state,
                crate::db::PolicyAuthority::Cluster,
            )
            .unwrap();
            assert!(
                fixture
                    .payload
                    .auto_suspend
                    .as_ref()
                    .unwrap()
                    .owns(&fixture.row())
            );
            assert_eq!(fixture.row().next_auto_event_at, row.next_auto_event_at);
        }
        let delay = row.next_auto_event_at.unwrap() - before;
        let nominal = (5_u64 << attempt.min(4)).min(60);
        assert!(delay >= nominal * 8 / 10);
        assert!(delay <= nominal * 12 / 10 + 1);
        assert_eq!(
            due_stage(&row.auto_lifecycle, &row.auto_lifecycle_state, now_secs())
                .unwrap()
                .3,
            due_at
        );
        let db = fixture.service.db.lock().unwrap();
        let job = crate::db::read::auto_suspend_upload(&db, &row.id)
            .unwrap()
            .unwrap();
        let persisted: SnapshotUploadJobs = job.payload_as().unwrap();
        assert_eq!(job.job_id, fixture.job_id);
        assert_eq!(persisted.snapshot_id, fixture.payload.snapshot_id);
        assert_eq!(
            persisted.meta.local_artifact_id,
            fixture.payload.meta.local_artifact_id
        );
        assert_eq!(persisted.auto_suspend.unwrap().failures, attempt + 1);
    }
    let terminal = node_local_backup_status(ArtifactBackupSubmissionError::Admission(
        crate::snapshot_upload::SnapshotSubmissionError::Admission {
            artifact: SnapshotArtifact::Memory,
            source: Status::failed_precondition("superseded capture"),
        },
    ));
    assert!(!is_transient_backup_handoff(&terminal));
    assert!(
        terminal
            .metadata()
            .contains_key(AUTO_LIFECYCLE_SUSPEND_BACKUP_FENCE_FAILED_METADATA)
    );
}

#[tokio::test]
async fn new_activity_policy_execution_and_cleanup_defeat_the_prepared_retry() {
    for change in 0..8 {
        let fixture = Fixture::new();
        fixture
            .service
            .verify_auto_suspend_handoff_capture(&fixture.payload)
            .await
            .unwrap();
        let row = fixture.row();
        if change == 4 {
            fixture
                .service
                .invalidate_auto_suspend_handoff(&row.id)
                .unwrap();
        } else if change == 7 {
            let mut row = fixture.row();
            pplx_space_sbx_mgr_store::auto_lifecycle::mark_error_and_disarm_conditions(
                &mut row.auto_lifecycle_state,
                &row.auto_lifecycle,
                "terminal capture fence",
            );
            crate::db::update_auto_lifecycle_state(
                &fixture.service.db.lock().unwrap(),
                &row.id,
                &row.auto_lifecycle_state,
            )
            .unwrap();
            assert!(
                !fixture
                    .payload
                    .auto_suspend
                    .as_ref()
                    .unwrap()
                    .owns(&fixture.row())
            );
        } else if change == 5 {
            tokio::fs::remove_file(
                fixture
                    .payload
                    .snapshot_dir
                    .join(crate::snapshot_upload::SNAPSHOT_METADATA_FILENAME),
            )
            .await
            .unwrap();
        } else if change == 6 {
            let mut replaced = fixture.payload.meta.clone();
            replaced.local_artifact_id = Some(LocalArtifactId::new());
            tokio::fs::write(
                fixture
                    .payload
                    .snapshot_dir
                    .join(crate::snapshot_upload::SNAPSHOT_METADATA_FILENAME),
                serde_json::to_vec(&replaced).unwrap(),
            )
            .await
            .unwrap();
        } else {
            let db = fixture.service.db.lock().unwrap();
            match change {
                0 => {
                    let mut state = row.auto_lifecycle_state;
                    crate::auto_lifecycle::touch_access(
                        &mut state,
                        &row.auto_lifecycle,
                        now_secs(),
                    );
                    crate::db::update_auto_lifecycle_state(&db, &row.id, &state).unwrap();
                }
                1 => {
                    let mut replacement = row.auto_lifecycle.clone();
                    replacement.stages[0].at = 90;
                    crate::db::update_auto_lifecycle_with_state(
                        &db,
                        &row.id,
                        &replacement,
                        &row.auto_lifecycle_state,
                        row.auto_lifecycle_authority,
                    )
                    .unwrap();
                }
                2 => {
                    let fields = crate::service::tests::service_tests::runtime_fields(
                        ExecutionGeneration::new(9).unwrap(),
                        9,
                    );
                    crate::db::update_runtime_fields(&db, row.id.parse().unwrap(), fields).unwrap();
                }
                3 => {
                    assert!(
                        crate::db::set_cleanup_intent(
                            &db,
                            &row.id,
                            true,
                            CleanupCompletion::MarkStopped
                        )
                        .unwrap()
                    );
                }
                _ => unreachable!(),
            }
        }
        let error = fixture
            .service
            .verify_auto_suspend_handoff_capture(&fixture.payload)
            .await
            .unwrap_err();
        assert!(
            !crate::snapshot_upload::SnapshotSubmissionError::Admission {
                artifact: SnapshotArtifact::Metadata,
                source: error.clone(),
            }
            .is_transient()
        );
        assert!(
            error
                .metadata()
                .contains_key(AUTO_LIFECYCLE_SUSPEND_BACKUP_FENCE_FAILED_METADATA),
            "change {change}"
        );
        assert_eq!(fixture.row().state, SandboxState::Paused);
    }
}
