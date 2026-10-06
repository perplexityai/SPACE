use pplx_space_common_core::sql;
use pplx_space_db_test_fixtures::{TestDb, TestDbResult, pg_test_db};
use rstest::{fixture, rstest};

use super::*;

const TEST_ORG: OrgId = OrgId::from_uuid(uuid::Uuid::from_u128(1));

struct StoreFixture {
    store: TemplateBuildJobsStore,
    builder_nodes: [NodeId; 2],
    db: TestDb,
}

#[fixture]
async fn store_fixture() -> TestDbResult<StoreFixture> {
    let db = pg_test_db().await?;
    let store = TemplateBuildJobsStore::new(db.pool.clone());
    let builder_nodes = [NodeId::new(), NodeId::new()];
    for (index, node_id) in builder_nodes.iter().enumerate() {
        sqlx::query(sql!(
            "INSERT INTO nodes (id, address, role, managed_by, state)
             VALUES ($1, $2::INET, 'builder', 'k8s', 'ready')"
        ))
        .bind(node_id.as_uuid())
        .bind(format!("10.0.0.{}", index + 1))
        .execute(&db.pool)
        .await?;
    }
    Ok(StoreFixture {
        store,
        builder_nodes,
        db,
    })
}

fn template_name() -> TemplateName {
    TemplateName::parse("code-runtime").unwrap()
}

#[rstest]
#[tokio::test]
async fn records_e2b_build_on_the_pending_job(
    #[future] store_fixture: TestDbResult<StoreFixture>,
) -> TestDbResult<()> {
    let fixture = store_fixture.await?;
    let template_name = template_name();
    let client_build_id = BuildRunId::new().to_string();
    let daemon_job_id = BuildRunId::new();
    let spec = BuildSpec {
        tags: vec!["latest".to_string()],
        cpu_count: Some(4),
        memory_mb: Some(8192),
    };
    fixture
        .store
        .record_build_spec(TEST_ORG, &template_name, &client_build_id, spec.clone())
        .await?;

    fixture
        .store
        .record_accepted_build(
            TEST_ORG,
            &template_name,
            Some(&client_build_id),
            daemon_job_id,
            Some(fixture.builder_nodes[0]),
            None,
        )
        .await?;
    let job = fixture
        .store
        .lookup_build(TEST_ORG, &template_name, &client_build_id)
        .await?;
    let stored_spec = fixture
        .store
        .lookup_build_spec(TEST_ORG, &template_name, &client_build_id)
        .await?;
    let builder_node = fixture.store.find_builder_node(daemon_job_id).await?;

    assert_eq!(job.map(|job| job.daemon_job_id), Some(daemon_job_id));
    assert_eq!(stored_spec, Some(spec));
    assert_eq!(builder_node, Some(fixture.builder_nodes[0]));
    Ok(())
}

#[rstest]
#[tokio::test]
async fn records_admin_build_with_the_daemon_job_id_as_its_build_id(
    #[future] store_fixture: TestDbResult<StoreFixture>,
) -> TestDbResult<()> {
    let fixture = store_fixture.await?;
    let template_name = template_name();
    let daemon_job_id = BuildRunId::new();

    fixture
        .store
        .record_accepted_build(
            TEST_ORG,
            &template_name,
            None,
            daemon_job_id,
            Some(fixture.builder_nodes[0]),
            None,
        )
        .await?;
    let job = fixture
        .store
        .lookup_build(TEST_ORG, &template_name, &daemon_job_id.to_string())
        .await?;

    assert_eq!(job.map(|job| job.daemon_job_id), Some(daemon_job_id));
    Ok(())
}

#[rstest]
#[tokio::test]
async fn recording_unknown_builder_node_is_atomic(
    #[future] store_fixture: TestDbResult<StoreFixture>,
) -> TestDbResult<()> {
    let fixture = store_fixture.await?;
    let template_name = template_name();
    let daemon_job_id = BuildRunId::new();

    let error = fixture
        .store
        .record_accepted_build(
            TEST_ORG,
            &template_name,
            None,
            daemon_job_id,
            Some(NodeId::new()),
            None,
        )
        .await
        .unwrap_err();
    let job = fixture
        .store
        .lookup_build(TEST_ORG, &template_name, &daemon_job_id.to_string())
        .await?;

    assert!(matches!(
        error,
        TemplateBuildJobsStoreError::Storage(sqlx::Error::Database(ref error))
            if error.is_foreign_key_violation()
    ));
    assert_eq!(job, None);
    Ok(())
}

#[rstest]
#[tokio::test]
async fn deleting_builder_node_keeps_job_for_unpinned_fallback(
    #[future] store_fixture: TestDbResult<StoreFixture>,
) -> TestDbResult<()> {
    let fixture = store_fixture.await?;
    let template_name = template_name();
    let daemon_job_id = BuildRunId::new();
    fixture
        .store
        .record_accepted_build(
            TEST_ORG,
            &template_name,
            None,
            daemon_job_id,
            Some(fixture.builder_nodes[0]),
            None,
        )
        .await?;

    sqlx::query(sql!("DELETE FROM nodes WHERE id = $1"))
        .bind(fixture.builder_nodes[0].as_uuid())
        .execute(&fixture.db.pool)
        .await?;
    let job = fixture
        .store
        .lookup_build(TEST_ORG, &template_name, &daemon_job_id.to_string())
        .await?;
    let builder_node = fixture.store.find_builder_node(daemon_job_id).await?;

    assert_eq!(job.map(|job| job.daemon_job_id), Some(daemon_job_id));
    assert_eq!(builder_node, None);
    Ok(())
}

#[rstest]
#[tokio::test]
async fn job_writes_preserve_owner_and_legacy_nulls(
    #[future] store_fixture: TestDbResult<StoreFixture>,
) -> TestDbResult<()> {
    let fixture = store_fixture.await?;
    let name = template_name();
    let other = OrgId::from_uuid(uuid::Uuid::from_u128(2));
    fixture
        .store
        .record_build_spec(TEST_ORG, &name, "owned", BuildSpec::default())
        .await?;
    sqlx::query(sql!(
        "INSERT INTO template_build_jobs (template_id, build_id) VALUES ($1, 'legacy')"
    ))
    .bind(name.as_str())
    .execute(&fixture.db.pool)
    .await?;
    let job = BuildRunId::new();

    let conflict = fixture
        .store
        .record_build_spec(other, &name, "owned", BuildSpec::default())
        .await;
    fixture
        .store
        .record_accepted_build(TEST_ORG, &name, Some("owned"), job, None, None)
        .await?;
    fixture
        .store
        .record_accepted_build(TEST_ORG, &name, Some("legacy"), job, None, None)
        .await?;

    assert!(matches!(
        conflict,
        Err(TemplateBuildJobsStoreError::OwnershipConflict)
    ));
    let rows: Vec<(String, Option<uuid::Uuid>)> = sqlx::query_as(sql!(
        "SELECT build_id, org_id FROM template_build_jobs WHERE template_id = $1 ORDER BY build_id"
    ))
    .bind(name.as_str())
    .fetch_all(&fixture.db.pool)
    .await?;
    assert_eq!(
        rows,
        vec![
            ("legacy".into(), None),
            ("owned".into(), Some(TEST_ORG.as_uuid()))
        ]
    );
    Ok(())
}

#[rstest]
#[tokio::test]
async fn input_writes_reject_a_different_template_owner(
    #[future] store_fixture: TestDbResult<StoreFixture>,
) -> TestDbResult<()> {
    let fixture = store_fixture.await?;
    let name = template_name();
    sqlx::query(sql!(
        "INSERT INTO templates (id, name, org_id) VALUES ($3, $1, $2)"
    ))
    .bind(name.as_str())
    .bind(TEST_ORG.as_uuid())
    .bind(uuid::Uuid::now_v7())
    .execute(&fixture.db.pool)
    .await?;
    let other = OrgId::from_uuid(uuid::Uuid::from_u128(2));

    let rejected = fixture
        .store
        .record_build_spec(other, &name, "foreign", BuildSpec::default())
        .await;
    fixture
        .store
        .record_build_spec(TEST_ORG, &name, "owner", BuildSpec::default())
        .await?;

    assert!(matches!(
        rejected,
        Err(TemplateBuildJobsStoreError::OwnershipConflict)
    ));
    let count: i64 = sqlx::query_scalar(sql!(
        "SELECT COUNT(*) FROM template_build_jobs WHERE template_id = $1"
    ))
    .bind(name.as_str())
    .fetch_one(&fixture.db.pool)
    .await?;
    assert_eq!(count, 1);
    Ok(())
}

#[rstest]
#[case::owner(Some(1), 1, true)]
#[case::other_org(Some(1), 2, false)]
#[case::legacy(None, 1, true)]
#[case::legacy_other_org(None, 2, true)]
#[tokio::test]
async fn build_reads_enforce_known_owners_and_preserve_legacy_builds(
    #[future] store_fixture: TestDbResult<StoreFixture>,
    #[case] owner: Option<u128>,
    #[case] caller: u128,
    #[case] visible: bool,
) -> TestDbResult<()> {
    let fixture = store_fixture.await?;
    let name = template_name();
    let job = BuildRunId::new();
    sqlx::query(sql!(
        "INSERT INTO template_build_jobs (template_id, build_id, daemon_job_id, cpu_count, org_id)
         VALUES ($1, 'build', $2, 4, $3)"
    ))
    .bind(name.as_str())
    .bind(job.to_string())
    .bind(owner.map(uuid::Uuid::from_u128))
    .execute(&fixture.db.pool)
    .await?;
    let caller = OrgId::from_uuid(uuid::Uuid::from_u128(caller));

    let read_job = fixture.store.lookup_build(caller, &name, "build").await?;
    let spec = fixture
        .store
        .lookup_build_spec(caller, &name, "build")
        .await?;

    assert_eq!(
        read_job.map(|row| row.daemon_job_id),
        visible.then_some(job)
    );
    assert_eq!(spec.map(|row| row.cpu_count), visible.then_some(Some(4)));
    Ok(())
}

#[rstest]
#[tokio::test]
async fn new_create_old_submit_new_poll_preserves_in_flight_build(
    #[future] store_fixture: TestDbResult<StoreFixture>,
) -> TestDbResult<()> {
    let fixture = store_fixture.await?;
    let name = template_name();
    let job = BuildRunId::new();
    fixture
        .store
        .record_build_spec(
            TEST_ORG,
            &name,
            "build",
            BuildSpec {
                cpu_count: Some(4),
                ..Default::default()
            },
        )
        .await?;
    let old_spec: Option<i32> = sqlx::query_scalar(sql!(
        "SELECT cpu_count FROM template_build_jobs WHERE template_id = $1 AND build_id = 'build'"
    ))
    .bind(name.as_str())
    .fetch_one(&fixture.db.pool)
    .await?;

    sqlx::query(sql!(
        "INSERT INTO template_build_jobs (template_id, build_id, daemon_job_id)
         VALUES ($1, 'build', $2)
         ON CONFLICT (template_id, build_id) DO UPDATE
         SET daemon_job_id = EXCLUDED.daemon_job_id"
    ))
    .bind(name.as_str())
    .bind(job.to_string())
    .execute(&fixture.db.pool)
    .await?;
    let read_job = fixture.store.lookup_build(TEST_ORG, &name, "build").await?;

    assert_eq!(old_spec, Some(4));
    assert_eq!(read_job.map(|row| row.daemon_job_id), Some(job));
    assert!(
        fixture
            .store
            .lookup_build(OrgId::from_uuid(uuid::Uuid::from_u128(2)), &name, "build")
            .await?
            .is_none()
    );
    Ok(())
}

#[rstest]
#[tokio::test]
async fn legacy_build_reads_respect_a_registered_template_owner(
    #[future] store_fixture: TestDbResult<StoreFixture>,
) -> TestDbResult<()> {
    let fixture = store_fixture.await?;
    let name = template_name();
    sqlx::query(sql!(
        "INSERT INTO templates (id, name, org_id) VALUES ($1, $2, $3)"
    ))
    .bind(uuid::Uuid::now_v7())
    .bind(name.as_str())
    .bind(TEST_ORG.as_uuid())
    .execute(&fixture.db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO template_build_jobs (template_id, build_id, daemon_job_id) VALUES ($1, 'legacy', $2)"
    )).bind(name.as_str()).bind(BuildRunId::new().to_string()).execute(&fixture.db.pool).await?;
    let other = OrgId::from_uuid(uuid::Uuid::from_u128(2));

    let job = fixture.store.lookup_build(other, &name, "legacy").await?;
    let spec = fixture
        .store
        .lookup_build_spec(other, &name, "legacy")
        .await?;

    assert!(job.is_none());
    assert!(spec.is_none());
    assert!(
        fixture
            .store
            .lookup_build(TEST_ORG, &name, "legacy")
            .await?
            .is_some()
    );
    Ok(())
}
