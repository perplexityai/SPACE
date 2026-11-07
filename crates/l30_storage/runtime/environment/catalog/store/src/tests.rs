use pplx_space_common_core::sql;
use pplx_space_common_core::types::{Digest, EnvironmentReference};
use pplx_space_db_test_fixtures::try_pg_test_db;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;

use super::*;
use pplx_space_common_core::types::{BootProfile, ExecutionCompatibilityProfile, VersionSortKey};
use pplx_space_execution_common::environment::EnvironmentDefinition;

fn definition() -> EnvironmentDefinition {
    EnvironmentDefinition::from_document(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../../../../../proto/execution/v1/fixtures/e0.json"
    )))
    .unwrap()
    .into_runtime_neutral()
    .unwrap()
}

fn certification(
    upgrade_edges: impl IntoIterator<Item = EnvironmentReference>,
    memory_edges: impl IntoIterator<Item = EnvironmentReference>,
    sort_key: Option<VersionSortKey>,
) -> KnownEnvironment {
    KnownEnvironment::new(definition(), sort_key, upgrade_edges, memory_edges).unwrap()
}

fn known_version(version: &str) -> KnownEnvironment {
    let mut spec = definition().descriptor().spec().clone();
    let RuntimeBinding::ManagedRelease(release) = &mut spec.runtime;
    release.runtime.version = version.parse().unwrap();
    KnownEnvironment::new(
        EnvironmentDefinition::new(spec.try_into().unwrap()),
        Some(VersionSortKey::new(vec![1]).unwrap()),
        [],
        [],
    )
    .unwrap()
}

async fn publish_known(pool: &PgPool, environment: &KnownEnvironment) -> TestResult {
    let mut tx = pool.begin().await?;
    assert_eq!(super::publish_known(&mut tx, [environment], 32).await?, 0);
    tx.commit().await?;
    Ok(())
}

#[derive(Debug, PartialEq, sqlx::FromRow)]
struct CertificationRow {
    upgrade_edges: Vec<String>,
    memory_edges: Vec<String>,
    version_sort_key: Option<Vec<i32>>,
    retired_at: Option<String>,
    updated_at: String,
    xmin: String,
}

async fn row(pool: &PgPool) -> Result<CertificationRow, sqlx::Error> {
    sqlx::query_as(sql!(
        "SELECT upgrade_edges, memory_edges, version_sort_key,
                retired_at::text, updated_at::text, xmin::text
         FROM runtime_environments WHERE environment_ref = $1"
    ))
    .bind(definition().reference().to_string())
    .fetch_one(pool)
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn publishes_known_definitions_and_preserves_retirement_without_row_churn() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let known = certification([], [], Some(VersionSortKey::new(vec![1, 15, 1])?));
    publish_known(&db.pool, &known).await?;
    assert_eq!(list_unretired(&db.pool).await?, vec![known.reference()]);
    let initial = row(&db.pool).await?;
    publish_known(&db.pool, &known).await?;
    assert_eq!(row(&db.pool).await?, initial);
    // The operator's exact timestamp, not merely its non-nullness, survives a
    // later node publication. xmin also proves no hidden UPDATE churn.
    sqlx::query(sql!(
        "UPDATE runtime_environments SET retired_at = now() - interval '1 day'
         WHERE environment_ref = $1"
    ))
    .bind(known.reference().to_string())
    .execute(&db.pool)
    .await?;
    let retired = row(&db.pool).await?;
    publish_known(&db.pool, &known).await?;
    assert_eq!(row(&db.pool).await?, retired);
    assert!(list_unretired(&db.pool).await?.is_empty());
    // Also force the real conflict UPDATE branch, not only the no-op WHERE
    // branch. A new build refusing ordering may update evidence, never the
    // operator's retirement timestamp.
    publish_known(&db.pool, &certification([], [], None)).await?;
    let updated = row(&db.pool).await?;
    assert_eq!(updated.retired_at, retired.retired_at);
    assert_ne!(updated.xmin, retired.xmin);
    assert!(updated.version_sort_key.is_none());
    assert!(list_unretired(&db.pool).await?.is_empty());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn concurrent_certifications_intersect_and_replays_never_regrow_edges() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let a = environment(10);
    let b = environment(11);
    let c = environment(12);
    let left = certification([a, b], [b], Some(VersionSortKey::new(vec![1, 15, 1])?));
    let right = certification([b, c], [b, c], Some(VersionSortKey::new(vec![1, 15, 1])?));
    // The two possible insert winners must lead to the same intersection.
    let (left_result, right_result) = tokio::join!(
        publish_known(&db.pool, &left),
        publish_known(&db.pool, &right),
    );
    left_result?;
    right_result?;
    let intersection = row(&db.pool).await?;
    assert_eq!(intersection.upgrade_edges, vec![b.to_string()]);
    assert_eq!(intersection.memory_edges, vec![b.to_string()]);
    publish_known(&db.pool, &left).await?;
    publish_known(&db.pool, &right).await?;
    assert_eq!(row(&db.pool).await?, intersection);
    // Existing-row updates take no admission lock. Concurrent builds can
    // withdraw different evidence without either update restoring the other.
    let without_memory = certification([b], [], Some(VersionSortKey::new(vec![1, 15, 1])?));
    let without_order = certification([b], [b], None);
    let (left_result, right_result) = tokio::join!(
        publish_known(&db.pool, &without_memory),
        publish_known(&db.pool, &without_order),
    );
    left_result?;
    right_result?;
    let concurrent = row(&db.pool).await?;
    assert_eq!(concurrent.upgrade_edges, vec![b.to_string()]);
    assert!(concurrent.memory_edges.is_empty());
    assert!(concurrent.version_sort_key.is_none());
    publish_known(&db.pool, &certification([], [], None)).await?;
    let refused = row(&db.pool).await?;
    assert!(refused.upgrade_edges.is_empty());
    assert!(refused.memory_edges.is_empty());
    assert!(refused.version_sort_key.is_none());
    publish_known(&db.pool, &left).await?;
    assert_eq!(row(&db.pool).await?, refused);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn conflicting_certifications_are_commutative_in_both_publication_orders() -> TestResult {
    let a = environment(10);
    let b = environment(11);
    let c = environment(12);
    let left = certification([a, b], [b], Some(VersionSortKey::new(vec![1, 15, 1])?));
    let right = certification([b, c], [b, c], Some(VersionSortKey::new(vec![1, 17, 0])?));
    for order in [[&left, &right], [&right, &left]] {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        for certification in order {
            publish_known(&db.pool, certification).await?;
        }
        let intersection = row(&db.pool).await?;
        assert_eq!(intersection.upgrade_edges, vec![b.to_string()]);
        assert_eq!(intersection.memory_edges, vec![b.to_string()]);
        assert!(intersection.version_sort_key.is_none());
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn publication_rolls_back_with_its_callers_transaction() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let mut tx = db.pool.begin().await?;
    super::publish_known(&mut tx, [&certification([], [], None)], 32).await?;
    assert_eq!(list_unretired(&mut *tx).await?.len(), 1);
    tx.rollback().await?;
    assert!(list_unretired(&db.pool).await?.is_empty());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn unchanged_publications_do_not_wait_for_another_publishers_commit() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let known = certification([], [], None);
    publish_known(&db.pool, &known).await?;
    let mut first = db.pool.begin().await?;
    super::publish_known(&mut first, [&known], 32).await?;
    let mut second = db.pool.begin().await?;
    sqlx::query(sql!("SET LOCAL lock_timeout = '250ms'"))
        .execute(&mut *second)
        .await?;
    // The first publisher is still open. A replay must not wait for it.
    assert_eq!(super::publish_known(&mut second, [&known], 32).await?, 0);
    second.commit().await?;
    first.rollback().await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn admission_is_bounded_across_concurrent_reports_and_preserves_existing_updates()
-> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let publish = |known: KnownEnvironment| {
        let pool = db.pool.clone();
        async move {
            let mut tx = pool.begin().await?;
            let deferred = super::publish_known(&mut tx, [&known], 2).await?;
            tx.commit().await?;
            Ok::<_, PublishKnownError>(deferred)
        }
    };
    let (a, b, c) = tokio::join!(
        publish(known_version("1.0.0")),
        publish(known_version("2.0.0")),
        publish(known_version("3.0.0")),
    );
    assert_eq!(a? + b? + c?, 1);
    assert_eq!(list_unretired(&db.pool).await?.len(), 2);
    // Repeated distinct reports cannot grow the persisted catalog.
    assert_eq!(publish(known_version("4.0.0")).await?, 1);
    let admitted = list_unretired(&db.pool).await?;
    let existing = ["1.0.0", "2.0.0", "3.0.0"]
        .into_iter()
        .map(known_version)
        .find(|known| known.reference() == admitted[0])
        .unwrap();
    let reduced = KnownEnvironment::new(existing.definition().clone(), None, [], [])?;
    let mut tx = db.pool.begin().await?;
    assert_eq!(super::publish_known(&mut tx, [&reduced], 2).await?, 0);
    tx.commit().await?;
    let sort_key: Option<Vec<i32>> = sqlx::query_scalar(sql!(
        "SELECT version_sort_key FROM runtime_environments WHERE environment_ref = $1"
    ))
    .bind(reduced.reference().to_string())
    .fetch_one(&db.pool)
    .await?;
    assert!(sort_key.is_none());
    sqlx::query(sql!(
        "UPDATE runtime_environments SET retired_at = now() WHERE environment_ref = $1"
    ))
    .bind(reduced.reference().to_string())
    .execute(&db.pool)
    .await?;
    assert_eq!(publish(known_version("4.0.0")).await?, 0);
    assert_eq!(list_unretired(&db.pool).await?.len(), 2);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn large_reports_obey_a_fixed_write_budget_and_update_existing_at_capacity() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let existing = known_version("1.0.0");
    let retired = known_version("2.0.0");
    publish_known(&db.pool, &existing).await?;
    publish_known(&db.pool, &retired).await?;
    sqlx::query(sql!(
        "UPDATE runtime_environments SET retired_at = now() WHERE environment_ref = $1"
    ))
    .bind(retired.reference().to_string())
    .execute(&db.pool)
    .await?;
    // Enforce a generous fixed write budget at the database, rather than a
    // timing threshold dependent on host load. It must not scale with report
    // length, including when every new entry is deferred.
    sqlx::query(sql!(
        "CREATE FUNCTION enforce_publication_write_budget() RETURNS trigger AS $$
         DECLARE remaining integer :=
             NULLIF(current_setting('space_test.catalog_write_budget', true), '')::integer;
         BEGIN
             IF remaining IS NOT NULL THEN
                 IF remaining <= 0 THEN
                     RAISE EXCEPTION 'publication catalog write budget exceeded';
                 END IF;
                 PERFORM set_config('space_test.catalog_write_budget', (remaining - 1)::text, true);
             END IF;
             RETURN NULL;
         END;
         $$ LANGUAGE plpgsql"
    ))
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "CREATE TRIGGER publication_write_budget
         BEFORE INSERT OR UPDATE ON runtime_environments
         FOR EACH STATEMENT EXECUTE FUNCTION enforce_publication_write_budget()"
    ))
    .execute(&db.pool)
    .await?;
    let mut report = (3..67)
        .map(|version| known_version(&format!("{version}.0.0")))
        .collect::<Vec<_>>();
    for known in [&existing, &retired] {
        report.push(KnownEnvironment::new(
            known.definition().clone(),
            None,
            [],
            [],
        )?);
    }
    let mut tx = db.pool.begin().await?;
    sqlx::query(sql!("SET LOCAL space_test.catalog_write_budget = '8'"))
        .execute(&mut *tx)
        .await?;
    assert_eq!(super::publish_known(&mut tx, &report, 1).await?, 64);
    tx.commit().await?;
    assert_eq!(list_unretired(&db.pool).await?, vec![existing.reference()]);
    let rows: Vec<(Option<Vec<i32>>, bool)> = sqlx::query_as(sql!(
        "SELECT version_sort_key, retired_at IS NOT NULL
         FROM runtime_environments ORDER BY retired_at NULLS FIRST"
    ))
    .fetch_all(&db.pool)
    .await?;
    assert_eq!(rows, vec![(None, false), (None, true)]);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn runtime_neutral_definitions_need_not_have_kernel_or_state_format() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let mut spec = definition().descriptor().spec().clone();
    let BootProfile::ArtifactsV1 { artifacts, .. } = &mut spec.boot else {
        panic!("normalized definition has generic boot artifacts");
    };
    artifacts.clear();
    let ExecutionCompatibilityProfile::RuntimeV1 { state_format, .. } = &mut spec.compatibility
    else {
        panic!("normalized definition has generic compatibility");
    };
    *state_format = None;
    let definition = EnvironmentDefinition::new(spec.try_into()?);
    let reference = definition.reference();
    publish_known(&db.pool, &KnownEnvironment::new(definition, None, [], [])?).await?;
    let metadata: (Option<String>, Option<String>, Option<String>) = sqlx::query_as(sql!(
        "SELECT kernel_version, state_format_name, state_format_version
             FROM runtime_environments WHERE environment_ref = $1"
    ))
    .bind(reference.to_string())
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(metadata, (None, None, None));
    assert_eq!(list_unretired(&db.pool).await?, vec![reference]);
    assert!(list_legacy_upgrade_candidates(&db.pool).await?.is_empty());
    Ok(())
}

fn environment(byte: u8) -> EnvironmentReference {
    EnvironmentReference::new(Digest::sha256([byte; 32]))
}

/// Insert a catalog row as a node publish would, retired or not.
async fn insert(pool: &PgPool, environment: &str, retired: bool) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO runtime_environments (
             environment_ref, runtime, runtime_version, version_sort_key,
             kernel_version, state_format_name, state_format_version, retired_at
         )
         VALUES ($1, 'firecracker', '1.17.0', '{1,17,0}', '6.1.155',
                 'firecracker-snapshot', '12.0.0',
                 CASE WHEN $2 THEN now() END)"
    ))
    .bind(environment)
    .bind(retired)
    .execute(pool)
    .await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn retired_environments_are_left_out() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    insert(&db.pool, &environment(2).to_string(), false).await?;
    insert(&db.pool, &environment(1).to_string(), true).await?;
    insert(&db.pool, &environment(0).to_string(), false).await?;

    assert_eq!(
        list_unretired(&db.pool).await?,
        vec![environment(0), environment(2)]
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn an_unparseable_reference_fails_the_read() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    // The schema refuses a malformed reference, so drop its CHECK in this
    // isolated database to stand in for a row the parser rejects.
    sqlx::query(sql!(
        "ALTER TABLE runtime_environments
         DROP CONSTRAINT runtime_environments_environment_ref_check"
    ))
    .execute(&db.pool)
    .await?;
    insert(&db.pool, &environment(0).to_string(), false).await?;
    insert(&db.pool, "sha256:not-a-digest", false).await?;

    let error = list_unretired(&db.pool)
        .await
        .expect_err("a malformed reference must fail the whole read");
    assert!(
        matches!(
            &error,
            ListUnretiredError::InvalidEnvironmentReference { value, .. }
                if value == "sha256:not-a-digest"
        ),
        "{error:?}"
    );
    Ok(())
}
