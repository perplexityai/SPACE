use pplx_space_common_core::sql;
use pplx_space_common_core::types::{
    BootProfile, Digest, EnvironmentReference, ExecutionCompatibilityProfile, VersionSortKey,
};
use pplx_space_db_test_fixtures::try_pg_test_db;
use pplx_space_execution_common::environment::EnvironmentDefinition;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;

use super::*;

fn definition() -> EnvironmentDefinition {
    EnvironmentDefinition::from_document(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../../../../../proto/execution/v1/fixtures/e0.json"
    )))
    .unwrap()
    .into_runtime_neutral()
    .unwrap()
}

#[tokio::test]
async fn registration_is_idempotent_and_preserves_existing_metadata_and_retirement() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let target = environment(10);
    let known = KnownEnvironment::new(
        definition(),
        Some(VersionSortKey::new(vec![1, 17, 0])?),
        [target],
        [],
    )?;
    let (first, second) = tokio::join!(
        register_known(&db.pool, &known),
        register_known(&db.pool, &known),
    );
    first?;
    second?;
    assert_eq!(list_unretired(&db.pool).await?, vec![known.reference()]);

    sqlx::query(sql!(
        "UPDATE runtime_environments SET retired_at = now() WHERE environment_ref = $1"
    ))
    .bind(known.reference().to_string())
    .execute(&db.pool)
    .await?;
    let conflicting = KnownEnvironment::new(definition(), None, [], [])?;
    register_known(&db.pool, &conflicting).await?;
    let metadata: (Option<Vec<i32>>, Vec<String>, bool) = sqlx::query_as(sql!(
        "SELECT version_sort_key, upgrade_edges, retired_at IS NOT NULL
         FROM runtime_environments WHERE environment_ref = $1"
    ))
    .bind(known.reference().to_string())
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(
        metadata,
        (Some(vec![1, 17, 0]), vec![target.to_string()], true)
    );
    assert!(list_unretired(&db.pool).await?.is_empty());
    Ok(())
}

#[tokio::test]
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
    let known = KnownEnvironment::new(EnvironmentDefinition::new(spec.try_into()?), None, [], [])?;
    register_known(&db.pool, &known).await?;
    let metadata: (Option<String>, Option<String>, Option<String>) = sqlx::query_as(sql!(
        "SELECT kernel_version, state_format_name, state_format_version
         FROM runtime_environments WHERE environment_ref = $1"
    ))
    .bind(known.reference().to_string())
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(metadata, (None, None, None));
    assert_eq!(list_unretired(&db.pool).await?, vec![known.reference()]);
    assert!(list_legacy_upgrade_candidates(&db.pool).await?.is_empty());
    Ok(())
}

fn environment(byte: u8) -> EnvironmentReference {
    EnvironmentReference::new(Digest::sha256([byte; 32]))
}

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
    // Simulate corrupted persisted data in this isolated database.
    sqlx::query(sql!(
        "ALTER TABLE runtime_environments
         DROP CONSTRAINT runtime_environments_environment_ref_check"
    ))
    .execute(&db.pool)
    .await?;
    insert(&db.pool, &environment(0).to_string(), false).await?;
    insert(&db.pool, "sha256:not-a-digest", false).await?;
    let error = list_unretired(&db.pool).await.unwrap_err();
    assert!(matches!(
        error,
        ListUnretiredError::InvalidEnvironmentReference { value, .. }
            if value == "sha256:not-a-digest"
    ));
    Ok(())
}
