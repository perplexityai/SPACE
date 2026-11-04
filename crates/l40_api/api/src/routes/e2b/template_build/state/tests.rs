use super::*;
use pplx_space_db_test_fixtures::{TestDbResult, pg_test_db};
use rstest::rstest;

#[rstest]
#[case::same_owner_retry(1, "original", true)]
#[case::same_owner_reupload(1, "replacement", true)]
#[case::other_owner(2, "original", false)]
#[case::other_owner_replacement(2, "replacement", false)]
#[tokio::test]
async fn owned_blob_writes_allow_only_the_owner_to_replace_content(
    #[case] caller: u128,
    #[case] digest: &str,
    #[case] accepted: bool,
) -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let state = TemplateBuildState::new(db.pool.clone());
    let name = TemplateName::parse("build-inputs").unwrap();
    let owner = OrgId::from_uuid(uuid::Uuid::from_u128(1));
    state.record_blob(owner, &name, "files", "original").await?;
    let caller = OrgId::from_uuid(uuid::Uuid::from_u128(caller));

    let result = state.record_blob(caller, &name, "files", digest).await;

    assert_eq!(result.is_ok(), accepted);
    if !accepted {
        assert!(matches!(result, Err(RecordBlobError::Conflict)));
    }
    let row: (String, Option<uuid::Uuid>) = sqlx::query_as(sql!(
        "SELECT blob_sha256, org_id FROM template_build_blobs WHERE template_id = $1 AND files_hash = 'files'"
    )).bind(name.as_str()).fetch_one(&db.pool).await?;
    let expected_digest = if accepted { digest } else { "original" };
    assert_eq!(row, (expected_digest.into(), Some(owner.as_uuid())));
    Ok(())
}

/// Mappings written before uploads carried an owner have `org_id IS NULL`.
async fn insert_unowned_blob(
    pool: &PgPool,
    name: &TemplateName,
    files_hash: &str,
    blob_sha256: &str,
) -> TestDbResult<()> {
    sqlx::query(sql!(
        "INSERT INTO template_build_blobs (template_id, files_hash, blob_sha256, org_id)
         VALUES ($1, $2, $3, NULL)"
    ))
    .bind(name.as_str())
    .bind(files_hash)
    .bind(blob_sha256)
    .execute(pool)
    .await?;
    Ok(())
}

/// The first owned upload claims an unowned mapping, so later writers meet
/// the ownership check instead of a row that anyone may replace.
#[tokio::test]
async fn owned_upload_claims_an_unowned_legacy_mapping() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let state = TemplateBuildState::new(db.pool.clone());
    let name = TemplateName::parse("build-inputs").unwrap();
    let owner = OrgId::from_uuid(uuid::Uuid::from_u128(1));
    insert_unowned_blob(&db.pool, &name, "files", "legacy").await?;

    state.record_blob(owner, &name, "files", "retry").await?;

    let row: (String, Option<uuid::Uuid>) = sqlx::query_as(sql!(
        "SELECT blob_sha256, org_id FROM template_build_blobs WHERE template_id = $1 AND files_hash = 'files'"
    )).bind(name.as_str()).fetch_one(&db.pool).await?;
    assert_eq!(row, ("retry".into(), Some(owner.as_uuid())));
    Ok(())
}

#[tokio::test]
async fn concurrent_cross_org_blob_writers_cannot_replace_the_winner() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let state = TemplateBuildState::new(db.pool.clone());
    let name = TemplateName::parse("build-inputs").unwrap();
    let first_owner = OrgId::from_uuid(uuid::Uuid::from_u128(1));
    let second_owner = OrgId::from_uuid(uuid::Uuid::from_u128(2));

    let (a, b) = tokio::join!(
        state.record_blob(first_owner, &name, "files", "a"),
        state.record_blob(second_owner, &name, "files", "b")
    );

    let winner = match (a, b) {
        (Ok(()), Err(RecordBlobError::Conflict)) => ("a".to_string(), first_owner.as_uuid()),
        (Err(RecordBlobError::Conflict), Ok(())) => ("b".to_string(), second_owner.as_uuid()),
        result => panic!("one organization must retain the mapping: {result:?}"),
    };
    let row: (String, uuid::Uuid) = sqlx::query_as(sql!(
        "SELECT blob_sha256, org_id FROM template_build_blobs WHERE template_id = $1 AND files_hash = 'files'"
    )).bind(name.as_str()).fetch_one(&db.pool).await?;
    assert_eq!(row, winner);
    Ok(())
}

#[rstest]
#[case::owner(Some(1), 1, true)]
#[case::other_org(Some(1), 2, false)]
#[case::legacy(None, 1, true)]
#[case::legacy_other_org(None, 2, true)]
#[tokio::test]
async fn copy_reads_enforce_known_owners_and_preserve_legacy_uploads(
    #[case] owner: Option<u128>,
    #[case] caller: u128,
    #[case] visible: bool,
) -> TestDbResult<()> {
    use super::super::{
        translate::build_recipe,
        wire::{TemplateBuildStartV2, TemplateStep},
    };
    let db = pg_test_db().await?;
    let state = TemplateBuildState::new(db.pool.clone());
    let name = TemplateName::parse("build-inputs").unwrap();
    let digest = format!("sha256:{}", "a".repeat(64));
    match owner {
        Some(id) => {
            let owner = OrgId::from_uuid(uuid::Uuid::from_u128(id));
            state.record_blob(owner, &name, "files", &digest).await?;
        }
        None => insert_unowned_blob(&db.pool, &name, "files", &digest).await?,
    }
    let caller = OrgId::from_uuid(uuid::Uuid::from_u128(caller));
    let request = TemplateBuildStartV2 {
        steps: vec![TemplateStep {
            kind: "COPY".into(),
            args: vec!["file".into(), "/app/file".into()],
            files_hash: Some("files".into()),
            ..Default::default()
        }],
        ..Default::default()
    };

    let recipe = build_recipe(caller, &name, &request, &state, None, None).await;

    match recipe {
        Ok(recipe) => {
            assert!(visible);
            let Some(pplx_space_build_mgr_client::proto::recipe::step::Body::Copy(copy)) =
                recipe.steps[0].body.as_ref()
            else {
                panic!("expected COPY step");
            };
            assert_eq!(
                copy.payload,
                Some(pplx_space_build_mgr_client::proto::recipe::copy::Payload::BlobSha256(digest))
            );
        }
        Err(error) => {
            assert!(!visible);
            assert_eq!(error.status.as_u16(), 400);
        }
    }
    Ok(())
}
