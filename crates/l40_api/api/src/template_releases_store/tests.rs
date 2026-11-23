//! A digest stored under both spellings, `sha256:<hex>` and bare `<hex>`:
//! the API's create-by-digest and placement lookups answer the release
//! `pplx_space_template_release_original::digest_original` picks, which the
//! shared spelling-pair fixture names. The internal API and warm-pool tests
//! assert against the same fixture.

use super::*;
use pplx_space_db_test_fixtures::pg_test_db;
use pplx_space_template_release_store::spelling_pairs::{OLD_INDEX, seed_spelling_pairs};
use pplx_space_template_store::test_support::tag_release;
use pplx_space_test_env::TestResult;

const TEMPLATE: &str = "spelling-pairs";

/// Create-by-digest and create-by-tag resolve either spelling to the
/// digest's original, and
/// placement reads that release's binding. Every release the rule doesn't
/// pick is bound to an environment of its own, so answering one of them
/// would show.
#[rstest::rstest]
#[case::without_twins(false)]
#[case::with_twins(true)]
#[tokio::test(flavor = "current_thread")]
async fn create_and_placement_resolve_the_digest_original(#[case] twins: bool) -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    sqlx::query(sql!("INSERT INTO templates (id, name) VALUES ($1, $2)"))
        .bind(Uuid::now_v7())
        .bind(TEMPLATE)
        .execute(pool)
        .await?;
    // Twins need the old index gone: they share their original's template
    // and hash. Without them, the case runs on the schema this change first
    // deploys to, with the old index in place.
    if twins {
        sqlx::query(sqlx::AssertSqlSafe(sql!(format!(
            "DROP INDEX IF EXISTS {OLD_INDEX}"
        ))))
        .execute(pool)
        .await?;
    }
    let twin_metadata = twins.then_some(r#"{"environment": "fixture-twin"}"#);
    let pairs = seed_spelling_pairs(pool, TEMPLATE, twin_metadata).await?;
    sqlx::query(sql!(
        "UPDATE template_releases
         SET execution_metadata = jsonb_build_object('environment', id::TEXT)
         WHERE template_name = $1 AND execution_metadata IS NULL"
    ))
    .bind(TEMPLATE)
    .execute(pool)
    .await?;

    let template = TemplateName::parse(TEMPLATE)?;
    for pair in &pairs {
        // A create names the canonical spelling; a bare `<hex>` reference
        // doesn't parse.
        let resolved = resolve_template_release_by_digest(
            pool,
            &template,
            pair.canonical_hash.parse()?,
            RuntimeKind::Firecracker,
            OrgId::new(),
        )
        .await?;
        assert_eq!(resolved, Some((pair.original, pair.canonical_hash.clone())));
        // A create by a tag left on the other spelling's original, as one
        // written before the shared rule can be, resolves the original too,
        // with the hash as the tagged release stores it.
        tag_release(pool, TEMPLATE, pair.other(), "legacy").await?;
        let other_hash = if pair.other() == pair.canonical {
            &pair.canonical_hash
        } else {
            &pair.bare_hash
        };
        let tagged = resolve_template_release_by_tag(
            pool,
            &template,
            &TemplateTag::parse("legacy")?,
            RuntimeKind::Firecracker,
            OrgId::new(),
        )
        .await?;
        assert_eq!(tagged, Some((pair.original, other_hash.clone())));
        for hash in [&pair.canonical_hash, &pair.bare_hash] {
            assert_eq!(
                release_environment(pool, TEMPLATE, hash).await?,
                Some(pair.original.to_string()),
                "{hash}"
            );
        }
    }
    Ok(())
}
