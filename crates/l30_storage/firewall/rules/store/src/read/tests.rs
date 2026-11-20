use pplx_space_db_test_fixtures::try_pg_test_db;
use pplx_space_firewall_rules_protocol::encode_revision;
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use uuid::Uuid;

use super::*;
use crate::tests::rollout;

/// Store a revision of `rules` in `version` and adopt it.
async fn adopt(pool: &PgPool, version: i64, rules: &str) -> TestResult<RevisionId> {
    let id = RevisionId(Uuid::now_v7());
    sqlx::query(sql!(
        "WITH revision AS (
             INSERT INTO firewall_rules_revisions (id, ruleset_format_version, ruleset)
             VALUES ($1, $2, $3::jsonb) RETURNING id)
         UPDATE firewall_rules_control
         SET enrollment_default_revision_id = (SELECT id FROM revision)"
    ))
    .bind(id)
    .bind(version)
    .bind(rules)
    .execute(pool)
    .await?;
    Ok(id)
}

/// Catches an empty adopted revision read as none, the newest revision or an
/// unfinished rollout's candidate read instead of the adopted one, rules
/// reordered, and an unreadable or missing default read as a first push.
#[tokio::test(flavor = "current_thread")]
async fn only_the_adopted_document_is_read_and_faults_are_errors() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    assert_eq!(read_adopted(pool).await?, None);

    let empty = adopt(pool, 1, "[]").await?;
    let (id, revision) = read_adopted(pool).await?.ok_or("nothing adopted")?;
    assert_eq!(id, empty);
    assert_eq!(revision.rules(), []);

    // Not in ID order, and with the canonical encoding's key order.
    let rules = r#"[{"id":"web","all_of":[{"left":{"payload":{"protocol":"tcp","field":"dport"}},"op":"==","right":80}]},{"id":"dns","all_of":[{"left":{"payload":{"protocol":"udp","field":"dport"}},"op":"==","right":53}]}]"#;
    let adopted = adopt(pool, 1, rules).await?;
    rollout(pool, "canary").await?;
    let (id, revision) = read_adopted(pool).await?.ok_or("nothing adopted")?;
    let document = format!(r#"{{"schema_version":1,"rules":{rules}}}"#);
    assert_eq!((id, encode_revision(&revision)), (adopted, document));

    let newer = adopt(pool, 2, "[]").await?;
    let unreadable = read_adopted(pool).await;
    assert!(
        matches!(unreadable, Err(ReadAdoptedError::StoredRevision { id, .. }) if id == newer),
        "{unreadable:?}"
    );
    sqlx::query(sql!("DELETE FROM firewall_rules_control"))
        .execute(pool)
        .await?;
    let missing = read_adopted(pool).await;
    assert!(
        matches!(missing, Err(ReadAdoptedError::MissingControlRow)),
        "{missing:?}"
    );
    Ok(())
}
