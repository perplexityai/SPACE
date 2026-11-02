use pplx_space_common_core::sql;
use pplx_space_common_core::types::{ClientId, SandboxId, SandboxLifecycle};
use pplx_space_db_test_fixtures::try_pg_test_db;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use uuid::Uuid;

use super::*;

async fn sandbox_in(pool: &PgPool, lifecycle: &str) -> TestResult<SandboxId> {
    let id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO sandboxes
            (id, user_id, org_id, managed_by, lifecycle, template_id, network_policy_generation)
         VALUES ($1, $2, $3, 'k8s', $4, 'tmpl-store-test', 1)"
    ))
    .bind(id)
    .bind(Uuid::from_u128(0x10))
    .bind(Uuid::from_u128(0x1))
    .bind(lifecycle)
    .execute(pool)
    .await?;
    Ok(SandboxId::from_uuid(id))
}

async fn record(pool: &PgPool, id: SandboxId) -> TestResult<Option<String>> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT activating_client_id FROM sandboxes WHERE id = $1"
    ))
    .bind(id)
    .fetch_one(pool)
    .await?)
}

/// The record follows a new runtime in every lifecycle where an activation
/// can be creating one, and never in the others.
#[tokio::test(flavor = "current_thread")]
async fn the_record_is_written_only_where_an_activation_can_create_a_runtime() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let alpha = ClientId::from_header(Some("alpha"));
    for lifecycle in SandboxLifecycle::all()
        .iter()
        .filter(|lifecycle| attributes_runtime(**lifecycle))
        .map(|lifecycle| lifecycle.as_str())
    {
        let id = sandbox_in(&db.pool, lifecycle).await?;
        assert!(
            set_activating_client(&db.pool, id, Some(&alpha)).await?,
            "{lifecycle}"
        );
        assert_eq!(
            record(&db.pool, id).await?.as_deref(),
            Some("alpha"),
            "{lifecycle}"
        );
        assert_eq!(
            activating_client(&db.pool, id).await?,
            Some(Some(alpha.clone())),
            "{lifecycle}"
        );
    }
    for lifecycle in UNATTRIBUTED_LIFECYCLES {
        let id = sandbox_in(&db.pool, lifecycle).await?;
        assert!(
            !set_activating_client(&db.pool, id, Some(&alpha)).await?,
            "{lifecycle}"
        );
        assert_eq!(record(&db.pool, id).await?, None, "{lifecycle}");
    }
    Ok(())
}

/// Writing the value already recorded changes nothing, and a runtime with no
/// attribution clears the record.
#[tokio::test(flavor = "current_thread")]
async fn an_unchanged_value_is_a_no_op_and_none_clears_the_record() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let id = sandbox_in(&db.pool, "started").await?;
    let native = ClientId::native();

    assert!(set_activating_client(&db.pool, id, Some(&native)).await?);
    assert!(!set_activating_client(&db.pool, id, Some(&native)).await?);
    assert!(set_activating_client(&db.pool, id, None).await?);
    assert_eq!(record(&db.pool, id).await?, None);
    assert_eq!(activating_client(&db.pool, id).await?, Some(None));
    assert!(!set_activating_client(&db.pool, id, None).await?);
    assert_eq!(activating_client(&db.pool, SandboxId::new()).await?, None);
    Ok(())
}

/// The lifecycle classification and the list the write guards with agree:
/// every typed lifecycle the classification rejects is listed, no listed one
/// is attributed, and the rest of the list are stored states with no variant.
#[test]
fn the_unattributed_list_matches_the_lifecycle_classification() {
    for lifecycle in SandboxLifecycle::all() {
        assert_eq!(
            UNATTRIBUTED_LIFECYCLES.contains(&lifecycle.as_str()),
            !attributes_runtime(*lifecycle),
            "{lifecycle:?}"
        );
    }
    for listed in UNATTRIBUTED_LIFECYCLES {
        let typed = SandboxLifecycle::all()
            .iter()
            .any(|lifecycle| lifecycle.as_str() == listed);
        assert!(
            typed || ["deleted", "cleanup_required"].contains(&listed),
            "{listed}"
        );
    }
}
