//! What a successful start does to the sandbox row's artifact pin.
//!
//! The property: **once a start lands, a disk pin no longer describes the
//! sandbox, so it's dropped; a suspend pin is kept, and a row the start can't
//! write keeps whatever it had.** A stopped re-placement's finalize leaves its
//! disk pin in place, and a pin that outlived the next start and stop would
//! make every later forced drain of the sandbox's node refuse it.

use pplx_space_common_core::types::{LocalArtifactId, SandboxId};

use super::*;
use pplx_space_db_test_fixtures::pg_test_db;
use pplx_space_test_env::TestResult;

async fn insert_pinned(
    pool: &PgPool,
    lifecycle: &str,
    pin: Option<(LocalArtifactId, &str, &str)>,
) -> TestResult<SandboxId> {
    let sandbox_id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, managed_by,
             network_policy_override_json, network_policy_generation,
             current_local_artifact_id, current_local_artifact_kind,
             current_local_artifact_origin
         )
         VALUES (
             $1,
             '00000000-0000-0000-0000-000000000010',
             '00000000-0000-0000-0000-000000000001',
             $2, 'unplaced', 'k8s', $3, 1, $4, $5, $6
         )"
    ))
    .bind(sandbox_id)
    .bind(lifecycle)
    .bind(sqlx::types::Json(
        &pplx_space_net_common::NetworkPolicyV1::default(),
    ))
    .bind(pin.map(|(id, _, _)| id))
    .bind(pin.map(|(_, kind, _)| kind))
    .bind(pin.map(|(_, _, origin)| origin))
    .execute(pool)
    .await?;
    Ok(sandbox_id)
}

async fn row(
    pool: &PgPool,
    sandbox_id: SandboxId,
) -> TestResult<(String, Option<LocalArtifactId>, Option<String>)> {
    Ok(sqlx::query_as(sql!(
        "SELECT lifecycle, current_local_artifact_id, current_local_artifact_kind
           FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?)
}

#[tokio::test(flavor = "current_thread")]
async fn a_successful_start_drops_a_disk_pin_and_keeps_a_suspend_pin() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;

    let disk = LocalArtifactId::new();
    let re_placed = insert_pinned(pool, "stopped", Some((disk, "disk", "stop"))).await?;
    let full = LocalArtifactId::new();
    let suspended = insert_pinned(pool, "suspended", Some((full, "full", "suspend"))).await?;
    let failed_disk = LocalArtifactId::new();
    let failed = insert_pinned(pool, "failed", Some((failed_disk, "disk", "stop"))).await?;

    for sandbox_id in [re_placed, suspended, failed] {
        maybe_mark_started_after_start(pool, sandbox_id).await;
    }

    assert_eq!(
        row(pool, re_placed).await?,
        ("started".to_owned(), None, None),
        "a started guest can write past a stopped rootfs's disk pin"
    );
    assert_eq!(
        row(pool, suspended).await?,
        ("started".to_owned(), Some(full), Some("full".to_owned())),
        "a suspend pin stays, as the node keeps it on `started`"
    );
    assert_eq!(
        row(pool, failed).await?,
        (
            "failed".to_owned(),
            Some(failed_disk),
            Some("disk".to_owned())
        ),
        "a row the start may not write keeps its pin"
    );
    Ok(())
}
