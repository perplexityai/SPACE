//! Characterization of `complete_direct_restore_lease`, the Direct restore's
//! final write to Postgres `sandboxes.lifecycle`, against the real Postgres
//! fixture.
//!
//! The statement's only guard is the restore lease token: it names no
//! lifecycle. What keeps it from completing a row that left `restoring` is the
//! `sandboxes_clear_restore_lease` trigger, which clears the token on every
//! write that leaves the row outside `restoring`. So the sweep moves a claimed
//! row to each stored lifecycle with a plain update, as a concurrent writer
//! would, and then completes it.

use pplx_space_common_core::types::SandboxLifecycle;

use super::*;
use crate::controllers::sandbox::lifecycle::characterization_tests::NON_VARIANT_LIFECYCLES;

/// The Postgres `sandboxes` columns the completion writes, and the lease
/// columns the triggers move with its lifecycle.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
struct Row {
    lifecycle: String,
    placement: String,
    restore_lease_token: Option<Uuid>,
    lease_expires: bool,
    preserve_local_artifact_on_restore_failure: bool,
}

async fn row(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<Row> {
    Ok(sqlx::query_as::<_, Row>(sql!(
        "SELECT lifecycle, placement, restore_lease_token,
                restore_lease_expires_at IS NOT NULL AS lease_expires,
                preserve_local_artifact_on_restore_failure
           FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id.as_uuid())
    .fetch_one(pool)
    .await?)
}

async fn set_lifecycle(pool: &PgPool, sandbox_id: SandboxId, lifecycle: &str) -> TestResult {
    sqlx::query(sql!("UPDATE sandboxes SET lifecycle = $2 WHERE id = $1"))
        .bind(sandbox_id.as_uuid())
        .bind(lifecycle)
        .execute(pool)
        .await?;
    Ok(())
}

/// A claimed restore completes to `started` / `on_node`, releasing its lease,
/// only while the row is still `restoring`: a move to any other stored
/// lifecycle cleared the token, so the completion is refused as a lost
/// restore and the row keeps what that move wrote.
#[tokio::test(flavor = "current_thread")]
async fn completion_applies_only_while_the_claimed_row_is_still_restoring() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let stored = SandboxLifecycle::all()
        .iter()
        .map(|lifecycle| lifecycle.as_str())
        .chain(NON_VARIANT_LIFECYCLES);

    let mut mismatches = Vec::new();
    for lifecycle in stored {
        let sandbox_id = SandboxId::new();
        insert_sandbox(&fixture.pool, sandbox_id, None).await?;
        let lease = fixture
            .substrate
            .claim_direct_restore_lease(sandbox_id)
            .await?;
        set_lifecycle(&fixture.pool, sandbox_id, lifecycle).await?;
        let before = row(&fixture.pool, sandbox_id).await?;

        let result = fixture
            .substrate
            .complete_direct_restore_lease(sandbox_id, lease)
            .await;

        let completes = lifecycle == "restoring";
        let expected = if completes {
            Row {
                lifecycle: "started".to_owned(),
                placement: "on_node".to_owned(),
                restore_lease_token: None,
                lease_expires: false,
                preserve_local_artifact_on_restore_failure: false,
            }
        } else {
            before
        };
        let refused = matches!(result, Err(SubstrateError::RestoreInFlight { .. }));
        let actual = row(&fixture.pool, sandbox_id).await?;
        if refused == completes || actual != expected {
            mismatches.push(format!("{lifecycle}: {result:?}, row {actual:?}"));
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
    Ok(())
}

/// A lease that expired and was claimed again no longer completes: the
/// newer claim's token stands, and the row stays `restoring` under it.
#[tokio::test(flavor = "current_thread")]
async fn completion_refuses_a_lease_another_claim_replaced() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, None).await?;
    let stale = fixture
        .substrate
        .claim_direct_restore_lease(sandbox_id)
        .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET restore_lease_expires_at = NOW() - INTERVAL '1 second'
          WHERE id = $1"
    ))
    .bind(sandbox_id.as_uuid())
    .execute(&fixture.pool)
    .await?;
    let current = fixture
        .substrate
        .claim_direct_restore_lease(sandbox_id)
        .await?;
    let before = row(&fixture.pool, sandbox_id).await?;
    assert_eq!(before.restore_lease_token, Some(current.token));

    let error = fixture
        .substrate
        .complete_direct_restore_lease(sandbox_id, stale)
        .await
        .expect_err("a replaced lease must not complete the restore");

    assert!(
        matches!(error, SubstrateError::RestoreInFlight { .. }),
        "{error:?}"
    );
    assert_eq!(row(&fixture.pool, sandbox_id).await?, before);
    Ok(())
}
