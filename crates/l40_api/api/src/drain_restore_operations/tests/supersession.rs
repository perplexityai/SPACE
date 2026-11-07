//! A user's lifecycle call that records no operation (an ECW verb, ingress)
//! supersedes a waiting drain restore at `SandboxController::action`, while
//! the row is `suspended`.

use std::time::Duration;

use crate::metrics::sandbox::api::SandboxApiMetricIdentity;
use pplx_space_common_core::sql;
use pplx_space_common_core::types::{OrgId, SandboxId};
use pplx_space_sandbox_store::sweeper::EvacuationRestoreTarget;
use uuid::Uuid;

use crate::controllers::sandbox::lifecycle::SandboxAction;
use crate::controllers::sandbox::{SandboxController, fetch_sandbox};
use crate::controllers::substrate::Substrate;
use crate::controllers::substrate::kubernetes::tests::{
    insert_test_sandbox_row, substrate_with_store,
};
use crate::drain_restore_operations::{DrainRestoreOperationPayload, enqueue_drain_restore};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

const ORG: OrgId = OrgId::from_uuid(Uuid::from_u128(1));

/// With the drain's v2 on, a superseding call on a `suspended` sandbox
/// cancels its waiting drain restore; a call that doesn't supersede one, and
/// any call on a sandbox that isn't `suspended`, leave it waiting. With it
/// off nothing enqueues one, and no call looks. The call's own outcome
/// doesn't matter here: the supersession comes first.
#[tokio::test(flavor = "current_thread")]
async fn a_superseding_call_on_a_suspended_sandbox_cancels_its_restore() -> Result<(), BoxError> {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let pool = &db.pool;
    let controller = SandboxController::new(Substrate::from_kubernetes(substrate_with_store(
        pool.clone(),
    )));
    for (case, lifecycle, action, enabled, canceled) in [
        (
            "ECW pause, suspended",
            "suspended",
            SandboxAction::Suspend,
            true,
        ),
        (
            "ECW resume, suspended",
            "suspended",
            SandboxAction::Restore,
            true,
        ),
        ("pause, suspended", "suspended", SandboxAction::Pause, true),
        (
            "checkpoint, suspended",
            "suspended",
            SandboxAction::Checkpoint,
            false,
        ),
        // A stopped sandbox has no drain restore: its next start places it.
        (
            "ECW pause, stopped",
            "stopped",
            SandboxAction::Suspend,
            false,
        ),
        // A restore never waits on a running sandbox: the call leaves it.
        (
            "ECW pause, started",
            "started",
            SandboxAction::Suspend,
            false,
        ),
    ]
    .into_iter()
    .flat_map(|(case, lifecycle, action, canceled)| {
        [(true, canceled), (false, false)]
            .map(|(enabled, canceled)| (case, lifecycle, action, enabled, canceled))
    }) {
        crate::drain_restore_operations::set_drain_v2_enabled(enabled);
        let id = SandboxId::new();
        insert_test_sandbox_row(pool, id, lifecycle).await?;
        let restore = enqueue_drain_restore(
            pool,
            ORG,
            DrainRestoreOperationPayload::new(id, EvacuationRestoreTarget::Started, Uuid::now_v7()),
        )
        .await?;
        let sandbox = fetch_sandbox(pool, id).await?.expect("the row exists");

        let _ = tokio::time::timeout(
            Duration::from_secs(3),
            controller.action(
                pool,
                id,
                Some(&sandbox),
                SandboxApiMetricIdentity::ecw("test"),
                action,
                None,
            ),
        )
        .await;

        let state: String = sqlx::query_scalar(sql!(
            "SELECT state FROM public_operations WHERE operation_id = $1"
        ))
        .bind(restore.as_uuid())
        .fetch_one(pool)
        .await?;
        let expected = if canceled { "canceled" } else { "queued" };
        assert_eq!(state, expected, "{case}, v2 enabled {enabled}");
    }
    Ok(())
}
