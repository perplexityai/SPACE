//! The suspend action's lifecycle write on the Kubernetes substrate, whose
//! `suspend_execution` commits `suspended` itself.

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use pplx_space_common_core::types::{LocalArtifactId, SandboxId};
use pplx_space_db_test_fixtures::pg_test_db;
use pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity;
use pplx_space_test_env::TestResult;

use super::*;
use crate::controllers::substrate::kubernetes::tests::{
    deleted_pod_kubernetes_client, insert_k8s_execution, insert_test_sandbox_row,
    substrate_with_store_and_client,
};

const POD_NAME: &str = "space-sbx-suspend-race-g1";
const POD_UID: &str = "suspend-race-uid";

/// A wake that claims the sandbox right after the substrate commits its
/// suspend keeps the row. The suspend action writes no lifecycle of its own
/// afterwards: a trailing `suspended` would put the woken, routed sandbox
/// back to `suspended` while its new execution stays active, the shape whose
/// pod holds the only copy of the sandbox's state.
///
/// A deferred trigger stands in for the wake: it moves the row to
/// `restoring` when the transaction that committed `suspended` commits,
/// which is after the substrate's write and before anything the controller
/// does next.
#[tokio::test(flavor = "current_thread")]
async fn suspend_keeps_a_wake_that_claimed_after_the_suspend_committed() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let sandbox_id = SandboxId::new();
    insert_test_sandbox_row(pool, sandbox_id, "suspending").await?;
    // The node already committed the suspend, so the substrate only retires
    // the pod and finalizes the row; it never reaches the node.
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET current_local_artifact_id = $2,
                current_local_artifact_kind = 'full',
                current_local_artifact_origin = 'suspend'
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(LocalArtifactId::new())
    .execute(pool)
    .await?;
    insert_k8s_execution(pool, sandbox_id, 1, POD_NAME, POD_UID, None, true).await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions SET state = 'suspended' WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "CREATE FUNCTION wake_claims_after_suspend() RETURNS trigger AS $$
         BEGIN
             UPDATE sandboxes SET lifecycle = 'restoring' WHERE id = NEW.id;
             RETURN NULL;
         END;
         $$ LANGUAGE plpgsql"
    ))
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "CREATE CONSTRAINT TRIGGER wake_claims_after_suspend
             AFTER UPDATE OF lifecycle ON sandboxes
             DEFERRABLE INITIALLY DEFERRED
             FOR EACH ROW
             WHEN (OLD.lifecycle = 'suspending' AND NEW.lifecycle = 'suspended')
             EXECUTE FUNCTION wake_claims_after_suspend()"
    ))
    .execute(pool)
    .await?;

    let mut substrate = substrate_with_store_and_client(
        pool.clone(),
        deleted_pod_kubernetes_client(POD_NAME, POD_UID, Arc::new(AtomicUsize::new(0))),
    );
    substrate.k8s.k8s_blocking_delete_timeout = std::time::Duration::from_millis(500);
    let controller = SandboxController::new(crate::controllers::substrate::Substrate::Kubernetes(
        Box::new(substrate),
    ));
    let sandbox = crate::controllers::sandbox::fetch_sandbox(pool, sandbox_id)
        .await?
        .expect("sandbox row exists");

    controller
        .action(
            pool,
            sandbox_id,
            Some(&sandbox),
            SandboxApiMetricIdentity::native(SandboxAction::Suspend.as_str()),
            SandboxAction::Suspend,
            None,
        )
        .await?;

    let lifecycle: String =
        sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
            .bind(sandbox_id)
            .fetch_one(pool)
            .await?;
    assert_eq!(lifecycle, "restoring");
    Ok(())
}
