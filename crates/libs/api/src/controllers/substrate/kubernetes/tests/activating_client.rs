//! A pod is labeled with the client that activated its execution. The key is
//! stored on the execution row, so a pod rebuilt from the row keeps it, and a
//! replacement generation inherits it unless the activation names a client.

use std::sync::{Arc, Mutex};

use pplx_space_common_core::kubernetes::SANDBOX_CLIENT_ID_LABEL;
use pplx_space_common_core::sql;
use pplx_space_common_core::types::{
    ClientKey, SandboxActivationAttemptId, SandboxId, SandboxTransitionTargetState,
};
use pplx_space_test_env::TestResult;
use uuid::Uuid;

use super::*;
use crate::controllers::substrate::kubernetes::pod_ops::{
    ActivationAttemptAssignment, PodMaterializationOwnership, StartedRecoveryOwnership,
};

/// A started sandbox whose generation-2 execution, activated by `background-client`, lost
/// its pod; a ready portable snapshot lets recovery replace the generation.
async fn sandbox_with_a_lost_background_execution(pool: &sqlx::PgPool) -> TestResult<SandboxId> {
    let sandbox_id = SandboxId::new();
    let artifact_id = Uuid::now_v7();
    insert_test_sandbox_row(pool, sandbox_id, "started").await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET current_local_artifact_id = $2,
                current_local_artifact_kind = 'full',
                current_local_artifact_origin = 'suspend'
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(artifact_id)
    .execute(pool)
    .await?;
    insert_ready_full_snapshot_relative_to_lifecycle(
        pool,
        sandbox_id,
        "s3://space-test/suspend-snapshots/ready?region=us-east-1",
        1,
        artifact_id,
    )
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
             (sandbox_id, generation, pod_name, pod_namespace, pod_uid, state, active,
              activating_client_key)
         VALUES
             ($1, 2, 'space-sbx-missing-g2', 'space', 'missing-pod-uid',
              'recovering', TRUE, 'background-client')"
    ))
    .bind(sandbox_id)
    .execute(pool)
    .await?;
    Ok(sandbox_id)
}

#[rstest::rstest]
#[case::inherits_without_a_client(None, "background-client")]
#[case::takes_the_activating_client(Some("scheduled-client"), "scheduled-client")]
#[tokio::test(flavor = "current_thread")]
async fn a_replacement_generation_is_attributed_to_its_activating_client(
    #[case] activating_client: Option<&str>,
    #[case] expected: &str,
) -> TestResult {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let pool = db.pool.clone();
    let sandbox_id = sandbox_with_a_lost_background_execution(&pool).await?;
    let sandbox = crate::controllers::sandbox::fetch_sandbox(&pool, sandbox_id)
        .await?
        .expect("inserted sandbox must be visible");
    let captured_pod_creates = Arc::new(Mutex::new(Vec::new()));
    let substrate = substrate_with_store_and_client(
        pool.clone(),
        mock_kubernetes_client(Arc::clone(&captured_pod_creates)),
    );
    let assignment = ActivationAttemptAssignment::new(
        SandboxActivationAttemptId::new(),
        SandboxTransitionTargetState::Started,
        activating_client.map(|id| ClientKey::allowlisted(id).expect("valid client key")),
    );

    substrate
        .ensure_pod_exists(
            &sandbox,
            None,
            None,
            None,
            PodMaterializationOwnership::new(
                Some(StartedRecoveryOwnership::ExistingExecution { generation: 2 }),
                None,
                Some(assignment),
            ),
        )
        .await?;

    let recorded: Option<String> = sqlx::query_scalar(sql!(
        "SELECT activating_client_key
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(recorded.as_deref(), Some(expected));
    let created_pods = captured_pod_creates.lock().expect("capture mutex poisoned");
    let [pod] = created_pods.as_slice() else {
        panic!("expected one pod create, got {}", created_pods.len());
    };
    assert_eq!(
        pod.pointer("/metadata/labels")
            .and_then(|labels| labels.get(SANDBOX_CLIENT_ID_LABEL))
            .and_then(serde_json::Value::as_str),
        Some(expected)
    );
    Ok(())
}

/// A started sandbox whose execution was retired has no active row, so the
/// started-recovery claim inserts the next generation itself. It records the
/// waking client when there is one, and inherits otherwise.
#[rstest::rstest]
#[case::inherits_without_a_client(None, "background-client")]
#[case::takes_the_activating_client(Some("scheduled-client"), "scheduled-client")]
#[tokio::test(flavor = "current_thread")]
async fn a_started_recovery_claim_attributes_its_new_generation(
    #[case] activating_client: Option<&str>,
    #[case] expected: &str,
) -> TestResult {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let pool = db.pool.clone();
    let sandbox_id = SandboxId::new();
    insert_test_sandbox_row(&pool, sandbox_id, "started").await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
             (sandbox_id, generation, pod_name, pod_namespace, state, active,
              activating_client_key)
         VALUES
             ($1, 1, 'space-sbx-retired-g1', 'space', 'replaced', FALSE, 'background-client')"
    ))
    .bind(sandbox_id)
    .execute(&pool)
    .await?;
    let substrate = substrate_with_store(pool.clone());
    let client = activating_client.map(|id| ClientKey::allowlisted(id).expect("valid client key"));

    substrate
        .store
        .claim_started_recovery(sandbox_id, client.as_ref())
        .await?;

    let recorded: (i64, Option<String>) = sqlx::query_as(sql!(
        "SELECT generation, activating_client_key
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(recorded, (2, Some(expected.to_string())));
    Ok(())
}

/// A generation an older writer inserted has no key. Inheritance skips it, so
/// one unrecorded generation doesn't erase the attribution for every later
/// successor.
#[tokio::test(flavor = "current_thread")]
async fn inheritance_skips_a_generation_with_no_recorded_client() -> TestResult {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let pool = db.pool.clone();
    let sandbox_id = SandboxId::new();
    insert_test_sandbox_row(&pool, sandbox_id, "started").await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
             (sandbox_id, generation, pod_name, pod_namespace, state, active,
              activating_client_key)
         VALUES
             ($1, 1, 'space-sbx-g1', 'space', 'replaced', FALSE, 'background-client'),
             ($1, 2, 'space-sbx-g2', 'space', 'replaced', FALSE, NULL)"
    ))
    .bind(sandbox_id)
    .execute(&pool)
    .await?;
    let substrate = substrate_with_store(pool.clone());

    substrate
        .store
        .claim_started_recovery(sandbox_id, None)
        .await?;

    let recorded: (i64, Option<String>) = sqlx::query_as(sql!(
        "SELECT generation, activating_client_key
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(recorded, (3, Some("background-client".to_string())));
    Ok(())
}
