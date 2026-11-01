//! A pod is labeled with the client whose activation creates it: the id the
//! activation resolved at entry, or, for a rebuild no entry resolved, the
//! sandbox's record as the row holds it when the pod is built. Tracking the new pod then sets the record to that label.

use std::sync::{Arc, Mutex};

use pplx_space_common_core::kubernetes::SANDBOX_CLIENT_ID_LABEL;
use pplx_space_common_core::sql;
use pplx_space_common_core::types::{
    ClientId, SandboxActivationAttemptId, SandboxId, SandboxTransitionTargetState,
};
use pplx_space_test_env::TestResult;
use uuid::Uuid;

use super::*;
use crate::controllers::substrate::kubernetes::pod_ops::{
    ActivationAttemptAssignment, PodMaterializationOwnership, StartedRecoveryOwnership,
};
use crate::controllers::substrate::{KeyResolution, SubstrateError};

/// A started sandbox recorded as activated by `background-client`, whose
/// generation-2 execution lost its pod; a ready portable snapshot lets
/// recovery rebuild it.
async fn sandbox_with_a_lost_background_execution(pool: &sqlx::PgPool) -> TestResult<SandboxId> {
    let sandbox_id = SandboxId::new();
    let artifact_id = Uuid::now_v7();
    insert_test_sandbox_row(pool, sandbox_id, "started").await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET current_local_artifact_id = $2,
                current_local_artifact_kind = 'full',
                current_local_artifact_origin = 'suspend',
                activating_client_id = 'background-client'
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
             (sandbox_id, generation, pod_name, pod_namespace, pod_uid, state, active)
         VALUES
             ($1, 2, 'space-sbx-missing-g2', 'space', 'missing-pod-uid', 'recovering', TRUE)"
    ))
    .bind(sandbox_id)
    .execute(pool)
    .await?;
    Ok(sandbox_id)
}

#[rstest::rstest]
#[case::a_rebuild_reads_the_record(KeyResolution::Unresolved, Some("background-client"))]
#[case::an_activation_names_its_client(
    KeyResolution::Resolved(Some(ClientId::parse("scheduled-client").unwrap())),
    Some("scheduled-client")
)]
#[case::an_activation_with_nothing_to_inherit(KeyResolution::Resolved(None), None)]
#[tokio::test(flavor = "current_thread")]
async fn a_rebuilt_pod_is_labeled_with_its_activating_client(
    #[case] key: KeyResolution,
    #[case] expected: Option<&str>,
) -> TestResult {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let pool = db.pool.clone();
    let sandbox_id = sandbox_with_a_lost_background_execution(&pool).await?;
    let mut sandbox = crate::controllers::sandbox::fetch_sandbox(&pool, sandbox_id)
        .await?
        .expect("inserted sandbox must be visible");
    // The caller's copy is stale: the rebuild reads the record from the row.
    sandbox.activating_client_id = Some(ClientId::parse("stale-client")?);
    let captured_pod_creates = Arc::new(Mutex::new(Vec::new()));
    let substrate = substrate_with_store_and_client(
        pool.clone(),
        mock_kubernetes_client(Arc::clone(&captured_pod_creates)),
    );
    let assignment = ActivationAttemptAssignment::new(
        SandboxActivationAttemptId::new(),
        SandboxTransitionTargetState::Started,
        key,
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

    let created_pods = captured_pod_creates
        .lock()
        .expect("capture mutex poisoned")
        .clone();
    let [pod] = created_pods.as_slice() else {
        panic!("expected one pod create, got {}", created_pods.len());
    };
    assert_eq!(
        pod.pointer("/metadata/labels")
            .and_then(|labels| labels.get(SANDBOX_CLIENT_ID_LABEL))
            .and_then(serde_json::Value::as_str),
        expected,
        "the pod's label"
    );
    let record: Option<String> = sqlx::query_scalar(sql!(
        "SELECT activating_client_id FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        record.as_deref(),
        expected,
        "the record follows the new pod"
    );
    Ok(())
}

/// Refuses every live-cap check and counts them.
#[derive(Default)]
struct AtLiveCap(Mutex<usize>);

impl crate::controllers::substrate::ActivationAdmission for AtLiveCap {
    fn admit(
        &self,
        _class: crate::controllers::substrate::ActivationAdmissionClass,
    ) -> crate::controllers::substrate::ActivationAdmissionFuture<'_> {
        Box::pin(async { Ok(()) })
    }

    fn reserve_live(
        &self,
        _client: ClientId,
        _sandbox: SandboxId,
    ) -> crate::controllers::substrate::LiveCapFuture<'_> {
        *self.0.lock().expect("count mutex poisoned") += 1;
        Box::pin(async { Err(at_cap()) })
    }

    fn precheck_live(
        &self,
        _client: ClientId,
        _sandbox: SandboxId,
    ) -> crate::controllers::substrate::ActivationAdmissionFuture<'_> {
        *self.0.lock().expect("count mutex poisoned") += 1;
        Box::pin(async { Err(at_cap()) })
    }
}

fn at_cap() -> crate::controllers::substrate::ActivationAdmissionRejection {
    crate::controllers::substrate::ActivationAdmissionRejection::new(
        "client live sandbox cap reached".to_string(),
        Some(std::time::Duration::from_secs(5)),
        false,
    )
}

/// `ensure_pod_exists` is the live cap's `Check` point (RFC Decision 6): a
/// charge whose cap is decided where a pod gets created is checked on the
/// branch about to create one, and a refusal creates no pod; a charge
/// reserved upstream, or one that isn't capped, isn't checked again.
/// Kubernetes only: Direct has no check point (the "Per-client live cap"
/// row in `direct.rs`'s gap table).
#[rstest::rstest]
#[case::checked_where_the_pod_is_created(crate::controllers::substrate::CapPolicy::Check, true)]
#[case::a_call_that_claimed_no_lease(
    crate::controllers::substrate::CapPolicy::ReserveAtClaim,
    true
)]
#[case::reserved_upstream(crate::controllers::substrate::CapPolicy::Reserved, false)]
#[case::not_capped(crate::controllers::substrate::CapPolicy::Skip, false)]
#[tokio::test(flavor = "current_thread")]
async fn a_pod_create_is_checked_against_the_live_cap_by_its_cap_policy(
    #[case] cap: crate::controllers::substrate::CapPolicy,
    #[case] checked: bool,
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
    let admission = Arc::new(AtLiveCap::default());
    let charge = crate::controllers::substrate::RecoveryCharge::exempt(
        Some(&admission),
        cap,
        crate::controllers::substrate::ActivationAttribution::inherit_unresolved(),
    );

    let result = substrate
        .ensure_pod_exists(
            &sandbox,
            None,
            None,
            None,
            PodMaterializationOwnership::new(
                Some(StartedRecoveryOwnership::ExistingExecution { generation: 2 }),
                None,
                Some(ActivationAttemptAssignment::new(
                    SandboxActivationAttemptId::new(),
                    SandboxTransitionTargetState::Started,
                    KeyResolution::Unresolved,
                )),
            )
            .with_charge(&charge),
        )
        .await;

    let creates = captured_pod_creates
        .lock()
        .expect("capture mutex poisoned")
        .len();
    let checks = *admission.0.lock().expect("count mutex poisoned");
    if checked {
        assert!(
            matches!(
                result,
                Err(SubstrateError::AdmissionRejected {
                    rate_limited: false,
                    ..
                })
            ),
            "a refusal is a refused charge: {result:?}"
        );
        assert_eq!(
            (checks, creates),
            (1, 0),
            "checked once, and no pod created"
        );
    } else {
        result?;
        assert_eq!(
            (checks, creates),
            (0, 1),
            "not checked, and the pod created"
        );
    }
    Ok(())
}

/// An observer that finds the tracked pod unchanged but the record lagging its
/// label repairs the record alone. The row's state stays as it was, so a
/// failed recovery whose pod is still up isn't made routable by the repair.
#[tokio::test(flavor = "current_thread")]
async fn a_record_only_repair_leaves_the_execution_state_alone() -> TestResult {
    use k8s_openapi::api::core::v1::{Pod, PodIP, PodStatus};
    use std::collections::BTreeMap;

    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let pool = db.pool.clone();
    let sandbox_id = SandboxId::new();
    let pod_name = "space-sbx-failed-recovery-g1";
    insert_test_sandbox_row(&pool, sandbox_id, "started").await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET activating_client_id = 'stale-client' WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
             (sandbox_id, generation, pod_name, pod_namespace, pod_uid, pod_ip, state, active)
         VALUES ($1, 1, $2, 'space', 'tracked-pod-uid', '127.0.0.1'::INET,
                 'recovery_failed', TRUE)"
    ))
    .bind(sandbox_id)
    .bind(pod_name)
    .execute(&pool)
    .await?;
    let pod = Pod {
        metadata: kube::api::ObjectMeta {
            name: Some(pod_name.to_string()),
            namespace: Some("space".to_string()),
            uid: Some("tracked-pod-uid".to_string()),
            labels: Some(BTreeMap::from([(
                SANDBOX_CLIENT_ID_LABEL.to_string(),
                "labeled-client".to_string(),
            )])),
            ..Default::default()
        },
        status: Some(PodStatus {
            phase: Some("Running".to_string()),
            pod_ip: Some("127.0.0.1".to_string()),
            pod_ips: Some(vec![PodIP {
                ip: "127.0.0.1".to_string(),
            }]),
            ..Default::default()
        }),
        ..Default::default()
    };
    let substrate = substrate_with_store_client_proxy_port_and_pod(
        pool.clone(),
        mock_kubernetes_client(Arc::new(Mutex::new(Vec::new()))),
        DEFAULT_SANDBOX_PROXY_PORT,
        Some(pod),
    );

    let _ = substrate.resolve_pod_ip(sandbox_id).await;

    let (state, record): (String, Option<String>) = sqlx::query_as(sql!(
        "SELECT e.state, s.activating_client_id
           FROM sandbox_k8s_executions e
           JOIN sandboxes s ON s.id = e.sandbox_id
          WHERE e.sandbox_id = $1 AND e.active"
    ))
    .bind(sandbox_id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        record.as_deref(),
        Some("labeled-client"),
        "the record is repaired"
    );
    assert_eq!(state, "recovery_failed", "the row's state is left alone");
    Ok(())
}

/// With attribution off, a rebuilt pod carries no label even when its
/// activation names a client, and tracking it leaves the record as it was.
#[tokio::test(flavor = "current_thread")]
async fn attribution_off_leaves_the_pod_unlabeled_and_the_record_alone() -> TestResult {
    use crate::controllers::substrate::ActivationClientAttribution;

    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let pool = db.pool.clone();
    let sandbox_id = sandbox_with_a_lost_background_execution(&pool).await?;
    let sandbox = crate::controllers::sandbox::fetch_sandbox(&pool, sandbox_id)
        .await?
        .expect("inserted sandbox must be visible");
    let captured_pod_creates = Arc::new(Mutex::new(Vec::new()));
    let mut substrate = substrate_with_store_and_client(
        pool.clone(),
        mock_kubernetes_client(Arc::clone(&captured_pod_creates)),
    );
    substrate.k8s.activation_client_attribution = ActivationClientAttribution::Disabled;
    substrate.store = super::super::store::K8sStore::new(
        pool.clone(),
        "space".to_string(),
        ActivationClientAttribution::Disabled,
    );
    let assignment = ActivationAttemptAssignment::new(
        SandboxActivationAttemptId::new(),
        SandboxTransitionTargetState::Started,
        KeyResolution::Resolved(Some(ClientId::parse("scheduled-client")?)),
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

    let created_pods = captured_pod_creates
        .lock()
        .expect("capture mutex poisoned")
        .clone();
    let [pod] = created_pods.as_slice() else {
        panic!("expected one pod create, got {}", created_pods.len());
    };
    assert_eq!(
        pod.pointer("/metadata/labels")
            .and_then(|labels| labels.get(SANDBOX_CLIENT_ID_LABEL)),
        None,
        "the pod is unlabeled"
    );
    let record: Option<String> = sqlx::query_scalar(sql!(
        "SELECT activating_client_id FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        record.as_deref(),
        Some("background-client"),
        "the record is left alone"
    );
    Ok(())
}
