//! The control path's dormant-wake fallback is taken only for Kubernetes
//! (`'k8s'`) rows.

use std::sync::{Arc, Mutex};

use pplx_space_common_core::types::SandboxId;
use pplx_space_test_env::TestResult;

use super::*;
use crate::controllers::substrate::Substrate;

/// A dormant row has no runtime to reach, so the first runtime_client fails
/// without charging. Only a `'k8s'` row then takes the dormant-wake fallback,
/// whose activation charge the zero-capacity admission refuses (429) before
/// any pod is created. An otherwise identical `'space'` row returns the
/// runtime error instead.
#[tokio::test(flavor = "current_thread")]
async fn control_runtime_client_wakes_only_kubernetes_rows_after_an_unreachable_runtime()
-> TestResult {
    for (managed_by, takes_wake_fallback) in [("k8s", true), ("space", false)] {
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let pool = db.pool.clone();
        let sandbox_id = SandboxId::new();
        insert_test_sandbox_row(&pool, sandbox_id, "suspended").await?;
        sqlx::query(pplx_space_common_core::sql!(
            "UPDATE sandboxes SET managed_by = $2 WHERE id = $1"
        ))
        .bind(sandbox_id)
        .bind(managed_by)
        .execute(&pool)
        .await?;
        let sandbox = crate::controllers::sandbox::fetch_sandbox(&pool, sandbox_id)
            .await?
            .expect("sandbox row");

        let captured_pod_creates = Arc::new(Mutex::new(Vec::new()));
        let substrate = Substrate::from_kubernetes(substrate_with_store_and_client(
            pool.clone(),
            mock_kubernetes_client(Arc::clone(&captured_pod_creates)),
        ));
        let admission = crate::cluster_admission::ClusterSandboxAdmission::with_config(
            // Direct is only a placeholder capacity source: it reports zero
            // eligible nodes, as a synced, empty Kubernetes node cache would.
            Substrate::from_direct(crate::controllers::substrate::direct::DirectSubstrate::new(
                pool.clone(),
            )),
            crate::cluster_admission::ClusterSandboxAdmissionConfig {
                enabled: true,
                refill_rps: 6.0,
                burst: 60.0,
                redis_url: None,
            },
            pplx_space_feature_flags::FeatureFlagClient::disabled(),
        );
        let controller = crate::controllers::sandbox::SandboxController::with_cluster_admission(
            substrate, admission,
        );

        let status = match controller
            .control_runtime_client(
                &pool,
                &sandbox,
                crate::controllers::sandbox::lifecycle::SandboxAction::Update,
            )
            .await
        {
            Ok(_) => panic!("{managed_by}: a suspended sandbox has no runtime to reach"),
            Err(error) => error.status(),
        };
        assert_eq!(
            status == axum::http::StatusCode::TOO_MANY_REQUESTS,
            takes_wake_fallback,
            "{managed_by}: {status}",
        );
        assert!(
            captured_pod_creates
                .lock()
                .expect("capture mutex poisoned")
                .is_empty(),
            "{managed_by}: no pod is created",
        );
    }
    Ok(())
}
