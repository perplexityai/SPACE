//! The control path's dormant-wake fallback is taken only for Kubernetes
//! (`'k8s'`) rows, and the builder lookup reads only Kubernetes nodes.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use k8s_openapi::api::core::v1::Pod;
use pplx_space_common_core::sql;
use pplx_space_common_core::types::{NodeId, SandboxId};
use pplx_space_test_env::TestResult;

use super::*;
use crate::controllers::substrate::{Substrate, SubstrateAdapter};

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

/// A ready builder node-api pod on `node_name` at `127.0.0.1:{port}`.
fn builder_pod_on_node(name: &str, node_name: &str, port: u16) -> Pod {
    let mut pod = node_api_pod_on_node(name, node_name);
    pod.metadata.labels = Some(BTreeMap::from([(
        "app.kubernetes.io/name".to_string(),
        "space-builder".to_string(),
    )]));
    pod.metadata.annotations = Some(BTreeMap::from([(
        "space.pplx.net/node-api-port".to_string(),
        port.to_string(),
    )]));
    if let Some(status) = pod.status.as_mut() {
        status.pod_ip = Some("127.0.0.1".to_string());
    }
    pod
}

/// Two builder nodes, each with a `node_k8s` row and a ready builder pod; the
/// second is labelled `'space'`, so only the `managed_by` predicate can refuse
/// it. The Kubernetes builder's pod points at a listener, so a dial is
/// observable.
#[tokio::test(flavor = "current_thread")]
async fn builder_lookup_reads_only_kubernetes_nodes() -> TestResult {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    let kubernetes = NodeId::new();
    let direct = NodeId::new();
    for (node_id, managed_by, node_name) in [
        (kubernetes, "k8s", "builder-kubernetes"),
        (direct, "space", "builder-direct"),
    ] {
        sqlx::query(sql!(
            "INSERT INTO nodes (id, address, role, managed_by, state)
             VALUES ($1, '127.0.0.1', 'builder', $2, 'ready')"
        ))
        .bind(node_id)
        .bind(managed_by)
        .execute(&db.pool)
        .await?;
        sqlx::query(sql!(
            "INSERT INTO node_k8s (node_id, k8s_node_name) VALUES ($1, $2)"
        ))
        .bind(node_id)
        .bind(node_name)
        .execute(&db.pool)
        .await?;
    }
    let substrate = Arc::new(substrate_with_inventory_caches_and_pool(
        vec![
            builder_pod_on_node("builder-api-kubernetes", "builder-kubernetes", port),
            builder_pod_on_node("builder-api-direct", "builder-direct", port),
        ],
        Vec::new(),
        db.pool.clone(),
    ));

    let refused = match substrate.builder_client_for_node(direct).await {
        Ok(_) => panic!("a Direct builder must not be dialed through Kubernetes"),
        Err(error) => error,
    };
    assert_eq!(
        refused.status(),
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    );
    assert!(
        refused.to_string().contains("is not registered"),
        "{refused}"
    );

    // Aborted when dropped, including when the bound below fails the test.
    let dialing = Arc::clone(&substrate);
    let mut dial = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move {
        dialing.builder_client_for_node(kubernetes).await
    }));
    // Past the lookup the client either dials or fails on transport
    // configuration first; either way it was not refused as unregistered.
    let outcome: TestResult<Option<_>> = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::select! {
            accepted = listener.accept() => {
                accepted?;
                Ok(None)
            }
            dialed = &mut dial => Ok(Some(dialed?)),
        }
    })
    .await
    .expect("the Kubernetes builder is dialed or fails past the lookup");
    drop(dial);
    if let Some(Err(error)) = outcome? {
        assert!(!error.to_string().contains("is not registered"), "{error}");
    }
    Ok(())
}
