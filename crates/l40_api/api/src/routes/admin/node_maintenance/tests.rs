use pplx_space_accounts::NodeId;

use super::*;
use pplx_space_db_test_fixtures::try_pg_test_db;

use pplx_space_test_env::TestResult;

mod substrate_scope;

/// The node registry routes are open only where the running substrate keeps
/// its nodes in the admin registry. Deleting a node that does not exist
/// reaches the registry lookup on Direct and is refused before it on
/// Kubernetes.
#[tokio::test(flavor = "current_thread")]
async fn node_registry_delete_follows_the_running_substrate() -> TestResult {
    // `AppState::from_pool` chooses its substrate from the process
    // environment. Keep it Direct whatever the host sets, so construction
    // never reaches a real cluster before the Kubernetes fixture is swapped in.
    if !pplx_space_test_env::enter_test_process(&[(
        "SPACE_K8S_CONTROL_PLANE",
        Some(std::ffi::OsStr::new("false")),
    )]) {
        return Ok(());
    }
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let actor = sqlx::query_as::<_, crate::models::User>(sql!(
        "SELECT u.*
         FROM users u
         JOIN clusters c ON c.managing_org_id = u.org_id
         WHERE c.singleton
           AND u.role = 'organization_admin'
         ORDER BY u.id
         LIMIT 1"
    ))
    .fetch_one(&db.pool)
    .await?;
    let direct = AppState::from_pool(db.pool.clone()).await?;
    assert_eq!(direct.sandbox_controller.managed_by(), "space");

    #[cfg_attr(not(feature = "kubernetes"), allow(unused_mut))]
    let mut cases = vec![(direct, StatusCode::NOT_FOUND, "Node not found")];
    #[cfg(feature = "kubernetes")]
    {
        let mut kubernetes = AppState::from_pool(db.pool.clone()).await?;
        kubernetes.sandbox_controller = crate::controllers::sandbox::SandboxController::new(
            crate::controllers::substrate::Substrate::from_kubernetes(
                crate::controllers::substrate::kubernetes::tests::substrate_with_store(
                    db.pool.clone(),
                ),
            ),
        );
        cases.push((
            kubernetes,
            StatusCode::NOT_IMPLEMENTED,
            "direct node registry is only available for the direct/self-managed substrate",
        ));
    }

    for (state, expected_status, expected_error) in cases {
        let substrate = state.sandbox_controller.managed_by();
        let response = delete_node(
            State(state.clone()),
            Extension(AuthUser(actor.clone())),
            Path(NodeId::new()),
        )
        .await
        .into_response();
        assert_eq!(response.status(), expected_status, "{substrate} substrate");
        let body: serde_json::Value =
            serde_json::from_slice(&axum::body::to_bytes(response.into_body(), 100_000).await?)?;
        assert_eq!(body["error"], expected_error, "{substrate} substrate");
    }
    Ok(())
}
