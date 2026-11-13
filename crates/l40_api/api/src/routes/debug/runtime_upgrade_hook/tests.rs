use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use pplx_space_common_core::types::{OrgId, SandboxId, UserId};
use time::OffsetDateTime;
use tower::ServiceExt;

use super::RuntimeUpgradeTestHook;
use crate::middleware::auth::AuthUser;
use crate::models::User;

fn actor(org_id: OrgId) -> User {
    User {
        id: UserId::new(),
        email: "runtime-upgrade-hook@example.test".to_string(),
        password_hash: String::new(),
        display_name: "runtime upgrade hook".to_string(),
        org_id,
        role: crate::user_role::UserRole::OrganizationAdmin,
        owner_source: pplx_space_common_core::types::SandboxOwnerSource::Local,
        created_at: OffsetDateTime::now_utc(),
        updated_at: OffsetDateTime::now_utc(),
    }
}

async fn post(
    hook: RuntimeUpgradeTestHook,
    actor: User,
) -> Result<StatusCode, Box<dyn std::error::Error + Send + Sync>> {
    post_for(hook, actor, &SandboxId::new().to_string()).await
}

/// [`post`] naming the sandbox as `spec`.
async fn post_for(
    hook: RuntimeUpgradeTestHook,
    actor: User,
    spec: &str,
) -> Result<StatusCode, Box<dyn std::error::Error + Send + Sync>> {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let state = crate::routes::AppState::from_pool(db.pool.clone())
        .await
        .expect("app state from the test pool");
    let router = super::super::admin_v1_debug_router(hook).with_state(state);
    let mut request = Request::builder()
        .method(Method::POST)
        .uri(format!("/debug/sandbox/{spec}/upgrade-stopped-runtime"))
        .body(Body::empty())?;
    request.extensions_mut().insert(AuthUser(actor));
    let status = router.oneshot(request).await?.status();
    db.pool.close().await;
    Ok(status)
}

/// The trigger exists only while its setting is on, which it is by default
/// nowhere; on, it takes a managing-org admin like the rest of the debug
/// surface, and reaches the substrate. This substrate is Direct, which
/// refuses the operation under its gap row (501).
#[tokio::test]
async fn the_trigger_is_mounted_only_when_enabled_and_only_for_operators()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // `AppState::from_pool` reads the substrate from process env: pin it to
    // Direct, whatever the host sets.
    if !pplx_space_test_env::enter_test_process(&[(
        "SPACE_K8S_CONTROL_PLANE",
        Some(std::ffi::OsStr::new("false")),
    )]) {
        return Ok(());
    }
    let operator = actor(OrgId::from_uuid(pplx_space_db_test_fixtures::SEED_ORG_ID));
    assert_eq!(
        RuntimeUpgradeTestHook::default(),
        RuntimeUpgradeTestHook::Disabled
    );
    assert_eq!(
        post(RuntimeUpgradeTestHook::Disabled, operator.clone()).await?,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        post(RuntimeUpgradeTestHook::Enabled, actor(OrgId::new())).await?,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        post(RuntimeUpgradeTestHook::Enabled, operator.clone()).await?,
        StatusCode::NOT_IMPLEMENTED
    );
    // The admin surface's other identifier forms reach the substrate too, and
    // anything else is refused before it.
    assert_eq!(
        post_for(
            RuntimeUpgradeTestHook::Enabled,
            operator.clone(),
            &format!("sbx_{}", SandboxId::new().as_uuid().simple())
        )
        .await?,
        StatusCode::NOT_IMPLEMENTED
    );
    assert_eq!(
        post_for(RuntimeUpgradeTestHook::Enabled, operator, "not-a-sandbox").await?,
        StatusCode::BAD_REQUEST
    );
    Ok(())
}

#[test]
fn only_a_truthy_setting_enables_the_trigger() {
    assert!(RuntimeUpgradeTestHook::from_enabled(true).is_enabled());
    assert!(!RuntimeUpgradeTestHook::from_enabled(false).is_enabled());
}
