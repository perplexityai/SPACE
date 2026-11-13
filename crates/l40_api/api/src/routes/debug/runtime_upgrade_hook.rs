//! A test-only trigger for [`SubstrateAdapter::upgrade_stopped_runtime`]:
//! `POST /debug/sandbox/{sandbox_id}/upgrade-stopped-runtime` on the admin
//! surface, for a smoke run that has to drive the operation end to end.
//!
//! Nothing in SPACE calls the operation yet; a future upgrade campaign will.
//! The route exists only while [`RuntimeUpgradeTestHook::ENV`] is on, which
//! it is by default nowhere: with the setting off the route isn't mounted
//! and answers 404. It takes a managing-org admin, as the rest of the debug
//! surface does.
//!
//! [`SubstrateAdapter::upgrade_stopped_runtime`]:
//! crate::controllers::substrate::SubstrateAdapter::upgrade_stopped_runtime

use axum::{
    Extension, Json,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};

use crate::controllers::ControllerError;
use crate::controllers::substrate::runtime_upgrade::StoppedRuntimeUpgrade;
use crate::middleware::auth::AuthUser;
use crate::routes::AppState;

#[cfg(test)]
mod tests;

/// Whether the admin surface mounts the stopped-runtime-upgrade trigger.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum RuntimeUpgradeTestHook {
    #[default]
    Disabled,
    Enabled,
}

impl RuntimeUpgradeTestHook {
    /// Off unless truthy. A test hook: binaries built without the
    /// `test-hooks` feature, as release images are, never read it, so the
    /// route can't be mounted in them.
    pub const ENV: &str = "SPACE_ADMIN_RUNTIME_UPGRADE_TEST_HOOK_ENABLED";

    pub fn from_enabled(enabled: bool) -> Self {
        if enabled {
            Self::Enabled
        } else {
            Self::Disabled
        }
    }

    pub fn from_env() -> Self {
        Self::from_enabled(pplx_space_common_core::env::env_flag(
            pplx_space_common_core::env::test_hook_var(Self::ENV).as_deref(),
            false,
        ))
    }

    pub fn is_enabled(self) -> bool {
        self == Self::Enabled
    }
}

/// Upgrade the stopped sandbox's runtime without starting it, waiting at
/// most the node-RPC blocking budget for the boot. A failure is the
/// operation's own, mapped as any substrate error is. A retry that takes an
/// unfinished attempt over repeats its operation; one after a rollback or a
/// refusal retired the execution is a new decision under a new operation.
pub async fn upgrade_stopped_runtime(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Path(spec): Path<String>,
) -> Response {
    if let Err(response) = super::sandbox::require_managing_org_admin(&state, &actor).await {
        return *response;
    }
    // The same identifier forms as the rest of the admin surface: a bare
    // UUID, `sbx_<uuid>` or the sandbox's pod name.
    let Ok(sandbox_id) = crate::routes::admin::sandbox_id::parse_admin_sandbox_id(&spec) else {
        return super::sandbox::api_error(StatusCode::BAD_REQUEST, "invalid_sandbox_id");
    };
    match state
        .sandbox_controller
        .upgrade_stopped_runtime(
            sandbox_id,
            crate::controllers::substrate::NODE_RPC_BLOCKING_BUDGET,
        )
        .await
    {
        Ok(StoppedRuntimeUpgrade::AsRecorded) => {
            Json(serde_json::json!({ "outcome": "as_recorded" })).into_response()
        }
        Ok(StoppedRuntimeUpgrade::Upgraded {
            target,
            operation_id,
        }) => Json(serde_json::json!({
            "outcome": "upgraded",
            "target_environment": target.to_string(),
            "operation_id": operation_id,
        }))
        .into_response(),
        Err(error) => ControllerError::from(error).into_response(),
    }
}
