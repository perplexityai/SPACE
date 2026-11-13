use axum::{
    Router,
    routing::{get, post},
};

use super::AppState;

mod response;
pub mod runtime_upgrade_hook;
pub mod sandbox;
mod sources;

pub use runtime_upgrade_hook::RuntimeUpgradeTestHook;

pub(in crate::routes) fn admin_v1_debug_router(hook: RuntimeUpgradeTestHook) -> Router<AppState> {
    let router = Router::new().route(
        "/debug/sandbox/{sandbox_id}",
        get(sandbox::get_sandbox_debug),
    );
    if !hook.is_enabled() {
        return router;
    }
    router.route(
        "/debug/sandbox/{sandbox_id}/upgrade-stopped-runtime",
        post(runtime_upgrade_hook::upgrade_stopped_runtime),
    )
}
