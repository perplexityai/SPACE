use axum::{Router, routing::get};

use super::AppState;

mod response;
pub mod sandbox;
mod sources;

pub(in crate::routes) fn admin_v1_debug_router() -> Router<AppState> {
    Router::new().route(
        "/debug/sandbox/{sandbox_id}",
        get(sandbox::get_sandbox_debug),
    )
}
