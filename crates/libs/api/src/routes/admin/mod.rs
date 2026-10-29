//! SPACE operator surface.
//!
//! Per the Public API and Data Plane LLD this hosts cluster-admin actions
//! (drain, terminate, cordon, …) on shared SPACE objects. Dedicated admin
//! hosts mount this surface at `/v1`; shared hosts mount it at `/admin/api/v1`.
//!
//! ### Why one catch-all path for `{node_id}` / `{node_id}:action`
//!
//! The LLD specifies AIP-style colon-action verbs (e.g. `POST
//! /v1/nodes/UUID:drain`). matchit 0.8 — the router backing axum 0.8 —
//! does not support dynamic suffixes after a named parameter (you cannot
//! write `/{node_id}:drain`). We therefore route the last segment as a
//! single `{spec}` parameter that carries either a bare UUID (for `GET`) or
//! a `UUID:action` pair (for `POST`), and parse the two cases inside the
//! handlers. This preserves the external URL shape exactly while staying
//! within the router's grammar.

use std::time::Instant;

use axum::{
    Router,
    extract::{Request, State},
    http::{Method, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, put},
};

use super::{AppState, authz, debug};
use crate::{
    auth,
    external_console::ExternalConsoleAuth,
    middleware::auth::{AuthOrg, AuthUser, bearer_token},
    routes::public::error::PublicApiError,
};

mod api_key_store;
mod api_keys;
pub mod audit;
mod docs;
mod identity;
mod node_group_store;
pub mod node_groups;
mod node_maintenance;
mod node_store;
pub mod nodes;
mod operations;
mod runtime_environment_store;
mod runtime_environments;
mod sandboxes;
// `pub(in crate::routes)` rather than private: `routes::debug` is mounted
// inside this router (`debug::admin_v1_debug_router`, merged by `v1_router`
// below) and takes `support::contention_disposition` from here, so the whole
// `/admin/api/v1` tree agrees on the *status* a lost lock race gets. The body
// still differs by module: this layer, `templates`, and `sandboxes`'
// `create_command` render a `PublicErrorEnvelope`; `sandboxes`' action
// handlers render `{"error", "operation_id", "retryable"}`; the rest a bare
// `{"error": …}` with no retry field. `Retry-After` comes only from the two
// paths that answer through `PublicApiError` — this layer, and
// `create_command`, which has two — its sandbox lookup through
// `From<sqlx::Error>` and its session write through
// `From<PublicSessionStoreError>`, both landing on `lock_contention()`.
pub(in crate::routes) mod support;
mod template_store;
mod template_visibility;
mod templates;
#[cfg(test)]
mod test_fixtures;
mod warm_pools;

/// Build the shared-host admin API surface.
pub fn shared_host_router(state: AppState) -> Router<AppState> {
    Router::new()
        .route("/admin/api/llms.txt", get(docs::llms_txt))
        .route("/admin/api/openapi.json", get(docs::openapi_json))
        .nest("/admin/api/v1", v1_router(state))
}

/// Build the dedicated-host admin API surface.
pub fn dedicated_host_router(state: AppState) -> Router<AppState> {
    Router::new()
        .route("/llms.txt", get(docs::llms_txt))
        .route("/openapi.json", get(docs::openapi_json))
        .nest("/v1", v1_router(state))
}

/// Build the admin v1 route tree. The returned routes are relative to the
/// version mount so admin submodules cannot accidentally claim non-admin paths.
pub fn v1_router(state: AppState) -> Router<AppState> {
    Router::new()
        .route(
            "/nodes",
            get(nodes::list_admin_nodes).post(node_maintenance::create_node),
        )
        .route(
            "/node-groups",
            get(node_groups::list_node_groups).post(node_groups::create_node_group),
        )
        .route(
            "/node-groups/{spec}",
            get(node_groups::get_node_group)
                .post(node_groups::dispatch_node_group_action)
                .delete(node_groups::delete_node_group),
        )
        .route(
            "/node-groups/{spec}/template-policy",
            put(node_groups::set_template_policy),
        )
        .route("/node-groups/{spec}/compat", put(node_groups::set_compat))
        .route(
            "/nodes:refresh-status",
            axum::routing::post(node_maintenance::refresh_node_status),
        )
        .route(
            "/nodes:reconcile",
            axum::routing::post(node_maintenance::reconcile_nodes),
        )
        .route(
            "/nodes/templates",
            get(node_maintenance::list_node_templates),
        )
        .route(
            "/nodes/{spec}",
            get(nodes::get_admin_node)
                .post(nodes::dispatch_node_action)
                .patch(node_maintenance::update_node)
                .delete(node_maintenance::delete_node),
        )
        .route(
            "/nodes/{node_id}/sandboxes",
            get(nodes::list_node_sandboxes),
        )
        .route(
            "/nodes/{node_id}/health",
            get(node_maintenance::node_health),
        )
        .route(
            "/nodes/{node_id}/network-policy:reconcile",
            axum::routing::post(node_maintenance::reconcile_node_network_policy),
        )
        .route(
            "/nodes/{node_id}/template-pulls",
            axum::routing::post(node_maintenance::pull_node_template),
        )
        .route(
            "/nodes/{node_id}/templates/{template_name}",
            axum::routing::delete(node_maintenance::delete_node_template),
        )
        .route("/operations", get(operations::list_admin_operations))
        .route(
            "/operations/{operation_id}",
            get(operations::get_admin_operation),
        )
        .route("/audit-events", get(audit::list_audit_events))
        .route(
            "/runtime-environments",
            get(runtime_environments::get_runtime_environments),
        )
        .route(
            "/runtime-environments/default",
            put(runtime_environments::set_default_runtime_environment),
        )
        .route(
            "/runtime-environments/nodes",
            get(runtime_environments::list_runtime_environment_nodes),
        )
        .route(
            "/runtime-environments/population",
            get(runtime_environments::get_runtime_environment_population),
        )
        .route(
            "/runtime-environments/{environment_ref}/state",
            put(runtime_environments::set_runtime_environment_state),
        )
        .route(
            "/warm-pools",
            get(warm_pools::list_warm_pools).put(warm_pools::upsert_warm_pool),
        )
        .route(
            "/warm-pools/{pool_id}",
            axum::routing::delete(warm_pools::disable_warm_pool),
        )
        .route(
            "/warm-pool-policies",
            get(warm_pools::list_warm_pool_policies).put(warm_pools::upsert_warm_pool_policy),
        )
        .route(
            "/warm-pool-policy-capabilities",
            get(warm_pools::get_warm_pool_policy_capabilities),
        )
        .route(
            "/templates",
            get(templates::list_templates).post(templates::create_template),
        )
        .route(
            "/templates/{template_name}",
            get(templates::get_template)
                .patch(templates::update_template)
                .put(template_visibility::set_template_visibility)
                .delete(templates::delete_template),
        )
        .route(
            "/system-templates/{template}/builds",
            axum::routing::post(templates::start_system_template_build),
        )
        .route(
            "/templates/{template_name}/builds",
            axum::routing::post(templates::start_template_build),
        )
        .route(
            "/templates/{template_name}/builds/{job_id}",
            get(templates::get_template_build_status),
        )
        .route(
            "/templates/{template_name}/base-snapshot-policy",
            get(templates::get_base_snapshot_policy).patch(templates::update_base_snapshot_policy),
        )
        .route(
            "/templates/{template_name}/base-snapshot-policy:reconcile",
            axum::routing::post(templates::reconcile_base_snapshot_policy),
        )
        .route(
            "/templates/{template_name}/pool",
            get(templates::template_pool_status)
                .post(templates::upsert_template_pool)
                .delete(templates::delete_template_pool),
        )
        .route("/template-releases", get(templates::list_template_releases))
        .route(
            "/template-releases/{release_id}",
            axum::routing::delete(templates::delete_template_release),
        )
        .route(
            "/template-releases/{release_id}/tag",
            put(templates::tag_template_release),
        )
        .route(
            "/template-releases/{release_id}/tags/{tag}",
            axum::routing::delete(templates::delete_template_release_tag),
        )
        .route(
            "/cluster",
            get(identity::get_cluster).patch(identity::update_cluster),
        )
        .route("/orgs", get(identity::list_orgs).post(identity::create_org))
        .route(
            "/orgs/{org_id}",
            get(identity::get_org)
                .patch(identity::update_org)
                .delete(identity::delete_org),
        )
        .route(
            "/orgs/{org_id}/users",
            get(identity::list_users_for_org).post(identity::create_user_for_org),
        )
        .route(
            "/orgs/{org_id}/master-key",
            get(identity::get_org_master_key),
        )
        .route(
            "/orgs/{org_id}/api-keys",
            get(api_keys::list_api_keys).post(api_keys::create_api_key),
        )
        .route(
            "/orgs/{org_id}/api-keys/{api_key_id}",
            axum::routing::delete(api_keys::delete_api_key),
        )
        .route(
            "/users/{user_id}",
            get(identity::get_user)
                .patch(identity::update_user)
                .delete(identity::delete_user),
        )
        .route(
            "/users/{user_id}/role",
            axum::routing::put(identity::update_user_role),
        )
        .route(
            "/sandboxes",
            get(sandboxes::list_sandboxes).post(sandboxes::create_sandbox),
        )
        .route(
            "/sandboxes/{spec}",
            get(sandboxes::get_sandbox)
                .patch(sandboxes::update_sandbox)
                .post(sandboxes::dispatch_sandbox_action),
        )
        .route(
            "/sandboxes/{spec}/commands",
            axum::routing::post(sandboxes::create_command),
        )
        .merge(debug::admin_v1_debug_router())
        .layer(middleware::from_fn_with_state(state, require_admin_auth))
}

/// The retryable response for a lost lock race in [`require_admin_auth`], or
/// `None` when the error is a real failure.
///
/// This layer, not the handler, is where a contended `/admin` request whose
/// contention is on *this* layer's tables gets decided. `require_admin_auth`
/// wraps the whole `v1_router` — including the merged
/// `debug::admin_v1_debug_router` — and runs three statements of its own
/// before any handler: `get_session_user`'s `users`-`sessions` join and its
/// `organizations` read, then — for a caller who is an org admin, since
/// `is_managing_org_admin` short-circuits on the role otherwise — the same
/// `SELECT managing_org_id FROM clusters` each module's private gate repeats.
/// All of them answered 500, so DDL on any of those four tables wedged the
/// whole tree with an internal error and no handler's classified answer was
/// ever reached. Contention on a handler's own
/// table — `templates`, `sandboxes`, `public_operations` — never touches this
/// layer, so those cases were always the handler's to answer.
///
/// [`support::ContentionScope::ReadOnly`], because all three statements are
/// reads that wrote nothing: every contention SQLSTATE still answers a
/// retryable 503, but none of them takes the write-specific 409. The
/// disposition is read from `support` so this layer cannot disagree with the
/// handlers about what counts as contention; the *response* is
/// `PublicApiError` because that is what the rest of this middleware speaks,
/// and it carries the `Retry-After` the admin envelope has no field for.
fn read_contention_response(error: &sqlx::Error) -> Option<Response> {
    // Every status the disposition can return is answered as the retryable
    // 503, which is the only one `ReadOnly` produces today — an invariant
    // asserted in `tests::a_read_only_statement_is_never_blamed_for_a_write_interleave`
    // rather than here, since discarding the status means a change to it would
    // not fail this call site's own test.
    //
    // Two things this deliberately does not do, both of which were tried:
    // fall through to a 500 for an unrecognised status — a retryable race
    // reported as an internal failure is the defect this whole disposition
    // removes — and render `PublicApiError::conflict` for a future 409, which
    // is flagged `retryable: false` and so would ship an envelope
    // contradicting its own "retry the request" message. A neutral 503 is
    // right for anything in the retryable set; a `ReadOnly` 409 would need a
    // retryable-conflict constructor that does not exist.
    support::contention_disposition(support::ContentionScope::ReadOnly, error)
        .map(|_| PublicApiError::lock_contention().into_response())
}

async fn require_admin_auth(
    State(state): State<AppState>,
    mut request: Request,
    next: Next,
) -> Response {
    // With an external console authenticator installed, a console request
    // with an admitted assertion is the only admin credential; bearer sessions
    // are ignored. Admin authorization below is unchanged either way.
    let (user, org) = if let Some(external_console) = &state.external_console {
        let method = request.method().clone();
        match external_console
            .resolve(&state.pool, &method, request.headers())
            .await
        {
            ExternalConsoleAuth::Principal(principal) => *principal,
            ExternalConsoleAuth::NotConsole => {
                return PublicApiError::unauthorized("Console sign-in is required").into_response();
            }
            ExternalConsoleAuth::Denied(error) => return error.into_response(),
        }
    } else {
        let Some(token) = bearer_token(request.headers()) else {
            return PublicApiError::unauthorized("Missing or invalid Authorization header")
                .into_response();
        };

        match auth::get_session_user(&state.pool, token).await {
            Ok(Some(context)) => context,
            Ok(None) => {
                return PublicApiError::unauthorized("Invalid or expired session").into_response();
            }
            Err(error) => {
                if let Some(contended) = read_contention_response(&error) {
                    return contended;
                }
                tracing::error!(%error, "failed to authenticate admin session");
                return PublicApiError::internal().into_response();
            }
        }
    };

    match authz::is_managing_org_admin(&state.pool, &user).await {
        Ok(true) => {}
        Ok(false) => {
            return PublicApiError::forbidden("Caller is not a managing organization admin")
                .into_response();
        }
        Err(error) => {
            if let Some(contended) = read_contention_response(&error) {
                return contended;
            }
            tracing::error!(%error, "failed to authorize admin session");
            return PublicApiError::internal().into_response();
        }
    }

    let method = request.method().clone();
    let method_label = method.as_str().to_owned();
    let path = request.uri().path().to_owned();
    let started_at = Instant::now();
    let audit_actor = user.clone();

    request.extensions_mut().insert(AuthUser(user));
    request.extensions_mut().insert(AuthOrg(org));

    let response = next.run(request).await;
    let status_code = response.status().as_u16();
    let duration_ms = i64::try_from(started_at.elapsed().as_millis()).unwrap_or(i64::MAX);

    if admin_request_should_persist_audit_event(&method)
        && let Err(error) = audit::record_request(
            &state.pool,
            audit::AdminAuditRequestEntry {
                actor: &audit_actor,
                method: &method_label,
                path: &path,
                status_code,
                duration_ms,
            },
        )
        .await
    {
        let mode = "fail_closed";
        pplx_space_metrics::admin::audit_persist_failed(method.as_str(), mode);
        tracing::warn!(%error, method = method.as_str(), path, status_code, mode, "failed to persist admin audit event");
        return audit_failure_response(response.status());
    }

    response
}

/// The response for a failed admin audit-event write.
///
/// Deliberately unclassified, unlike the two gate arms above: this runs after
/// `next.run`, and the audit guarantee is what keeps it fail-closed at a 500
/// whichever SQLSTATE the insert died on.
///
/// The *retry advice* comes from the handler's status alone, because that is
/// the only sound evidence this layer has:
///
/// - **Any 4xx, or a 503 — keep the retry.** A 503 from this surface is itself
///   the retryable answer this disposition produces, so withholding it there
///   would take a contended request that wrote nothing and report a permanent
///   internal failure — the exact defect this change removes, reintroduced one
///   layer up. The 4xx band is the coarser half of the same rule: it covers a
///   refusal that wrote nothing, but it also covers *permanent* refusals — a
///   400 for a malformed `Idempotency-Key`, a 409 for a reused one — where
///   what is actually retryable is the audit write, not the operation, and the
///   caller cannot tell the difference. Both directions cost something here
///   and the status cannot separate them; this is the side that does not turn
///   a transient refusal into a permanent-looking failure.
/// - **Anything else — withhold it.** In practice a 2xx or a 500/502/504; the
///   rule is "not a 4xx and not a 503", so a 3xx or an unusual 5xx lands here
///   too, which is the conservative side. A 2xx committed, and a replay
///   of `POST /admin/api/v1/orgs` would create a second organization under an
///   id the caller never learns, which `internal()`'s unconditional
///   `retryable: true` used to invite. On the rest, whether the handler
///   committed is not knowable: `templates::update_template` answers 500 from
///   its reload arm *after* `tx.commit()` returned.
///
/// **The cost, stated because it is real, and it is a regression from before
/// this change:** on the surfaces that require a caller-supplied
/// `Idempotency-Key` — `sandboxes`' `create_sandbox`,
/// `dispatch_sandbox_action` and `update_sandbox`, plus
/// `templates::delete_template` — a replay *is* safe, and it is the caller's
/// only route back to the `operation_id` the discarded 2xx carried. Those used
/// to be advertised as retryable, because `internal()` advertised everything
/// as retryable; they are withheld now.
///
/// Deciding that from the request's `Idempotency-Key` header was tried and
/// reverted: header *presence* is the caller's choice, while honouring it is
/// the handler's, and only `sandboxes` and `templates` read it. A client that
/// stamps the header on every mutation — the ordinary SDK pattern — would have
/// been told a committed `POST /admin/api/v1/orgs` was safe to replay, which is
/// the duplicate this rule exists to prevent.
///
/// Narrowing it correctly needs the *handler* to say its write was keyed — a
/// response extension it inserts and this layer reads. A header on the request
/// is not that signal, and nothing here guesses at one.
fn audit_failure_response(handler_status: StatusCode) -> Response {
    // 429 is not listed: it is a 4xx, so `is_client_error` already covers it.
    let handler_already_said_come_back =
        handler_status.is_client_error() || handler_status == StatusCode::SERVICE_UNAVAILABLE;
    let error = PublicApiError::internal();
    if handler_already_said_come_back {
        return error.into_response();
    }
    error.not_retryable().into_response()
}

fn admin_request_should_persist_audit_event(method: &Method) -> bool {
    !matches!(method, &Method::GET | &Method::HEAD | &Method::OPTIONS)
}

#[cfg(test)]
mod tests;
