//! Axum handlers for the e2b-compat template-build URLs.
//!
//! Endpoint <-> SDK call mapping:
//!
//! * `POST /v3/templates`                                  ↔ `request_build`
//! * `GET  /templates/aliases/{alias}`                     ↔ `check_alias_exists`
//! * `GET  /templates/{templateID}/files/{filesHash}`      ↔ `get_file_upload_link`
//! * `POST /v2/templates/{templateID}/builds/{buildID}`    ↔ `trigger_build`
//! * `GET  /templates/{templateID}/builds/{buildID}/status` ↔ `get_build_status`
//!
//! See the parent `mod.rs` for caveats (snapshot semantics, persisted build
//! state, base alias).

use axum::{
    Extension, Json,
    extract::{OriginalUri, Path as AxumPath, State},
    http::{HeaderMap, StatusCode, header, uri::Authority},
    response::IntoResponse,
};
use pplx_space_build_mgr_client::proto::{self as build_proto, BuildState};
use pplx_space_common_core::types::{RuntimeKind, TemplateName, TemplateTag, TemplateVisibility};
use serde::Deserialize;
use uuid::Uuid;

use super::super::super::AppState;
use super::super::translate::controller_error;
use super::translate::build_recipe;
use super::upload::mint_file_upload_query;
use super::wire::{
    BuildStatusReason, TemplateAliasResponse, TemplateBuildFileUpload, TemplateBuildInfo,
    TemplateBuildRequestV3, TemplateBuildStartV2, TemplateRequestResponseV3,
};
use crate::{
    controllers::{
        ControllerError,
        sandbox::{TemplateAccess, gate_template_access, resolve_template_release_by_tag},
        template::{TemplateBuildStatusPollError, TemplateBuildSubmission, TemplateBuildTarget},
    },
    e2b::errors::E2bApiError,
    middleware::auth::{AuthEcwActor, AuthOrg},
};

// ---------------------------------------------------------------------------
// POST /v3/templates
// ---------------------------------------------------------------------------

pub async fn create_template_v3(
    State(state): State<AppState>,
    Extension(AuthEcwActor(actor)): Extension<AuthEcwActor>,
    crate::e2b::E2bJson(req): crate::e2b::E2bJson<TemplateBuildRequestV3>,
) -> Result<impl IntoResponse, E2bApiError> {
    let actor = actor.ok_or_else(E2bApiError::unauthorized)?;
    validate_template_build_request(&req)?;
    // The SDK sends `name`; older clients send `alias`. Either works.
    //
    // e2b allows `name="my-template:v1"` where the suffix after the first
    // `:` is folded into the build's tag list (vendored
    // `api/internal/handlers/template_request_build_v3.go:68` +
    // `shared/pkg/id/id.go:91`). Mirror that here: split at the first
    // `:`, parse the prefix through TemplateName (so the templateID
    // can't contain `..`, `/`, etc. — buildmgr later does
    // `format!("{TEMPLATE_DIR}/{template_id}")`), and append the
    // suffix tag to the body-supplied tag list.
    // Normalize like vendored e2b's id.ParseName: trim leading/trailing
    // whitespace, lowercase the identifier (e2b's namespace/alias must
    // match `^[a-z0-9-_]+$`), then split on the first `:` to extract
    // an embedded tag suffix.
    let raw_name = req
        .name
        .as_deref()
        .or(req.alias.as_deref())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| E2bApiError::bad_request("name (or alias) is required"))?;
    let (raw_template_id, embedded_tag) = match raw_name.split_once(':') {
        Some((id, tag)) if !tag.is_empty() => (id, Some(tag.to_string())),
        _ => (raw_name, None),
    };
    let normalized_id = raw_template_id.to_ascii_lowercase();
    // Reject e2b's `namespace/alias` form before the regex check —
    // vendored e2b accepts it (`shared/pkg/id/id.go:80`) and resolves
    // the namespace against the caller's team, but SPACE has no team
    // scoping yet so we can't honor the namespace. A clearer error
    // beats a generic "must match regex" rejection.
    if normalized_id.contains('/') {
        return Err(E2bApiError::bad_request(format!(
            "template name {raw_template_id:?} uses an e2b `namespace/alias` form; \
             SPACE doesn't support team namespaces in this MVP — pass a flat \
             alias instead (the namespace would otherwise be silently dropped)",
        )));
    }
    // Enforce vendored e2b's identifierRegex `^[a-z0-9-_]+$`
    // (`shared/pkg/id/id.go:17`). That's narrower than SPACE's
    // TemplateName::parse, so a value that's valid e2b is also valid SPACE.
    if !is_e2b_identifier(&normalized_id) {
        return Err(E2bApiError::bad_request(format!(
            "invalid template name {raw_template_id:?}: must match ^[a-z0-9-_]+$",
        )));
    }
    // Defense-in-depth: also run TemplateName::parse so the SPACE-side
    // path-traversal guard still fires if the e2b regex is ever relaxed.
    let template_name = TemplateName::parse(&normalized_id).map_err(|err| {
        E2bApiError::bad_request(format!("invalid template name {raw_template_id:?}: {err}"))
    })?;
    let template_id = template_name.as_str().to_string();
    let build_id = Uuid::now_v7().to_string();

    // Mirror vendored e2b's id.ParseName + ValidateAndDeduplicateTags
    // (handlers/template_request_build_v3.go + shared/pkg/id/id.go):
    //  * Tags are trimmed + lowercased + format-validated.
    //  * The `:default` suffix is the reserved DefaultTag — drop it.
    //  * UUID-shaped tags are reserved for build IDs — reject.
    //  * Dedup case-insensitively.
    // body-supplied tags go through the same path so SDK callers can't
    // sneak a malformed tag past us. The embedded `:tag` suffix from the
    // template name lands at the tail of the list, but only if not
    // already present.
    let mut tags: Vec<String> = Vec::new();
    for raw in req.tags.unwrap_or_default() {
        // Body tags go through e2b's stricter validation: empty after
        // trim is an error (vendored e2b rejects `tags=[""]`), but
        // `tags=["default"]` is valid — only the embedded `:default`
        // suffix below is dropped.
        let t = normalize_body_tag(&raw)?;
        if !tags.iter().any(|existing| existing == &t) {
            tags.push(t);
        }
    }
    if let Some(raw) = embedded_tag.as_deref()
        && let Some(t) = normalize_embedded_tag(raw)?
        && !tags.iter().any(|existing| existing == &t)
    {
        tags.push(t);
    }

    // Validate sizing at the wire boundary. The SDK's `cpuCount` /
    // `memoryMB` come over as `u32`, and any sane build VM sits well
    // below the i32 ceiling — but the Postgres-backed state path will
    // narrow to `INTEGER` (i32), and the build daemon's BuildConfig
    // fields are also bounded. Reject pathological values here so a
    // crafted body can't panic a downstream `try_from` or run the
    // builder into the ground.
    let cpu_count = req.cpu_count;
    let memory_mb = req.memory_mb;
    if let Some(value) = cpu_count.filter(|value| *value > i32::MAX as u32) {
        return Err(E2bApiError::bad_request(format!(
            "cpuCount={:?} exceeds the supported range",
            value,
        )));
    }
    if let Some(value) = memory_mb.filter(|value| *value > i32::MAX as u32) {
        return Err(E2bApiError::bad_request(format!(
            "memoryMB={:?} exceeds the supported range",
            value,
        )));
    }

    // Stash sizing + tags keyed by (templateID, buildID). The SDK doesn't
    // repeat any of this in the subsequent v2 trigger-build call, and
    // keying by templateID alone would have concurrent builds for the
    // same alias clobber each other.
    state
        .template_build_state
        .record_build_spec(
            actor.org_id,
            &template_name,
            &build_id,
            super::state::BuildSpec {
                tags: tags.clone(),
                cpu_count,
                memory_mb,
            },
        )
        .await
        .map_err(|err| super::template_build_store_error("record template build spec", err))?;

    Ok((
        StatusCode::ACCEPTED,
        Json(TemplateRequestResponseV3 {
            aliases: vec![template_id.clone()],
            build_id,
            names: vec![template_id.clone()],
            public: false,
            tags,
            template_id,
        }),
    ))
}

fn validate_template_build_request(req: &TemplateBuildRequestV3) -> Result<(), E2bApiError> {
    if req.team_id.is_some() {
        return Err(E2bApiError::bad_request(
            "unsupported template build parameter: teamID",
        ));
    }
    if matches!(req.min_free_disk_mb, Some(value) if value > 0) {
        return Err(E2bApiError::bad_request(
            "unsupported template build parameter: minFreeDiskMb greater than zero",
        ));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// GET /templates/aliases/{alias}
// ---------------------------------------------------------------------------

pub async fn check_alias(
    State(state): State<AppState>,
    Extension(AuthOrg(org)): Extension<AuthOrg>,
    AxumPath(alias): AxumPath<String>,
) -> Result<Json<TemplateAliasResponse>, E2bApiError> {
    let alias = normalize_template_alias(&alias)?;
    let caller_org = org.id;
    let TemplateAccess::Authorized { visibility } =
        gate_template_access(&state.pool, &alias, caller_org)
            .await
            .map_err(controller_error)?
    else {
        return Err(E2bApiError::not_found("template alias"));
    };

    if resolve_template_release_by_tag(
        &state.pool,
        &alias,
        &TemplateTag::latest(),
        RuntimeKind::Firecracker,
        caller_org,
    )
    .await
    .map_err(controller_error)?
    .is_none()
    {
        return Err(E2bApiError::not_found("template alias"));
    }

    Ok(Json(TemplateAliasResponse {
        template_id: alias.into(),
        public: visibility == TemplateVisibility::Generic,
    }))
}

fn normalize_template_alias(raw: &str) -> Result<TemplateName, E2bApiError> {
    let alias = raw.trim().to_ascii_lowercase();
    if alias.contains(':') {
        return Err(E2bApiError::bad_request(
            "tagged template alias selectors are not supported; pass a plain alias",
        ));
    }
    if !is_e2b_identifier(&alias) {
        return Err(E2bApiError::bad_request(format!(
            "invalid template alias {raw:?}: must match ^[a-z0-9-_]+$",
        )));
    }
    TemplateName::parse(alias)
        .map_err(|error| E2bApiError::bad_request(format!("invalid template alias: {error}")))
}

// ---------------------------------------------------------------------------
// GET /templates/{templateID}/files/{filesHash}
// ---------------------------------------------------------------------------

pub async fn get_file_upload(
    State(_state): State<AppState>,
    Extension(AuthOrg(org)): Extension<AuthOrg>,
    OriginalUri(original_uri): OriginalUri,
    headers: HeaderMap,
    AxumPath((template_id, files_hash)): AxumPath<(String, String)>,
) -> Result<impl IntoResponse, E2bApiError> {
    // Validate the templateID so the returned PUT URL doesn't carry
    // unsanitized path segments back to us; the PUT handler runs the same
    // validation, but rejecting here lets the SDK fail fast.
    let template_id = TemplateName::parse(template_id)
        .map_err(|err| E2bApiError::bad_request(format!("invalid templateID: {err}")))?
        .as_str()
        .to_string();

    // We don't yet cache uploads across builds; always tell the SDK to PUT.
    // E2B 2.34 uploads with a fresh httpx client that has neither the API
    // client's base URL nor its x-api-key header. Return an absolute URL with
    // a short-lived, path-bound capability so that client can reach the
    // authenticated upload route without exposing the ECW API key.
    //
    // Status MUST be 201 — the SDK's `_parse_response` only accepts 201 as
    // success here (returns None on anything else, which surfaces to the
    // caller as "Failed to get file upload link").
    let origin = request_origin(&headers)?;
    let upload_path = file_upload_path(&template_id, &files_hash, original_uri.path());
    let upload_query = mint_file_upload_query(org.id, &upload_path)?;
    let url = file_upload_url(&origin, &upload_path, &upload_query);
    Ok((
        StatusCode::CREATED,
        Json(TemplateBuildFileUpload {
            present: false,
            url: Some(url),
        }),
    ))
}

fn file_upload_path(template_id: &str, files_hash: &str, request_path: &str) -> String {
    let path_root = if request_path.starts_with(crate::routes::E2B_API_PATH_ROOT) {
        crate::routes::E2B_API_PATH_ROOT
    } else {
        ""
    };
    format!("{path_root}/v1/internal/e2b-files/{template_id}/{files_hash}")
}

fn file_upload_url(origin: &str, upload_path: &str, upload_query: &str) -> String {
    format!("{origin}{upload_path}?{upload_query}")
}

fn request_origin(headers: &HeaderMap) -> Result<String, E2bApiError> {
    // TLS terminates before space-api in deployed environments, where the
    // ingress supplies x-forwarded-proto. A direct connection reaches this
    // plaintext HTTP listener, so HTTP is the correct fallback.
    let scheme = headers
        .get("x-forwarded-proto")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(',').next())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("http");
    if !matches!(scheme, "http" | "https") {
        return Err(E2bApiError::bad_request("invalid x-forwarded-proto"));
    }

    let authority = headers
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| E2bApiError::bad_request("Host header is required"))?
        .parse::<Authority>()
        .map_err(|_| E2bApiError::bad_request("invalid Host header"))?;

    Ok(format!("{scheme}://{authority}"))
}

// ---------------------------------------------------------------------------
// POST /v2/templates/{templateID}/builds/{buildID}
// ---------------------------------------------------------------------------

pub async fn start_build(
    State(state): State<AppState>,
    Extension(AuthEcwActor(actor)): Extension<AuthEcwActor>,
    AxumPath((template_id, build_id)): AxumPath<(String, String)>,
    crate::e2b::E2bJson(req): crate::e2b::E2bJson<TemplateBuildStartV2>,
) -> Result<impl IntoResponse, E2bApiError> {
    let actor = actor.ok_or_else(E2bApiError::unauthorized)?;
    // Re-validate the templateID at the v2 boundary too — even though
    // create_template_v3 only stashes parsed names, the SDK can call this
    // directly with any URL segment. Buildmgr formats this into a
    // filesystem path; validate before forwarding.
    let template_name = TemplateName::parse(template_id)
        .map_err(|err| E2bApiError::bad_request(format!("invalid templateID: {err}")))?;
    // Reject direct v2 calls that didn't come through v3 — without the
    // create_template_v3 stash we have no tags or sizing, and silently
    // proceeding would publish a build with neither. Vendored e2b
    // rejects the same shape.
    let spec = state
        .template_build_state
        .lookup_build_spec(actor.org_id, &template_name, &build_id)
        .await
        .map_err(|err| super::template_build_store_error("lookup template build spec", err))?
        .ok_or_else(|| {
            E2bApiError::not_found(
                "no pending build for this (templateID, buildID); call POST /v3/templates first",
            )
        })?;
    let recipe = build_recipe(
        actor.org_id,
        &template_name,
        &req,
        &state.template_build_state,
        spec.cpu_count,
        spec.memory_mb,
    )
    .await?;
    state
        .template_controller
        .submit_template_build(TemplateBuildSubmission {
            target: TemplateBuildTarget::Organization(template_name),
            client_build_id: Some(build_id),
            recipe,
            force: req.force_build(),
            publish_tags: publish_tags(spec.tags),
            client: build_proto::ClientContext {
                source_path: String::new(),
                cli_version: env!("CARGO_PKG_VERSION").to_string(),
                hostname: hostname().unwrap_or_default(),
            },
            org_id: actor.org_id,
        })
        .await
        .map_err(|error| template_build_submit_error(error.into()))?;

    Ok((StatusCode::ACCEPTED, Json(serde_json::json!({}))))
}

// ---------------------------------------------------------------------------
// GET /templates/{templateID}/builds/{buildID}/status
//
// The SDK sends `logsOffset` (+ `limit`, `level`) so it can incrementally
// stream log entries. SPACE accepts those compatibility parameters even
// though it currently returns no log entries. Unknown keys remain accepted,
// matching E2B's open OpenAPI object contract.
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub(crate) struct BuildStatusQuery {
    logs_offset: Option<u64>,
    limit: Option<u64>,
    level: Option<String>,
}

pub async fn get_build_status(
    State(state): State<AppState>,
    Extension(AuthOrg(org)): Extension<AuthOrg>,
    AxumPath((template_id, build_id)): AxumPath<(String, String)>,
    crate::e2b::E2bQuery(_query): crate::e2b::E2bQuery<BuildStatusQuery>,
) -> Result<impl IntoResponse, E2bApiError> {
    let template_name = TemplateName::parse(template_id)
        .map_err(|err| E2bApiError::bad_request(format!("invalid templateID: {err}")))?;
    let template_id = template_name.as_str().to_string();
    let job = state
        .template_build_state
        .lookup_build(org.id, &template_name, &build_id)
        .await
        .map_err(|err| super::template_build_store_error("lookup template build job", err))?
        .ok_or_else(|| E2bApiError::not_found("template build"))?;

    let job_id = job.daemon_job_id.to_string();
    let resp = match state
        .template_controller
        .poll_template_build_status(job_id)
        .await
    {
        Ok(resp) => resp,
        Err(TemplateBuildStatusPollError::Transient { reason, source }) => {
            tracing::warn!(
                template_id = %template_id,
                build_id = %build_id,
                reason = reason.as_str(),
                error = %source,
                "transient error while polling build status; see `reason` for the source"
            );
            return Ok(Json(building_template_build_info(template_id, build_id)));
        }
        Err(error) => return Err(template_build_status_error(error)),
    };

    let (status, reason) = map_state(resp.state, resp.error);
    Ok(Json(TemplateBuildInfo {
        build_id,
        log_entries: vec![],
        logs: vec![],
        status,
        template_id,
        reason,
    }))
}

fn building_template_build_info(template_id: String, build_id: String) -> TemplateBuildInfo {
    TemplateBuildInfo {
        build_id,
        log_entries: vec![],
        logs: vec![],
        status: "building",
        template_id,
        reason: None,
    }
}

fn template_build_submit_error(error: ControllerError) -> E2bApiError {
    // Reconstructing from `status()` + message drops the lock-contention
    // marking, and with it the `Api` attribution and the paced `Retry-After` —
    // the builder-node lookup behind this RPC is a plain read that can abort
    // with `55P03`. Delegate that case to the shared translator, which is the
    // one place that knows how to carry it.
    if error.is_lock_contention() {
        return controller_error(error);
    }
    E2bApiError::new(
        error.status(),
        format!("BuilderApi/BuildTemplate rpc: {error}"),
    )
}

fn template_build_status_error(error: TemplateBuildStatusPollError) -> E2bApiError {
    let TemplateBuildStatusPollError::Failed(error) = error else {
        return E2bApiError::new(
            StatusCode::BAD_GATEWAY,
            "BuilderApi/GetBuildTemplateStatus rpc: transient builder status error",
        );
    };
    E2bApiError::new(
        error.status(),
        format!("BuilderApi/GetBuildTemplateStatus rpc: {error}"),
    )
}

fn map_state(raw: i32, error: String) -> (&'static str, Option<BuildStatusReason>) {
    match BuildState::try_from(raw).unwrap_or(BuildState::Unspecified) {
        BuildState::Pending | BuildState::Running => ("building", None),
        BuildState::Succeeded => ("ready", None),
        BuildState::Failed | BuildState::PublishFailed => {
            let message = if error.is_empty() {
                "build failed".to_string()
            } else {
                error
            };
            ("error", Some(BuildStatusReason { message }))
        }
        BuildState::Unspecified => ("waiting", None),
    }
}

/// Vendored e2b's identifier regex is `^[a-z0-9-_]+$`
/// (`shared/pkg/id/id.go:17`). Caller is responsible for
/// trimming + lowercasing the input first; this only matches.
fn is_e2b_identifier(value: &str) -> bool {
    !value.is_empty()
        && value
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_')
}

/// Vendored e2b's tag regex is `^[a-z0-9-_.]+$`
/// (`shared/pkg/id/id.go:18`) — same as identifier plus `.`. Notably
/// allows leading `-` and `.`, which SPACE's own `TemplateTag::parse`
/// rejects; this function uses e2b's grammar verbatim so the shim
/// stays wire-compatible.
fn is_e2b_tag(value: &str) -> bool {
    !value.is_empty()
        && value.chars().all(|c| {
            c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_' || c == '.'
        })
}

/// Trim + lowercase + validate a body tag. Empty tags are an error
/// (`POST {tags=[""]}` is invalid in vendored e2b); `"default"` is
/// permitted in the body list (only `:default` suffix embedded in the
/// name is reserved). UUID-shaped tags are rejected (reserved for
/// build IDs).
fn normalize_body_tag(raw: &str) -> Result<String, E2bApiError> {
    let trimmed = raw.trim().to_ascii_lowercase();
    if trimmed.is_empty() {
        return Err(E2bApiError::bad_request(format!(
            "invalid tag {raw:?}: must not be empty"
        )));
    }
    if Uuid::parse_str(&trimmed).is_ok() {
        return Err(E2bApiError::bad_request(format!(
            "tag {raw:?} is UUID-shaped (reserved for build IDs)",
        )));
    }
    if !is_e2b_tag(&trimmed) {
        return Err(E2bApiError::bad_request(format!(
            "invalid tag {raw:?}: must match ^[a-z0-9-_.]+$",
        )));
    }
    Ok(trimmed)
}

/// Trim + lowercase + validate the `:tag` suffix embedded in the
/// `name`. Empty or `default` is dropped (vendored e2b's reserved
/// DefaultTag). UUID-shaped tags are rejected.
fn normalize_embedded_tag(raw: &str) -> Result<Option<String>, E2bApiError> {
    let trimmed = raw.trim().to_ascii_lowercase();
    if trimmed.is_empty() || trimmed == "default" {
        return Ok(None);
    }
    if Uuid::parse_str(&trimmed).is_ok() {
        return Err(E2bApiError::bad_request(format!(
            "tag {raw:?} is UUID-shaped (reserved for build IDs)",
        )));
    }
    if !is_e2b_tag(&trimmed) {
        return Err(E2bApiError::bad_request(format!(
            "invalid tag {raw:?}: must match ^[a-z0-9-_.]+$",
        )));
    }
    Ok(Some(trimmed))
}

fn publish_tags(mut tags: Vec<String>) -> Vec<String> {
    // An untagged E2B build updates the bare template name. Explicit tags
    // leave that mutable alias unchanged.
    if tags.is_empty() {
        tags.push("latest".to_string());
    }
    tags
}

fn hostname() -> Option<String> {
    std::env::var("HOSTNAME")
        .ok()
        .filter(|h| !h.is_empty())
        .or_else(|| {
            std::fs::read_to_string("/etc/hostname")
                .ok()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
        })
}

#[cfg(test)]
mod tests;
