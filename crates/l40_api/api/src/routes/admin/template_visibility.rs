//! `PUT /admin/v1/templates/{name}:visibility` operator surface.
//!
//! v1 of the org-scoped-templates RFC classifies templates at (or right
//! after) create; visibility flips of an *in-use* template are deferred to a
//! follow-up (residue gates, prefetch quiescing, cache eviction, migration).
//! This endpoint therefore allows a flip — in either direction — only while
//! the template is quiescent: no live or restorable sandboxes reference it,
//! and (for a narrowing flip) no node still caches its rootfs. Anything else
//! is a 409 pointing at the residue.

use axum::{
    Extension, Json,
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use pplx_space_common_core::types::OrgId;
use pplx_space_common_core::types::TemplateName;
use pplx_space_common_core::types::TemplateVisibility;
use pplx_space_common_core::types::admin::{
    AdminTemplateVisibilityResponse, SetTemplateVisibilityRequest,
};

use super::audit::{self, AdminTemplateAuditEntry};
use super::node_group_store::org_exists_in_tx;
use super::support::{api_error, db_error, parse_json_body, require_managing_org_admin};
use super::template_store;
use crate::middleware::auth::AuthUser;
use crate::routes::AppState;

/// Parse the `{spec}` segment of `PUT /admin/v1/templates/{spec}`, which
/// must be a `name:visibility` pair (template names cannot contain `:`, so
/// the split is unambiguous). Mirrors the `/admin/v1/nodes/{spec}`
/// workaround for matchit's lack of dynamic suffixes.
fn parse_template_spec(spec: &str) -> Result<TemplateName, (StatusCode, Json<serde_json::Value>)> {
    let Some((name_part, action_part)) = spec.split_once(':') else {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "template admin PUT requires an :action suffix (visibility)",
        ));
    };
    if action_part != "visibility" {
        return Err(api_error(
            StatusCode::NOT_FOUND,
            format!("unknown admin action {action_part:?}; expected: visibility"),
        ));
    }
    TemplateName::parse(name_part).map_err(|err| {
        api_error(
            StatusCode::BAD_REQUEST,
            format!("invalid template name: {err}"),
        )
    })
}

fn visibility_state_summary(visibility: TemplateVisibility, org_id: Option<OrgId>) -> String {
    let org = org_id.map_or_else(|| "none".to_string(), |id| id.to_string());
    format!("visibility={visibility} org={org}")
}

/// `true` when the request describes the state the template row is already
/// in — including a desired-state PUT that repeats the current owner. Such
/// requests must short-circuit before the in-use/cached-rootfs guards, or an
/// idempotent retry against an in-use template would 409 even though no row
/// would change. (A *differing* supplied owner is rejected by the
/// reassignment guard before this check runs.)
fn is_visibility_noop(
    request: &SetTemplateVisibilityRequest,
    current_visibility: TemplateVisibility,
    current_org_id: Option<OrgId>,
) -> bool {
    request.visibility == current_visibility
        && (request.org_id.is_none() || request.org_id == current_org_id)
}

/// `PUT /admin/v1/templates/{name}:visibility` — set a template's launch
/// scope, optionally assigning the owning org in the same write.
///
/// The flag is consumed by the create-time authorization gate in template
/// resolution and by node-group placement, which land with the sibling
/// create-path changes of this RFC. Until those ship, setting `org_specific`
/// records intent (and is enforced by this surface's own compat/assign-org
/// invariants) but does not yet block cross-org launches.
pub async fn set_template_visibility(
    State(state): State<AppState>,
    Extension(AuthUser(actor)): Extension<AuthUser>,
    Path(spec): Path<String>,
    body: Bytes,
) -> axum::response::Response {
    if let Err(err) = require_managing_org_admin(&state.pool, &actor).await {
        return err.into_response();
    }
    let template_name = match parse_template_spec(&spec) {
        Ok(name) => name,
        Err(err) => return err.into_response(),
    };
    let request: SetTemplateVisibilityRequest = match parse_json_body(&body, "visibility") {
        Ok(request) => request,
        Err(err) => return err.into_response(),
    };
    if request.org_id.is_some() && matches!(request.visibility, TemplateVisibility::Generic) {
        return api_error(
            StatusCode::BAD_REQUEST,
            "org_id may only accompany visibility 'org_specific'; ownership of a \
             generic template is not changed through this endpoint",
        )
        .into_response();
    }

    let mut tx = match state.pool.begin().await {
        Ok(tx) => tx,
        Err(err) => return db_error(err).into_response(),
    };
    let template =
        match template_store::lock_template_for_update(&mut tx, template_name.as_str()).await {
            Ok(Some(row)) => row,
            Ok(None) => {
                return api_error(StatusCode::NOT_FOUND, "Template not found").into_response();
            }
            Err(err) => return db_error(err).into_response(),
        };
    let from_state = visibility_state_summary(template.visibility, template.org_id);
    let audit_failure = |error: &str| {
        audit::record_template(AdminTemplateAuditEntry {
            actor: &actor,
            action: "template-set-visibility",
            template_name: template_name.as_str(),
            from_state: Some(&from_state),
            to_state: None,
            success: false,
            error: Some(error),
        });
    };

    // Ownership reassignment of an already-owned template is a different
    // operation with different residue implications; not supported here.
    if let (Some(requested), Some(current)) = (request.org_id, template.org_id)
        && requested != current
    {
        audit_failure("conflict: ownership reassignment not supported");
        return api_error(
            StatusCode::CONFLICT,
            "template already has an owning org; reassigning ownership is not \
             supported through this endpoint",
        )
        .into_response();
    }

    // A supplied org_id wins over the row's current owner (the
    // reassignment guard above already rejected a conflicting pair, so
    // "wins" only ever assigns a previously NULL owner).
    let effective_org = request.org_id.or(template.org_id);
    if matches!(request.visibility, TemplateVisibility::OrgSpecific) && effective_org.is_none() {
        audit_failure("org-specific requires an owner");
        return api_error(
            StatusCode::BAD_REQUEST,
            "an org-specific template must have an owning org; supply org_id in the \
             request body",
        )
        .into_response();
    }
    if let Some(org_id) = request.org_id {
        match org_exists_in_tx(&mut tx, org_id).await {
            Ok(true) => {}
            Ok(false) => {
                audit_failure("unknown org");
                return api_error(StatusCode::BAD_REQUEST, "unknown org").into_response();
            }
            Err(err) => return db_error(err).into_response(),
        }
    }

    if is_visibility_noop(&request, template.visibility, template.org_id) {
        // No-op: already in the requested state.
        drop(tx);
        return Json(AdminTemplateVisibilityResponse {
            name: template.name,
            visibility: template.visibility,
            org_id: template.org_id,
            pruned_compat_rows: 0,
        })
        .into_response();
    }

    // Flip guards. Both directions require sandbox quiescence (a flip of an
    // in-use template is the deferred follow-up); narrowing additionally
    // requires no cached rootfs anywhere — that residue is exactly what the
    // data-isolation guarantee forbids on shared nodes.
    let in_use = match template_store::template_sandboxes_in_use_in_tx(
        &mut tx,
        template_name.as_str(),
    )
    .await
    {
        Ok(count) => count,
        Err(err) => return db_error(err).into_response(),
    };
    if in_use > 0 {
        audit_failure("conflict: template is in use");
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({
                "error": "template still has live or restorable sandboxes; visibility \
                          flips of an in-use template are not supported in v1",
                "sandboxes_in_use": in_use,
            })),
        )
            .into_response();
    }
    // Widening (org-specific → generic) an owner-encrypted template would
    // publish content wrapped under one org's key to every org — the
    // launchable-but-undecryptable state the RFC forbids. Re-keying on widen
    // is deferred to the encryption follow-up, so v1 refuses rather than
    // silently exposing the artifact.
    let widening = matches!(request.visibility, TemplateVisibility::Generic)
        && matches!(template.visibility, TemplateVisibility::OrgSpecific);
    if widening {
        // The encryption sidecars are credential-fenced, so this check runs on
        // the credential pool rather than the `space_user` flip transaction
        // (which has no grant on those tables).
        let owner_encrypted = match template_store::template_has_owner_encryption(
            state.credentials_pool.pool(),
            template_name.as_str(),
        )
        .await
        {
            Ok(has) => has,
            Err(err) => return db_error(err).into_response(),
        };
        if owner_encrypted {
            audit_failure("conflict: owner-encrypted template cannot widen");
            return (
                StatusCode::CONFLICT,
                Json(serde_json::json!({
                    "error": "template content is encrypted to its owning org; widening \
                              to generic would leave it launchable but undecryptable for \
                              other orgs. Re-keying on widen is deferred to the encryption \
                              follow-up and is not supported in v1.",
                })),
            )
                .into_response();
        }
    }

    let narrowing = matches!(request.visibility, TemplateVisibility::OrgSpecific)
        && matches!(template.visibility, TemplateVisibility::Generic);
    if narrowing {
        let cached =
            match template_store::template_cached_rootfs_in_tx(&mut tx, template_name.as_str())
                .await
            {
                Ok(count) => count,
                Err(err) => return db_error(err).into_response(),
            };
        if cached > 0 {
            audit_failure("conflict: rootfs still cached on nodes");
            return (
                StatusCode::CONFLICT,
                Json(serde_json::json!({
                    "error": "template rootfs is still cached on nodes; wait for cache \
                              eviction (or clear the caches) before narrowing to \
                              org-specific",
                    "cached_nodes": cached,
                })),
            )
                .into_response();
        }
    }

    if let Err(err) = template_store::update_template_visibility_in_tx(
        &mut tx,
        template_name.as_str(),
        request.visibility,
        request.org_id,
    )
    .await
    {
        return db_error(err).into_response();
    }
    let pruned_compat_rows = if narrowing {
        // `effective_org` is Some here: org-specific without an owner was
        // rejected above.
        let owner = effective_org.expect("narrowing flip validated to have an owner");
        match template_store::prune_cross_org_compat_in_tx(&mut tx, template_name.as_str(), owner)
            .await
        {
            Ok(count) => count,
            Err(err) => return db_error(err).into_response(),
        }
    } else {
        0
    };
    if let Err(err) = tx.commit().await {
        return db_error(err).into_response();
    }

    let response = AdminTemplateVisibilityResponse {
        name: template.name,
        visibility: request.visibility,
        org_id: effective_org,
        pruned_compat_rows,
    };
    audit::record_template(AdminTemplateAuditEntry {
        actor: &actor,
        action: "template-set-visibility",
        template_name: template_name.as_str(),
        from_state: Some(&from_state),
        to_state: Some(&visibility_state_summary(request.visibility, effective_org)),
        success: true,
        error: None,
    });
    Json(response).into_response()
}

#[cfg(test)]
mod tests;
