//! Admin OpenAPI entries for operator actions on other organizations'
//! resources.
//!
//! Applied after every other path is inserted: it adds the routes that mirror
//! public, org-scoped ones and amends the entries of routes whose behavior
//! changed. The amendments live here rather than in the
//! [`super::admin_openapi_value`] literal, which is at `json!`'s recursion
//! limit.

use serde_json::{Map, Value, json};

pub(super) fn insert_cross_org_paths(paths: &mut Map<String, Value>) {
    insert_sandbox_paths(paths);
    amend_api_key_paths(paths);
    amend_org_paths(paths);
    amend_sandbox_action_path(paths);
    amend(
        paths,
        "/audit-events",
        "get",
        "One event per admin write, and per read of another organization's data that names a target. target_org_id, target_type (api_key or sandbox) and target_id name the resource when the handler records one; they are null otherwise.",
        &[],
    );
}

/// The `method` operation of `path`, when both are documented.
fn operation<'a>(
    paths: &'a mut Map<String, Value>,
    path: &str,
    method: &str,
) -> Option<&'a mut Map<String, Value>> {
    paths
        .get_mut(path)
        .and_then(|item| item.get_mut(method))
        .and_then(Value::as_object_mut)
}

/// Set the description of an operation and add or replace responses.
fn amend(
    paths: &mut Map<String, Value>,
    path: &str,
    method: &str,
    description: &str,
    responses: &[(&str, &str)],
) {
    let Some(operation) = operation(paths, path, method) else {
        return;
    };
    operation.insert("description".to_string(), json!(description));
    if let Some(existing) = operation
        .get_mut("responses")
        .and_then(Value::as_object_mut)
    {
        for (status, description) in responses {
            existing.insert((*status).to_string(), json!({ "description": description }));
        }
    }
}

fn insert_sandbox_paths(paths: &mut Map<String, Value>) {
    let sandbox_id = json!({ "name": "sandbox_id", "in": "path", "required": true, "schema": { "type": "string", "pattern": "^sbx_" } });
    paths.insert(
        "/sandboxes/{sandbox_id}/terminal-sessions".to_string(),
        json!({
            "post": {
                "summary": "Open a terminal session in any organization's sandbox",
                "description": "Same body and response as the public POST /v1/sandboxes/{sandbox_id}/terminal-sessions, created under the sandbox's owning organization and attributed to the calling admin. The attach token connects to this session only and expires as the public route's does. The audit event names the sandbox and its organization.",
                "parameters": [
                    sandbox_id.clone(),
                    { "name": "Idempotency-Key", "in": "header", "required": true, "schema": { "type": "string" } }
                ],
                "responses": {
                    "201": { "description": "Session with a one-time attach token" },
                    "400": { "description": "Invalid sandbox id, terminal size or idempotency key" },
                    "404": { "description": "Sandbox not found" },
                    "409": { "description": "Idempotency conflict" }
                }
            }
        }),
    );
    paths.insert(
        "/sandboxes/{sandbox_id}/snapshots".to_string(),
        json!({
            "get": {
                "summary": "List any organization's sandbox snapshots",
                "description": "Same page as the public GET /v1/sandboxes/{sandbox_id}/snapshots, read under the sandbox's owning organization whether or not that organization can see the public route. The read is audited with the sandbox and its organization.",
                "parameters": [
                    sandbox_id,
                    { "name": "cursor", "in": "query", "required": false, "schema": { "type": "string" } },
                    { "name": "limit", "in": "query", "required": false, "schema": { "type": "integer", "minimum": 0, "maximum": 1000 } }
                ],
                "responses": {
                    "200": { "description": "Snapshot page (data, next_cursor)" },
                    "400": { "description": "Invalid sandbox id or cursor" },
                    "404": { "description": "Sandbox not found" }
                }
            }
        }),
    );
}

fn amend_api_key_paths(paths: &mut Map<String, Value>) {
    let keys = "/orgs/{org_id}/api-keys";
    amend(
        paths,
        keys,
        "get",
        "Any organization's keys, newest first. Revoked keys are listed, with revoked_at, only with include_revoked=true. last_used_at is when the key last authenticated, recorded at most once every five minutes.",
        &[],
    );
    if let Some(list) = operation(paths, keys, "get") {
        list.insert(
            "parameters".to_string(),
            json!([
                { "name": "include_revoked", "in": "query", "required": false, "schema": { "type": "boolean", "default": false } }
            ]),
        );
    }
    amend(
        paths,
        keys,
        "post",
        "Creates a key for any organization and returns the raw secret exactly once. The key is attributed to the calling admin (created_by) and calls only the allowed_surfaces APIs as the organization: as its creator when the creator belongs to it, otherwise as the organization's oldest user. The audit event names the organization and the key id, never the secret.",
        &[("404", "Organization not found")],
    );
    if let Some(responses) = operation(paths, keys, "post")
        .and_then(|create| create.get_mut("responses"))
        .and_then(Value::as_object_mut)
    {
        responses.remove("403");
    }
    amend(
        paths,
        "/orgs/{org_id}/api-keys/{api_key_id}",
        "delete",
        "The key stops authenticating at once and is listed with revoked_at when include_revoked=true. Revoking a revoked key succeeds and keeps its first revocation time. The audit event names the organization and the key id.",
        &[],
    );
}

fn amend_org_paths(paths: &mut Map<String, Value>) {
    let branding = "theme_primary and theme_secondary must be #rgb or #rrggbb hex colors, and logo_url an absolute https URL (http only on localhost or a loopback address).";
    let invalid = [(
        "400",
        "Invalid branding value; field names the request field",
    )];
    amend(paths, "/orgs", "post", branding, &invalid);
    amend(paths, "/orgs/{org_id}", "patch", branding, &invalid);
}

fn amend_sandbox_action_path(paths: &mut Map<String, Value>) {
    let path = "/sandboxes/{sandbox_id}:force-{action}";
    amend(
        paths,
        path,
        "post",
        "Runs under the sandbox's owning organization. rollback takes the public :rollback body ({snapshot_name, auto_restart?}), which is part of the idempotency fingerprint; other actions ignore the body. The audit event names the sandbox and its organization.",
        &[("400", "Invalid action, rollback body or idempotency key")],
    );
    if let Some(actions) = operation(paths, path, "post")
        .and_then(|action| action.get_mut("parameters"))
        .and_then(Value::as_array_mut)
        .and_then(|parameters| {
            parameters
                .iter_mut()
                .find(|parameter| parameter.get("name") == Some(&json!("action")))
        })
        .and_then(|parameter| parameter.pointer_mut("/schema/enum"))
        .and_then(Value::as_array_mut)
    {
        actions.push(json!("rollback"));
    }
}
