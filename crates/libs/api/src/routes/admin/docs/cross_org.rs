//! Admin OpenAPI entries for the routes that mirror public, org-scoped ones
//! for any organization's sandbox.

use serde_json::{Map, Value, json};

pub(super) fn insert_admin_sandbox_session_paths(paths: &mut Map<String, Value>) {
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
