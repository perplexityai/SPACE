use axum::{
    Json,
    http::{HeaderValue, header},
    response::IntoResponse,
};
use pplx_space_public_api::routes::{
    M1_STATUS_EXTENSION, MAX_UTF8_BYTES_EXTENSION, NativeOperationStatus,
};
use serde_json::{Map, Value, json};
use std::collections::BTreeSet;

use self::path_contract::{
    complete_control_plane_responses, insert_command_paths, insert_path_parameters,
    insert_process_paths, insert_sandbox_lifecycle_paths, insert_sandbox_operation_paths,
};
use self::schema_contract::{
    add_generator_safe_enum_names, insert_command_event_stream_schema, insert_generated_m1_schemas,
    retain_referenced_schemas, set_data_plane_security,
};

const OPENAPI_HTTP_METHODS: [&str; 8] = [
    "delete", "get", "head", "options", "patch", "post", "put", "trace",
];

#[cfg(test)]
mod compatibility;
mod error_examples;
mod path_contract;
mod schema_contract;
mod success_examples;

pub(crate) async fn openapi_json() -> Json<Value> {
    Json(default_openapi_value())
}

pub(crate) async fn full_openapi_json() -> Json<Value> {
    Json(public_openapi_value())
}

pub(crate) async fn m1_openapi_json() -> Json<Value> {
    Json(m1_sdk_openapi_value())
}

pub(crate) async fn llms_txt() -> impl IntoResponse {
    (
        [(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/plain; charset=utf-8"),
        )],
        "SPACE API\n\n- SPACE OpenAPI (M1 SDK operations plus the incubating operations that are not hidden): /openapi.json or /api/openapi.json\n- M1 SDK OpenAPI: /openapi-m1.json or /api/openapi-m1.json\n- Full SPACE OpenAPI, hidden operations included (requires API credentials): /openapi-full.json or /api/openapi-full.json\n- Admin OpenAPI: /admin/api/openapi.json on shared hosts\n- SPACE API base path: /v1 on api.space hosts, /api/v1 on shared hosts\n- Admin API base path: /admin/api/v1 on shared hosts\n- ECW (E2B-compatible) API base path: / on e2b.api.space hosts, /e2b/api on shared hosts\n",
    )
}

fn public_openapi_value() -> Value {
    const MAX_IDEMPOTENCY_KEY_BYTES: usize =
        pplx_space_public_api::limits::MAX_IDEMPOTENCY_KEY_BYTES;
    let idempotency_key_parameter = json!({
        "name": "Idempotency-Key",
        "in": "header",
        "required": true,
        "schema": {
            "type": "string",
            "minLength": 1,
            "maxLength": MAX_IDEMPOTENCY_KEY_BYTES,
            MAX_UTF8_BYTES_EXTENSION: MAX_IDEMPOTENCY_KEY_BYTES,
            "pattern": "^[\\t -~]+$",
            "description": format!("1 to {MAX_IDEMPOTENCY_KEY_BYTES} printable ASCII characters, spaces or tabs.")
        }
    });
    let data_plane_events_path =
        pplx_space_public_api::routes::STREAM_SESSION_EVENTS.relative_path();
    let data_plane_events_item = json!({
        "get": {
            "summary": "Stream process output or file-watch events over SSE",
            "description": "Streams output from process or file-watch sessions.\n\n### Connect\n\n- Use the session token, not your management API token. Send it in `Authorization: Bearer ...` or `Sec-WebSocket-Protocol`; query-string tokens are rejected.\n- SSE `id`s increase. Every file-watch frame after `attached` has one. On a process stream only `stdout`, `stderr` and `resync_required` frames do; other frames carry none, so the last `id` received stays the one to resume from.\n- Echo the `id` exactly as received: process stream ids are not consecutive. They stay below 2^53, so a JavaScript `Number` holds them exactly.\n\n### Reconnect\n\n- Send the last received `id` as `Last-Event-ID`. An `id` is only valid for the session whose stream sent it.\n- A process stream resumes with the output after that frame while the sandbox retains it, a bounded amount per process. Without `Last-Event-ID` it starts from the beginning of the output, which can repeat output already received.\n- When the output to resume from is no longer retained, the stream sends `resync_required` with reason `cursor_out_of_window`, then continues with the oldest output still retained (with live output on sandboxes from older templates). When `Last-Event-ID` is past the process's output, it sends the same frame and replays the output from its start. Keep output already received.\n- An `id` that does not resume exactly (one from an earlier SPACE version, one altered in transit, or one sent after the stream replayed the output from its start behind `cursor_out_of_window` or continued with live output) replays the retained output behind `resync_required` with reason `cursor_replay_unavailable`.\n- Every `id` a resumed process stream sends is above `Last-Event-ID`.\n- Process stream ids carry the output position for a process's first 2^33 - 1 output chunks (about 8.6 billion; some 99 days at 1,000 separate writes a second). After that, each resume replays the output still retained behind `resync_required`.\n- If the process exited while disconnected, reconnecting still reports `process_exited`.\n- A file-watch stream resumes live: its first identified frame is `resync_required`, with reason `cursor_replay_unavailable` and id `Last-Event-ID + 1`. Re-list the watched path before using new events.",
            "responses": {
                "200": { "description": "Server-sent event stream using DataPlaneServerFrame JSON payloads" },
                "401": { "description": "Invalid or expired data-plane token" },
                "404": { "description": "Session not found" }
            }
        }
    });

    let mut spec = json!({
        "openapi": "3.1.0",
        "info": {
            "title": "SPACE API",
            "version": env!("CARGO_PKG_VERSION"),
            "description": "### Errors and retries\n\n- Keep `x-request-id` for troubleshooting. Management errors use `PublicErrorEnvelope`, with a stable `code` and a `retryable` flag; SSE admission errors use the nested `DataPlaneHttpErrorEnvelope`.\n- Follow the returned `retryable` flag, subject to the duplicate-work precautions below. If no valid envelope is available, HTTP 429 and 5xx may be transient, but do not blindly retry mutations with ambiguous outcomes.\n- Wait for `Retry-After` when present (whole seconds). Otherwise, start with a one-second delay and double it with jitter. Stop after five attempts.\n\n### Avoid duplicate work\n\nOnly endpoints declaring a required `Idempotency-Key` deduplicate mutations. Retry those with the same method, path, body, and key. Within 24 hours, this returns the original result, except that replaying a session or command create whose session was revoked returns `conflict` with `{\"key\": \"reason\", \"value\": \"session_ended\"}` in `details` and no token. A different body returns `idempotency_conflict`; a new or expired key can start another operation. Process kill and signal do not use this mechanism: inspect ambiguous outcomes before repeating them, because a repeat can apply the action again.\n\n### Error codes\n\n- Management errors that are not retryable: `invalid_request` (400), `unauthorized` (401), `forbidden` (403), `not_found` (404), `conflict` and `idempotency_conflict` (409), `failed_precondition` (412), `quota_exceeded` (429, an organization or per-user sandbox limit).\n- Usually retryable management errors: `rate_limited` (429), `capacity_unavailable` and `service_unavailable` (503), `internal` (500). Always check the returned `retryable` flag. Data-plane mappings differ; see each response's envelope.\n- Unsupported methods (405), oversized bodies (413), wrong content types (415), and invalid request schemas (422) use `invalid_request` with `retryable: false`. Other unhandled management 5xx errors use `internal`.\n- A file route's `not_found` lists `{\"key\": \"resource\", \"value\": \"path\"}` in `details` when the sandbox is yours and the path does not exist. Without that detail, treat it as the sandbox not being found: a missing sandbox and one you cannot access answer alike.\n- A sandbox create or update that exceeds a documented input limit returns `invalid_request` listing `{\"key\": \"field\", \"value\": \"<field>\"}` in `details`, such as `metadata` or `resources.vcpu`."
        },
        "servers": [
            { "url": "/v1" },
            {
                "url": "/api/v1",
                "description": "Shared-host public API root"
            },
            {
                "url": "/v1",
                "description": "Data-plane host for WebSocket attach and proxy sessions"
            }
        ],
        "paths": {
            "/templates": {
                "get": {
                    "summary": "List templates visible to the caller",
                    "parameters": [
                        { "name": "cursor", "in": "query", "required": false, "schema": { "type": "string" } },
                        { "name": "limit", "in": "query", "required": false, "schema": { "type": "integer", "minimum": 1, "maximum": 1000 } }
                    ],
                    "responses": {
                        "200": { "description": "Template page" }
                    }
                }
            },
            "/templates/{template_name}": {
                "get": {
                    "summary": "Get one visible template by name",
                    "responses": {
                        "200": { "description": "Template" },
                        "404": { "description": "Template not found" }
                    }
                }
            },
            "/sandboxes/{sandbox_id}": {
                "patch": {
                    "summary": "Update sandbox metadata, network policy, or in-place resources",
                    "description": "Returns a sandbox operation. Metadata is replaced when provided. Network policy is applied as a sandbox override. vcpu and memory_mib are mutable in place; disk_mib is rejected because it requires recreate or restore.",
                    "parameters": [idempotency_key_parameter.clone()],
                    "responses": {
                        "202": { "description": "Sandbox operation" },
                        "400": { "description": "Invalid sandbox id, or unsupported update field" },
                        "404": { "description": "Sandbox not found" },
                        "409": { "description": "Idempotency conflict" }
                    }
                }
            },
            "/sandboxes/{sandbox_id}/terminal-sessions": {
                "post": {
                    "summary": "Create a terminal session",
                    "parameters": [idempotency_key_parameter.clone()],
                    "responses": {
                        "201": { "description": "Terminal session with attach token" },
                        "409": { "description": "Idempotency conflict, or a replayed create whose session was revoked" }
                    }
                }
            },
            "/sandboxes/{sandbox_id}/process-sessions": {
                "post": {
                    "summary": "Create a process session",
                    "description": "A sandbox that is not running is handled as `createCommand` handles it: woken when its `auto_lifecycle.wake.on` includes `command` or `access`, otherwise refused with a `sandbox_not_running` conflict.",
                    "parameters": [idempotency_key_parameter.clone()],
                    "responses": {
                        "201": { "description": "Process session with attach token" },
                        "409": { "description": "Idempotency conflict, a replayed create whose session was revoked, or the sandbox is not running and this process cannot wake it" }
                    }
                }
            },
            "/sandboxes/{sandbox_id}/file-watch-sessions": {
                "post": {
                    "summary": "Create a file watch session",
                    "parameters": [idempotency_key_parameter.clone()],
                    "responses": {
                        "201": { "description": "File watch session with attach token" },
                        "409": { "description": "Idempotency conflict, or a replayed create whose session was revoked" }
                    }
                }
            },
            "/sandboxes/{sandbox_id}/port-sessions": {
                "post": {
                    "summary": "Create a port session",
                    "parameters": [idempotency_key_parameter.clone()],
                    "responses": {
                        "201": { "description": "Port session with scoped attach token" },
                        "409": { "description": "Idempotency conflict, or a replayed create whose session was revoked" }
                    }
                }
            },
            "/sandboxes/{sandbox_id}/protocol-sessions": {
                "post": {
                    "summary": "Create a protocol session bound to a sandbox-local service port",
                    "parameters": [idempotency_key_parameter.clone()],
                    "responses": {
                        "201": { "description": "Protocol proxy session with scoped attach token" },
                        "409": { "description": "Idempotency conflict, or a replayed create whose session was revoked" }
                    }
                }
            },
            "/sandboxes/{sandbox_id}/files:stat": {
                "post": {
                    "summary": "Stat a guest file path",
                    "responses": {
                        "200": { "description": "File metadata" },
                        "404": { "description": "Sandbox or file not found" }
                    }
                }
            },
            "/sandboxes/{sandbox_id}/files:list": {
                "post": {
                    "summary": "List guest file paths",
                    "responses": {
                        "200": { "description": "File metadata list" },
                        "404": { "description": "Sandbox or path not found" }
                    }
                }
            },
            "/sandboxes/{sandbox_id}/files:read": {
                "post": {
                    "summary": "Read bytes from a guest file path",
                    "description": "Requires an explicit length no larger than the control-plane API read cap. Use data-plane transfer surfaces for larger reads.",
                    "responses": {
                        "200": {
                            "description": "File byte stream",
                            "content": { "application/octet-stream": {} }
                        },
                        "400": { "description": "Invalid sandbox id, or missing or oversized length" },
                        "404": { "description": "Sandbox or file not found" }
                    }
                }
            },
            "/sandboxes/{sandbox_id}/sessions": {
                "get": {
                    "summary": "List data-plane sessions for a sandbox",
                    "parameters": [
                        { "name": "cursor", "in": "query", "required": false, "schema": { "type": "string" } },
                        { "name": "limit", "in": "query", "required": false, "schema": { "type": "integer", "minimum": 1, "maximum": 1000 } },
                        { "name": "kind", "in": "query", "required": false, "schema": { "type": "string", "enum": ["terminal", "process", "file_watch", "port", "protocol"] } }
                    ],
                    "responses": {
                        "200": { "description": "Session page" },
                        "404": { "description": "Sandbox not found" }
                    }
                }
            },
            "/sandboxes/{sandbox_id}/sessions/{session_id}": {
                "get": {
                    "summary": "Get a data-plane session",
                    "responses": {
                        "200": { "description": "Session" },
                        "404": { "description": "Session not found" }
                    }
                },
                "delete": {
                    "summary": "Revoke a data-plane session",
                    "responses": {
                        "200": { "description": "Revoked session" },
                        "404": { "description": "Session not found" }
                    }
                }
            },
            "/sandboxes/{sandbox_id}/lifecycle-events": {
                "get": {
                    "summary": "Stream sandbox lifecycle operation events over Server-Sent Events",
                    "responses": {
                        "200": { "description": "Sandbox lifecycle event stream", "content": { "text/event-stream": {} } },
                        "404": { "description": "Sandbox not found" }
                    }
                }
            },
            "/operations/{operation_id}/events": {
                "get": {
                    "summary": "List or stream operation events",
                    "responses": {
                        "200": {
                            "description": "Operation event page or Server-Sent Events stream",
                            "content": {
                                "application/json": {},
                                "text/event-stream": {}
                            }
                        },
                        "404": { "description": "Operation not found" }
                    }
                }
            },
            "/sessions/{session_id}/attach": {
                "get": {
                    "summary": "Attach to a terminal, process, or file-watch session over WebSocket",
                    "description": "Send the scoped data-plane token using `Sec-WebSocket-Protocol` or a first `attach` frame. Query-string tokens are rejected. Native terminal and process sessions permit one attached interactive writer per session; accepted stdin frames are acknowledged with `input_accepted`, while `signal`, `close_stdin`, and `heartbeat` frames are acknowledged with `control_accepted`. The `close` frame closes the stream only. A process the sandbox cannot run because it is not running and may not be woken ends with a non-retryable `error` frame, code `backend_stream_error`, whose `details` match createCommand's `sandbox_not_running` conflict; SSE answers the same refusal as a 409 `conflict`, so branch on `details` `reason`. A `session_not_ready` frame is retryable: something is about to give the sandbox a node, including the wake an attach queues for a sandbox at rest whose `auto_lifecycle.wake.on` allows the attach: `command` or `access` for the first attach of a process or terminal, which starts it, and `access` for a reattach to one that already started or a file watch. When nothing is, the attach ends with that same non-retryable `backend_stream_error` frame.",
                    "responses": {
                        "101": { "description": "WebSocket stream using DataPlaneClientFrame and DataPlaneServerFrame" },
                        "401": { "description": "Invalid or expired data-plane token" },
                        "409": { "description": "Native interactive session is already attached" }
                    }
                }
            },
            "/sessions/{session_id}/proxy": {
                "get": {
                    "summary": "Proxy a port or protocol session",
                    "description": "Transparent proxy to a sandbox-local service. Documented as `get` because a contract needs one method, but every HTTP method is forwarded, and any sub-path below this route is forwarded verbatim. Neither the method nor the sub-path is part of the SPACE contract; the target service defines them. Send the scoped data-plane token using `Authorization: Bearer ...` or `Sec-WebSocket-Protocol`; query-string tokens are rejected.",
                    "responses": {
                        "200": { "description": "Proxied response" },
                        "101": { "description": "Proxied WebSocket upgrade" },
                        "401": { "description": "Invalid or expired data-plane token" }
                    }
                }
            }
        },
        "components": {
            "securitySchemes": {
                "apiKey": {
                    "type": "http",
                    "scheme": "bearer"
                }
            },
            "headers": {
                "RequestId": {
                    "description": "Stable request id for support and retry diagnostics. Echoes the first non-blank of `x-request-id` and `x-correlation-id` when it is at most 128 bytes of `[A-Za-z0-9._:-]`; otherwise, including when the supplied value is longer or contains other characters, SPACE issues a fresh `req_<uuid>`.",
                    "schema": { "type": "string" }
                }
            },
            "schemas": {
                "DataPlaneClientFrame": {
                    "oneOf": [
                        { "type": "object", "properties": { "type": { "const": "attach" }, "token": { "type": "string" } }, "required": ["type", "token"] },
                        { "type": "object", "properties": { "type": { "const": "input" }, "seq": { "type": "integer", "minimum": 0 }, "data_base64": { "type": "string" } }, "required": ["type", "seq", "data_base64"] },
                        { "type": "object", "properties": { "type": { "const": "resize" }, "cols": { "type": "integer", "minimum": 1 }, "rows": { "type": "integer", "minimum": 1 } }, "required": ["type", "cols", "rows"] },
                        { "type": "object", "properties": { "type": { "const": "signal" }, "seq": { "type": "integer", "minimum": 0 }, "signal": { "type": "integer" } }, "required": ["type", "seq", "signal"] },
                        { "type": "object", "properties": { "type": { "const": "close_stdin" }, "seq": { "type": "integer", "minimum": 0 } }, "required": ["type", "seq"] },
                        { "type": "object", "properties": { "type": { "const": "heartbeat" }, "seq": { "type": "integer", "minimum": 0 } }, "required": ["type", "seq"] },
                        { "type": "object", "properties": { "type": { "const": "close" } }, "required": ["type"] }
                    ]
                }
            }
        }
    });
    spec.get_mut("paths")
        .and_then(Value::as_object_mut)
        .expect("OpenAPI paths should be an object")
        .insert(data_plane_events_path.to_string(), data_plane_events_item);

    let spec_object = spec
        .as_object_mut()
        .expect("OpenAPI document should be an object");
    spec_object.insert(
        "jsonSchemaDialect".to_string(),
        json!("https://json-schema.org/draft/2020-12/schema"),
    );
    spec_object.insert("security".to_string(), json!([{ "apiKey": [] }]));
    let security_schemes = spec_object
        .get_mut("components")
        .and_then(|components| components.get_mut("securitySchemes"))
        .and_then(Value::as_object_mut)
        .expect("security schemes should be an object");
    security_schemes.insert(
        "dataPlaneToken".to_string(),
        json!({
            "type": "http",
            "scheme": "bearer",
            "description": "Scoped data-plane token returned in a session or command handle."
        }),
    );
    security_schemes.insert(
        "dataPlaneWebSocketProtocol".to_string(),
        json!({
            "type": "apiKey",
            "in": "header",
            "name": "Sec-WebSocket-Protocol",
            "description": "A scoped data-plane token may be carried as a WebSocket subprotocol."
        }),
    );

    insert_generated_m1_schemas(&mut spec);
    insert_command_event_stream_schema(&mut spec);
    set_data_plane_security(&mut spec);
    let paths = spec
        .get_mut("paths")
        .and_then(Value::as_object_mut)
        .expect("paths should be an object");
    insert_command_paths(paths, &idempotency_key_parameter);
    insert_sandbox_operation_paths(paths, &idempotency_key_parameter);
    insert_sandbox_extra_paths(paths);
    insert_sandbox_lifecycle_paths(paths, &idempotency_key_parameter);
    insert_process_paths(paths);
    insert_snapshot_paths(paths);
    insert_port_paths(paths, &idempotency_key_parameter);
    insert_file_mutation_paths(paths, &idempotency_key_parameter);
    insert_path_parameters(&mut spec);
    complete_control_plane_responses(&mut spec);
    add_generator_safe_enum_names(&mut spec);
    apply_native_operation_manifest(&mut spec);
    error_examples::insert_error_examples(&mut spec);
    success_examples::insert_success_examples(&mut spec);
    spec
}

fn apply_native_operation_manifest(spec: &mut Value) {
    use pplx_space_public_api::routes::NATIVE_V1_OPERATIONS;

    let paths = spec
        .get_mut("paths")
        .and_then(Value::as_object_mut)
        .expect("paths should be an object");
    let mut classified = BTreeSet::new();
    for operation in NATIVE_V1_OPERATIONS {
        let path = operation.relative_path();
        let method = operation.method().openapi_name();
        assert!(
            classified.insert((path, method)),
            "duplicate native operation manifest entry: {method} {path}"
        );
        let contract = paths
            .get_mut(path)
            .and_then(Value::as_object_mut)
            .and_then(|item| item.get_mut(method))
            .and_then(Value::as_object_mut)
            .expect("manifest operation should exist in the native OpenAPI contract");
        contract.insert("operationId".to_string(), json!(operation.operation_id()));
        contract.insert(
            M1_STATUS_EXTENSION.to_string(),
            json!(operation.status().as_str()),
        );
    }

    let documented = paths
        .iter()
        .flat_map(|(path, item)| {
            item.as_object()
                .expect("path item should be an object")
                .keys()
                .filter(|method| OPENAPI_HTTP_METHODS.contains(&method.as_str()))
                .map(move |method| (path.as_str(), method.as_str()))
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(
        documented, classified,
        "every native OpenAPI operation must have an explicit manifest classification"
    );
}

pub(crate) fn m1_sdk_openapi_value() -> Value {
    operations_openapi_value(|status| status.is_m1_supported())
}

/// The document `/openapi.json` serves: every operation except those hidden
/// by default, which `/openapi-full.json` still documents.
pub(crate) fn default_openapi_value() -> Value {
    operations_openapi_value(|status| !status.is_hidden_by_default())
}

/// The native contract narrowed to the operations whose status `keep` accepts.
fn operations_openapi_value(keep: impl Fn(NativeOperationStatus) -> bool) -> Value {
    use pplx_space_public_api::routes::NATIVE_V1_OPERATIONS;

    let mut spec = public_openapi_value();
    let supported = NATIVE_V1_OPERATIONS
        .iter()
        .filter(|operation| keep(operation.status()))
        .map(|operation| (operation.relative_path(), operation.method().openapi_name()))
        .collect::<BTreeSet<_>>();
    spec.get_mut("paths")
        .and_then(Value::as_object_mut)
        .expect("paths should be an object")
        .retain(|path, item| {
            let operations = item.as_object_mut().expect("path item should be an object");
            operations.retain(|method, _| {
                method == "parameters" || supported.contains(&(path.as_str(), method.as_str()))
            });
            operations
                .keys()
                .any(|method| OPENAPI_HTTP_METHODS.contains(&method.as_str()))
        });
    retain_referenced_schemas(&mut spec);
    spec
}

fn insert_sandbox_extra_paths(paths: &mut Map<String, Value>) {
    paths.insert(
        "/sandboxes/{sandbox_id}/stats".to_string(),
        json!({
            "get": {
                "summary": "Get bounded live sandbox resource and network counters",
                "description": "Stats are read through the node runtime path and may fail while the sandbox is migrating or the node is unavailable.",
                "responses": {
                    "200": { "description": "Sandbox stats" },
                    "404": { "description": "Sandbox not found" },
                    "409": { "description": "Sandbox is not currently reachable for stats" }
                }
            }
        }),
    );
    paths.insert(
        "/sandboxes/{sandbox_id}/metrics-stream".to_string(),
        json!({
            "get": {
                "summary": "Stream live sandbox stats over Server-Sent Events",
                "responses": {
                    "200": { "description": "Sandbox metrics event stream", "content": { "text/event-stream": {} } },
                    "404": { "description": "Sandbox not found" },
                    "503": { "description": "Metrics stream unavailable" }
                }
            }
        }),
    );
    paths.insert(
        "/sandboxes/{sandbox_id}/network-policy".to_string(),
        json!({
            "get": {
                "summary": "Get the effective sandbox network policy",
                "responses": {
                    "200": { "description": "Effective network policy" },
                    "404": { "description": "Sandbox not found" }
                }
            }
        }),
    );
    paths.insert(
        "/sandboxes/{sandbox_id}/lifecycle-events".to_string(),
        json!({
            "get": {
                "summary": "Stream sandbox lifecycle operation events over Server-Sent Events",
                "responses": {
                    "200": { "description": "Sandbox lifecycle event stream", "content": { "text/event-stream": {} } },
                    "404": { "description": "Sandbox not found" }
                }
            }
        }),
    );
}

fn insert_port_paths(paths: &mut Map<String, Value>, idempotency_key_parameter: &Value) {
    paths.insert(
        "/sandboxes/{sandbox_id}/ports".to_string(),
        json!({
            "get": {
                "summary": "List durable sandbox port exposure intents",
                "responses": {
                    "200": { "description": "Port exposure list" },
                    "404": { "description": "Sandbox not found" }
                }
            }
        }),
    );
    paths.insert(
        "/sandboxes/{sandbox_id}/ports/{port}/open".to_string(),
        json!({
            "post": {
                "summary": "Mark a sandbox port exposure desired-open",
                "description": "Persists desired state and enqueues node reconciliation; low-level host networking is not created synchronously on the API request path.",
                "parameters": [idempotency_key_parameter.clone()],
                "responses": {
                    "200": { "description": "Port exposure intent" },
                    "404": { "description": "Sandbox not found" }
                }
            }
        }),
    );
    paths.insert(
        "/sandboxes/{sandbox_id}/ports/{port}/close".to_string(),
        json!({
            "post": {
                "summary": "Mark a sandbox port exposure desired-closed",
                "parameters": [idempotency_key_parameter.clone()],
                "responses": {
                    "200": { "description": "Port exposure intent" },
                    "404": { "description": "Sandbox not found" }
                }
            }
        }),
    );
}

fn insert_snapshot_paths(paths: &mut Map<String, Value>) {
    paths.insert(
        "/sandboxes/{sandbox_id}/snapshots".to_string(),
        json!({
            "get": {
                "summary": "List snapshots for a sandbox",
                "parameters": [
                    { "name": "cursor", "in": "query", "required": false, "schema": { "type": "string" } },
                    { "name": "limit", "in": "query", "required": false, "schema": { "type": "integer", "minimum": 1, "maximum": 1000 } }
                ],
                "responses": {
                    "200": { "description": "Snapshot page" },
                    "404": { "description": "Sandbox not found" }
                }
            }
        }),
    );
    paths.insert(
        "/sandboxes/{sandbox_id}/snapshots/{snapshot_id}".to_string(),
        json!({
            "get": {
                "summary": "Get one sandbox snapshot",
                "responses": {
                    "200": { "description": "Snapshot" },
                    "404": { "description": "Snapshot not found" }
                }
            }
        }),
    );
}

/// How `files:write`, `files:write_batch` and `files:rename` treat their
/// `Idempotency-Key`; the sandbox holds the result it replays.
fn file_key_replayed() -> String {
    format!(
        "The `Idempotency-Key` is bound to this request in this sandbox until it has gone unused for {} minutes: the same key with any other path, data or field returns 409 `idempotency_conflict` and changes nothing. A retry of the same request returns the original result if that request completed within the last minute, and otherwise runs again.",
        crate::public_operations::KEY_BINDING_TTL_MINUTES
    )
}

/// How `files:mkdir` and `files:remove` treat their `Idempotency-Key`.
fn file_key_rerun() -> String {
    format!(
        "The `Idempotency-Key` is bound to this request in this sandbox until it has gone unused for {} minutes: the same key with any other path or field returns 409 `idempotency_conflict` and changes nothing. A retry of the same request runs again, which leaves the same result.",
        crate::public_operations::KEY_BINDING_TTL_MINUTES
    )
}

fn insert_file_mutation_paths(paths: &mut Map<String, Value>, idempotency_key_parameter: &Value) {
    paths.insert(
        "/sandboxes/{sandbox_id}/files:write".to_string(),
        json!({
            "post": {
                "summary": "Write bounded bytes to a guest file path",
                "description": format!(
                    "`data_base64` carries at most 16 MiB of file content. The request body limit covers that payload's base64 and the other fields, plus room for encoders that escape `/` as `\\/` at up to twice the rate of uniformly distributed data. A payload near 16 MiB whose base64 is mostly `/` (long runs of 0xff bytes) can exceed the limit when escaped; send it without escaping `/`, or split it across writes with `append`. Creating a file, or overwriting a regular file that the writing user owns with no other hard links and no access ACL, replaces the destination with a temporary file where it can, which publishes the new content atomically: readers see the previous content (or no file) or the new, never a partial file. A destination that cannot be renamed over (such as a bind mount), a directory that refuses the temporary file, appends and other files are written in place, without that guarantee, and synced before the write is acknowledged. Acknowledgement does not guarantee that a created or overwritten file persists across a sandbox crash, a forced stop or a host failure, and a writeback error can surface after it. An ordinary stop attempts to flush the sandbox's root filesystem first, but proceeds if that attempt fails or times out. No persistence deadline is promised. With `parents` set, missing parent directories of `path` are created first, as `files:mkdir` creates them for the same `user`; without it a missing parent returns 404. The request body is limited to {} bytes, counting any JSON escaping. {}",
                    super::public::WRITE_FILE_BODY_LIMIT_BYTES,
                    file_key_replayed()
                ),
                "parameters": [idempotency_key_parameter.clone()],
                "responses": {
                    "200": { "description": "Write result" },
                    "400": { "description": "Invalid sandbox id, or invalid file write request" },
                    "404": { "description": "Sandbox or parent path not found" },
                    "409": { "description": "Idempotency conflict" },
                    "412": { "description": "The sandbox's disk or quota is full; free space before retrying" },
                    "413": { "description": "The request body exceeds the write limit" }
                }
            }
        }),
    );
    paths.insert(
        "/sandboxes/{sandbox_id}/files:write_batch".to_string(),
        json!({
            "post": {
                "summary": "Write a bounded batch of guest files",
                "description": format!(
                    "A batch holds at most {} entries, and their decoded data and paths together may total at most {} bytes (3 MiB), because the batch travels to the sandbox as one gRPC message of at most {} bytes. A larger batch gets 400. With `parents` set, every entry's missing parent directories are created first, as `files:mkdir` creates them for the same `user`; without it a missing parent returns 404. The request body is limited to {} bytes, counting any JSON escaping. Use `files:write` for larger files. {}",
                    pplx_space_common_core::spaced::MAX_WRITE_BATCH_ENTRIES,
                    super::public::MAX_FILE_WRITE_BATCH_BYTES,
                    pplx_space_common_core::spaced::MAX_MESSAGE_BYTES,
                    super::public::WRITE_FILE_BATCH_BODY_LIMIT_BYTES,
                    file_key_replayed()
                ),
                "parameters": [idempotency_key_parameter.clone()],
                "responses": {
                    "200": { "description": "Batch write result" },
                    "400": { "description": "Invalid sandbox id, or invalid file write batch request, including a batch over its data and path limit" },
                    "404": { "description": "Sandbox or parent path not found" },
                    "409": { "description": "Idempotency conflict" },
                    "413": { "description": "The request body exceeds the batch limit" }
                }
            }
        }),
    );
    paths.insert(
        "/sandboxes/{sandbox_id}/files:changes_enable".to_string(),
        json!({
            "post": {
                "summary": "Enable guest file change tracking",
                "parameters": [idempotency_key_parameter.clone()],
                "responses": {
                    "200": { "description": "Tracking status" },
                    "400": { "description": "Invalid sandbox id, or invalid change-tracking request" },
                    "404": { "description": "Sandbox not found" }
                }
            }
        }),
    );
    paths.insert(
        "/sandboxes/{sandbox_id}/files:changes_disable".to_string(),
        json!({
            "post": {
                "summary": "Disable guest file change tracking",
                "parameters": [idempotency_key_parameter.clone()],
                "responses": {
                    "200": { "description": "Tracking disabled" },
                    "404": { "description": "Sandbox not found" }
                }
            }
        }),
    );
    paths.insert(
        "/sandboxes/{sandbox_id}/files:modified".to_string(),
        json!({
            "post": {
                "summary": "List files modified within a retained tracking window",
                "description": "`window_seconds`, `since`, and `cursor` are mutually exclusive filters.",
                "responses": {
                    "200": { "description": "Modified file list" },
                    "400": { "description": "Invalid sandbox id, or invalid modified-file filter" },
                    "404": { "description": "Sandbox not found" }
                }
            }
        }),
    );
    paths.insert(
        "/sandboxes/{sandbox_id}/files:mkdir".to_string(),
        json!({
            "post": {
                "summary": "Create a guest directory idempotently",
                "description": file_key_rerun(),
                "parameters": [idempotency_key_parameter.clone()],
                "responses": {
                    "200": { "description": "Directory created or already present" },
                    "400": { "description": "Invalid sandbox id, or a non-idempotent mkdir mode" },
                    "404": { "description": "Sandbox not found" },
                    "409": { "description": "Idempotency conflict" }
                }
            }
        }),
    );
    paths.insert(
        "/sandboxes/{sandbox_id}/files:remove".to_string(),
        json!({
            "post": {
                "summary": "Remove a guest file path idempotently",
                "description": file_key_rerun(),
                "parameters": [idempotency_key_parameter.clone()],
                "responses": {
                    "200": { "description": "Path removed or already absent" },
                    "400": { "description": "Invalid sandbox id, or a non-idempotent remove mode" },
                    "404": { "description": "Sandbox not found" },
                    "409": { "description": "Idempotency conflict" }
                }
            }
        }),
    );
    paths.insert(
        "/sandboxes/{sandbox_id}/files:rename".to_string(),
        json!({
            "post": {
                "summary": "Rename a guest file path",
                "description": file_key_replayed(),
                "parameters": [idempotency_key_parameter.clone()],
                "responses": {
                    "200": { "description": "Rename result" },
                    "404": { "description": "Sandbox or source path not found" },
                    "409": { "description": "Rename conflict, or idempotency conflict" }
                }
            }
        }),
    );
}

#[cfg(test)]
mod tests;
