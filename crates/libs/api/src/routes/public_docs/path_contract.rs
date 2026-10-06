use serde_json::{Map, Value, json};

use super::OPENAPI_HTTP_METHODS;
use pplx_space_public_api::{
    OperationId, PublicSandboxId, PublicSnapshotId, SandboxLifecycleAction, SessionKind,
    routes::{
        CHECKPOINT_SANDBOX, CREATE_COMMAND, CREATE_SANDBOX, DELETE_SANDBOX,
        DISABLE_SANDBOX_SNAPSHOTS, ENABLE_SANDBOX_SNAPSHOTS, GET_OPERATION, GET_PROCESS,
        GET_SANDBOX, KILL_PROCESS, KILL_SANDBOX, LIST_PROCESSES, LIST_SANDBOXES, NativeOperation,
        PAUSE_SANDBOX, RESTORE_SANDBOX, RESUME_SANDBOX, ROLLBACK_SANDBOX, SIGNAL_PROCESS,
        START_SANDBOX, STOP_SANDBOX, SUSPEND_SANDBOX, WAKE_SANDBOX,
    },
};

const UUID_PATTERN: &str =
    "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}";

fn insert_credential_reference_paths(paths: &mut Map<String, Value>) {
    use pplx_space_public_api::routes;
    for (operation, summary, schema) in [
        (
            routes::REGISTER_CREDENTIAL_REFERENCE,
            "Register an authorized reference without fetching its value",
            "CredentialReference",
        ),
        (
            routes::DELETE_CREDENTIAL_REFERENCE,
            "Retire an unused reference; preserve its tombstone",
            "",
        ),
    ] {
        let mut value = json!({
            "summary": summary,
            "description": "Experimental, organization-scoped metadata only. Registration neither fetches values nor grants provider access. Only configured AWS Secrets Manager current-value locators are supported; unknown providers, options and raw secret fields are rejected. Source changes and retirement of attached references are not supported.",
            "responses": {
                "400":{"description":"Invalid identifier or locator"},
                "401":{"description":"Authentication required"},
                "403":{"description":"Provider reference is unauthorized"},
                "404":{"description":"Resource not found in the authenticated organization"},
                "409":{"description":"Idempotency conflict"},
                "412":{"description":"Reference remains attached"},
                "422":{"description":"Request contains unknown fields or unsupported locator variants"},
                "500":{"description":"Metadata operation failed"},
                "503":{"description":"Database contention; retry"}
            }
        });
        let status = if operation == routes::REGISTER_CREDENTIAL_REFERENCE {
            "201"
        } else if schema.is_empty() {
            "204"
        } else {
            "200"
        };
        let response = if schema.is_empty() {
            json!({"description":"Reference retired; external secret unchanged"})
        } else {
            json!({"description":"Metadata result", "content":{"application/json":{"schema":{"$ref":format!("#/components/schemas/{schema}")}}}})
        };
        let object = value.as_object_mut().expect("operation is an object");
        object
            .get_mut("responses")
            .and_then(Value::as_object_mut)
            .expect("operation has response map")
            .insert(status.into(), response);
        if operation == routes::REGISTER_CREDENTIAL_REFERENCE {
            object.insert("requestBody".into(), json!({"required":true,"content":{"application/json":{"schema":{"$ref":"#/components/schemas/RegisterCredentialReferenceRequest"}}}}));
            object.insert("description".into(), json!(
                "Register an immutable reference with a stable UUIDv7. request_id is durable and organization-scoped; identical retries return the original ID even after retirement, and reuse with different intent conflicts. Equal locators never implicitly merge references. Registration only checks configured provider scope and never fetches credential values. Unsupported providers and locator options fail explicitly."
            ));
        }
        insert_public_route(paths, operation, value);
    }
}

pub(super) fn insert_command_paths(
    paths: &mut Map<String, Value>,
    idempotency_key_parameter: &Value,
) {
    insert_public_route(
        paths,
        CREATE_COMMAND,
        json!({
                "summary": "Create command",
                "description": "Returns a command handle and a session token. Creating the handle does not start the process.\n\n- Attach to `/sessions/{session_id}/events` with the returned session token, not your management API token. Attaching to the stream starts execution.\n- Read stdout, stderr, and the exit status from that stream.\n- Interactive input and terminals use a separate WebSocket API, outside this reference.\n\n### Example request\n\n```json\n{\"command\":[\"sh\",\"-c\",\"printf 'Hello from SPACE\\\\n'\"],\"timeout\":\"30s\"}\n```",
                "parameters": [idempotency_key_parameter.clone()],
                "requestBody": {
                    "required": true,
                    "content": {
                        "application/json": {
                            "schema": { "$ref": "#/components/schemas/CreateCommandRequest" }
                        }
                    }
                },
                "responses": {
                    "201": {
                        "description": "Command handle with event stream token",
                        "content": {
                            "application/json": {
                                "schema": { "$ref": "#/components/schemas/PublicCommand" }
                            }
                        }
                    },
                    "400": { "description": "Invalid sandbox id, idempotency key, or command request" },
                    "401": { "description": "Authentication required" },
                    "404": { "description": "Sandbox not found" },
                    "409": { "description": "Idempotency conflict" },
                    "413": { "description": "Request body is too large" },
                    "415": { "description": "Content-Type must be application/json" },
                    "422": { "description": "JSON body does not match CreateCommandRequest" },
                    "500": { "description": "Command session could not be persisted" }
                }
        }),
    );
}

pub(super) fn insert_sandbox_operation_paths(
    paths: &mut Map<String, Value>,
    idempotency_key_parameter: &Value,
) {
    insert_public_route(
        paths,
        pplx_space_public_api::routes::SET_SANDBOX_TIMEOUT,
        json!({
            "summary": "Set the sandbox idle timeout",
            "description": "Sets the idle timeout and restarts the idle clock from now. Accepts only a single access-idle delete or suspend rule that can be preserved by the timeout controller; custom wake configurations are rejected. The action and wake settings are preserved. Timeout must resolve to positive whole seconds, at most 4294967295 seconds (for example, 1.5m becomes 90s). Paused or suspended sandboxes persist the timeout without waking; it takes effect once running again, and expires_at reports the stored deadline, which may be null. For a running sandbox, expires_at is a conservative estimate captured before the node call, assuming node and API clocks agree within five seconds, clamped to the observation time; getSandbox reports the published deadline once available. Retry with the same Idempotency-Key to receive the original response without restarting the clock again.",
            "parameters": [idempotency_key_parameter.clone()],
            "requestBody": {"required":true,"content":{"application/json":{"schema":{"$ref":"#/components/schemas/SetSandboxTimeoutRequest"}}}},
            "responses": {
                "200":{"description":"Idle timeout updated","content":{"application/json":{"schema":{"$ref":"#/components/schemas/SetSandboxTimeoutResponse"}}}},
                "400":{"description":"Invalid timeout, sandbox identifier or idempotency key"},
                "401":{"description":"Authentication required"},
                "404":{"description":"Sandbox not found in your organization"},
                "409":{"description":"Idempotency conflict"},
                "412":{"description":"Terminal sandbox, unsupported policy or execution changed"},
                "500":{"description":"Timeout update failed internally"},
                "503":{"description":"Deadline update could not be acknowledged; retry"}
            }
        }),
    );
    insert_credential_reference_paths(paths);
    insert_public_route(
        paths,
        CREATE_SANDBOX,
        json!({
            "summary": "Create a sandbox",
            "description": "Returns an operation handle while the sandbox is being created.\n\n### Wait for readiness\n\nPoll the operation to detect completion. `GET /sandboxes/{sandbox_id}` can return 404 while creation is pending.\n\n### Idle timeout\n\nIf `auto_lifecycle` is omitted or null, the sandbox suspends after six idle hours. Access and command activity reset the idle clock. An explicit policy, including a disabled one, is preserved.\n\n### Example request\n\nRequires the published `seed:latest` template:\n\n```json\n{\"template\":\"seed:latest\",\"runtime\":\"fc\"}\n```",
            "parameters": [idempotency_key_parameter.clone()],
            "requestBody": {
                "required": true,
                "content": {
                    "application/json": {
                        "schema": { "$ref": "#/components/schemas/CreateSandboxRequest" }
                    }
                }
            },
            "responses": {
                "202": {
                    "description": "Sandbox creation operation accepted",
                    "content": {
                        "application/json": {
                            "schema": { "$ref": "#/components/schemas/SandboxOperationResponse" }
                        }
                    }
                },
                "400": { "description": "Invalid idempotency key or sandbox request" },
                "401": { "description": "Authentication required" },
                "409": { "description": "Idempotency conflict" },
                "413": { "description": "Request body is too large" },
                "415": { "description": "Content-Type must be application/json" },
                "422": { "description": "JSON body does not match CreateSandboxRequest" },
                "500": { "description": "Sandbox operation could not be persisted" },
                "503": { "description": "Sandbox operation is temporarily unavailable" }
            }
        }),
    );
    insert_public_route(
        paths,
        LIST_SANDBOXES,
        json!({
            "summary": "List sandboxes",
            "description": "Returns sandboxes ordered by creation time and ID, newest first.",
            "parameters": [
                {
                    "name": "cursor",
                    "in": "query",
                    "required": false,
                    "schema": {
                        "type": "string",
                        "maxLength": 4096,
                        "pattern": "^pc1_[A-Za-z0-9_-]+\\.[A-Za-z0-9_-]+$"
                    }
                },
                {
                    "name": "limit",
                    "in": "query",
                    "required": false,
                    "description": "Maximum number of sandboxes. Omitted or zero uses the server default of 100; values above 1000 are capped at 1000.",
                    "schema": { "type": "integer", "minimum": 0, "maximum": 65535 }
                }
            ],
            "responses": {
                "200": {
                    "description": "Sandbox page",
                    "content": {
                        "application/json": {
                            "schema": { "$ref": "#/components/schemas/PublicSandboxPage" }
                        }
                    }
                },
                "400": { "description": "Invalid sandbox cursor" },
                "401": { "description": "Authentication required" },
                "500": { "description": "Sandbox listing failed internally" }
            }
        }),
    );
    insert_public_route(
        paths,
        GET_SANDBOX,
        json!({
            "summary": "Get a sandbox",
            "description": "Returns the sandbox's current lifecycle state, conditions, template, stored resource requests, and metadata. This is not an operation result or command exit status. Deleted sandboxes and sandboxes outside your organization return not_found. After an asynchronous action, poll its operation and inspect this resource before starting dependent work.",
            "responses": {
                "200": {
                    "description": "Sandbox",
                    "content": {
                        "application/json": {
                            "schema": { "$ref": "#/components/schemas/PublicSandbox" }
                        }
                    }
                },
                "400": { "description": "Invalid sandbox id" },
                "401": { "description": "Authentication required" },
                "404": { "description": "Sandbox not found" },
                "500": { "description": "Sandbox lookup failed internally" }
            }
        }),
    );
    insert_public_route(
        paths,
        DELETE_SANDBOX,
        json!({
            "summary": "Delete a sandbox",
            "description": "Returns an operation handle while the sandbox is being deleted. Poll the operation for completion. Reusing the same idempotency key for the same sandbox returns the original operation.",
            "parameters": [idempotency_key_parameter.clone()],
            "responses": {
                "202": {
                    "description": "Sandbox deletion operation accepted",
                    "content": {
                        "application/json": {
                            "schema": { "$ref": "#/components/schemas/SandboxOperationResponse" }
                        }
                    }
                },
                "400": { "description": "Invalid sandbox id or idempotency key" },
                "401": { "description": "Authentication required" },
                "404": { "description": "Sandbox not found" },
                "409": { "description": "Idempotency conflict" },
                "500": { "description": "Sandbox deletion operation could not be persisted" },
                "503": { "description": "Sandbox operation is temporarily unavailable" }
            }
        }),
    );
    insert_public_route(
        paths,
        GET_OPERATION,
        json!({
            "summary": "Get an operation",
            "description": "Returns the current state and result of an asynchronous operation owned by the authenticated organization.",
            "responses": {
                "200": {
                    "description": "Operation",
                    "content": {
                        "application/json": {
                            "schema": { "$ref": "#/components/schemas/PublicOperationRecord" }
                        }
                    }
                },
                "400": { "description": "Invalid operation id" },
                "401": { "description": "Authentication required" },
                "404": { "description": "Operation not found" },
                "500": { "description": "Operation lookup failed internally" }
            }
        }),
    );
}

pub(super) fn insert_process_paths(paths: &mut Map<String, Value>) {
    insert_public_route(
        paths,
        LIST_PROCESSES,
        json!({
                "summary": "List running processes",
                "description": "Returns running processes ordered by process ID, not completed-command history. To start a process and collect its output, create a command and attach to its event stream.",
                "parameters": [
                    {
                        "name": "cursor",
                        "in": "query",
                        "required": false,
                        "schema": {
                            "type": "string",
                            "maxLength": 4096,
                            "pattern": "^pc1_[A-Za-z0-9_-]+\\.[A-Za-z0-9_-]+$"
                        }
                    },
                    {
                        "name": "limit",
                        "in": "query",
                        "required": false,
                        "description": "Maximum number of processes. Omitted or zero uses the server default of 100; values above 1000 are capped at 1000.",
                        "schema": { "type": "integer", "minimum": 0, "maximum": 65535 }
                    }
                ],
                "responses": {
                    "200": {
                        "description": "Process page",
                        "content": {
                            "application/json": {
                                "schema": { "$ref": "#/components/schemas/PublicProcessPage" }
                            }
                        }
                    },
                    "400": { "description": "Invalid sandbox id or cursor" },
                    "401": { "description": "Authentication required" },
                    "403": { "description": "Caller is not allowed to list processes" },
                    "404": { "description": "Sandbox not found" },
                    "409": { "description": "Sandbox process state conflicts with the request" },
                    "412": { "description": "Sandbox process-list precondition failed" },
                    "429": { "description": "Runtime request rate limited" },
                    "500": { "description": "Process listing failed internally" },
                    "503": { "description": "Sandbox runtime unavailable" }
                }
        }),
    );
    insert_public_route(
        paths,
        GET_PROCESS,
        json!({
                "summary": "Get a running process",
                "description": "Returns metadata for a running process, not completed-command history. Environment variables are never returned. Collect output and the exit status from the command's event stream.",
                "responses": {
                    "200": {
                        "description": "Process metadata",
                        "content": {
                            "application/json": {
                                "schema": { "$ref": "#/components/schemas/PublicProcess" }
                            }
                        }
                    },
                    "400": { "description": "Invalid sandbox or process id" },
                    "401": { "description": "Authentication required" },
                    "403": { "description": "Caller is not allowed to inspect processes" },
                    "404": { "description": "Sandbox or process not found" },
                    "409": { "description": "Sandbox process state conflicts with the request" },
                    "412": { "description": "Process lookup precondition failed" },
                    "429": { "description": "Runtime request rate limited" },
                    "500": { "description": "Process lookup failed internally" },
                    "503": { "description": "Sandbox runtime unavailable" }
                }
        }),
    );
    insert_public_route(
        paths,
        SIGNAL_PROCESS,
        process_action_path(
            "Send a Unix signal to a process",
            Some("#/components/schemas/ProcessSignalRequest"),
        ),
    );
    insert_public_route(
        paths,
        KILL_PROCESS,
        process_action_path("Kill a process", None),
    );
}

fn process_action_path(summary: &str, request_schema: Option<&str>) -> Value {
    let request_body = request_schema.map(|schema| {
        json!({
            "required": true,
            "content": {
                "application/json": { "schema": { "$ref": schema } }
            }
        })
    });
    let mut operation = json!({
        "summary": summary,
        "description": if request_schema.is_some() {
            "Send a Unix signal using JSON such as {\"signal\":15}; valid signal numbers are 1 through 64. Use the opaque process_id returned by the command or process list, not the guest's numeric pid. HTTP 200 with {\"ok\":true} means the runtime accepted the action, not that the process exited. No operation handle is returned; observe the command's stream for its exit result. This endpoint does not deduplicate with Idempotency-Key. Do not blindly retry an ambiguous outcome: a repeated request may deliver the signal again."
        } else {
            "Kill the identified process; no request body is needed. Use the opaque process_id returned by the command or process list, not the guest's numeric pid. HTTP 200 with {\"ok\":true} means the runtime accepted the action, not that the process exited. No operation handle is returned; observe the command's stream for its exit result. This endpoint does not deduplicate with Idempotency-Key. Inspect the outcome before repeating an ambiguous request."
        },
        "responses": {
            "200": {
                "description": "Process action applied",
                "content": {
                    "application/json": {
                        "schema": { "$ref": "#/components/schemas/ProcessMutationResponse" }
                    }
                }
            },
            "400": { "description": "Invalid sandbox id, process id, or signal" },
            "401": { "description": "Authentication required" },
            "403": { "description": "Caller or runtime refused the action" },
            "404": { "description": "Sandbox or process not found" },
            "409": { "description": "Process state conflicts with the action" },
            "412": { "description": "Process action precondition failed" },
            "429": { "description": "Runtime request rate limited" },
            "500": { "description": "Process action failed internally" },
            "503": { "description": "Sandbox runtime unavailable" }
        }
    });
    if let Some(request_body) = request_body {
        let operation = operation
            .as_object_mut()
            .expect("process action operation should be an object");
        operation.insert("requestBody".to_string(), request_body);
        let responses = operation
            .get_mut("responses")
            .and_then(Value::as_object_mut)
            .expect("process action responses should be an object");
        responses.insert(
            "413".to_string(),
            json!({ "description": "Request body is too large" }),
        );
        responses.insert(
            "415".to_string(),
            json!({ "description": "Content-Type must be application/json" }),
        );
    }
    operation
}

fn insert_public_route(paths: &mut Map<String, Value>, route: NativeOperation, operation: Value) {
    paths
        .entry(route.relative_path().to_string())
        .or_insert_with(|| Value::Object(Map::new()))
        .as_object_mut()
        .expect("path item should be an object")
        .insert(route.method().openapi_name().to_string(), operation);
}

pub(super) fn data_plane_error_response(description: &str) -> Value {
    json!({
        "description": description,
        "content": {
            "application/json": {
                "schema": { "$ref": "#/components/schemas/DataPlaneHttpErrorEnvelope" }
            }
        }
    })
}

pub(super) fn insert_path_parameters(spec: &mut Value) {
    let paths = spec
        .get_mut("paths")
        .and_then(Value::as_object_mut)
        .expect("paths should be an object");
    for (path, item) in paths {
        let parameters = path_parameter_names(path)
            .map(|name| {
                json!({
                    "name": name,
                    "in": "path",
                    "required": true,
                    "schema": path_parameter_schema(name)
                })
            })
            .collect::<Vec<_>>();
        if !parameters.is_empty() {
            item.as_object_mut()
                .expect("path item should be an object")
                .insert("parameters".to_string(), Value::Array(parameters));
        }
    }
}

pub(super) fn path_parameter_names(path: &str) -> impl Iterator<Item = &str> {
    path.split('{')
        .skip(1)
        .filter_map(|suffix| suffix.split_once('}').map(|(parameter, _suffix)| parameter))
}

fn path_parameter_schema(name: &str) -> Value {
    match name {
        "sandbox_id" => prefixed_uuid_schema(PublicSandboxId::PREFIX),
        "credential_reference_id" => {
            json!({ "type": "string", "format": "uuid", "pattern": format!("^{UUID_PATTERN}$") })
        }
        "operation_id" => prefixed_uuid_schema(OperationId::PREFIX),
        "snapshot_id" => prefixed_uuid_schema(PublicSnapshotId::PREFIX),
        "session_id" => {
            let prefixes = SessionKind::ALL
                .iter()
                .map(|kind| {
                    kind.id_prefix()
                        .strip_suffix('_')
                        .expect("session ID prefixes should end in an underscore")
                })
                .map(regex::escape)
                .collect::<Vec<_>>()
                .join("|");
            json!({ "type": "string", "pattern": format!("^({prefixes})_{UUID_PATTERN}$") })
        }
        "process_id" => {
            let max_bytes = pplx_space_common_core::types::MAX_PROCESS_ID_BYTES;
            json!({
                "type": "string",
                "minLength": 1,
                "maxLength": max_bytes,
                pplx_space_public_api::routes::MAX_UTF8_BYTES_EXTENSION: max_bytes,
                "pattern": "^[^\\u0000]+$"
            })
        }
        "port" => json!({ "type": "integer", "minimum": 1, "maximum": 65535 }),
        _ => json!({ "type": "string", "minLength": 1 }),
    }
}

fn prefixed_uuid_schema(prefix: &str) -> Value {
    json!({
        "type": "string",
        "pattern": format!("^{}{UUID_PATTERN}$", regex::escape(prefix))
    })
}

pub(super) fn complete_control_plane_responses(spec: &mut Value) {
    let paths = spec
        .get_mut("paths")
        .and_then(Value::as_object_mut)
        .expect("paths should be an object");
    for (path, item) in paths {
        if path.starts_with("/sessions/") {
            continue;
        }
        let operations = item.as_object_mut().expect("path item should be an object");
        for method in OPENAPI_HTTP_METHODS {
            let Some(operation) = operations.get_mut(method).and_then(Value::as_object_mut) else {
                continue;
            };
            let responses = operation
                .get_mut("responses")
                .and_then(Value::as_object_mut)
                .expect("operation responses should be an object");
            responses
                .entry("401".to_string())
                .or_insert_with(|| json!({ "description": "Authentication required" }));
            responses
                .entry("500".to_string())
                .or_insert_with(|| json!({ "description": "Internal server error" }));
            for (status, response) in responses {
                let response = response
                    .as_object_mut()
                    .expect("response should be an object");
                let headers = response
                    .entry("headers".to_string())
                    .or_insert_with(|| json!({}))
                    .as_object_mut()
                    .expect("response headers should be an object");
                headers.insert(
                    "x-request-id".to_string(),
                    json!({ "$ref": "#/components/headers/RequestId" }),
                );
                if matches!(status.as_str(), "429" | "503") {
                    headers.insert(
                        "Retry-After".to_string(),
                        json!({
                            "description": "Wait this many whole seconds before retrying (minimum 1, rounded up; not an HTTP date). Not every 429 or 503 includes this header. When absent, start with a one-second delay and double it with jitter.",
                            "schema": { "type": "integer", "minimum": 1 }
                        }),
                    );
                }
                if status.parse::<u16>().is_ok_and(|status| status >= 400) {
                    response.insert(
                        "content".to_string(),
                        json!({
                            "application/json": {
                                "schema": { "$ref": "#/components/schemas/PublicErrorEnvelope" }
                            }
                        }),
                    );
                }
            }
        }
    }
}

/// The operation and prose for one sandbox lifecycle action.
struct LifecycleAction {
    operation: NativeOperation,
    summary: &'static str,
    description: &'static str,
}

/// Resolves a lifecycle action to the operation that serves it.
///
/// A `match` rather than a lookup table: adding a `SandboxLifecycleAction`
/// variant then fails to compile until it is declared in the manifest, instead
/// of silently serving an action the contract does not describe.
///
/// Wording follows the lifecycle vocabulary in `AGENTS.md`, because the
/// difference between pause, suspend, and wake is the part clients get wrong.
fn lifecycle_action(action: SandboxLifecycleAction) -> Option<LifecycleAction> {
    let (operation, summary, description) = match action {
        SandboxLifecycleAction::Start => (
            START_SANDBOX,
            "Start a sandbox",
            "Starts a stopped sandbox. Does not imply snapshot persistence.",
        ),
        SandboxLifecycleAction::Stop => (
            STOP_SANDBOX,
            "Stop a sandbox",
            "Stops a running sandbox without persisting VM memory.",
        ),
        SandboxLifecycleAction::Pause => (
            PAUSE_SANDBOX,
            "Pause a sandbox",
            "Freezes the sandbox in memory without releasing its resources. Use suspend to release resources, or wake to return to running.",
        ),
        SandboxLifecycleAction::Resume => (
            RESUME_SANDBOX,
            "Resume a paused sandbox",
            "Returns a paused sandbox to running. Use wake to accept any valid recovery path.",
        ),
        SandboxLifecycleAction::Suspend => (
            SUSPEND_SANDBOX,
            "Suspend a sandbox",
            "Saves the sandbox's memory and state, then releases its resources. Use wake to return to running.",
        ),
        SandboxLifecycleAction::Restore => (
            RESTORE_SANDBOX,
            "Restore a suspended sandbox",
            "Restores a suspended sandbox from its persisted snapshot. Use wake to accept any valid recovery path.",
        ),
        SandboxLifecycleAction::Wake => (
            WAKE_SANDBOX,
            "Wake a sandbox",
            "Returns a sandbox to running by whichever path is valid for its current state: start, resume, or restore. Use this when the caller does not need to know which.",
        ),
        SandboxLifecycleAction::Kill => (
            KILL_SANDBOX,
            "Kill a sandbox",
            "Terminates the sandbox without a graceful stop.",
        ),
        SandboxLifecycleAction::Checkpoint => (
            CHECKPOINT_SANDBOX,
            "Checkpoint a sandbox",
            "Captures a snapshot of the running sandbox.",
        ),
        SandboxLifecycleAction::EnableSnapshots => (
            ENABLE_SANDBOX_SNAPSHOTS,
            "Enable snapshot capture for a sandbox",
            "Turns on periodic snapshot capture.",
        ),
        SandboxLifecycleAction::DisableSnapshots => (
            DISABLE_SANDBOX_SNAPSHOTS,
            "Disable snapshot capture for a sandbox",
            "Turns off periodic snapshot capture.",
        ),
        SandboxLifecycleAction::Rollback => (
            ROLLBACK_SANDBOX,
            "Roll back a sandbox",
            "Rolls the sandbox back to a named snapshot.",
        ),
        // `delete` is not a custom `:action` route. It is
        // `DELETE /sandboxes/{sandbox_id}`, generated with the CRUD operations.
        SandboxLifecycleAction::Delete => return None,
    };
    Some(LifecycleAction {
        operation,
        summary,
        description,
    })
}

pub(super) fn insert_sandbox_lifecycle_paths(
    paths: &mut Map<String, Value>,
    idempotency_key_parameter: &Value,
) {
    for action in SandboxLifecycleAction::ALL {
        let Some(lifecycle) = lifecycle_action(*action) else {
            continue;
        };
        let mut operation = json!({
            "summary": lifecycle.summary,
            "description": lifecycle.description,
            "parameters": [idempotency_key_parameter.clone()],
            "responses": {
                "202": {
                    "description": "Sandbox lifecycle operation accepted",
                    "content": {
                        "application/json": {
                            "schema": { "$ref": "#/components/schemas/SandboxOperationResponse" }
                        }
                    }
                },
                "400": { "description": "Invalid sandbox id, idempotency key, or action body" },
                "404": { "description": "Sandbox not found" },
                "409": { "description": "Idempotency key was reused with a different request" },
                "503": { "description": "Sandbox operation is temporarily unavailable" }
            }
        });
        if *action == SandboxLifecycleAction::Rollback {
            let operation = operation
                .as_object_mut()
                .expect("lifecycle operation should be an object");
            operation.insert(
                "requestBody".to_string(),
                json!({
                    "required": true,
                    "content": {
                        "application/json": {
                            "schema": { "$ref": "#/components/schemas/RollbackSandboxRequest" }
                        }
                    }
                }),
            );
            let responses = operation
                .get_mut("responses")
                .and_then(Value::as_object_mut)
                .expect("lifecycle responses should be an object");
            responses.insert(
                "413".to_string(),
                json!({ "description": "Request body is too large" }),
            );
        }
        insert_public_route(paths, lifecycle.operation, operation);
    }
}
