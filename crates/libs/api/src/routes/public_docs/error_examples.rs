//! Representative response values, not new error or retry contracts.

use pplx_space_public_api::{
    DataPlaneHttpError, DataPlaneHttpErrorCode, DataPlaneHttpErrorEnvelope, PublicErrorCode,
    PublicErrorEnvelope, PublicSandboxId, SandboxLifecycleState,
    routes::{
        CREATE_COMMAND, CREATE_SANDBOX, DELETE_SANDBOX, NATIVE_V1_OPERATIONS, NativeOperation,
        PAUSE_SANDBOX, STREAM_SESSION_EVENTS, SUSPEND_SANDBOX, WAKE_SANDBOX,
    },
    sandbox_not_running_details,
};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::routes::public::error::error_contract;

const EXAMPLE_NOTE: &str = "Illustrative, not exhaustive. Messages can change. Use the returned retryable flag, not this example, to decide whether to retry.";

fn sandbox_mutation(operation: NativeOperation) -> bool {
    matches!(
        operation,
        CREATE_SANDBOX | DELETE_SANDBOX | PAUSE_SANDBOX | SUSPEND_SANDBOX | WAKE_SANDBOX
    )
}

fn control_plane_examples(
    operation: NativeOperation,
    status: &str,
    message: &str,
) -> Option<Value> {
    use PublicErrorCode::*;
    // Match the translators in public/{sandboxes,processes,sessions}.rs.
    // Framework JSON rejections retain their HTTP status with invalid_request.
    let codes = match status {
        "400" | "413" | "415" | "422" => vec![InvalidRequest],
        "401" => vec![Unauthorized],
        "403" => vec![Forbidden],
        "404" => vec![NotFound],
        "409" if operation == CREATE_COMMAND || sandbox_mutation(operation) => {
            vec![Conflict, IdempotencyConflict]
        }
        "409" => vec![Conflict],
        "412" => vec![FailedPrecondition],
        "429" if operation == CREATE_SANDBOX => vec![QuotaExceeded],
        "429" => vec![RateLimited],
        "500" => vec![Internal],
        "503" if sandbox_mutation(operation) => vec![CapacityUnavailable, ServiceUnavailable],
        "503" => vec![ServiceUnavailable],
        _ => return None,
    };
    Some(Value::Object(
        codes
            .into_iter()
            .map(|code| {
                let (_, retryable) = error_contract(&code);
                let name = code.as_str().to_owned();
                // createCommand's conflict is the sandbox not running.
                let not_running = operation == CREATE_COMMAND && code == Conflict;
                let message = match code {
                    Conflict if not_running => "sandbox is suspended, not running",
                    Conflict => "Request conflicts with the current state",
                    IdempotencyConflict => "Idempotency key was used with a different request",
                    CapacityUnavailable => "Sandbox capacity is temporarily unavailable",
                    ServiceUnavailable => "The operation is temporarily unavailable",
                    Internal => "Internal server error",
                    _ => message,
                };
                let value = PublicErrorEnvelope {
                    code,
                    message: message.to_owned(),
                    request_id: None,
                    retryable,
                    details: if not_running {
                        sandbox_not_running_details(
                            PublicSandboxId::from_uuid(Uuid::nil()),
                            &SandboxLifecycleState::Suspended,
                        )
                    } else {
                        Vec::new()
                    },
                };
                (name, json!({"description": EXAMPLE_NOTE, "value": value}))
            })
            .collect(),
    ))
}

fn data_plane_examples(status: &str, message: &str) -> Option<Value> {
    use DataPlaneHttpErrorCode::*;
    // Native SSE admission/stream errors and route_resolution_http_error in
    // data/plane/server/src/native.rs and native/auth.rs. In particular,
    // attached-session conflicts are retryable; invalid route state is not.
    let (codes, retryable) = match status {
        "400" => (vec![InvalidRequest], false),
        "401" => (vec![Unauthorized], false),
        "403" => (vec![Forbidden], false),
        "404" => (vec![NotFound], false),
        "409" => (vec![Conflict], true),
        "429" => (vec![RateLimited], true),
        "500" => (vec![Internal], false),
        "503" => (vec![NotReady, ServiceUnavailable, StreamUnavailable], true),
        _ => return None,
    };
    Some(Value::Object(
        codes
            .into_iter()
            .map(|code| {
                let name = code.as_str().to_owned();
                let value = DataPlaneHttpErrorEnvelope {
                    error: DataPlaneHttpError {
                        code,
                        message: message.to_owned(),
                        retryable,
                    },
                };
                (name, json!({"description": EXAMPLE_NOTE, "value": value}))
            })
            .collect(),
    ))
}

pub(super) fn insert_error_examples(spec: &mut Value) {
    // Keep this change bounded to operations published in the supported
    // reference. Both canonical documents use the same response annotations.
    for operation in NATIVE_V1_OPERATIONS
        .iter()
        .copied()
        .filter(|operation| operation.status().is_m1_supported())
    {
        let responses = spec
            .get_mut("paths")
            .and_then(|paths| paths.get_mut(operation.relative_path()))
            .and_then(|item| item.get_mut(operation.method().openapi_name()))
            .and_then(|operation| operation.get_mut("responses"))
            .and_then(Value::as_object_mut)
            .expect("supported operation has responses");
        for (status, response) in responses {
            if !status.parse::<u16>().is_ok_and(|status| status >= 400) {
                continue;
            }
            let message = response
                .get("description")
                .and_then(Value::as_str)
                .expect("response has a description");
            let examples = if operation == STREAM_SESSION_EVENTS {
                data_plane_examples(status, message)
            } else {
                control_plane_examples(operation, status, message)
            }
            .expect("supported response status should have an error example mapping");
            let content = response
                .get_mut("content")
                .and_then(|content| content.get_mut("application/json"))
                .and_then(Value::as_object_mut)
                .expect("error response has a JSON envelope");
            content.insert("examples".to_owned(), examples);
        }
    }
}
