//! Illustrative responses use the wire types, not renderer-generated placeholders.

use pplx_space_public_api::{
    DataPlaneAttachInfo, DataPlaneSessionId, DataPlaneTokenTransport, OperationId, OperationKind,
    OperationState, OperationTargetType, ProcessMutationResponse, ProcessSessionId, PublicCommand,
    PublicOperationRecord, PublicOperationResult, PublicPage, PublicProcess, PublicSandbox,
    PublicSandboxId, PublicSandboxOperationResult, SandboxLifecycleState, SandboxOperationResponse,
    SandboxResources, SessionState, process_id_for_session,
    routes::{NATIVE_V1_OPERATIONS, STREAM_SESSION_EVENTS},
};
use serde_json::{Value, json};
use time::OffsetDateTime;
use uuid::Uuid;

pub(super) fn insert_success_examples(spec: &mut Value) {
    let sandbox_id = PublicSandboxId::from_uuid(Uuid::nil());
    let operation_id = OperationId::from_uuid(Uuid::nil());
    let session_id = DataPlaneSessionId::Process(ProcessSessionId::from_uuid(Uuid::nil()));
    let process_id = process_id_for_session(session_id);
    let created_at = OffsetDateTime::from_unix_timestamp(1_790_596_800)
        .expect("fixed example timestamp is valid");
    let sandbox = PublicSandbox {
        id: sandbox_id,
        name: Some("example-sandbox".to_owned()),
        state: SandboxLifecycleState::Running,
        failure_code: None,
        conditions: Vec::new(),
        template: Some("seed:latest".to_owned()),
        template_digest: None,
        resources: SandboxResources {
            vcpu: None,
            memory_mib: None,
            disk_mib: None,
        },
        metadata: Default::default(),
        expires_at: Some(created_at + time::Duration::hours(6)),
        idle_timeout: Some("21600s".to_owned()),
        idle_action: Some(pplx_space_public_api::SandboxIdleAction::Suspend),
        created_at,
        updated_at: created_at,
    };
    let process = PublicProcess {
        process_id: process_id.clone(),
        pid: 42,
        command: vec!["sleep".to_owned(), "30".to_owned()],
        cwd: None,
        user: None,
        tag: None,
    };
    let command = PublicCommand {
        session_id,
        process_id,
        sandbox_id,
        state: SessionState::Pending,
        attach: Some(DataPlaneAttachInfo {
            url: format!("/v1/sessions/{session_id}/attach"),
            events_url: Some(format!("/v1/sessions/{session_id}/events")),
            token: "EXAMPLE_SESSION_TOKEN_NOT_A_CREDENTIAL".to_owned(),
            expires_at: created_at + time::Duration::minutes(5),
            token_transport: vec![
                DataPlaneTokenTransport::AuthorizationHeader,
                DataPlaneTokenTransport::SecWebSocketProtocol,
            ],
        }),
        created_at,
        updated_at: created_at,
        expires_at: None,
    };
    let operation = PublicOperationRecord {
        id: operation_id,
        kind: OperationKind::CreateSandbox,
        state: OperationState::Succeeded,
        target_type: Some(OperationTargetType::Sandbox),
        target_id: Some(sandbox_id.to_string()),
        progress_message: "Sandbox created".to_owned(),
        terminal_error_code: None,
        terminal_error: None,
        result: Some(PublicOperationResult::Sandbox(
            PublicSandboxOperationResult {
                sandbox_id,
                state: SandboxLifecycleState::Running,
                action: None,
                controller_response: None,
            },
        )),
        created_at,
        started_at: Some(created_at),
        completed_at: Some(created_at + time::Duration::seconds(1)),
        updated_at: created_at + time::Duration::seconds(1),
    };
    let examples = [
        ("PublicSandbox", json!(sandbox)),
        (
            "PublicSandboxPage",
            json!(PublicPage {
                data: vec![sandbox],
                next_cursor: None
            }),
        ),
        ("PublicProcess", json!(process)),
        (
            "PublicProcessPage",
            json!(PublicPage {
                data: vec![process],
                next_cursor: None
            }),
        ),
        ("PublicCommand", json!(command)),
        ("PublicOperationRecord", json!(operation)),
        (
            "SandboxOperationResponse",
            json!(SandboxOperationResponse {
                operation_id,
                sandbox_id
            }),
        ),
        (
            "ProcessMutationResponse",
            json!(ProcessMutationResponse { ok: true }),
        ),
    ];
    for route in NATIVE_V1_OPERATIONS
        .iter()
        .copied()
        .filter(|route| route.status().is_m1_supported() && *route != STREAM_SESSION_EVENTS)
    {
        let responses = spec
            .get_mut("paths")
            .and_then(|paths| paths.get_mut(route.relative_path()))
            .and_then(|item| item.get_mut(route.method().openapi_name()))
            .and_then(|operation| operation.get_mut("responses"))
            .and_then(Value::as_object_mut)
            .expect("supported operation has responses");
        for (status, response) in responses {
            if !status.starts_with('2') {
                continue;
            }
            let media = response
                .get_mut("content")
                .and_then(|content| content.get_mut("application/json"))
                .and_then(Value::as_object_mut)
                .expect("supported response has JSON content");
            let component = media
                .get("schema")
                .and_then(|schema| schema.get("$ref"))
                .and_then(Value::as_str)
                .and_then(|reference| reference.strip_prefix("#/components/schemas/"))
                .expect("supported JSON response has a component schema");
            let value = &examples
                .iter()
                .find(|(name, _)| *name == component)
                .expect("supported response has a typed example")
                .1;
            media.insert(
                "examples".to_owned(),
                json!({
                    "illustrative": {
                        "description": "Illustrative response with synthetic IDs and timestamps. The token placeholder is not a credential; use values returned by your deployment.",
                        "value": value
                    }
                }),
            );
        }
    }
}
