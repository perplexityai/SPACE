use pplx_space_public_api::routes::STREAM_SESSION_EVENTS;
use serde_json::{Map, Value, json};
use std::collections::BTreeSet;

use super::path_contract::data_plane_error_response;

pub(super) fn set_data_plane_security(spec: &mut Value) {
    let bearer_or_subprotocol = json!([
        { "dataPlaneToken": [] },
        { "dataPlaneWebSocketProtocol": [] }
    ]);
    for path in [
        "/paths/~1sessions~1{session_id}~1events/get",
        "/paths/~1sessions~1{session_id}~1proxy/get",
    ] {
        spec.pointer_mut(path)
            .and_then(Value::as_object_mut)
            .expect("data-plane operation should be an object")
            .insert("security".to_string(), bearer_or_subprotocol.clone());
    }
    spec.pointer_mut("/paths/~1sessions~1{session_id}~1attach/get")
        .and_then(Value::as_object_mut)
        .expect("data-plane attach operation should be an object")
        .insert(
            "security".to_string(),
            json!([
                { "dataPlaneToken": [] },
                { "dataPlaneWebSocketProtocol": [] },
                {}
            ]),
        );
}

pub(super) fn add_generator_safe_enum_names(spec: &mut Value) {
    let schemas = spec
        .get_mut("components")
        .and_then(|components| components.get_mut("schemas"))
        .and_then(Value::as_object_mut)
        .expect("component schemas should be an object");
    for (name, schema) in schemas {
        add_schema_enum_names(schema, &openapi_identifier(name));
    }
}

fn add_schema_enum_names(schema: &mut Value, prefix: &str) {
    let Value::Object(object) = schema else {
        if let Value::Array(values) = schema {
            for (index, value) in values.iter_mut().enumerate() {
                add_schema_enum_names(value, &format!("{prefix}_{index}"));
            }
        }
        return;
    };

    if let Some(values) = object.get("enum").and_then(Value::as_array) {
        let names = values
            .iter()
            .map(|value| {
                let value = value
                    .as_str()
                    .expect("generated OpenAPI enums should contain strings");
                Value::String(format!("{prefix}_{}", openapi_identifier(value)))
            })
            .collect();
        object.insert("x-enum-varnames".to_string(), Value::Array(names));
    }

    for (name, value) in object {
        if name == "x-enum-varnames" {
            continue;
        }
        add_schema_enum_names(value, &format!("{prefix}_{}", openapi_identifier(name)));
    }
}

fn openapi_identifier(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect()
}

pub(super) fn insert_command_event_stream_schema(spec: &mut Value) {
    let response = json!({
        "description": "### Read the stream\n\n- Each SSE event carries one `DataPlaneServerFrame` JSON object. Ignore comment heartbeats.\n- Decode stdout and stderr's `data_base64` using standard base64.\n- Success requires `process_exited` with exit code zero and no error or missing output. HTTP 200 or a closed stream alone is not success.\n\n### Example output\n\nExcerpt, not a complete stream:\n\n```text\nid: 4503599627632640\nevent: stdout\ndata: {\"type\":\"stdout\",\"seq\":1,\"data_base64\":\"SGVsbG8gZnJvbSBTUEFDRQo=\"}\n\nevent: process_exited\ndata: {\"type\":\"process_exited\",\"process_id\":\"proc_00000000-0000-0000-0000-000000000000\",\"exit_code\":0,\"status\":\"exited\"}\n\n```",
        "content": {
            "text/event-stream": {
                "schema": { "type": "string" },
                pplx_space_public_api::routes::SSE_DATA_SCHEMA_EXTENSION: { "$ref": "#/components/schemas/DataPlaneServerFrame" }
            }
        },
        "headers": {
            "Cache-Control": {
                "description": "Disables intermediary caching for the live event stream.",
                "schema": { "type": "string", "const": "no-cache" }
            }
        }
    });

    let operation = spec
        .pointer_mut("/paths/~1sessions~1{session_id}~1events/get")
        .and_then(Value::as_object_mut)
        .expect("command event stream operation should be an object");
    operation.insert(
        "operationId".to_string(),
        json!(STREAM_SESSION_EVENTS.operation_id()),
    );
    operation.insert(
        "parameters".to_string(),
        json!([{
            "name": "Last-Event-ID",
            "in": "header",
            "required": false,
            "description": "SSE `id` of the last frame received.\n\n- Echo the `id` exactly as received, on the same session's stream.\n- A process stream resumes with the output after that frame while it is retained; otherwise `resync_required` precedes the output it continues with: `cursor_out_of_window` when the output is evicted or the `id` is past it, `cursor_replay_unavailable` when the `id` does not resume exactly.\n- A file-watch stream resumes live; its first identified frame is `resync_required` (`cursor_replay_unavailable`), with id `Last-Event-ID + 1`.\n- Values above the `int64` maximum, and process stream values too high to number a later frame above, return 400.",
            "schema": {
                "type": "integer",
                "format": "int64",
                "minimum": 0,
                "maximum": i64::MAX
            }
        }]),
    );
    operation.insert(
        "responses".to_string(),
        json!({
            "200": response,
            "400": data_plane_error_response("Invalid session id, session kind, token transport, or Last-Event-ID, or a pty process session that has not started"),
            "401": data_plane_error_response("Missing, invalid, or expired data-plane token"),
            "403": data_plane_error_response("Token does not authorize this session"),
            "404": data_plane_error_response("Session not found"),
            "409": data_plane_error_response("Process session is already attached (retryable), or the sandbox is not running and this command cannot wake it (not retryable; `details` as on createCommand's conflict). The same 409 answers whether the node refused to start the command or the sandbox has no node and nothing is about to wake it. Over WebSocket the same refusal is an `error` frame with code `backend_stream_error` and the same `details`: branch on `details` `reason` (`sandbox_not_running`), not on the code"),
            "429": data_plane_error_response("Attached-stream limit exceeded"),
            "500": data_plane_error_response("Session route state is invalid"),
            "503": data_plane_error_response("Session route or backend stream is unavailable. `not_ready` is retryable, with `Retry-After`: something is about to give the sandbox a node. It is heading to running, or a start, resume, restore or wake is queued for it, such as the wake a command queued. Attaching to a sandbox at rest queues one when its `auto_lifecycle.wake.on` allows the attach: `command` or `access` for the first attach, which starts the command, and `access` for a reattach to a command that already started. A sandbox at rest that nothing is about to wake answers the 409 `sandbox_not_running` conflict instead")
        }),
    );
}

pub(super) fn insert_generated_m1_schemas(spec: &mut Value) {
    let settings = schemars::generate::SchemaSettings::draft2020_12().with(|settings| {
        settings.definitions_path = "/components/schemas".into();
        settings.meta_schema = None;
    });

    let mut request_generator = settings.clone().for_deserialize().into_generator();
    request_generator.subschema_for::<pplx_space_public_api::credential_references::RegisterCredentialReferenceRequest>();
    request_generator.subschema_for::<pplx_space_public_api::CreateSandboxRequest>();
    request_generator.subschema_for::<pplx_space_public_api::SetSandboxTimeoutRequest>();
    request_generator.subschema_for::<pplx_space_public_api::CreateCommandRequest>();
    request_generator.subschema_for::<pplx_space_public_api::ProcessSignalRequest>();
    request_generator.subschema_for::<pplx_space_public_api::RollbackSandboxRequest>();

    let mut response_generator = settings.for_serialize().into_generator();
    response_generator
        .subschema_for::<pplx_space_public_api::credential_references::CredentialReference>();
    response_generator.subschema_for::<pplx_space_public_api::SandboxOperationResponse>();
    response_generator.subschema_for::<pplx_space_public_api::SetSandboxTimeoutResponse>();
    response_generator
        .subschema_for::<pplx_space_public_api::PublicPage<pplx_space_public_api::PublicSandbox>>();
    response_generator.subschema_for::<pplx_space_public_api::PublicSandbox>();
    response_generator.subschema_for::<pplx_space_public_api::PublicOperationRecord>();
    response_generator.subschema_for::<pplx_space_public_api::PublicCommand>();
    response_generator
        .subschema_for::<pplx_space_public_api::PublicPage<pplx_space_public_api::PublicProcess>>();
    response_generator.subschema_for::<pplx_space_public_api::DataPlaneServerFrame>();
    response_generator.subschema_for::<pplx_space_public_api::PublicProcess>();
    response_generator.subschema_for::<pplx_space_public_api::ProcessMutationResponse>();
    response_generator.subschema_for::<pplx_space_public_api::PublicErrorEnvelope>();
    response_generator.subschema_for::<pplx_space_public_api::DataPlaneHttpErrorEnvelope>();

    let schemas = spec
        .get_mut("components")
        .and_then(|components| components.get_mut("schemas"))
        .and_then(Value::as_object_mut)
        .expect("component schemas should be an object");
    let request_schemas = request_generator.take_definitions(true);
    let response_schemas = response_generator.take_definitions(true);
    for (name, request_schema) in &request_schemas {
        if let Some(response_schema) = response_schemas.get(name) {
            assert_eq!(
                request_schema, response_schema,
                "request and response generators produced incompatible {name} schemas"
            );
        }
    }
    let mut generated_schemas = request_schemas;
    for (name, schema) in response_schemas {
        generated_schemas.entry(name).or_insert(schema);
    }
    for name in generated_schemas.keys() {
        assert!(
            !schemas.contains_key(name),
            "generated schema {name} must not overwrite a hand-authored component"
        );
    }
    schemas.extend(generated_schemas);
    flatten_operation_result_variants(schemas);
    schemas
        .values_mut()
        .for_each(normalize_unsigned_integer_formats);
    add_unknown_data_plane_frame_variant(schemas);
}

fn flatten_operation_result_variants(schemas: &mut Map<String, Value>) {
    let variants = schemas
        .get("PublicOperationResult")
        .and_then(|schema| schema.get("oneOf"))
        .and_then(Value::as_array)
        .expect("PublicOperationResult should be a tagged union")
        .iter()
        .map(|variant| {
            let tag = variant
                .pointer("/properties/type")
                .cloned()
                .expect("operation result variant should declare its type tag");
            let object = variant
                .as_object()
                .expect("operation result variant should be an object");
            let name = object
                .get("$ref")
                .and_then(Value::as_str)
                .and_then(|reference| reference.strip_prefix("#/components/schemas/"))
                .expect("operation result variant should reference a component");
            let mut flattened = schemas
                .get(name)
                .and_then(Value::as_object)
                .cloned()
                .expect("operation result component should be an object");

            flattened
                .entry("properties".to_string())
                .or_insert_with(|| json!({}))
                .as_object_mut()
                .expect("operation result properties should be an object")
                .insert("type".to_string(), tag);
            flattened
                .entry("required".to_string())
                .or_insert_with(|| json!([]))
                .as_array_mut()
                .expect("operation result required fields should be an array")
                .push(json!("type"));
            Value::Object(flattened)
        })
        .collect();

    schemas
        .get_mut("PublicOperationResult")
        .and_then(|schema| schema.get_mut("oneOf"))
        .expect("PublicOperationResult should be a tagged union")
        .clone_from(&Value::Array(variants));
}

fn normalize_unsigned_integer_formats(schema: &mut Value) {
    let Value::Object(object) = schema else {
        if let Value::Array(values) = schema {
            values
                .iter_mut()
                .for_each(normalize_unsigned_integer_formats);
        }
        return;
    };

    // OpenAPI only standardizes int32 and int64. OpenAPI Generator otherwise
    // treats Schemars' uint32/uint64 formats as int32, narrowing generated Go
    // clients. Keep the unsigned domain explicit through minimum/maximum.
    let normalized = object
        .get("format")
        .and_then(Value::as_str)
        .and_then(|format| match format {
            "uint8" => Some(("int32", u8::MAX as u64)),
            "uint16" => Some(("int32", u16::MAX as u64)),
            "uint32" => Some(("int64", u32::MAX as u64)),
            "uint64" => Some(("int64", i64::MAX as u64)),
            _ => None,
        });
    if let Some((format, maximum)) = normalized {
        object.insert("format".to_string(), json!(format));
        object
            .entry("minimum".to_string())
            .or_insert_with(|| json!(0));
        object
            .entry("maximum".to_string())
            .or_insert_with(|| json!(maximum));
    }

    object
        .values_mut()
        .for_each(normalize_unsigned_integer_formats);
}

fn add_unknown_data_plane_frame_variant(schemas: &mut Map<String, Value>) {
    let variants = schemas
        .get_mut("DataPlaneServerFrame")
        .and_then(|schema| schema.get_mut("oneOf"))
        .and_then(Value::as_array_mut)
        .expect("DataPlaneServerFrame should be a tagged union");
    variants.retain(|variant| {
        variant
            .pointer("/properties/type/const")
            .and_then(Value::as_str)
            != Some("unknown")
    });
    let known_types = variants
        .iter()
        .map(|variant| {
            variant
                .pointer("/properties/type/const")
                .expect("data-plane frame variant should have a type tag")
                .as_str()
                .expect("data-plane frame variant should have a type tag")
        })
        .collect::<Vec<_>>();
    variants.push(json!({
        "type": "object",
        "description": "A future server frame type not understood by this version of the client.",
        "properties": {
            "type": {
                "type": "string",
                "not": { "enum": known_types }
            }
        },
        "required": ["type"],
        "additionalProperties": true
    }));
}

/// Collects every `#/components/schemas/` name referenced anywhere in `value`.
pub(super) fn collect_component_schema_references(value: &Value, names: &mut BTreeSet<String>) {
    match value {
        Value::Object(object) => {
            if let Some(name) = object
                .get("$ref")
                .and_then(Value::as_str)
                .and_then(|reference| reference.strip_prefix("#/components/schemas/"))
            {
                names.insert(name.to_owned());
            }
            object
                .values()
                .for_each(|value| collect_component_schema_references(value, names));
        }
        Value::Array(values) => values
            .iter()
            .for_each(|value| collect_component_schema_references(value, names)),
        _ => {}
    }
}

/// Drops component schemas that nothing left in the document references.
///
/// Component schemas are shared across the whole native surface, so a
/// projection that retains only some paths still carries every schema. A
/// generator run against the M1 document would then emit models for the
/// incubating surface the projection exists to exclude.
pub(super) fn retain_referenced_schemas(spec: &mut Value) {
    let mut reachable = BTreeSet::new();
    // Headers are seeded too: a header schema may reference a component, and a
    // response can reach a schema only through `components/headers`.
    for pointer in ["/paths", "/components/headers"] {
        if let Some(value) = spec.pointer(pointer) {
            collect_component_schema_references(value, &mut reachable);
        }
    }

    let schemas = spec
        .pointer("/components/schemas")
        .and_then(Value::as_object)
        .expect("component schemas should be an object");
    let mut pending = reachable.iter().cloned().collect::<Vec<_>>();
    while let Some(name) = pending.pop() {
        let Some(schema) = schemas.get(&name) else {
            continue;
        };
        let mut references = BTreeSet::new();
        collect_component_schema_references(schema, &mut references);
        for reference in references {
            if reachable.insert(reference.clone()) {
                pending.push(reference);
            }
        }
    }

    spec.pointer_mut("/components/schemas")
        .and_then(Value::as_object_mut)
        .expect("component schemas should be an object")
        .retain(|name, _| reachable.contains(name));
}
