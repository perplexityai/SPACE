use super::command::MAX_COMMAND_TIMEOUT_SOURCE_BYTES;
use crate::routes::{MAX_AGGREGATE_UTF8_BYTES_EXTENSION, MAX_UTF8_BYTES_EXTENSION};
use pplx_space_common_core::types::{
    HashAlg, MAX_ENV_ENTRIES, MAX_ENV_KEY_BYTES, MAX_ENV_TOTAL_BYTES, MAX_ENV_VALUE_BYTES,
    MAX_GUEST_PATH_BYTES, MAX_GUEST_USER_NAME_BYTES, MAX_PROCESS_COMMAND_ARGS,
    MAX_PROCESS_COMMAND_BYTES, MAX_PROCESS_ID_BYTES, MIN_DIGEST_PREFIX_HEX_LEN,
    RUNTIME_CLOUD_HYPERVISOR, RUNTIME_CLOUD_HYPERVISOR_ALIAS, RUNTIME_FIRECRACKER,
    RUNTIME_FIRECRACKER_ALIAS, RUNTIME_GVISOR, RUNTIME_GVISOR_ALIAS,
};
use schemars::{JsonSchema, Schema, SchemaGenerator};

use super::{OperationId, ProcessSessionId, PublicSandboxId, SessionKind};

// ProcessCommand accounts for one separator byte per argument.
const MAX_SINGLE_PROCESS_COMMAND_ARG_BYTES: usize = MAX_PROCESS_COMMAND_BYTES - 1;
const UUID_PATTERN: &str =
    "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}";

pub(crate) fn date_time(generator: &mut SchemaGenerator) -> Schema {
    let mut schema = String::json_schema(generator);
    schema.insert("format".into(), "date-time".into());
    schema
}

pub(crate) fn optional_date_time(generator: &mut SchemaGenerator) -> Schema {
    let _ = generator;
    schemars::json_schema!({
        "anyOf": [
            { "type": "string", "format": "date-time" },
            { "type": "null" }
        ]
    })
}

fn incubating(mut schema: Schema) -> Schema {
    schema.insert(
        crate::routes::M1_STATUS_EXTENSION.into(),
        crate::routes::NativeOperationStatus::Incubating
            .as_str()
            .into(),
    );
    schema
}

pub(crate) fn optional_incubating_date_time(generator: &mut SchemaGenerator) -> Schema {
    incubating(optional_date_time(generator))
}

pub(crate) fn optional_incubating_string(generator: &mut SchemaGenerator) -> Schema {
    incubating(Option::<String>::json_schema(generator))
}

pub(crate) fn optional_incubating_idle_action(generator: &mut SchemaGenerator) -> Schema {
    incubating(Option::<crate::SandboxIdleAction>::json_schema(generator))
}

pub(crate) fn optional_incubating_idle_action_input(generator: &mut SchemaGenerator) -> Schema {
    let _ = generator;
    incubating(schemars::json_schema!({
        "anyOf": [
            { "type": "string", "enum": crate::SandboxIdleAction::KNOWN_VALUES },
            { "type": "null" }
        ]
    }))
}

fn string_with_pattern(generator: &mut SchemaGenerator, pattern: impl Into<String>) -> Schema {
    let mut schema = String::json_schema(generator);
    schema.insert("pattern".into(), pattern.into().into());
    schema
}

fn prefixed_uuid_pattern(prefix: &str) -> String {
    format!("^{}{UUID_PATTERN}$", regex::escape(prefix))
}

pub(crate) fn public_sandbox_id(generator: &mut SchemaGenerator) -> Schema {
    string_with_pattern(generator, prefixed_uuid_pattern(PublicSandboxId::PREFIX))
}

pub(crate) fn operation_id(generator: &mut SchemaGenerator) -> Schema {
    string_with_pattern(generator, prefixed_uuid_pattern(OperationId::PREFIX))
}

pub(crate) fn page_cursor(generator: &mut SchemaGenerator) -> Schema {
    let mut schema = string_with_pattern(generator, "^pc1_[A-Za-z0-9_-]+\\.[A-Za-z0-9_-]+$");
    schema.insert("maxLength".into(), 4096.into());
    schema
}

/// `name`, `name:tag`, or `name@sha256:<hex>`, matching `TemplateReference`.
///
/// The digest bounds come from the parser's own constants rather than literals:
/// `sha256` is the only algorithm `HashAlg::parse` accepts, and a digest is
/// either a full `digest_hex_len()` hex string or a prefix of at least
/// `MIN_DIGEST_PREFIX_HEX_LEN`. Hex is lowercase only; the parser rejects
/// uppercase.
///
/// `.` and `..` are excluded with `not`/`pattern` rather than a negative
/// lookahead: JSON Schema patterns are ECMA-262, but Go's RE2 has no lookahead,
/// so a lookahead here would fail to compile in a generated Go client.
pub(crate) fn template_reference(generator: &mut SchemaGenerator) -> Schema {
    // `name` is bounded at 255 bytes and a tag at 128 by the parser, so the
    // longest reference is `name:tag`. Those two bounds are literals in
    // `pplx_space_common_core::types::template`, so
    // `sandbox_create_schema_tracks_public_wire_parsing` is what keeps this
    // honest.
    const MAX_TEMPLATE_REFERENCE_BYTES: usize = 255 + 1 + 128;
    const NAME: &str = "[A-Za-z0-9._-]{1,255}";
    const TAG: &str = "[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}";
    let algorithm = HashAlg::Sha256;
    let digest = format!(
        "{algorithm}:[0-9a-f]{{{min},{max}}}",
        min = MIN_DIGEST_PREFIX_HEX_LEN,
        max = algorithm.digest_hex_len(),
    );
    let _ = generator;
    schemars::json_schema!({
        "type": "string",
        "minLength": 1,
        "maxLength": MAX_TEMPLATE_REFERENCE_BYTES,
        MAX_UTF8_BYTES_EXTENSION: MAX_TEMPLATE_REFERENCE_BYTES,
        "pattern": format!("^{NAME}(:{TAG}|@{digest})?$"),
        "not": { "pattern": "^[.]{1,2}([:@]|$)" },
        "examples": [
            "seed",
            "seed:latest",
            "seed@sha256:0000000000000000000000000000000000000000000000000000000000000000"
        ],
        "description": "Template reference as `name`, `name:tag`, or `name@sha256:<hex>`. A bare name resolves to `:latest`. A digest may be truncated to a unique prefix."
    })
}

/// The runtime selector, including the aliases the deserializer accepts.
///
/// `RuntimeKind::parse` takes both the canonical name and a short alias, so
/// listing only canonical values would make a schema-validating client reject
/// requests the server accepts. Whether the public boundary should stop
/// accepting aliases is a separate decision; until it does, the contract has to
/// describe what is accepted.
pub(crate) fn optional_runtime_kind(generator: &mut SchemaGenerator) -> Schema {
    let _ = generator;
    schemars::json_schema!({
        "anyOf": [
            {
                "type": "string",
                "enum": [
                    RUNTIME_FIRECRACKER,
                    RUNTIME_CLOUD_HYPERVISOR,
                    RUNTIME_GVISOR,
                    RUNTIME_FIRECRACKER_ALIAS,
                    RUNTIME_CLOUD_HYPERVISOR_ALIAS,
                    RUNTIME_GVISOR_ALIAS
                ],
                "description": "Sandbox runtime. Prefer the canonical names; the short aliases are accepted for compatibility. Input enums are closed, so new runtimes are added additively."
            },
            { "type": "null" }
        ]
    })
}

/// An optional request field whose document shape is not part of the M1
/// contract.
///
/// `network_policy` and `auto_lifecycle` are internal types today -
/// `NetworkPolicyV1` from `pplx-space-net-common` and `AutoLifecyclePolicy`
/// from `pplx-space-common-core`. Describing their trees here means restating
/// validation this crate does not own, and review found three ways that goes
/// wrong: destinations accepting `{"ip": "not-an-ip"}`, stage durations
/// accepting `0s` and `1.5s`, and over-precise or overflowing durations. Each
/// is a schema that tells an SDK to accept a request the server rejects.
///
/// Publishing an open object instead keeps the field usable without promising a
/// structure this crate cannot validate. RFC decision 7 also holds that the
/// lifecycle rule engine - wake sources, condition sets, ordered stages - stays
/// unpublished until it is replaced by a flat `idle_timeout`/`idle_action`
/// shape, and freezing it here would make that replacement breaking.
///
/// The server still validates both strictly, so a generated client cannot
/// pre-validate these fields. That looseness is the point: it is the field's
/// status made visible rather than a contract we have to keep.
fn optional_incubating_object(description: &str) -> Schema {
    schemars::json_schema!({
        "anyOf": [
            { "type": "object", "description": description },
            { "type": "null" }
        ],
        crate::routes::M1_STATUS_EXTENSION: crate::routes::NativeOperationStatus::Incubating.as_str()
    })
}

pub(crate) fn optional_network_policy(generator: &mut SchemaGenerator) -> Schema {
    let _ = generator;
    optional_incubating_object(
        "Sandbox egress policy override, applied on top of the template default.",
    )
}

pub(crate) fn optional_auto_lifecycle_policy(generator: &mut SchemaGenerator) -> Schema {
    let _ = generator;
    optional_incubating_object(
        "Idle-lifecycle policy. Omit it to receive the server-side six-hour idle-to-suspend default, which access and command activity reset. An explicit policy, including a disabled one, is preserved.",
    )
}

pub(crate) fn data_plane_session_id(generator: &mut SchemaGenerator) -> Schema {
    let prefixes = SessionKind::ALL
        .iter()
        .map(|kind| regex::escape(kind.id_prefix()))
        .collect::<Vec<_>>()
        .join("|");
    string_with_pattern(generator, format!("^({prefixes}){UUID_PATTERN}$"))
}

pub(crate) fn process_session_id(generator: &mut SchemaGenerator) -> Schema {
    string_with_pattern(generator, prefixed_uuid_pattern(ProcessSessionId::PREFIX))
}

pub(crate) fn process_id(generator: &mut SchemaGenerator) -> Schema {
    let mut schema = string_with_pattern(generator, "^[^\\u0000]+$");
    schema.insert("maxLength".into(), MAX_PROCESS_ID_BYTES.into());
    schema.insert(MAX_UTF8_BYTES_EXTENSION.into(), MAX_PROCESS_ID_BYTES.into());
    schema
}

pub(crate) fn process_command(generator: &mut SchemaGenerator) -> Schema {
    let _ = generator;
    schemars::json_schema!({
        "type": "array",
        "minItems": 1,
        "maxItems": MAX_PROCESS_COMMAND_ARGS,
        MAX_AGGREGATE_UTF8_BYTES_EXTENSION: MAX_PROCESS_COMMAND_BYTES,
        "items": {
            "type": "string",
            "pattern": "^[^\\u0000]*$",
            "maxLength": MAX_SINGLE_PROCESS_COMMAND_ARG_BYTES,
            MAX_UTF8_BYTES_EXTENSION: MAX_SINGLE_PROCESS_COMMAND_ARG_BYTES
        },
        "description": format!(
            "Non-empty argv with at most {MAX_PROCESS_COMMAND_ARGS} entries and {MAX_PROCESS_COMMAND_BYTES} aggregate UTF-8 bytes including separators. Arguments cannot contain NUL."
        )
    })
}

pub(crate) fn optional_guest_path(generator: &mut SchemaGenerator) -> Schema {
    let _ = generator;
    schemars::json_schema!({
        "anyOf": [
            {
                "type": "string",
                "minLength": 1,
                "maxLength": MAX_GUEST_PATH_BYTES,
                MAX_UTF8_BYTES_EXTENSION: MAX_GUEST_PATH_BYTES,
                "pattern": "^[^\\p{Cc}]+$",
                "not": { "pattern": "^([A-Za-z]:|//|\\\\\\\\)" },
                "description": "Non-empty Linux guest path. Windows drive-letter and UNC paths are rejected."
            },
            { "type": "null" }
        ]
    })
}

pub(crate) fn optional_guest_user(generator: &mut SchemaGenerator) -> Schema {
    let _ = generator;
    schemars::json_schema!({
        "anyOf": [
            {
                "type": "string",
                "minLength": 1,
                "maxLength": MAX_GUEST_USER_NAME_BYTES,
                MAX_UTF8_BYTES_EXTENSION: MAX_GUEST_USER_NAME_BYTES,
                "pattern": "^[^\\p{Cc}]+$",
                "description": "Guest user to run as. Defaults to the sandbox's configured user."
            },
            { "type": "null" }
        ]
    })
}

pub(crate) fn environment_map(generator: &mut SchemaGenerator) -> Schema {
    let _ = generator;
    schemars::json_schema!({
        "type": "object",
        "maxProperties": MAX_ENV_ENTRIES,
        MAX_AGGREGATE_UTF8_BYTES_EXTENSION: MAX_ENV_TOTAL_BYTES,
        "propertyNames": {
            "type": "string",
            "minLength": 1,
            "maxLength": MAX_ENV_KEY_BYTES,
            "pattern": "^[A-Za-z_][A-Za-z0-9_]*$"
        },
        "additionalProperties": {
            "type": "string",
            "maxLength": MAX_ENV_VALUE_BYTES,
            MAX_UTF8_BYTES_EXTENSION: MAX_ENV_VALUE_BYTES,
            "pattern": "^[^\\u0000]*$"
        },
        "description": format!(
            "Environment map with at most {MAX_ENV_ENTRIES} entries and {MAX_ENV_TOTAL_BYTES} aggregate UTF-8 bytes across keys and values."
        )
    })
}

pub(crate) fn optional_simple_duration(generator: &mut SchemaGenerator) -> Schema {
    let _ = generator;
    schemars::json_schema!({
        "anyOf": [
            {
                "type": "string",
                "maxLength": MAX_COMMAND_TIMEOUT_SOURCE_BYTES,
                "pattern": "^((0*[1-9][0-9]{0,8}(\\.(0+|[0-9]{0,8}[1-9]0*))?|0+\\.[0-9]{0,8}[1-9]0*)s|(0*[1-9][0-9]{0,8}(\\.(0+|[0-9]{0,9}[1-9]0*))?|0+\\.[0-9]{0,9}[1-9]0*)m|(0*[1-9][0-9]{0,8}(\\.(0+|[0-9]{0,10}[1-9]0*))?|0+\\.[0-9]{0,10}[1-9]0*)[hd])$",
                "examples": ["30s", "1.5m", "1h"],
                "description": "Positive SPACE compact duration with at most nine integer digits. Units are seconds, minutes, hours, or days; the value must resolve to nanosecond precision."
            },
            { "type": "null" }
        ]
    })
}
