use super::*;
use pplx_space_common_core::types::{SandboxId, TemplateReference, TemplateSelector};
use pplx_space_metrics::response::{ResponseKind, ResponseOrigin};
use pplx_space_metrics::sandbox::api::{
    SandboxApiMetricIdentity, TemplateSelectorKind, TemplateSelectorRequest, TemplateTagShape,
};
use pplx_space_net_common::{
    DomainName, EgressDefaultAction, EgressDestinationV1, EgressPolicyV1, IpCidr, NetworkPolicyV1,
    NetworkPolicyVersion, TlsInterception, WildcardDomainName,
};
use std::net::IpAddr;

use crate::controllers::ControllerError;
use crate::controllers::sandbox::list_page::ListPage;

pub(super) use crate::e2b::{
    auto_lifecycle_policy as e2b_auto_lifecycle_policy,
    lifecycle_config_from_policy as e2b_lifecycle_config_from_policy, with_ecw_dns_resolver_allows,
};

#[derive(Debug)]
pub(super) struct CreateSandboxTranslation {
    pub(super) native: CreateSandboxRequest,
    pub(super) template_id: String,
}

const NETWORK_POLICY_METADATA_KEY: &str = "network.policy";
const TLS_INTERCEPTION_METADATA_KEY: &str = "network.tls_interception";
const RESOURCES_METADATA_KEY: &str = "space.resources";

#[derive(Debug, Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateResources {
    vcpu: Option<u32>,
    memory_mib: Option<u32>,
}

pub(super) fn translate_create_request(
    mut req: CreateSandboxRequestE2b,
) -> Result<CreateSandboxTranslation, E2bApiError> {
    // Reserved metadata carries SPACE create options through an unmodified SDK.
    let resources = req
        .metadata
        .remove(RESOURCES_METADATA_KEY)
        .map(|raw| serde_json::from_str::<CreateResources>(&raw))
        .transpose()
        .map_err(|error| E2bApiError::bad_request(format!("invalid space.resources: {error}")))?
        .unwrap_or_default();
    for (field, value) in [
        ("vcpu", resources.vcpu),
        ("memory_mib", resources.memory_mib),
    ] {
        // Resource columns are signed 32-bit integers; reject before admission.
        if let Some(value) = value
            && (value == 0 || value > i32::MAX as u32)
        {
            return Err(E2bApiError::bad_request(format!(
                "space.resources.{field} must be between 1 and {}",
                i32::MAX
            )));
        }
    }
    let mut unsupported = Vec::new();
    let network_policy = if let Some(raw) = req.metadata.remove(NETWORK_POLICY_METADATA_KEY) {
        if req.network.is_some()
            || req.allow_internet_access.is_some()
            || req.metadata.contains_key(TLS_INTERCEPTION_METADATA_KEY)
        {
            return Err(E2bApiError::bad_request(
                "network.policy cannot be combined with network, allow_internet_access, or network.tls_interception",
            ));
        }
        // Full policies have native replacement semantics, including explicit DNS permissions.
        Some(
            serde_json::from_str::<NetworkPolicyV1>(&raw).map_err(|error| {
                E2bApiError::bad_request(format!("invalid network.policy: {error}"))
            })?,
        )
    } else {
        let parsed_network = parse_e2b_network_config(req.network.as_ref())?;
        let tls_interception = parse_e2b_tls_interception(&req.metadata)?;
        let mut policy = resolve_e2b_network_policy(req.allow_internet_access, parsed_network)?;
        match policy.as_mut() {
            Some(policy) => policy.tls_interception = tls_interception,
            None if tls_interception == TlsInterception::On => {
                return Err(E2bApiError::bad_request(
                    "network.tls_interception=on requires a concrete network policy",
                ));
            }
            None => {}
        }
        policy
    };
    if req.mcp.is_some() {
        unsupported.push("mcp");
    }
    if !req.volume_mounts.is_empty() {
        unsupported.push("volumeMounts");
    }
    if req.auto_pause_memory == Some(false) {
        unsupported.push("autoPauseMemory=false");
    }
    if req.iam.is_some() {
        unsupported.push("iam");
    }
    if !unsupported.is_empty() {
        return Err(E2bApiError::bad_request(format!(
            "unsupported sandbox create parameters: {}",
            unsupported.join(", ")
        )));
    }

    reject_nul("metadata", &req.metadata)?;
    reject_nul("envVars", &req.env_vars)?;

    let ignored_metadata = req
        .metadata
        .keys()
        .filter(|key| !matches!(key.as_str(), "name" | TLS_INTERCEPTION_METADATA_KEY))
        .cloned()
        .collect::<Vec<_>>();
    if !ignored_metadata.is_empty() {
        // Routine: native metadata carries any key, and the fields are kept.
        tracing::debug!(
            fields = %ignored_metadata.join(", "),
            "forwarding unrecognized E2B metadata fields as native metadata",
        );
    }
    let template_id = req
        .template_id
        .unwrap_or_else(|| ECW_DEFAULT_TEMPLATE_ID.to_string());
    let template = template_id
        .parse::<TemplateReference>()
        .map_err(|error| E2bApiError::bad_request(format!("invalid template_id: {error}")))?;
    record_ecw_template_selector_request(&template);
    let metadata = e2b_metadata_to_native(req.metadata);
    let timeout_secs = req.timeout.unwrap_or(ECW_DEFAULT_CREATE_TIMEOUT_SECS);
    let auto_pause = req.auto_pause.unwrap_or(false);
    let auto_resume = parse_e2b_auto_resume(req.auto_resume.as_ref())?;
    let auto_lifecycle = e2b_auto_lifecycle_policy(timeout_secs, auto_pause, auto_resume);

    Ok(CreateSandboxTranslation {
        native: CreateSandboxRequest {
            name: None,
            template: Some(template),
            runtime: None,
            boot_mode: None,
            env: req.env_vars,
            metadata,
            vcpu: resources.vcpu,
            memory_mib: resources.memory_mib,
            disk_mib: None,
            file_changes: None,
            auto_lifecycle: Some(auto_lifecycle),
            // The E2B SDK assumes `Sandbox.create` returns a usable sandbox —
            // the caller will fire `set_timeout`, `connect`, fs ops, etc. as
            // soon as the call returns. Block until the substrate reports the
            // sandbox is command-ready so those follow-ups don't race the
            // sbx-mgr `Starting → Started` transition.
            wait_until_ready: true,
            network_policy,
            // Placement is an internal/operator-only knob; ECW/E2B never pins.
            node_pool: None,
        },
        template_id,
    })
}

/// Postgres cannot store NUL in text or jsonb, so a create carrying one in
/// `field` is an invalid request rather than an insert failure.
fn reject_nul(field: &str, entries: &StringMap) -> Result<(), E2bApiError> {
    if entries
        .iter()
        .any(|(key, value)| key.contains('\0') || value.contains('\0'))
    {
        return Err(E2bApiError::bad_request(format!(
            "{field} must not contain NUL"
        )));
    }
    Ok(())
}

fn parse_e2b_tls_interception(metadata: &StringMap) -> Result<TlsInterception, E2bApiError> {
    match metadata
        .get(TLS_INTERCEPTION_METADATA_KEY)
        .map(String::as_str)
    {
        None | Some("off") => Ok(TlsInterception::Off),
        Some("on") => Ok(TlsInterception::On),
        Some(_) => Err(E2bApiError::bad_request(
            "network.tls_interception must be off or on",
        )),
    }
}

pub(super) fn ecw_template_selector_metric_labels(
    template: &TemplateReference,
) -> (TemplateSelectorKind, TemplateTagShape, &str) {
    match template.selector() {
        TemplateSelector::Tag(tag) if tag.as_str() == "latest" => (
            TemplateSelectorKind::Latest,
            TemplateTagShape::Latest,
            tag.as_str(),
        ),
        TemplateSelector::Tag(tag) => (
            TemplateSelectorKind::Tag,
            classify_ecw_template_tag(tag.as_str()),
            tag.as_str(),
        ),
        TemplateSelector::ArtifactDigest(_) => (
            TemplateSelectorKind::Digest,
            TemplateTagShape::NotTag,
            "<none>",
        ),
        TemplateSelector::ArtifactDigestPrefix(_) => (
            TemplateSelectorKind::DigestPrefix,
            TemplateTagShape::NotTag,
            "<none>",
        ),
    }
}

fn record_ecw_template_selector_request(template: &TemplateReference) {
    let (selector_kind, tag_shape, requested_tag) = ecw_template_selector_metric_labels(template);
    pplx_space_metrics::sandbox::api::template_selector_requested(TemplateSelectorRequest {
        identity: SandboxApiMetricIdentity::ecw("create"),
        template: template.name().as_str(),
        selector_kind,
        tag_shape,
        requested_tag,
    });
}

fn classify_ecw_template_tag(tag: &str) -> TemplateTagShape {
    if tag.len() == 40 && tag.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        TemplateTagShape::GitSha40
    } else {
        TemplateTagShape::Other
    }
}

#[derive(Debug, Default)]
pub(super) struct ParsedE2bNetworkConfig {
    pub(super) allow_destinations: Vec<EgressDestinationV1>,
    pub(super) deny_destinations: Vec<EgressDestinationV1>,
}

impl ParsedE2bNetworkConfig {
    fn has_rules(&self) -> bool {
        !self.allow_destinations.is_empty() || !self.deny_destinations.is_empty()
    }
}

fn parse_e2b_network_config(
    network: Option<&Value>,
) -> Result<ParsedE2bNetworkConfig, E2bApiError> {
    let Some(network) = network else {
        return Ok(ParsedE2bNetworkConfig::default());
    };

    let Value::Object(object) = network else {
        return if network.is_null() {
            Ok(ParsedE2bNetworkConfig::default())
        } else {
            Err(E2bApiError::bad_request(
                "network must be an object when provided",
            ))
        };
    };

    let supported_fields = [
        "allowPublicTraffic",
        "allow_public_traffic",
        "allowOut",
        "allow_out",
        "denyOut",
        "deny_out",
    ];
    let unsupported_fields = object
        .keys()
        .filter(|key| !supported_fields.contains(&key.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    if !unsupported_fields.is_empty() {
        return Err(E2bApiError::bad_request(format!(
            "unsupported network fields: {}; supported fields are allowPublicTraffic, allowOut, denyOut",
            unsupported_fields.join(", ")
        )));
    }

    let allow_public_traffic = parse_bool_alias(
        object,
        "allowPublicTraffic",
        "allow_public_traffic",
        "network.allowPublicTraffic",
    )?;
    if allow_public_traffic == Some(true) {
        return Err(E2bApiError::bad_request(
            "network.allowPublicTraffic=true is not supported; ECW sandbox service traffic is token-authenticated",
        ));
    }

    let allow_destinations = parse_egress_list(
        object,
        "allowOut",
        "allow_out",
        "network.allowOut",
        EgressListKind::Allow,
    )?;
    let deny_destinations = parse_egress_list(
        object,
        "denyOut",
        "deny_out",
        "network.denyOut",
        EgressListKind::Deny,
    )?;

    Ok(ParsedE2bNetworkConfig {
        allow_destinations,
        deny_destinations,
    })
}

/// The default-egress decision for an ECW `network` config. `Open` permits
/// everything not explicitly denied; `Closed` denies everything not
/// explicitly allowed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EcwEgressDefault {
    Open,
    Closed,
}

/// Decide the default-egress posture from `(allow_internet_access, allowOut,
/// denyOut)`. The E2B deny-all-CIDR marker forces `Closed`
/// only in the `(_, allowOut-present, deny)` shape — a marker with no `allowOut`
/// keeps the ordinary allow-list/block-list decision.
fn ecw_egress_default(
    allow_internet_access: Option<bool>,
    parsed: &ParsedE2bNetworkConfig,
) -> Result<EcwEgressDefault, E2bApiError> {
    let has_allow = !parsed.allow_destinations.is_empty();
    let has_deny = !parsed.deny_destinations.is_empty();
    let has_only_deny_all_markers =
        has_deny && parsed.deny_destinations.iter().all(is_e2b_deny_all_marker);

    Ok(match (allow_internet_access, has_allow, has_deny) {
        // E2B treats a deny-all CIDR as a default-deny marker that can accompany
        // domain allow rules; the marker wins over allow_internet_access because
        // keeping it as an explicit deny would block every resolved IP. Requires
        // `allowOut` present — same guard V1 has always used.
        (_, true, true) if has_only_deny_all_markers => EcwEgressDefault::Closed,
        (Some(true), _, _) => EcwEgressDefault::Open,
        (Some(false), _, _) => EcwEgressDefault::Closed,
        // Allow-list semantics: explicit allows, everything else denied.
        (None, true, false) => EcwEgressDefault::Closed,
        // Block-list semantics: explicit denies, everything else permitted.
        (None, false, true) => EcwEgressDefault::Open,
        (None, true, true) => {
            return Err(E2bApiError::bad_request(
                "network.allowOut and network.denyOut both set; specify allow_internet_access to disambiguate the default action",
            ));
        }
        (None, false, false) => unreachable!("guarded by has_rules() before calling"),
    })
}

fn resolve_e2b_network_policy(
    allow_internet_access: Option<bool>,
    parsed: ParsedE2bNetworkConfig,
) -> Result<Option<NetworkPolicyV1>, E2bApiError> {
    if !parsed.has_rules() && allow_internet_access.is_none() {
        return Ok(None);
    }

    let default_action = match ecw_egress_default(allow_internet_access, &parsed)? {
        EcwEgressDefault::Open => EgressDefaultAction::Allow,
        EcwEgressDefault::Closed => EgressDefaultAction::Deny,
    };

    let ParsedE2bNetworkConfig {
        allow_destinations,
        deny_destinations,
    } = parsed;
    let allow_destinations = if default_action == EgressDefaultAction::Deny
        && allow_destinations.iter().any(|destination| {
            matches!(
                destination,
                EgressDestinationV1::Domain(_) | EgressDestinationV1::WildcardDomain(_)
            )
        }) {
        with_ecw_dns_resolver_allows(allow_destinations)
    } else {
        allow_destinations
    };
    let deny_destinations = if default_action == EgressDefaultAction::Deny {
        deny_destinations
            .into_iter()
            .filter(|destination| !is_e2b_deny_all_marker(destination))
            .collect()
    } else {
        deny_destinations
    };

    let egress = EgressPolicyV1::new(default_action, allow_destinations, deny_destinations)
        .map_err(|err| E2bApiError::bad_request(format!("invalid network policy: {err}")))?;

    Ok(Some(NetworkPolicyV1 {
        version: NetworkPolicyVersion::V1,
        egress,
        tls_interception: Default::default(),
        tls_interception_exemptions: Vec::new(),
        network_credential_rules: Vec::new(),
    }))
}

fn is_e2b_deny_all_marker(destination: &EgressDestinationV1) -> bool {
    let EgressDestinationV1::Cidr(cidr) = destination else {
        return false;
    };
    *cidr
        == "0.0.0.0/0"
            .parse::<IpCidr>()
            .expect("static IPv4 deny-all CIDR must be valid")
        || *cidr
            == "::/0"
                .parse::<IpCidr>()
                .expect("static IPv6 deny-all CIDR must be valid")
}

fn parse_bool_alias(
    object: &serde_json::Map<String, Value>,
    camel: &'static str,
    snake: &'static str,
    label: &'static str,
) -> Result<Option<bool>, E2bApiError> {
    match (object.get(camel), object.get(snake)) {
        (None, None) => Ok(None),
        (Some(Value::Bool(camel_val)), Some(Value::Bool(snake_val))) if camel_val == snake_val => {
            Ok(Some(*camel_val))
        }
        (Some(Value::Bool(_)), Some(Value::Bool(_))) => Err(E2bApiError::bad_request(format!(
            "network.{camel} conflicts with network.{snake}"
        ))),
        (Some(Value::Bool(value)), None) | (None, Some(Value::Bool(value))) => Ok(Some(*value)),
        _ => Err(E2bApiError::bad_request(format!(
            "{label} must be a boolean"
        ))),
    }
}

#[derive(Clone, Copy)]
enum EgressListKind {
    Allow,
    Deny,
}

fn parse_egress_list(
    object: &serde_json::Map<String, Value>,
    camel: &'static str,
    snake: &'static str,
    label: &'static str,
    kind: EgressListKind,
) -> Result<Vec<EgressDestinationV1>, E2bApiError> {
    let raw = match (object.get(camel), object.get(snake)) {
        (None, None) => return Ok(Vec::new()),
        (Some(_), Some(_)) => {
            return Err(E2bApiError::bad_request(format!(
                "network.{camel} conflicts with network.{snake}"
            )));
        }
        (Some(value), None) | (None, Some(value)) => value,
    };

    let array = match raw {
        Value::Array(items) => items,
        Value::Null => return Ok(Vec::new()),
        _ => {
            return Err(E2bApiError::bad_request(format!(
                "{label} must be an array of strings"
            )));
        }
    };

    array
        .iter()
        .map(|item| match item {
            Value::String(value) => parse_egress_destination(value, label, kind),
            // Static message: `item` is arbitrary client JSON that would leak
            // into `otel.status_description`.
            _ => Err(E2bApiError::bad_request(format!(
                "{label} entries must be strings"
            ))),
        })
        .collect()
}

fn parse_egress_destination(
    value: &str,
    label: &'static str,
    kind: EgressListKind,
) -> Result<EgressDestinationV1, E2bApiError> {
    // Error messages must stay static (only the `&'static str` field `label`):
    // `E2bApiError` records its message in `otel.status_description` and the
    // response, so the client's raw destination value and any parser text —
    // which echoes that value — must not be interpolated into an error.
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(E2bApiError::bad_request(format!(
            "{label} entries must not be empty"
        )));
    }

    if trimmed.contains('/') {
        return trimmed
            .parse::<IpCidr>()
            .map(EgressDestinationV1::Cidr)
            .map_err(|_| E2bApiError::bad_request(format!("{label} has an invalid CIDR range")));
    }
    if let Ok(ip) = trimmed.parse::<IpAddr>() {
        return Ok(EgressDestinationV1::Ip(ip));
    }

    // V1 enforcement cannot express domain denies.
    if matches!(kind, EgressListKind::Deny) {
        return Err(E2bApiError::bad_request(format!(
            "{label} only accepts IP addresses and CIDR ranges; domain entries are not supported on the data plane"
        )));
    }

    if trimmed.starts_with("*.") {
        return trimmed
            .parse::<WildcardDomainName>()
            .map(EgressDestinationV1::WildcardDomain)
            .map_err(|_| {
                E2bApiError::bad_request(format!("{label} has an invalid wildcard domain"))
            });
    }

    trimmed
        .parse::<DomainName>()
        .map(EgressDestinationV1::Domain)
        .map_err(|_| E2bApiError::bad_request(format!("{label} has an invalid destination")))
}

pub(super) fn parse_e2b_auto_resume(raw: Option<&Value>) -> Result<bool, E2bApiError> {
    let Some(raw) = raw else {
        return Ok(false);
    };

    match raw {
        Value::Null => Ok(false),
        Value::Bool(enabled) => Ok(*enabled),
        Value::Object(object) => match object.get("enabled") {
            Some(Value::Bool(enabled)) => Ok(*enabled),
            Some(_) => Err(E2bApiError::bad_request(
                "autoResume.enabled must be a boolean",
            )),
            None => Ok(false),
        },
        _ => Err(E2bApiError::bad_request(
            "autoResume must be a boolean or {enabled}",
        )),
    }
}

pub(super) fn e2b_metadata_to_native(metadata: StringMap) -> SandboxMetadata {
    metadata
        .into_iter()
        .map(|(key, value)| (key, e2b_metadata_value_to_native(value)))
        .collect()
}

pub(super) fn e2b_metadata_value_to_native(value: String) -> SandboxMetadataValue {
    match value.as_str() {
        "true" => SandboxMetadataValue::Bool(true),
        "false" => SandboxMetadataValue::Bool(false),
        _ => match serde_json::from_str::<i64>(&value) {
            Ok(number) => SandboxMetadataValue::Number(number),
            Err(_) => SandboxMetadataValue::String(value),
        },
    }
}

// Default and maximum number of rows fetched from the `sandboxes` table for a
// single ECW list page. The list path fans out to the substrate for every row
// it returns to the client, so unbounded pages would be O(org-size).
pub(super) const ECW_LIST_DEFAULT_LIMIT: u32 = 1000;
pub(super) const ECW_LIST_MAX_LIMIT: u32 = 1000;

#[derive(Debug, Clone, Copy)]
pub(super) struct ListCursor {
    pub(super) created_at: OffsetDateTime,
    pub(super) id: SandboxId,
}

impl From<ListCursor> for SandboxPageCursor {
    fn from(cursor: ListCursor) -> Self {
        Self {
            created_at: cursor.created_at,
            id: cursor.id,
        }
    }
}

pub(super) fn encode_list_cursor(sandbox: &Sandbox) -> String {
    let raw = format!(
        "{}:{}",
        sandbox
            .created_at
            .format(&Rfc3339)
            .expect("OffsetDateTime always formats as RFC3339"),
        sandbox.id,
    );
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(raw.as_bytes())
}

pub(super) fn decode_list_cursor(token: &str) -> Result<ListCursor, E2bApiError> {
    let raw = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(token.as_bytes())
        .map_err(|_| E2bApiError::bad_request("Invalid nextToken"))?;
    let raw =
        std::str::from_utf8(&raw).map_err(|_| E2bApiError::bad_request("Invalid nextToken"))?;
    let (created_at, id) = raw
        .rsplit_once(':')
        .ok_or_else(|| E2bApiError::bad_request("Invalid nextToken"))?;
    let created_at = OffsetDateTime::parse(created_at, &Rfc3339)
        .map_err(|_| E2bApiError::bad_request("Invalid nextToken"))?;
    let id: SandboxId = id
        .parse()
        .map_err(|_| E2bApiError::bad_request("Invalid nextToken"))?;
    Ok(ListCursor { created_at, id })
}

// Fetch a keyset-paginated page of sandboxes for `org_id`, ordered newest
// first. Pagination is on `(created_at DESC, id DESC)` so timestamps that tie
// (e.g. bulk-created sandboxes) still yield a strict ordering. Returns the
// caller-requested `limit` rows plus one sentinel row when more exist; the
// caller drops the sentinel and uses it as the "next cursor" signal.
//
// `metadata_filter` is pushed into the controller list query, so only matching
// rows are fetched. This keeps the per-page substrate fanout proportional to
// the matching subset rather than the entire org. Without pushdown, a busy org
// with sparse matches forces the handler to walk many raw pages before the SDK
// paginator sees enough hits.
/// Resolves the sandbox access scope for an authenticated ECW request:
/// federated when the trusted federated integration asserted an upstream owner,
/// otherwise the local org of the API key.
pub(super) fn ecw_access_scope(
    org: &Organization,
    federated: &Option<Extension<crate::middleware::auth::AuthFederatedOwner>>,
) -> SandboxAccessScope {
    match federated {
        Some(Extension(crate::middleware::auth::AuthFederatedOwner(owner))) => {
            SandboxAccessScope::FederatedSubject {
                owner: owner.clone(),
                legacy_org_id: org.id,
            }
        }
        None => SandboxAccessScope::LocalOrg { org_id: org.id },
    }
}

pub(super) async fn fetch_scoped_sandbox_page(
    state: &AppState,
    scope: &SandboxAccessScope,
    cursor: Option<ListCursor>,
    limit: u32,
    metadata_filter: &[(String, String)],
) -> Result<ListPage<Sandbox>, E2bApiError> {
    let metadata_jsonb = build_metadata_filter_jsonb(metadata_filter);
    state
        .sandbox_controller
        .scoped_sandbox_page(
            &state.pool,
            scope,
            cursor.map(Into::into),
            limit,
            metadata_jsonb,
        )
        .await
        .map_err(controller_error)
}

// Build a JSONB filter object that matches the storage shape of
// `sandboxes.metadata`. The metadata is stored after `e2b_metadata_value_to_native`
// converts string-typed query values into their typed variants ("3" -> number
// 3, "true" -> bool true, otherwise string), so the filter JSON has to apply
// the same conversion or `@>` containment will miss type-coerced rows.
pub(super) fn build_metadata_filter_jsonb(
    metadata_filter: &[(String, String)],
) -> Option<serde_json::Value> {
    if metadata_filter.is_empty() {
        return None;
    }
    let mut obj = serde_json::Map::with_capacity(metadata_filter.len());
    for (key, value) in metadata_filter {
        let typed = match e2b_metadata_value_to_native(value.clone()) {
            SandboxMetadataValue::String(s) => serde_json::Value::String(s),
            SandboxMetadataValue::Number(n) => serde_json::Value::Number(n.into()),
            SandboxMetadataValue::Bool(b) => serde_json::Value::Bool(b),
        };
        obj.insert(key.clone(), typed);
    }
    Some(serde_json::Value::Object(obj))
}

pub(super) async fn fetch_scoped_sandbox(
    state: &AppState,
    scope: &SandboxAccessScope,
    sandbox_id: &str,
) -> Result<Sandbox, E2bApiError> {
    let sandbox_id = parse_sandbox_id(sandbox_id)?;
    record_ecw_sandbox_span_tag(sandbox_id);
    let sandbox = state
        .sandbox_controller
        .scoped_sandbox(&state.pool, scope, sandbox_id)
        .await
        .map_err(controller_error)?;

    let Some(sandbox) = sandbox else {
        return Err(E2bApiError::not_found("Sandbox"));
    };

    Ok(sandbox)
}

pub(super) fn parse_sandbox_id(sandbox_id: &str) -> Result<SandboxId, E2bApiError> {
    sandbox_id
        .parse()
        .map_err(|_| E2bApiError::not_found("Sandbox"))
}

pub(super) async fn build_listed_sandbox(
    _state: &AppState,
    sandbox: &Sandbox,
    native: &SandboxResponse,
) -> Result<Option<ListedSandboxE2b>, E2bApiError> {
    let Some(sandbox_state) = translate_sandbox_state(native) else {
        return Ok(None);
    };

    let runtime_spec = cached_runtime_spec(sandbox, native);
    let started_at = sandbox_started_at(sandbox, native);
    let metadata = sandbox_metadata(sandbox, native);

    Ok(Some(ListedSandboxE2b {
        template_id: sandbox_template_id(sandbox, native, &runtime_spec),
        alias: None,
        sandbox_id: sandbox.id.to_string(),
        client_id: sandbox.id.to_string(),
        started_at: e2b_timestamp(started_at),
        end_at: e2b_timestamp(sandbox_end_at(sandbox, native)),
        cpu_count: runtime_spec
            .cpu_count
            .unwrap_or(ECW_RESPONSE_DEFAULT_CPU_COUNT),
        memory_mb: runtime_spec
            .memory_mib
            .unwrap_or(ECW_RESPONSE_DEFAULT_MEMORY_MB),
        disk_size_mb: runtime_spec
            .disk_size_mib
            .unwrap_or(ECW_RESPONSE_DEFAULT_DISK_SIZE_MB),
        metadata,
        state: sandbox_state,
        envd_version: ECW_RESPONSE_DEFAULT_ENVD_VERSION.to_string(),
        volume_mounts: Vec::new(),
    }))
}

pub(super) fn build_detail_sandbox(
    keyring: &pplx_space_common_svc::ecw_credential::EcwCredentialKeyring,
    sandbox: &Sandbox,
    native: &SandboxResponse,
    domain: Option<String>,
) -> Result<SandboxDetailE2b, E2bApiError> {
    let sandbox_state =
        translate_detail_sandbox_state(native).ok_or_else(|| terminal_sandbox_error(native))?;
    let runtime_spec = cached_runtime_spec(sandbox, native);
    let started_at = sandbox_started_at(sandbox, native);
    let metadata = sandbox_metadata(sandbox, native);

    Ok(SandboxDetailE2b {
        template_id: sandbox_template_id(sandbox, native, &runtime_spec),
        alias: None,
        sandbox_id: sandbox.id.to_string(),
        client_id: sandbox.id.to_string(),
        started_at: e2b_timestamp(started_at),
        end_at: e2b_timestamp(sandbox_end_at(sandbox, native)),
        envd_version: ECW_RESPONSE_DEFAULT_ENVD_VERSION.to_string(),
        envd_access_token: ecw_token(
            keyring,
            sandbox.id,
            sandbox.ecw_credential_epoch,
            EcwCredentialKind::Envd,
        ),
        domain,
        cpu_count: runtime_spec
            .cpu_count
            .unwrap_or(ECW_RESPONSE_DEFAULT_CPU_COUNT),
        memory_mb: runtime_spec
            .memory_mib
            .unwrap_or(ECW_RESPONSE_DEFAULT_MEMORY_MB),
        disk_size_mb: runtime_spec
            .disk_size_mib
            .unwrap_or(ECW_RESPONSE_DEFAULT_DISK_SIZE_MB),
        metadata,
        state: sandbox_state,
        volume_mounts: Vec::new(),
    })
}

fn cached_runtime_spec(sandbox: &Sandbox, native: &SandboxResponse) -> E2bRuntimeSpec {
    E2bRuntimeSpec {
        template_id: native
            .template
            .clone()
            .or_else(|| sandbox.template_id.clone()),
        cpu_count: sandbox.vcpu.and_then(|value| u32::try_from(value).ok()),
        memory_mib: sandbox
            .memory_mib
            .and_then(|value| u32::try_from(value).ok()),
        disk_size_mib: native
            .disk_usage_bytes
            .map(bytes_to_mib)
            .or_else(|| sandbox.disk_mib.and_then(|value| u32::try_from(value).ok())),
    }
}

pub(super) fn sandbox_template_id(
    sandbox: &Sandbox,
    native: &SandboxResponse,
    runtime_spec: &E2bRuntimeSpec,
) -> String {
    if let Some(template_id) = runtime_spec
        .template_id
        .clone()
        .or_else(|| native.template.clone())
        .or_else(|| sandbox.template_id.clone())
    {
        return template_id;
    }

    tracing::warn!(
        sandbox_id = %sandbox.id,
        "missing native template for ECW sandbox response; using default template"
    );
    ECW_DEFAULT_TEMPLATE_ID.to_string()
}

pub(super) fn sandbox_started_at(sandbox: &Sandbox, native: &SandboxResponse) -> OffsetDateTime {
    native.created_at.unwrap_or(sandbox.created_at)
}

pub(super) fn sandbox_end_at(sandbox: &Sandbox, native: &SandboxResponse) -> OffsetDateTime {
    if let Some(next_auto_event_at) = native
        .auto_lifecycle_status
        .as_ref()
        .and_then(|status| status.next_auto_event_at)
        .and_then(|value| i64::try_from(value).ok())
        .and_then(|value| OffsetDateTime::from_unix_timestamp(value).ok())
    {
        return next_auto_event_at;
    }

    let timeout_secs = native
        .auto_lifecycle
        .as_ref()
        .and_then(e2b_lifecycle_config_from_policy)
        .or_else(|| e2b_lifecycle_config_from_policy(&sandbox.auto_lifecycle.0))
        .map(|config| config.timeout_secs)
        .unwrap_or(ECW_RESPONSE_DEFAULT_TIMEOUT_SECS);

    sandbox_started_at(sandbox, native) + Duration::seconds(i64::from(timeout_secs))
}

pub(super) fn sandbox_metadata(sandbox: &Sandbox, native: &SandboxResponse) -> StringMap {
    let source = if native.metadata.is_empty() {
        &sandbox.metadata.0
    } else {
        &native.metadata
    };

    source
        .iter()
        .map(|(key, value)| (key.clone(), e2b_metadata_value(value)))
        .collect()
}

pub(super) fn e2b_metadata_value(value: &SandboxMetadataValue) -> String {
    match value {
        SandboxMetadataValue::String(value) => value.clone(),
        SandboxMetadataValue::Number(value) => value.to_string(),
        SandboxMetadataValue::Bool(value) => value.to_string(),
    }
}

pub(crate) fn translate_sandbox_state(native: &SandboxResponse) -> Option<SandboxStateE2b> {
    translate_lifecycle_state(raw_sandbox_lifecycle(native))
}

fn translate_detail_sandbox_state(native: &SandboxResponse) -> Option<SandboxStateE2b> {
    match terminal_sandbox_state(native) {
        Some(TerminalSandboxState::Failed) => Some(SandboxStateE2b::Failed),
        Some(TerminalSandboxState::Stopped) => None,
        None => translate_sandbox_state(native),
    }
}

pub(super) fn terminal_sandbox_error(native: &SandboxResponse) -> E2bApiError {
    match terminal_sandbox_state(native) {
        Some(TerminalSandboxState::Failed) => E2bApiError::failed_to_place_sandbox(),
        Some(TerminalSandboxState::Stopped) | None => E2bApiError::not_found("Sandbox"),
    }
}

fn raw_sandbox_lifecycle(native: &SandboxResponse) -> &str {
    native
        .status
        .as_deref()
        .or(native.lifecycle.as_deref())
        .unwrap_or("started")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TerminalSandboxState {
    Failed,
    Stopped,
}

fn terminal_sandbox_state(native: &SandboxResponse) -> Option<TerminalSandboxState> {
    terminal_state(raw_sandbox_lifecycle(native))
}

fn terminal_state(raw: &str) -> Option<TerminalSandboxState> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "failed" | "quarantined" => Some(TerminalSandboxState::Failed),
        "stopped" => Some(TerminalSandboxState::Stopped),
        _ => None,
    }
}

pub(super) fn translate_lifecycle_state(raw: &str) -> Option<SandboxStateE2b> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "paused" | "suspended" => Some(SandboxStateE2b::Paused),
        "stopped" | "failed" | "quarantined" => None,
        _ => Some(SandboxStateE2b::Running),
    }
}

pub(super) fn parse_metadata_query(
    metadata: Option<&str>,
) -> Result<Vec<(String, String)>, E2bApiError> {
    let Some(metadata) = metadata.filter(|metadata| !metadata.is_empty()) else {
        return Ok(Vec::new());
    };

    let mut seen = std::collections::HashSet::new();
    let mut filters = Vec::new();
    for pair in metadata.split('&') {
        let (key, value) = pair
            .split_once('=')
            .ok_or_else(|| E2bApiError::bad_request("Invalid metadata query"))?;
        let key = decode_metadata_component(key)?;
        let value = decode_metadata_component(value)?;
        // Reject duplicate keys explicitly. JSONB containment is single-value
        // per key, so silently collapsing a `metadata=env=prod&env=staging`
        // request would drop a caller-supplied predicate before SQL ran.
        // Better to fail fast at the boundary than to honor only the last
        // value and leave the caller wondering why their filter didn't
        // narrow.
        if !seen.insert(key.clone()) {
            return Err(E2bApiError::bad_request(format!(
                "duplicate metadata key '{key}' in filter"
            )));
        }
        filters.push((key, value));
    }
    Ok(filters)
}

fn decode_metadata_component(value: &str) -> Result<String, E2bApiError> {
    // The Python SDK percent-encodes components, then form-encodes them again.
    // Split pairs before decoding so encoded '&' and '=' stay inside values.
    // The HTTP extractor already handled the outer query and its '+' semantics.
    let value = percent_encoding::percent_decode_str(value)
        .decode_utf8()
        .map_err(|_| E2bApiError::bad_request("Invalid UTF-8 in metadata query"))?;
    percent_encoding::percent_decode_str(&value)
        .decode_utf8()
        .map(std::borrow::Cow::into_owned)
        .map_err(|_| E2bApiError::bad_request("Invalid UTF-8 in metadata query"))
}

pub(super) fn parse_state_filter(state: Option<&str>) -> Result<Vec<SandboxStateE2b>, E2bApiError> {
    let Some(state) = state.filter(|state| !state.is_empty()) else {
        return Ok(Vec::new());
    };

    state
        .split(',')
        .map(|value| match value.trim().to_ascii_lowercase().as_str() {
            "running" => Ok(SandboxStateE2b::Running),
            "paused" => Ok(SandboxStateE2b::Paused),
            _ => Err(E2bApiError::bad_request("Invalid sandbox state filter")),
        })
        .collect()
}

/// A budget-exhausted create/restore/wake wait carries a `Retry-After` hint
/// (the node is still converging): map it to the 502 `sandbox_timeout` shape
/// the E2B SDK has built-in recovery for (resume + retry) rather than an opaque
/// 5xx. The E2B SDK does not reliably auto-retry a 409 on these paths, so
/// keying off the retry hint — the "converging, retry the same sandbox"
/// marker — rather than a status code is deliberate.
fn sdk_recoverable_timeout(err: &ControllerError) -> Option<E2bApiError> {
    err.retry_after().map(|_| {
        E2bApiError::sandbox_timeout()
            .with_classification(ResponseOrigin::Cluster, ResponseKind::Timeout)
    })
}

/// A node-RPC timeout that escaped as a bare status code rather than a
/// budget-exhausted [`SubstrateError::Timeout`] (which carries the `retry_after`
/// marker). The dominant case is the per-request tonic channel `.timeout`
/// cutting a slow-but-not-stuck restore/access mid-RPC: tonic surfaces that as
/// `Code::Cancelled` → HTTP 408, and a gRPC deadline as `DeadlineExceeded` →
/// HTTP 504. Neither carries `retry_after`, and the SDK doesn't recover a bare
/// 408/504, so map both to the SDK-recoverable `sandbox_timeout` (502) shape.
pub(super) fn is_node_rpc_timeout(status: StatusCode) -> bool {
    status == StatusCode::REQUEST_TIMEOUT || status == StatusCode::GATEWAY_TIMEOUT
}

pub(super) fn controller_error(err: ControllerError) -> E2bApiError {
    // Before the status-based branches: a lock-race 503 is indistinguishable
    // from a fleet 503 by status alone, and the fallthrough below would label
    // it `Cluster`/`BackendStatus`.
    if err.is_lock_contention() {
        return E2bApiError::lock_contention();
    }
    if let Some(mapped) = sdk_recoverable_timeout(&err) {
        return mapped;
    }
    let retry_after_header = err.retry_after_header();
    let status = err.clone().into_response().status();
    if is_node_rpc_timeout(status) {
        return E2bApiError::sandbox_timeout()
            .with_classification(ResponseOrigin::Node, ResponseKind::Timeout);
    }
    let raw_message = err.to_string();
    if is_sandbox_not_running_error(status, &raw_message) {
        return E2bApiError::sandbox_timeout()
            .with_classification(ResponseOrigin::Cluster, ResponseKind::RouteNotReady);
    }
    if is_sandbox_wake_failed_precondition_error(status, &raw_message) {
        return E2bApiError::sandbox_timeout()
            .with_classification(ResponseOrigin::Cluster, ResponseKind::RouteNotReady);
    }
    if let Some(rate_limited) = rate_limited_e2b_error(&err) {
        return rate_limited;
    }

    let message = if status == StatusCode::INTERNAL_SERVER_ERROR {
        "Internal server error".to_string()
    } else {
        raw_message
    };

    let error = E2bApiError::new(status, message)
        .with_classification(ResponseOrigin::Cluster, ResponseKind::BackendStatus);
    match retry_after_header {
        Some(retry_after) => error.with_retry_after(retry_after),
        None => error,
    }
}

/// Preserve a cluster-activation admission rejection as a real ECW 429 +
/// `Retry-After` rather than collapsing it into a generic upstream 5xx. The
/// implicit access / wake paths introduce 429s that carry a header-only retry
/// hint, so `retry_after()` (the converging-timeout marker consumed by
/// `sdk_recoverable_timeout`) is `None` and they must be classified explicitly.
/// Returns `None` for any non-429 error so callers fall through to their normal
/// mapping.
pub(super) fn rate_limited_e2b_error(err: &ControllerError) -> Option<E2bApiError> {
    if err.clone().into_response().status() != StatusCode::TOO_MANY_REQUESTS {
        return None;
    }
    let error = E2bApiError::new(StatusCode::TOO_MANY_REQUESTS, err.to_string())
        .with_classification(ResponseOrigin::Cluster, ResponseKind::Admission);
    Some(match err.retry_after_header() {
        Some(retry_after) => error.with_retry_after(retry_after),
        None => error,
    })
}

pub(super) fn restore_controller_error(err: ControllerError) -> E2bApiError {
    let status = err.status();
    let raw_message = err.to_string();
    if is_failed_terminal_restore_conflict(status, &raw_message) {
        return E2bApiError::failed_to_place_sandbox();
    }

    controller_error(err)
}

fn is_failed_terminal_restore_conflict(status: StatusCode, message: &str) -> bool {
    status == StatusCode::CONFLICT
        && message.eq_ignore_ascii_case("sandbox cannot be restored while lifecycle is failed")
}

fn is_sandbox_not_running_error(status: StatusCode, message: &str) -> bool {
    status == StatusCode::CONFLICT && message.eq_ignore_ascii_case("sandbox is not running")
}

fn is_sandbox_wake_failed_precondition_error(status: StatusCode, message: &str) -> bool {
    // Preserve the existing retryable ECW response for this legacy access-wake
    // message. This is a message-specific compatibility rule, not a general
    // restriction on HTTP 412; missing restore context remains a precondition
    // failure rather than a timeout.
    status == StatusCode::PRECONDITION_FAILED && message.contains("cannot wake for access")
}

pub(super) fn bytes_to_mib(bytes: u64) -> u32 {
    let mib = bytes.saturating_add((1024 * 1024) - 1) / (1024 * 1024);
    mib.min(u32::MAX as u64) as u32
}

#[cfg(test)]
mod retry_translation_tests;
