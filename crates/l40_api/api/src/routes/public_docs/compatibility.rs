use serde_json::{Map, Value};
use std::collections::BTreeSet;
use thiserror::Error;

use super::OPENAPI_HTTP_METHODS;
use pplx_space_public_api::routes::{
    MAX_AGGREGATE_UTF8_BYTES_EXTENSION, MAX_UTF8_BYTES_EXTENSION, SSE_DATA_SCHEMA_EXTENSION,
};

type Result<T = ()> = std::result::Result<T, CompatibilityError>;

#[derive(Debug, Error)]
#[error("OpenAPI compatibility failure at {location}: {detail}")]
pub(super) struct CompatibilityError {
    location: String,
    detail: String,
}

pub(super) fn check_openapi_compatibility(baseline: &Value, current: &Value) -> Result {
    same(
        baseline.get("servers"),
        current.get("servers"),
        "servers changed",
    )?;
    same(
        baseline.get("security"),
        current.get("security"),
        "root security changed",
    )?;
    compare_security_schemes(baseline, current)?;
    let baseline_paths = object(baseline.get("paths"), "paths")?;
    let current_paths = object(current.get("paths"), "paths")?;
    for (path, baseline_item) in baseline_paths {
        let current_item = current_paths
            .get(path)
            .ok_or_else(|| fail(path, "path was removed"))?;
        for method in OPENAPI_HTTP_METHODS {
            let Some(baseline_operation) = baseline_item.get(method) else {
                continue;
            };
            let location = format!("{} {path}", method.to_uppercase());
            let current_operation = current_item
                .get(method)
                .ok_or_else(|| fail(&location, "operation was removed"))?;
            if baseline_operation.get("operationId") != current_operation.get("operationId") {
                return Err(fail(&location, "operationId changed"));
            }
            same(
                baseline_operation.get("security"),
                current_operation.get("security"),
                format!("{location} security changed"),
            )?;
            same(
                baseline_operation.get("servers"),
                current_operation.get("servers"),
                format!("{location} servers changed"),
            )?;
            let old = parameters(baseline, baseline_item, baseline_operation, &location)?;
            let new = parameters(current, current_item, current_operation, &location)?;
            compare_parameters(baseline, current, &old, &new, &location)?;
            compare_request_body(
                baseline,
                current,
                baseline_operation.get("requestBody"),
                current_operation.get("requestBody"),
                &location,
            )?;
            compare_responses(
                baseline,
                current,
                baseline_operation,
                current_operation,
                &location,
            )?;
        }
    }
    Ok(())
}

fn same(old: Option<&Value>, new: Option<&Value>, detail: impl Into<String>) -> Result {
    (old == new)
        .then_some(())
        .ok_or_else(|| fail("OpenAPI document", detail))
}

fn compare_security_schemes(baseline: &Value, current: &Value) -> Result {
    let old = baseline
        .pointer("/components/securitySchemes")
        .and_then(Value::as_object);
    let new = current
        .pointer("/components/securitySchemes")
        .and_then(Value::as_object);
    for (name, scheme) in old.into_iter().flatten() {
        if new.and_then(|schemes| schemes.get(name)) != Some(scheme) {
            return Err(fail(
                "components.securitySchemes",
                format!("security scheme {name} changed or was removed"),
            ));
        }
    }
    Ok(())
}

fn parameters<'a>(
    document: &'a Value,
    item: &'a Value,
    operation: &'a Value,
    location: &str,
) -> Result<Vec<&'a Value>> {
    let mut found: Vec<&Value> = Vec::new();
    for value in array(item.get("parameters"))
        .iter()
        .chain(array(operation.get("parameters")))
    {
        let value = resolve(document, value, "parameters", location)?;
        let identity = parameter_identity(value, location)?;
        if let Some(index) = found.iter().position(|candidate| {
            parameter_identity(candidate, location).is_ok_and(|v| v == identity)
        }) {
            found[index] = value;
        } else {
            found.push(value);
        }
    }
    Ok(found)
}

fn parameter_identity<'a>(value: &'a Value, location: &str) -> Result<(&'a str, String)> {
    let name = field(value, "name", location)?;
    let place = field(value, "in", location)?;
    Ok((
        place,
        if place == "header" {
            name.to_ascii_lowercase()
        } else {
            name.to_owned()
        },
    ))
}

fn compare_parameters(
    baseline: &Value,
    current: &Value,
    old: &[&Value],
    new: &[&Value],
    location: &str,
) -> Result {
    for parameter in old {
        let (place, name) = parameter_identity(parameter, location)?;
        let current_parameter = new
            .iter()
            .find(|value| {
                parameter_identity(value, location).is_ok_and(|v| v == (place, name.clone()))
            })
            .ok_or_else(|| fail(location, format!("{place} parameter {name} was removed")))?;
        if !required(parameter) && required(current_parameter) {
            return Err(fail(location, format!("parameter {name} became required")));
        }
        SchemaComparator::new(baseline, current, Direction::Request).optional(
            parameter.get("schema"),
            current_parameter.get("schema"),
            &format!("{location} parameter {name}"),
        )?;
    }
    for parameter in new.iter().filter(|value| required(value)) {
        let identity = parameter_identity(parameter, location)?;
        if !old
            .iter()
            .any(|value| parameter_identity(value, location).is_ok_and(|v| v == identity))
        {
            return Err(fail(
                location,
                format!("required {} parameter {} was added", identity.0, identity.1),
            ));
        }
    }
    Ok(())
}

fn compare_request_body(
    baseline: &Value,
    current: &Value,
    old: Option<&Value>,
    new: Option<&Value>,
    location: &str,
) -> Result {
    let Some(old) = old else {
        let added_required = new
            .map(|body| resolve(current, body, "requestBodies", location))
            .transpose()?
            .is_some_and(required);
        return if added_required {
            Err(fail(location, "required request body was added"))
        } else {
            Ok(())
        };
    };
    let new = new.ok_or_else(|| fail(location, "request body was removed"))?;
    let old = resolve(baseline, old, "requestBodies", location)?;
    let new = resolve(current, new, "requestBodies", location)?;
    if !required(old) && required(new) {
        return Err(fail(location, "request body became required"));
    }
    compare_content(
        baseline,
        current,
        old.get("content"),
        new.get("content"),
        Direction::Request,
        &format!("{location} request body"),
    )
}

fn compare_responses(
    baseline: &Value,
    current: &Value,
    old_operation: &Value,
    new_operation: &Value,
    location: &str,
) -> Result {
    let old = object(old_operation.get("responses"), location)?;
    let new = object(new_operation.get("responses"), location)?;
    if !old.keys().all(|status| new.contains_key(status)) {
        return Err(fail(location, "response status was removed"));
    }
    for (status, old_response) in old {
        let place = format!("{location} response {status}");
        let old_response = resolve(baseline, old_response, "responses", &place)?;
        let new_response = resolve(current, &new[status], "responses", &place)?;
        compare_headers(baseline, current, old_response, new_response, &place)?;
        compare_content(
            baseline,
            current,
            old_response.get("content"),
            new_response.get("content"),
            Direction::Response,
            &place,
        )?;
    }
    Ok(())
}

fn compare_headers(
    baseline: &Value,
    current: &Value,
    old_response: &Value,
    new_response: &Value,
    location: &str,
) -> Result {
    let old = object_or_empty(old_response.get("headers"));
    let new = object_or_empty(new_response.get("headers"));
    for (name, old_header) in old {
        let new_header = new
            .iter()
            .find_map(|(candidate, value)| candidate.eq_ignore_ascii_case(name).then_some(value))
            .ok_or_else(|| fail(location, format!("response header {name} was removed")))?;
        let old_header = resolve(baseline, old_header, "headers", location)?;
        let new_header = resolve(current, new_header, "headers", location)?;
        SchemaComparator::new(current, baseline, Direction::Response).optional(
            new_header.get("schema"),
            old_header.get("schema"),
            &format!("{location} header {name}"),
        )?;
    }
    Ok(())
}

fn compare_content(
    baseline: &Value,
    current: &Value,
    old: Option<&Value>,
    new: Option<&Value>,
    direction: Direction,
    location: &str,
) -> Result {
    let old = object_or_empty(old);
    let new = object_or_empty(new);
    if !old.keys().all(|media_type| new.contains_key(media_type)) {
        return Err(fail(location, "media type was removed"));
    }
    for (media_type, old_media) in old {
        let new_media = &new[media_type];
        let place = format!("{location} {media_type}");
        for keyword in ["schema", SSE_DATA_SCHEMA_EXTENSION] {
            let at = if keyword == "schema" {
                place.clone()
            } else {
                format!("{place} {keyword}")
            };
            match direction {
                Direction::Request => SchemaComparator::new(baseline, current, direction)
                    .optional(old_media.get(keyword), new_media.get(keyword), &at)?,
                Direction::Response => SchemaComparator::new(current, baseline, direction)
                    .optional(new_media.get(keyword), old_media.get(keyword), &at)?,
            }
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum Direction {
    Request,
    Response,
}

// Proves baseline ⊆ current for requests and current ⊆ baseline for responses.
struct SchemaComparator<'a> {
    subset_document: &'a Value,
    superset_document: &'a Value,
    direction: Direction,
    seen: BTreeSet<(String, String)>,
}

#[rustfmt::skip]
impl<'a> SchemaComparator<'a> {
    fn new(subset_document: &'a Value, superset_document: &'a Value, direction: Direction) -> Self {
        Self {
            subset_document,
            superset_document,
            direction,
            seen: BTreeSet::new(),
        }
    }

    fn optional(&mut self, subset: Option<&Value>, superset: Option<&Value>, location: &str) -> Result {
        let subset = match subset {
            Some(schema) => schema,
            None => empty(),
        };
        let superset = match superset {
            Some(schema) => schema,
            None => empty(),
        };
        self.schema(subset, superset, location)
    }

    fn schema(&mut self, subset: &Value, superset: &Value, location: &str) -> Result {
        if subset == &Value::Bool(false) || superset == &Value::Bool(true) {
            return Ok(());
        }
        if subset.is_boolean() || superset.is_boolean() {
            return Err(fail(location, self.breakage("boolean schema")));
        }
        validate_schema(subset, location)?;
        validate_schema(superset, location)?;
        let subset = resolve_schema(self.subset_document, subset, location)?;
        let superset = resolve_schema(self.superset_document, superset, location)?;
        if !self.seen.insert((subset.to_string(), superset.to_string())) {
            return Ok(());
        }
        if let Some(variants) = union(subset) {
            for (index, variant) in variants.iter().enumerate() {
                let variant = resolve_schema(self.subset_document, variant, location)?;
                self.schema(
                    &branch(subset, variant, location)?,
                    superset,
                    &format!("{location} variant {index}"),
                )?;
            }
            return Ok(());
        }
        if let Some(variants) = union(superset) {
            let mut reasons = Vec::new();
            for variant in variants {
                let saved = self.seen.clone();
                let variant = resolve_schema(self.superset_document, variant, location)?;
                match self.schema(subset, &branch(superset, variant, location)?, location) {
                    Ok(()) => return Ok(()),
                    Err(error) => reasons.push(error.detail),
                }
                self.seen = saved;
            }
            return Err(fail(location, format!("not accepted by any union variant: {}", reasons.join("; "))));
        }
        compare_values(subset, superset, self.direction, location)?;
        compare_types(subset, superset, self.direction, location)?;
        compare_bounds(subset, superset, self.direction, location)?;
        compare_exact(subset, superset, self.direction, location)?;
        self.compare_not(subset, superset, location)?;
        self.compare_object(subset, superset, location)?;
        self.keyword(subset, superset, "items", location)?;
        self.keyword(subset, superset, "propertyNames", location)
    }

    fn compare_not(&mut self, subset: &Value, superset: &Value, location: &str) -> Result {
        match (subset.get("not"), superset.get("not")) {
            (_, None) => Ok(()),
            (None, Some(excluded))
                if values(subset).is_some_and(|allowed| values(excluded).is_some_and(|excluded| allowed.is_disjoint(&excluded))) =>
            {
                Ok(())
            }
            (None, Some(_)) => Err(fail(location, self.breakage("exclusions"))),
            (Some(subset_not), Some(superset_not)) => {
                std::mem::swap(&mut self.subset_document, &mut self.superset_document);
                let result = self.schema(superset_not, subset_not, &format!("{location}.not"));
                std::mem::swap(&mut self.subset_document, &mut self.superset_document);
                result
            }
        }
    }

    fn compare_object(&mut self, subset: &Value, superset: &Value, location: &str) -> Result {
        let subset_required = strings(subset.get("required"));
        let superset_required = strings(superset.get("required"));
        if !superset_required.is_subset(&subset_required) {
            return Err(fail(location, self.breakage("required fields")));
        }
        let subset_properties = object_or_empty(subset.get("properties"));
        let superset_properties = object_or_empty(superset.get("properties"));
        let subset_additional = additional(subset.get("additionalProperties"));
        let superset_additional = additional(superset.get("additionalProperties"));
        for (name, expected) in superset_properties {
            let actual = subset_properties.get(name);
            match (actual, subset_additional) {
                (Some(actual), _) => self.schema(actual, expected, &format!("{location}.{name}"))?,
                // A property the baseline never declared, on a schema that
                // accepted anything extra. For a request that means the
                // server took the key and ignored it, so declaring it now is
                // additive: no client could have depended on a value that
                // never reached behaviour. Comparing `{}` against the new
                // schema would instead read every typed addition as a
                // narrowing and freeze the request body permanently.
                //
                // Requests only. The response direction reaches this arm when
                // the baseline documents a property the current contract has
                // dropped, which is a real break for a reader.
                (None, Additional::Any) if matches!(self.direction, Direction::Request) => {}
                (None, Additional::Any) => self.schema(empty(), expected, &format!("{location}.{name}"))?,
                (None, Additional::Schema(actual)) => self.schema(actual, expected, &format!("{location}.{name}"))?,
                (None, Additional::Forbidden) => {}
            }
        }
        for (name, actual) in subset_properties {
            if superset_properties.contains_key(name) {
                continue;
            }
            match superset_additional {
                Additional::Any => {}
                Additional::Forbidden => {
                    return Err(fail(location, format!("property {name} is no longer accepted")));
                }
                Additional::Schema(expected) => self.schema(actual, expected, &format!("{location}.{name}"))?,
            }
        }
        match (subset_additional, superset_additional) {
            (Additional::Forbidden, _) | (_, Additional::Any) => Ok(()),
            (Additional::Any, _) => Err(fail(location, self.breakage("additional properties"))),
            (Additional::Schema(_), Additional::Forbidden) => Err(fail(location, self.breakage("additional properties"))),
            (Additional::Schema(actual), Additional::Schema(expected)) => self.schema(actual, expected, &format!("{location}.*")),
        }
    }

    fn keyword(&mut self, subset: &Value, superset: &Value, keyword: &str, location: &str) -> Result {
        match (subset.get(keyword), superset.get(keyword)) {
            (_, None) => Ok(()),
            (None, Some(_)) => Err(fail(location, self.breakage(keyword))),
            (Some(actual), Some(expected)) => self.schema(actual, expected, &format!("{location}.{keyword}")),
        }
    }

    fn breakage(&self, subject: &str) -> String {
        format!(
            "{} {subject} became {} restrictive",
            self.direction.name(),
            match self.direction {
                Direction::Request => "more",
                Direction::Response => "less",
            }
        )
    }
}

#[derive(Clone, Copy)]
enum Additional<'a> {
    Forbidden,
    Any,
    Schema(&'a Value),
}

fn additional(value: Option<&Value>) -> Additional<'_> {
    match value {
        Some(Value::Bool(false)) => Additional::Forbidden,
        Some(schema @ Value::Object(_)) => Additional::Schema(schema),
        _ => Additional::Any,
    }
}

impl Direction {
    fn name(self) -> &'static str {
        match self {
            Self::Request => "request",
            Self::Response => "response",
        }
    }
}

#[rustfmt::skip]
mod constraints {
use super::*;

pub(super) fn compare_values(subset: &Value, superset: &Value, direction: Direction, location: &str) -> Result {
    let subset_values = values(subset);
    let superset_values = values(superset);
    if matches!(direction, Direction::Response)
        && subset_values.is_none()
        && superset_values.as_ref().is_some_and(|old| opened_string_enum(subset, superset, old))
    {
        return Ok(());
    }
    if matches!((&subset_values, &superset_values), (None, Some(_))) || matches!((&subset_values, &superset_values), (Some(a), Some(b)) if !a.is_subset(b)) {
        return Err(fail(location, format!("{} enum or const became {} restrictive", direction.name(), if matches!(direction, Direction::Request) { "more" } else { "less" })));
    }
    Ok(())
}

/// A response `enum` of strings may open into an extensible `type: string`
/// (MVP RFC §7: output vocabularies grow additively). The new schema has to
/// keep documenting every previously closed value as an `example`; dropping
/// the enum without that is a lost contract, not an opening.
fn opened_string_enum(current: &Value, baseline: &Value, old_values: &BTreeSet<String>) -> bool {
    let all_strings = baseline
        .get("enum")
        .and_then(Value::as_array)
        .is_some_and(|values| values.iter().all(Value::is_string));
    let plain_string = schema_types(current).is_some_and(|types| types == BTreeSet::from(["string".to_owned()]));
    let documented: BTreeSet<String> = current
        .get("examples")
        .and_then(Value::as_array)
        .map(|examples| examples.iter().map(Value::to_string).collect())
        .unwrap_or_default();
    all_strings && plain_string && old_values.is_subset(&documented)
}

pub(super) fn compare_types(subset: &Value, superset: &Value, direction: Direction, location: &str) -> Result {
    let subset = schema_types(subset).or_else(|| value_types(subset));
    let superset = schema_types(superset);
    let contained = |a: &BTreeSet<String>, b: &BTreeSet<String>| {
        a.iter()
            .all(|value| b.contains(value) || (value == "integer" && b.contains("number")))
    };
    if matches!((&subset, &superset), (None, Some(_))) || matches!((&subset, &superset), (Some(a), Some(b)) if !contained(a, b)) {
        return Err(fail(
            location,
            format!(
                "{} schema types became {} restrictive",
                direction.name(),
                if matches!(direction, Direction::Request) { "more" } else { "less" }
            ),
        ));
    }
    Ok(())
}

pub(super) fn compare_bounds(subset: &Value, superset: &Value, direction: Direction, location: &str) -> Result {
    for (keyword, lower) in [
        ("minimum", true),
        ("minLength", true),
        ("minItems", true),
        ("minProperties", true),
        ("maximum", false),
        ("maxLength", false),
        ("maxItems", false),
        ("maxProperties", false),
        (MAX_AGGREGATE_UTF8_BYTES_EXTENSION, false),
        (MAX_UTF8_BYTES_EXTENSION, false),
    ] {
        let actual = bound(subset, keyword);
        let expected = bound(superset, keyword);
        let compatible = match (actual, expected) {
            (_, None) => true,
            (None, Some(_)) => false,
            (Some((a, ax)), Some((b, bx))) if lower => a > b || (a.partial_cmp(&b) == Some(std::cmp::Ordering::Equal) && (ax || !bx)),
            (Some((a, ax)), Some((b, bx))) => a < b || (a.partial_cmp(&b) == Some(std::cmp::Ordering::Equal) && (ax || !bx)),
        };
        if !compatible {
            return Err(fail(
                location,
                format!(
                    "{} {keyword} became {} restrictive",
                    direction.name(),
                    if matches!(direction, Direction::Request) { "more" } else { "less" }
                ),
            ));
        }
    }
    Ok(())
}

fn bound(schema: &Value, keyword: &str) -> Option<(f64, bool)> {
    let exclusive = match keyword {
        "minimum" => "exclusiveMinimum",
        "maximum" => "exclusiveMaximum",
        _ => "",
    };
    schema
        .get(exclusive)
        .and_then(Value::as_f64)
        .map(|value| (value, true))
        .or_else(|| schema.get(keyword).and_then(Value::as_f64).map(|value| (value, false)))
}

pub(super) fn compare_exact(subset: &Value, superset: &Value, direction: Direction, location: &str) -> Result {
    for keyword in ["pattern", "format"] {
        if let Some(expected) = superset.get(keyword)
            && subset.get(keyword) != Some(expected)
            && !(keyword == "pattern" && pattern_contains(expected, subset.get(keyword)))
        {
            return Err(fail(location, format!("{} {keyword} changed incompatibly", direction.name())));
        }
    }
    Ok(())
}

/// Whether every string `subset` matches is provably matched by `superset`.
///
/// A pattern matches when any of its top-level alternatives does, so a
/// superset that keeps each of the subset's alternatives verbatim matches at
/// least as much. Anything else needs regex containment, which this does not
/// attempt: a reordered or rewritten alternative is reported as a change.
fn pattern_contains(superset: &Value, subset: Option<&Value>) -> bool {
    let Some(superset) = superset.as_str() else {
        return false;
    };
    if matches!(superset, ".*" | "^.*$") {
        return true;
    }
    let (Some(wide), Some(narrow)) = (
        top_level_alternatives(superset),
        subset.and_then(Value::as_str).and_then(top_level_alternatives),
    ) else {
        return false;
    };
    narrow.iter().all(|alternative| wide.contains(alternative))
}

/// Splits an ECMA-262 pattern at the `|`s outside groups and character
/// classes, or returns `None` when its groups or classes do not balance.
fn top_level_alternatives(pattern: &str) -> Option<Vec<&str>> {
    let mut alternatives = Vec::new();
    let (mut depth, mut in_class, mut escaped, mut start) = (0usize, false, false, 0);
    for (index, character) in pattern.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        match character {
            '\\' => escaped = true,
            '[' if !in_class => in_class = true,
            ']' if in_class => in_class = false,
            '(' if !in_class => depth += 1,
            ')' if !in_class => depth = depth.checked_sub(1)?,
            '|' if !in_class && depth == 0 => {
                alternatives.push(pattern.get(start..index)?);
                start = index + 1;
            }
            _ => {}
        }
    }
    if escaped || in_class || depth != 0 {
        return None;
    }
    alternatives.push(pattern.get(start..)?);
    Some(alternatives)
}

pub(super) fn union(schema: &Value) -> Option<&Vec<Value>> {
    schema
        .get("oneOf")
        .or_else(|| schema.get("anyOf"))
        .and_then(Value::as_array)
}

pub(super) fn branch(schema: &Value, variant: &Value, location: &str) -> Result<Value> {
    let mut combined = schema
        .as_object()
        .cloned()
        .ok_or_else(|| fail(location, "union must be an object"))?;
    combined.remove("oneOf");
    combined.remove("anyOf");
    for (keyword, value) in variant
        .as_object()
        .ok_or_else(|| fail(location, "union variant must be an object"))?
    {
        if annotation(keyword) {
            combined.insert(keyword.clone(), value.clone());
        } else if keyword == "required" {
            let mut required = strings(combined.get("required"));
            required.extend(strings(Some(value)));
            combined.insert(keyword.clone(), Value::Array(required.into_iter().map(Value::String).collect()));
        } else if combined
            .insert(keyword.clone(), value.clone())
            .is_some_and(|sibling| sibling != *value)
        {
            return Err(fail(location, format!("union variant overlaps sibling keyword {keyword}")));
        }
    }
    Ok(Value::Object(combined))
}

pub(super) fn validate_schema(schema: &Value, location: &str) -> Result {
    const MODELED: &str = "$ref type enum const oneOf anyOf not required properties additionalProperties items propertyNames minimum exclusiveMinimum maximum exclusiveMaximum minLength maxLength minItems maxItems minProperties maxProperties pattern format";
    let Some(object) = schema.as_object() else {
        return Err(fail(location, "schema must be an object"));
    };
    if object.contains_key("oneOf") && object.contains_key("anyOf") {
        return Err(fail(location, "oneOf and anyOf cannot be combined"));
    }
    if object.contains_key("$ref") && object.keys().any(|key| key != "$ref" && !annotation(key)) {
        return Err(fail(location, "$ref siblings are not supported"));
    }
    if let Some(keyword) = object
        .keys()
        .find(|key| !MODELED.split_whitespace().any(|known| known == *key) && !annotation(key) && !key.starts_with("x-"))
    {
        return Err(fail(location, format!("schema keyword {keyword} is not supported")));
    }
    Ok(())
}

fn annotation(keyword: &str) -> bool {
    const ANNOTATIONS: &str = "title description default deprecated readOnly writeOnly examples example $comment discriminator xml externalDocs";
    ANNOTATIONS.split_whitespace().any(|known| known == keyword)
}

pub(super) fn resolve_schema<'a>(document: &'a Value, schema: &'a Value, location: &str) -> Result<&'a Value> {
    let Some(name) = schema
        .get("$ref")
        .and_then(Value::as_str)
        .and_then(|value| value.strip_prefix("#/components/schemas/"))
    else {
        return Ok(schema);
    };
    document
        .pointer(&format!("/components/schemas/{name}"))
        .ok_or_else(|| fail(location, format!("component schema {name} is missing")))
}

pub(super) fn resolve<'a>(document: &'a Value, value: &'a Value, kind: &str, location: &str) -> Result<&'a Value> {
    let prefix = format!("#/components/{kind}/");
    let Some(name) = value
        .get("$ref")
        .and_then(Value::as_str)
        .and_then(|value| value.strip_prefix(&prefix))
    else {
        return Ok(value);
    };
    document
        .pointer(&format!("/components/{kind}/{name}"))
        .ok_or_else(|| fail(location, format!("component {kind} {name} is missing")))
}

pub(super) fn values(schema: &Value) -> Option<BTreeSet<String>> {
    if let Some(value) = schema.get("const") {
        return Some(BTreeSet::from([value.to_string()]));
    }
    schema
        .get("enum")
        .and_then(Value::as_array)
        .map(|values| values.iter().map(Value::to_string).collect())
}

fn schema_types(schema: &Value) -> Option<BTreeSet<String>> {
    match schema.get("type") {
        Some(Value::String(value)) => Some(BTreeSet::from([value.clone()])),
        Some(Value::Array(values)) => Some(values.iter().filter_map(Value::as_str).map(str::to_owned).collect()),
        _ => None,
    }
}

fn value_types(schema: &Value) -> Option<BTreeSet<String>> {
    let types = schema
        .get("const")
        .into_iter()
        .chain(schema.get("enum").and_then(Value::as_array).into_iter().flatten())
        .map(|value| match value {
            Value::Null => "null",
            Value::Bool(_) => "boolean",
            Value::Number(number) if number.is_i64() || number.is_u64() => "integer",
            Value::Number(_) => "number",
            Value::String(_) => "string",
            Value::Array(_) => "array",
            Value::Object(_) => "object",
        })
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();
    (!types.is_empty()).then_some(types)
}
}

use constraints::*;

fn strings(value: Option<&Value>) -> BTreeSet<String> {
    value
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect()
}

fn object<'a>(value: Option<&'a Value>, location: &str) -> Result<&'a Map<String, Value>> {
    value
        .and_then(Value::as_object)
        .ok_or_else(|| fail(location, "expected an object"))
}

fn object_or_empty(value: Option<&Value>) -> &Map<String, Value> {
    static EMPTY: std::sync::LazyLock<Map<String, Value>> = std::sync::LazyLock::new(Map::new);
    value.and_then(Value::as_object).unwrap_or(&EMPTY)
}

fn empty() -> &'static Value {
    static EMPTY: std::sync::LazyLock<Value> =
        std::sync::LazyLock::new(|| Value::Object(Map::new()));
    &EMPTY
}

fn array(value: Option<&Value>) -> &[Value] {
    value
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default()
}

fn field<'a>(value: &'a Value, field: &str, location: &str) -> Result<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| fail(location, format!("parameter {field} is missing")))
}

fn required(value: &Value) -> bool {
    value.get("required") == Some(&Value::Bool(true))
}

fn fail(location: impl Into<String>, detail: impl Into<String>) -> CompatibilityError {
    CompatibilityError {
        location: location.into(),
        detail: detail.into(),
    }
}

#[cfg(test)]
mod tests;
