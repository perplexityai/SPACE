//! The strict, versioned JSON decoder.
//!
//! `serde_json` only tokenizes here. The text is read into [`Json`], which
//! keeps every object entry in document order, repeated keys included, and the
//! grammar is then checked by hand. A derived `Deserialize` would be shorter,
//! but its errors are free text classified only as syntax or data, located at
//! the enclosing element, and inconsistent across shapes: a repeated key is
//! `duplicate field` in a struct, `expected value` in an externally tagged
//! enum, and lost in `did not match any variant` inside an untagged one. The
//! hand-written grammar gives every failure a typed [`DecodeErrorKind`] at the
//! exact [`JsonPath`] that the CLI and the Admin API can both map, and checks
//! repeated keys the same way in every object.

use std::collections::HashSet;
use std::fmt;
use std::net::Ipv4Addr;
use std::num::NonZeroU64;

use pplx_space_firewall_rules::{
    FirewallRuleRevision, HeaderField, HeaderProtocol, IntegerRange, Ipv4Prefix, Match, NameError,
    PayloadHeader, Read, Rule, RuleId, RuleRevisionError, TcpOptionName, Value,
};
use serde::de::{self, Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};

use crate::SCHEMA_VERSION;

const EQUALITY: &str = "==";

/// Decodes one complete revision document.
pub fn decode_revision(text: &str) -> Result<FirewallRuleRevision, DecodeError> {
    let json = serde_json::from_str(text).map_err(DecodeError::syntax)?;
    decode_document(&json)
}

/// Decodes one complete revision document from UTF-8 bytes.
pub fn decode_revision_bytes(bytes: &[u8]) -> Result<FirewallRuleRevision, DecodeError> {
    let json = serde_json::from_slice(bytes).map_err(DecodeError::syntax)?;
    decode_document(&json)
}

/// A firewall rules document that does not follow the grammar.
#[derive(Debug, thiserror::Error)]
#[error("{path}: {kind}")]
pub struct DecodeError {
    path: JsonPath,
    kind: DecodeErrorKind,
}

impl DecodeError {
    fn new(path: &JsonPath, kind: DecodeErrorKind) -> Self {
        Self {
            path: path.clone(),
            kind,
        }
    }

    fn syntax(error: serde_json::Error) -> Self {
        Self {
            path: JsonPath::root(),
            kind: DecodeErrorKind::Syntax(error),
        }
    }

    /// Where the problem is. Syntax errors carry the root path; their line and
    /// column are in the kind.
    pub fn path(&self) -> &JsonPath {
        &self.path
    }

    pub fn kind(&self) -> &DecodeErrorKind {
        &self.kind
    }
}

/// What is wrong at a [`DecodeError`]'s path.
#[derive(Debug, thiserror::Error)]
pub enum DecodeErrorKind {
    /// serde_json could not parse the text, for example because it is not
    /// JSON or holds a number too large for a 64-bit float such as `1e400`.
    /// This fails before any element has a path, so the path is the root and
    /// the error's line and column give the location.
    #[error("malformed JSON at line {line}, column {column}", line = .0.line(), column = .0.column())]
    Syntax(#[source] serde_json::Error),
    #[error("expected {expected}, found {found}")]
    WrongType {
        expected: &'static str,
        found: JsonKind,
    },
    #[error("unsupported schema version {found}; only {SCHEMA_VERSION} is supported")]
    UnsupportedSchemaVersion { found: i128 },
    #[error("missing required field {field:?}")]
    MissingField { field: &'static str },
    #[error("unexpected field {field:?}")]
    UnexpectedField { field: String },
    #[error("field {field:?} appears more than once")]
    DuplicateField { field: String },
    #[error("expected exactly one of {expected}, found none")]
    MissingVariant { expected: &'static str },
    #[error("{first:?} and {second:?} cannot appear in the same object")]
    CompetingVariants { first: String, second: String },
    #[error("expected a {expected}-element array, found {found} elements")]
    TupleLength { expected: usize, found: usize },
    #[error("unsupported operator {op:?}; only \"==\" is supported")]
    UnsupportedOperator { op: String },
    #[error("all_of must contain at least one comparison")]
    EmptyAllOf,
    #[error("mask must be nonzero")]
    ZeroMask,
    #[error("{text:?} is not an IPv4 address in dotted-quad form")]
    InvalidIpv4 { text: String },
    #[error("IPv4 prefix length {len} is outside 0..=32")]
    PrefixLength { len: u64 },
    #[error("range start {start} exceeds its end {end}")]
    ReversedRange { start: u64, end: u64 },
    #[error("rule ID {id:?} repeats the rule at index {first_index}", id = .id.as_str())]
    DuplicateRuleId { id: RuleId, first_index: usize },
    #[error(transparent)]
    InvalidName(NameError),
}

/// The JSON type found where another was expected.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JsonKind {
    Null,
    True,
    False,
    UnsignedInteger,
    NegativeInteger,
    /// A number with a fraction or exponent, or an integer above `u64::MAX`.
    OtherNumber,
    String,
    Array,
    Object,
}

impl JsonKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Null => "null",
            Self::True => "true",
            Self::False => "false",
            Self::UnsignedInteger => "an unsigned integer",
            Self::NegativeInteger => "a negative integer",
            Self::OtherNumber => "a number that is not an unsigned 64-bit integer",
            Self::String => "a string",
            Self::Array => "an array",
            Self::Object => "an object",
        }
    }
}

impl fmt::Display for JsonKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The location of an element in a firewall rules document, written as an
/// RFC 9535 JSONPath such as `$.rules[1].all_of[0].left` or `$["123"]`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JsonPath(Vec<PathSegment>);

/// One step of a [`JsonPath`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PathSegment {
    Key(String),
    Index(usize),
}

impl JsonPath {
    fn root() -> Self {
        Self(Vec::new())
    }

    fn key(&self, key: &str) -> Self {
        let mut path = self.clone();
        path.0.push(PathSegment::Key(key.to_owned()));
        path
    }

    fn index(&self, index: usize) -> Self {
        let mut path = self.clone();
        path.0.push(PathSegment::Index(index));
        path
    }

    pub fn segments(&self) -> &[PathSegment] {
        &self.0
    }
}

impl fmt::Display for JsonPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("$")?;
        for segment in &self.0 {
            match segment {
                PathSegment::Key(key) if is_member_name_shorthand(key) => write!(f, ".{key}")?,
                PathSegment::Key(key) => {
                    // A JSON string literal is a valid RFC 9535 name selector.
                    let quoted = serde_json::to_string(key).map_err(|_| fmt::Error)?;
                    write!(f, "[{quoted}]")?;
                }
                PathSegment::Index(index) => write!(f, "[{index}]")?,
            }
        }
        Ok(())
    }
}

/// Whether RFC 9535 lets `key` follow a dot: an ASCII letter or `_`, then
/// ASCII letters, digits or `_`. Any other key, including a non-ASCII one the
/// RFC would also allow, is written as a bracketed JSON string.
fn is_member_name_shorthand(key: &str) -> bool {
    let mut chars = key.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// A parsed JSON value that keeps every object entry, repeated keys included,
/// in document order.
#[derive(Debug)]
enum Json {
    Null,
    Bool(bool),
    Unsigned(u64),
    Negative(i64),
    OtherNumber,
    String(String),
    Array(Vec<Json>),
    Object(Vec<(String, Json)>),
}

impl Json {
    /// The value of an integer of either sign.
    fn integer(&self) -> Option<i128> {
        match *self {
            Self::Unsigned(value) => Some(i128::from(value)),
            Self::Negative(value) => Some(i128::from(value)),
            _ => None,
        }
    }

    fn kind(&self) -> JsonKind {
        match self {
            Self::Null => JsonKind::Null,
            Self::Bool(true) => JsonKind::True,
            Self::Bool(false) => JsonKind::False,
            Self::Unsigned(_) => JsonKind::UnsignedInteger,
            Self::Negative(_) => JsonKind::NegativeInteger,
            Self::OtherNumber => JsonKind::OtherNumber,
            Self::String(_) => JsonKind::String,
            Self::Array(_) => JsonKind::Array,
            Self::Object(_) => JsonKind::Object,
        }
    }
}

impl<'de> Deserialize<'de> for Json {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(JsonVisitor)
    }
}

struct JsonVisitor;

impl<'de> Visitor<'de> for JsonVisitor {
    type Value = Json;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a JSON value")
    }

    fn visit_unit<E: de::Error>(self) -> Result<Json, E> {
        Ok(Json::Null)
    }

    fn visit_bool<E: de::Error>(self, value: bool) -> Result<Json, E> {
        Ok(Json::Bool(value))
    }

    fn visit_u64<E: de::Error>(self, value: u64) -> Result<Json, E> {
        Ok(Json::Unsigned(value))
    }

    fn visit_i64<E: de::Error>(self, value: i64) -> Result<Json, E> {
        Ok(match u64::try_from(value) {
            Ok(value) => Json::Unsigned(value),
            Err(_) => Json::Negative(value),
        })
    }

    fn visit_f64<E: de::Error>(self, _value: f64) -> Result<Json, E> {
        Ok(Json::OtherNumber)
    }

    fn visit_str<E: de::Error>(self, value: &str) -> Result<Json, E> {
        Ok(Json::String(value.to_owned()))
    }

    fn visit_string<E: de::Error>(self, value: String) -> Result<Json, E> {
        Ok(Json::String(value))
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Json, A::Error> {
        let mut elements = Vec::new();
        while let Some(element) = seq.next_element()? {
            elements.push(element);
        }
        Ok(Json::Array(elements))
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Json, A::Error> {
        let mut entries = Vec::new();
        while let Some((key, value)) = map.next_entry::<String, Json>()? {
            entries.push((key, value));
        }
        Ok(Json::Object(entries))
    }
}

/// The entries of one JSON object, checked for repeated and unexpected keys.
struct Fields<'a> {
    path: &'a JsonPath,
    entries: &'a [(String, Json)],
}

impl<'a> Fields<'a> {
    /// Reads `json` as an object whose keys are all in `allowed`, each at most
    /// once. A repeated key is reported before an unexpected one.
    fn new(
        json: &'a Json,
        path: &'a JsonPath,
        expected: &'static str,
        allowed: &[&str],
    ) -> Result<Self, DecodeError> {
        let Json::Object(entries) = json else {
            return Err(wrong_type(path, expected, json));
        };
        let mut seen = HashSet::with_capacity(entries.len());
        if let Some((key, _)) = entries.iter().find(|(key, _)| !seen.insert(key.as_str())) {
            return Err(DecodeError::new(
                &path.key(key),
                DecodeErrorKind::DuplicateField { field: key.clone() },
            ));
        }
        if let Some((key, _)) = entries
            .iter()
            .find(|(key, _)| !allowed.contains(&key.as_str()))
        {
            return Err(unexpected_field(&path.key(key), key));
        }
        Ok(Self { path, entries })
    }

    /// The value of a required field. `null` is reported as the wrong type, not
    /// as an absent field defaulted to anything.
    fn required(&self, field: &'static str) -> Result<(&'a Json, JsonPath), DecodeError> {
        self.entries
            .iter()
            .find(|(key, _)| key == field)
            .map(|(_, value)| (value, self.path.key(field)))
            .ok_or_else(|| DecodeError::new(self.path, DecodeErrorKind::MissingField { field }))
    }

    /// The single entry of an object that names exactly one variant.
    fn variant(&self, expected: &'static str) -> Result<(&'a str, &'a Json), DecodeError> {
        match self.entries {
            [(key, value)] => Ok((key, value)),
            [] => Err(DecodeError::new(
                self.path,
                DecodeErrorKind::MissingVariant { expected },
            )),
            [(first, _), (second, _), ..] => Err(DecodeError::new(
                self.path,
                DecodeErrorKind::CompetingVariants {
                    first: first.clone(),
                    second: second.clone(),
                },
            )),
        }
    }
}

fn unexpected_field(path: &JsonPath, field: &str) -> DecodeError {
    DecodeError::new(
        path,
        DecodeErrorKind::UnexpectedField {
            field: field.to_owned(),
        },
    )
}

fn wrong_type(path: &JsonPath, expected: &'static str, found: &Json) -> DecodeError {
    DecodeError::new(
        path,
        DecodeErrorKind::WrongType {
            expected,
            found: found.kind(),
        },
    )
}

const REVISION: &str = "a revision object";

/// Reads the schema version, then decodes the rest with that version's
/// grammar. The version is read before any other field is checked, so a newer
/// document is reported as an unsupported version, not as unexpected fields.
fn decode_document(json: &Json) -> Result<FirewallRuleRevision, DecodeError> {
    let root = JsonPath::root();
    let Json::Object(entries) = json else {
        return Err(wrong_type(&root, REVISION, json));
    };
    let version_path = root.key("schema_version");
    let mut versions = entries.iter().filter(|(key, _)| key == "schema_version");
    let Some((_, version)) = versions.next() else {
        return Err(DecodeError::new(
            &root,
            DecodeErrorKind::MissingField {
                field: "schema_version",
            },
        ));
    };
    if versions.next().is_some() {
        return Err(DecodeError::new(
            &version_path,
            DecodeErrorKind::DuplicateField {
                field: "schema_version".to_owned(),
            },
        ));
    }
    let Some(found) = version.integer() else {
        return Err(wrong_type(
            &version_path,
            "an integer schema version",
            version,
        ));
    };
    match u64::try_from(found) {
        Ok(SCHEMA_VERSION) => decode_v1(json, &root),
        _ => Err(DecodeError::new(
            &version_path,
            DecodeErrorKind::UnsupportedSchemaVersion { found },
        )),
    }
}

/// Schema version 1: `{"schema_version": 1, "rules": [<rule>, ...]}`.
fn decode_v1(json: &Json, root: &JsonPath) -> Result<FirewallRuleRevision, DecodeError> {
    let fields = Fields::new(json, root, REVISION, &["schema_version", "rules"])?;
    let (rules, rules_path) = fields.required("rules")?;
    let Json::Array(rules) = rules else {
        return Err(wrong_type(&rules_path, "an array of rules", rules));
    };
    let rules = rules
        .iter()
        .enumerate()
        .map(|(index, rule)| decode_rule(rule, &rules_path.index(index)))
        .collect::<Result<Vec<_>, _>>()?;

    FirewallRuleRevision::new(rules).map_err(|error| match error {
        RuleRevisionError::DuplicateRuleId {
            id,
            first_index,
            duplicate_index,
        } => DecodeError::new(
            &rules_path.index(duplicate_index).key("id"),
            DecodeErrorKind::DuplicateRuleId { id, first_index },
        ),
    })
}

fn decode_rule(json: &Json, path: &JsonPath) -> Result<Rule, DecodeError> {
    let fields = Fields::new(json, path, "a rule object", &["id", "all_of"])?;
    let (id, id_path) = fields.required("id")?;
    let id = decode_name(id, &id_path, "a string rule ID", RuleId::new)?;

    let (all_of, all_of_path) = fields.required("all_of")?;
    let Json::Array(all_of) = all_of else {
        return Err(wrong_type(&all_of_path, "an array of comparisons", all_of));
    };
    let all_of = all_of
        .iter()
        .enumerate()
        .map(|(index, comparison)| decode_match(comparison, &all_of_path.index(index)))
        .collect::<Result<Vec<_>, _>>()?;

    Rule::new(id, all_of).map_err(|_| DecodeError::new(&all_of_path, DecodeErrorKind::EmptyAllOf))
}

fn decode_match(json: &Json, path: &JsonPath) -> Result<Match, DecodeError> {
    let fields = Fields::new(json, path, "a comparison object", &["left", "op", "right"])?;

    let (op, op_path) = fields.required("op")?;
    match op {
        Json::String(op) if op == EQUALITY => {}
        Json::String(op) => {
            return Err(DecodeError::new(
                &op_path,
                DecodeErrorKind::UnsupportedOperator { op: op.clone() },
            ));
        }
        other => return Err(wrong_type(&op_path, "a string operator", other)),
    }

    let (left, left_path) = fields.required("left")?;
    let left = decode_read(left, &left_path)?;
    let (right, right_path) = fields.required("right")?;
    let right = decode_value(right, &right_path)?;
    Ok(Match::new(left, right))
}

const READ_VARIANTS: &str = "\"payload\", \"tcp option\" or \"&\"";

fn decode_read(json: &Json, path: &JsonPath) -> Result<Read, DecodeError> {
    let fields = Fields::new(json, path, "a read object", &["payload", "tcp option", "&"])?;
    let (variant, value) = fields.variant(READ_VARIANTS)?;
    let path = path.key(variant);
    match variant {
        "payload" => decode_payload_header(value, &path).map(Read::Payload),
        "tcp option" => {
            let fields = Fields::new(value, &path, "a TCP option object", &["name"])?;
            let (name, name_path) = fields.required("name")?;
            let name = decode_name(name, &name_path, "a string option name", TcpOptionName::new)?;
            Ok(Read::TcpOption(name))
        }
        "&" => decode_masked_payload(value, &path),
        other => Err(unexpected_field(&path, other)),
    }
}

fn decode_masked_payload(json: &Json, path: &JsonPath) -> Result<Read, DecodeError> {
    let [header, mask] = decode_pair(json, path, "a [read, mask] array")?;

    // Only a payload header may be masked: a TCP option or another masked read
    // inside the tuple is an unexpected field of the inner read.
    let header_path = path.index(0);
    let fields = Fields::new(header, &header_path, "a payload read object", &["payload"])?;
    let (_, header_json) = fields.variant("\"payload\"")?;
    let header = decode_payload_header(header_json, &header_path.key("payload"))?;

    let mask_path = path.index(1);
    let mask = decode_u64(mask, &mask_path, "an unsigned 64-bit integer mask")?;
    let mask = NonZeroU64::new(mask)
        .ok_or_else(|| DecodeError::new(&mask_path, DecodeErrorKind::ZeroMask))?;
    Ok(Read::MaskedPayload { header, mask })
}

fn decode_payload_header(json: &Json, path: &JsonPath) -> Result<PayloadHeader, DecodeError> {
    let fields = Fields::new(
        json,
        path,
        "a payload header object",
        &["protocol", "field"],
    )?;
    let (protocol, protocol_path) = fields.required("protocol")?;
    let protocol = decode_name(
        protocol,
        &protocol_path,
        "a string protocol name",
        HeaderProtocol::new,
    )?;
    let (field, field_path) = fields.required("field")?;
    let field = decode_name(field, &field_path, "a string field name", HeaderField::new)?;
    Ok(PayloadHeader::new(protocol, field))
}

const VALUE: &str = "an unsigned integer, IPv4 address string, true, prefix object or range object";

fn decode_value(json: &Json, path: &JsonPath) -> Result<Value, DecodeError> {
    match json {
        Json::Unsigned(value) => Ok(Value::Integer(*value)),
        Json::Bool(true) => Ok(Value::True),
        Json::String(text) => decode_ipv4(text, path).map(Value::Ipv4),
        Json::Object(_) => {
            let fields = Fields::new(json, path, VALUE, &["prefix", "range"])?;
            let (variant, value) = fields.variant("\"prefix\" or \"range\"")?;
            let path = path.key(variant);
            match variant {
                "prefix" => decode_prefix(value, &path).map(Value::Ipv4Prefix),
                "range" => decode_range(value, &path).map(Value::Range),
                other => Err(unexpected_field(&path, other)),
            }
        }
        other => Err(wrong_type(path, VALUE, other)),
    }
}

fn decode_prefix(json: &Json, path: &JsonPath) -> Result<Ipv4Prefix, DecodeError> {
    let fields = Fields::new(json, path, "a prefix object", &["addr", "len"])?;
    let (addr, addr_path) = fields.required("addr")?;
    let addr = decode_string(addr, &addr_path, "an IPv4 address string")?;
    let addr = decode_ipv4(&addr, &addr_path)?;
    let (len, len_path) = fields.required("len")?;
    let len = decode_u64(len, &len_path, "an unsigned integer prefix length")?;
    let prefix_length = || DecodeError::new(&len_path, DecodeErrorKind::PrefixLength { len });
    let narrow = u8::try_from(len).map_err(|_| prefix_length())?;
    Ipv4Prefix::new(addr, narrow).map_err(|_| prefix_length())
}

fn decode_range(json: &Json, path: &JsonPath) -> Result<IntegerRange, DecodeError> {
    let [start, end] = decode_pair(json, path, "a [start, end] array")?;
    let start = decode_u64(start, &path.index(0), "an unsigned 64-bit integer")?;
    let end = decode_u64(end, &path.index(1), "an unsigned 64-bit integer")?;
    IntegerRange::new(start, end)
        .map_err(|_| DecodeError::new(path, DecodeErrorKind::ReversedRange { start, end }))
}

fn decode_pair<'a>(
    json: &'a Json,
    path: &JsonPath,
    expected: &'static str,
) -> Result<[&'a Json; 2], DecodeError> {
    match json {
        Json::Array(elements) => match elements.as_slice() {
            [first, second] => Ok([first, second]),
            _ => Err(DecodeError::new(
                path,
                DecodeErrorKind::TupleLength {
                    expected: 2,
                    found: elements.len(),
                },
            )),
        },
        other => Err(wrong_type(path, expected, other)),
    }
}

fn decode_string(
    json: &Json,
    path: &JsonPath,
    expected: &'static str,
) -> Result<String, DecodeError> {
    match json {
        Json::String(value) => Ok(value.clone()),
        other => Err(wrong_type(path, expected, other)),
    }
}

/// Reads a string and checks it against its name's syntax rule.
fn decode_name<T>(
    json: &Json,
    path: &JsonPath,
    expected: &'static str,
    new: impl FnOnce(String) -> Result<T, NameError>,
) -> Result<T, DecodeError> {
    let text = decode_string(json, path, expected)?;
    new(text).map_err(|error| DecodeError::new(path, DecodeErrorKind::InvalidName(error)))
}

fn decode_u64(json: &Json, path: &JsonPath, expected: &'static str) -> Result<u64, DecodeError> {
    match json {
        Json::Unsigned(value) => Ok(*value),
        other => Err(wrong_type(path, expected, other)),
    }
}

/// Reads strict dotted-quad IPv4: four decimal octets without leading zeros,
/// as `Ipv4Addr`'s parser accepts. IPv6 and other forms are rejected.
fn decode_ipv4(text: &str, path: &JsonPath) -> Result<Ipv4Addr, DecodeError> {
    text.parse().map_err(|_| {
        DecodeError::new(
            path,
            DecodeErrorKind::InvalidIpv4 {
                text: text.to_owned(),
            },
        )
    })
}

#[cfg(test)]
mod tests;
