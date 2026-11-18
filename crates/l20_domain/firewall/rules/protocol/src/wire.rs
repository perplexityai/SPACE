//! The schema version 1 wire types. They mirror the grammar in the crate docs
//! and are private, so the model has no `Serialize` and this crate is the only
//! writer. Derived field order is the canonical key order.

use std::fmt;
use std::marker::PhantomData;
use std::net::Ipv4Addr;
use std::num::NonZeroU64;

use serde::de::value::MapAccessDeserializer;
use serde::de::{MapAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};

/// The schema version alone. Other keys are ignored, so a document of any
/// version gets this far.
#[derive(Deserialize)]
pub(crate) struct Version {
    pub(crate) schema_version: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RevisionV1 {
    pub(crate) schema_version: u64,
    pub(crate) rules: Vec<Object<RuleV1>>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RuleV1 {
    pub(crate) id: String,
    pub(crate) all_of: Vec<Object<MatchV1>>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MatchV1 {
    pub(crate) left: ReadV1,
    pub(crate) op: Op,
    pub(crate) right: ValueV1,
}

#[derive(Serialize, Deserialize)]
pub(crate) enum ReadV1 {
    #[serde(rename = "payload")]
    Payload(Object<HeaderV1>),
    #[serde(rename = "tcp option")]
    TcpOption(Object<TcpOptionV1>),
    #[serde(rename = "&")]
    Masked(Object<PayloadReadV1>, NonZeroU64),
}

/// The read inside `&`: only a payload header may be masked.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PayloadReadV1 {
    pub(crate) payload: Object<HeaderV1>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HeaderV1 {
    pub(crate) protocol: String,
    pub(crate) field: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TcpOptionV1 {
    pub(crate) name: String,
}

/// Read from a string only: a derived unit variant would also accept
/// `{"==": null}`.
#[derive(Serialize, Deserialize)]
#[serde(try_from = "String")]
pub(crate) enum Op {
    #[serde(rename = "==")]
    Eq,
}

impl TryFrom<String> for Op {
    type Error = UnsupportedOperator;

    fn try_from(op: String) -> Result<Self, Self::Error> {
        if op == "==" {
            Ok(Self::Eq)
        } else {
            Err(UnsupportedOperator(op))
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("unsupported operator {0:?}; only \"==\" is supported")]
pub(crate) struct UnsupportedOperator(String);

/// `false` decodes here and is rejected when converted to the model.
#[derive(Serialize, Deserialize)]
#[serde(
    untagged,
    expecting = "expected an unsigned integer, an IPv4 address, true, a prefix or a range"
)]
pub(crate) enum ValueV1 {
    Integer(u64),
    Ipv4(Ipv4Addr),
    Bool(bool),
    Tagged(TaggedValueV1),
}

/// The object values. An externally tagged enum takes exactly one key, so a
/// repeated or extra key cannot fall back to another variant.
#[derive(Serialize, Deserialize)]
pub(crate) enum TaggedValueV1 {
    #[serde(rename = "prefix")]
    Prefix(Object<PrefixV1>),
    #[serde(rename = "range")]
    Range(u64, u64),
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PrefixV1 {
    pub(crate) addr: Ipv4Addr,
    pub(crate) len: u8,
}

/// Reads `T` from a JSON object only. A derived struct also accepts an array
/// of its fields in order, which the grammar does not.
#[derive(Serialize)]
#[serde(transparent)]
pub(crate) struct Object<T>(pub(crate) T);

impl<'de, T: Deserialize<'de>> Deserialize<'de> for Object<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ObjectVisitor<T>(PhantomData<T>);

        impl<'de, T: Deserialize<'de>> Visitor<'de> for ObjectVisitor<T> {
            type Value = T;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("an object")
            }

            fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<T, A::Error> {
                T::deserialize(MapAccessDeserializer::new(map))
            }
        }

        deserializer
            .deserialize_map(ObjectVisitor(PhantomData))
            .map(Object)
    }
}
