//! The serde shapes a derive alone would get wrong: objects read only from
//! JSON objects, the `{left, op, right}` comparison, the TCP option and masked
//! reads, and the value.

use std::fmt;
use std::net::Ipv4Addr;
use std::num::NonZeroU64;

use serde::de::{self, Deserializer, IgnoredAny, MapAccess, Unexpected, Visitor};
use serde::{Deserialize, Serialize, Serializer, forward_to_deserialize_any};

use crate::model::{Name, PayloadHeader, Value};

/// Reads anything as a map. A derived struct also accepts an array of its
/// fields in order, which the grammar does not.
pub(crate) struct MapOnly<D>(pub(crate) D);

impl<'de, D: Deserializer<'de>> Deserializer<'de> for MapOnly<D> {
    type Error = D::Error;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, D::Error> {
        self.0.deserialize_map(visitor)
    }

    fn is_human_readable(&self) -> bool {
        self.0.is_human_readable()
    }

    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf option unit unit_struct newtype_struct seq tuple
        tuple_struct map struct enum identifier ignored_any
    }
}

/// A `T` read from a JSON object only, for positions that are not a field of a
/// type with its own map-only impl.
struct Map<T>(T);

impl<'de, T: Deserialize<'de>> Deserialize<'de> for Map<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        T::deserialize(MapOnly(deserializer)).map(Map)
    }
}

/// The comparison every [`crate::Match`] reads and writes.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Comparison<L, R> {
    pub(crate) left: L,
    pub(crate) op: Op,
    pub(crate) right: R,
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

/// `{"name": N}`, the operand of `"tcp option"`.
pub(crate) mod tcp_option {
    use super::*;

    #[derive(Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct TcpOption<N> {
        name: N,
    }

    pub(crate) fn serialize<S: Serializer>(name: &Name, serializer: S) -> Result<S::Ok, S::Error> {
        TcpOption { name }.serialize(serializer)
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Name, D::Error> {
        TcpOption::<Name>::deserialize(MapOnly(deserializer)).map(|option| option.name)
    }
}

/// `[{"payload": HEADER}, MASK]`, the operand of `"&"`: only a payload header
/// may be masked.
pub(crate) mod masked {
    use super::*;

    #[derive(Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct PayloadRead<H> {
        payload: H,
    }

    pub(crate) fn serialize<S: Serializer>(
        header: &PayloadHeader,
        mask: &NonZeroU64,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        (PayloadRead { payload: header }, mask).serialize(serializer)
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<(PayloadHeader, NonZeroU64), D::Error> {
        let (Map(PayloadRead { payload }), mask) =
            <(Map<PayloadRead<PayloadHeader>>, NonZeroU64)>::deserialize(deserializer)?;
        Ok((payload, mask))
    }
}

/// The literal `true`; `false` is not a value.
pub(crate) fn literal_true<S: Serializer>(serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_bool(true)
}

/// Reads a [`Value`] by its JSON type, so a bad value reports its own reason.
pub(crate) struct ValueVisitor;

#[derive(Deserialize)]
#[serde(field_identifier, rename_all = "lowercase")]
enum ValueKey {
    Prefix,
    Range,
}

impl<'de> Visitor<'de> for ValueVisitor {
    type Value = Value;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("an unsigned integer, an IPv4 address, true, a prefix or a range")
    }

    fn visit_u64<E: de::Error>(self, value: u64) -> Result<Value, E> {
        Ok(Value::Integer(value))
    }

    // serde_json reads a non-negative integer as `u64`; other formats may not.
    fn visit_i64<E: de::Error>(self, value: i64) -> Result<Value, E> {
        let negative = |_| E::invalid_value(Unexpected::Signed(value), &self);
        u64::try_from(value).map(Value::Integer).map_err(negative)
    }

    fn visit_str<E: de::Error>(self, value: &str) -> Result<Value, E> {
        let invalid = |_| E::invalid_value(Unexpected::Str(value), &"an IPv4 address");
        value.parse::<Ipv4Addr>().map(Value::Ipv4).map_err(invalid)
    }

    fn visit_bool<E: de::Error>(self, value: bool) -> Result<Value, E> {
        if value {
            Ok(Value::True)
        } else {
            Err(E::custom("false is not a value; only true is"))
        }
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Value, A::Error> {
        let value = match map.next_key()? {
            Some(ValueKey::Prefix) => Some(Value::Ipv4Prefix(map.next_value()?)),
            Some(ValueKey::Range) => Some(Value::Range(map.next_value()?)),
            None => None,
        };
        match (value, map.next_key::<IgnoredAny>()?) {
            (Some(value), None) => Ok(value),
            _ => Err(de::Error::custom(
                "expected an object with exactly one key, prefix or range",
            )),
        }
    }
}
