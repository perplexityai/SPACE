//! The serde shapes a derive alone would get wrong: objects read only from
//! JSON objects, the `{left, op, right}` comparison, the TCP option and masked
//! reads, and the literal `true`.

use std::num::NonZeroU64;

use serde::de::{self, Deserializer, Visitor};
use serde::{Deserialize, Serialize, Serializer, forward_to_deserialize_any};

use crate::model::{Name, PayloadHeader};

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
pub(crate) mod literal_true {
    use super::*;

    pub(crate) fn serialize<S: Serializer>(serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bool(true)
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<(), D::Error> {
        if bool::deserialize(deserializer)? {
            Ok(())
        } else {
            Err(de::Error::custom("false is not a value; only true is"))
        }
    }
}
