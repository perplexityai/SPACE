//! The schema version 1 envelope around the model's comparisons. Each struct
//! is generic so one definition reads owned values and writes borrowed ones.
//! Derived field order is the canonical key order.

use std::fmt;
use std::marker::PhantomData;

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
pub(crate) struct RevisionV1<R> {
    pub(crate) schema_version: u64,
    pub(crate) rules: R,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RuleV1<I, A> {
    pub(crate) id: I,
    pub(crate) all_of: A,
}

/// Reads `T` from a JSON object only. A derived struct also accepts an array
/// of its fields in order, which the grammar does not.
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
