//! Conversion between the vendor-neutral targeting inputs and OpenFeature's,
//! in both directions: [`Attributes`] into an [`EvaluationContext`] on the way
//! in, and OpenFeature's [`StructValue`] into JSON on the way out.

use open_feature::{EvaluationContext, EvaluationContextFieldValue, StructValue, Value};

use crate::{AttributeValue, Attributes};

/// Build the context a provider resolves against.
///
/// The subject key becomes the targeting key, which is the field OpenFeature
/// providers hash for percentage rollouts. Every SPACE subject shape — sandbox
/// id, `<cluster_id>/<node_name>`, the constant cluster subject — is already a
/// string, so the mapping is direct and the rollout bucket is stable for the
/// same reason it is under Eppo: the same subject string hashes the same way.
pub(super) fn evaluation_context(subject_key: &str, attributes: &Attributes) -> EvaluationContext {
    let mut context = EvaluationContext::default().with_targeting_key(subject_key);
    for (key, value) in attributes {
        if RESERVED_ATTRIBUTE_NAMES.contains(&key.as_str()) {
            tracing::warn!(
                attribute = key,
                "dropping a targeting attribute whose name is reserved by the evaluation context"
            );
            continue;
        }
        if let Some(value) = field_value(value) {
            context.add_custom_field(key.clone(), value);
        }
    }
    context
}

/// Names a backend puts in the same namespace as the custom fields, and would
/// let an attribute overwrite.
///
/// flagd builds its targeting input by inserting the targeting key and its own
/// metadata first and then inserting every custom field over the top, so an
/// attribute called `targetingKey` replaces the subject outright — a
/// percentage rollout would then bucket on the attribute instead of the
/// subject, silently and for that flag only. `$flagd` is the same hazard for
/// flagd's own metadata.
///
/// Dropping is deliberate. The alternative is a rollout that looks healthy and
/// selects the wrong population, which no telemetry would distinguish from a
/// correct one; a warning costs one log line at a call site that should never
/// send these names in the first place.
const RESERVED_ATTRIBUTE_NAMES: [&str; 2] = ["targetingKey", "$flagd"];

/// One targeting attribute, or nothing when OpenFeature cannot carry it.
///
/// `Null` is the only shape with nowhere to go: `EvaluationContextFieldValue`
/// has no null variant, so the key is dropped and a targeting rule sees the
/// attribute as absent rather than as present-with-no-value. That is a real
/// difference from Eppo, where `AttributeValue::Null` reaches the rules intact,
/// and it is why [`AttributeValue`]'s own documentation warns against relying
/// on `Null` round-tripping through every provider.
fn field_value(value: &AttributeValue) -> Option<EvaluationContextFieldValue> {
    match value {
        AttributeValue::Bool(value) => Some(EvaluationContextFieldValue::Bool(*value)),
        // Always `Float`, never `Int`: the neutral vocabulary carries one `f64`
        // number and nothing records whether it was integral. flagd's RPC path
        // converts every number to a double anyway, but its in-process engine
        // keeps an `Int` exact — so an integer beyond 2^53 loses precision
        // there that it would not have lost had the type survived.
        AttributeValue::Number(value) => Some(EvaluationContextFieldValue::Float(*value)),
        AttributeValue::String(value) => Some(EvaluationContextFieldValue::String(value.clone())),
        AttributeValue::Null => None,
    }
}

/// Convert a resolved structure into the JSON a caller deserializes.
///
/// OpenFeature resolves JSON-typed flags as `StructValue`, which is an object
/// and nothing else, so this always produces a JSON object. A flag whose value
/// is a bare number, a string, or a top-level array cannot be resolved through
/// the JSON read at all — the provider rejects it before reaching here. SPACE's
/// three JSON flags are all objects or arrays-inside-objects, and flag
/// definitions written for this adapter have to stay that way.
pub(super) fn struct_value_to_json(value: StructValue) -> serde_json::Value {
    serde_json::Value::Object(
        value
            .fields
            .into_iter()
            .map(|(key, value)| (key, value_to_json(value)))
            .collect(),
    )
}

/// A resolved value as JSON.
///
/// Non-finite floats become `null`: JSON has no NaN or infinity, and
/// `serde_json::Number::from_f64` refuses them. A flag definition cannot
/// express one in the first place — it would have to arrive from a provider
/// that computed it — so `null` records "there was a number here that JSON
/// cannot hold" rather than silently dropping the field.
fn value_to_json(value: Value) -> serde_json::Value {
    match value {
        Value::Bool(value) => serde_json::Value::Bool(value),
        Value::Int(value) => serde_json::Value::Number(value.into()),
        Value::Float(value) => serde_json::Number::from_f64(value)
            .map_or(serde_json::Value::Null, serde_json::Value::Number),
        Value::String(value) => serde_json::Value::String(value),
        Value::Array(values) => {
            serde_json::Value::Array(values.into_iter().map(value_to_json).collect())
        }
        Value::Struct(value) => struct_value_to_json(value),
    }
}
