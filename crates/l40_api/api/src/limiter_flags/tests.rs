use pplx_space_cluster_id::ClusterId;
use pplx_space_feature_flags::{
    AttributeValue, Attributes, FeatureFlagClient, FeatureFlagProvider, ProviderState, Unresolved,
};
use serde_json::Value;

use super::*;

pub(crate) fn cluster(value: &str) -> ClusterId {
    ClusterId::parse(value).expect("valid cluster id")
}

/// A flag backend holding one definition, `flag_key`. A read whose
/// `cluster_id` attribute names the rule's cluster resolves the rule's value;
/// every other read falls through to `default`, or to no assignment when there
/// is none, the way a flagd `if` rule ending in `null` falls through to its
/// `defaultVariant`.
///
/// It panics on any subject but [`SUBJECT`], so a read that changes the
/// subject fails a test instead of quietly moving a percentage bucket.
struct ClusterRule {
    flag_key: &'static str,
    rule: Option<(&'static str, Value)>,
    default: Option<Value>,
}

impl ClusterRule {
    fn value(&self, flag_key: &str, subject_key: &str, attributes: &Attributes) -> Option<&Value> {
        assert_eq!(
            subject_key, SUBJECT,
            "the limiter flags keep a constant subject"
        );
        if flag_key != self.flag_key {
            return None;
        }
        let matched = self.rule.as_ref().filter(|(cluster_id, _)| {
            attributes.get(CLUSTER_ID_ATTRIBUTE)
                == Some(&AttributeValue::String((*cluster_id).to_owned()))
        });
        matched.map(|(_, value)| value).or(self.default.as_ref())
    }
}

impl FeatureFlagProvider for ClusterRule {
    fn state(&self) -> ProviderState {
        ProviderState::Ready
    }

    fn bool_value(
        &self,
        flag_key: &str,
        subject_key: &str,
        attributes: &Attributes,
    ) -> Result<bool, Unresolved> {
        self.value(flag_key, subject_key, attributes)
            .and_then(Value::as_bool)
            .ok_or(Unresolved::NoAssignment)
    }

    fn integer_value(&self, _: &str, _: &str, _: &Attributes) -> Result<i64, Unresolved> {
        Err(Unresolved::NoAssignment)
    }

    fn float_value(&self, _: &str, _: &str, _: &Attributes) -> Result<f64, Unresolved> {
        Err(Unresolved::NoAssignment)
    }

    fn string_value(&self, _: &str, _: &str, _: &Attributes) -> Result<String, Unresolved> {
        Err(Unresolved::NoAssignment)
    }

    fn json_value(
        &self,
        flag_key: &str,
        subject_key: &str,
        attributes: &Attributes,
    ) -> Result<Value, Unresolved> {
        self.value(flag_key, subject_key, attributes)
            .cloned()
            .ok_or(Unresolved::NoAssignment)
    }
}

/// A client whose `flag_key` serves `value` to `cluster_id` only, and
/// `default` (or nothing) to every other read.
pub(crate) fn targeting_one_cluster(
    flag_key: &'static str,
    cluster_id: &'static str,
    value: Value,
    default: Option<Value>,
) -> FeatureFlagClient {
    FeatureFlagClient::from_provider(ClusterRule {
        flag_key,
        rule: Some((cluster_id, value)),
        default,
    })
}

/// A client whose `flag_key` has no targeting rule: every read gets `default`,
/// or nothing.
pub(crate) fn without_a_rule(flag_key: &'static str, default: Option<Value>) -> FeatureFlagClient {
    FeatureFlagClient::from_provider(ClusterRule {
        flag_key,
        rule: None,
        default,
    })
}

#[test]
fn a_cluster_id_becomes_the_cluster_id_attribute() {
    assert_eq!(
        attributes(Some(&cluster("cluster-a"))),
        Attributes::from([(
            "cluster_id".to_owned(),
            AttributeValue::String("cluster-a".to_owned())
        )]),
    );
}

/// A process with no cluster id reads the flags exactly as it did before the
/// attribute existed.
#[test]
fn no_cluster_id_sends_no_attribute() {
    assert_eq!(attributes(None), Attributes::default());
}
