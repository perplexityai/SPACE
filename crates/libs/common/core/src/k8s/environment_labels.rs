//! Node labels naming the execution environments a node can run, and the
//! required node affinity a bound sandbox pod renders over them.
//!
//! node-ctl writes one label per environment in its node's latest complete
//! environment observation (`usable_environments`), and removes the label
//! when the environment leaves it. With the environment placement gate on,
//! a pod whose work is bound to an environment requires its label, so
//! kube-scheduler places it only on a node that published it. The writer, the
//! pod builder ([`environment_requirement`]) and the census
//! ([`node_publishes_environment`], [`node_publishes_any_environment`]) share
//! [`environment_label_key`] and [`ENVIRONMENT_USABLE_LABEL_VALUE`], so they
//! can't disagree on the label.

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::NodeSelectorRequirement;

use crate::types::EnvironmentReference;

/// Every environment label's key starts with this.
pub const ENVIRONMENT_LABEL_KEY_PREFIX: &str = "space.pplx.net/env-";

/// The value of every environment label.
pub const ENVIRONMENT_USABLE_LABEL_VALUE: &str = "usable";

/// Pod annotation recording the environment a bound sandbox pod's work runs
/// under, while the environment placement gate evaluates (shadow or on), so a
/// census can compare it with the labels of the node the pod landed on.
pub const SANDBOX_ENVIRONMENT_ANNOTATION: &str = "space.pplx.net/environment";

/// The environment a bound sandbox pod renders: always as
/// [`SANDBOX_ENVIRONMENT_ANNOTATION`], and as a required node affinity on its
/// [`environment_label_key`] when `required`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PodEnvironment {
    pub reference: EnvironmentReference,
    /// The gate is on, so the pod may run only on a node labeled with it.
    pub required: bool,
}

/// The longest name part (after the `/`) a Kubernetes label key may have.
const LABEL_NAME_MAX: usize = 63;

/// The node label key that says the node can run `reference`.
///
/// A reference (`sha256:` and 64 hex digits) doesn't fit a label name, so
/// the key carries the algorithm and as many leading digits as fit: 52 for
/// SHA-256, 208 bits, which no two catalog environments share.
pub fn environment_label_key(reference: &EnvironmentReference) -> String {
    let (domain, name_prefix) = ENVIRONMENT_LABEL_KEY_PREFIX
        .split_once('/')
        .expect("the prefix names a domain");
    let mut name = format!("{name_prefix}{}", reference.to_string().replace(':', "-"));
    name.truncate(LABEL_NAME_MAX);
    format!("{domain}/{name}")
}

/// Whether `key` is an environment label, which node-ctl owns.
pub fn is_environment_label_key(key: &str) -> bool {
    key.starts_with(ENVIRONMENT_LABEL_KEY_PREFIX)
}

/// The required node-affinity expression a pod bound to `reference` carries
/// with the gate on: its label, set to [`ENVIRONMENT_USABLE_LABEL_VALUE`].
pub fn environment_requirement(reference: &EnvironmentReference) -> NodeSelectorRequirement {
    NodeSelectorRequirement {
        key: environment_label_key(reference),
        operator: "In".to_string(),
        values: Some(vec![ENVIRONMENT_USABLE_LABEL_VALUE.to_string()]),
    }
}

/// Whether a node labeled `labels` satisfies [`environment_requirement`] for
/// `reference`.
pub fn node_publishes_environment(
    labels: &BTreeMap<String, String>,
    reference: &EnvironmentReference,
) -> bool {
    labels
        .get(&environment_label_key(reference))
        .map(String::as_str)
        == Some(ENVIRONMENT_USABLE_LABEL_VALUE)
}

/// Whether a node labeled `labels` publishes any environment at all.
pub fn node_publishes_any_environment(labels: &BTreeMap<String, String>) -> bool {
    labels.iter().any(|(key, value)| {
        is_environment_label_key(key) && value == ENVIRONMENT_USABLE_LABEL_VALUE
    })
}

// Explicit because this module is itself reached through `#[path]`, so rustc
// would resolve a bare `mod tests;` to `k8s/tests.rs`.
#[cfg(test)]
#[path = "environment_labels/tests.rs"]
mod tests;
