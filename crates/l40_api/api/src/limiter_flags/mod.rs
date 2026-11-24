//! The subject and targeting attributes the admission limiters' flags are
//! evaluated with.
//!
//! `space-cluster-rate-limiter`, `space-api-client-concurrency-limiter` and
//! `space-api-client-concurrency-quotas` each configure one cluster as a
//! whole, so every replica evaluates them against the same constant subject.
//! A percentage rule on a constant subject is all-or-nothing, and when several
//! clusters read one flag environment, a constant subject alone can't tell
//! them apart either. The process's cluster id travels alongside it as the
//! `cluster_id` attribute, so a targeting rule can match one cluster while the
//! others fall through to the flag's default.
//!
//! The subject stays constant so that adding the attribute moves no percentage
//! bucket, and a process with no cluster id sends no attribute.

use pplx_space_cluster_id::{ClusterId, process_cluster_id};
use pplx_space_feature_flags::Attributes;

#[cfg(test)]
pub(crate) mod tests;

/// The subject every limiter flag is evaluated against.
pub(crate) const SUBJECT: &str = "space-cluster";

/// The attribute that carries the cluster id.
pub(crate) const CLUSTER_ID_ATTRIBUTE: &str = "cluster_id";

/// The targeting attributes for `cluster_id`: `cluster_id` when there is one,
/// none otherwise.
pub(crate) fn attributes(cluster_id: Option<&ClusterId>) -> Attributes {
    cluster_id
        .map(|cluster_id| {
            Attributes::from([(
                CLUSTER_ID_ATTRIBUTE.to_owned(),
                cluster_id.as_str().to_owned().into(),
            )])
        })
        .unwrap_or_default()
}

/// The targeting attributes for this process: its cluster id is the one
/// [`process_cluster_id`] owns, which is also the `cluster_id` tag on the
/// limiters' decision metrics.
pub(crate) fn process_attributes() -> Attributes {
    attributes(process_cluster_id())
}
