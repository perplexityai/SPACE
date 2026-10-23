/// Kubernetes label-value grammar lives in `pplx-space-k8s-ids` (re-exported
/// through `pplx-space-common-core`) so there is one validator across the
/// workspace; re-exported here for the historical
/// `pplx_space_common_svc::types::k8s::LabelValue` path.
pub use pplx_space_common_core::types::{LabelValue, LabelValueParseError};

/// Kubernetes-derived SPACE node identity lives in `pplx-space-k8s-node-id`;
/// re-exported here for the historical `pplx_space_common_svc::types::k8s`
/// paths.
pub use pplx_space_k8s_node_id::{space_k8s_node_id, space_node_id};
