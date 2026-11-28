//! A condition of a sandbox's network-policy status.

/// One condition of a sandbox's network-policy status. sandbox-teardown's
/// observed-state read decodes the conditions a node published from
/// `node_facet_conditions`, and the API adds one it computes, which replaces
/// an observed condition of the same `type`.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct NetworkPolicyCondition {
    #[cfg_attr(feature = "serde", serde(rename = "type"))]
    pub condition_type: String,
    pub status: String,
    pub reason: Option<String>,
    pub message: Option<String>,
    pub last_transition_at: Option<String>,
}
