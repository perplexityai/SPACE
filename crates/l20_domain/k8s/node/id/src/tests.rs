use pplx_space_common_core::types::{NodeId, NodeRole};

use super::{space_k8s_node_id, space_node_id};

#[test]
fn space_k8s_node_id_is_role_scoped_and_stable() {
    let node_name = "ip-10-0-0-1.ec2.internal";

    let worker_id = space_k8s_node_id(node_name, "worker");
    let builder_id = space_k8s_node_id(node_name, "builder");

    assert_ne!(worker_id, builder_id);
    assert_eq!(worker_id, space_k8s_node_id(node_name, "worker"));
}

#[test]
fn space_node_id_preserves_direct_node_id() {
    let expected = NodeId::new();

    let actual = space_node_id(&expected.to_string(), NodeRole::Builder).unwrap();

    assert_eq!(actual, expected);
}

#[test]
fn space_node_id_canonicalizes_kubernetes_node_name() {
    let node_name = "ip-10-0-0-1.ec2.internal";

    let actual = space_node_id(node_name, NodeRole::Builder).unwrap();

    assert_eq!(
        actual,
        space_k8s_node_id(node_name, NodeRole::Builder.as_str())
    );
}

#[test]
fn space_node_id_rejects_unknown_identity_shape() {
    let result = space_node_id("not a node id", NodeRole::Builder);

    result.unwrap_err();
}
