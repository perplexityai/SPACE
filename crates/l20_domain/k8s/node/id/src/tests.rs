use pplx_space_common_core::types::{NodeId, NodeRole};

use super::{Ec2InstanceId, space_k8s_node_id, space_node_id};

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

/// Every `providerID` shape the parser accepts or refuses: an instance ID
/// is `i-` and 8 or 17 lowercase hex digits, the form EC2 issues, in the last
/// segment of an `aws:///` providerID.
#[test]
fn ec2_instance_id_from_provider_id_matrix() {
    let cases: &[(&str, Option<&str>)] = &[
        // The EKS form, with the availability zone, both lengths.
        ("aws:///us-west-2a/i-0abc123d", Some("i-0abc123d")),
        (
            "aws:///us-east-1c/i-0123456789abcdef0",
            Some("i-0123456789abcdef0"),
        ),
        // Without a zone, and with extra segments: only the last one counts.
        ("aws:///i-0abc123d", Some("i-0abc123d")),
        (
            "aws:///us-west-2/us-west-2a/i-0123456789abcdef0",
            Some("i-0123456789abcdef0"),
        ),
        // Not EC2's form: the prefix alone, another length, upper case, not
        // hex.
        ("aws:///us-west-2a/i-", None),
        ("aws:///us-west-2a/i-0abc123def", None),
        ("aws:///us-west-2a/i-0ABC123D", None),
        ("aws:///us-west-2a/i-0abc123g", None),
        // Kind, another cloud, and a malformed AWS scheme.
        ("kind://docker/space/space-worker", None),
        ("gce://project/zone/instance", None),
        ("aws://us-west-2a/i-0abc123d", None),
        ("AWS:///us-west-2a/i-0abc123d", None),
        // AWS scheme without an instance ID in the last segment.
        ("aws:///us-west-2a/not-an-id", None),
        ("aws:///i-0abc123d/", None),
        ("aws:///", None),
        // A bare instance ID is not a providerID.
        ("i-0abc123d", None),
        ("", None),
    ];
    for (provider_id, expected) in cases {
        assert_eq!(
            Ec2InstanceId::from_provider_id(provider_id)
                .as_ref()
                .map(Ec2InstanceId::as_str),
            *expected,
            "{provider_id:?}"
        );
    }
}

/// The same rule for an instance ID on its own.
#[test]
fn an_instance_id_is_ec2s_form() {
    for (value, valid) in [
        ("i-0abc123d", true),
        ("i-0123456789abcdef0", true),
        ("i-0abc123def", false),
        ("i-", false),
        ("i-0ABC123D", false),
        ("x-0abc123d", false),
    ] {
        assert_eq!(Ec2InstanceId::parse(value).is_some(), valid, "{value:?}");
    }
}
