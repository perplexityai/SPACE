use super::*;

#[test]
fn node_pool_name_accepts_label_values_and_rejects_junk() {
    for value in ["isolated-test-pool", "pool_1", "a", "A.B-c_9", "0"] {
        let parsed = NodePoolName::parse(value).expect(value);
        assert_eq!(parsed.as_str(), value);
        assert_eq!(parsed.to_string(), value);
    }
    // Empty is the one rule NodePoolName adds on top of the shared label
    // grammar (an empty label value is legal Kubernetes but pins nothing).
    assert_eq!(NodePoolName::parse(""), Err(NodePoolNameParseError::Empty));
    // Grammar failures (edges, charset, length) are delegated to the shared
    // LabelValue validator, not re-implemented here.
    for bad in ["-lead", "trail-", "has space", &"a".repeat(64)] {
        assert!(
            matches!(
                NodePoolName::parse(bad),
                Err(NodePoolNameParseError::InvalidLabelValue(_))
            ),
            "{bad:?} must be rejected by the label grammar"
        );
    }
}

#[test]
fn node_roles_parse_canonical_storage_values() {
    assert_eq!(NodeRole::parse("worker").unwrap(), NodeRole::Worker);
    assert_eq!(NodeRole::parse("builder").unwrap(), NodeRole::Builder);
    assert_eq!(NodeRole::parse("cluster").unwrap(), NodeRole::Cluster);
    NodeRole::parse("api").unwrap_err();
}

#[test]
fn node_managed_by_parse_canonical_storage_values() {
    assert_eq!(NodeManagedBy::parse("space").unwrap(), NodeManagedBy::Space);
    assert_eq!(
        NodeManagedBy::parse("k8s").unwrap(),
        NodeManagedBy::Kubernetes
    );
    NodeManagedBy::parse("manual").unwrap_err();
}

#[test]
fn node_address_parses_ip_addresses_only() {
    assert_eq!(
        "10.0.0.1".parse::<NodeAddress>().unwrap().to_string(),
        "10.0.0.1"
    );
    "worker.local".parse::<NodeAddress>().unwrap_err();
}
