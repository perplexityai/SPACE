use super::*;

#[test]
fn pod_uid_rejects_empty_values() {
    assert_eq!(PodUid::parse(" pod-uid ").unwrap().as_str(), "pod-uid");
    PodUid::parse(" ").unwrap_err();
}

#[test]
fn pod_resource_version_rejects_empty_values() {
    assert_eq!(
        PodResourceVersion::parse(" 84213 ").unwrap().as_str(),
        "84213"
    );
    PodResourceVersion::parse(" ").unwrap_err();
}

#[test]
fn pod_name_accepts_dns_subdomains_only() {
    assert_eq!(
        PodName::parse(" space-sbx-0189f-g2 ").unwrap().as_str(),
        "space-sbx-0189f-g2"
    );
    PodName::parse("").unwrap_err();
    PodName::parse(" ").unwrap_err();
    PodName::parse("Space-Sbx").unwrap_err();
    PodName::parse("-leading-dash").unwrap_err();
    PodName::parse("trailing-dot.").unwrap_err();
    PodName::parse("under_score").unwrap_err();
    PodName::parse("a".repeat(254)).unwrap_err();
}

#[test]
fn k8s_node_name_accepts_dns_subdomains_only() {
    assert_eq!(
        K8sNodeName::parse("ip-10-0-1-2.ec2.internal")
            .unwrap()
            .as_str(),
        "ip-10-0-1-2.ec2.internal"
    );
    K8sNodeName::parse("").unwrap_err();
    K8sNodeName::parse("Worker.Node").unwrap_err();
    K8sNodeName::parse(".leading-dot").unwrap_err();
}

#[test]
fn k8s_node_uid_rejects_empty_values() {
    assert_eq!(
        K8sNodeUid::parse(" 0f3a-uid ").unwrap().as_str(),
        "0f3a-uid"
    );
    K8sNodeUid::parse(" ").unwrap_err();
}
