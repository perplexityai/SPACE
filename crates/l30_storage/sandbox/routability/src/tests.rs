use super::*;

#[test]
fn routable_k8s_execution_exists_uses_provided_alias() {
    let fragment = routable_k8s_execution_exists("s");
    assert!(fragment.contains("rex.sandbox_id = s.id"));
    assert!(fragment.contains("rex.active"));
    assert!(fragment.contains("rex.state = 'running'"));
    assert!(fragment.contains("rex.pod_ip IS NOT NULL"));
}

#[test]
fn routable_execution_or_non_k8s_exempts_other_substrates() {
    let fragment = routable_execution_or_non_k8s("sandboxes", "$5");
    assert!(fragment.contains("sandboxes.managed_by <> $5"));
    assert!(fragment.contains("rex.sandbox_id = sandboxes.id"));
}
