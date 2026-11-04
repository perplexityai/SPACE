use super::*;
use pplx_space_common_core::types::SandboxId;

fn policy_row(
    own: Option<pplx_space_net_common::NetworkPolicy>,
    sandbox_generation: i64,
) -> SandboxPolicyRow {
    SandboxPolicyRow {
        sandbox_id: SandboxId::from_uuid(Uuid::nil()),
        template_id: Some("seed".to_string()),
        network_policy_json: own.map(Json),
        network_policy_generation: sandbox_generation,
        template_network_policy_json: Some(Json(NetworkPolicyV1::deny_all())),
    }
}

#[test]
fn effective_generation_is_sandbox_lineage_when_inherited() {
    // A cleared override: the sandbox generation was bumped (1 -> 2) by the
    // clear. The node fences the sandbox's apply on that sandbox generation,
    // so the effective generation must be 2. The earlier code substituted
    // the template's generation on the inherited path, which is an unrelated
    // (often lower) counter; the inherit apply then collided with the
    // installed gen-1 override at the same-or-lower generation and the node
    // rejected it as a conflict forever, so the clear never converged.
    let inherited = policy_row(None, 2);
    assert_eq!(sandbox_effective_policy_generation(&inherited), 2);
}

#[test]
fn effective_generation_is_sandbox_lineage_for_override() {
    let overridden = policy_row(Some(NetworkPolicyV1::allow_all()), 5);
    assert_eq!(sandbox_effective_policy_generation(&overridden), 5);
}
