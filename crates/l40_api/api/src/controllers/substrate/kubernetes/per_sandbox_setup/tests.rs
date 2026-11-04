use std::collections::BTreeMap;

use pplx_space_common_core::types::{AutoLifecyclePolicy, Email, OrgId, TemplateName, UserId};
use pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity;
use pplx_space_net_common::NetworkPolicyV1;
use uuid::Uuid;

use super::*;
use crate::controllers::substrate::{
    ActorRef, CreateBootMode, CreateExecutionRequest, PooledExecutionClaim,
    PooledExecutionClaimRequest, SandboxResources, TemplateRef,
};
use pplx_space_common_core::types::SandboxId;

fn actor() -> ActorRef {
    ActorRef::new(
        UserId::from_uuid(Uuid::from_u128(1)),
        OrgId::from_uuid(Uuid::from_u128(2)),
        Email::parse("user@example.com".to_string()).unwrap(),
    )
}

/// The structural guarantee: a cold create and a warm-pool adoption built
/// from equivalent inputs resolve to byte-identical per-sandbox setup. If a
/// new setup field is added but only wired into one path's resolver, this
/// equality breaks.
fn assert_cold_matches_adoption(
    network_policy_override: Option<&NetworkPolicyV1>,
    auto_lifecycle: &AutoLifecyclePolicy,
    template_network_policy: &NetworkPolicyV1,
) {
    let actor = actor();
    let env: BTreeMap<String, String> = BTreeMap::new();
    let template = TemplateName::parse("seed").unwrap();
    let resources = SandboxResources {
        vcpu: None,
        memory_mib: None,
        disk_mib: None,
    };
    let template_ref = TemplateRef {
        template: &template,
        content_hash: Some("hash"),
        boot_mode: CreateBootMode::Fallthrough,
        runtime: "fc",
    };

    let cold = CreateExecutionRequest {
        metric_identity: SandboxApiMetricIdentity::native("create"),
        sandbox_id: SandboxId::from_uuid(Uuid::from_u128(3)),
        actor: &actor,
        template_ref: &template_ref,
        resources: &resources,
        env: &env,
        auto_lifecycle,
        network_policy_override,
        network_policy_generation: 1,
        template_network_policy,
        template_network_policy_generation: 7,
        disk_io_limits: Default::default(),
        restore_from: None,
        wait_until_ready: true,
        volume_encryption: false,
        node_pool: None,
    };

    let pooled = PooledExecutionClaimRequest {
        metric_identity: SandboxApiMetricIdentity::native("create"),
        actor: &actor,
        template: "seed",
        content_hash: Some("hash"),
        template_release_id: None,
        runtime: "fc",
        vcpu: None,
        memory_mib: None,
        env: &env,
        auto_lifecycle,
        network_policy_override,
        network_policy_generation: 1,
        template_network_policy,
        template_network_policy_generation: 7,
        volume_encryption: false,
    };
    let claim = PooledExecutionClaim {
        claim_id: Uuid::now_v7(),
        pool_slot_id: Some(Uuid::now_v7()),
        sandbox_id: SandboxId::new(),
        content_hash: "hash".to_string(),
        pod_name: "space-sbx-test-g1".to_string(),
        pod_uid: None,
        execution_generation: 1,
        pod_ip: "10.0.0.2".to_string(),
        vcpu: None,
        memory_mib: None,
        disk_mib: None,
        disk_io_limits: Default::default(),
        active_node_id: None,
        route_identity: None,
    };

    assert_eq!(
        PerSandboxSetup::from_create_request(&cold),
        PerSandboxSetup::from_claim_request(&pooled, &claim),
        "warm-pool adoption must resolve the same per-sandbox setup as a cold create"
    );
}

#[test]
fn adoption_inherits_template_network_policy_setup_like_cold() {
    let auto_lifecycle = AutoLifecyclePolicy::default();
    let template_policy = NetworkPolicyV1::deny_all();
    assert_cold_matches_adoption(None, &auto_lifecycle, &template_policy);
}

#[test]
fn adoption_applies_network_policy_override_setup_like_cold() {
    let auto_lifecycle = AutoLifecyclePolicy::default();
    let template_policy = NetworkPolicyV1::deny_all();
    let override_policy = NetworkPolicyV1::allow_all();
    assert_cold_matches_adoption(Some(&override_policy), &auto_lifecycle, &template_policy);
}

#[test]
fn setup_captures_owner_and_effective_policy() {
    let actor = actor();
    let template_policy = NetworkPolicyV1::deny_all();
    let override_policy = NetworkPolicyV1::allow_all();
    let auto_lifecycle = AutoLifecyclePolicy::default();
    let mut env = BTreeMap::new();
    env.insert("SANDBOX_TYPE".to_string(), "example-workload".to_string());
    let setup = PerSandboxSetup::resolve(
        &actor,
        EnvMap::try_from(env).unwrap(),
        &auto_lifecycle,
        Some(&override_policy),
        &template_policy,
        true,
        Default::default(),
    );
    assert!(setup.volume_encryption);
    assert_eq!(setup.sandbox_owner, actor.sandbox_owner);
    assert_eq!(setup.process_env.as_map().len(), 1);
    assert!(!format!("{setup:?}").contains("example-workload"));
    assert_eq!(setup.network_policy.effective_policy, override_policy);
    assert_eq!(
        setup.network_policy.override_policy.as_ref(),
        Some(&override_policy)
    );
    assert_eq!(setup.network_policy.authority, PolicyAuthority::Cluster);
    assert_eq!(
        setup.node_fs,
        NodeFilesystemSetup::EnsureArtifactDirPermissions
    );
}
