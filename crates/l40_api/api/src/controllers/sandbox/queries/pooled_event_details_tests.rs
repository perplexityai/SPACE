use super::*;
use crate::federated_identity::FederatedOwner;
use pplx_space_common_core::types::SandboxId;
use pplx_space_common_core::types::{UpstreamIssuer, UpstreamOrgId, UpstreamSubjectId};

const DEFAULT_POOL_VCPU: &str = "2";
const DEFAULT_POOL_MEMORY_MIB: &str = "2048";
const EXPLICIT_POOL_VCPU: &str = "4";
const EXPLICIT_POOL_MEMORY_MIB: &str = "8192";

fn pool_event_details(
    pool_shape: PooledExecutionPoolShape,
    event_type: SandboxEventType,
) -> SandboxEventDetails {
    let requested_template = TemplateReference::latest(TemplateName::parse("seed").unwrap());
    let resources = pool_attempt_resources(Some(pool_shape));
    let context = SandboxCreateEventContext {
        requested_template: &requested_template,
        template: "seed",
        content_hash: "sha256:pooled",
        runtime: "fc",
        boot_mode: CreateBootMode::Fallthrough,
        resources: &resources,
        federated_owner: None,
        volume_encryption: false,
        tls_interception: TlsInterception::Off,
    };
    match event_type {
        SandboxEventType::PoolSlotMissed => context.details().result("miss"),
        SandboxEventType::PoolSlotClaimed => pool_slot_claimed_event_details(
            &PooledExecutionClaim {
                claim_id: Uuid::now_v7(),
                pool_slot_id: Some(Uuid::now_v7()),
                sandbox_id: SandboxId::new(),
                content_hash: "sha256:pooled".to_string(),
                pod_name: "space-sbx-pooled-g1".to_string(),
                pod_uid: None,
                execution_generation: 1,
                pod_ip: "10.0.0.10".to_string(),
                vcpu: Some(pool_shape.vcpu),
                memory_mib: Some(pool_shape.memory_mib),
                disk_mib: pool_shape.disk_mib,
                disk_io_limits: pool_shape.disk_io_limits,
                active_node_id: Some(NodeId::new()),
                route_identity: None,
            },
            &context,
        ),
        _ => panic!("unsupported pool event type"),
    }
}

fn federated_owner() -> FederatedOwner {
    FederatedOwner {
        issuer: UpstreamIssuer::parse("example-issuer").unwrap(),
        org_id: UpstreamOrgId::parse("8a3f64c2-1f2b-4c4e-9a90-1f0d4f6f5a01").unwrap(),
        subject_id: UpstreamSubjectId::parse("123").unwrap(),
    }
}

#[test]
fn pool_slot_claimed_event_details_preserve_federated_owner() {
    let owner = federated_owner();
    let claim = PooledExecutionClaim {
        claim_id: Uuid::now_v7(),
        pool_slot_id: Some(Uuid::now_v7()),
        sandbox_id: SandboxId::new(),
        content_hash: "sha256:pooled".to_string(),
        pod_name: "space-sbx-pooled-g1".to_string(),
        pod_uid: None,
        execution_generation: 1,
        pod_ip: "10.0.0.10".to_string(),
        vcpu: Some(2),
        memory_mib: Some(4096),
        disk_mib: None,
        disk_io_limits: Default::default(),
        active_node_id: Some(NodeId::new()),
        route_identity: None,
    };
    let requested_template = TemplateReference::latest(TemplateName::parse("seed").unwrap());
    let resources = SandboxResources {
        vcpu: claim.vcpu,
        memory_mib: claim.memory_mib,
        disk_mib: None,
    };
    let create_event_context = SandboxCreateEventContext {
        requested_template: &requested_template,
        template: "seed",
        content_hash: &claim.content_hash,
        runtime: "fc",
        boot_mode: CreateBootMode::Pool,
        resources: &resources,
        federated_owner: Some(&owner),
        volume_encryption: false,
        tls_interception: TlsInterception::Off,
    };

    let details = pool_slot_claimed_event_details(&claim, &create_event_context);

    let details = format!("{details:?}");
    assert!(details.contains("owner_source: Some(\"federated_identity\")"));
    assert!(details.contains("upstream_issuer: Some(\"example-issuer\")"));
    assert!(details.contains("upstream_org_id: Some(\"8a3f64c2-1f2b-4c4e-9a90-1f0d4f6f5a01\")"));
    assert!(details.contains("upstream_subject_id: Some(\"123\")"));
}

#[test]
fn default_pool_hit_and_miss_emit_the_same_effective_shape() {
    let shape = PooledExecutionPoolShape::new(
        DEFAULT_POOL_VCPU.parse().unwrap(),
        DEFAULT_POOL_MEMORY_MIB.parse().unwrap(),
        Default::default(),
    );

    for event_type in [
        SandboxEventType::PoolSlotClaimed,
        SandboxEventType::PoolSlotMissed,
    ] {
        let details = format!("{:?}", pool_event_details(shape, event_type));
        assert!(
            details.contains(&format!("vcpu: Some(\"{DEFAULT_POOL_VCPU}\")")),
            "{details}"
        );
        assert!(
            details.contains(&format!("memory_mib: Some(\"{DEFAULT_POOL_MEMORY_MIB}\")")),
            "{details}"
        );
    }
}

#[test]
fn explicit_pool_hit_and_miss_emit_the_same_effective_shape() {
    let shape = PooledExecutionPoolShape::new(
        EXPLICIT_POOL_VCPU.parse().unwrap(),
        EXPLICIT_POOL_MEMORY_MIB.parse().unwrap(),
        Default::default(),
    );

    for event_type in [
        SandboxEventType::PoolSlotClaimed,
        SandboxEventType::PoolSlotMissed,
    ] {
        let details = format!("{:?}", pool_event_details(shape, event_type));
        assert!(
            details.contains(&format!("vcpu: Some(\"{EXPLICIT_POOL_VCPU}\")")),
            "{details}"
        );
        assert!(
            details.contains(&format!("memory_mib: Some(\"{EXPLICIT_POOL_MEMORY_MIB}\")")),
            "{details}"
        );
    }
}

#[test]
fn missing_pool_does_not_invent_shape_facets() {
    let event = pool_attempt_resources(None);

    assert_eq!(event.vcpu, None);
    assert_eq!(event.memory_mib, None);
    assert_eq!(event.disk_mib, None);
}

#[test]
fn pool_hit_and_miss_report_policy_disk_capacity() {
    let shape = PooledExecutionPoolShape::with_disk_mib(2, 4096, Some(32768), Default::default());

    for event_type in [
        SandboxEventType::PoolSlotClaimed,
        SandboxEventType::PoolSlotMissed,
    ] {
        let details = format!("{:?}", pool_event_details(shape, event_type));
        assert!(details.contains("disk_mib: Some(\"32768\")"), "{details}");
    }
}

#[test]
fn create_event_details_carry_volume_encryption_stamp() {
    let requested_template = TemplateReference::latest(TemplateName::parse("seed").unwrap());
    let resources = SandboxResources {
        vcpu: Some(2),
        memory_mib: Some(4096),
        disk_mib: None,
    };

    for volume_encryption in [true, false] {
        let details = format!(
            "{:?}",
            SandboxCreateEventContext {
                requested_template: &requested_template,
                template: "seed",
                content_hash: "sha256:cold",
                runtime: "fc",
                boot_mode: CreateBootMode::Fallthrough,
                resources: &resources,
                federated_owner: None,
                volume_encryption,
                tls_interception: TlsInterception::Off,
            }
            .details()
        );
        assert!(
            details.contains(&format!("volume_encryption: Some({volume_encryption})")),
            "{details}"
        );
    }
}

#[test]
fn create_event_details_carry_effective_tls_interception_mode() {
    let requested_template = TemplateReference::latest(TemplateName::parse("seed").unwrap());
    let resources = SandboxResources {
        vcpu: Some(2),
        memory_mib: Some(4096),
        disk_mib: None,
    };

    for (tls_interception, expected) in [(TlsInterception::On, "on"), (TlsInterception::Off, "off")]
    {
        let details = format!(
            "{:?}",
            SandboxCreateEventContext {
                requested_template: &requested_template,
                template: "seed",
                content_hash: "sha256:cold",
                runtime: "fc",
                boot_mode: CreateBootMode::Fallthrough,
                resources: &resources,
                federated_owner: None,
                volume_encryption: false,
                tls_interception,
            }
            .details()
        );
        assert!(
            details.contains(&format!("tls_interception: Some(\"{expected}\")")),
            "{details}"
        );
    }
}

#[test]
fn effective_tls_interception_prefers_sandbox_override_over_template_default() {
    let template = NetworkPolicyV1 {
        tls_interception: TlsInterception::Off,
        ..Default::default()
    };
    let override_on = NetworkPolicyV1 {
        tls_interception: TlsInterception::On,
        ..Default::default()
    };

    assert_eq!(
        effective_tls_interception(None, &template),
        TlsInterception::Off
    );
    assert_eq!(
        effective_tls_interception(Some(&override_on), &template),
        TlsInterception::On
    );
}
