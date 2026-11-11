use super::{
    AdminUserResponse, CreateSandboxRequest, CreateTemplateRequest, SandboxFederatedServiceGrant,
    SandboxMetadataValue, SandboxResponse, UpdateOrgRequest, UpdateSandboxRequest,
    UpdateTemplateRequest, UpdateUserRequest, User,
};
use crate::user_role::UserRole;
use pplx_space_common_core::types::{
    OrgId, SandboxAutoLifecyclePatch, SandboxId, SandboxOwnerSource, UserId,
};
use time::OffsetDateTime;

#[test]
fn federated_service_grant_debug_redacts_the_grant_blob() {
    let secret = b"super-secret-opaque-federated-service-grant";
    let grant = SandboxFederatedServiceGrant {
        sandbox_id: SandboxId::new(),
        grant_audience: "credential-provider".to_string(),
        grant_purpose: "federated_owner".to_string(),
        grant_blob: secret.to_vec(),
        grant_expires_at: OffsetDateTime::now_utc(),
        created_at: OffsetDateTime::now_utc(),
        updated_at: OffsetDateTime::now_utc(),
    };

    let rendered = format!("{grant:?}");

    assert!(
        !rendered.contains("super-secret"),
        "grant blob leaked into Debug: {rendered}"
    );
    assert!(
        rendered.contains(&format!("[redacted {} bytes]", secret.len())),
        "{rendered}"
    );
    // Routing metadata stays visible for debugging.
    assert!(rendered.contains("federated_owner"));
}

#[test]
fn update_user_request_rejects_role_field() {
    let request = serde_json::from_value::<UpdateUserRequest>(serde_json::json!({
        "display_name": "Still Member",
        "role": "organization_admin",
    }));

    let err = request.unwrap_err();
    assert!(err.to_string().contains("unknown field"));
}

#[test]
fn admin_user_response_exposes_only_password_login_status() {
    for (owner_source, password_hash, expected) in [
        (SandboxOwnerSource::Local, "!", false),
        (SandboxOwnerSource::Local, "disabled-service-account", false),
        (SandboxOwnerSource::Local, "bcrypt-hash", true),
        (
            SandboxOwnerSource::FederatedIdentity,
            "federated-user-no-local-password",
            false,
        ),
        (SandboxOwnerSource::FederatedIdentity, "bcrypt-hash", false),
    ] {
        let user = User {
            id: UserId::new(),
            email: "operator@example.test".to_string(),
            password_hash: password_hash.to_string(),
            display_name: "Operator".to_string(),
            org_id: OrgId::new(),
            role: UserRole::Member,
            owner_source,
            created_at: OffsetDateTime::now_utc(),
            updated_at: OffsetDateTime::now_utc(),
        };

        let ordinary_value = serde_json::to_value(&user).unwrap();
        assert!(ordinary_value.get("password_login_configured").is_none());
        assert!(ordinary_value.get("password_hash").is_none());
        assert!(ordinary_value.get("owner_source").is_none());

        let response = AdminUserResponse::from(user);
        let value = serde_json::to_value(response).unwrap();
        assert_eq!(value["password_login_configured"], expected);
        assert!(value.get("password_hash").is_none());
    }
}

#[test]
fn update_org_request_byok_enabled_distinguishes_omitted_from_set() {
    let omitted: UpdateOrgRequest = serde_json::from_value(serde_json::json!({})).unwrap();
    assert_eq!(omitted.byok_enabled, None);

    let enabled: UpdateOrgRequest =
        serde_json::from_value(serde_json::json!({ "byok_enabled": true })).unwrap();
    assert_eq!(enabled.byok_enabled, Some(true));

    let disabled: UpdateOrgRequest =
        serde_json::from_value(serde_json::json!({ "byok_enabled": false })).unwrap();
    assert_eq!(disabled.byok_enabled, Some(false));
}

#[test]
fn sandbox_response_serializes_volume_encryption() {
    let response = SandboxResponse {
        volume_encryption: true,
        ..Default::default()
    };
    let value = serde_json::to_value(&response).unwrap();
    assert_eq!(value["volume_encryption"], serde_json::json!(true));
}

#[test]
fn update_org_request_distinguishes_omitted_from_clear() {
    let omitted: UpdateOrgRequest = serde_json::from_value(serde_json::json!({})).unwrap();
    assert_eq!(omitted.logo_url, None);

    let clear: UpdateOrgRequest = serde_json::from_value(serde_json::json!({
        "logo_url": null,
        "snapshot_base_object_storage_uri": null,
    }))
    .unwrap();
    assert_eq!(clear.logo_url, Some(super::NullablePatch::Clear));
    assert_eq!(
        clear.snapshot_base_object_storage_uri,
        Some(super::NullablePatch::Clear)
    );

    let set: UpdateOrgRequest = serde_json::from_value(serde_json::json!({
        "logo_url": "https://example.test/logo.png",
        "snapshot_base_object_storage_uri": "s3://bucket/prefix?region=us-west-2",
    }))
    .unwrap();
    assert_eq!(
        set.logo_url,
        Some(super::NullablePatch::Set(
            "https://example.test/logo.png".to_string()
        ))
    );
    assert_eq!(
        set.snapshot_base_object_storage_uri,
        Some(super::NullablePatch::Set(
            "s3://bucket/prefix?region=us-west-2".to_string()
        ))
    );
}

#[test]
fn update_sandbox_request_preserves_omitted_fields_as_none() {
    let req: UpdateSandboxRequest = serde_json::from_value(serde_json::json!({})).unwrap();

    assert_eq!(req.vcpu, None);
    assert_eq!(req.memory_mib, None);
    assert_eq!(req.pids_max, None);
}

#[test]
fn update_sandbox_request_preserves_explicit_zero() {
    let req: UpdateSandboxRequest = serde_json::from_value(serde_json::json!({
        "vcpu": 0,
        "memory_mib": 0,
        "pids_max": 0
    }))
    .unwrap();

    assert_eq!(req.vcpu, Some(0));
    assert_eq!(req.memory_mib, Some(0));
    assert_eq!(req.pids_max, Some(0));
}

#[test]
fn update_sandbox_request_parses_set_auto_lifecycle_patch() {
    let req: UpdateSandboxRequest = serde_json::from_value(serde_json::json!({
        "auto_lifecycle": {
            "mode": "set",
            "policy": {}
        }
    }))
    .unwrap();

    assert!(matches!(
        req.auto_lifecycle,
        Some(SandboxAutoLifecyclePatch::Set { .. })
    ));
}

#[test]
fn update_sandbox_request_parses_inherit_auto_lifecycle_patch() {
    let req: UpdateSandboxRequest = serde_json::from_value(serde_json::json!({
        "auto_lifecycle": {
            "mode": "inherit_template_default"
        }
    }))
    .unwrap();

    assert_eq!(
        req.auto_lifecycle,
        Some(SandboxAutoLifecyclePatch::InheritTemplateDefault)
    );
}

#[test]
fn update_sandbox_request_rejects_unknown_fields() {
    let err = serde_json::from_value::<UpdateSandboxRequest>(serde_json::json!({
        "auto_lifecycle": {
            "mode": "inherit_template_default"
        },
        "inherit_template_auto_lifecycle": true
    }))
    .unwrap_err();

    assert!(err.to_string().contains("unknown field"));
}

#[test]
fn network_policy_write_requests_accept_domain_denies_while_deserializing() {
    let policy = serde_json::json!({
        "version": "v1",
        "egress": {
            "default_action": "deny",
            "deny": [{"domain": "blocked.example.com"}]
        }
    });

    serde_json::from_value::<CreateSandboxRequest>(serde_json::json!({
        "network_policy": policy.clone()
    }))
    .unwrap();
    serde_json::from_value::<UpdateSandboxRequest>(serde_json::json!({
        "network_policy": {"mode": "set", "policy": policy.clone()}
    }))
    .unwrap();
    serde_json::from_value::<CreateTemplateRequest>(serde_json::json!({
        "name": "seed",
        "network_policy": policy.clone()
    }))
    .unwrap();
    serde_json::from_value::<UpdateTemplateRequest>(serde_json::json!({
        "network_policy": policy
    }))
    .unwrap();
}

#[test]
fn network_policy_write_requests_accept_tls_interception_on() {
    use pplx_space_net_common::TlsInterception;

    let policy = serde_json::json!({
        "version": "v1",
        "egress": {"default_action": "deny"},
        "tls_interception": "on"
    });

    let create = serde_json::from_value::<CreateSandboxRequest>(serde_json::json!({
        "network_policy": policy.clone()
    }))
    .unwrap();
    assert_eq!(
        create.network_policy.unwrap().tls_interception,
        TlsInterception::On
    );

    let update = serde_json::from_value::<super::UpdateSandboxRequest>(serde_json::json!({
        "network_policy": {"mode": "set", "policy": policy.clone()}
    }))
    .unwrap();
    let Some(super::SandboxNetworkPolicyPatch::Set { policy: update }) = update.network_policy
    else {
        panic!("expected set policy");
    };
    assert_eq!(update.tls_interception, TlsInterception::On);

    let template = serde_json::from_value::<CreateTemplateRequest>(serde_json::json!({
        "name": "test",
        "description": null,
        "network_policy": policy.clone()
    }))
    .unwrap();
    assert_eq!(
        template.network_policy.tls_interception,
        TlsInterception::On
    );

    let template_update =
        serde_json::from_value::<super::UpdateTemplateRequest>(serde_json::json!({
            "network_policy": policy
        }))
        .unwrap();
    assert_eq!(
        template_update.network_policy.unwrap().tls_interception,
        TlsInterception::On
    );
}

#[test]
fn sandbox_metadata_accepts_shallow_scalar_values() {
    let metadata: std::collections::BTreeMap<String, SandboxMetadataValue> =
        serde_json::from_value(serde_json::json!({
            "session": "abc",
            "priority": 3,
            "debug": true
        }))
        .unwrap();

    assert_eq!(
        metadata.get("session"),
        Some(&SandboxMetadataValue::String("abc".to_string()))
    );
    assert_eq!(
        metadata.get("priority"),
        Some(&SandboxMetadataValue::Number(3))
    );
    assert_eq!(
        metadata.get("debug"),
        Some(&SandboxMetadataValue::Bool(true))
    );
}

#[test]
fn sandbox_metadata_rejects_nested_null_and_float_values() {
    for value in [
        serde_json::json!({"nested": {"x": "y"}}),
        serde_json::json!({"items": ["x"]}),
        serde_json::json!({"missing": null}),
        serde_json::json!({"ratio": 1.25}),
    ] {
        serde_json::from_value::<std::collections::BTreeMap<String, SandboxMetadataValue>>(value)
            .unwrap_err();
    }
}
