//! Create's auto-lifecycle authority checks, which run just before template
//! lookup, against a scripted template resolver. Template-manager and
//! node-manager are fakes, so a request that gets past a check stops at the
//! resolver's scripted answer.

use super::*;
use crate::node_manager_transport::fake::FakeNodeManager;
use crate::template_resolver::TemplateLookupError;
use crate::template_resolver::fake::{FakeTemplateResolver, LookupChannel};
use pplx_space_common_core::host::linux::identities::users;
use pplx_space_common_svc::PeerIdentity;
use pplx_space_net_common::proto::PolicyAuthority;
use pplx_space_template_mgr_client::proto::{GetTemplateRequest, TemplateRef};

/// The resolver's refusal, distinct from every refusal a check produces before
/// lookup.
const LOOKUP_REFUSAL: &str = "scripted template lookup refusal";

fn refusing_lookup() -> FakeTemplateResolver {
    FakeTemplateResolver::refusing(Status::not_found(LOOKUP_REFUSAL))
}

fn unreachable_template_manager() -> FakeTemplateResolver {
    FakeTemplateResolver::new(|| {
        let invalid = "http://[".parse::<http::Uri>().unwrap_err();
        Err(TemplateLookupError::Connect(
            pplx_space_template_mgr_client::ConnectError::InvalidUri(invalid),
        ))
    })
}

#[tokio::test]
async fn create_checks_auto_lifecycle_authority_before_template_lookup() {
    if !crate::service::tests::enter_route_test_process(&[
        ("EPPO_SDK_KEY", None),
        ("SPACE_FLAGD_ENABLED", None),
    ]) {
        return;
    }
    let forwarded = PeerIdentity::new(
        users::SPACE_NODEAPI.uid,
        users::SPACE_NODEAPI.primary_group.gid,
        None,
    );
    let local = PeerIdentity::new(0, 0, None);
    struct Case {
        name: &'static str,
        peer: PeerIdentity,
        authority: i32,
        templates: fn() -> FakeTemplateResolver,
        code: tonic::Code,
        message_prefix: &'static str,
        message_contains: &'static str,
        looks_up_template: bool,
    }
    let cases = [
        Case {
            name: "local authority from a forwarded peer",
            peer: forwarded.clone(),
            authority: PolicyAuthority::Local as i32,
            templates: refusing_lookup,
            code: tonic::Code::PermissionDenied,
            message_prefix: "authority=local requires a local operator peer",
            message_contains: "",
            looks_up_template: false,
        },
        Case {
            name: "unspecified authority",
            peer: forwarded.clone(),
            authority: PolicyAuthority::Unspecified as i32,
            templates: refusing_lookup,
            code: tonic::Code::InvalidArgument,
            message_prefix: "policy authority proto value is unspecified",
            message_contains: "",
            looks_up_template: false,
        },
        Case {
            name: "unknown authority",
            peer: forwarded.clone(),
            authority: 99,
            templates: refusing_lookup,
            code: tonic::Code::InvalidArgument,
            message_prefix: "policy authority proto value 99 is unknown",
            message_contains: "",
            looks_up_template: false,
        },
        Case {
            name: "cluster authority from a forwarded peer",
            peer: forwarded.clone(),
            authority: PolicyAuthority::Cluster as i32,
            templates: refusing_lookup,
            code: tonic::Code::NotFound,
            message_prefix: "GetTemplate failed: ",
            message_contains: LOOKUP_REFUSAL,
            looks_up_template: true,
        },
        Case {
            name: "local authority from a local peer",
            peer: local,
            authority: PolicyAuthority::Local as i32,
            templates: refusing_lookup,
            code: tonic::Code::NotFound,
            message_prefix: "GetTemplate failed: ",
            message_contains: LOOKUP_REFUSAL,
            looks_up_template: true,
        },
        Case {
            name: "accepted authority with template-manager unreachable",
            peer: forwarded,
            authority: PolicyAuthority::Cluster as i32,
            templates: unreachable_template_manager,
            code: tonic::Code::Internal,
            message_prefix: "cannot connect to template-manager: ",
            message_contains: "invalid gRPC endpoint URI",
            looks_up_template: true,
        },
    ];
    for case in cases {
        let templates = Arc::new((case.templates)());
        let node_manager = FakeNodeManager::default().serve();
        let service = SandboxManagerService::new_with_options(
            crate::db::test_conn(),
            SandboxManagerServiceOptions {
                node_manager: node_manager.connector(),
                template_resolver: Arc::clone(&templates) as _,
                ..SandboxManagerServiceOptions::for_test()
            },
        );
        let mut request = Request::new(CreateRequest {
            template: "seed".into(),
            owner: "root@local".into(),
            runtime: pplx_space_common_core::types::RUNTIME_FIRECRACKER_ALIAS.into(),
            boot_mode: BootMode::Cold as i32,
            sandbox_id: Some(SandboxId::new().to_string()),
            auto_lifecycle: Some(Default::default()),
            auto_lifecycle_authority: case.authority,
            ..Default::default()
        });
        request.extensions_mut().insert(case.peer);

        let error = service.create(request).await.expect_err(case.name);

        assert_eq!(error.code(), case.code, "{}: {error}", case.name);
        assert!(
            error.message().starts_with(case.message_prefix)
                && error.message().contains(case.message_contains),
            "{}: {error}",
            case.name
        );
        let lookups = templates.lookups();
        let expected_lookups = if case.looks_up_template {
            vec![GetTemplateRequest {
                caller_org_id: None,
                template: Some(TemplateRef {
                    name: "seed".into(),
                    content_hash: None,
                    tag: None,
                }),
                vcpu: None,
                memory_mib: None,
            }]
        } else {
            Vec::new()
        };
        assert_eq!(templates.requests(), expected_lookups, "{}", case.name);
        // Create dials template-manager for its lookup and sends no metadata.
        for lookup in &lookups {
            assert_eq!(lookup.channel, LookupChannel::Dialed, "{}", case.name);
            assert!(lookup.metadata.is_empty(), "{}", case.name);
        }
        assert!(node_manager.calls().is_empty(), "{}", case.name);
    }
}
