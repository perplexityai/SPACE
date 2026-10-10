use pplx_space_common_core::types::{NodeId, SandboxId};

use super::*;

struct RefusedCreate {
    name: &'static str,
    req: CreateRequest,
    content_hash_resolved: Option<&'static str>,
    code: tonic::Code,
    message: &'static str,
}

fn create_request() -> CreateRequest {
    CreateRequest {
        template: "seed".to_owned(),
        runtime: "fc".to_owned(),
        ..Default::default()
    }
}

// A refusal ahead of the durable insert must leave neither a sandbox row nor a
// cleanup episode: a stranded `Creating` row would need cleanup that no caller
// requested. The Create RPC validates the request before it gets here, so only
// refusals that depend on the resolved template or the resolved sandbox id are
// listed. Successful creates need the fixed sandbox cache root.
#[tokio::test]
async fn create_refusals_before_insert_persist_nothing() {
    let cases = [
        RefusedCreate {
            name: "template hash cannot be computed",
            req: create_request(),
            content_hash_resolved: None,
            code: tonic::Code::Internal,
            message: "compute template hash",
        },
        RefusedCreate {
            name: "route identity bound to another sandbox",
            req: CreateRequest {
                route_identity: Some(crate::proto::SpaceApiRouteIdentity {
                    sandbox_id: Some(SandboxId::new().to_string()),
                    ..Default::default()
                }),
                ..create_request()
            },
            content_hash_resolved: Some("hash"),
            code: tonic::Code::InvalidArgument,
            message: "does not match the resolved sandbox",
        },
    ];

    let template_dir = tempfile::tempdir().unwrap();
    let template_dir = template_dir.path().to_str().unwrap();
    let owner = SandboxOwner::parse("user").unwrap();
    let auto_lifecycle = AutoLifecyclePolicy::disabled();
    for case in cases {
        let service = SandboxManagerService::new_with_options(
            crate::db::test_conn(),
            SandboxManagerServiceOptions {
                local_node_id: Some(NodeId::new()),
                ..Default::default()
            },
        );
        let id = SandboxId::new().to_string();
        let error = service
            .create_with_id(CreateWithIdArgs {
                req: &case.req,
                sandbox_owner: &owner,
                effective_mode: BootMode::Cold,
                id: id.clone(),
                template_dir,
                snapshot_dir_resolved: None,
                snapshot_vcpu: None,
                content_hash_resolved: case.content_hash_resolved,
                auto_lifecycle: &auto_lifecycle,
                auto_lifecycle_authority: crate::db::PolicyAuthority::Cluster,
                default_network_policy_authority: crate::db::PolicyAuthority::Cluster,
            })
            .await
            .expect_err(case.name);

        assert_eq!(error.code(), case.code, "{}: {error:?}", case.name);
        assert!(
            error.message().contains(case.message),
            "{}: {error:?}",
            case.name
        );
        let db = service.db_conn().unwrap();
        assert!(
            crate::db::get_sandbox(&db, &id).unwrap().is_none(),
            "{}: refused create persisted a sandbox row",
            case.name
        );
        assert!(
            crate::db::cleanup_episode(&db, &id).unwrap().is_none(),
            "{}: refused create persisted a cleanup episode",
            case.name
        );
    }
}
