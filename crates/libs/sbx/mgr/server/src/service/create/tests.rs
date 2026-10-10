use pplx_space_common_core::types::{NodeId, SandboxId};

use super::*;

struct RefusedCreate {
    name: &'static str,
    req: CreateRequest,
    id: String,
    effective_mode: BootMode,
    content_hash_resolved: Option<&'static str>,
    config: crate::config::SandboxConfig,
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

// Every refusal ahead of the durable insert must leave neither a sandbox row
// nor a cleanup episode: a stranded `Creating` row would need cleanup that no
// caller requested. Successful creates need the fixed sandbox cache root, so
// only the refusals are reachable here.
#[tokio::test]
async fn create_refusals_before_insert_persist_nothing() {
    let other_sandbox = SandboxId::new().to_string();
    let cases = [
        RefusedCreate {
            name: "non-uuid sandbox id",
            req: create_request(),
            id: "not-a-sandbox-id".to_owned(),
            effective_mode: BootMode::Cold,
            content_hash_resolved: Some("hash"),
            config: Default::default(),
            code: tonic::Code::InvalidArgument,
            message: "invalid sandbox id",
        },
        RefusedCreate {
            name: "base snapshot without file-backed memory",
            req: create_request(),
            id: SandboxId::new().to_string(),
            effective_mode: BootMode::BaseSnapshot,
            content_hash_resolved: Some("hash"),
            config: crate::config::SandboxConfig {
                hugepages: true,
                ..Default::default()
            },
            code: tonic::Code::FailedPrecondition,
            message: "file-backed memory",
        },
        RefusedCreate {
            name: "template hash cannot be computed",
            req: create_request(),
            id: SandboxId::new().to_string(),
            effective_mode: BootMode::Cold,
            content_hash_resolved: None,
            config: Default::default(),
            code: tonic::Code::Internal,
            message: "compute template hash",
        },
        RefusedCreate {
            name: "pool member with snapshots",
            req: CreateRequest {
                pool_template: Some("pool".to_owned()),
                snapshots_enabled: true,
                ..create_request()
            },
            id: SandboxId::new().to_string(),
            effective_mode: BootMode::Cold,
            content_hash_resolved: Some("hash"),
            config: Default::default(),
            code: tonic::Code::InvalidArgument,
            message: "snapshots_enabled",
        },
        RefusedCreate {
            name: "pool member with volume encryption",
            req: CreateRequest {
                pool_template: Some("pool".to_owned()),
                volume_encryption: true,
                ..create_request()
            },
            id: SandboxId::new().to_string(),
            effective_mode: BootMode::Cold,
            content_hash_resolved: Some("hash"),
            config: Default::default(),
            code: tonic::Code::InvalidArgument,
            message: "volume_encryption",
        },
        RefusedCreate {
            name: "route identity bound to another sandbox",
            req: CreateRequest {
                route_identity: Some(crate::proto::SpaceApiRouteIdentity {
                    sandbox_id: Some(other_sandbox.clone()),
                    ..Default::default()
                }),
                ..create_request()
            },
            id: SandboxId::new().to_string(),
            effective_mode: BootMode::Cold,
            content_hash_resolved: Some("hash"),
            config: Default::default(),
            code: tonic::Code::InvalidArgument,
            message: "does not match the resolved sandbox",
        },
        RefusedCreate {
            name: "unknown auto-lifecycle authority",
            req: CreateRequest {
                auto_lifecycle: Some(Default::default()),
                auto_lifecycle_authority: i32::MAX,
                ..create_request()
            },
            id: SandboxId::new().to_string(),
            effective_mode: BootMode::Cold,
            content_hash_resolved: Some("hash"),
            config: Default::default(),
            code: tonic::Code::InvalidArgument,
            message: "policy authority",
        },
        RefusedCreate {
            name: "partial disk I/O assignment",
            req: CreateRequest {
                disk_io_max_operations_per_sec: Some(1_000),
                ..create_request()
            },
            id: SandboxId::new().to_string(),
            effective_mode: BootMode::Cold,
            content_hash_resolved: Some("hash"),
            config: Default::default(),
            code: tonic::Code::InvalidArgument,
            message: "both be present or both be absent",
        },
    ];

    let template_dir = tempfile::tempdir().unwrap();
    let template_dir = template_dir.path().to_str().unwrap();
    let owner = SandboxOwner::parse("user").unwrap();
    let auto_lifecycle = AutoLifecyclePolicy::disabled();
    for case in cases {
        let service = SandboxManagerService::new_with_config(
            crate::db::test_conn(),
            SandboxManagerServiceOptions {
                local_node_id: Some(NodeId::new()),
                ..Default::default()
            },
            case.config,
        );
        let error = service
            .create_with_id(CreateWithIdArgs {
                req: &case.req,
                sandbox_owner: &owner,
                effective_mode: case.effective_mode,
                id: case.id.clone(),
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
            crate::db::get_sandbox(&db, &case.id).unwrap().is_none(),
            "{}: refused create persisted a sandbox row",
            case.name
        );
        assert!(
            crate::db::cleanup_episode(&db, &case.id).unwrap().is_none(),
            "{}: refused create persisted a cleanup episode",
            case.name
        );
    }
}
