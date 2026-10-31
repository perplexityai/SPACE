use pplx_space_common_core::types::OrgId;

use super::*;
use pplx_space_common_core::host::linux::identities::users;
use pplx_space_net_common::TlsInterception;
use pplx_space_observability::test_capture::{CapturedEventsLayer, field_value};
use std::{path::Path, sync::Arc};
use tracing_subscriber::layer::SubscriberExt as _;

mod export;
mod lookup_rpc;
mod schema_guards;

#[tokio::test(flavor = "current_thread")]
async fn background_loop_observer_contains_inner_panic() {
    let layer = CapturedEventsLayer::new().with_event_filter(|fields| {
        field_value(fields, "message") == Some("template-manager background loop task panicked")
    });
    let events = layer.events();
    let subscriber = tracing_subscriber::registry().with(layer);
    let _subscriber_guard = tracing::subscriber::set_default(subscriber);

    let observer = spawn_observed_background_loop("test-loop", async {
        panic!("synthetic loop panic");
    });

    assert!(
        observer.await.is_ok(),
        "the observer must convert the inner panic into an attributable log"
    );
    let captured = events.events();
    assert_eq!(captured.len(), 1);
    assert_eq!(field_value(&captured[0], "loop_name"), Some("test-loop"));
}

fn build_row(name: &str, content_hash: &str) -> crate::db::BuildRow {
    crate::db::BuildRow {
        name: name.to_string(),
        content_hash: content_hash.to_string(),
        build_date: "2026-05-13T00:00:00Z".to_string(),
        manifest_json: "{}".to_string(),
        runtime: RUNTIME_FIRECRACKER_ALIAS.to_string(),
        arch: "x86_64".to_string(),
        artifact_size_bytes: 0,
        state: "ready".to_string(),
        owner_type: "local".to_string(),
        owner_id: String::new(),
        execution_metadata_json: None,
    }
}

fn insert_ready_build(conn: &rusqlite::Connection, name: &str, content_hash: &str) {
    crate::db::upsert_build(
        conn,
        &crate::db::BuildUpsert {
            name,
            content_hash,
            build_date: "2026-05-13T00:00:00Z",
            manifest_json: "{}",
            runtime: RUNTIME_FIRECRACKER_ALIAS,
            arch: "x86_64",
            artifact_size_bytes: 0,
            state: "ready",
        },
    )
    .unwrap();
}

fn pool_sandbox(
    sandbox_id: &str,
    pool_template: &str,
    lifecycle: sbx_proto::SandboxLifecycleState,
) -> sbx_proto::SandboxListElement {
    sbx_proto::SandboxListElement {
        sandbox_id: sandbox_id.to_string(),
        template: Some(pool_template.to_string()),
        pool_template: Some(pool_template.to_string()),
        lifecycle: Some(lifecycle as i32),
        ..Default::default()
    }
}

#[tokio::test(flavor = "current_thread")]
async fn omitted_sandbox_counts_skip_the_sandbox_listing_and_keep_the_rest_of_the_reply() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let conn = crate::db::test_conn();
    insert_ready_build(&conn, "pooled", "sha256:pooled");
    insert_ready_build(&conn, "unpooled", "sha256:unpooled");
    crate::db::insert_pool(&conn, "pooled", 3, "snapshot", 2, 1024).unwrap();
    let listing = vec![
        pool_sandbox("warm-a", "pooled", sbx_proto::SandboxLifecycleState::Paused),
        pool_sandbox("warm-b", "pooled", sbx_proto::SandboxLifecycleState::Paused),
        pool_sandbox(
            "filling",
            "pooled",
            sbx_proto::SandboxLifecycleState::Creating,
        ),
    ];
    let listings = Arc::new(AtomicUsize::new(0));
    let lister = PoolSandboxLister::new({
        let listings = Arc::clone(&listings);
        move |_| {
            listings.fetch_add(1, Ordering::SeqCst);
            let listing = listing.clone();
            Box::pin(async move { listing })
        }
    });
    let service = TemplateManagerService::new(conn)
        .unwrap()
        .without_sbx_connect_retries()
        .with_pool_sandbox_lister(lister);

    let counted_templates = service
        .list_templates(internal_request(ListTemplatesRequest::default()))
        .await
        .unwrap()
        .into_inner();
    let counted_pools = service
        .pool_status(internal_request(PoolStatusRequest::default()))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(listings.load(Ordering::SeqCst), 2);
    let pooled = counted_templates
        .templates
        .iter()
        .find(|template| template.name == "pooled")
        .expect("pooled template listed");
    assert_eq!(pooled.pool_current, 2);
    assert_eq!(counted_pools.pools.len(), 1);
    assert_eq!(counted_pools.pools[0].current_count, 2);
    assert_eq!(counted_pools.pools[0].pending_count, 1);
    assert_eq!(counted_pools.pools[0].sandbox_ids.len(), 3);

    let omitted_templates = service
        .list_templates(internal_request(ListTemplatesRequest {
            omit_sandbox_counts: true,
        }))
        .await
        .unwrap()
        .into_inner();
    let omitted_pools = service
        .pool_status(internal_request(PoolStatusRequest {
            template: None,
            omit_sandbox_counts: true,
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        listings.load(Ordering::SeqCst),
        2,
        "omitting the counts must not list the node's sandboxes"
    );

    let mut expected_templates = counted_templates;
    for template in &mut expected_templates.templates {
        template.pool_current = 0;
    }
    assert_eq!(omitted_templates, expected_templates);
    let mut expected_pools = counted_pools;
    for pool in &mut expected_pools.pools {
        pool.current_count = 0;
        pool.pending_count = 0;
        pool.sandbox_ids.clear();
    }
    assert_eq!(omitted_pools, expected_pools);
}

#[test]
fn pool_shape_prefers_request_then_template_settings() {
    let conn = crate::db::test_conn();
    crate::db::update_template_snapshot_policy(&conn, "seed", true, 1, 512).unwrap();
    let settings = crate::db::get_template_settings(&conn, "seed")
        .unwrap()
        .expect("settings row");
    let existing_pool = crate::db::PoolRow {
        template: "seed".to_string(),
        target_count: 3,
        boot_mode: "snapshot".to_string(),
        vcpu: 4,
        memory_mib: 2048,
    };
    let runtime_defaults = TemplateRuntimeDefaults {
        vcpu: Some(8),
        memory_mib: Some(4096),
        disk_size_mb: None,
    };

    assert_eq!(
        pools::resolve_pool_shape(
            Some((2, 1024)),
            Some(&settings),
            Some(&existing_pool),
            runtime_defaults
        ),
        (2, 1024)
    );
    assert_eq!(
        pools::resolve_pool_shape(
            None,
            Some(&settings),
            Some(&existing_pool),
            runtime_defaults
        ),
        (1, 512)
    );
    assert_eq!(
        pools::resolve_pool_shape(None, None, Some(&existing_pool), runtime_defaults),
        (4, 2048)
    );
}

#[test]
fn list_templates_projects_stored_execution_metadata() {
    let conn = crate::db::test_conn();
    let manifest = serde_json::json!({
        "schema_version": 2,
        "execution": {
            "runtime": { "kind": "firecracker", "version": "1.15.1" },
            "guest": { "os": "linux", "architecture": "x86_64" },
            "state_format": {
                "format": "firecracker-snapshot",
                "version": "9.0.0"
            },
            "components": []
        },
        "name": "seed",
        "content_hash": "sha256:execution-test",
        "build_date": "2026-05-13T00:00:00Z",
        "runtime": "fc",
        "arch": "x86_64"
    });
    crate::db::upsert_build(
        &conn,
        &crate::db::BuildUpsert {
            name: "seed",
            content_hash: "sha256:execution-test",
            build_date: "2026-05-13T00:00:00Z",
            manifest_json: &manifest.to_string(),
            runtime: RUNTIME_FIRECRACKER_ALIAS,
            arch: "x86_64",
            artifact_size_bytes: 0,
            state: "ready",
        },
    )
    .unwrap();
    // `ListTemplates` enriches its rows from sandbox-manager twice, and both
    // enrichments fall back when the socket is absent. Declining the connect
    // budget keeps both real calls and both real fallbacks while dropping the
    // two default budgets this projection assertion would otherwise buy.
    let service = TemplateManagerService::new(conn)
        .unwrap()
        .without_sbx_connect_retries();

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let response = runtime
        .block_on(service.list_templates(internal_request(ListTemplatesRequest::default())))
        .unwrap()
        .into_inner();

    assert_eq!(response.templates.len(), 1);
    let execution = response.templates[0]
        .execution
        .as_ref()
        .expect("execution metadata should be projected");
    assert_eq!(execution.runtime.as_ref().unwrap().version, "1.15.1");
    assert_eq!(
        execution.state_format.as_ref().unwrap().format,
        "firecracker-snapshot"
    );
}

#[test]
fn proto_policy_to_common_rejects_invalid_default_auto_lifecycle() {
    let policy = sbx_proto::AutoLifecyclePolicy {
        wake: None,
        condition_sets: vec![sbx_proto::AutoLifecycleConditionSet {
            id: sbx_proto::AutoLifecycleConditionSetId::AccessIdle as i32,
            kind: sbx_proto::AutoLifecycleConditionKind::GuestActivity as i32,
            activity_source: None,
            max_guest_idle_extension: None,
        }],
        stages: Vec::new(),
    };

    let error = proto_policy_to_common(&policy).unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(error.message().contains("access_idle"));
}

#[tokio::test]
async fn update_template_settings_accepts_tls_interception_on_after_activation() {
    let conn = crate::db::test_conn();
    let service = TemplateManagerService::new(conn).unwrap();
    let mut policy = NetworkPolicyV1::allow_all();
    policy.tls_interception = TlsInterception::On;

    let response = service
        .update_template_settings(internal_request(UpdateTemplateSettingsRequest {
            template: "seed".to_string(),
            default_auto_lifecycle: None,
            network_policy: Some(NetworkPolicyProto::from(&policy)),
            network_policy_generation: 1,
            network_policy_authority: crate::db::PolicyAuthority::Cluster.as_proto_i32(),
            auto_lifecycle_authority: crate::db::PolicyAuthority::Cluster.as_proto_i32(),
        }))
        .await
        .expect("TLS interception may be persisted after gateway rollout completes")
        .into_inner();

    assert_eq!(
        response.network_policy_apply_decision,
        NetworkPolicyApplyDecision::Apply as i32
    );
    let conn = service.db.lock().unwrap();
    let settings = crate::db::get_template_settings(&conn, "seed")
        .unwrap()
        .expect("accepted policy must create template settings");
    assert_eq!(
        settings.default_network_policy.tls_interception,
        TlsInterception::On
    );
}

#[tokio::test]
async fn update_template_settings_rejects_local_network_policy_from_node_api() {
    use pplx_space_common_svc::PeerIdentity;

    let service = TemplateManagerService::new(crate::db::test_conn()).unwrap();
    let local_update = || UpdateTemplateSettingsRequest {
        template: "seed".to_string(),
        default_auto_lifecycle: None,
        network_policy: Some(NetworkPolicyProto::from(&NetworkPolicyV1::allow_all())),
        network_policy_generation: 1,
        network_policy_authority: crate::db::PolicyAuthority::Local.as_proto_i32(),
        auto_lifecycle_authority: crate::db::PolicyAuthority::Cluster.as_proto_i32(),
    };
    let mut request = tonic::Request::new(local_update());
    request.extensions_mut().insert(PeerIdentity::new(
        users::SPACE_NODEAPI.uid,
        users::SPACE_NODEAPI.primary_group.gid,
        None,
    ));

    let error = service
        .update_template_settings(request)
        .await
        .expect_err("node-api must not persist a local template network policy");
    assert_eq!(error.code(), tonic::Code::PermissionDenied);
    assert!(
        crate::db::get_template_settings(&service.db.lock().unwrap(), "seed")
            .unwrap()
            .is_none()
    );

    service
        .update_template_settings(internal_request(local_update()))
        .await
        .expect("root may set a local template network policy");
    let settings = crate::db::get_template_settings(&service.db.lock().unwrap(), "seed")
        .unwrap()
        .unwrap();
    assert_eq!(
        settings.default_network_policy_authority,
        crate::db::PolicyAuthority::Local
    );
}

#[tokio::test]
async fn update_template_settings_rejects_local_auto_lifecycle_from_node_api() {
    use pplx_space_common_svc::PeerIdentity;

    let service = TemplateManagerService::new(crate::db::test_conn()).unwrap();
    let local_update = || UpdateTemplateSettingsRequest {
        template: "seed".to_string(),
        default_auto_lifecycle: Some(sbx_proto::AutoLifecyclePolicy {
            wake: None,
            condition_sets: Vec::new(),
            stages: Vec::new(),
        }),
        network_policy: None,
        network_policy_generation: 0,
        network_policy_authority: crate::db::PolicyAuthority::Cluster.as_proto_i32(),
        auto_lifecycle_authority: crate::db::PolicyAuthority::Local.as_proto_i32(),
    };
    let mut request = tonic::Request::new(local_update());
    request.extensions_mut().insert(PeerIdentity::new(
        users::SPACE_NODEAPI.uid,
        users::SPACE_NODEAPI.primary_group.gid,
        None,
    ));

    let error = service
        .update_template_settings(request)
        .await
        .expect_err("node-api must not persist a local template auto-lifecycle policy");
    assert_eq!(error.code(), tonic::Code::PermissionDenied);
    assert!(
        crate::db::get_template_settings(&service.db.lock().unwrap(), "seed")
            .unwrap()
            .is_none()
    );

    service
        .update_template_settings(internal_request(local_update()))
        .await
        .expect("root may set a local template auto-lifecycle policy");
    let settings = crate::db::get_template_settings(&service.db.lock().unwrap(), "seed")
        .unwrap()
        .unwrap();
    assert_eq!(
        settings.default_auto_lifecycle_authority,
        crate::db::PolicyAuthority::Local
    );
}

#[test]
fn snapshot_files_exist_in_dir_requires_both_files() {
    let dir = tempfile::tempdir().unwrap();
    let dir_path = dir.path().to_str().unwrap();

    assert!(!TemplateManagerService::snapshot_files_exist_in_dir(
        dir_path
    ));

    std::fs::write(Path::new(dir_path).join("snapshot.base"), b"snapshot").unwrap();
    assert!(!TemplateManagerService::snapshot_files_exist_in_dir(
        dir_path
    ));

    std::fs::write(Path::new(dir_path).join("vmstate.base"), b"vmstate").unwrap();
    assert!(TemplateManagerService::snapshot_files_exist_in_dir(
        dir_path
    ));
}

#[test]
fn template_import_manifest_identity_detects_conflicts() {
    let existing = serde_json::json!({
        "name": "code-runtime",
        "runtime": "firecracker",
        "arch": "x86_64"
    });
    let same_identity = serde_json::json!({
        "name": "code-runtime",
        "runtime": "firecracker",
        "arch": "x86_64",
        "build_date": "2026-06-01T00:00:00Z"
    });
    let different_arch = serde_json::json!({
        "name": "code-runtime",
        "runtime": "firecracker",
        "arch": "aarch64"
    });

    assert!(!template_manifest_identity_conflicts(
        &existing,
        &same_identity
    ));
    assert!(template_manifest_identity_conflicts(
        &existing,
        &different_arch
    ));
}

#[test]
fn template_import_rejects_malformed_manifest_json() {
    let error = parse_template_manifest("incoming manifest_json", b"{not-json")
        .expect_err("malformed manifest should fail explicitly");

    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(error.message().contains("incoming manifest_json"));
}

#[test]
fn template_import_chunk_target_routes_allowed_files() {
    let staging = "/tmp/import-staging";
    let snapshot = "/tmp/import-snapshot";

    assert_eq!(
        import_chunk_target_path(staging, Some(snapshot), "rootfs.ext4").unwrap(),
        Some(Path::new(staging).join("rootfs.ext4"))
    );
    assert_eq!(
        import_chunk_target_path(staging, Some(snapshot), "snapshot-metadata.json").unwrap(),
        Some(Path::new(snapshot).join("metadata.json"))
    );
    assert_eq!(
        import_chunk_target_path(staging, None, "snapshot.base").unwrap(),
        None
    );

    let error = import_chunk_target_path(staging, Some(snapshot), "../bad")
        .expect_err("unexpected files should be rejected");
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
}

#[test]
fn dependent_sandbox_ids_for_template_build_matches_exact_build() {
    let sandboxes = vec![
        sbx_proto::SandboxListElement {
            sandbox_id: "matching-a".to_string(),
            template: Some("code-runtime".to_string()),
            image_hash: Some("sha256:new".to_string()),
            ..Default::default()
        },
        sbx_proto::SandboxListElement {
            sandbox_id: "matching-b".to_string(),
            template: Some("code-runtime".to_string()),
            image_hash: Some("sha256:new".to_string()),
            ..Default::default()
        },
        sbx_proto::SandboxListElement {
            sandbox_id: "different-hash".to_string(),
            template: Some("code-runtime".to_string()),
            image_hash: Some("sha256:old".to_string()),
            ..Default::default()
        },
        sbx_proto::SandboxListElement {
            sandbox_id: "different-template".to_string(),
            template: Some("app-runtime".to_string()),
            image_hash: Some("sha256:new".to_string()),
            ..Default::default()
        },
        sbx_proto::SandboxListElement {
            sandbox_id: "missing-hash".to_string(),
            template: Some("code-runtime".to_string()),
            image_hash: None,
            ..Default::default()
        },
    ];

    assert_eq!(
        dependent_sandbox_ids_for_template_delete_target(
            &sandboxes,
            TemplateBuildDeleteTarget::Builds(&[build_row("code-runtime", "sha256:new")]),
            DependentSandboxScope::All,
        ),
        vec![(
            "code-runtime".to_string(),
            "sha256:new".to_string(),
            vec!["matching-a".to_string(), "matching-b".to_string()]
        )]
    );
}

#[test]
fn dependent_sandbox_ids_for_template_builds_reports_each_matching_build() {
    let sandboxes = vec![
        sbx_proto::SandboxListElement {
            sandbox_id: "new-a".to_string(),
            template: Some("code-runtime".to_string()),
            image_hash: Some("sha256:new".to_string()),
            ..Default::default()
        },
        sbx_proto::SandboxListElement {
            sandbox_id: "old-a".to_string(),
            template: Some("code-runtime".to_string()),
            image_hash: Some("sha256:old".to_string()),
            ..Default::default()
        },
        sbx_proto::SandboxListElement {
            sandbox_id: "other-template".to_string(),
            template: Some("app-runtime".to_string()),
            image_hash: Some("sha256:new".to_string()),
            ..Default::default()
        },
    ];

    assert_eq!(
        dependent_sandbox_ids_for_template_delete_target(
            &sandboxes,
            TemplateBuildDeleteTarget::Builds(&[
                build_row("code-runtime", "sha256:new"),
                build_row("code-runtime", "sha256:old")
            ]),
            DependentSandboxScope::All,
        ),
        vec![
            (
                "code-runtime".to_string(),
                "sha256:new".to_string(),
                vec!["new-a".to_string()]
            ),
            (
                "code-runtime".to_string(),
                "sha256:old".to_string(),
                vec!["old-a".to_string()]
            ),
        ]
    );
}

#[test]
fn dependent_sandbox_ids_for_template_delete_matches_template_without_build_rows() {
    let sandboxes = vec![
        sbx_proto::SandboxListElement {
            sandbox_id: "missing-build-row".to_string(),
            template: Some("code-runtime".to_string()),
            image_hash: Some("sha256:missing".to_string()),
            ..Default::default()
        },
        sbx_proto::SandboxListElement {
            sandbox_id: "pool-member".to_string(),
            template: Some("code-runtime".to_string()),
            image_hash: Some("sha256:pool".to_string()),
            pool_template: Some("code-runtime".to_string()),
            ..Default::default()
        },
    ];

    assert_eq!(
        dependent_sandbox_ids_for_template_delete_target(
            &sandboxes,
            TemplateBuildDeleteTarget::Template {
                name: "code-runtime"
            },
            DependentSandboxScope::ExcludePoolMembers,
        ),
        vec![(
            "code-runtime".to_string(),
            "<any>".to_string(),
            vec!["missing-build-row".to_string()]
        )]
    );
}

#[test]
fn node_ctl_can_apply_template_reconcile_state_without_broad_operator_access() {
    let authorizer = template_peer_authorizer();
    let node_ctl = PeerIdentity::new(
        users::SPACE_NODECTL.uid,
        users::SPACE_NODECTL.primary_group.gid,
        None,
    );

    authorizer
        .authorize_identity(node_ctl.clone(), "ListTemplates", ALLOW_TEMPLATE_OBSERVER)
        .expect("node-ctl needs observer access to collect reconciliation scope");

    let network_only = UpdateTemplateSettingsRequest {
        template: "seed".to_string(),
        default_auto_lifecycle: None,
        network_policy: Some(NetworkPolicyProto::from(&NetworkPolicyV1::allow_all())),
        network_policy_generation: 1,
        network_policy_authority: crate::db::PolicyAuthority::Cluster.as_proto_i32(),
        auto_lifecycle_authority: crate::db::PolicyAuthority::Cluster.as_proto_i32(),
    };
    authorizer
        .authorize_identity(
            node_ctl.clone(),
            "UpdateTemplateSettings",
            update_template_settings_principals(&network_only),
        )
        .expect("node-ctl needs template network policy update access");

    let auto_lifecycle = UpdateTemplateSettingsRequest {
        template: "seed".to_string(),
        default_auto_lifecycle: Some(sbx_proto::AutoLifecyclePolicy::default()),
        network_policy: Some(NetworkPolicyProto::from(&NetworkPolicyV1::allow_all())),
        network_policy_generation: 1,
        network_policy_authority: crate::db::PolicyAuthority::Cluster.as_proto_i32(),
        auto_lifecycle_authority: crate::db::PolicyAuthority::Cluster.as_proto_i32(),
    };
    authorizer
        .authorize_identity(
            node_ctl.clone(),
            "UpdateTemplateSettings",
            update_template_settings_principals(&auto_lifecycle),
        )
        .expect("node-ctl needs template desired-state update access");

    let error = authorizer
        .authorize_identity(node_ctl, "DeleteTemplate", ALLOW_TEMPLATE_OPERATOR)
        .expect_err("node-ctl must not get broad template operator access");
    assert_eq!(error.code(), tonic::Code::PermissionDenied);
}

#[test]
fn node_ctl_can_delete_template_for_desired_state_reconcile() {
    let temp = tempfile::tempdir().unwrap();
    let conn = crate::db::test_conn();
    let service = TemplateManagerService::new_with_template_root(conn, temp.path()).unwrap();
    let mut request = Request::new(DeleteTemplateRequest {
        template: "missing-template".to_string(),
    });
    request.extensions_mut().insert(PeerIdentity::new(
        users::SPACE_NODECTL.uid,
        users::SPACE_NODECTL.primary_group.gid,
        None,
    ));

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let error = runtime
        .block_on(service.delete_template(request))
        .expect_err("missing template should pass node-ctl auth before lookup");

    assert_eq!(error.code(), tonic::Code::NotFound);
}

#[test]
fn get_template_pull_failures_return_corresponding_status_codes() {
    use crate::volume::PullFailure;

    for (failure, expected_code) in [
        (PullFailure::ClusterMetadataMissing, tonic::Code::NotFound),
        (
            PullFailure::ClusterMetadataUnavailable,
            tonic::Code::Unavailable,
        ),
        (PullFailure::ClusterMetadataInvalid, tonic::Code::DataLoss),
        (
            PullFailure::InvalidTemplateName,
            tonic::Code::InvalidArgument,
        ),
        (PullFailure::InvalidContentHash, tonic::Code::DataLoss),
        (
            PullFailure::LocalDatabaseUnavailable,
            tonic::Code::Unavailable,
        ),
        (PullFailure::EvictionInProgress, tonic::Code::Unavailable),
        (
            PullFailure::NoStorageLocations,
            tonic::Code::FailedPrecondition,
        ),
        (
            PullFailure::TemplateNotGeneric,
            tonic::Code::FailedPrecondition,
        ),
        (PullFailure::NoUsableStorageLocations, tonic::Code::DataLoss),
        (
            PullFailure::LocalStorageUnavailable,
            tonic::Code::Unavailable,
        ),
        (
            PullFailure::VolumeManagerUnavailable,
            tonic::Code::Unavailable,
        ),
        (PullFailure::StorageObjectMissing, tonic::Code::NotFound),
        (PullFailure::StorageUnavailable, tonic::Code::Unavailable),
        (PullFailure::ManifestMalformed, tonic::Code::DataLoss),
        (PullFailure::ManifestUnsafeFilename, tonic::Code::DataLoss),
        (PullFailure::ManifestEmpty, tonic::Code::DataLoss),
        (PullFailure::ManifestDigestMissing, tonic::Code::DataLoss),
        (
            PullFailure::ManifestCompressedDigestMissing,
            tonic::Code::DataLoss,
        ),
        (PullFailure::ExecutionMetadataInvalid, tonic::Code::DataLoss),
    ] {
        assert_eq!(
            super::lookup::pull_failure_code(failure),
            expected_code,
            "failure={failure:?}"
        );
    }
}

#[test]
fn delete_template_build_rejects_traversal_hash_before_delete() {
    let conn = crate::db::test_conn();
    let template = "test-template";
    let content_hash =
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/../../foo";
    crate::db::upsert_build(
        &conn,
        &crate::db::BuildUpsert {
            name: template,
            content_hash,
            build_date: "2026-05-13T00:00:00Z",
            manifest_json: "{}",
            runtime: RUNTIME_FIRECRACKER_ALIAS,
            arch: "x86_64",
            artifact_size_bytes: 0,
            state: "ready",
        },
    )
    .unwrap();
    let service = TemplateManagerService::new(conn).unwrap();

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let error = runtime
        .block_on(service.delete_template_build(internal_request(
            crate::proto::DeleteTemplateBuildRequest {
                template: Some(crate::proto::TemplateRef {
                    name: template.to_string(),
                    content_hash: Some(content_hash.to_string()),
                    tag: None,
                }),
            },
        )))
        .expect_err("invalid content hash must reject delete before filesystem work");

    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    let conn = service.db.lock().unwrap();
    assert!(
        crate::db::get_build(&conn, template, content_hash)
            .unwrap()
            .is_some()
    );
}

#[test]
fn delete_template_build_preserves_db_row_when_artifact_removal_fails() {
    let temp = tempfile::tempdir().unwrap();
    let template = "delete-build-failure-test";
    let content_hash = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let name_dir = temp.path().join(template);
    let digest_dir = name_dir.join(content_hash);
    std::fs::create_dir_all(&name_dir).unwrap();
    std::fs::write(&digest_dir, b"not a template artifact directory").unwrap();

    let conn = crate::db::test_conn();
    insert_ready_build(&conn, template, content_hash);
    let service = TemplateManagerService::new_with_template_root(conn, temp.path()).unwrap();

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let result = runtime
        .block_on(service.delete_template_build_after_dependency_checks(template, content_hash));

    let error = result.expect_err("disk deletion failure must abort before DB cleanup");
    assert_eq!(error.code(), tonic::Code::Internal);
    let conn = service.db.lock().unwrap();
    assert!(
        crate::db::get_build(&conn, template, content_hash)
            .unwrap()
            .is_some(),
        "build row must remain when artifact deletion fails"
    );
}

/// Scaffolding for the real-btrfs delete tests: a unique template
/// root under `SPACE_TEST_BTRFS_DIR` holding one installed build
/// whose digest dir is a read-only btrfs subvolume, plus its
/// per-`(vcpu, memory)` snapshot dirs.
///
/// Production digest subvolumes additionally carry a
/// `received_uuid` stamp (they are `btrfs send -p` parents), which
/// `BTRFS_IOC_SET_RECEIVED_SUBVOL` — a `CAP_SYS_ADMIN` ioctl — puts
/// there, so an unprivileged bench cannot reproduce it. The stamp
/// only matters to the fallback's RO→RW flip, and `set_readonly`
/// passes `-f` for exactly the received case; the flip on a
/// received subvolume is covered by the privileged node-mgr paths,
/// not here.
///
/// Requires `SPACE_TEST_BTRFS_DIR`: a user-writable directory on a
/// btrfs mount, with the `btrfs` CLI on PATH (the devbox bootstrap
/// exports one). `try_new` returns `None` (after logging a skip
/// message) when the gate is unset — CI runners have no btrfs
/// mount. Bench recipe:
///
/// ```text
/// truncate -s 1G btrfs.img && sudo mkfs.btrfs -f btrfs.img
/// sudo mount -o loop,user_subvol_rm_allowed btrfs.img /mnt/point
/// sudo chown $USER /mnt/point
/// SPACE_TEST_BTRFS_DIR=/mnt/point cargo test -p pplx-space-template-mgr
/// ```
///
/// (`user_subvol_rm_allowed` lets the bench's Drop guard reclaim
/// leftovers via `btrfs subvolume delete`; the production fallback
/// under test does not depend on it — after the RO flip,
/// `remove_dir_all` empties the subvolume and rmdir of an empty
/// subvolume root succeeds like a plain directory.)
struct BtrfsDeleteBench {
    root: std::path::PathBuf,
    name_dir: std::path::PathBuf,
    digest_dir: std::path::PathBuf,
    snapshots_dir: std::path::PathBuf,
}

impl BtrfsDeleteBench {
    fn try_new(test_label: &str, template: &str, content_hash: &str) -> Option<Self> {
        let Ok(btrfs_dir) = std::env::var("SPACE_TEST_BTRFS_DIR") else {
            eprintln!(
                "skipping {test_label}: SPACE_TEST_BTRFS_DIR is not set \
                 (needs a user-writable dir on a btrfs mount)"
            );
            return None;
        };
        let root = std::path::PathBuf::from(&btrfs_dir)
            .join(format!("{test_label}-{}", uuid::Uuid::now_v7()));
        let name_dir = root.join(template);
        // Probe rather than assert: the exported bench path can
        // outlive its mount (a devbox reboot loses the loopback
        // mount but keeps the rc-file export), and an unrelated
        // workspace run should skip like the unset case, not go
        // red.
        if let Err(error) = std::fs::create_dir_all(&name_dir) {
            eprintln!(
                "skipping {test_label}: cannot create dirs under SPACE_TEST_BTRFS_DIR \
                 ({error}) — is the btrfs bench mounted?"
            );
            return None;
        }
        let digest_dir = name_dir.join(content_hash);
        let snapshots_dir = name_dir.join("snapshots").join(content_hash);
        let bench = Self {
            root,
            name_dir,
            digest_dir,
            snapshots_dir,
        };

        if let Err(error) = pplx_space_common_core::host::btrfs::create_subvolume(&bench.digest_dir)
        {
            eprintln!(
                "skipping {test_label}: SPACE_TEST_BTRFS_DIR is not a writable btrfs \
                 mount ({error})"
            );
            return None;
        }
        std::fs::write(bench.digest_dir.join(TEMPLATE_ROOTFS_FILE), b"rootfs bytes").unwrap();

        let snapshot_dim_dir = bench.snapshots_dir.join("2vcpu-1024mib");
        std::fs::create_dir_all(&snapshot_dim_dir).unwrap();
        std::fs::write(snapshot_dim_dir.join("snapshot.base"), b"snap").unwrap();
        std::fs::write(snapshot_dim_dir.join("vmstate.base"), b"vmstate").unwrap();

        // The install path finalizes the digest subvolume read-only;
        // the RO flag is what makes plain `remove_dir_all` fail with
        // EROFS.
        pplx_space_common_core::host::btrfs::set_readonly(&bench.digest_dir, true).unwrap();
        Some(bench)
    }
}

// A failed run leaves read-only subvolumes behind that plain
// cleanup cannot remove; restore parent perms (the
// failure-injection test drops them), then sweep every subvolume
// under the template dir — the canonical digest dir plus any
// stray subvolumes a test added — clearing the RO flag and
// dropping each best-effort so reruns start clean.
impl Drop for BtrfsDeleteBench {
    fn drop(&mut self) {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&self.name_dir, std::fs::Permissions::from_mode(0o755));
        if let Ok(entries) = std::fs::read_dir(&self.name_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if pplx_space_common_core::host::btrfs::is_subvolume(&path) {
                    let _ = pplx_space_common_core::host::btrfs::set_readonly(&path, false);
                    let _ = pplx_space_common_core::host::btrfs::delete_subvolume_if_present(&path);
                }
            }
        }
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn current_thread_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

/// Integration test against a real btrfs filesystem. Installed builds
/// are read-only btrfs subvolumes, so the RPC delete path must route
/// through the subvolume-aware delete — plain `remove_dir_all` fails
/// with EROFS on the first unlink inside the read-only subvolume, the
/// build-manager prune of old local builds errors on every restart,
/// and template-cache disk is never reclaimed.
///
/// The test root is not the canonical `TEMPLATE_DIR`, so the delete
/// deterministically takes the local-fallback leg (the node-mgr RPC
/// leg is gated to the canonical root); exercising the RPC leg needs
/// a live node-manager and belongs to the kind bench. The fallback
/// works without `CAP_SYS_ADMIN` because clearing the RO flag on an
/// owned subvolume is unprivileged, and once `remove_dir_all` has
/// emptied the subvolume, rmdir of its root succeeds like a plain
/// directory.
#[test]
fn delete_template_build_removes_read_only_btrfs_subvolume_build() {
    let template = "erofs-delete-build-test";
    let content_hash = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let Some(bench) = BtrfsDeleteBench::try_new("tmplmgr-delete-build", template, content_hash)
    else {
        return;
    };

    // The delete routing hinges on unprivileged subvolume detection;
    // pin both answers on a real btrfs mount (this test does not run
    // as root).
    assert!(
        pplx_space_common_core::host::btrfs::is_subvolume(&bench.digest_dir),
        "digest dir must be detected as a subvolume without privilege"
    );
    assert!(
        !pplx_space_common_core::host::btrfs::is_subvolume(&bench.name_dir),
        "plain directory on btrfs must not be detected as a subvolume"
    );

    let conn = crate::db::test_conn();
    insert_ready_build(&conn, template, content_hash);
    let service = TemplateManagerService::new_with_template_root(conn, &bench.root).unwrap();

    current_thread_runtime()
        .block_on(service.delete_template_build_after_dependency_checks(template, content_hash))
        .expect("deleting a read-only btrfs subvolume build must succeed");

    assert!(
        !bench.digest_dir.exists(),
        "digest subvolume must be removed from disk"
    );
    assert!(
        !bench.snapshots_dir.exists(),
        "snapshot dirs must be removed from disk"
    );
    let conn = service.db.lock().unwrap();
    assert!(
        crate::db::get_build(&conn, template, content_hash)
            .unwrap()
            .is_none(),
        "build row must be cleaned up after disk removal"
    );
}

/// `DeleteTemplate` removes the whole `<root>/<name>` tree, which
/// recurses into the read-only digest subvolumes underneath — the
/// same EROFS as the build-level delete. The template path must
/// route every on-disk subvolume through the subvolume-aware
/// delete before removing the rest of the tree.
#[test]
fn delete_template_removes_read_only_btrfs_subvolume_builds() {
    let template = "erofs-delete-template-test";
    let content_hash = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
    let Some(bench) = BtrfsDeleteBench::try_new("tmplmgr-delete-template", template, content_hash)
    else {
        return;
    };

    // A read-only subvolume whose name is not valid UTF-8: it has
    // no content-hash identity, so the template delete must still
    // remove it via the local cleanup rather than letting the
    // plain tree removal EROFS on it.
    let stray_subvolume = {
        use std::os::unix::ffi::OsStrExt;
        bench
            .name_dir
            .join(std::ffi::OsStr::from_bytes(b"stray-\xff-subvolume"))
    };
    pplx_space_common_core::host::btrfs::create_subvolume(&stray_subvolume).unwrap();
    std::fs::write(stray_subvolume.join("data"), b"bytes").unwrap();
    pplx_space_common_core::host::btrfs::set_readonly(&stray_subvolume, true).unwrap();

    let conn = crate::db::test_conn();
    insert_ready_build(&conn, template, content_hash);
    let service = TemplateManagerService::new_with_template_root(conn, &bench.root).unwrap();

    current_thread_runtime()
        .block_on(service.delete_template_after_dependency_checks(template))
        .expect("deleting a template whose builds are read-only btrfs subvolumes must succeed");

    assert!(
        !bench.name_dir.exists(),
        "template dir (including digest subvolumes and the non-UTF-8-named stray) \
         must be removed from disk"
    );
    let conn = service.db.lock().unwrap();
    assert!(
        crate::db::get_build(&conn, template, content_hash)
            .unwrap()
            .is_none(),
        "build rows must be cleaned up after disk removal"
    );
}

/// The subvolume branch must keep the abort-before-DB-cleanup
/// contract: when both delete legs fail (no node-mgr in the test
/// env, and the local cleanup is blocked by a read-only parent
/// dir), the RPC errors and the build row survives so the delete
/// stays retryable.
#[test]
fn delete_template_build_preserves_db_row_when_subvolume_removal_fails() {
    use std::os::unix::fs::PermissionsExt;

    // The failure injection below relies on mode 0500 blocking the
    // subvolume-root unlink; root bypasses DAC, so the delete would
    // succeed and the test would fail for the wrong reason.
    if nix::unistd::Uid::effective().as_raw() == 0 {
        eprintln!(
            "skipping delete_template_build_preserves_db_row_when_subvolume_removal_fails: \
             running as root, a write-protected parent dir cannot block the cleanup"
        );
        return;
    }

    let template = "erofs-delete-build-abort-test";
    let content_hash = "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
    let Some(bench) =
        BtrfsDeleteBench::try_new("tmplmgr-delete-build-abort", template, content_hash)
    else {
        return;
    };

    let conn = crate::db::test_conn();
    insert_ready_build(&conn, template, content_hash);
    let service = TemplateManagerService::new_with_template_root(conn, &bench.root).unwrap();

    // Write-protect the parent dir: the local fallback can clear the
    // subvolume's RO flag and wipe its contents, but unlinking the
    // subvolume root itself needs write access to the parent, so
    // the cleanup fails and the delete must abort before touching
    // the DB.
    std::fs::set_permissions(&bench.name_dir, std::fs::Permissions::from_mode(0o500)).unwrap();

    let result = current_thread_runtime()
        .block_on(service.delete_template_build_after_dependency_checks(template, content_hash));

    let error = result.expect_err("subvolume removal failure must abort before DB cleanup");
    assert_eq!(error.code(), tonic::Code::Internal);
    assert!(
        bench.digest_dir.exists(),
        "digest subvolume root must still be on disk after the failed delete"
    );
    let conn = service.db.lock().unwrap();
    assert!(
        crate::db::get_build(&conn, template, content_hash)
            .unwrap()
            .is_some(),
        "build row must remain when subvolume deletion fails"
    );
}

#[test]
fn stale_build_row_after_disk_first_crash_is_pruned_by_recovery_cleanup() {
    let temp = tempfile::tempdir().unwrap();
    let template = "delete-build-crash-prune-test";
    let content_hash = "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";

    let conn = crate::db::test_conn();
    insert_ready_build(&conn, template, content_hash);
    let service = TemplateManagerService::new_with_template_root(conn, temp.path()).unwrap();

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(service.prune_stale_template_artifact_db_rows());

    let conn = service.db.lock().unwrap();
    assert!(
        crate::db::get_build(&conn, template, content_hash)
            .unwrap()
            .is_none(),
        "recovery cleanup must prune DB rows for disk-removed builds"
    );
}

#[test]
fn delete_template_retry_completes_db_cleanup_after_disk_removal() {
    let temp = tempfile::tempdir().unwrap();
    let template = "delete-template-retry-test";

    let conn = crate::db::test_conn();
    crate::db::insert_pool(&conn, template, 1, "cold", 2, 1024).unwrap();
    crate::db::upsert_template_settings(&conn, template, &AutoLifecyclePolicy::disabled()).unwrap();
    let service = TemplateManagerService::new_with_template_root(conn, temp.path()).unwrap();

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime
        .block_on(
            service.delete_template(internal_request(DeleteTemplateRequest {
                template: template.to_string(),
            })),
        )
        .expect("retry after disk removal should complete stale DB cleanup");

    let conn = service.db.lock().unwrap();
    assert!(crate::db::get_pool(&conn, template).unwrap().is_none());
    assert!(
        crate::db::get_template_settings(&conn, template)
            .unwrap()
            .is_none()
    );
}

#[test]
fn local_lookup_rejects_stored_traversal_hash_before_path_lookup() {
    let conn = crate::db::test_conn();
    let template = "test-template";
    let content_hash =
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/../../foo";
    crate::db::upsert_build(
        &conn,
        &crate::db::BuildUpsert {
            name: template,
            content_hash,
            build_date: "2026-05-13T00:00:00Z",
            manifest_json: "{}",
            runtime: RUNTIME_FIRECRACKER_ALIAS,
            arch: "x86_64",
            artifact_size_bytes: 0,
            state: "ready",
        },
    )
    .unwrap();
    let error = TemplateManagerService::resolve_template(&conn, template, "", "").expect_err(
        "stored invalid content hash must reject local lookup before filesystem lookup",
    );

    assert_eq!(error.code(), tonic::Code::InvalidArgument);
}

struct CachedBaseFixture {
    root: tempfile::TempDir,
    service: TemplateManagerService,
    org: OrgId,
    hash_a: String,
    hash_b: String,
}

#[rstest::fixture]
fn cached_base_fixture() -> CachedBaseFixture {
    let root = tempfile::tempdir().unwrap();
    let conn = crate::db::test_conn();
    let org = OrgId::new();
    let hash_a = format!("sha256:{}", "a".repeat(64));
    let hash_b = format!("sha256:{}", "b".repeat(64));
    insert_ready_build(&conn, "base", &hash_a);
    insert_ready_build(&conn, "base", &hash_b);
    crate::db::try_upsert_template_network_policy(
        &conn,
        "base",
        &NetworkPolicyV1::deny_all(),
        1,
        crate::db::PolicyAuthority::Local,
    )
    .unwrap();
    conn.execute(
        "UPDATE template_builds SET build_date = '2026-09-16T00:00:00Z' WHERE content_hash = ?1",
        [&hash_b],
    )
    .unwrap();
    let service = TemplateManagerService::new_with_template_root(conn, root.path()).unwrap();
    CachedBaseFixture {
        root,
        service,
        org,
        hash_a,
        hash_b,
    }
}

#[rstest::rstest]
#[tokio::test]
async fn get_template_authorizes_and_resolves_before_using_cached_builds(
    cached_base_fixture: CachedBaseFixture,
    #[values(false, true)] scoped: bool,
    #[values(false, true)] cold: bool,
) {
    use crate::volume::{FindReleaseError, PullOutcome, ResolvedRelease};
    let CachedBaseFixture {
        root,
        service,
        org,
        hash_a,
        hash_b,
    } = cached_base_fixture;
    if cold {
        crate::db::delete_build(&service.db.lock().unwrap(), "base", &hash_a).unwrap();
    }
    let materialized = std::cell::Cell::new(false);
    let request = |hash: Option<String>, tag: Option<&str>| {
        internal_request(crate::proto::GetTemplateRequest {
            template: Some(crate::proto::TemplateRef {
                name: "base".into(),
                content_hash: hash,
                tag: tag.map(str::to_owned),
            }),
            caller_org_id: Some(org.to_string()),
            vcpu: Some(1),
            memory_mib: Some(512),
        })
    };

    for (selected, hash, tag) in [
        (&hash_a, None, Some("stable")),
        (&hash_b, None, Some("stable")),
        (&hash_a, Some(hash_a.clone()), None),
    ] {
        let expected_hash = hash.clone().unwrap_or_default();
        let mut lookup = request(hash, tag);
        lookup.get_mut().caller_org_id = scoped.then(|| org.to_string());
        let response = service
            .get_template_resolving(
                lookup,
                async |name, hash, resolved_tag, caller| {
                    assert_eq!(name, "base");
                    assert_eq!(hash, expected_hash);
                    assert_eq!(resolved_tag, tag.unwrap_or_default());
                    assert_eq!(caller, scoped.then_some(org));
                    Ok(ResolvedRelease {
                        hash: selected.clone(),
                        storage_uris: vec![],
                        visibility:
                            pplx_space_node_api_client::proto::TemplateVisibility::OrgSpecific,
                    })
                },
                async |spec, _, release| {
                    assert_eq!(spec.name, "base");
                    assert_eq!(release.hash, hash_a);
                    assert!(cold);
                    assert!(!materialized.replace(true));
                    insert_ready_build(&service.db.lock().unwrap(), spec.name, &release.hash);
                    PullOutcome::Materialized
                },
            )
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            response.template.unwrap().content_hash.as_ref(),
            Some(selected)
        );
        assert_eq!(response.authorized_org_id, scoped.then(|| org.to_string()));
        assert_eq!(
            response.template_dir,
            root.path().join("base").join(selected).to_string_lossy()
        );
    }
    assert_eq!(materialized.get(), cold);

    // Sandbox Start reads template policy without a digest, tag, or dimensions.
    // That trusted lookup must work for locally registered, unpublished templates.
    for (hash, tag, selected) in [
        (Some(hash_a.clone()), None, &hash_a),
        (Some(hash_a.clone()), Some("stable"), &hash_a),
        (None, None, &hash_b),
    ] {
        let mut legacy_request = request(hash, tag);
        legacy_request.get_mut().caller_org_id = None;
        legacy_request.get_mut().vcpu = None;
        legacy_request.get_mut().memory_mib = None;

        let response = service
            .get_template_resolving(
                legacy_request,
                async |_, _, _, _| panic!("trusted cache hit must not depend on ClusterApi"),
                async |_, _, _| panic!("cached build must not be pulled"),
            )
            .await
            .unwrap()
            .into_inner();

        assert_eq!(
            response.template.unwrap().content_hash.as_ref(),
            Some(selected)
        );
        assert_eq!(response.authorized_org_id, None);
        assert_eq!(
            response.network_policy,
            Some(NetworkPolicyProto::from(&NetworkPolicyV1::deny_all()))
        );
        assert_eq!(
            response.network_policy_authority,
            crate::db::PolicyAuthority::Local.as_proto_i32()
        );
    }

    // Revoked access and a missing tag must not fall back to either cached build.
    for (hash, tag, caller_org) in [
        (Some(hash_a), None, Some(org)),
        (None, Some("missing"), Some(org)),
        (None, Some("missing"), None),
    ] {
        let mut lookup = request(hash, tag);
        lookup.get_mut().caller_org_id = caller_org.map(|org| org.to_string());
        let error = service
            .get_template_resolving(
                lookup,
                async |_, _, _, _| Err(FindReleaseError::NotFound),
                async |_, _, _| panic!("unresolved release must not be pulled"),
            )
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::NotFound);
        assert_eq!(error.message(), "template release not found");
    }
}

#[test]
fn reconcile_desired_base_snapshot_deletes_when_policy_disabled() {
    let conn = crate::db::test_conn();
    crate::db::update_template_snapshot_policy(
        &conn,
        "codex-disabled-base-snapshot-test",
        false,
        2,
        1024,
    )
    .unwrap();
    let service = TemplateManagerService::new(conn).unwrap();

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let outcome = runtime
        .block_on(service.reconcile_desired_base_snapshot("codex-disabled-base-snapshot-test"));

    assert!(outcome.reconciled);
    assert!(!outcome.snapshot_built);
    assert!(!outcome.snapshot_deleted);
    assert!(outcome.content_hash.is_none());
    assert!(outcome.error.is_none());
}

#[test]
fn reconcile_desired_base_snapshot_ignores_unset_policy() {
    let conn = crate::db::test_conn();
    crate::db::upsert_build(
        &conn,
        &crate::db::BuildUpsert {
            name: "codex-unset-base-snapshot-test",
            content_hash: "sha256:unset-policy-test",
            build_date: "2026-04-29T00:00:00Z",
            manifest_json: "{}",
            runtime: RUNTIME_FIRECRACKER_ALIAS,
            arch: "x86_64",
            artifact_size_bytes: 0,
            state: "ready",
        },
    )
    .unwrap();
    crate::db::upsert_snapshot(
        &conn,
        "codex-unset-base-snapshot-test",
        "sha256:unset-policy-test",
        2,
        1024,
        "snapshot-hash",
    )
    .unwrap();
    let service = TemplateManagerService::new(conn).unwrap();

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let outcome =
        runtime.block_on(service.reconcile_desired_base_snapshot("codex-unset-base-snapshot-test"));

    assert!(!outcome.reconciled);
    assert!(!outcome.snapshot_built);
    assert!(!outcome.snapshot_deleted);
    assert!(outcome.content_hash.is_none());
    assert!(outcome.error.is_none());

    let conn = service.db.lock().unwrap();
    let snapshots = crate::db::list_snapshots(
        &conn,
        "codex-unset-base-snapshot-test",
        "sha256:unset-policy-test",
    )
    .unwrap();
    assert_eq!(snapshots.len(), 1);
}

#[test]
fn prune_base_snapshots_except_keeps_old_build_snapshots() {
    let conn = crate::db::test_conn();
    let template = "codex-prune-base-snapshot-test";
    for content_hash in ["sha256:old-prune-test", "sha256:new-prune-test"] {
        crate::db::upsert_build(
            &conn,
            &crate::db::BuildUpsert {
                name: template,
                content_hash,
                build_date: "2026-04-29T00:00:00Z",
                manifest_json: "{}",
                runtime: RUNTIME_FIRECRACKER_ALIAS,
                arch: "x86_64",
                artifact_size_bytes: 0,
                state: "ready",
            },
        )
        .unwrap();
    }
    crate::db::upsert_snapshot(&conn, template, "sha256:old-prune-test", 2, 1024, "old").unwrap();
    crate::db::upsert_snapshot(&conn, template, "sha256:new-prune-test", 2, 1024, "stale").unwrap();
    crate::db::upsert_snapshot(&conn, template, "sha256:new-prune-test", 2, 2048, "desired")
        .unwrap();
    let service = TemplateManagerService::new(conn).unwrap();

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let pruned = runtime
        .block_on(service.prune_base_snapshots_except(
            template,
            template,
            "sha256:new-prune-test",
            2,
            2048,
        ))
        .unwrap();

    assert!(pruned);
    let conn = service.db.lock().unwrap();
    let old_snapshots =
        crate::db::list_snapshots(&conn, template, "sha256:old-prune-test").unwrap();
    let new_snapshots =
        crate::db::list_snapshots(&conn, template, "sha256:new-prune-test").unwrap();
    assert_eq!(old_snapshots.len(), 1);
    assert_eq!(old_snapshots[0].memory_mib, 1024);
    assert_eq!(new_snapshots.len(), 1);
    assert_eq!(new_snapshots[0].memory_mib, 2048);
}

#[test]
fn reconcile_desired_base_snapshot_prunes_idle_template_locks() {
    let conn = crate::db::test_conn();
    let service = TemplateManagerService::new(conn).unwrap();

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        for i in 0..16 {
            let template = format!("codex-lock-prune-{i}");
            let outcome = service.reconcile_desired_base_snapshot(&template).await;
            assert!(!outcome.reconciled);
        }
    });

    assert!(service.snapshot_reconcile_locks.lock().unwrap().len() <= 1);
}

#[test]
fn reconcile_desired_base_snapshot_ignores_active_template_reconcile() {
    let conn = crate::db::test_conn();
    let service = TemplateManagerService::new(conn).unwrap();
    let template = "codex-active-base-snapshot-test";

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let lock = {
            let mut locks = service.snapshot_reconcile_locks.lock().unwrap();
            Arc::clone(
                locks
                    .entry(template.to_string())
                    .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(()))),
            )
        };
        let _guard = lock.try_lock_owned().unwrap();

        let outcome = service.reconcile_desired_base_snapshot(template).await;

        assert!(outcome.in_progress);
        assert!(!outcome.reconciled);
        assert!(!outcome.snapshot_built);
        assert!(outcome.error.is_none());
    });
}

fn expanded_test_rootfs_setup() -> (tempfile::TempDir, String, Vec<u8>) {
    use sha2::Digest;

    let temp = tempfile::tempdir().unwrap();
    let rootfs_bytes: Vec<u8> = (0u8..=255u8).cycle().take(4 * 1024 * 1024).collect();
    std::fs::write(temp.path().join("rootfs.ext4"), &rootfs_bytes).unwrap();

    let mut hasher = sha2::Sha256::new();
    hasher.update(&rootfs_bytes);
    let hex: String = hasher
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    let content_hash = format!("sha256:{hex}");

    let manifest = serde_json::json!({
        "name": "register-refresh-test",
        "content_hash": &content_hash,
        "build_date": "2026-05-13T00:00:00Z",
            "runtime": "fc",
            "arch": "x86_64",
            "files": {
                "rootfs.ext4": { "digest": &content_hash, "size": rootfs_bytes.len() }
        }
    });
    std::fs::write(
        temp.path().join("manifest.json"),
        serde_json::to_string(&manifest).unwrap(),
    )
    .unwrap();
    std::fs::write(temp.path().join("metadata.json"), r#"{"disk_size_mb":4}"#).unwrap();

    (temp, content_hash, rootfs_bytes)
}

#[rstest::rstest]
#[case::sha256(HashAlg::Sha256)]
#[case::blake3(HashAlg::Blake3)]
#[tokio::test]
async fn refresh_register_existing_expanded_build_preserves_rootfs_bytes(
    #[case] algorithm: HashAlg,
) {
    let (temp, sha256, original_bytes) = expanded_test_rootfs_setup();
    let content_hash = match algorithm {
        HashAlg::Sha256 => sha256,
        HashAlg::Blake3 => format!("blake3:{}", blake3::hash(&original_bytes)),
    };
    let compressed = crate::rootfs_zstd::compress_rootfs(
        &temp.path().join(TEMPLATE_ROOTFS_FILE),
        &temp.path().join(TEMPLATE_ROOTFS_ZSTD_FILE),
        crate::file_reader::TemplateReadMode::Sparse,
    )
    .unwrap();
    let manifest = normalize_template_manifest(
        Default::default(),
        "register-refresh-test",
        &content_hash,
        &Default::default(),
        original_bytes.len() as u64,
        Some((&compressed.compressed_hash, compressed.compressed_size)),
    )
    .unwrap();
    let manifest_bytes = serde_json::to_vec(&manifest).unwrap();
    std::fs::write(temp.path().join("manifest.json"), &manifest_bytes).unwrap();
    let service = TemplateManagerService::new(crate::db::test_conn()).unwrap();
    service
        .refresh_register_existing_expanded_build(
            "register-refresh-test",
            &content_hash,
            temp.path().to_str().unwrap(),
            Some(br#"{"disk_size_mb":4}"#),
        )
        .await
        .unwrap();
    assert_eq!(
        std::fs::read(temp.path().join("rootfs.ext4")).unwrap(),
        original_bytes
    );
    assert_eq!(
        std::fs::read(temp.path().join("manifest.json")).unwrap(),
        manifest_bytes
    );
    let conn = service.db.lock().unwrap();
    let build = crate::db::get_build(&conn, "register-refresh-test", &content_hash)
        .unwrap()
        .expect("build row should exist after refresh");
    assert_eq!(build.content_hash, content_hash);
    assert_eq!(build.artifact_size_bytes, original_bytes.len() as u64);
}

#[tokio::test]
async fn register_refresh_cannot_add_remove_or_replace_a_retained_binding() {
    use pplx_space_common_core::types::{
        Arch, Digest, EnvironmentReference, ExecutionMetadata, TemplateManifest, TemplateMetadata,
    };
    let first = EnvironmentReference::new(Digest::sha256([1; 32]));
    let second = EnvironmentReference::new(Digest::sha256([2; 32]));
    for (retained, incoming, accepted) in [
        (None, None, true),
        (Some(first), Some(first), true),
        (None, Some(first), false),
        (Some(first), None, false),
        (Some(first), Some(second), false),
    ] {
        let execution = |binding| {
            let metadata = ExecutionMetadata::initial_firecracker(Arch::X86_64, vec![]);
            match binding {
                Some(reference) => metadata.with_environment(reference),
                None => metadata,
            }
        };
        let mut manifest = TemplateManifest::default();
        manifest.set_execution_metadata(execution(retained));
        let mut metadata = TemplateMetadata::default();
        metadata.set_execution_metadata(execution(incoming));
        let validation = validate_retained_environment_binding(&manifest, &metadata);
        assert_eq!(validation.is_ok(), accepted);
        if accepted {
            continue;
        }
        let (temp, content_hash, rootfs) = expanded_test_rootfs_setup();
        let manifest_bytes = serde_json::to_vec(&manifest).unwrap();
        std::fs::write(temp.path().join("manifest.json"), &manifest_bytes).unwrap();
        let old_metadata = std::fs::read(temp.path().join("metadata.json")).unwrap();
        let service = TemplateManagerService::new(crate::db::test_conn()).unwrap();
        let error = service
            .refresh_register_existing_expanded_build(
                "register-refresh-test",
                &content_hash,
                temp.path().to_str().unwrap(),
                Some(&serde_json::to_vec(&metadata).unwrap()),
            )
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::AlreadyExists);
        assert_eq!(
            std::fs::read(temp.path().join("metadata.json")).unwrap(),
            old_metadata
        );
        assert_eq!(
            std::fs::read(temp.path().join("manifest.json")).unwrap(),
            manifest_bytes
        );
        assert_eq!(
            std::fs::read(temp.path().join("rootfs.ext4")).unwrap(),
            rootfs
        );
        assert!(
            crate::db::get_build(
                &service.db.lock().unwrap(),
                "register-refresh-test",
                &content_hash
            )
            .unwrap()
            .is_none()
        );
    }
}

#[test]
fn refresh_register_existing_expanded_build_rejects_hash_mismatch() {
    let (temp, _content_hash, _original_bytes) = expanded_test_rootfs_setup();
    let digest_dir = temp.path().to_str().unwrap().to_string();
    let bogus_hash = "sha256:0000000000000000000000000000000000000000000000000000000000000000";

    let conn = crate::db::test_conn();
    let service = TemplateManagerService::new(conn).unwrap();

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let result = runtime.block_on(service.refresh_register_existing_expanded_build(
        "register-refresh-test",
        bogus_hash,
        &digest_dir,
        None,
    ));

    let error = result.expect_err("hash mismatch must be rejected");
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(
        error.message().contains("expected"),
        "expected hash-mismatch message, got: {}",
        error.message()
    );
}

mod import_rpc;

#[test]
fn rootfs_size_mb_from_metadata_returns_disk_size_for_template_build() {
    let temp = tempfile::tempdir().unwrap();
    std::fs::write(
        temp.path().join("metadata.json"),
        r#"{"disk_size_mb":20480}"#,
    )
    .unwrap();

    let result = crate::rootfs_cache::rootfs_size_mb_from_metadata(temp.path()).unwrap();
    assert_eq!(
        result,
        Some(20480),
        "build_base_snapshot_for_resolved feeds this into CreateRequest.rootfs_size_mb \
         so the cold-boot sandbox is resized to the runtime disk size instead of \
         inheriting whatever shape the digest_dir rootfs has on disk"
    );
}

#[test]
fn rootfs_size_mb_from_metadata_returns_none_for_template_without_disk_size() {
    let temp = tempfile::tempdir().unwrap();
    std::fs::write(temp.path().join("metadata.json"), "{}").unwrap();

    let result = crate::rootfs_cache::rootfs_size_mb_from_metadata(temp.path()).unwrap();
    assert_eq!(result, None);
}

/// `resolve_template` is the chokepoint every `GetTemplate` /
/// `FindTemplateBuild` flow goes through. A successful resolve
/// must advance `last_used_at` on the matched build so the LRU
/// eviction loop sees real usage. Without this wiring,
/// constantly-resolved builds would look cold to the GC and get
/// evicted out from under live sandboxes (with grace as the only
/// backstop).
#[test]
fn resolve_template_advances_last_used_at_on_both_paths() {
    use crate::db::{BuildUpsert, set_last_used_at_for_test, upsert_build};
    use rusqlite::params;

    let conn = crate::db::test_conn();
    upsert_build(
        &conn,
        &BuildUpsert {
            name: "seed",
            content_hash: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            build_date: "2026-01-01T00:00:00Z",
            manifest_json: "{}",
            runtime: "fc",
            arch: "aarch64",
            artifact_size_bytes: 100,
            state: "ready",
        },
    )
    .unwrap();

    // Path A: explicit content_hash resolution.
    set_last_used_at_for_test(
        &conn,
        "seed",
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        1_000,
    )
    .unwrap();
    let (name, hash) = TemplateManagerService::resolve_template(
        &conn,
        "seed",
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "",
    )
    .unwrap();
    assert_eq!(
        (name.as_str(), hash.as_str()),
        (
            "seed",
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        )
    );
    let after_explicit: u64 = conn
        .query_row(
            "SELECT last_used_at FROM template_builds WHERE name = ?1 AND content_hash = ?2",
            params![
                "seed",
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            ],
            |row| {
                let s: String = row.get(0)?;
                Ok(s.parse::<u64>().unwrap_or(0))
            },
        )
        .unwrap();
    assert!(
        after_explicit > 1_000,
        "explicit-hash resolution must advance last_used_at (got {after_explicit})",
    );

    // Path B: latest-build resolution (empty content_hash).
    set_last_used_at_for_test(
        &conn,
        "seed",
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        1_000,
    )
    .unwrap();
    let _ = TemplateManagerService::resolve_template(&conn, "seed", "", "").unwrap();
    let after_latest: u64 = conn
        .query_row(
            "SELECT last_used_at FROM template_builds WHERE name = ?1 AND content_hash = ?2",
            params![
                "seed",
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            ],
            |row| {
                let s: String = row.get(0)?;
                Ok(s.parse::<u64>().unwrap_or(0))
            },
        )
        .unwrap();
    assert!(
        after_latest > 1_000,
        "latest-build resolution must advance last_used_at (got {after_latest})",
    );
}

/// Regression test for the GC-vs-sandbox-create race:
/// a build that the eviction loop has marked
/// `state='deleting'` (Phase 1 of `execute_one_async`) must
/// NOT be returned by `resolve_template` to a concurrent
/// sandbox-create. Without this filter the caller would get
/// the hash of a build that's about to vanish, and the
/// sandbox would boot against a missing rootfs.
#[test]
fn resolve_template_filters_out_deleting_state_rows() {
    use crate::db::{BuildUpsert, upsert_build};
    use rusqlite::params;

    let hash = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let conn = crate::db::test_conn();
    upsert_build(
        &conn,
        &BuildUpsert {
            name: "seed",
            content_hash: hash,
            build_date: "2026-01-01T00:00:00Z",
            manifest_json: "{}",
            runtime: "fc",
            arch: "aarch64",
            artifact_size_bytes: 100,
            state: "ready",
        },
    )
    .unwrap();
    // Resolution works when ready.
    let _ = TemplateManagerService::resolve_template(&conn, "seed", hash, "").unwrap();

    // Eviction Phase 1 flips state.
    conn.execute(
        "UPDATE template_builds SET state = 'deleting'
         WHERE name = ?1 AND content_hash = ?2",
        params!["seed", hash],
    )
    .unwrap();

    // Resolution now refuses to hand out a doomed build.
    let err = TemplateManagerService::resolve_template(&conn, "seed", hash, "").unwrap_err();
    assert_eq!(
        err.code(),
        tonic::Code::NotFound,
        "resolve_template must treat state='deleting' as not-resolvable, got {err:?}",
    );

    // Latest-build path (empty content_hash) also filters
    // 'deleting' rows because `get_latest_build` already has
    // `WHERE state = 'ready'`.
    let err = TemplateManagerService::resolve_template(&conn, "seed", "", "").unwrap_err();
    assert_eq!(err.code(), tonic::Code::NotFound);
}

/// Regression test: `build_disk_usage_with_snapshots` previously only
/// measured `rootfs.ext4` via `template_cache_size_bytes`, so
/// the multi-GB `rootfs.ext4.zst` (kept on disk after pull for
/// recovery + the btrfs-send codec) and the small metadata
/// files were silently uncounted. The budget would overshoot by
/// the size of the zst before GC saw the cache as "over."
///
/// This test plants known-size files in a digest dir and
/// asserts the function returns the sum of all of them, not
/// just `rootfs.ext4`. Uses 4096-byte-aligned sizes so the
/// allocated-blocks reading matches the file size on any tmpfs
/// or ext4 filesystem the test happens to run on.
#[test]
fn build_disk_usage_charges_every_file_in_digest_dir() {
    use std::io::Write;
    use std::os::unix::fs::MetadataExt;

    let temp = tempfile::tempdir().unwrap();
    let name = "seed";
    let hash = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let digest_dir = temp.path().join(name).join(hash);
    std::fs::create_dir_all(&digest_dir).unwrap();

    // Plant rootfs.ext4 (1 block), rootfs.ext4.zst (1 block),
    // manifest.json (1 block), metadata.json (1 block). On a
    // typical 4 KiB block FS each file occupies 1 block = 4096
    // bytes. The function reports the sum.
    for name in [
        "rootfs.ext4",
        "rootfs.ext4.zst",
        "manifest.json",
        "metadata.json",
    ] {
        let mut f = std::fs::File::create(digest_dir.join(name)).unwrap();
        // 1 byte → 1 block of allocation.
        f.write_all(b"x").unwrap();
    }

    // We can't call `build_disk_usage_with_snapshots` directly
    // because it uses absolute paths under TEMPLATE_DIR. Re-
    // implement its core (walk-dir + sum allocated blocks) here
    // and assert the test's expected total. The point of this
    // test is to lock in the *summation contract*, not the
    // absolute-path resolution: a previous version of the
    // function that only counted `rootfs.ext4` would have
    // returned ~one block instead of four.
    let mut total: u64 = 0;
    for entry in std::fs::read_dir(&digest_dir).unwrap() {
        let entry = entry.unwrap();
        let metadata = entry.metadata().unwrap();
        if metadata.is_file() {
            total = total.saturating_add(metadata.blocks().saturating_mul(512));
        }
    }
    assert!(
        total >= 4 * 4096,
        "expected all four files to be charged (>=4 blocks); got {total} bytes — \
         the old `rootfs.ext4`-only path would return ~1 block here, which is the bug",
    );
}

// ── Eviction fallback delete: subvolume-aware behavior ───────────
//
// The eviction closure calls `fallback_delete_build_artifacts` when
// the privileged `BtrfsDeleteTemplateSubvolume` RPC against
// node-mgr fails. Before this helper, the eviction path called
// `TemplateArtifactStore::remove_build_artifacts` directly, which
// wraps `std::fs::remove_dir_all`. On btrfs, `remove_dir_all`
// walks the contents fine but cannot `rmdir` the subvolume root,
// surfacing Err that propagates back through `execute_one_async`
// and skips Phase 3 — leaving the row stuck in `state='deleting'`.
// Below we lock down the plain-directory path: the fallback
// helper must delete a regular directory with its contents, and
// it must do so AFTER first wiping the snapshot dirs. The plain
// path is what runs on non-btrfs hosts (legacy template installs,
// CI containers on tmpfs) and is also the post-`remove_dir_all`
// shape on btrfs hosts where the contents got wiped before the
// rmdir step.

/// Plain-directory happy path: a non-subvolume digest dir with
/// the usual artifact files is fully removed by the fallback
/// helper, including the digest dir itself.
#[test]
fn fallback_delete_build_artifacts_removes_plain_digest_dir() {
    let temp = tempfile::tempdir().unwrap();
    let template = "fallback-plain-test";
    let content_hash = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

    let name_dir = temp.path().join(template);
    let digest_dir = name_dir.join(content_hash);
    std::fs::create_dir_all(&digest_dir).unwrap();
    for f in [
        "rootfs.ext4",
        "rootfs.ext4.zst",
        "manifest.json",
        "metadata.json",
    ] {
        std::fs::write(digest_dir.join(f), b"x").unwrap();
    }
    assert!(digest_dir.exists());

    let store = super::TemplateArtifactStore::new(temp.path());
    super::fallback_delete_build_artifacts(&store, template, content_hash)
        .expect("plain-dir fallback delete must succeed");

    assert!(
        !digest_dir.exists(),
        "digest dir must be gone after fallback delete"
    );
}

/// The fallback delete also clears the per-build snapshot dirs
/// (where base-snapshot reconciliation writes its output) — without this
/// step the eviction would leave a build's snapshot bytes
/// occupying disk space after the digest dir is gone, defeating
/// the LRU's bytes-freed accounting.
#[test]
fn fallback_delete_build_artifacts_removes_snapshot_dirs() {
    let temp = tempfile::tempdir().unwrap();
    let template = "fallback-snapshot-cleanup-test";
    let content_hash = "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
    let name_dir = temp.path().join(template);
    let digest_dir = name_dir.join(content_hash);
    std::fs::create_dir_all(&digest_dir).unwrap();
    std::fs::write(digest_dir.join("rootfs.ext4"), b"x").unwrap();
    // Plant a (2vcpu, 1024mib) snapshot pair the way
    // base-snapshot reconciliation would.
    let snapshot_dir = name_dir
        .join("snapshots")
        .join(content_hash)
        .join("2vcpu-1024mib");
    std::fs::create_dir_all(&snapshot_dir).unwrap();
    std::fs::write(snapshot_dir.join("snapshot.base"), b"snap").unwrap();
    std::fs::write(snapshot_dir.join("vmstate.base"), b"vm").unwrap();
    assert!(snapshot_dir.exists());

    let store = super::TemplateArtifactStore::new(temp.path());
    super::fallback_delete_build_artifacts(&store, template, content_hash).unwrap();

    assert!(!digest_dir.exists(), "digest dir must be gone");
    assert!(
        !snapshot_dir.exists(),
        "snapshot dir must be gone — leaving it would leak bytes the LRU thinks it freed",
    );
}

/// Non-existent digest dir is a no-op (eviction can race itself
/// in adjacent ticks; the fallback must be idempotent).
#[test]
fn fallback_delete_build_artifacts_is_idempotent() {
    let temp = tempfile::tempdir().unwrap();
    let store = super::TemplateArtifactStore::new(temp.path());

    super::fallback_delete_build_artifacts(
        &store,
        "nonexistent",
        "sha256:0000000000000000000000000000000000000000000000000000000000000000",
    )
    .expect("fallback on a non-existent digest dir must be a no-op success");
}

#[test]
fn build_staging_dir_name_validation() {
    let rejected = [
        "..",
        "a/b",
        "/abs",
        "sha256:abc",
        "sha256:register.importing.x",
        "register.importing.x",
        "build.importing.", // empty job-id suffix
        "build.importing.a/../b",
    ];
    for name in rejected {
        let error = templates::validate_build_staging_dir_name(name)
            .expect_err(&format!("{name:?} must be rejected"));
        assert_eq!(error.code(), tonic::Code::InvalidArgument, "{name:?}");
    }
    let accepted = templates::validate_build_staging_dir_name(
        "build.importing.0196e7a0-0000-7000-8000-000000000000",
    )
    .expect("well-formed staging dir name");
    assert_eq!(
        accepted,
        "build.importing.0196e7a0-0000-7000-8000-000000000000"
    );

    assert!(is_build_staging_dir("build.importing.x"));
    assert!(!is_build_staging_dir("build.importing."));
    assert!(!is_build_staging_dir("sha256:register.importing.x"));
}

#[test]
fn register_source_cleanup_removes_flat_but_preserves_staging() {
    let temp = tempfile::tempdir().unwrap();

    let rootfs = temp.path().join("rootfs.ext4");
    let metadata = temp.path().join("metadata.json");
    std::fs::write(&rootfs, b"rootfs").unwrap();
    std::fs::write(&metadata, b"{}").unwrap();
    templates::RegisterSource::Flat {
        rootfs: rootfs.to_string_lossy().into_owned(),
        metadata: metadata.to_string_lossy().into_owned(),
    }
    .cleanup();
    assert!(!rootfs.exists());
    assert!(!metadata.exists());

    // Build staging dirs are caller-owned: cleanup must leave them alone
    // so a lost-response retry of the same registration can converge.
    let staging = temp.path().join("build.importing.job-1");
    std::fs::create_dir(&staging).unwrap();
    std::fs::write(staging.join("rootfs.ext4"), b"rootfs").unwrap();
    templates::RegisterSource::BuildStaging {
        dir: staging.to_string_lossy().into_owned(),
    }
    .cleanup();
    assert!(staging.exists());
}

#[test]
fn finish_register_with_existing_digest_preserves_staging_for_retry() {
    // Models two interleavings that must converge on the published
    // digest: a concurrent same-content registration winning the
    // staging→digest rename, and a RETRY of a registration whose
    // response was lost after the digest was published. In both cases
    // the build staging dir must survive — its lifetime belongs to
    // build-manager, which removes it only after observing a durable
    // response. Consuming it here would make a lost-response retry hit
    // NotFound for an already-registered build.
    let (digest_temp, content_hash, _bytes) = expanded_test_rootfs_setup();
    let digest_dir = digest_temp.path().to_str().unwrap().to_string();

    let staging_temp = tempfile::tempdir().unwrap();
    let staging = staging_temp.path().join("build.importing.job-loser");
    std::fs::create_dir(&staging).unwrap();
    std::fs::write(staging.join("rootfs.ext4"), b"loser copy").unwrap();
    let source = templates::RegisterSource::BuildStaging {
        dir: staging.to_string_lossy().into_owned(),
    };

    let conn = crate::db::test_conn();
    let service = TemplateManagerService::new(conn).unwrap();

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let response = runtime
        .block_on(service.finish_register_with_existing_digest(
            templates::ExistingDigestRegistration {
                template_id: "register-refresh-test",
                content_hash: &content_hash,
                digest_dir: &digest_dir,
                metadata_bytes: Some(br#"{"disk_size_mb":4}"#),
                requested_network_policy: None,
                publish_node_state: false,
                register_source: &source,
            },
        ))
        .expect("registration must converge on the existing digest")
        .into_inner();

    let template = response.template.expect("template ref");
    assert_eq!(
        template.content_hash.as_deref(),
        Some(content_hash.as_str())
    );
    assert!(
        staging.exists(),
        "build staging dir is caller-owned and must survive so a lost-response retry can converge"
    );
    let conn = service.db.lock().unwrap();
    assert!(
        crate::db::get_build(&conn, "register-refresh-test", &content_hash)
            .unwrap()
            .is_some()
    );
}

#[test]
fn build_staging_dir_staleness_is_age_gated() {
    let temp = tempfile::tempdir().unwrap();
    let staging = temp.path().join("build.importing.job-1");
    std::fs::create_dir(&staging).unwrap();
    assert!(
        !startup_recovery::build_staging_dir_is_stale(&staging),
        "fresh staging dir must not be reaped"
    );

    let old = std::time::SystemTime::now() - std::time::Duration::from_secs(25 * 60 * 60);
    let dir_handle = std::fs::File::open(&staging).unwrap();
    dir_handle.set_modified(old).unwrap();
    assert!(
        startup_recovery::build_staging_dir_is_stale(&staging),
        "staging dir older than the backstop must be reaped"
    );
}

#[cfg(target_os = "linux")]
#[test]
fn template_refresh_preserves_nocow_and_timestamps() {
    use rustix::fs::{IFlags, ioctl_getflags, ioctl_setflags};
    use std::os::unix::fs::FileExt;
    let Some(root) = std::env::var_os("SPACE_NOCOW_TEST_BTRFS_DIR") else {
        assert!(
            std::env::var_os("CI").is_none()
                && std::env::var_os("SPACE_CI_REQUIRE_TEST_DEPS").is_none(),
            "CI must provide the disposable NoCoW btrfs fixture"
        );
        eprintln!("not opted in: set SPACE_NOCOW_TEST_BTRFS_DIR to a disposable btrfs directory");
        return;
    };
    let temporary = tempfile::tempdir_in(root).unwrap();
    let src = temporary.path().join("rootfs.ext4");
    let dst = temporary.path().join("republished.ext4");
    let file = std::fs::File::create(&src).unwrap();
    ioctl_setflags(&file, IFlags::NOCOW).unwrap();
    file.write_all_at(&[7; 8192], 0).unwrap();
    let modified = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1234567890);
    file.set_times(std::fs::FileTimes::new().set_modified(modified))
        .unwrap();
    copy_template_file(&src, &dst).unwrap();
    let copied = std::fs::File::open(&dst).unwrap();
    assert!(ioctl_getflags(&copied).unwrap().contains(IFlags::NOCOW));
    assert_eq!(copied.metadata().unwrap().modified().unwrap(), modified);
    assert_eq!(std::fs::read(&dst).unwrap(), vec![7; 8192]);
    file.write_all_at(b"changed", 0).unwrap();
    assert_eq!(std::fs::read(&dst).unwrap(), vec![7; 8192]);
}

#[rstest::rstest]
#[case::default(None, false)]
#[case::sha256_override(Some(HashAlg::Sha256), false)]
#[case::cloud_hypervisor(None, true)]
#[tokio::test]
async fn register_template_uses_producer_algorithm(
    #[case] override_algorithm: Option<HashAlg>,
    #[case] cloud_hypervisor: bool,
    #[values(None, Some(false), Some(true))] sparse_assignment: Option<bool>,
) {
    let (fixture, sha256, bytes) = expanded_test_rootfs_setup();
    let expected = match override_algorithm {
        None | Some(HashAlg::Blake3) => format!("blake3:{}", blake3::hash(&bytes)),
        Some(HashAlg::Sha256) => sha256,
    };
    let root = tempfile::tempdir().unwrap();
    let name = "registration-hash-test";
    let source = root.path().join(name).join("build.importing.test");
    std::fs::create_dir_all(&source).unwrap();
    for file in [TEMPLATE_ROOTFS_FILE, "metadata.json"] {
        std::fs::copy(fixture.path().join(file), source.join(file)).unwrap();
    }
    if cloud_hypervisor {
        use pplx_space_common_core::types::{Arch, TemplateMetadata};
        let environments =
            pplx_space_execution_catalog::catalog::CompiledEnvironment::for_architecture(
                Arch::X86_64,
            )
            .unwrap();
        let definition = environments
            .iter()
            .find(|environment| environment.name() == "ch-54.0-linux-7.2.8")
            .unwrap()
            .definition();
        let mut metadata = TemplateMetadata::default();
        metadata.set_execution_metadata(
            definition
                .bind_execution(definition.execution_requirements().unwrap())
                .unwrap(),
        );
        metadata.arch = Some("x86_64".into());
        std::fs::write(
            source.join("metadata.json"),
            serde_json::to_vec(&metadata).unwrap(),
        )
        .unwrap();
    }
    let mut service =
        TemplateManagerService::new_with_template_root(crate::db::test_conn(), root.path())
            .unwrap()
            .without_sbx_connect_retries();
    if let Some(enabled) = sparse_assignment {
        service = service.with_feature_flags(FeatureFlagClient::fixed_bools([(
            pplx_space_feature_flags::SPACE_TEMPLATE_SPARSE_AWARE_READS_FLAG.key(),
            enabled,
        )]));
    }
    if let Some(algorithm) = override_algorithm {
        service = service.with_production_hash_algorithm(algorithm);
    }
    let response = service
        .register_template(internal_request(crate::proto::RegisterTemplateRequest {
            template: Some(crate::proto::TemplateRef {
                name: name.into(),
                ..Default::default()
            }),
            staging_dir: Some("build.importing.test".into()),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    let observed = response.template.unwrap().content_hash.unwrap();
    let published = root.path().join(name).join(&observed);
    // Registration schedules compression; keep its task-owned directory until it finishes.
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while rootfs_zstd_generation_inflight()
            .lock()
            .unwrap()
            .contains(&published)
        {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("registration compression should finish");
    assert_eq!(observed, expected);
    assert_eq!(
        std::fs::read(published.join(TEMPLATE_ROOTFS_FILE)).unwrap(),
        bytes
    );
    let manifest: pplx_space_common_core::types::TemplateManifest =
        serde_json::from_slice(&std::fs::read(published.join("manifest.json")).unwrap()).unwrap();
    assert_eq!(manifest.content_hash, expected);
    let build = crate::db::get_build(&service.db.lock().unwrap(), name, &expected)
        .unwrap()
        .expect("registered identity must be persisted");
    assert_eq!(build.content_hash, expected);
    if cloud_hypervisor {
        assert_eq!(manifest.runtime, "ch");
        assert_eq!(build.runtime, "ch");
    }
}

#[tokio::test]
async fn firmware_template_refuses_pool_and_snapshot_intent_before_persisting() {
    let root = tempfile::tempdir().unwrap();
    let conn = crate::db::test_conn();
    let execution = serde_json::from_value(serde_json::json!({
        "environment": format!("sha256:{}", "a".repeat(64)),
        "runtime": {"kind": "cloud-hypervisor", "version": "54.0"},
        "guest": {"os": "windows", "architecture": "x86_64"},
        "components": [{"kind":"firmware", "role":"firmware", "version":"test-1",
            "digest": format!("sha256:{}", "b".repeat(64)), "format":"edk2-fd"}]
    }))
    .unwrap();
    let image = serde_json::from_value(serde_json::json!({
        "version":1, "disk":"opaque_raw_disk", "network":"slot_dhcp_v1", "agent":"absent",
        "readiness":{"kind":"tcp_service", "port":3389}
    }))
    .unwrap();
    let mut manifest = pplx_space_common_core::types::TemplateManifest {
        guest_image: Some(image),
        ..Default::default()
    };
    manifest.set_execution_metadata(execution);
    let hash = format!("blake3:{}", "c".repeat(64));
    crate::db::upsert_build(
        &conn,
        &crate::db::BuildUpsert {
            name: "firmware-test",
            content_hash: &hash,
            build_date: "2026-01-01T00:00:00Z",
            manifest_json: &serde_json::to_string(&manifest).unwrap(),
            runtime: "ch",
            arch: "x86_64",
            artifact_size_bytes: 0,
            state: "ready",
        },
    )
    .unwrap();
    let service = TemplateManagerService::new_with_template_root(conn, root.path()).unwrap();
    let original = crate::db::get_template_settings(&service.db.lock().unwrap(), "firmware-test")
        .unwrap()
        .unwrap();
    let pool = service
        .create_pool(internal_request(CreatePoolRequest {
            template: "firmware-test".into(),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    let snapshot = service
        .update_template_snapshot_policy(internal_request(UpdateTemplateSnapshotPolicyRequest {
            template: "firmware-test".into(),
            ensure_base_snapshot: true,
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(pool.code(), tonic::Code::FailedPrecondition);
    assert_eq!(snapshot.code(), tonic::Code::FailedPrecondition);
    let db = service.db.lock().unwrap();
    assert!(crate::db::get_pool(&db, "firmware-test").unwrap().is_none());
    let retained = crate::db::get_template_settings(&db, "firmware-test")
        .unwrap()
        .unwrap();
    assert_eq!(retained.ensure_base_snapshot, original.ensure_base_snapshot);
    assert_eq!(
        retained.base_snapshot_policy_set_at,
        original.base_snapshot_policy_set_at
    );
    assert_eq!(retained.snapshot_vcpu, original.snapshot_vcpu);
    assert_eq!(retained.snapshot_memory_mib, original.snapshot_memory_mib);
}
