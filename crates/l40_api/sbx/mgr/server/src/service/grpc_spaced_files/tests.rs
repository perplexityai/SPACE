use super::*;

#[test]
fn cpu_stat_usage_usec_parses_the_usage_line() {
    let content = "usage_usec 41398047\nuser_usec 40000000\nsystem_usec 1398047\n";
    assert_eq!(cpu_stat_usage_usec(content), Some(41_398_047));
    assert_eq!(cpu_stat_usage_usec("user_usec 1\n"), None);
}

#[test]
fn inspect_warnings_are_rate_limited_per_sandbox() {
    let warnings = PerSandboxWarnings::new(Duration::from_secs(60));
    let start = Instant::now();
    assert!(warnings.admit("a", start));
    assert!(!warnings.admit("a", start + Duration::from_secs(59)));
    // Another sandbox is not held back by the first one's warning.
    assert!(warnings.admit("b", start + Duration::from_secs(1)));
    assert!(warnings.admit("a", start + Duration::from_secs(60)));
    // Admitting a warning drops every expired entry.
    assert!(warnings.admit("c", start + Duration::from_secs(120)));
    assert_eq!(warnings.last_warned.lock().unwrap().len(), 1);
}

/// A memory update names guest memory, and node-manager gets the cgroup limit
/// a launch gives that guest: its memory plus the node's headroom, capped at
/// the node's sandbox cgroup size. With the guest's value alone, the update
/// would leave the VM no headroom.
#[tokio::test]
async fn memory_update_sends_the_cgroup_limit_a_launch_gives_that_guest() {
    use crate::node_manager_transport::fake::FakeNodeManager;
    use pplx_space_accounts::SandboxOwner;
    use pplx_space_node_mgr_client::proto::{
        NetworkPoolStatusRequest, NetworkPoolStatusResponse, UpdateCgroupRequest,
        UpdateCgroupResponse,
    };
    use pplx_space_sandbox_model::SandboxRuntimeLabel;

    let conn = crate::db::test_conn();
    let id = SandboxId::new().to_string();
    crate::db::insert_sandbox(
        &conn,
        crate::db::InsertSandboxParams {
            id: &id,
            uuid8: &SandboxRuntimeLabel::parse("01234567").unwrap(),
            state: SandboxState::Started.into(),
            template: "seed",
            template_hash: None,
            user: &SandboxOwner::parse("test-owner").unwrap(),
            runtime: "fc",
            guest_image: Default::default(),
            snapshots_enabled: false,
            rootfs_size_mb: None,
            disk_io_limits: Default::default(),
            pool_template: None,
            volume_encryption: false,
            process_env: &Default::default(),
            auto_lifecycle: &AutoLifecyclePolicy::disabled(),
            network_policy_override: None,
            network_policy_generation: 0,
            network_policy_authority: crate::db::PolicyAuthority::Local,
            auto_lifecycle_authority: crate::db::PolicyAuthority::Local,
        },
    )
    .unwrap();
    let sent = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let recorded = std::sync::Arc::clone(&sent);
    let node = FakeNodeManager::default()
        .unary("GetNetworkPoolStatus", |_: NetworkPoolStatusRequest| {
            Ok(NetworkPoolStatusResponse {
                sandbox_cgroup_memory_mib: 16_512,
                sandbox_guest_memory_max_mib: 16_384,
                sandbox_memory_headroom_mib: 128,
                ..Default::default()
            })
        })
        .unary("UpdateCgroup", move |request: UpdateCgroupRequest| {
            recorded.lock().unwrap().push(request.memory_mib);
            Ok(UpdateCgroupResponse {})
        })
        .serve();
    let service = SandboxManagerService::new_with_options(
        conn,
        SandboxManagerServiceOptions {
            node_manager: node.connector(),
            ..SandboxManagerServiceOptions::for_test()
        },
    );

    for (guest_memory_mib, cgroup_memory_mib) in
        [(1024, 1152), (512, 640), (16_384, 16_512), (65_536, 16_512)]
    {
        service
            .update(super::super::activation_support::internal_request(
                UpdateRequest {
                    sandbox_id: id.clone(),
                    memory_mib: Some(guest_memory_mib),
                    ..Default::default()
                },
            ))
            .await
            .unwrap();
        assert_eq!(
            sent.lock().unwrap().pop(),
            Some(Some(cgroup_memory_mib)),
            "{guest_memory_mib} MiB of guest memory"
        );
    }
}
