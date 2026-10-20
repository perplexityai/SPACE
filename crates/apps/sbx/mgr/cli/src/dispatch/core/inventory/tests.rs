use super::*;

#[test]
fn template_hash_display_marks_stale_template_hashes() {
    let mut template_hashes = HashMap::new();
    template_hashes.insert("seed".to_string(), "sha256:abcdef0123459999".to_string());
    let sandbox = SandboxInfo {
        template: Some("seed".to_string()),
        image_hash: Some("sha256:abcdef0123459999".to_string()),
        ..Default::default()
    };
    assert_eq!(
        template_hash_display(&sandbox, &template_hashes),
        "abcdef012345"
    );

    let sandbox = SandboxInfo {
        template: Some("seed".to_string()),
        image_hash: Some("sha256:abcdef0123450000".to_string()),
        ..Default::default()
    };
    assert_eq!(
        template_hash_display(&sandbox, &template_hashes),
        "abcdef012345*"
    );
}

#[test]
fn sandbox_health_renders_composed_status_for_running_states() {
    let healthy = SandboxInfo {
        lifecycle: Some(pplx_space_sbx_mgr_client::proto::SandboxLifecycleState::Started as i32),
        health_status: pplx_space_sbx_mgr_client::proto::SandboxHealthStatus::Healthy as i32,
        ..Default::default()
    };
    assert_eq!(sandbox_health(&healthy), "\u{2705}");

    let degraded = SandboxInfo {
        lifecycle: Some(pplx_space_sbx_mgr_client::proto::SandboxLifecycleState::Paused as i32),
        health_status: pplx_space_sbx_mgr_client::proto::SandboxHealthStatus::Degraded as i32,
        ..Default::default()
    };
    assert_eq!(sandbox_health(&degraded), "\u{26a0}\u{fe0f}");

    let initializing = SandboxInfo {
        lifecycle: Some(pplx_space_sbx_mgr_client::proto::SandboxLifecycleState::Started as i32),
        health_status: pplx_space_sbx_mgr_client::proto::SandboxHealthStatus::Initializing as i32,
        ..Default::default()
    };
    assert_eq!(sandbox_health(&initializing), "\u{23f3}");

    let stopped = SandboxInfo {
        lifecycle: Some(pplx_space_sbx_mgr_client::proto::SandboxLifecycleState::Stopped as i32),
        ..Default::default()
    };
    assert_eq!(sandbox_health(&stopped), "-");
}
