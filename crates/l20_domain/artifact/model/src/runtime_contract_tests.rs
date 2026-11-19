#[test]
fn template_cache_paths_preserve_space_layout() {
    assert_eq!(
        crate::template_build_dir(
            "seed",
            "abc",
            pplx_space_template_model::TemplateCachePartition::Legacy
        ),
        "/space/cache/firecracker/templates/seed/abc"
    );
    let environment = pplx_space_execution_spec::EnvironmentReference::new(
        pplx_space_content_digest::Digest::sha256([2; 32]),
    );
    assert_eq!(
        crate::template_snapshot_dir(
            "seed",
            "abc",
            pplx_space_template_model::TemplateCachePartition::Environment(environment),
            2,
            1024
        ),
        format!(
            "/space/cache/firecracker/templates/seed/snapshots/abc@{environment}/2vcpu-1024mib"
        )
    );
}
