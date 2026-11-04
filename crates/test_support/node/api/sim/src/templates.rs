use pplx_space_node_api_client::proto::{NodeTemplateBuild, SnapshotInfo};

/// Seeded fake template inventory reported by the simulator.
///
/// The values are picked to look enough like a real worker's `space-template-mgr`
/// inventory that scheduler/reconciler code can reason about template
/// presence without ever pulling a real artifact.
pub fn default_template_builds() -> Vec<NodeTemplateBuild> {
    vec![
        NodeTemplateBuild {
            name: "seed".into(),
            content_hash: Some("sim-seed-0000000000000001".into()),
            build_date: Some("2026-01-01T00:00:00Z".into()),
            artifact_size_bytes: Some(256 * 1024 * 1024),
            cache_size_bytes: Some(256 * 1024 * 1024),
            disk_size_mb: Some(4096),
            runtime: Some("fc".into()),
            arch: Some("aarch64".into()),
            snapshots: vec![SnapshotInfo {
                vcpu: 2,
                memory_mib: 1024,
                snapshot_hash: "sim-seed-snap-0001".into(),
            }],
        },
        NodeTemplateBuild {
            name: "code-runtime".into(),
            content_hash: Some("sim-code-runtime-0000000000000001".into()),
            build_date: Some("2026-01-01T00:00:00Z".into()),
            artifact_size_bytes: Some(1024 * 1024 * 1024),
            cache_size_bytes: Some(1024 * 1024 * 1024),
            disk_size_mb: Some(8192),
            runtime: Some("fc".into()),
            arch: Some("aarch64".into()),
            snapshots: vec![SnapshotInfo {
                vcpu: 2,
                memory_mib: 2048,
                snapshot_hash: "sim-code-runtime-snap-0001".into(),
            }],
        },
    ]
}
