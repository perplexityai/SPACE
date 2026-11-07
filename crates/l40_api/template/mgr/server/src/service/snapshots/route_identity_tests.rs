use super::*;

#[test]
fn base_snapshot_create_and_start_delegate_direct_identity_to_sandbox_manager() {
    // Request construction preserves identity; admission owns capability checks.
    for runtime in pplx_space_common_core::types::RuntimeKind::all() {
        let create = base_snapshot_create_request(
            "seed",
            "sha256:test",
            crate::db::TemplateCachePartition::Legacy,
            Some(1024),
            runtime,
        );
        let start = base_snapshot_start_request("sandbox-id".to_string(), 2, 2048, 60);

        assert_eq!(create.runtime, runtime.alias());
        assert_eq!(create.route_identity, None);
        assert_eq!(start.route_identity, None);
    }
}
