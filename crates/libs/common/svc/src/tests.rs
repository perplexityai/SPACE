use pplx_space_common_core::types::cluster::ClusterId;

use super::*;

#[test]
fn the_process_cluster_id_replaces_or_clears_the_callers() {
    let process = ClusterId::parse("process-cluster").unwrap();
    for (process_id, expected) in [
        (Some(&process), Some("process-cluster")),
        // SPACE_CLUSTER_ID unset: sandbox events carry no cluster id, so
        // neither may logs and metrics.
        (None, None),
    ] {
        let context = with_process_cluster_id(
            LogContext::default().with_cluster_id("caller-cluster"),
            process_id,
        );
        assert_eq!(context.cluster_id(), expected);
    }
}
