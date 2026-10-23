// `metric_vocabulary!` is invoked unqualified throughout this crate; the
// macro prelude makes it resolve in every module without a `use`.
#[macro_use(metric_vocabulary)]
extern crate pplx_space_metrics_emit;

mod emit;

#[cfg(any(test, feature = "test-capture"))]
pub mod test_capture {
    #[cfg(feature = "test-capture")]
    pub use crate::emit::test_capture::{
        CapturedCounter, CounterCaptureGuard, ValueCaptureGuard, capture_counters_guard,
        capture_values_guard,
    };
    pub use crate::emit::test_capture::{CapturedValue, capture_values};
}

pub mod admin;
pub mod auth;
pub mod byok;
pub mod cluster_reconciler;
pub mod credmgr;
pub mod data_plane;
pub mod db;
pub mod feature_flags;
pub mod gateway;
pub use pplx_space_grpc_metrics as grpc;
pub mod internal_api;
pub mod jobs;
pub mod leader_election;
pub mod node_api;
pub mod node_disk;
pub mod node_drain;
pub mod node_group_labeler;
pub mod node_group_mirror;
pub mod node_group_placement;
pub mod node_manager;
pub mod node_network;
pub mod node_service_readiness;
pub mod object_store;
pub mod operations;
pub mod reconcile;
pub mod response;
pub mod sandbox;
pub mod sandbox_eviction;
pub mod sandbox_fork;
pub mod snapshot;
pub mod snapshot_retention_sweeper;
pub mod snapshot_storage_sweeper;
pub mod stream_presence;
pub mod template;
pub mod tokio_runtime;
pub mod tombstone;
pub mod volume_manager;
pub mod warm_pool;
