mod emit;

/// Declare a bounded metric-tag vocabulary: the enum, its canonical `as_str`,
/// and a `Display` that agrees with it.
///
/// Every tag value SPACE emits has to come from a closed set — an unbounded
/// tag is a cardinality incident — and the three pieces have to agree, so they
/// are generated together rather than written three times per enum.
///
/// Doc comments are forwarded, on the enum and on each variant, because what a
/// tag value *means* is the part an operator reading the metrics catalog needs
/// and the part a hand-written copy tends to omit.
///
/// Textually scoped from the crate root, so it is available to every module
/// declared below without a `use`.
macro_rules! metric_vocabulary {
    (
        $(#[$enum_meta:meta])*
        $name:ident { $($(#[$variant_meta:meta])* $variant:ident => $value:literal),+ $(,)? }
    ) => {
        $(#[$enum_meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum $name {
            $($(#[$variant_meta])* $variant),+
        }

        impl $name {
            pub const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $value),+
                }
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str(self.as_str())
            }
        }
    };
}

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
pub mod grpc;
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
pub mod placement_fence;
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
