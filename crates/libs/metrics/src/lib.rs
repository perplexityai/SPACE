// `metric_vocabulary!` is invoked unqualified throughout this crate; the
// macro prelude makes it resolve in every module without a `use`.
#[macro_use(metric_vocabulary)]
extern crate pplx_space_metrics_emit;

mod emit;

pub mod admin;
pub mod auth;
pub mod byok;
pub mod cluster_reconciler;
pub mod credmgr;
pub mod data_plane;
pub mod db;
pub mod gateway;
pub mod internal_api;
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
pub mod operations;
pub mod reconcile;
pub mod response;
pub mod runtime_supervisor;
pub mod runtime_upgrade;
pub mod sandbox;
pub mod sandbox_eviction;
pub mod sandbox_fork;
pub mod snapshot;
pub mod snapshot_retention_sweeper;
pub mod snapshot_storage_sweeper;
pub mod stream_presence;
pub mod template;
pub mod tombstone;
pub mod volume_manager;
pub mod warm_pool;
