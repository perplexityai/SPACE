//! The metrics the cluster controller records.

pub mod admin;
pub mod cluster_reconciler;
pub mod data_plane;
pub mod dead_node;
pub mod leader_election;
pub mod node_drain;
pub mod node_group_labeler;
pub mod node_group_mirror;
pub mod node_service_readiness;
pub mod operations;
pub mod reconcile;
pub mod sandbox;
pub mod snapshot_storage_sweeper;
pub mod tombstone;
pub mod warm_pool;
