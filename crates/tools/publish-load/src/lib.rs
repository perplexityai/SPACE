//! Support for the synthetic, launcher-owned PublishNodeState benchmark.

pub mod service;
pub mod store;

use std::path::PathBuf;

use clap::Parser;
use pplx_space_node_api_client::proto::PublishedSandboxState;

#[derive(Clone, Debug, Parser)]
pub struct Args {
    #[arg(long, default_value_t = 50)]
    pub nodes: u32,
    #[arg(long, default_value_t = 160)]
    pub sandboxes_per_node: u32,
    #[arg(long, default_value_t = 10)]
    pub minutes: u32,
    #[arg(long, default_value_t = 42)]
    pub seed: u64,
    /// Wall seconds per simulated second; use 1 for standard evidence.
    #[arg(long, default_value_t = 1.0)]
    pub time_scale: f64,
    #[arg(long)]
    pub service_bin: PathBuf,
    #[arg(long)]
    pub migrator_bin: PathBuf,
    /// Fresh directory; reports never contain database URLs or credentials.
    #[arg(long)]
    pub output: PathBuf,
}

/// One simulated node's inventory and its one-time rare transition.
pub struct Node {
    pub id: String,
    pub sandboxes: Vec<PublishedSandboxState>,
    pub rare: PublishedSandboxState,
    pub retire_after_rare: bool,
}
