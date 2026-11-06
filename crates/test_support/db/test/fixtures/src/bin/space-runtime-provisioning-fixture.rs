//! Synthetic registration for an explicitly disposable real-service smoke bench.

use std::error::Error;
use std::str::FromStr;

use pplx_space_common_core::types::NodeId;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    if std::env::args().skip(1).collect::<Vec<_>>() != ["--disposable-local-database"] {
        return Err("requires --disposable-local-database".into());
    }
    let url = std::env::var("SPACE_LOCAL_FIXTURE_DATABASE_URL")?;
    let options = PgConnectOptions::from_str(&url)?;
    if !matches!(options.get_host(), "127.0.0.1" | "localhost") {
        return Err("the fixture only registers a loopback-local disposable database".into());
    }
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await?;
    let node_id = NodeId::new();
    pplx_space_db_test_fixtures::insert_ready_worker_node(&pool, node_id).await?;
    // Machine-readable fixture output, not a service diagnostic.
    println!("{node_id}");
    pool.close().await;
    Ok(())
}
