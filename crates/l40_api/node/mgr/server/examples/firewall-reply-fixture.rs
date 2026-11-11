//! Production rule installer for scripts/netgtw/tests/reply_boundary.py.

use std::path::Path;
use std::time::Duration;

use pplx_space_node_mgr::slot_pool::{NetworkPool, NetworkPoolConfig, NodeCtlNodeStatusDirtySink};
use pplx_space_node_mgr::{firewall, net};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../../../../db/node-manager");

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let namespace = "sns-reply-test";
    match std::env::args().nth(1).as_deref() {
        Some("recover") => recover_pool().await?,
        Some("setup") => {
            firewall::setup_namespace_rules(namespace, "vpeer", "198.18.0.1", "169.254.0.21")?;
            net::setup_tproxy(namespace, 12345, "vpeer", false)?;
            for protocol in ["tcp", "udp"] {
                firewall::allow_guest_port(namespace, "vpeer", 9000, protocol)?;
            }
        }
        Some("validate") => {
            firewall::validate_namespace_firewall_config(namespace, "vpeer", "169.254.0.21")?;
        }
        Some("reapply") => {
            net::setup_tproxy(namespace, 12345, "vpeer", false)?;
        }
        _ => {
            return Err(
                "expected setup, validate, reapply, or recover inside the isolated reply-boundary harness"
                    .into(),
            );
        }
    }
    Ok(())
}

// Real migrations and startup worker, inside the harness's disposable /space.
async fn recover_pool() -> Result<(), Box<dyn std::error::Error>> {
    let db = Path::new(pplx_space_common_core::host::linux::paths::NODE_MANAGER_DB);
    pplx_space_sqlite::migrate("node-manager", db, &MIGRATOR).await?;
    let mut config = NetworkPoolConfig::new(3);
    config.pod_netns_path = Some("host".into());
    NetworkPool::validate_startup(&config, db)?;
    let tracker = TaskTracker::new();
    let shutdown = CancellationToken::new();
    let pool = NetworkPool::open(
        config,
        128,
        tracker.clone(),
        shutdown.clone(),
        std::sync::Arc::new(NodeCtlNodeStatusDirtySink),
    )?;
    let recovered = tokio::time::timeout(Duration::from_secs(15), async {
        while pool.counts()?.ready != 3 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok::<_, Box<dyn std::error::Error>>(())
    })
    .await;
    shutdown.cancel();
    tracker.close();
    tracker.wait().await;
    recovered??;
    Ok(())
}
