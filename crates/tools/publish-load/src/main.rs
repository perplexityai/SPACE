//! Synthetic, loopback-only PublishNodeState workload against the real service.

mod measure;
mod workload;

use std::time::Duration;

use anyhow::{Result, ensure};
use clap::Parser;
use pplx_space_db_test_fixtures::{TestDb, pg_test_db};
use pplx_space_publish_load::{Args, service, store};

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    ensure!(args.nodes > 0 && args.sandboxes_per_node > 1 && args.minutes > 0);
    ensure!(
        args.nodes < 1_000_000 && args.sandboxes_per_node < 1_000_000,
        "fixture IDs reserve separate 24-bit node and sandbox ranges"
    );
    ensure!(args.seed <= u64::from(u32::MAX), "seed must fit 32 bits");
    ensure!(args.minutes <= 1440, "at most 24 simulated hours per run");
    ensure!(args.time_scale.is_finite() && args.time_scale > 0.0);
    ensure!(!args.output.exists(), "output directory already exists");
    ensure!(
        std::env::var_os("SPACE_TEST_DEPS_OPTIONAL").is_none(),
        "dependency skips are forbidden"
    );
    pplx_space_rustls_provider::install_default_rustls_crypto_provider();
    tokio::fs::create_dir_all(&args.output).await?;
    // Unlike optional unit-test fixtures, a missing database is a hard error.
    ensure!(
        std::env::var_os("SPACE_TEST_POSTGRES_ADMIN_URL").is_some(),
        "SPACE_TEST_POSTGRES_ADMIN_URL is required"
    );
    store::verify_owned_cluster().await?;
    let db = pg_test_db().await.map_err(anyhow::Error::from_boxed)?;
    let result = run(&args, &db).await;
    db.pool.close().await;
    drop(db);
    result
}

async fn run(args: &Args, db: &TestDb) -> Result<()> {
    let nodes = store::seed(&db.pool, args).await?;
    let mut service = service::LocalService::start(args, &db.database_url).await?;
    let result = async {
        // Fill the service's fixed-size pool and observed projections before
        // resetting statistics. Every measured run still uses a fresh database.
        workload::prime(&service, &nodes).await?;
        pplx_space_db_test_fixtures::publish_load::outcome::analyze(&db.pool)
            .await
            .map_err(anyhow::Error::from_boxed)?;
        // PostgreSQL's cumulative WAL counters flush asynchronously. Let the
        // priming writes reach shared stats before resetting their baseline.
        tokio::time::sleep(Duration::from_secs(2)).await;
        let measurement = measure::Measurement::start(&db.pool, service.pid()?, args).await?;
        let durations = workload::run(args, &service, nodes).await?;
        let report = measurement.finish(&db.pool, args, &durations).await?;
        store::dump(&db.pool, &args.output).await?;
        store::verify_transitions(&db.pool, args).await?;
        tokio::fs::write(
            args.output.join("report.json"),
            serde_json::to_vec_pretty(&report)?,
        )
        .await?;
        Ok::<_, anyhow::Error>(())
    }
    .await;
    service.stop().await?;
    result
}
