//! Controlled local workload using the same public helpers as SPACE services.
//! No custom recorder/exporter; see scripts/tests/observability/metrics/README.md.
use std::time::Duration;

use pplx_space_metrics::{node_manager, sandbox};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let log_context = match pplx_space_common_core::types::cluster::process_cluster_id() {
        Some(cluster_id) => {
            pplx_space_observability::LogContext::default().with_cluster_id(cluster_id.as_str())
        }
        None => pplx_space_observability::LogContext::default(),
    };
    let guard = pplx_space_observability::init("space-oss-workload", log_context)?;
    let start = tokio::time::Instant::now();
    let mut increments = 0_u64;
    let mut active_ticks = 0_u64;
    for tick in 0..144_u64 {
        tokio::time::sleep_until(start + Duration::from_secs(tick * 5)).await;
        let increment = match tick {
            0..=23 => 2,
            24..=35 => 0,
            36..=71 => 5,
            72..=95 => 1,
            _ => 3,
        };
        if increment != 0 {
            node_manager::loop_devices_created(increment, increment * 2);
            sandbox::api::sandbox_created(false);
            for value in [1.0, 2.0, 4.0, 8.0] {
                sandbox::publication::state_convergence_duration_ms("started", value);
            }
            for value in [2, 4, 8] {
                sandbox::worker::guest_observed_specs("oss-synthetic", None, Some(value));
            }
            increments += u64::from(increment);
            active_ticks += 1;
        }
        let gauge = match tick {
            0..=11 => Some(7),
            12..=23 => Some(0),
            24..=119 => None,
            _ => Some(11),
        };
        if let Some(value) = gauge {
            node_manager::cleanup_operation_in_flight(value);
        }
        tracing::info!(
            tick,
            increment,
            increments,
            active_ticks,
            ?gauge,
            "oss workload tick"
        );
    }
    // Keep the final periodic export inside the measured twelve-minute run.
    tokio::time::sleep_until(start + Duration::from_secs(720)).await;
    // Drop flushes metrics synchronously; keep Tokio's I/O workers available.
    tokio::task::spawn_blocking(move || drop(guard)).await?;
    Ok(())
}
