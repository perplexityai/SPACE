//! # Test matrix
//!
//! ## C — `ReconcilerConfig`
//! - C1 `c1_default_is_disabled_with_24h_min_age`
//!
//! ## T — Tick behavior (fake sink)
//! - T1 `t1_no_stuck_rows_emits_empty_report`
//! - T2 `t2_stuck_rows_reaped_into_report`
//! - T3 `t3_db_error_does_not_panic_and_reaps_nothing`
//! - T4 `t4_tick_passes_configured_min_age_to_sink`
//!
//! The reap statement's CAS and predicate semantics are pinned by
//! `pplx-space-snapshot-store`'s `upload::fail_stuck_full_uploads` tests.

use pplx_space_accounts::NodeId;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_snapshot_model::SnapshotId;

use std::sync::Mutex as StdMutex;
use std::time::Duration;

use pplx_space_durations::PositiveDuration;

use super::*;

// -----------------------------------------------------------------
// Fakes
// -----------------------------------------------------------------

#[derive(Default)]
struct FakeSinkState {
    /// Rows to return from the next `reap_stuck_creating` call.
    claim: Vec<ReapedSnapshot>,
    /// When true, the next `reap_stuck_creating` returns an error.
    db_error: bool,
    /// Every `min_age` the reconciler asked about, in order.
    min_age_calls: Vec<PositiveDuration>,
}

struct FakeSink(StdMutex<FakeSinkState>);

impl FakeSink {
    fn new() -> Self {
        Self(StdMutex::new(FakeSinkState::default()))
    }

    fn set_claim(&self, rows: Vec<ReapedSnapshot>) {
        self.0.lock().unwrap().claim = rows;
    }

    fn set_db_error(&self) {
        self.0.lock().unwrap().db_error = true;
    }

    fn min_age_calls(&self) -> Vec<PositiveDuration> {
        self.0.lock().unwrap().min_age_calls.clone()
    }
}

impl ReaperSink for FakeSink {
    async fn reap_stuck_creating(
        &self,
        min_age: PositiveDuration,
    ) -> Result<Vec<ReapedSnapshot>, sqlx::Error> {
        let mut state = self.0.lock().unwrap();
        state.min_age_calls.push(min_age);
        if state.db_error {
            state.db_error = false;
            return Err(sqlx::Error::PoolClosed);
        }
        Ok(std::mem::take(&mut state.claim))
    }
}

fn enabled_config(min_age_secs: u64) -> ReconcilerConfig {
    ReconcilerConfig {
        enabled: true,
        tick: DEFAULT_TICK,
        min_age: PositiveDuration::new(Duration::from_secs(min_age_secs)).unwrap(),
    }
}

fn reaped_row() -> ReapedSnapshot {
    ReapedSnapshot {
        snapshot_id: SnapshotId::new(),
        sandbox_id: SandboxId::new(),
        source_node_id: Some(NodeId::new()),
    }
}

// -----------------------------------------------------------------
// C — Config
// -----------------------------------------------------------------

#[test]
fn c1_default_is_disabled_with_24h_min_age() {
    let config = ReconcilerConfig::default();
    assert!(!config.enabled, "reaper must default to disabled");
    assert_eq!(
        config.min_age.get(),
        Duration::from_secs(DEFAULT_MIN_AGE_SECS)
    );
    // The default must sit comfortably above the worker's ~10.13h full
    // retry window so a slow-but-alive upload is never reaped.
    assert!(
        config.min_age.get() >= Duration::from_secs(36_455),
        "default min_age must exceed the worker retry window"
    );
}

// -----------------------------------------------------------------
// T — Tick behavior
// -----------------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn t1_no_stuck_rows_emits_empty_report() {
    let sink = FakeSink::new();
    let reconciler = Reconciler::new(enabled_config(86_400), sink);
    let report = reconciler.tick().await;
    assert_eq!(report, TickReport::default());
}

#[tokio::test(flavor = "current_thread")]
async fn t2_stuck_rows_reaped_into_report() {
    let sink = FakeSink::new();
    sink.set_claim(vec![reaped_row(), reaped_row(), reaped_row()]);
    let reconciler = Reconciler::new(enabled_config(86_400), sink);
    let report = reconciler.tick().await;
    assert_eq!(report.reaped, 3);
}

#[tokio::test(flavor = "current_thread")]
async fn t3_db_error_does_not_panic_and_reaps_nothing() {
    let sink = FakeSink::new();
    sink.set_db_error();
    let reconciler = Reconciler::new(enabled_config(86_400), sink);
    let report = reconciler.tick().await;
    assert_eq!(report, TickReport::default());
}

#[tokio::test(flavor = "current_thread")]
async fn t4_tick_passes_configured_min_age_to_sink() {
    let sink = FakeSink::new();
    let reconciler = Reconciler::new(enabled_config(12_345), sink);
    let _ = reconciler.tick().await;
    let calls = reconciler.sink.min_age_calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].get(), Duration::from_secs(12_345));
}
