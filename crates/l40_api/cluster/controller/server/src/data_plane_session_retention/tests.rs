use std::sync::{Arc, Mutex};

use super::*;

#[derive(Clone, Default)]
struct FakeSink {
    requests: Arc<Mutex<Vec<(Duration, u32)>>>,
    reports: Arc<Mutex<Vec<TickReport>>>,
}

impl FakeSink {
    fn push_report(&self, report: TickReport) {
        self.reports.lock().unwrap().push(report);
    }

    fn requests(&self) -> Vec<(Duration, u32)> {
        self.requests.lock().unwrap().clone()
    }
}

impl RetentionSink for FakeSink {
    async fn gc_expired_sessions(
        &self,
        grace: Duration,
        max_records: u32,
    ) -> Result<TickReport, sqlx::Error> {
        self.requests.lock().unwrap().push((grace, max_records));
        Ok(self.reports.lock().unwrap().pop().unwrap_or_default())
    }
}

#[tokio::test]
async fn tick_uses_configured_batch_and_grace() {
    let sink = FakeSink::default();
    sink.push_report(TickReport {
        sessions_deleted: 7,
    });
    let reconciler = Reconciler::new(
        ReconcilerConfig {
            tick: Duration::from_secs(60),
            grace: Duration::from_secs(900),
            max_records_per_tick: 42,
        },
        sink.clone(),
    );

    let report = reconciler.tick().await.expect("tick should succeed");

    assert_eq!(sink.requests(), vec![(Duration::from_secs(900), 42)]);
    assert_eq!(
        report,
        TickReport {
            sessions_deleted: 7,
        }
    );
}
