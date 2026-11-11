use std::sync::{Arc, Mutex};

use super::*;

#[derive(Clone, Default)]
struct FakeSink {
    requests: Arc<Mutex<Vec<PublicOperationGcRequest>>>,
    reports: Arc<Mutex<Vec<PublicOperationGcReport>>>,
}

impl FakeSink {
    fn push_report(&self, report: PublicOperationGcReport) {
        self.reports.lock().unwrap().push(report);
    }

    fn requests(&self) -> Vec<PublicOperationGcRequest> {
        self.requests.lock().unwrap().clone()
    }
}

impl RetentionSink for FakeSink {
    async fn gc_expired(
        &self,
        request: PublicOperationGcRequest,
    ) -> Result<PublicOperationGcReport, PublicOperationStoreError> {
        self.requests.lock().unwrap().push(request);
        Ok(self.reports.lock().unwrap().pop().unwrap_or_default())
    }
}

fn config(max_records_per_batch: u32, max_batches_per_tick: u32) -> ReconcilerConfig {
    ReconcilerConfig {
        tick: Duration::from_secs(60),
        max_records_per_batch,
        max_batches_per_tick,
    }
}

fn batch(operations_deleted: u64, idempotency_records_deleted: u64) -> PublicOperationGcReport {
    PublicOperationGcReport {
        operations_deleted,
        idempotency_records_deleted,
    }
}

#[tokio::test]
async fn tick_uses_configured_batch_and_reports_counts() {
    let sink = FakeSink::default();
    sink.push_report(batch(3, 5));
    let reconciler = Reconciler::new(config(42, 20), sink.clone());

    let report = reconciler.tick().await.expect("tick should succeed");

    assert_eq!(
        sink.requests(),
        vec![PublicOperationGcRequest {
            max_records: 42,
            payload_scrub_rules: FEDERATED_OPERATION_PAYLOAD_SCRUB_RULES,
        }]
    );
    assert_eq!(
        report,
        TickReport {
            operations_deleted: 3,
            idempotency_records_deleted: 5,
        }
    );
}

/// A full batch means more may be expired, so the tick runs another, until
/// one comes back short; either kind of record filling its batch counts.
#[tokio::test]
async fn tick_keeps_deleting_while_batches_come_back_full() {
    let sink = FakeSink::default();
    // Reports are popped from the end: full idempotency, full operations,
    // then a short batch.
    for report in [batch(1, 2), batch(10, 3), batch(0, 10)] {
        sink.push_report(report);
    }
    let reconciler = Reconciler::new(config(10, 20), sink.clone());

    let report = reconciler.tick().await.expect("tick should succeed");

    assert_eq!(sink.requests().len(), 3);
    assert_eq!(
        report,
        TickReport {
            operations_deleted: 11,
            idempotency_records_deleted: 15,
        }
    );
}

/// However far behind retention is, one tick runs at most its budget of
/// batches and leaves the rest for the next.
#[tokio::test]
async fn tick_stops_at_its_batch_budget() {
    let sink = FakeSink::default();
    for _ in 0..5 {
        sink.push_report(batch(0, 10));
    }
    let reconciler = Reconciler::new(config(10, 3), sink.clone());

    let report = reconciler.tick().await.expect("tick should succeed");

    assert_eq!(sink.requests().len(), 3);
    assert_eq!(report.idempotency_records_deleted, 30);
}
