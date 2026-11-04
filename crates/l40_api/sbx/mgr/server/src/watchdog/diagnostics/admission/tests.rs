use std::collections::HashMap;
use std::sync::Arc;

use super::super::TransitionId;
use super::*;

fn request(sandbox_id: &str, console_tails: ConsoleTails) -> EvidenceRequest {
    EvidenceRequest {
        sandbox_id: sandbox_id.to_string(),
        transition_id: TransitionId::new(),
        console_tails,
        queued_at: Instant::now(),
        merged_transitions: 0,
    }
}

/// Collectors saturated: nothing drains while 10,000 sandboxes degrade at
/// once and one more flaps through every tenth of them.
#[test]
fn queue_holds_one_report_per_sandbox_within_its_capacity() {
    let queue = EvidenceQueue::new(EVIDENCE_QUEUE_CAPACITY);
    let mut admissions = HashMap::<Admission, usize>::new();
    let mut flapping_transitions = Vec::new();
    for index in 0..10_000 {
        let admitted = queue.admit(request(&format!("sbx_{index}"), ConsoleTails::Included));
        *admissions.entry(admitted).or_default() += 1;
        if index % 10 == 0 {
            // Only the first degraded transition claims the tail allowance.
            let tails = if index == 0 {
                ConsoleTails::Included
            } else {
                ConsoleTails::RateLimited
            };
            let flap = request("sbx_flapping", tails);
            flapping_transitions.push(flap.transition_id);
            *admissions.entry(queue.admit(flap)).or_default() += 1;
        }
        assert!(queue.len() <= EVIDENCE_QUEUE_CAPACITY);
    }

    assert_eq!(
        admissions,
        HashMap::from([
            (Admission::Queued, EVIDENCE_QUEUE_CAPACITY),
            (Admission::Merged, 999),
            (Admission::Dropped, 10_000 - (EVIDENCE_QUEUE_CAPACITY - 1)),
        ])
    );
    let drained = std::iter::from_fn(|| queue.pop()).collect::<Vec<_>>();
    assert_eq!(drained.len(), EVIDENCE_QUEUE_CAPACITY);
    assert_eq!(drained[0].sandbox_id, "sbx_0");
    let flapping = &drained[1];
    assert_eq!(flapping.sandbox_id, "sbx_flapping");
    assert_eq!(flapping.merged_transitions, 999);
    assert_eq!(Some(&flapping.transition_id), flapping_transitions.last());
    assert_eq!(flapping.console_tails, ConsoleTails::Included);

    // A report taken by a collector no longer counts against its sandbox.
    assert_eq!(
        queue.admit(request("sbx_flapping", ConsoleTails::NotRequested)),
        Admission::Queued
    );
}

#[tokio::test]
async fn waiting_collectors_receive_each_admitted_report_once() {
    let queue = Arc::new(EvidenceQueue::new(EVIDENCE_QUEUE_CAPACITY));
    let (received, mut reports) = tokio::sync::mpsc::channel(8);
    let collectors = (0..2)
        .map(|_| {
            let queue = Arc::clone(&queue);
            let received = received.clone();
            tokio::spawn(async move {
                loop {
                    let report = queue.next().await;
                    if received.send(report.sandbox_id).await.is_err() {
                        return;
                    }
                }
            })
        })
        .collect::<Vec<_>>();
    drop(received);
    tokio::task::yield_now().await;

    for sandbox_id in ["sbx_a", "sbx_b", "sbx_c"] {
        assert_eq!(
            queue.admit(request(sandbox_id, ConsoleTails::NotRequested)),
            Admission::Queued
        );
    }
    let mut delivered = Vec::new();
    for _ in 0..3 {
        delivered.push(
            tokio::time::timeout(std::time::Duration::from_secs(5), reports.recv())
                .await
                .expect("a waiting collector takes each report")
                .expect("collectors are running"),
        );
    }
    delivered.sort();
    assert_eq!(delivered, ["sbx_a", "sbx_b", "sbx_c"]);
    assert_eq!(queue.len(), 0);
    for collector in collectors {
        collector.abort();
    }
}
