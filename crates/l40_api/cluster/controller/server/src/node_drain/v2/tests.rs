//! The v2 loop's decisions against a scripted sink and terminator.

use std::collections::HashMap;
use std::sync::Mutex;

use pplx_space_sbx_mgr_client::proto::DrainEvacuated;

use super::*;

#[derive(Default)]
struct FakeSink {
    nodes: Vec<V2Node>,
    responses: HashMap<(NodeId, bool), Result<DrainNodeResponse, String>>,
    records: HashMap<String, EvacuationRecord>,
    calls: Mutex<Vec<String>>,
    /// When set, each `DrainNode` call waits here before it answers.
    barrier: Option<tokio::sync::Barrier>,
}

impl FakeSink {
    fn log(&self, entry: String) {
        self.calls.lock().unwrap().push(entry);
    }

    fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }
}

impl DrainV2Sink for FakeSink {
    async fn list_v2_nodes(&self) -> Result<Vec<V2Node>, String> {
        Ok(self.nodes.clone())
    }

    async fn admit_evacuation(&self, node_id: NodeId, epoch: u64) -> Result<bool, String> {
        self.log(format!("admit {node_id} {epoch}"));
        Ok(true)
    }

    async fn ensure_cordon(&self, node_id: NodeId) -> Result<(), String> {
        self.log(format!("cordon {node_id}"));
        Ok(())
    }

    async fn drain_node(
        &self,
        node_id: NodeId,
        epoch: u64,
        cancel: bool,
    ) -> Result<DrainNodeResponse, String> {
        self.log(format!("drain_node {node_id} {epoch} cancel={cancel}"));
        if let Some(barrier) = &self.barrier {
            barrier.wait().await;
        }
        self.responses
            .get(&(node_id, cancel))
            .cloned()
            .unwrap_or_else(|| Err("unscripted".into()))
    }

    async fn record_evacuation(
        &self,
        _node_id: NodeId,
        evacuated: &DrainEvacuated,
    ) -> Result<EvacuationRecord, String> {
        self.log(format!("record {}", evacuated.sandbox_id));
        Ok(self
            .records
            .get(&evacuated.sandbox_id)
            .cloned()
            .unwrap_or(EvacuationRecord::Recorded))
    }

    async fn record_report(
        &self,
        node_id: NodeId,
        epoch: u64,
        report: serde_json::Value,
    ) -> Result<bool, String> {
        self.log(format!("report {node_id} {epoch} {report}"));
        Ok(true)
    }

    async fn advance_drained(&self, node_id: NodeId, epoch: u64) -> Result<bool, String> {
        self.log(format!("advance {node_id} {epoch}"));
        Ok(true)
    }

    async fn finish_cancel(&self, node_id: NodeId, epoch: u64) -> Result<bool, String> {
        self.log(format!("finish_cancel {node_id} {epoch}"));
        Ok(true)
    }

    async fn record_error(&self, node_id: NodeId, message: &str) {
        self.log(format!("error {node_id} {message}"));
    }

    async fn record_terminated(&self, node_id: NodeId, epoch: u64) -> Result<bool, String> {
        self.log(format!("terminated {node_id} {epoch}"));
        Ok(true)
    }
}

struct FakeTerminator {
    result: Result<(), String>,
    calls: Mutex<Vec<NodeId>>,
}

impl FakeTerminator {
    fn ok() -> Self {
        Self {
            result: Ok(()),
            calls: Mutex::default(),
        }
    }
}

impl InstanceTerminator for FakeTerminator {
    async fn terminate(&self, node_id: NodeId) -> Result<(), TerminateError> {
        self.calls.lock().unwrap().push(node_id);
        self.result.clone().map_err(TerminateError::Provider)
    }
}

fn node(phase: Option<&str>, epoch: Option<u64>, cancel: bool) -> V2Node {
    V2Node {
        id: NodeId::new(),
        phase: phase.map(str::to_owned),
        epoch,
        cancel,
        terminated: false,
        waiting: false,
    }
}

fn response(sealed_epoch: Option<u64>, done: bool, evacuated: &[&str]) -> DrainNodeResponse {
    DrainNodeResponse {
        sealed_epoch,
        done,
        evacuated: evacuated
            .iter()
            .map(|id| DrainEvacuated {
                sandbox_id: (*id).to_owned(),
                prior: DrainPrior::Started as i32,
                artifact_id: "artifact".to_owned(),
            })
            .collect(),
        ..Default::default()
    }
}

/// A row without an epoch gets no node call at all: the seal it would set
/// has no epoch the row records.
#[tokio::test]
async fn a_v2_node_without_an_epoch_gets_no_call() {
    let sink = FakeSink {
        nodes: vec![node(Some("draining"), None, false)],
        ..FakeSink::default()
    };
    tick(&sink, &FakeTerminator::ok(), DEFAULT_MAX_EVACUATING_NODES).await;
    assert!(sink.calls().is_empty(), "{:?}", sink.calls());
}

/// At most the cap's nodes evacuate: a waiting drain is admitted, oldest
/// first, only to a free slot, and sealed in the tick that admits it. A
/// drained node holds no slot. The ones still waiting are cordoned and get
/// no `DrainNode` call, so nothing seals them.
#[tokio::test]
async fn waiting_drains_are_admitted_oldest_first_up_to_the_cap() {
    let waiting = |epoch: u64| V2Node {
        waiting: true,
        ..node(Some("draining"), Some(epoch), false)
    };
    let evacuating = node(Some("draining"), Some(1), false);
    let drained = node(Some("drained"), Some(2), false);
    let (newest, oldest, middle) = (waiting(30), waiting(10), waiting(20));
    let nodes = vec![
        evacuating.clone(),
        drained.clone(),
        newest.clone(),
        oldest.clone(),
        middle.clone(),
    ];
    let sink = FakeSink {
        responses: nodes
            .iter()
            .map(|node| ((node.id, false), Ok(response(node.epoch, false, &[]))))
            .collect(),
        nodes,
        ..FakeSink::default()
    };
    let report = tick(&sink, &FakeTerminator::ok(), 2).await;
    assert_eq!(report.admitted, [oldest.id]);
    let calls = sink.calls();
    assert!(
        calls.contains(&format!("admit {} 10", oldest.id)),
        "{calls:?}"
    );
    for sealed in [&evacuating, &drained, &oldest] {
        assert!(
            calls.contains(&format!(
                "drain_node {} {} cancel=false",
                sealed.id,
                sealed.epoch.unwrap()
            )),
            "{calls:?}"
        );
    }
    for held in [&newest, &middle] {
        assert!(calls.contains(&format!("cordon {}", held.id)), "{calls:?}");
        assert!(
            !calls
                .iter()
                .any(|call| call.starts_with(&format!("drain_node {}", held.id))
                    || call.starts_with(&format!("admit {}", held.id))),
            "{calls:?}"
        );
    }
}

/// A tick calls its nodes at once: two nodes whose `DrainNode` calls each
/// wait for the other both answer.
#[tokio::test]
async fn a_tick_calls_its_nodes_at_once() {
    let nodes = vec![
        node(Some("draining"), Some(1), false),
        node(Some("draining"), Some(1), false),
    ];
    let sink = FakeSink {
        responses: nodes
            .iter()
            .map(|node| ((node.id, false), Ok(response(Some(1), false, &[]))))
            .collect(),
        nodes,
        barrier: Some(tokio::sync::Barrier::new(2)),
        ..FakeSink::default()
    };
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        tick(&sink, &FakeTerminator::ok(), DEFAULT_MAX_EVACUATING_NODES),
    )
    .await
    .expect("the two calls wait for each other");
}

/// A draining node advances only on a `done` report under its own epoch with
/// every evacuation recorded: not on a stale seal's report, a report that
/// isn't done, or an evacuation still waiting. The control advances.
#[tokio::test]
async fn draining_advances_only_on_a_done_report_under_its_epoch() {
    for (case, answer, record, advances) in [
        (
            "control",
            response(Some(5), true, &["a"]),
            EvacuationRecord::Recorded,
            true,
        ),
        (
            "a stale seal",
            response(Some(4), true, &["a"]),
            EvacuationRecord::Recorded,
            false,
        ),
        (
            "not done",
            response(Some(5), false, &["a"]),
            EvacuationRecord::Recorded,
            false,
        ),
        (
            "an evacuation waiting",
            response(Some(5), true, &["a"]),
            EvacuationRecord::NotYet("uploading".into()),
            false,
        ),
    ] {
        let draining = node(Some("draining"), Some(5), false);
        let sink = FakeSink {
            nodes: vec![draining.clone()],
            responses: HashMap::from([((draining.id, false), Ok(answer))]),
            records: HashMap::from([("a".to_owned(), record)]),
            ..FakeSink::default()
        };
        let report = tick(&sink, &FakeTerminator::ok(), DEFAULT_MAX_EVACUATING_NODES).await;
        assert_eq!(report.drained == [draining.id], advances, "{case}");
        let calls = sink.calls();
        assert_eq!(
            calls.iter().any(|call| call.starts_with("advance")),
            advances,
            "{case}: {calls:?}"
        );
        // Cordon, then the call under the row's epoch, then each record,
        // then the report.
        let order: Vec<_> = calls
            .iter()
            .map(|call| call.split(' ').next().unwrap())
            .collect();
        assert_eq!(
            &order[..4],
            ["cordon", "drain_node", "record", "report"],
            "{case}"
        );
        assert!(calls[1].ends_with(" 5 cancel=false"), "{case}: {calls:?}");
    }
}

/// A drained node keeps reporting, so `:terminate` reads a current report,
/// but never advances again.
#[tokio::test]
async fn a_drained_node_keeps_reporting_and_never_advances() {
    let drained = node(Some("drained"), Some(5), false);
    let sink = FakeSink {
        nodes: vec![drained.clone()],
        responses: HashMap::from([((drained.id, false), Ok(response(Some(5), true, &[])))]),
        ..FakeSink::default()
    };
    tick(&sink, &FakeTerminator::ok(), DEFAULT_MAX_EVACUATING_NODES).await;
    let calls = sink.calls();
    assert!(
        calls.iter().any(|call| call.starts_with("report")),
        "{calls:?}"
    );
    assert!(
        !calls.iter().any(|call| call.starts_with("advance")),
        "{calls:?}"
    );
}

/// The report carries what the loop could not record yet.
#[tokio::test]
async fn the_report_names_the_evacuations_not_yet_recorded() {
    let answer = response(Some(5), true, &["a", "b"]);
    let json = report_json(
        &answer,
        &[
            ("a".into(), EvacuationRecord::Recorded),
            ("b".into(), EvacuationRecord::NotYet("uploading".into())),
        ],
    );
    assert_eq!(
        json["unrecorded"],
        serde_json::json!([{"sandbox_id": "b", "reason": "uploading"}])
    );
    assert_eq!(json["sealed_epoch"], 5);
    assert_eq!(json["evacuated"][0]["prior"], "started");
}

/// An undrained node is cancelled until the node no longer holds the seal
/// under its epoch; only then are its columns cleared. A failed call is
/// recorded and retried.
#[tokio::test]
async fn an_undrained_node_is_cancelled_until_unsealed() {
    for (case, answer, clears) in [
        ("unsealed", Ok(response(None, false, &[])), true),
        (
            "sealed under a later epoch",
            Ok(response(Some(6), false, &[])),
            true,
        ),
        ("still sealed", Ok(response(Some(5), false, &[])), false),
        ("unreachable", Err("unreachable".into()), false),
    ] {
        let undrained = node(None, Some(5), true);
        let sink = FakeSink {
            nodes: vec![undrained.clone()],
            responses: HashMap::from([((undrained.id, true), answer)]),
            ..FakeSink::default()
        };
        let report = tick(&sink, &FakeTerminator::ok(), DEFAULT_MAX_EVACUATING_NODES).await;
        assert_eq!(report.cancelled == [undrained.id], clears, "{case}");
        let calls = sink.calls();
        assert!(calls[0].ends_with(" 5 cancel=true"), "{case}: {calls:?}");
        assert!(
            !calls.iter().any(|call| call.starts_with("cordon")),
            "{case}"
        );
    }
    // An undrained row without the mark is left alone.
    let idle = node(None, Some(5), false);
    let sink = FakeSink {
        nodes: vec![idle],
        ..FakeSink::default()
    };
    tick(&sink, &FakeTerminator::ok(), DEFAULT_MAX_EVACUATING_NODES).await;
    assert!(sink.calls().is_empty());
}

/// A terminating node is terminated, then flagged; a failed terminate flags
/// nothing, and a node already flagged is neither terminated nor flagged
/// again.
#[tokio::test]
async fn a_terminating_node_is_terminated_then_recorded_once() {
    for (case, terminator, recorded) in [
        ("terminated", FakeTerminator::ok(), true),
        (
            "failed",
            FakeTerminator {
                result: Err("denied".into()),
                calls: Mutex::default(),
            },
            false,
        ),
    ] {
        let terminating = node(Some("terminating"), Some(5), false);
        let sink = FakeSink {
            nodes: vec![terminating.clone()],
            ..FakeSink::default()
        };
        let report = tick(&sink, &terminator, DEFAULT_MAX_EVACUATING_NODES).await;
        assert_eq!(
            *terminator.calls.lock().unwrap(),
            [terminating.id],
            "{case}"
        );
        assert_eq!(report.terminated == [terminating.id], recorded, "{case}");
        let expected = format!("terminated {} 5", terminating.id);
        assert_eq!(
            sink.calls().contains(&expected),
            recorded,
            "{case}: {:?}",
            sink.calls()
        );
    }

    let already = V2Node {
        terminated: true,
        ..node(Some("terminating"), Some(5), false)
    };
    let sink = FakeSink {
        nodes: vec![already],
        ..FakeSink::default()
    };
    let terminator = FakeTerminator::ok();
    let report = tick(&sink, &terminator, DEFAULT_MAX_EVACUATING_NODES).await;
    assert!(terminator.calls.lock().unwrap().is_empty());
    assert!(report.terminated.is_empty() && sink.calls().is_empty());
}

/// With new v2 drains not admitted, the loop still owns the persisted ones:
/// it drains a draining node to `drained` and cancels an undrained one, and
/// refuses to terminate, leaving the node `terminating` with its error.
#[tokio::test]
async fn the_loop_owns_persisted_drains_without_terminating() {
    let draining = node(Some("draining"), Some(5), false);
    let undrained = node(None, Some(5), true);
    let terminating = node(Some("terminating"), Some(5), false);
    let sink = FakeSink {
        nodes: vec![draining.clone(), undrained.clone(), terminating.clone()],
        responses: HashMap::from([
            ((draining.id, false), Ok(response(Some(5), true, &[]))),
            ((undrained.id, true), Ok(response(None, false, &[]))),
        ]),
        ..FakeSink::default()
    };
    let report = tick(&sink, &V2Terminator::Refuse, DEFAULT_MAX_EVACUATING_NODES).await;
    assert_eq!(report.drained, [draining.id]);
    assert_eq!(report.cancelled, [undrained.id]);
    assert!(report.terminated.is_empty());
    let calls = sink.calls();
    assert!(
        calls
            .iter()
            .any(|call| call.starts_with(&format!("error {}", terminating.id))),
        "{calls:?}"
    );
}

/// A draining node that doesn't answer its `DrainNode` call reads 1 on the
/// unanswered gauge, so a dead node pages as a blocked one does; one that
/// answers reads 0.
#[tokio::test(flavor = "current_thread")]
async fn a_node_that_does_not_answer_shows_on_the_unanswered_gauge() {
    for (case, answer, unanswered) in [
        ("dead", Err("unreachable".into()), 1.0),
        ("answering", Ok(response(Some(5), false, &[])), 0.0),
    ] {
        let draining = node(Some("draining"), Some(5), false);
        let sink = FakeSink {
            nodes: vec![draining.clone()],
            responses: HashMap::from([((draining.id, false), answer)]),
            ..FakeSink::default()
        };
        let capture = pplx_space_metrics_emit::test_capture::capture_values_guard();
        tick(&sink, &FakeTerminator::ok(), DEFAULT_MAX_EVACUATING_NODES).await;
        let values = capture.finish();
        let gauge = values
            .iter()
            .find(|value| value.name == "node_drain.v2.unanswered")
            .map(|value| value.value);
        assert_eq!(gauge, Some(unanswered), "{case}: {values:?}");
    }
}
