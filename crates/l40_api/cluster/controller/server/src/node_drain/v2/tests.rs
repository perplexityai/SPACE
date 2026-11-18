//! The v2 loop's decisions against a scripted sink and EC2.

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;

use pplx_space_sbx_mgr_client::proto::DrainEvacuated;

use super::*;

#[derive(Default)]
struct FakeSink {
    nodes: Vec<V2Node>,
    responses: HashMap<(NodeId, bool), Result<DrainNodeResponse, String>>,
    /// A cancel's answer once it names evacuations to forget; the cancel's
    /// own answer when unset.
    after_forget: HashMap<NodeId, DrainNodeResponse>,
    records: HashMap<String, EvacuationRecord>,
    cordon_error: Option<String>,
    calls: Mutex<Vec<String>>,
    /// When set, each `DrainNode` call waits here before it answers.
    barrier: Option<tokio::sync::Barrier>,
    /// The terminate transaction's answer; `Fenced` on the node's recorded
    /// instance when unset.
    fence: Option<Fence>,
    /// Whether a Node of the name exists; none when unset.
    nodes_present: Vec<String>,
    /// The terminated delete's answer; `Deleted` when unset.
    delete: Option<TerminatedDelete>,
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
        self.cordon_error.clone().map_or(Ok(()), Err)
    }

    async fn drain_node(
        &self,
        node_id: NodeId,
        epoch: u64,
        cancel: bool,
        forget: &[String],
    ) -> Result<DrainNodeResponse, String> {
        if forget.is_empty() {
            self.log(format!("drain_node {node_id} {epoch} cancel={cancel}"));
        } else {
            self.log(format!(
                "drain_node {node_id} {epoch} cancel={cancel} forget={}",
                forget.join(",")
            ));
        }
        if let Some(barrier) = &self.barrier {
            barrier.wait().await;
        }
        if !forget.is_empty()
            && let Some(answer) = self.after_forget.get(&node_id)
        {
            return Ok(answer.clone());
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

    async fn release_suspended(
        &self,
        node_id: NodeId,
        epoch: u64,
    ) -> Result<Vec<SandboxId>, String> {
        self.log(format!("release {node_id} {epoch}"));
        Ok(Vec::new())
    }

    async fn fence(&self, node_id: NodeId, epoch: u64) -> Result<Fence, String> {
        self.log(format!("fence {node_id} {epoch}"));
        Ok(self
            .fence
            .clone()
            .unwrap_or_else(|| Fence::Fenced(INSTANCE.to_owned())))
    }

    async fn record_dispatched(
        &self,
        node_id: NodeId,
        epoch: u64,
        instance_id: &str,
    ) -> Result<bool, String> {
        self.log(format!("dispatched {node_id} {epoch} {instance_id}"));
        Ok(true)
    }

    async fn record_terminated_observed(
        &self,
        node_id: NodeId,
        epoch: u64,
        instance_id: &str,
    ) -> Result<bool, String> {
        self.log(format!("observed {node_id} {epoch} {instance_id}"));
        Ok(true)
    }

    async fn node_exists(&self, k8s_node_name: &str) -> Result<bool, String> {
        self.log(format!("node_exists {k8s_node_name}"));
        Ok(self.nodes_present.iter().any(|name| name == k8s_node_name))
    }

    async fn delete_terminated(
        &self,
        node: &V2Node,
        epoch: u64,
        fence: &str,
    ) -> Result<TerminatedDelete, String> {
        self.log(format!("delete {} {epoch} {fence}", node.id));
        Ok(self.delete.clone().unwrap_or(TerminatedDelete::Deleted))
    }

    async fn let_go(&self, node: &V2Node, epoch: u64, trace: &str) -> Result<bool, String> {
        self.log(format!("let_go {} {epoch} {trace}", node.id));
        Ok(true)
    }
}

/// The instance every test node records.
const INSTANCE: &str = "i-0000000000000c0c0";

/// EC2 and Auto Scaling, scripted: each instance answers `running` unless
/// scripted otherwise.
struct FakeAuthority {
    terminates: bool,
    answers: HashMap<String, InstanceEvidence>,
    terminate: Result<(), String>,
    described: Mutex<Vec<String>>,
    terminated: Mutex<Vec<(String, bool)>>,
}

impl FakeAuthority {
    fn ok() -> Self {
        Self {
            terminates: true,
            answers: HashMap::new(),
            terminate: Ok(()),
            described: Mutex::default(),
            terminated: Mutex::default(),
        }
    }

    fn answering(evidence: InstanceEvidence) -> Self {
        Self {
            answers: HashMap::from([(INSTANCE.to_owned(), evidence)]),
            ..Self::ok()
        }
    }

    fn described(&self) -> Vec<String> {
        self.described.lock().unwrap().clone()
    }

    fn terminated(&self) -> Vec<(String, bool)> {
        self.terminated.lock().unwrap().clone()
    }
}

impl V2InstanceAuthority for FakeAuthority {
    fn terminates(&self) -> bool {
        self.terminates
    }

    async fn describe_instance(&self, instance_id: &str) -> InstanceEvidence {
        self.described.lock().unwrap().push(instance_id.to_owned());
        self.answers
            .get(instance_id)
            .cloned()
            .unwrap_or_else(|| InstanceEvidence::Listed("running".to_owned()))
    }

    async fn terminate_instance(
        &self,
        instance_id: &str,
        decrement_capacity: bool,
    ) -> Result<(), String> {
        self.terminated
            .lock()
            .unwrap()
            .push((instance_id.to_owned(), decrement_capacity));
        self.terminate.clone()
    }
}

/// One tick with a fresh backoff, as a controller that just started runs it.
async fn run_tick<S: DrainV2Sink, A: V2InstanceAuthority>(
    sink: &S,
    authority: &A,
    max_evacuating: usize,
) -> V2TickReport {
    tick(sink, authority, max_evacuating, &V2LoopState::default()).await
}

fn node(phase: Option<&str>, epoch: Option<u64>, cancel: bool) -> V2Node {
    let id = NodeId::new();
    V2Node {
        id,
        phase: phase.map(str::to_owned),
        epoch,
        cancel,
        unrecorded_since: BTreeMap::new(),
        waiting: false,
        k8s_node_name: Some(format!(
            "worker-{}.synthetic.internal",
            id.as_uuid().simple()
        )),
        k8s_node_uid: Some("uid".to_owned()),
        instance_id: Some(INSTANCE.to_owned()),
        heartbeat_fresh: true,
        fence: None,
        dispatched: false,
        dispatched_at: None,
        terminated_observed_at: None,
        decrement_capacity: false,
    }
}

/// A `terminating` node fenced on [`INSTANCE`], dispatched or not.
fn fenced(dispatched: bool) -> V2Node {
    V2Node {
        fence: Some(INSTANCE.to_owned()),
        dispatched,
        dispatched_at: dispatched.then(super::unix_now),
        heartbeat_fresh: false,
        ..node(Some("terminating"), Some(5), false)
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
    run_tick(&sink, &FakeAuthority::ok(), DEFAULT_MAX_EVACUATING_NODES).await;
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
    let report = run_tick(&sink, &FakeAuthority::ok(), 2).await;
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

/// A node holds an evacuation slot from its admission until it leaves v2:
/// while it evacuates, and while it is undrained with its cancel not yet
/// finished, letting its running suspends and uploads finish. A waiting
/// drain, cancelled or not, a drained or terminating node, and a row gone,
/// its cancel finished or the row deleted, hold none, so the oldest waiting
/// drain beside it is admitted to the one slot.
#[tokio::test]
async fn a_node_holds_its_slot_until_it_leaves_v2() {
    let waiting = |phase: Option<&str>, cancel: bool| V2Node {
        waiting: true,
        ..node(phase, Some(5), cancel)
    };
    for (case, other, holds) in [
        (
            "evacuating",
            Some(node(Some("draining"), Some(5), false)),
            true,
        ),
        ("cancelling", Some(node(None, Some(5), true)), true),
        ("waiting", Some(waiting(Some("draining"), false)), false),
        ("cancelled while waiting", Some(waiting(None, true)), false),
        (
            "drained",
            Some(node(Some("drained"), Some(5), false)),
            false,
        ),
        ("terminating", Some(fenced(true)), false),
        ("left v2", None, false),
    ] {
        // The oldest drain, so a waiting one beside it comes second.
        let next = V2Node {
            epoch: Some(1),
            ..waiting(Some("draining"), false)
        };
        let nodes: Vec<V2Node> = other.iter().cloned().chain([next.clone()]).collect();
        let sink = FakeSink {
            responses: nodes
                .iter()
                .flat_map(|node| {
                    // A cancel still handing back what it evacuated.
                    let mut answer = response(None, false, &[]);
                    answer
                        .pending
                        .push(pplx_space_sbx_mgr_client::proto::DrainPending {
                            sandbox_id: "a".into(),
                            action: "suspend".into(),
                            ..Default::default()
                        });
                    [
                        ((node.id, false), Ok(response(node.epoch, false, &[]))),
                        ((node.id, true), Ok(answer)),
                    ]
                })
                .collect(),
            nodes,
            ..FakeSink::default()
        };
        let report = run_tick(&sink, &FakeAuthority::ok(), 1).await;
        let admitted: &[NodeId] = if holds { &[] } else { &[next.id] };
        assert_eq!(report.admitted, admitted, "{case}");
        assert_eq!(
            sink.calls()
                .iter()
                .any(|call| call.starts_with(&format!("drain_node {}", next.id))),
            !holds,
            "{case}: {:?}",
            sink.calls()
        );
    }
}

/// A drain cancelled while it still waited for a slot was never sealed or
/// called: one cancel call, which the node answers with nothing to hand
/// back, finishes it in the tick, with no cordon, admission or drain call,
/// and it keeps no other drain from its slot.
#[tokio::test]
async fn a_waiting_drains_cancel_finishes_at_once() {
    let cancelled = V2Node {
        waiting: true,
        ..node(None, Some(10), true)
    };
    let next = V2Node {
        waiting: true,
        ..node(Some("draining"), Some(20), false)
    };
    let sink = FakeSink {
        nodes: vec![cancelled.clone(), next.clone()],
        responses: HashMap::from([
            ((cancelled.id, true), Ok(response(None, false, &[]))),
            ((next.id, false), Ok(response(Some(20), false, &[]))),
        ]),
        ..FakeSink::default()
    };
    let report = run_tick(&sink, &FakeAuthority::ok(), 1).await;
    assert_eq!(report.cancelled, [cancelled.id]);
    assert_eq!(report.admitted, [next.id]);
    let calls: Vec<_> = sink
        .calls()
        .into_iter()
        .filter(|call| call.contains(&cancelled.id.to_string()))
        .collect();
    assert_eq!(
        calls.first().map(String::as_str),
        Some(format!("drain_node {} 10 cancel=true", cancelled.id).as_str()),
        "{calls:?}"
    );
    assert_eq!(
        calls
            .iter()
            .filter(|call| call.starts_with("drain_node"))
            .count(),
        1,
        "{calls:?}"
    );
    assert!(
        calls
            .last()
            .is_some_and(|call| call.starts_with("finish_cancel")),
        "{calls:?}"
    );
    assert!(
        !calls
            .iter()
            .any(|call| call.starts_with("cordon") || call.starts_with("admit")),
        "{calls:?}"
    );
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
        run_tick(&sink, &FakeAuthority::ok(), DEFAULT_MAX_EVACUATING_NODES),
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
        let report = run_tick(&sink, &FakeAuthority::ok(), DEFAULT_MAX_EVACUATING_NODES).await;
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
    run_tick(&sink, &FakeAuthority::ok(), DEFAULT_MAX_EVACUATING_NODES).await;
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
        &Recording {
            records: vec![
                ("a".into(), EvacuationRecord::Recorded),
                ("b".into(), EvacuationRecord::NotYet("uploading".into())),
            ],
            since: BTreeMap::from([("b".into(), 1_000)]),
            now: 1_060,
        },
    );
    assert_eq!(
        json["unrecorded"],
        serde_json::json!([{"sandbox_id": "b", "reason": "uploading", "since": 1_000}])
    );
    assert_eq!(json["blocked"], serde_json::json!([]));
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
        let report = run_tick(&sink, &FakeAuthority::ok(), DEFAULT_MAX_EVACUATING_NODES).await;
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
    run_tick(&sink, &FakeAuthority::ok(), DEFAULT_MAX_EVACUATING_NODES).await;
    assert!(sink.calls().is_empty());
}

/// With new v2 drains not admitted, the loop still owns the persisted ones:
/// it drains a draining node to `drained` and cancels an undrained one. With
/// no terminator that terminates, it never fences a `terminating` node,
/// which waits unfenced with the refusal as its error.
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
    let report = run_tick(&sink, &V2Terminator::Refuse, DEFAULT_MAX_EVACUATING_NODES).await;
    assert_eq!(report.drained, [draining.id]);
    assert_eq!(report.cancelled, [undrained.id]);
    assert!(report.fenced.is_empty());
    let calls = sink.calls();
    assert!(
        calls.contains(&format!("error {} {REFUSED_TERMINATE}", terminating.id)),
        "{calls:?}"
    );
    assert!(
        !calls.iter().any(|call| call.starts_with("fence ")),
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
        run_tick(&sink, &FakeAuthority::ok(), DEFAULT_MAX_EVACUATING_NODES).await;
        let values = capture.finish();
        let gauge = values
            .iter()
            .find(|value| value.name == "node_drain.v2.unanswered")
            .map(|value| value.value);
        assert_eq!(gauge, Some(unanswered), "{case}: {values:?}");
    }
}

/// A node that leaves the loop's listing, whatever took its row out of the
/// drain (an operator's let-go, a registration that took the row), has its
/// v2 gauges zeroed on the next tick, once; a node still listed goes on
/// publishing its own.
#[tokio::test(flavor = "current_thread")]
async fn a_node_that_leaves_the_listing_has_its_v2_gauges_zeroed() {
    let left = node(Some("draining"), Some(5), false);
    let stays = node(Some("draining"), Some(6), false);
    let sink = |nodes: Vec<V2Node>| FakeSink {
        responses: nodes
            .iter()
            .map(|node| ((node.id, false), Err("unreachable".to_owned())))
            .collect(),
        nodes,
        ..FakeSink::default()
    };
    let both = sink(vec![left.clone(), stays.clone()]);
    let only = sink(vec![stays.clone()]);
    let state = V2LoopState::default();
    let gauges = |values: &[pplx_space_metrics_emit::test_capture::CapturedValue],
                  node: &V2Node| {
        values
            .iter()
            .filter(|value| value.tags.contains(&format!("node:{}", node.id)))
            .map(|value| (value.name.clone(), value.value))
            .collect::<Vec<_>>()
    };
    tick(
        &both,
        &FakeAuthority::ok(),
        DEFAULT_MAX_EVACUATING_NODES,
        &state,
    )
    .await;
    for (case, cleared) in [("left", true), ("already cleared", false)] {
        let capture = pplx_space_metrics_emit::test_capture::capture_values_guard();
        tick(
            &only,
            &FakeAuthority::ok(),
            DEFAULT_MAX_EVACUATING_NODES,
            &state,
        )
        .await;
        let values = capture.finish();
        let zeroed: Vec<(String, f64)> = if cleared {
            ["unanswered", "blocked", "pending"]
                .map(|name| (format!("node_drain.v2.{name}"), 0.0))
                .to_vec()
        } else {
            Vec::new()
        };
        assert_eq!(gauges(&values, &left), zeroed, "{case}");
        assert_eq!(
            gauges(&values, &stays),
            [("node_drain.v2.unanswered".to_owned(), 1.0)],
            "{case}"
        );
    }
}

/// An undrained node hands back what the drain evacuated: each sandbox it
/// reports is recorded, the recorded ones are named for the node to forget,
/// and the columns clear only once the node, unsealed, has nothing left to
/// hand back. One not recorded yet, or work still pending, keeps the cancel
/// open. Once it clears, the node's v2 gauges read 0.
#[tokio::test(flavor = "current_thread")]
async fn an_undrained_node_hands_back_its_evacuations_before_the_cancel_clears() {
    let pending = DrainNodeResponse {
        pending: vec![pplx_space_sbx_mgr_client::proto::DrainPending {
            sandbox_id: "c".into(),
            action: "upload".into(),
            ..Default::default()
        }],
        ..response(None, false, &[])
    };
    for (case, answer, after_forget, record_b, clears) in [
        (
            "all recorded",
            response(None, false, &["a", "b"]),
            response(None, true, &[]),
            EvacuationRecord::Recorded,
            true,
        ),
        (
            "one not recorded yet",
            response(None, false, &["a", "b"]),
            response(None, false, &["b"]),
            EvacuationRecord::NotYet("the catalog doesn't hold the artifact as ready yet".into()),
            false,
        ),
        (
            "an upload pending",
            response(None, false, &["a"]),
            pending.clone(),
            EvacuationRecord::Recorded,
            false,
        ),
    ] {
        let undrained = node(None, Some(5), true);
        let sink = FakeSink {
            nodes: vec![undrained.clone()],
            responses: HashMap::from([((undrained.id, true), Ok(answer))]),
            after_forget: HashMap::from([(undrained.id, after_forget)]),
            records: HashMap::from([("b".to_owned(), record_b.clone())]),
            ..FakeSink::default()
        };
        let capture = pplx_space_metrics_emit::test_capture::capture_values_guard();
        let report = run_tick(&sink, &FakeAuthority::ok(), DEFAULT_MAX_EVACUATING_NODES).await;
        let gauges = capture.finish();
        assert_eq!(report.cancelled == [undrained.id], clears, "{case}");
        let calls = sink.calls();
        let recorded = if record_b.is_recorded() { "a,b" } else { "a" };
        let recorded = if case == "an upload pending" {
            "a"
        } else {
            recorded
        };
        assert!(
            calls.contains(&format!(
                "drain_node {} 5 cancel=true forget={recorded}",
                undrained.id
            )),
            "{case}: {calls:?}"
        );
        assert_eq!(
            calls.iter().any(|call| call.starts_with("finish_cancel")),
            clears,
            "{case}: {calls:?}"
        );
        if clears {
            let v2: Vec<_> = gauges
                .iter()
                .filter(|value| value.name.starts_with("node_drain.v2."))
                .map(|value| (value.name.as_str(), value.value))
                .collect();
            assert_eq!(
                v2,
                [
                    ("node_drain.v2.unanswered", 0.0),
                    ("node_drain.v2.blocked", 0.0),
                    ("node_drain.v2.pending", 0.0),
                ],
                "{case}"
            );
        }
    }
}

/// A cordon that fails keeps its error past the report, which clears the
/// last error; one that succeeds leaves none.
#[tokio::test]
async fn a_failed_cordon_keeps_its_error_until_a_cordon_succeeds() {
    for (case, cordon_error, keeps) in [
        ("failed", Some("node unreachable".to_owned()), true),
        ("converged", None, false),
    ] {
        let draining = node(Some("draining"), Some(5), false);
        let sink = FakeSink {
            nodes: vec![draining.clone()],
            responses: HashMap::from([((draining.id, false), Ok(response(Some(5), false, &[])))]),
            cordon_error,
            ..FakeSink::default()
        };
        run_tick(&sink, &FakeAuthority::ok(), DEFAULT_MAX_EVACUATING_NODES).await;
        let calls = sink.calls();
        let report = calls
            .iter()
            .position(|call| call.starts_with("report"))
            .expect("a report");
        assert_eq!(
            calls[report..].iter().any(|call| call
                == &format!(
                    "error {} cordon convergence failed: node unreachable",
                    draining.id
                )),
            keeps,
            "{case}: {calls:?}"
        );
    }
}

/// An evacuation unrecorded since a stored report longer than the bound is
/// reported `blocked` and counted on the blocked gauge, its clock carried
/// from that report; one unrecorded within the bound stays pending.
#[tokio::test(flavor = "current_thread")]
async fn an_evacuation_unrecorded_past_the_bound_is_blocked() {
    let bound = i64::try_from(UNRECORDED_BOUND.as_secs()).unwrap();
    for (case, since_ago, blocked) in [
        ("past the bound", bound + 60, true),
        ("within it", bound - 60, false),
    ] {
        let since = unix_now() - since_ago;
        let draining = V2Node {
            unrecorded_since: BTreeMap::from([("a".to_owned(), since)]),
            ..node(Some("draining"), Some(5), false)
        };
        let sink = FakeSink {
            nodes: vec![draining.clone()],
            responses: HashMap::from([(
                (draining.id, false),
                Ok(response(Some(5), false, &["a"])),
            )]),
            records: HashMap::from([(
                "a".to_owned(),
                EvacuationRecord::NotYet(
                    "the row runs nowhere the control plane knows of yet".into(),
                ),
            )]),
            ..FakeSink::default()
        };
        let capture = pplx_space_metrics_emit::test_capture::capture_values_guard();
        run_tick(&sink, &FakeAuthority::ok(), DEFAULT_MAX_EVACUATING_NODES).await;
        let gauges = capture.finish();
        let stored = sink
            .calls()
            .into_iter()
            .find_map(|call| {
                call.strip_prefix(&format!("report {} 5 ", draining.id))
                    .map(str::to_owned)
            })
            .expect("a report");
        let stored: serde_json::Value = serde_json::from_str(&stored).unwrap();
        assert_eq!(stored["unrecorded"][0]["since"], since, "{case}");
        let reasons: Vec<_> = stored["blocked"]
            .as_array()
            .unwrap()
            .iter()
            .map(|entry| entry["reason"].as_str().unwrap().to_owned())
            .collect();
        assert_eq!(
            reasons,
            if blocked {
                vec![
                    "not recorded for over 15 minutes: the row runs nowhere the control plane \
                     knows of yet"
                        .to_owned(),
                ]
            } else {
                vec![]
            },
            "{case}"
        );
        let gauge = |name: &str| {
            gauges
                .iter()
                .find(|value| value.name == name)
                .map(|value| value.value)
        };
        assert_eq!(
            (
                gauge("node_drain.v2.blocked"),
                gauge("node_drain.v2.pending")
            ),
            if blocked {
                (Some(1.0), Some(0.0))
            } else {
                (Some(0.0), Some(1.0))
            },
            "{case}"
        );
    }
}

/// The gauges count each row once, and the report blocks an overdue
/// evacuation only when the node doesn't block its sandbox already: a row
/// the node reports twice pending (an activation and the window's `queued`)
/// is one pending row, a row the node blocks whose evacuation is also
/// overdue is one blocked row with the node's reason alone, and a row both
/// pending and blocked counts as blocked. The controls: a row only
/// unrecorded past the bound, one within it, one the window holds for disk,
/// and a blocked entry without a sandbox.
#[test]
fn a_row_is_reported_once_however_many_reasons_name_it() {
    use pplx_space_sbx_mgr_client::proto::{DrainBlocked, DrainPending};
    let pending = |sandbox_id: &str, action: &str| DrainPending {
        sandbox_id: sandbox_id.into(),
        action: action.into(),
        ..Default::default()
    };
    let blocked = |sandbox_id: Option<&str>, reason: &str| DrainBlocked {
        sandbox_id: sandbox_id.map(str::to_owned),
        reason: reason.into(),
    };
    let answer = DrainNodeResponse {
        pending: vec![
            pending("queued", "activation"),
            pending("queued", "queued"),
            pending("disk", "disk"),
            pending("both", "upload"),
        ],
        blocked: vec![
            blocked(Some("both"), "upload failed 3 times: denied"),
            blocked(Some("blocked"), "the cache disk hasn't had room"),
            blocked(None, "orphan inventory scan failed"),
        ],
        ..response(Some(5), false, &["blocked", "late", "fresh"])
    };
    let bound = i64::try_from(UNRECORDED_BOUND.as_secs()).unwrap();
    let not_yet = || EvacuationRecord::NotYet("not projected yet".into());
    let recording = Recording {
        records: vec![
            ("blocked".into(), not_yet()),
            ("late".into(), not_yet()),
            ("fresh".into(), not_yet()),
        ],
        since: BTreeMap::from([
            ("blocked".into(), 0),
            ("late".into(), 0),
            ("fresh".into(), bound),
        ]),
        now: bound + 60,
    };

    assert_eq!(outstanding_rows(&answer, &recording), (4, 3));
    let json = report_json(&answer, &recording);
    let named: Vec<_> = json["blocked"]
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| entry["sandbox_id"].as_str().unwrap_or("-").to_owned())
        .collect();
    assert_eq!(named, ["both", "blocked", "-", "late"]);
    assert_eq!(json["unrecorded"].as_array().unwrap().len(), 3);
}

/// Whether the sink saw a call starting `prefix` for `node`.
fn called(sink: &FakeSink, prefix: &str, node: &V2Node) -> bool {
    sink.calls()
        .iter()
        .any(|call| call.starts_with(&format!("{prefix} {}", node.id)))
}

/// A `drained` node's tick releases the durably suspended rows still naming
/// it; a `draining` one's doesn't.
#[tokio::test]
async fn a_drained_tick_releases_and_a_draining_one_does_not() {
    let drained = node(Some("drained"), Some(5), false);
    let draining = node(Some("draining"), Some(6), false);
    let sink = FakeSink {
        responses: HashMap::from([
            ((drained.id, false), Ok(response(Some(5), true, &[]))),
            ((draining.id, false), Ok(response(Some(6), false, &[]))),
        ]),
        nodes: vec![drained.clone(), draining.clone()],
        ..FakeSink::default()
    };
    run_tick(&sink, &FakeAuthority::ok(), DEFAULT_MAX_EVACUATING_NODES).await;
    assert!(called(&sink, "release", &drained), "{:?}", sink.calls());
    assert!(!called(&sink, "release", &draining), "{:?}", sink.calls());
}

/// An unfenced `terminating` node runs the terminate transaction, and
/// nothing else: no cordon and no node call, so the loop writes no
/// Kubernetes object there. A transaction the require-empty predicate holds
/// fences nothing.
#[tokio::test]
async fn an_unfenced_terminating_node_runs_only_the_terminate_transaction() {
    for (case, answer, fences) in [
        ("fenced", None, true),
        (
            "held",
            Some(Fence::Held("terminate held".to_owned())),
            false,
        ),
        ("moved", Some(Fence::Moved), false),
    ] {
        let terminating = node(Some("terminating"), Some(5), false);
        let sink = FakeSink {
            nodes: vec![terminating.clone()],
            fence: answer,
            ..FakeSink::default()
        };
        let authority = FakeAuthority::ok();
        let report = run_tick(&sink, &authority, DEFAULT_MAX_EVACUATING_NODES).await;
        assert_eq!(report.fenced == [terminating.id], fences, "{case}");
        assert_eq!(
            sink.calls(),
            [format!("fence {} 5", terminating.id)],
            "{case}"
        );
        assert!(authority.described().is_empty() && authority.terminated().is_empty());
    }
}

/// A fenced node with no dispatch is dispatched only after EC2 lists the
/// fenced instance in a state other than `shutting-down` or `terminated`:
/// by the fenced id, with the node's decrement, recording `dispatched_at`.
/// A `shutting-down`, unlisted or unknown answer dispatches nothing and
/// says why; a `terminated` one lets the node go, since the drain can't
/// claim the termination.
#[tokio::test]
async fn a_fenced_node_is_dispatched_only_after_ec2_answers() {
    for (case, evidence, dispatches, lets_go) in [
        (
            "running",
            InstanceEvidence::Listed("running".into()),
            true,
            false,
        ),
        (
            "stopped",
            InstanceEvidence::Listed("stopped".into()),
            true,
            false,
        ),
        ("not listed", InstanceEvidence::NotListed, false, false),
        (
            "shutting-down",
            InstanceEvidence::ShuttingDown,
            false,
            false,
        ),
        (
            "unknown",
            InstanceEvidence::Unknown("timeout".into()),
            false,
            false,
        ),
        ("terminated", InstanceEvidence::Terminated, false, true),
    ] {
        let node = V2Node {
            decrement_capacity: true,
            ..fenced(false)
        };
        let sink = FakeSink {
            nodes: vec![node.clone()],
            ..FakeSink::default()
        };
        let authority = FakeAuthority::answering(evidence);
        let report = run_tick(&sink, &authority, DEFAULT_MAX_EVACUATING_NODES).await;
        assert_eq!(authority.described(), [INSTANCE], "{case}");
        let expected: &[(String, bool)] = if dispatches {
            &[(INSTANCE.to_owned(), true)]
        } else {
            &[]
        };
        assert_eq!(authority.terminated(), expected, "{case}");
        assert_eq!(report.dispatched == [node.id], dispatches, "{case}");
        assert_eq!(
            sink.calls()
                .contains(&format!("dispatched {} 5 {INSTANCE}", node.id)),
            dispatches,
            "{case}"
        );
        assert_eq!(report.let_go == [node.id], lets_go, "{case}");
        if lets_go {
            assert!(
                sink.calls().contains(&format!(
                    "let_go {} 5 v2 drain let go: fenced instance {INSTANCE} terminated; \
                     dispatch unconfirmed",
                    node.id
                )),
                "{:?}",
                sink.calls()
            );
        }
        assert_eq!(
            sink.calls()
                .iter()
                .any(|call| call.starts_with(&format!("error {}", node.id))
                    && call.ends_with("not dispatching its termination")),
            !dispatches && !lets_go,
            "{case}: {:?}",
            sink.calls()
        );
        assert!(!called(&sink, "delete", &node), "{case}");
    }
}

/// Once EC2 has been seen to list the fenced instance `terminated`, an
/// instance it no longer lists reads as terminated: an undispatched node is
/// let go, as on `terminated`, and dispatches nothing.
#[tokio::test]
async fn an_unlisted_instance_after_a_recorded_terminated_reads_terminated() {
    let node = V2Node {
        terminated_observed_at: Some(super::unix_now()),
        ..fenced(false)
    };
    let sink = FakeSink {
        nodes: vec![node.clone()],
        ..FakeSink::default()
    };
    let authority = FakeAuthority::answering(InstanceEvidence::NotListed);
    let report = run_tick(&sink, &authority, DEFAULT_MAX_EVACUATING_NODES).await;
    assert_eq!(report.let_go, [node.id], "{:?}", sink.calls());
    assert!(authority.terminated().is_empty());
    assert!(!called(&sink, "observed", &node), "{:?}", sink.calls());
}

/// Once `dispatched_at` is recorded the loop never dispatches again,
/// whatever EC2 answers; past the bound it says the termination was
/// accepted and the instance still runs. A row an earlier build's
/// terminator marked `terminated` is dispatched with no time, and is never
/// dispatched again either. No answer from EC2 is said on every check,
/// with the dispatch's age only where the row records one.
#[tokio::test]
async fn a_dispatched_node_is_never_dispatched_again() {
    let past_bound = super::unix_now() - i64::try_from(TERMINATE_BOUND.as_secs()).unwrap() - 60;
    let unknown = || InstanceEvidence::Unknown("the EC2 call timed out".to_owned());
    let running = || InstanceEvidence::Listed("running".to_owned());
    for (case, dispatched_at, evidence, says) in [
        ("just dispatched", Some(super::unix_now()), running(), None),
        (
            "past the bound",
            Some(past_bound),
            running(),
            Some("EC2 still answers running after 16 minutes"),
        ),
        (
            "an earlier build's terminated marker",
            None,
            running(),
            None,
        ),
        (
            "just dispatched, no answer",
            Some(super::unix_now()),
            unknown(),
            Some("was dispatched 0 minutes ago; EC2 answers unknown (the EC2 call timed out)"),
        ),
        (
            "past the bound, no answer",
            Some(past_bound),
            unknown(),
            Some("was dispatched 16 minutes ago; EC2 answers unknown"),
        ),
        (
            "an earlier build's terminated marker, no answer",
            None,
            unknown(),
            Some(
                "the termination of fenced instance i-0000000000000c0c0 was dispatched; EC2 \
                 answers unknown (the EC2 call timed out)",
            ),
        ),
    ] {
        let node = V2Node {
            dispatched_at,
            ..fenced(true)
        };
        let sink = FakeSink {
            nodes: vec![node.clone()],
            ..FakeSink::default()
        };
        let authority = FakeAuthority::answering(evidence);
        let report = run_tick(&sink, &authority, DEFAULT_MAX_EVACUATING_NODES).await;
        assert!(authority.terminated().is_empty(), "{case}");
        assert_eq!(report, V2TickReport::default(), "{case}");
        let errors: Vec<String> = sink
            .calls()
            .into_iter()
            .filter(|call| call.starts_with(&format!("error {}", node.id)))
            .collect();
        match says {
            Some(says) => assert!(
                errors.len() == 1 && errors[0].contains(says),
                "{case}: {errors:?}"
            ),
            None => assert!(errors.is_empty(), "{case}: {errors:?}"),
        }
    }
}

/// A refused dispatch counts on `node_drain.v2.terminate_failures` and
/// records its error; the backoff keeps the next tick from asking again at
/// once, and a later one asks again.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_refused_dispatch_counts_and_backs_off() {
    let node = fenced(false);
    let sink = FakeSink {
        nodes: vec![node.clone()],
        ..FakeSink::default()
    };
    let authority = FakeAuthority {
        terminate: Err("denied".into()),
        ..FakeAuthority::ok()
    };
    let state = V2LoopState::default();
    let counters = pplx_space_metrics_emit::test_capture::capture_counters_guard();
    tick(&sink, &authority, DEFAULT_MAX_EVACUATING_NODES, &state).await;
    let counted = counters.finish();
    assert!(
        counted
            .iter()
            .any(|counter| counter.name == "node_drain.v2.terminate_failures"
                && counter.tags.contains(&format!("node:{}", node.id))),
        "{counted:?}"
    );
    assert!(
        sink.calls()
            .iter()
            .any(|call| call.contains(&format!("terminating fenced instance {INSTANCE} failed"))),
        "{:?}",
        sink.calls()
    );
    tick(&sink, &authority, DEFAULT_MAX_EVACUATING_NODES, &state).await;
    assert_eq!(
        authority.terminated().len(),
        1,
        "the backoff holds the retry"
    );
    tokio::time::advance(DISPATCH_BACKOFF_BASE).await;
    tick(&sink, &authority, DEFAULT_MAX_EVACUATING_NODES, &state).await;
    assert_eq!(authority.terminated().len(), 2, "the backoff ran out");
}

/// A dispatched node whose fenced instance EC2 says is terminated is
/// deleted once a direct GET finds its Node gone. A Node still there waits
/// for it; an instance not yet terminated asks nothing of Kubernetes; a
/// guard that refuses the delete lets the node go; a row a registration
/// took has nothing left to do. The first `terminated` is recorded before
/// anything else; after it, an instance EC2 no longer lists is deleted as a
/// terminated one, and without it an unlisted instance never is, saying
/// why. A row an earlier build's terminator marked `terminated` is deleted
/// as a dispatched one.
#[tokio::test]
async fn a_dispatched_node_is_deleted_once_terminated_with_its_node_gone() {
    struct Case {
        name: &'static str,
        evidence: InstanceEvidence,
        observed: bool,
        legacy: bool,
        node_present: bool,
        delete: Option<TerminatedDelete>,
        looks_up: bool,
        deletes: Option<bool>,
    }
    let case = |name, evidence| Case {
        name,
        evidence,
        observed: false,
        legacy: false,
        node_present: false,
        delete: None,
        looks_up: true,
        deletes: Some(true),
    };
    let cases = [
        case("deleted", InstanceEvidence::Terminated),
        Case {
            node_present: true,
            deletes: None,
            ..case("node lingers", InstanceEvidence::Terminated)
        },
        Case {
            looks_up: false,
            deletes: None,
            ..case("still running", InstanceEvidence::Listed("running".into()))
        },
        Case {
            looks_up: false,
            deletes: None,
            ..case("shutting down", InstanceEvidence::ShuttingDown)
        },
        Case {
            delete: Some(TerminatedDelete::LetGo("trace".into())),
            deletes: Some(false),
            ..case("guard refused", InstanceEvidence::Terminated)
        },
        Case {
            delete: Some(TerminatedDelete::Unfenced),
            deletes: Some(false),
            ..case("unfenced", InstanceEvidence::Terminated)
        },
        Case {
            observed: true,
            ..case("terminated again", InstanceEvidence::Terminated)
        },
        Case {
            observed: true,
            ..case(
                "not listed after a recorded terminated",
                InstanceEvidence::NotListed,
            )
        },
        Case {
            looks_up: false,
            deletes: None,
            ..case(
                "not listed, never seen terminated",
                InstanceEvidence::NotListed,
            )
        },
        Case {
            legacy: true,
            ..case(
                "an earlier build's terminated marker",
                InstanceEvidence::Terminated,
            )
        },
    ];
    for case in cases {
        let name = case.name;
        let node = V2Node {
            terminated_observed_at: case.observed.then(super::unix_now),
            dispatched_at: if case.legacy {
                None
            } else {
                Some(super::unix_now())
            },
            ..fenced(true)
        };
        let k8s_name = node.k8s_node_name.clone().unwrap();
        let sink = FakeSink {
            nodes: vec![node.clone()],
            nodes_present: if case.node_present {
                vec![k8s_name.clone()]
            } else {
                Vec::new()
            },
            delete: case.delete.clone(),
            ..FakeSink::default()
        };
        let authority = FakeAuthority::answering(case.evidence.clone());
        let report = run_tick(&sink, &authority, DEFAULT_MAX_EVACUATING_NODES).await;
        let calls = sink.calls();
        assert_eq!(
            calls.contains(&format!("node_exists {k8s_name}")),
            case.looks_up,
            "{name}: {calls:?}"
        );
        assert_eq!(
            called(&sink, "delete", &node),
            case.deletes.is_some(),
            "{name}"
        );
        assert_eq!(
            report.deleted == [node.id],
            case.deletes == Some(true),
            "{name}"
        );
        assert_eq!(
            report.let_go == [node.id],
            matches!(case.delete, Some(TerminatedDelete::LetGo(_))),
            "{name}"
        );
        assert!(authority.terminated().is_empty(), "{name}");
        let records = case.evidence == InstanceEvidence::Terminated && !case.observed;
        let observed = format!("observed {} 5 {INSTANCE}", node.id);
        assert_eq!(
            calls.first() == Some(&observed),
            records,
            "{name}: {calls:?}"
        );
        assert_eq!(calls.contains(&observed), records, "{name}: {calls:?}");
        assert_eq!(
            calls
                .iter()
                .any(|call| call
                    .ends_with("was never seen to list it terminated; not deleting the row")),
            name == "not listed, never seen terminated",
            "{name}: {calls:?}"
        );
    }
}

/// Every unfenced v2 state lets go once EC2 answers `terminated` for the
/// recorded instance of a node whose heartbeat is stale, with the trace
/// naming it, and calls the node for nothing. Any other answer only records
/// what EC2 said, and a fresh heartbeat asks EC2 nothing.
#[tokio::test]
async fn every_unfenced_v2_state_lets_go_once_ec2_says_terminated() {
    type Build = fn() -> V2Node;
    let states: [(&str, Build); 5] = [
        ("terminating", || node(Some("terminating"), Some(5), false)),
        ("drained", || node(Some("drained"), Some(5), false)),
        ("draining", || node(Some("draining"), Some(5), false)),
        ("waiting", || V2Node {
            waiting: true,
            ..node(Some("draining"), Some(5), false)
        }),
        ("cancel pending", || node(None, Some(5), true)),
    ];
    for (state, build) in states {
        for (answer, evidence, lets_go) in [
            ("terminated", Some(InstanceEvidence::Terminated), true),
            ("shutting-down", Some(InstanceEvidence::ShuttingDown), false),
            (
                "stopped",
                Some(InstanceEvidence::Listed("stopped".into())),
                false,
            ),
            ("not listed", Some(InstanceEvidence::NotListed), false),
            (
                "unknown",
                Some(InstanceEvidence::Unknown("timeout".into())),
                false,
            ),
            ("fresh heartbeat", None, false),
        ] {
            let case = format!("{state} {answer}");
            let node = V2Node {
                heartbeat_fresh: evidence.is_none(),
                ..build()
            };
            let sink = FakeSink {
                nodes: vec![node.clone()],
                fence: Some(Fence::Moved),
                ..FakeSink::default()
            };
            let authority = FakeAuthority::answering(
                evidence
                    .clone()
                    .unwrap_or(InstanceEvidence::Listed("running".into())),
            );
            let report = run_tick(&sink, &authority, DEFAULT_MAX_EVACUATING_NODES).await;
            assert_eq!(report.let_go == [node.id], lets_go, "{case}");
            let trace = format!(
                "let_go {} 5 v2 drain let go: instance {INSTANCE} terminated outside the drain",
                node.id
            );
            assert_eq!(
                sink.calls().contains(&trace),
                lets_go,
                "{case}: {:?}",
                sink.calls()
            );
            if lets_go {
                assert_eq!(sink.calls(), [trace], "{case}");
            }
            assert_eq!(
                authority.described().is_empty(),
                evidence.is_none(),
                "{case}"
            );
            assert_eq!(
                sink.calls().iter().any(|call| call.contains("EC2 answers")),
                evidence.is_some() && !lets_go,
                "{case}: {:?}",
                sink.calls()
            );
            assert!(authority.terminated().is_empty(), "{case}");
        }
    }
}

/// A waiting drain is admitted only while its own heartbeat is fresh, and a
/// node holding a slot frees it early only when EC2 lists its instance
/// `shutting-down` or `terminated`: a stopped or unlisted instance, or no
/// answer, keeps it.
#[tokio::test]
async fn slots_follow_the_heartbeat_and_irreversible_ec2_evidence() {
    let waiting = |epoch: u64, fresh: bool| V2Node {
        waiting: true,
        heartbeat_fresh: fresh,
        ..node(Some("draining"), Some(epoch), false)
    };
    let stale = waiting(1, false);
    let fresh = waiting(2, true);
    let sink = FakeSink {
        responses: HashMap::from([((fresh.id, false), Ok(response(Some(2), false, &[])))]),
        nodes: vec![stale.clone(), fresh.clone()],
        ..FakeSink::default()
    };
    let report = run_tick(&sink, &FakeAuthority::ok(), 1).await;
    assert_eq!(report.admitted, [fresh.id], "only a fresh waiting node");

    for (case, evidence, frees) in [
        ("shutting-down", InstanceEvidence::ShuttingDown, true),
        ("terminated", InstanceEvidence::Terminated, true),
        ("stopped", InstanceEvidence::Listed("stopped".into()), false),
        ("not listed", InstanceEvidence::NotListed, false),
        (
            "unknown",
            InstanceEvidence::Unknown("timeout".into()),
            false,
        ),
    ] {
        for holder in [
            V2Node {
                heartbeat_fresh: false,
                ..node(Some("draining"), Some(1), false)
            },
            V2Node {
                heartbeat_fresh: false,
                ..node(None, Some(1), true)
            },
        ] {
            let next = waiting(9, true);
            let sink = FakeSink {
                responses: HashMap::from([((next.id, false), Ok(response(Some(9), false, &[])))]),
                nodes: vec![holder.clone(), next.clone()],
                ..FakeSink::default()
            };
            let report = run_tick(&sink, &FakeAuthority::answering(evidence.clone()), 1).await;
            assert_eq!(
                report.admitted == [next.id],
                frees,
                "{case} {:?}",
                holder.phase
            );
        }
    }
}
