//! The v2 loop's decisions against a scripted sink and EC2.

use std::collections::HashMap;
use std::sync::Mutex;

use pplx_space_node_registration_store::drain_v2::{StoredV2ArtifactKind, StoredV2Wait};
use pplx_space_sbx_mgr_client::proto::{
    CancelResult, DrainBlocked, DrainEvacuated, DrainPending, DrainReport, DrainWait,
    drain_node_response,
};

use super::*;

#[derive(Default)]
struct FakeSink {
    nodes: Vec<V2Node>,
    /// The answer to each node's `DrainNode`, by whether the call cancels.
    responses: HashMap<(NodeId, bool), Result<DrainNodeResponse, String>>,
    records: HashMap<String, EvacuationRecord>,
    cordon_error: Option<CordonFailure>,
    /// The cancel's finish's answer; `Ok(true)` when unset.
    finish_cancel: Option<Result<bool, String>>,
    calls: Mutex<Vec<String>>,
    /// When set, each `DrainNode` call waits here before it answers.
    barrier: Option<tokio::sync::Barrier>,
    /// When set, the first node's `DrainNode` call waits until the second
    /// node's call opens the gate.
    gate: Option<(NodeId, NodeId, tokio::sync::Notify)>,
    /// When set, every `let_go` answers this error.
    let_go_error: Option<String>,
    /// The terminate transaction's answer; `Fenced` on the node's recorded
    /// instance when unset.
    fence: Option<Fence>,
    /// Whether a Node of the name exists; none when unset.
    nodes_present: Vec<String>,
    /// The instance a present Node's `providerID` names, by Node name; none
    /// when unset.
    provider_instances: HashMap<String, String>,
    /// The terminated delete's answer; `Deleted` when unset.
    delete: Option<TerminatedDelete>,
    /// Every `DrainNode` request, in order.
    requests: Mutex<Vec<DrainNodeRequest>>,
    /// When set, every `DrainNode` call answers with this status.
    refusal: Option<tonic::Status>,
    /// The sandboxes each node's drain left suspended, as the count answers.
    still_suspended: HashMap<NodeId, u64>,
    /// Every report stored, in order.
    reports: Mutex<Vec<StoredV2Report>>,
    /// The running sandboxes and the recorded evacuations each stored report
    /// added, in order.
    added: Mutex<Vec<(Vec<String>, Vec<String>)>>,
    /// The bound of each count of sandboxes left suspended, in seconds, in
    /// order; kept out of `calls`, since every tick makes one.
    counted: Mutex<Vec<u64>>,
}

impl FakeSink {
    fn log(&self, entry: String) {
        self.calls.lock().unwrap().push(entry);
    }

    fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }

    fn requests(&self) -> Vec<DrainNodeRequest> {
        self.requests.lock().unwrap().clone()
    }

    fn added(&self) -> Vec<(Vec<String>, Vec<String>)> {
        self.added.lock().unwrap().clone()
    }

    /// The last report stored.
    fn stored(&self) -> StoredV2Report {
        self.reports
            .lock()
            .unwrap()
            .last()
            .cloned()
            .expect("a stored report")
    }
}

impl DrainV2Sink for FakeSink {
    async fn list_v2_nodes(&self) -> Result<Vec<V2Node>, String> {
        Ok(self.nodes.clone())
    }

    async fn ensure_cordon(&self, node_id: NodeId) -> Result<(), CordonFailure> {
        self.log(format!("cordon {node_id}"));
        self.cordon_error.clone().map_or(Ok(()), Err)
    }

    async fn drain_node(
        &self,
        node_id: NodeId,
        request: DrainNodeRequest,
    ) -> Result<DrainNodeResponse, DrainCallError> {
        self.requests.lock().unwrap().push(request);
        let cancel = matches!(request.op, Some(drain_node_request::Op::Cancel(_)));
        self.log(format!(
            "drain_node {node_id} {} cancel={cancel}",
            request.generation
        ));
        if let Some(barrier) = &self.barrier {
            barrier.wait().await;
        }
        if let Some((waits, opens, gate)) = &self.gate {
            if node_id == *waits {
                gate.notified().await;
            } else if node_id == *opens {
                gate.notify_one();
            }
        }
        if let Some(refusal) = &self.refusal {
            return Err(DrainCallError::status(refusal));
        }
        self.responses
            .get(&(node_id, cancel))
            .cloned()
            .unwrap_or_else(|| Err("unscripted".into()))
            .map_err(DrainCallError::other)
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
        generation: u64,
        report: StoredV2Report,
        running: Vec<String>,
        evacuated: Vec<String>,
    ) -> Result<bool, String> {
        self.log(format!("report {node_id} {generation}"));
        self.reports.lock().unwrap().push(report);
        self.added.lock().unwrap().push((running, evacuated));
        Ok(true)
    }

    async fn advance_drained(&self, node_id: NodeId, generation: u64) -> Result<bool, String> {
        self.log(format!("advance {node_id} {generation}"));
        Ok(true)
    }

    async fn return_drained(
        &self,
        node_id: NodeId,
        generation: u64,
        refused: &str,
    ) -> Result<bool, String> {
        self.log(format!("return {node_id} {generation} {refused}"));
        Ok(true)
    }

    async fn finish_cancel(&self, node_id: NodeId, generation: u64) -> Result<bool, String> {
        self.log(format!("finish_cancel {node_id} {generation}"));
        self.finish_cancel.clone().unwrap_or(Ok(true))
    }

    async fn evacuated_still_suspended(
        &self,
        after: Duration,
    ) -> Result<HashMap<NodeId, u64>, String> {
        self.counted.lock().unwrap().push(after.as_secs());
        Ok(self.still_suspended.clone())
    }

    async fn record_error(&self, node_id: NodeId, message: &str) {
        self.log(format!("error {node_id} {message}"));
    }

    async fn fence(&self, node_id: NodeId, generation: u64) -> Result<Fence, String> {
        self.log(format!("fence {node_id} {generation}"));
        Ok(self
            .fence
            .clone()
            .unwrap_or_else(|| Fence::Fenced(INSTANCE.to_owned())))
    }

    async fn record_dispatched(
        &self,
        node_id: NodeId,
        generation: u64,
        instance_id: &str,
    ) -> Result<bool, String> {
        self.log(format!("dispatched {node_id} {generation} {instance_id}"));
        Ok(true)
    }

    async fn record_terminated_observed(
        &self,
        node_id: NodeId,
        generation: u64,
        instance_id: &str,
    ) -> Result<bool, String> {
        self.log(format!("observed {node_id} {generation} {instance_id}"));
        Ok(true)
    }

    async fn node_exists(&self, k8s_node_name: &str) -> Result<bool, String> {
        self.log(format!("node_exists {k8s_node_name}"));
        Ok(self.nodes_present.iter().any(|name| name == k8s_node_name))
    }

    async fn node_machine(&self, k8s_node_name: &str) -> Result<NodeMachine, String> {
        Ok(
            if self.nodes_present.iter().any(|name| name == k8s_node_name) {
                NodeMachine::Live(self.provider_instances.get(k8s_node_name).cloned())
            } else {
                NodeMachine::Gone
            },
        )
    }

    async fn delete_terminated(
        &self,
        node: &V2Node,
        generation: u64,
        fence: &str,
    ) -> Result<TerminatedDelete, String> {
        self.log(format!("delete {} {generation} {fence}", node.id));
        Ok(self.delete.clone().unwrap_or(TerminatedDelete::Deleted))
    }

    async fn let_go(&self, node: &V2Node, generation: u64, trace: &str) -> Result<bool, String> {
        self.log(format!("let_go {} {generation} {trace}", node.id));
        self.let_go_error.clone().map_or(Ok(true), Err)
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

/// One tick with a fresh state, as a controller that just started runs it.
async fn run_tick<S: DrainV2Sink, A: V2InstanceAuthority>(sink: &S, authority: &A) -> V2TickReport {
    tick(sink, authority, &V2LoopState::default()).await
}

/// The database clock every test node is listed at, in Unix seconds.
const DB_NOW: i64 = 1_800_000_000;

/// A v2 node in `phase` (`None`: a cancel pending) under `generation`,
/// past its deadline, so it evacuates.
fn node(phase: Option<&str>, generation: Option<u64>) -> V2Node {
    let id = NodeId::new();
    V2Node {
        id,
        phase: phase.map(str::to_owned),
        generation,
        deadline_at: Some(DB_NOW),
        evacuate: true,
        report: None,
        k8s_node_name: Some(format!(
            "worker-{}.synthetic.internal",
            id.as_uuid().simple()
        )),
        k8s_node_uid: Some("uid".to_owned()),
        instance_id: Some(INSTANCE.to_owned()),
        heartbeat_fresh: true,
        fence: None,
        dispatched_at: None,
        terminated_observed_at: None,
        decrement_capacity: false,
        db_now: DB_NOW,
    }
}

/// A `terminating` node fenced on [`INSTANCE`], dispatched or not, its
/// heartbeat stale.
fn fenced(dispatched: bool) -> V2Node {
    V2Node {
        fence: Some(INSTANCE.to_owned()),
        dispatched_at: dispatched.then_some(DB_NOW),
        heartbeat_fresh: false,
        ..node(Some("terminating"), Some(5))
    }
}

fn evacuated(sandbox_id: &str) -> DrainEvacuated {
    DrainEvacuated {
        sandbox_id: sandbox_id.to_owned(),
        artifact_kind: pplx_space_sbx_mgr_client::proto::ArtifactKind::Full as i32,
        artifact_id: "artifact".to_owned(),
    }
}

fn pending(sandbox_id: &str, wait: DrainWait) -> DrainPending {
    DrainPending {
        sandbox_id: sandbox_id.into(),
        wait: wait as i32,
        running: wait == DrainWait::Hold,
        ..Default::default()
    }
}

fn blocked(reason: &str) -> DrainBlocked {
    DrainBlocked {
        reason: reason.to_owned(),
    }
}

/// A drain call's answer under `generation`: `report`.
fn answer(generation: u64, report: DrainReport) -> DrainNodeResponse {
    DrainNodeResponse {
        generation,
        result: Some(drain_node_response::Result::Drain(report)),
    }
}

/// A drain call's answer under `generation` reporting each of `evacuated`
/// suspended with a full snapshot, and nothing else.
fn response(generation: u64, evacuated_ids: &[&str]) -> DrainNodeResponse {
    answer(
        generation,
        DrainReport {
            evacuated: evacuated_ids.iter().map(|id| evacuated(id)).collect(),
            ..DrainReport::default()
        },
    )
}

/// A cancel's answer.
fn cancelled(generation: u64) -> DrainNodeResponse {
    DrainNodeResponse {
        generation,
        result: Some(drain_node_response::Result::Cancel(CancelResult {})),
    }
}

/// Whether the sink saw a call starting `prefix` for `node`.
fn called(sink: &FakeSink, prefix: &str, node: &V2Node) -> bool {
    sink.calls()
        .iter()
        .any(|call| call.starts_with(&format!("{prefix} {}", node.id)))
}

/// A row without a generation gets no node call at all: the seal it would
/// set has no generation the row records.
#[tokio::test]
async fn a_v2_node_without_a_generation_gets_no_call() {
    let sink = FakeSink {
        nodes: vec![node(Some("draining"), None)],
        ..FakeSink::default()
    };
    run_tick(&sink, &FakeAuthority::ok()).await;
    assert!(sink.calls().is_empty(), "{:?}", sink.calls());
}

/// A tick calls its nodes at once: two nodes whose `DrainNode` calls each
/// wait for the other both answer, and both drain.
#[tokio::test]
async fn a_tick_calls_its_nodes_at_once() {
    let nodes = vec![
        node(Some("draining"), Some(1)),
        node(Some("draining"), Some(1)),
    ];
    let mut ids: Vec<NodeId> = nodes.iter().map(|node| node.id).collect();
    ids.sort();
    let sink = FakeSink {
        responses: nodes
            .iter()
            .map(|node| ((node.id, false), Ok(response(1, &[]))))
            .collect(),
        nodes,
        barrier: Some(tokio::sync::Barrier::new(2)),
        ..FakeSink::default()
    };
    let mut report = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        run_tick(&sink, &FakeAuthority::ok()),
    )
    .await
    .expect("the two calls wait for each other");
    report.drained.sort();
    assert_eq!(report.drained, ids);
    for id in ids {
        assert!(
            sink.calls()
                .contains(&format!("drain_node {id} 1 cancel=false")),
            "{:?}",
            sink.calls()
        );
    }
}

/// A tick's calls don't wait in order: a node's `DrainNode` call that waits
/// for one listed `NODE_CONCURRENCY` places later doesn't keep that one
/// from starting, and every node drains.
#[tokio::test]
async fn a_slow_node_holds_no_slot_from_a_later_one() {
    let nodes: Vec<V2Node> = (0..=NODE_CONCURRENCY)
        .map(|_| node(Some("draining"), Some(1)))
        .collect();
    let first = nodes.first().expect("a first node").id;
    let last = nodes.last().expect("a last node").id;
    let mut ids: Vec<NodeId> = nodes.iter().map(|node| node.id).collect();
    ids.sort();
    let sink = FakeSink {
        responses: nodes
            .iter()
            .map(|node| ((node.id, false), Ok(response(1, &[]))))
            .collect(),
        nodes,
        gate: Some((first, last, tokio::sync::Notify::new())),
        ..FakeSink::default()
    };
    let mut report = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        run_tick(&sink, &FakeAuthority::ok()),
    )
    .await
    .expect("the last node's call opens the first's");
    report.drained.sort();
    assert_eq!(report.drained, ids);
}

/// A draining node advances once its report under its own generation lists
/// nothing pending or blocked and every evacuation is recorded: not while
/// anything is pending or blocked, or an evacuation still waits. The calls
/// come in order: the cordon, the call under the row's generation, each
/// record, then the report.
#[tokio::test]
async fn draining_advances_only_on_a_finished_report_under_its_generation() {
    for (case, report, record, advances) in [
        (
            "control",
            DrainReport {
                evacuated: vec![evacuated("a")],
                ..DrainReport::default()
            },
            EvacuationRecord::Recorded,
            true,
        ),
        (
            "pending",
            DrainReport {
                evacuated: vec![evacuated("a")],
                pending: vec![pending("b", DrainWait::Moving)],
                ..DrainReport::default()
            },
            EvacuationRecord::Recorded,
            false,
        ),
        (
            "a wait this build doesn't know",
            DrainReport {
                evacuated: vec![evacuated("a")],
                pending: vec![DrainPending {
                    wait: 9,
                    ..pending("b", DrainWait::Moving)
                }],
                ..DrainReport::default()
            },
            EvacuationRecord::Recorded,
            false,
        ),
        (
            "blocked",
            DrainReport {
                evacuated: vec![evacuated("a")],
                blocked: vec![blocked("orphan inventory scan failed")],
                ..DrainReport::default()
            },
            EvacuationRecord::Recorded,
            false,
        ),
        (
            "an evacuation waiting",
            DrainReport {
                evacuated: vec![evacuated("a")],
                ..DrainReport::default()
            },
            EvacuationRecord::NotYet("uploading".into()),
            false,
        ),
    ] {
        let draining = node(Some("draining"), Some(5));
        let sink = FakeSink {
            nodes: vec![draining.clone()],
            responses: HashMap::from([((draining.id, false), Ok(answer(5, report)))]),
            records: HashMap::from([("a".to_owned(), record)]),
            ..FakeSink::default()
        };
        let report = run_tick(&sink, &FakeAuthority::ok()).await;
        assert_eq!(report.drained == [draining.id], advances, "{case}");
        let calls = sink.calls();
        assert_eq!(
            calls.iter().any(|call| call.starts_with("advance")),
            advances,
            "{case}: {calls:?}"
        );
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

/// A drained node isn't called and its tick writes nothing: the one call of
/// the sink that names it converges its cordon, since it is still sealed. A
/// draining one's tick drains it.
#[tokio::test]
async fn a_drained_node_is_neither_called_nor_written() {
    let drained = node(Some("drained"), Some(5));
    let draining = node(Some("draining"), Some(6));
    let sink = FakeSink {
        responses: HashMap::from([((draining.id, false), Ok(response(6, &[])))]),
        nodes: vec![drained.clone(), draining.clone()],
        ..FakeSink::default()
    };
    run_tick(&sink, &FakeAuthority::ok()).await;
    let drained_calls: Vec<String> = sink
        .calls()
        .into_iter()
        .filter(|call| call.contains(&drained.id.to_string()))
        .collect();
    assert_eq!(drained_calls, [format!("cordon {}", drained.id)]);
    assert!(called(&sink, "drain_node", &draining), "{:?}", sink.calls());
}

/// EC2's listing of one instance id reads as one state only when it lists
/// the instance once; twice is no answer the loop acts on.
#[test]
fn ec2_states_read_as_evidence() {
    for (states, evidence) in [
        (&[][..], InstanceEvidence::NotListed),
        (&["terminated"][..], InstanceEvidence::Terminated),
        (&["shutting-down"][..], InstanceEvidence::ShuttingDown),
        (&["running"][..], InstanceEvidence::Listed("running".into())),
        (
            &["terminated", "running"][..],
            InstanceEvidence::Listed("listed 2 times".into()),
        ),
    ] {
        let states: Vec<String> = states.iter().map(|state| (*state).to_owned()).collect();
        assert_eq!(
            InstanceEvidence::from_states(&states),
            evidence,
            "{states:?}"
        );
    }
}

/// With new v2 drains not admitted, the loop still owns the persisted ones:
/// it drains a draining node to `drained` and cancels an undrained one. With
/// no terminator that terminates, it never fences a `terminating` node,
/// which waits unfenced with the refusal as its error.
#[tokio::test]
async fn the_loop_owns_persisted_drains_without_terminating() {
    let draining = node(Some("draining"), Some(5));
    let undrained = node(None, Some(5));
    let terminating = node(Some("terminating"), Some(5));
    let sink = FakeSink {
        nodes: vec![draining.clone(), undrained.clone(), terminating.clone()],
        responses: HashMap::from([
            ((draining.id, false), Ok(response(5, &[]))),
            ((undrained.id, true), Ok(cancelled(5))),
        ]),
        ..FakeSink::default()
    };
    let report = run_tick(&sink, &V2Terminator::Refuse).await;
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

/// A draining node that doesn't answer its `DrainNode` call, dead or
/// refusing, has why recorded as its error and stores no report; one that
/// answers stores its report.
#[tokio::test(flavor = "current_thread")]
async fn a_node_that_does_not_answer_records_why() {
    for (case, response, refusal, answers) in [
        ("dead", Err("unreachable".into()), None, false),
        (
            "refusing",
            Ok(response(5, &[])),
            Some(tonic::Status::unavailable("restart in progress")),
            false,
        ),
        ("answering", Ok(response(5, &[])), None, true),
    ] {
        let draining = node(Some("draining"), Some(5));
        let sink = FakeSink {
            nodes: vec![draining.clone()],
            responses: HashMap::from([((draining.id, false), response)]),
            refusal,
            ..FakeSink::default()
        };
        run_tick(&sink, &FakeAuthority::ok()).await;
        assert_eq!(
            called(&sink, "error", &draining),
            !answers,
            "{case}: {:?}",
            sink.calls()
        );
        assert_eq!(
            sink.reports.lock().unwrap().len(),
            usize::from(answers),
            "{case}"
        );
        if case == "refusing" {
            assert!(
                sink.calls()
                    .iter()
                    .any(|call| call.contains("restart in progress")),
                "{:?}",
                sink.calls()
            );
        }
    }
}

/// An `Aborted` drain call, another `DrainNode` call in progress on the
/// node, skips the node this tick: nothing is recorded and no error is
/// written.
#[tokio::test(flavor = "current_thread")]
async fn an_aborted_drain_call_skips_the_tick() {
    let draining = node(Some("draining"), Some(5));
    let sink = FakeSink {
        nodes: vec![draining.clone()],
        refusal: Some(tonic::Status::aborted(
            pplx_space_rpc_contracts::drain_node_signals::DRAIN_CALL_IN_PROGRESS_MARKER,
        )),
        ..FakeSink::default()
    };
    let report = run_tick(&sink, &FakeAuthority::ok()).await;
    assert_eq!(report, V2TickReport::default());
    assert_eq!(
        sink.calls(),
        [
            format!("cordon {}", draining.id),
            format!("drain_node {} 5 cancel=false", draining.id),
        ]
    );
}

/// A cordon that fails keeps its error past the report, which clears the
/// last error; one that succeeds leaves none.
#[tokio::test]
async fn a_failed_cordon_keeps_its_error_until_a_cordon_succeeds() {
    for (case, cordon_error, keeps) in [
        (
            "failed",
            Some(CordonFailure::Other("node unreachable".to_owned())),
            true,
        ),
        ("converged", None, false),
    ] {
        let draining = node(Some("draining"), Some(5));
        let sink = FakeSink {
            nodes: vec![draining.clone()],
            responses: HashMap::from([((draining.id, false), Ok(response(5, &[])))]),
            cordon_error,
            ..FakeSink::default()
        };
        run_tick(&sink, &FakeAuthority::ok()).await;
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

/// A cordon refused because the Node under the drain's node name is another
/// one than the drain's gets no `DrainNode` call: the call would reach
/// another machine. The error names it.
#[tokio::test]
async fn a_cordon_uid_mismatch_skips_the_drain_call() {
    let draining = node(Some("draining"), Some(5));
    let sink = FakeSink {
        nodes: vec![draining.clone()],
        responses: HashMap::from([((draining.id, false), Ok(response(5, &[])))]),
        cordon_error: Some(CordonFailure::UidMismatch(
            "the Kubernetes Node is another one than the drain's".to_owned(),
        )),
        ..FakeSink::default()
    };
    let report = run_tick(&sink, &FakeAuthority::ok()).await;
    assert_eq!(report, V2TickReport::default());
    assert_eq!(
        sink.calls(),
        [
            format!("cordon {}", draining.id),
            format!(
                "error {} cordon convergence failed: the Kubernetes Node is another one than the \
                 drain's; not calling DrainNode",
                draining.id
            ),
        ]
    );
}

/// A terminating node makes one fresh `DrainNode` call, records its report
/// as a draining node's, and fences only on a report with nothing pending
/// or blocked whose every evacuation is recorded. Anything else returns the
/// node to `drained` with why: a report that isn't empty names its counts
/// and first reasons; one with an evacuation not yet recorded names it; a
/// call that fails, an `Aborted` one or a node whose seal is unreadable
/// among them, names the failure. A fence the require-empty predicate holds, or a row
/// that moved, fences nothing and stays.
#[tokio::test]
async fn a_terminating_node_fences_only_on_a_fresh_empty_report() {
    let refusal = |status: tonic::Status| (Err(String::new()), Some(status));
    for (case, (response, refusal), fence, fences, returned, record) in [
        (
            "empty",
            (Ok(response(5, &["a"])), None),
            None,
            true,
            None,
            None,
        ),
        (
            "held",
            (Ok(response(5, &[])), None),
            Some(Fence::Held("terminate held".to_owned())),
            false,
            None,
            None,
        ),
        (
            "moved",
            (Ok(response(5, &[])), None),
            Some(Fence::Moved),
            false,
            None,
            None,
        ),
        (
            "a producer pending",
            (
                Ok(answer(
                    5,
                    DrainReport {
                        pending: vec![DrainPending {
                            reason: "a snapshot is being published".to_owned(),
                            ..pending("b", DrainWait::Moving)
                        }],
                        ..DrainReport::default()
                    },
                )),
                None,
            ),
            None,
            false,
            Some(
                "terminate refused: the node reports 1 pending, 0 blocked: b: a snapshot is \
                 being published",
            ),
            None,
        ),
        (
            "blocked",
            (
                Ok(answer(
                    5,
                    DrainReport {
                        blocked: vec![blocked("scan failed"), blocked("orphan")],
                        pending: vec![pending("c", DrainWait::External)],
                        ..DrainReport::default()
                    },
                )),
                None,
            ),
            None,
            false,
            Some(
                "terminate refused: the node reports 1 pending, 2 blocked: scan failed; orphan; c",
            ),
            None,
        ),
        (
            "an unreadable seal",
            refusal(tonic::Status::internal("the activation seal can't be read")),
            None,
            false,
            Some("terminate refused: DrainNode failed: "),
            None,
        ),
        (
            "aborted",
            refusal(tonic::Status::aborted("in progress")),
            None,
            false,
            Some("terminate refused: DrainNode failed: another DrainNode call is in progress"),
            None,
        ),
        (
            "another generation",
            (Ok(response(6, &[])), None),
            None,
            false,
            Some(
                "terminate refused: the node answered DrainNode under generation 5 sealed \
                 under generation 6",
            ),
            None,
        ),
        (
            "an evacuation not yet recorded",
            (Ok(response(5, &["a"])), None),
            None,
            false,
            Some(
                "terminate refused: the node reports 0 pending, 0 blocked, and 1 evacuated not \
                 yet recorded: a: uploading",
            ),
            Some(EvacuationRecord::NotYet("uploading".into())),
        ),
    ] {
        let terminating = node(Some("terminating"), Some(5));
        let sink = FakeSink {
            nodes: vec![terminating.clone()],
            responses: HashMap::from([((terminating.id, false), response)]),
            refusal,
            fence,
            records: record
                .map(|record| HashMap::from([("a".to_owned(), record)]))
                .unwrap_or_default(),
            ..FakeSink::default()
        };
        let authority = FakeAuthority::ok();
        let report = run_tick(&sink, &authority).await;
        assert_eq!(report.fenced == [terminating.id], fences, "{case}");
        assert_eq!(
            report.returned == [terminating.id],
            returned.is_some(),
            "{case}"
        );
        let calls = sink.calls();
        // The node may still be sealed: its cordon is converged before the
        // fresh call.
        assert_eq!(
            calls[..2],
            [
                format!("cordon {}", terminating.id),
                format!("drain_node {} 5 cancel=false", terminating.id),
            ],
            "{case}"
        );
        let fenced_call = format!("fence {} 5", terminating.id);
        match returned {
            Some(why) => {
                assert!(
                    calls.iter().any(|call| call
                        .starts_with(&format!("return {} 5 {why}", terminating.id))),
                    "{case}: {calls:?}"
                );
                assert!(!calls.contains(&fenced_call), "{case}: {calls:?}");
            }
            None => {
                assert!(calls.contains(&fenced_call), "{case}: {calls:?}");
                assert!(called(&sink, "report", &terminating), "{case}: {calls:?}");
            }
        }
        assert!(authority.described().is_empty() && authority.terminated().is_empty());
    }
}

/// A terminating or cancelling node whose Kubernetes Node has another UID
/// than the one registration recorded isn't called: the call would reach
/// another machine. The terminate is refused and the node returned to
/// drained, saying why; the cancel records the error and is retried next
/// tick. Any other cordon failure is recorded and the call goes on.
#[tokio::test]
async fn a_cordon_on_another_machine_stops_the_terminate_and_the_cancel() {
    for (case, phase) in [("terminate", Some("terminating")), ("cancel", None)] {
        let target = node(phase, Some(5));
        let sink = FakeSink {
            nodes: vec![target.clone()],
            cordon_error: Some(CordonFailure::UidMismatch(
                "the Node has uid other".to_owned(),
            )),
            ..FakeSink::default()
        };
        let report = run_tick(&sink, &FakeAuthority::ok()).await;
        let calls = sink.calls();
        assert!(!called(&sink, "drain_node", &target), "{case}: {calls:?}");
        assert!(
            report.fenced.is_empty() && report.cancelled.is_empty(),
            "{case}"
        );
        if phase.is_some() {
            assert_eq!(report.returned, [target.id], "{case}");
            assert!(
                calls.iter().any(|call| call.starts_with(&format!(
                    "return {} 5 terminate refused: cordon convergence failed: the Node has uid \
                     other; not calling DrainNode",
                    target.id
                ))),
                "{case}: {calls:?}"
            );
        } else {
            assert!(called(&sink, "error", &target), "{case}: {calls:?}");
            assert!(
                !called(&sink, "finish_cancel", &target),
                "{case}: {calls:?}"
            );
        }

        let sink = FakeSink {
            nodes: vec![target.clone()],
            cordon_error: Some(CordonFailure::Other("node unreachable".to_owned())),
            ..FakeSink::default()
        };
        run_tick(&sink, &FakeAuthority::ok()).await;
        assert!(
            called(&sink, "drain_node", &target),
            "{case}: {:?}",
            sink.calls()
        );
    }
}

/// Under a terminator that isn't `aws` a terminating node isn't called and
/// isn't fenced: it waits for an operator, and says why.
#[tokio::test]
async fn a_terminating_node_waits_without_a_terminating_authority() {
    let terminating = node(Some("terminating"), Some(5));
    let sink = FakeSink {
        nodes: vec![terminating.clone()],
        ..FakeSink::default()
    };
    let authority = FakeAuthority {
        terminates: false,
        ..FakeAuthority::ok()
    };
    run_tick(&sink, &authority).await;
    assert_eq!(
        sink.calls(),
        [format!(
            "error {} {}",
            terminating.id,
            super::cleanup::REFUSED_TERMINATE
        )]
    );
}

/// The stale-report regression: a node drained with an empty report, whose
/// sandbox-manager restarted unable to read its seal, and on which a
/// snapshot of a suspended sandbox the drain made durable was then taken.
/// `:terminate` moves it to `terminating`, and the terminating tick's fresh
/// call doesn't trust the stored report: the call fails on the unreadable
/// seal, or once the seal reads, reports the snapshot's producer pending.
/// Either way the node returns to `drained` with the error, unfenced.
#[tokio::test]
async fn a_stale_report_never_fences_a_node() {
    let stored = StoredV2Report {
        reported_at: DB_NOW - 60,
        evacuated_grew_at: DB_NOW - 120,
        ..StoredV2Report::default()
    };
    assert!(stored.finished(), "the stored report says nothing is left");
    for (case, response, refusal, why) in [
        (
            "the seal can't be read",
            Err(String::new()),
            Some(tonic::Status::failed_precondition(
                "the node's activation seal couldn't be read at startup",
            )),
            "terminate refused: DrainNode failed: ",
        ),
        (
            "the producer is pending",
            Ok(answer(
                5,
                DrainReport {
                    pending: vec![DrainPending {
                        reason: "a snapshot of the suspended sandbox".to_owned(),
                        ..pending("suspended", DrainWait::External)
                    }],
                    ..DrainReport::default()
                },
            )),
            None,
            "terminate refused: the node reports 1 pending, 0 blocked: suspended: a snapshot \
             of the suspended sandbox",
        ),
    ] {
        let terminating = V2Node {
            report: Some(stored.clone()),
            ..node(Some("terminating"), Some(5))
        };
        let sink = FakeSink {
            nodes: vec![terminating.clone()],
            responses: HashMap::from([((terminating.id, false), response)]),
            refusal,
            ..FakeSink::default()
        };
        let report = run_tick(&sink, &FakeAuthority::ok()).await;
        assert_eq!(report.returned, [terminating.id], "{case}");
        assert!(report.fenced.is_empty(), "{case}");
        let calls = sink.calls();
        assert!(!called(&sink, "fence", &terminating), "{case}: {calls:?}");
        assert!(
            calls
                .iter()
                .any(|call| call.starts_with(&format!("return {} 5 {why}", terminating.id))),
            "{case}: {calls:?}"
        );
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
        let report = run_tick(&sink, &authority).await;
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

/// An undispatched fenced node whose instance EC2 lists `terminated` is let
/// go only with its heartbeat stale: a fresh heartbeat says the machine
/// still answers, and the drain waits and says so.
#[tokio::test]
async fn an_undispatched_fenced_let_go_needs_a_stale_heartbeat() {
    let node = V2Node {
        heartbeat_fresh: true,
        ..fenced(false)
    };
    let sink = FakeSink {
        nodes: vec![node.clone()],
        ..FakeSink::default()
    };
    let report = run_tick(
        &sink,
        &FakeAuthority::answering(InstanceEvidence::Terminated),
    )
    .await;
    assert!(report.let_go.is_empty(), "{:?}", sink.calls());
    assert!(
        sink.calls().iter().any(|call| call
            .ends_with("is terminated, but the node's heartbeat is fresh; not letting it go")),
        "{:?}",
        sink.calls()
    );
}

/// Once EC2 has been seen to list the fenced instance `terminated`, an
/// instance it no longer lists reads as terminated: an undispatched node is
/// let go, as on `terminated`, and dispatches nothing.
#[tokio::test]
async fn an_unlisted_instance_after_a_recorded_terminated_reads_terminated() {
    let node = V2Node {
        terminated_observed_at: Some(DB_NOW),
        ..fenced(false)
    };
    let sink = FakeSink {
        nodes: vec![node.clone()],
        ..FakeSink::default()
    };
    let authority = FakeAuthority::answering(InstanceEvidence::NotListed);
    let report = run_tick(&sink, &authority).await;
    assert_eq!(report.let_go, [node.id], "{:?}", sink.calls());
    assert!(authority.terminated().is_empty());
    assert!(!called(&sink, "observed", &node), "{:?}", sink.calls());
}

/// Once `dispatched_at` is recorded the loop never dispatches again,
/// whatever EC2 answers; past the bound it says the termination was
/// accepted and the instance still runs. No answer from EC2 is said on
/// every check, with the dispatch's age.
#[tokio::test]
async fn a_dispatched_node_is_never_dispatched_again() {
    let bound = V2LoopConfig::default().terminate_bound;
    let past_bound = DB_NOW - i64::try_from(bound.as_secs()).unwrap() - 60;
    let unknown = || InstanceEvidence::Unknown("the EC2 call timed out".to_owned());
    let running = || InstanceEvidence::Listed("running".to_owned());
    for (case, dispatched_at, evidence, says) in [
        ("just dispatched", DB_NOW, running(), None),
        (
            "past the bound",
            past_bound,
            running(),
            Some("EC2 still answers running after 16 minutes"),
        ),
        (
            "just dispatched, no answer",
            DB_NOW,
            unknown(),
            Some("was dispatched 0 minutes ago; EC2 answers unknown (the EC2 call timed out)"),
        ),
        (
            "past the bound, no answer",
            past_bound,
            unknown(),
            Some("was dispatched 16 minutes ago; EC2 answers unknown"),
        ),
    ] {
        let node = V2Node {
            dispatched_at: Some(dispatched_at),
            ..fenced(true)
        };
        let sink = FakeSink {
            nodes: vec![node.clone()],
            ..FakeSink::default()
        };
        let authority = FakeAuthority::answering(evidence);
        let report = run_tick(&sink, &authority).await;
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

/// A refused dispatch records its error; the backoff keeps the next tick
/// from asking again at once, and a later one asks again. The base is the
/// config's, which its test hook can shrink.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_refused_dispatch_backs_off() {
    let node = fenced(false);
    let sink = FakeSink {
        nodes: vec![node.clone()],
        ..FakeSink::default()
    };
    let authority = FakeAuthority {
        terminate: Err("denied".into()),
        ..FakeAuthority::ok()
    };
    let config = V2LoopConfig {
        dispatch_backoff_base: Duration::from_secs(3),
        ..V2LoopConfig::default()
    };
    let state = V2LoopState::new(config);
    tick(&sink, &authority, &state).await;
    assert!(
        sink.calls()
            .iter()
            .any(|call| call.contains(&format!("terminating fenced instance {INSTANCE} failed"))),
        "{:?}",
        sink.calls()
    );
    tick(&sink, &authority, &state).await;
    assert_eq!(
        authority.terminated().len(),
        1,
        "the backoff holds the retry"
    );
    tokio::time::advance(config.dispatch_backoff_base).await;
    tick(&sink, &authority, &state).await;
    assert_eq!(authority.terminated().len(), 2, "the backoff ran out");
}

/// A fenced node whose dispatch is unconfirmed, whose instance EC2 says is
/// terminated, whose heartbeat is stale and whose Node is gone is let go; a
/// let-go that fails backs off like a refused dispatch: the next tick
/// neither asks EC2 nor tries again at once, and a later one does both.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_failed_let_go_backs_off() {
    let node = fenced(false);
    let sink = FakeSink {
        nodes: vec![node.clone()],
        let_go_error: Some("the row moved".into()),
        ..FakeSink::default()
    };
    let authority = FakeAuthority::answering(InstanceEvidence::Terminated);
    let config = V2LoopConfig {
        dispatch_backoff_base: Duration::from_secs(3),
        ..V2LoopConfig::default()
    };
    let state = V2LoopState::new(config);
    let let_goes = || {
        sink.calls()
            .iter()
            .filter(|call| call.starts_with(&format!("let_go {}", node.id)))
            .count()
    };
    tick(&sink, &authority, &state).await;
    tick(&sink, &authority, &state).await;
    assert_eq!(authority.described().len(), 1, "the backoff holds the ask");
    assert_eq!(let_goes(), 1, "the backoff holds the let-go");
    tokio::time::advance(config.dispatch_backoff_base).await;
    tick(&sink, &authority, &state).await;
    assert_eq!(authority.described().len(), 2, "the backoff ran out");
    assert_eq!(let_goes(), 2, "the backoff ran out");
}

/// A dispatched node whose fenced instance EC2 says is terminated is
/// deleted once a direct GET finds its Node gone. A Node still there waits
/// for it; an instance not yet terminated asks nothing of Kubernetes; a
/// guard that refuses the delete lets the node go; a row a registration
/// took has nothing left to do. The first `terminated` is recorded before
/// anything else; after it, an instance EC2 no longer lists is deleted as a
/// terminated one, and without it an unlisted instance never is, saying
/// why.
#[tokio::test]
async fn a_dispatched_node_is_deleted_once_terminated_with_its_node_gone() {
    struct Case {
        name: &'static str,
        evidence: InstanceEvidence,
        observed: bool,
        node_present: bool,
        delete: Option<TerminatedDelete>,
        looks_up: bool,
        deletes: Option<bool>,
    }
    let case = |name, evidence| Case {
        name,
        evidence,
        observed: false,
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
    ];
    for case in cases {
        let name = case.name;
        let node = V2Node {
            terminated_observed_at: case.observed.then_some(DB_NOW),
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
        let report = run_tick(&sink, &authority).await;
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

/// Let-go comes first, in every unfenced v2 state: with the node's
/// heartbeat stale, EC2's `terminated` for the recorded instance lets it go
/// with a trace naming it, recorded first (`terminated_observed_at`), and
/// the node is called for nothing; so does an unlisted instance once the
/// drain recorded it `terminated`. An unlisted one never seen `terminated`
/// isn't death, and any other answer only records what EC2 said, after
/// which the state's own step runs. A fresh heartbeat asks EC2 nothing.
#[tokio::test]
async fn every_unfenced_v2_state_lets_go_once_ec2_says_terminated() {
    type Build = fn() -> V2Node;
    let states: [(&str, Build); 4] = [
        ("terminating", || node(Some("terminating"), Some(5))),
        ("drained", || node(Some("drained"), Some(5))),
        ("draining", || node(Some("draining"), Some(5))),
        ("cancel pending", || node(None, Some(5))),
    ];
    for (state, build) in states {
        for (answer, evidence, observed, lets_go) in [
            (
                "terminated",
                Some(InstanceEvidence::Terminated),
                false,
                true,
            ),
            (
                "terminated, recorded",
                Some(InstanceEvidence::Terminated),
                true,
                true,
            ),
            (
                "not listed, recorded terminated",
                Some(InstanceEvidence::NotListed),
                true,
                true,
            ),
            (
                "shutting-down",
                Some(InstanceEvidence::ShuttingDown),
                false,
                false,
            ),
            (
                "stopped",
                Some(InstanceEvidence::Listed("stopped".into())),
                false,
                false,
            ),
            (
                "not listed",
                Some(InstanceEvidence::NotListed),
                false,
                false,
            ),
            (
                "unknown",
                Some(InstanceEvidence::Unknown("timeout".into())),
                false,
                false,
            ),
            ("fresh heartbeat", None, false, false),
        ] {
            let case = format!("{state} {answer}");
            let node = V2Node {
                heartbeat_fresh: evidence.is_none(),
                terminated_observed_at: observed.then_some(DB_NOW - 60),
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
            let report = run_tick(&sink, &authority).await;
            assert_eq!(report.let_go == [node.id], lets_go, "{case}");
            let trace = format!(
                "let_go {} 5 v2 drain let go: instance {INSTANCE} terminated outside the drain",
                node.id
            );
            let records = evidence == Some(InstanceEvidence::Terminated) && !observed;
            if lets_go {
                let mut expected = Vec::new();
                if records {
                    expected.push(format!("observed {} 5 {INSTANCE}", node.id));
                }
                expected.push(trace);
                assert_eq!(sink.calls(), expected, "{case}");
            } else {
                assert!(!sink.calls().contains(&trace), "{case}");
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

/// The machines a let-go test's Node can stand for: gone, or live on the
/// recorded instance, on another one, or naming none, with whether each
/// lets the node go and the reason a wait records.
const MACHINES: [(&str, Option<Option<&str>>, bool, &str); 4] = [
    ("node gone", None, true, ""),
    ("matching providerID", Some(Some(INSTANCE)), true, ""),
    (
        "stale instance id, live node",
        Some(Some("i-0000000000000d0d0")),
        false,
        "is live on instance i-0000000000000d0d0",
    ),
    (
        "no providerID, live node",
        Some(None),
        false,
        "is live and names no EC2 instance",
    ),
];

/// `sink` with `node`'s Kubernetes Node standing for `machine`.
fn with_machine(sink: FakeSink, node: &V2Node, machine: Option<Option<&str>>) -> FakeSink {
    let name = node.k8s_node_name.clone().unwrap();
    FakeSink {
        nodes_present: machine.map(|_| vec![name.clone()]).unwrap_or_default(),
        provider_instances: machine
            .flatten()
            .map(|instance| HashMap::from([(name, instance.to_owned())]))
            .unwrap_or_default(),
        ..sink
    }
}

/// An unfenced node is let go on EC2's `terminated` for its recorded
/// instance only when that is the machine it registered: its Kubernetes
/// Node is gone, or the Node's `providerID` names the recorded instance. A
/// live Node on another instance (the recorded one is stale) or naming none
/// is not let go: the drain waits and says why.
#[tokio::test]
async fn an_unfenced_let_go_needs_the_registered_machine() {
    for (case, machine, lets_go, reason) in MACHINES {
        let node = V2Node {
            heartbeat_fresh: false,
            ..node(Some("draining"), Some(5))
        };
        let sink = with_machine(
            FakeSink {
                nodes: vec![node.clone()],
                ..FakeSink::default()
            },
            &node,
            machine,
        );
        let authority = FakeAuthority::answering(InstanceEvidence::Terminated);
        let report = run_tick(&sink, &authority).await;
        assert_eq!(
            report.let_go == [node.id],
            lets_go,
            "{case}: {:?}",
            sink.calls()
        );
        assert_eq!(called(&sink, "let_go", &node), lets_go, "{case}");
        if !lets_go {
            assert!(
                sink.calls()
                    .iter()
                    .any(|call| call.starts_with(&format!("error {}", node.id))
                        && call.contains(reason)
                        && call.ends_with("not letting it go")),
                "{case}: {:?}",
                sink.calls()
            );
        }
        assert!(authority.terminated().is_empty(), "{case}");
    }
}

/// A fenced node whose termination the drain never confirmed is let go on
/// the fenced instance's `terminated` only when that is the machine it
/// registered, as an unfenced one is; otherwise it waits and says why, and
/// dispatches nothing.
#[tokio::test]
async fn an_undispatched_fenced_let_go_needs_the_registered_machine() {
    for (case, machine, lets_go, reason) in MACHINES {
        let node = fenced(false);
        let sink = with_machine(
            FakeSink {
                nodes: vec![node.clone()],
                ..FakeSink::default()
            },
            &node,
            machine,
        );
        let authority = FakeAuthority::answering(InstanceEvidence::Terminated);
        let report = run_tick(&sink, &authority).await;
        assert_eq!(
            report.let_go == [node.id],
            lets_go,
            "{case}: {:?}",
            sink.calls()
        );
        if !lets_go {
            assert!(
                sink.calls()
                    .iter()
                    .any(|call| call.starts_with(&format!("error {}", node.id))
                        && call.contains(reason)
                        && call.ends_with("not letting it go")),
                "{case}: {:?}",
                sink.calls()
            );
        }
        assert!(authority.terminated().is_empty(), "{case}");
        assert!(!called(&sink, "delete", &node), "{case}");
    }
}

/// A dispatched fenced node's row is deleted, or the node let go by the
/// delete's guard, only once its Kubernetes Node is gone: a live Node holds
/// it whatever its `providerID` names, the recorded instance or another.
#[tokio::test]
async fn a_dispatched_delete_waits_for_any_live_node() {
    for (case, machine, _, _) in MACHINES {
        let node = V2Node {
            terminated_observed_at: Some(DB_NOW),
            ..fenced(true)
        };
        let sink = with_machine(
            FakeSink {
                nodes: vec![node.clone()],
                ..FakeSink::default()
            },
            &node,
            machine,
        );
        let authority = FakeAuthority::answering(InstanceEvidence::Terminated);
        run_tick(&sink, &authority).await;
        assert_eq!(
            called(&sink, "delete", &node),
            machine.is_none(),
            "{case}: {:?}",
            sink.calls()
        );
        assert!(!called(&sink, "let_go", &node), "{case}");
    }
}

/// The stored report carries what the loop could not record yet, each
/// evacuation with its artifact's kind, each pending entry as the node
/// reported it, its wait and whether it was running, and the two stamps.
#[test]
fn the_stored_report_names_the_evacuations_not_yet_recorded() {
    let report = DrainReport {
        evacuated: vec![evacuated("a"), evacuated("b")],
        pending: vec![DrainPending {
            since: Some(prost_types::Timestamp {
                seconds: 990,
                nanos: 0,
            }),
            reason: "uploading".to_owned(),
            ..pending("c", DrainWait::Moving)
        }],
        blocked: vec![blocked("scan failed")],
    };
    let stored = stored_report(
        &report,
        &Recording {
            records: vec![
                ("a".into(), EvacuationRecord::Recorded),
                ("b".into(), EvacuationRecord::NotYet("uploading".into())),
            ],
            since: BTreeMap::from([("b".into(), 1_000)]),
            now: 1_060,
        },
        1_000,
    );
    assert_eq!(
        serde_json::to_value(&stored).unwrap(),
        serde_json::json!({
            "reported_at": 1_060,
            "evacuated_grew_at": 1_000,
            "evacuated": [
                {"sandbox_id": "a", "artifact_kind": "full", "artifact_id": "artifact"},
                {"sandbox_id": "b", "artifact_kind": "full", "artifact_id": "artifact"},
            ],
            "pending": [
                {"sandbox_id": "c", "wait": "moving", "running": false, "reason": "uploading", "since": 990, "pool_member": false},
            ],
            "blocked": [{"reason": "scan failed"}],
            "unrecorded": [{"sandbox_id": "b", "reason": "uploading", "since": 1_000}],
        })
    );
    assert_eq!(
        stored.evacuated[0].artifact_kind,
        StoredV2ArtifactKind::Full
    );
    assert_eq!(stored.pending[0].wait, StoredV2Wait::Moving);
}

/// Each drain call holds until the deadline and evacuates from it on, as
/// the listing read it by the database clock; a cancel names only the
/// generation.
#[tokio::test]
async fn a_drain_call_evacuates_from_the_deadline() {
    for evacuate in [false, true] {
        let draining = V2Node {
            evacuate,
            ..node(Some("draining"), Some(5))
        };
        let sink = FakeSink {
            nodes: vec![draining.clone()],
            responses: HashMap::from([((draining.id, false), Ok(response(5, &[])))]),
            ..FakeSink::default()
        };
        run_tick(&sink, &FakeAuthority::ok()).await;
        assert_eq!(sink.requests(), [drain_request(5, evacuate)], "{evacuate}");
        assert_eq!(
            drain_request(5, evacuate).op,
            Some(drain_node_request::Op::Drain(drain_node_request::Drain {
                evacuate
            }))
        );
    }
    assert_eq!(
        cancel_request(5),
        DrainNodeRequest {
            generation: 5,
            op: Some(drain_node_request::Op::Cancel(
                drain_node_request::Cancel {}
            )),
        }
    );
}

/// A drain call's answer is the drain's only as a drain report under the
/// drain's generation. Under another generation, as a cancel's result, or
/// with no result, the loop records nothing from it, names it in the last
/// error, and doesn't advance.
#[tokio::test(flavor = "current_thread")]
async fn an_answer_not_under_the_drains_generation_records_nothing() {
    for (case, response, named) in [
        (
            "an earlier generation",
            response(4, &["a"]),
            "sealed under generation 4",
        ),
        (
            "a later generation",
            response(6, &["a"]),
            "sealed under generation 6",
        ),
        ("a cancel's result", cancelled(5), "with a cancel's result"),
        (
            "no result",
            DrainNodeResponse {
                generation: 5,
                result: None,
            },
            "with no result",
        ),
    ] {
        let draining = node(Some("draining"), Some(5));
        let sink = FakeSink {
            nodes: vec![draining.clone()],
            responses: HashMap::from([((draining.id, false), Ok(response))]),
            ..FakeSink::default()
        };
        let report = run_tick(&sink, &FakeAuthority::ok()).await;
        assert!(report.drained.is_empty(), "{case}");
        let calls = sink.calls();
        assert!(
            !calls.iter().any(|call| call.starts_with("record")
                || call.starts_with("report")
                || call.starts_with("advance")),
            "{case}: {calls:?}"
        );
        assert!(
            calls.iter().any(|call| call.starts_with(&format!(
                "error {} the node answered DrainNode under generation 5 {named}; it records \
                 nothing from the answer",
                draining.id
            ))),
            "{case}: {calls:?}"
        );
    }
}

/// A cancel pending (a v2 row with no phase) sends `Cancel` under its
/// generation, and only a cancel's answer finishes it: the cordon's lift
/// and the clear of the columns, under the row's lock and only while the
/// row is still this generation's cancel. A drain-shaped answer, a call
/// that fails, and an `Aborted` one are unanswered: the error is recorded,
/// and the next tick asks again; so is a finish that
/// fails. A finish that finds the row moved on clears nothing. Each tick
/// converges the cordon first, since the node may still be sealed.
#[tokio::test(flavor = "current_thread")]
async fn a_cancel_pending_finishes_only_on_a_cancels_answer() {
    for (case, response, refusal, finish, order, clears) in [
        (
            "cancelled",
            Ok(cancelled(5)),
            None,
            None,
            &["cordon", "drain_node 5 cancel=true", "finish_cancel 5"][..],
            true,
        ),
        (
            "a drain's answer",
            Ok(response(5, &[])),
            None,
            None,
            &["cordon", "drain_node 5 cancel=true", "error"][..],
            false,
        ),
        (
            "aborted",
            Err(String::new()),
            Some(tonic::Status::aborted("in progress")),
            None,
            &["cordon", "drain_node 5 cancel=true", "error"][..],
            false,
        ),
        (
            "unreachable",
            Err("unreachable".into()),
            None,
            None,
            &["cordon", "drain_node 5 cancel=true", "error"][..],
            false,
        ),
        (
            "the uncordon fails",
            Ok(cancelled(5)),
            None,
            Some(Err("node unreachable".to_owned())),
            &[
                "cordon",
                "drain_node 5 cancel=true",
                "finish_cancel 5",
                "error",
            ][..],
            false,
        ),
        (
            "re-drained since the listing",
            Ok(cancelled(5)),
            None,
            Some(Ok(false)),
            &["cordon", "drain_node 5 cancel=true", "finish_cancel 5"][..],
            false,
        ),
    ] {
        let undrained = node(None, Some(5));
        let sink = FakeSink {
            nodes: vec![undrained.clone()],
            responses: HashMap::from([((undrained.id, true), response)]),
            refusal,
            finish_cancel: finish,
            ..FakeSink::default()
        };
        let report = run_tick(&sink, &FakeAuthority::ok()).await;
        assert_eq!(report.cancelled == [undrained.id], clears, "{case}");
        let id = undrained.id.to_string();
        let calls: Vec<String> = sink
            .calls()
            .into_iter()
            .filter(|call| call.contains(&id))
            .map(|call| {
                let call = call.replacen(&format!(" {id}"), "", 1);
                if call.starts_with("error") {
                    "error".to_owned()
                } else {
                    call
                }
            })
            .collect();
        assert_eq!(calls, order, "{case}");
    }
}

/// The pending entries a report lists running go to the drain's observed
/// set with the report, and the evacuations recorded to `evacuated_total`;
/// nothing else does.
#[tokio::test]
async fn a_report_adds_its_running_and_recorded_sandboxes() {
    let draining = node(Some("draining"), Some(5));
    let sink = FakeSink {
        nodes: vec![draining.clone()],
        responses: HashMap::from([(
            (draining.id, false),
            Ok(answer(
                5,
                DrainReport {
                    evacuated: vec![evacuated("suspended"), evacuated("late")],
                    pending: vec![
                        pending("held", DrainWait::Hold),
                        pending("paused", DrainWait::Moving),
                        DrainPending {
                            running: true,
                            ..pending("suspending", DrainWait::Moving)
                        },
                    ],
                    ..DrainReport::default()
                },
            )),
        )]),
        records: HashMap::from([(
            "late".to_owned(),
            EvacuationRecord::NotYet("not projected yet".into()),
        )]),
        ..FakeSink::default()
    };
    run_tick(&sink, &FakeAuthority::ok()).await;
    assert_eq!(
        sink.added(),
        [(
            vec!["held".to_owned(), "suspending".to_owned()],
            vec!["suspended".to_owned()]
        )]
    );
}

/// A running warm-pool member is no user's sandbox: of a report with a
/// running pool member and a running user sandbox, only the user sandbox
/// joins the observed set. The stored report keeps the member flagged.
#[tokio::test]
async fn a_running_pool_member_is_not_observed() {
    let draining = node(Some("draining"), Some(5));
    let sink = FakeSink {
        nodes: vec![draining.clone()],
        responses: HashMap::from([(
            (draining.id, false),
            Ok(answer(
                5,
                DrainReport {
                    pending: vec![
                        DrainPending {
                            pool_member: true,
                            ..pending("member", DrainWait::Hold)
                        },
                        pending("user", DrainWait::Hold),
                    ],
                    ..DrainReport::default()
                },
            )),
        )]),
        ..FakeSink::default()
    };
    run_tick(&sink, &FakeAuthority::ok()).await;
    assert_eq!(sink.added(), [(vec!["user".to_owned()], Vec::new())]);
    let stored = sink.stored();
    assert!(stored.pending[0].pool_member && !stored.pending[1].pool_member);
}

/// A tick counts the sandboxes its drains have seen running that are still
/// suspended past the config's bound once, with that bound.
#[tokio::test(flavor = "current_thread")]
async fn a_tick_counts_still_suspended_rows_once_with_its_bound() {
    let counted = node(Some("drained"), Some(5));
    let none = node(Some("terminating"), Some(6));
    let sink = FakeSink {
        nodes: vec![counted.clone(), none.clone()],
        responses: HashMap::from([((none.id, false), Ok(response(6, &[])))]),
        fence: Some(Fence::Moved),
        still_suspended: HashMap::from([(counted.id, 3)]),
        ..FakeSink::default()
    };
    let config = V2LoopConfig {
        still_suspended_after: Duration::from_secs(42),
        ..V2LoopConfig::default()
    };
    tick(&sink, &FakeAuthority::ok(), &V2LoopState::new(config)).await;
    assert_eq!(*sink.counted.lock().unwrap(), [42]);
}

/// A tick that lists no v2 node, every tick with drain v2 off, counts
/// nothing and calls nothing: its list is the only statement it runs.
#[tokio::test]
async fn a_tick_with_no_v2_node_counts_nothing() {
    let sink = FakeSink::default();
    let report = run_tick(&sink, &FakeAuthority::ok()).await;
    assert_eq!(report, V2TickReport::default());
    assert!(sink.counted.lock().unwrap().is_empty());
    assert!(sink.calls().is_empty(), "{:?}", sink.calls());
}

/// The gauges count each row once: a row the node reports twice pending is
/// one pending row, a held row with work started for it is pending, and an
/// unrecorded evacuation is pending. `blocked` counts the node-wide
/// problems.
#[test]
fn a_row_is_counted_once_however_many_entries_name_it() {
    let report = DrainReport {
        evacuated: vec![evacuated("late")],
        pending: vec![
            pending("twice", DrainWait::External),
            pending("twice", DrainWait::Moving),
            pending("disk", DrainWait::Moving),
            pending("held", DrainWait::Hold),
            pending("busy", DrainWait::Hold),
            pending("busy", DrainWait::Moving),
        ],
        blocked: vec![
            blocked("orphan inventory scan failed"),
            blocked("untracked runtime process 42"),
        ],
    };
    let recording = Recording {
        records: vec![(
            "late".into(),
            EvacuationRecord::NotYet("not projected yet".into()),
        )],
        since: BTreeMap::from([("late".into(), 0)]),
        now: 60,
    };
    assert_eq!(
        outstanding_rows(&report, &recording),
        OutstandingRows {
            blocked: 2,
            pending: 4,
            held: 1,
        }
    );
}

const MINUTE: i64 = 60;
const HOUR: i64 = 60 * MINUTE;
const DAY: i64 = 24 * HOUR;

/// Before its deadline a drain is never stalled, however long its node has
/// evacuated nothing. Past it, a drain that isn't finished stalls once
/// nothing more has been evacuated for the bound, counted from the later of
/// the set's last growth and the deadline. A finished drain never stalls,
/// and one without a deadline counts from the last growth.
#[test]
fn a_drain_stalls_only_past_its_deadline_with_nothing_evacuated_for_the_bound() {
    let bound = V2LoopConfig::default().stall_bound;
    for (case, finished, deadline, grew_at, stalls) in [
        (
            "holding a day before the deadline",
            false,
            Some(DB_NOW + 1),
            DB_NOW - DAY,
            false,
        ),
        (
            "14 minutes past the deadline",
            false,
            Some(DB_NOW - 14 * MINUTE),
            DB_NOW - DAY,
            false,
        ),
        (
            "15 minutes past the deadline",
            false,
            Some(DB_NOW - 15 * MINUTE),
            DB_NOW - DAY,
            true,
        ),
        (
            "grew 14 minutes ago, past the deadline",
            false,
            Some(DB_NOW - HOUR),
            DB_NOW - 14 * MINUTE,
            false,
        ),
        (
            "grew 15 minutes ago, past the deadline",
            false,
            Some(DB_NOW - HOUR),
            DB_NOW - 15 * MINUTE,
            true,
        ),
        (
            "finished, an hour past the deadline",
            true,
            Some(DB_NOW - HOUR),
            DB_NOW - DAY,
            false,
        ),
        (
            "no deadline, grew 14 minutes ago",
            false,
            None,
            DB_NOW - 14 * MINUTE,
            false,
        ),
        (
            "no deadline, grew 15 minutes ago",
            false,
            None,
            DB_NOW - 15 * MINUTE,
            true,
        ),
    ] {
        let stall = stalled(finished, deadline, grew_at, DB_NOW, bound, 2, 1);
        assert_eq!(stall.is_some(), stalls, "{case}");
        if let Some(stall) = stall {
            assert!(
                stall.starts_with("the drain is stalled: no sandbox evacuated for over 15 minutes"),
                "{case}: {stall}"
            );
            assert!(stall.contains("(2 pending, 1 blocked)"), "{case}: {stall}");
        }
    }
    // A shorter bound, as the test hook sets it, stalls sooner.
    let stall = stalled(
        false,
        Some(DB_NOW - 30),
        DB_NOW - DAY,
        DB_NOW,
        Duration::from_secs(30),
        0,
        1,
    );
    assert!(
        stall.is_some_and(|stall| stall.contains("for over 30 seconds")),
        "a 30 s bound"
    );
}

/// The stall clock a report stores, which is what the next tick, by a
/// controller that restarted in between, reads back: a drain past its
/// deadline whose node reports no new evacuation stalls 15 minutes after
/// the deadline under another controller, and stays stalled; a report
/// naming a new evacuation clears it and starts the clock again. A hold
/// before the deadline is never stalled. The stall is in `drain_last_error`;
/// the stored stamp plus the bound is when it began.
#[tokio::test(flavor = "current_thread")]
async fn the_stall_clock_survives_a_controller_restart() {
    let deadline = DB_NOW + HOUR;
    let draining = V2Node {
        deadline_at: Some(deadline),
        evacuate: false,
        ..node(Some("draining"), Some(5))
    };
    let held = |evacuated_ids: &[&str]| {
        Ok(answer(
            5,
            DrainReport {
                evacuated: evacuated_ids.iter().map(|id| evacuated(id)).collect(),
                pending: vec![pending("running", DrainWait::Hold)],
                ..DrainReport::default()
            },
        ))
    };
    let first = FakeSink {
        nodes: vec![draining.clone()],
        responses: HashMap::from([((draining.id, false), held(&["a"]))]),
        ..FakeSink::default()
    };
    run_tick(&first, &FakeAuthority::ok()).await;
    let mut stored = first.stored();
    assert_eq!(stored.evacuated_grew_at, DB_NOW);

    for (case, at, evacuated_ids, grew_at, stalls) in [
        (
            "holding until just before the deadline",
            deadline - 1,
            &["a"][..],
            DB_NOW,
            false,
        ),
        (
            "14 minutes past the deadline",
            deadline + 14 * MINUTE,
            &["a"][..],
            deadline,
            false,
        ),
        (
            "15 minutes past the deadline",
            deadline + 15 * MINUTE,
            &["a"][..],
            deadline,
            true,
        ),
        (
            "still nothing new, later",
            deadline + HOUR,
            &["a"][..],
            deadline,
            true,
        ),
        (
            "a new evacuation",
            deadline + 2 * HOUR,
            &["a", "b"][..],
            deadline + 2 * HOUR,
            false,
        ),
        (
            "one fewer reported is no growth",
            deadline + 2 * HOUR + 15 * MINUTE,
            &["b"][..],
            deadline + 2 * HOUR,
            true,
        ),
    ] {
        // Another controller, with nothing but the stored report.
        let relisted = V2Node {
            db_now: at,
            evacuate: at >= deadline,
            report: Some(stored.clone()),
            ..draining.clone()
        };
        let next = FakeSink {
            nodes: vec![relisted.clone()],
            responses: HashMap::from([((relisted.id, false), held(evacuated_ids))]),
            ..FakeSink::default()
        };
        run_tick(&next, &FakeAuthority::ok()).await;
        stored = next.stored();
        assert_eq!(stored.evacuated_grew_at, grew_at, "{case}");
        assert_eq!(
            next.calls().iter().any(
                |call| call.starts_with(&format!("error {} the drain is stalled", relisted.id))
            ),
            stalls,
            "{case}: {:?}",
            next.calls()
        );
    }
}

/// Each bound has its test hook, read in whole seconds through
/// `pplx_space_test_hooks::test_hook`; an unset, zero or unreadable hook
/// keeps the default, and the dispatch base never exceeds its cap.
#[test]
fn the_loop_config_reads_its_test_hooks() {
    let config = |vars: &[(&str, &str)]| {
        let mut env = pplx_space_test_env::TestEnv::default();
        for (name, value) in vars {
            env.set(name, value);
        }
        V2LoopConfig::from_environment(&env)
    };
    assert_eq!(config(&[]), V2LoopConfig::default());
    assert_eq!(
        V2LoopConfig::default(),
        V2LoopConfig {
            stall_bound: Duration::from_secs(900),
            still_suspended_after: Duration::from_secs(900),
            terminate_bound: Duration::from_secs(900),
            dispatch_backoff_base: Duration::from_secs(10),
        }
    );
    assert_eq!(
        config(&[
            (STALL_BOUND_TEST_ENV, "30"),
            (STILL_SUSPENDED_AFTER_TEST_ENV, "40"),
            (TERMINATE_BOUND_TEST_ENV, "50"),
            (DISPATCH_BACKOFF_BASE_TEST_ENV, "2"),
        ]),
        V2LoopConfig {
            stall_bound: Duration::from_secs(30),
            still_suspended_after: Duration::from_secs(40),
            terminate_bound: Duration::from_secs(50),
            dispatch_backoff_base: Duration::from_secs(2),
        }
    );
    assert_eq!(
        config(&[
            (STALL_BOUND_TEST_ENV, "0"),
            (TERMINATE_BOUND_TEST_ENV, "soon")
        ]),
        V2LoopConfig::default()
    );
    assert_eq!(
        config(&[(DISPATCH_BACKOFF_BASE_TEST_ENV, "600")]).dispatch_backoff_base,
        DISPATCH_BACKOFF_CAP
    );
}
