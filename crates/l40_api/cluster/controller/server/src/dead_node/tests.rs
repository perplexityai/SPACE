//! The loop against fakes of its sink, Node membership and EC2, with paused
//! time for the absence window. The Postgres-backed ticks are in [`live`].

use pplx_space_common_core::types::{SandboxId, SandboxLifecycle, SubstrateKind};

use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use pplx_space_sandbox_store::StoredSandboxLifecycle;
use pplx_space_sandbox_store::dead_node::SourceExecution;
use pplx_space_test_env::TestEnv;

use super::verdict::{DeadReason, LostReason, UnknownReason};
use super::*;

mod live;

const WINDOW: Duration = Duration::from_secs(600);
const DEAD_NAME: &str = "worker-dead.synthetic.internal";
const DEAD_UID: &str = "uid-dead";
const REFERENCE_UID: &str = "uid-live";
/// The dead node's recorded instance.
const DEAD_INSTANCE: &str = "i-0000000000000dead";
/// An instance a same-name predecessor ran on, also dead.
const OLDER_INSTANCE: &str = "i-0000000000000beef";
/// A same-name replacement's instance, live.
const REPLACEMENT_INSTANCE: &str = "i-00000000000000a11";
const REFERENCE_INSTANCE: &str = "i-00000000000000fee";

// -----------------------------------------------------------------
// Fakes
// -----------------------------------------------------------------

/// One write the fake sink was asked for.
#[derive(Debug, Clone, PartialEq, Eq)]
struct RecordedWrite {
    sandbox_id: SandboxId,
    outcome: RowOutcome,
    basis: WriteBasis,
}

struct FakeSink {
    nodes: Mutex<Vec<CandidateNode>>,
    references: Vec<ReferenceNode>,
    rows: Vec<DeadNodeRow>,
    writes: Mutex<Vec<RecordedWrite>>,
    write_result: Result<WriteResult, ()>,
    deletes: Mutex<Vec<NodeId>>,
    /// Each written status's mode, verdict and reason.
    statuses: Mutex<Vec<(String, String, String)>>,
}

impl FakeSink {
    fn with_node(node: CandidateNode) -> Self {
        Self {
            nodes: Mutex::new(vec![node]),
            references: vec![ReferenceNode {
                k8s_node_uid: REFERENCE_UID.to_owned(),
                instance_id: REFERENCE_INSTANCE.to_owned(),
            }],
            rows: Vec::new(),
            writes: Mutex::new(Vec::new()),
            write_result: Ok(WriteResult::Applied),
            deletes: Mutex::new(Vec::new()),
            statuses: Mutex::new(Vec::new()),
        }
    }

    fn writes(&self) -> Vec<RecordedWrite> {
        self.writes.lock().unwrap().clone()
    }
}

impl DeadNodeSink for Arc<FakeSink> {
    async fn candidate_nodes(
        &self,
        _managed_by: SubstrateKind,
        after: Option<NodeId>,
        limit: u32,
    ) -> Result<Vec<CandidateNode>, sqlx::Error> {
        let nodes = self.nodes.lock().unwrap();
        Ok(nodes
            .iter()
            .filter(|node| after.is_none_or(|after| node.node_id > after))
            .take(limit as usize)
            .cloned()
            .collect())
    }

    async fn reference_nodes(
        &self,
        _managed_by: SubstrateKind,
        _limit: u32,
    ) -> Result<Vec<ReferenceNode>, sqlx::Error> {
        Ok(self.references.clone())
    }

    async fn rows_on_node(
        &self,
        _node: &CandidateNode,
        _upload_settle: Duration,
        limit: u32,
    ) -> Result<Vec<DeadNodeRow>, sqlx::Error> {
        Ok(self.rows.iter().take(limit as usize).cloned().collect())
    }

    async fn write_row(
        &self,
        _node: &CandidateNode,
        write: &RowWrite<'_>,
        _upload_settle: Duration,
    ) -> Result<WriteResult, sqlx::Error> {
        self.writes.lock().unwrap().push(RecordedWrite {
            sandbox_id: write.sandbox_id,
            outcome: write.outcome,
            basis: write.basis,
        });
        self.write_result
            .map_err(|()| sqlx::Error::Protocol("synthetic write failure".to_owned()))
    }

    async fn delete_node(&self, node: &CandidateNode) -> Result<bool, sqlx::Error> {
        self.deletes.lock().unwrap().push(node.node_id);
        Ok(true)
    }

    async fn write_status(&self, status: &DeadNodeStatusWrite<'_>) -> Result<bool, sqlx::Error> {
        self.statuses.lock().unwrap().push((
            status.mode.to_owned(),
            status.verdict.to_owned(),
            status.reason.to_owned(),
        ));
        Ok(true)
    }
}

#[derive(Debug, thiserror::Error)]
#[error("synthetic lookup failure")]
struct LookupFailed;

struct FakeMembership {
    uids: Mutex<Option<HashSet<String>>>,
    exists: Mutex<HashMap<String, Result<bool, ()>>>,
}

impl FakeMembership {
    /// The reflector lists only the reference node; the API server has no
    /// Node by the dead node's name.
    fn dead_node_gone() -> Arc<Self> {
        Arc::new(Self {
            uids: Mutex::new(Some(HashSet::from([REFERENCE_UID.to_owned()]))),
            exists: Mutex::new(HashMap::from([(DEAD_NAME.to_owned(), Ok(false))])),
        })
    }

    fn set_uids(&self, uids: Option<&[&str]>) {
        *self.uids.lock().unwrap() =
            uids.map(|uids| uids.iter().map(|uid| (*uid).to_owned()).collect());
    }

    fn set_exists(&self, name: &str, answer: Result<bool, ()>) {
        self.exists.lock().unwrap().insert(name.to_owned(), answer);
    }
}

impl NodeMembership for Arc<FakeMembership> {
    type LookupError = LookupFailed;

    fn live_node_uids(&self) -> Option<HashSet<String>> {
        self.uids.lock().unwrap().clone()
    }

    async fn node_exists(&self, node_name: String) -> Result<bool, Self::LookupError> {
        match self.exists.lock().unwrap().get(&node_name) {
            Some(Ok(exists)) => Ok(*exists),
            Some(Err(())) | None => Err(LookupFailed),
        }
    }
}

/// EC2 by instance ID: each listed ID answers its states, in order, as one
/// entry each; an unlisted ID is absent. `failure` fails every call.
#[derive(Default)]
struct FakeEc2 {
    listed: Mutex<HashMap<String, Vec<&'static str>>>,
    failure: Mutex<Option<String>>,
    calls: Mutex<Vec<Vec<String>>>,
}

impl FakeEc2 {
    /// Every instance this file names is terminated but the replacement and
    /// the reference, which run.
    fn dead_instances() -> Arc<Self> {
        let ec2 = Self::default();
        for id in [DEAD_INSTANCE, OLDER_INSTANCE] {
            ec2.list(id, &["terminated"]);
        }
        ec2.list(REPLACEMENT_INSTANCE, &["running"]);
        ec2.list(REFERENCE_INSTANCE, &["running"]);
        Arc::new(ec2)
    }

    fn list(&self, id: &str, states: &[&'static str]) {
        self.listed
            .lock()
            .unwrap()
            .insert(id.to_owned(), states.to_vec());
    }

    fn unlist(&self, id: &str) {
        self.listed.lock().unwrap().remove(id);
    }

    fn calls(&self) -> Vec<Vec<String>> {
        self.calls.lock().unwrap().clone()
    }
}

impl InstanceIdLookup for FakeEc2 {
    fn describe_instances_by_ids(
        &self,
        instance_ids: Vec<String>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<MatchedInstance>, String>> + Send + '_>> {
        self.calls.lock().unwrap().push(instance_ids.clone());
        let failure = self.failure.lock().unwrap().clone();
        let listed = self.listed.lock().unwrap().clone();
        Box::pin(async move {
            if let Some(failure) = failure {
                return Err(failure);
            }
            Ok(instance_ids
                .iter()
                .flat_map(|id| {
                    listed
                        .get(id)
                        .into_iter()
                        .flatten()
                        .map(|state| MatchedInstance {
                            instance_id: id.clone(),
                            state: (*state).to_owned(),
                        })
                })
                .collect())
        })
    }
}

fn dead_node(uid: &str) -> CandidateNode {
    CandidateNode {
        node_id: NodeId::from_uuid(uuid::Uuid::from_u128(1)),
        managed_by: SubstrateKind::Kubernetes,
        k8s_node_name: Some(DEAD_NAME.to_owned()),
        k8s_node_uid: Some(uid.to_owned()),
        instance_id: Some(DEAD_INSTANCE.to_owned()),
        heartbeat: Heartbeat::Stale,
    }
}

/// The dead node's id and name, now held by a live replacement on
/// [`REPLACEMENT_INSTANCE`].
fn replaced_node() -> CandidateNode {
    CandidateNode {
        k8s_node_uid: Some("uid-replacement".to_owned()),
        instance_id: Some(REPLACEMENT_INSTANCE.to_owned()),
        heartbeat: Heartbeat::Fresh,
        ..dead_node(DEAD_UID)
    }
}

fn row(lifecycle: SandboxLifecycle, stamp: Option<Option<&str>>) -> DeadNodeRow {
    let lifecycle = StoredSandboxLifecycle::Live(lifecycle);
    DeadNodeRow {
        sandbox_id: SandboxId::new(),
        lifecycle: Some(lifecycle),
        stored_lifecycle: lifecycle.as_str().to_owned(),
        covered_by_snapshot: false,
        ready_snapshot: false,
        stop_artifact_durable: false,
        upload_pending: false,
        execution_elsewhere: false,
        source_execution: stamp.map(|instance_id| SourceExecution {
            id: uuid::Uuid::now_v7(),
            instance_id: instance_id.map(str::to_owned),
            active: true,
        }),
        reported_by_live_node: false,
        restore_owned: false,
    }
}

fn started(stamp: Option<&str>) -> DeadNodeRow {
    row(SandboxLifecycle::Started, Some(stamp))
}

fn config(mode: Mode) -> ReconcilerConfig {
    ReconcilerConfig {
        absence_window: WINDOW,
        ..ReconcilerConfig::with_mode(mode)
    }
}

fn reconciler<K: NodeMembership>(
    mode: Mode,
    sink: &Arc<FakeSink>,
    membership: K,
    authority: InstanceAuthority,
) -> Reconciler<Arc<FakeSink>, K> {
    Reconciler::new(
        config(mode),
        sink.clone(),
        membership,
        authority,
        SubstrateKind::Kubernetes,
    )
}

fn ec2_reconciler(
    mode: Mode,
    sink: &Arc<FakeSink>,
    ec2: &Arc<FakeEc2>,
) -> Reconciler<Arc<FakeSink>, Arc<FakeMembership>> {
    reconciler(
        mode,
        sink,
        FakeMembership::dead_node_gone(),
        InstanceAuthority::Ec2(ec2.clone()),
    )
}

fn verdicts(report: &TickReport) -> Vec<Verdict> {
    report.nodes.iter().map(|node| node.verdict).collect()
}

const LOST: Verdict = Verdict::Lost(LostReason::InstanceAndNodeGone);

// -----------------------------------------------------------------
// Config
// -----------------------------------------------------------------

/// Off unless set to `dry-run` or `live`, and off has no config: unset,
/// empty, `off`, a value for a mode that doesn't exist, or a typo. The
/// windows move only through the test hooks.
#[test]
fn mode_is_off_unless_dry_run_or_live_and_windows_move_only_by_test_hook() {
    let mut env = TestEnv::default();
    assert!(ReconcilerConfig::from_environment(&env).is_none(), "unset");
    for raw in ["off", "", "write", "dry_run", "true", "Live", "live-ish"] {
        env.set(MODE_ENV, raw);
        assert!(
            ReconcilerConfig::from_environment(&env).is_none(),
            "{raw:?}"
        );
    }
    for (raw, mode) in [
        ("dry-run", Mode::DryRun),
        (" dry-run ", Mode::DryRun),
        ("live", Mode::Live),
        (" live ", Mode::Live),
    ] {
        env.set(MODE_ENV, raw);
        let parsed = ReconcilerConfig::from_environment(&env).expect(raw);
        assert_eq!(parsed.mode, mode, "{raw:?}");
        assert_eq!(parsed.is_live(), mode == Mode::Live, "{raw:?}");
        assert_eq!(parsed.tick, DEFAULT_TICK);
        assert_eq!(parsed.absence_window, DEFAULT_ABSENCE_GRACE);
        assert_eq!(parsed.upload_settle, DEAD_UPLOAD_SETTLE);
        assert_eq!(parsed.max_writes_per_tick, MAX_WRITES_PER_TICK);
        assert!(!parsed.force_ec2);
    }
    env.set("SPACE_DEAD_NODE_TEST_TICK_SECS", "5");
    env.set("SPACE_DEAD_NODE_TEST_ABSENCE_WINDOW_SECS", "20");
    env.set("SPACE_DEAD_NODE_TEST_UPLOAD_SETTLE_SECS", "30");
    env.set("SPACE_DEAD_NODE_TEST_INSTANCE_AUTHORITY", "ec2");
    let hooked = ReconcilerConfig::from_environment(&env).expect("live");
    assert_eq!(hooked.tick, Duration::from_secs(5));
    assert_eq!(hooked.absence_window, Duration::from_secs(20));
    assert_eq!(hooked.upload_settle, Duration::from_secs(30));
    assert!(hooked.force_ec2);
    env.set(MODE_ENV, "off");
    assert!(
        ReconcilerConfig::from_environment(&env).is_none(),
        "hooked, off"
    );
}

// -----------------------------------------------------------------
// Node verdict evidence
// -----------------------------------------------------------------

/// The node is lost once its recorded Node has been absent for the whole
/// window, the API server has no Node by its name, and its recorded instance
/// is dead; before then it is settling. Each judged node gets a status.
#[tokio::test(start_paused = true)]
async fn node_is_lost_only_after_the_window() {
    let sink = Arc::new(FakeSink::with_node(dead_node(DEAD_UID)));
    let mut reconciler = ec2_reconciler(Mode::DryRun, &sink, &FakeEc2::dead_instances());
    let settling = Verdict::Unknown(UnknownReason::KubeAbsenceSettling);

    assert_eq!(verdicts(&reconciler.tick().await), vec![settling]);
    tokio::time::advance(WINDOW - Duration::from_secs(1)).await;
    assert_eq!(verdicts(&reconciler.tick().await), vec![settling]);
    tokio::time::advance(Duration::from_secs(1)).await;
    assert_eq!(verdicts(&reconciler.tick().await), vec![LOST]);
    let statuses = sink.statuses.lock().unwrap().clone();
    assert_eq!(
        statuses
            .iter()
            .map(|(mode, verdict, reason)| (mode.as_str(), verdict.as_str(), reason.as_str()))
            .collect::<Vec<_>>(),
        [
            ("dry-run", "unknown", "kube_absence_settling"),
            ("dry-run", "unknown", "kube_absence_settling"),
            ("dry-run", "lost", "instance_and_node_gone"),
        ]
    );
}

/// A replacement Node with the dead node's name and a new uid holds the node
/// verdict at unknown: the recorded uid is absent, but the direct GET finds
/// the name. When the row records the replacement's uid, the window restarts.
#[tokio::test(start_paused = true)]
async fn same_name_replacement_keeps_the_node_unknown() {
    let sink = Arc::new(FakeSink::with_node(dead_node(DEAD_UID)));
    let membership = FakeMembership::dead_node_gone();
    membership.set_uids(Some(&[REFERENCE_UID, "uid-replacement"]));
    membership.set_exists(DEAD_NAME, Ok(true));
    let mut reconciler = reconciler(
        Mode::DryRun,
        &sink,
        membership.clone(),
        InstanceAuthority::Ec2(FakeEc2::dead_instances()),
    );

    reconciler.tick().await;
    tokio::time::advance(WINDOW).await;
    assert_eq!(
        verdicts(&reconciler.tick().await),
        vec![Verdict::Unknown(UnknownReason::KubeNodeFound)]
    );

    membership.set_uids(Some(&[REFERENCE_UID]));
    membership.set_exists(DEAD_NAME, Ok(false));
    *sink.nodes.lock().unwrap() = vec![dead_node("uid-replacement")];
    assert_eq!(
        verdicts(&reconciler.tick().await),
        vec![Verdict::Unknown(UnknownReason::KubeAbsenceSettling)]
    );
    tokio::time::advance(WINDOW).await;
    assert_eq!(verdicts(&reconciler.tick().await), vec![LOST]);
}

/// A cold or empty reflector reads unknown and forgets every absence, and
/// the recorded uid's return ends one; either way the window starts over.
#[tokio::test(start_paused = true)]
async fn unknown_membership_or_a_returning_uid_restarts_the_window() {
    let sink = Arc::new(FakeSink::with_node(dead_node(DEAD_UID)));
    let membership = FakeMembership::dead_node_gone();
    let mut reconciler = reconciler(
        Mode::DryRun,
        &sink,
        membership.clone(),
        InstanceAuthority::Ec2(FakeEc2::dead_instances()),
    );
    let settling = Verdict::Unknown(UnknownReason::KubeAbsenceSettling);

    for interruption in [None, Some(&[][..]), Some(&[REFERENCE_UID, DEAD_UID][..])] {
        reconciler.tick().await;
        tokio::time::advance(WINDOW).await;
        membership.set_uids(interruption);
        let expected = match interruption {
            Some([_, _]) => UnknownReason::KubeNodePresent,
            // No reflector sample also leaves no reference, but the Node
            // evidence is read first.
            _ => UnknownReason::KubeMembershipUnknown,
        };
        assert_eq!(
            verdicts(&reconciler.tick().await),
            vec![Verdict::Unknown(expected)],
            "{interruption:?}"
        );
        membership.set_uids(Some(&[REFERENCE_UID]));
        assert_eq!(
            verdicts(&reconciler.tick().await),
            vec![settling],
            "{interruption:?}"
        );
        tokio::time::advance(WINDOW).await;
        assert_eq!(
            verdicts(&reconciler.tick().await),
            vec![LOST],
            "{interruption:?}"
        );
        reconciler.absent_since.clear();
    }
}

/// Without an instance authority (`kube-delete`, `noop`, Direct), a node whose
/// Node is gone reads unknown, and live mode writes nothing for it.
#[tokio::test(start_paused = true)]
async fn without_an_instance_authority_a_death_reads_unknown_and_nothing_is_written() {
    let sink = Arc::new(FakeSink {
        rows: vec![started(Some(DEAD_INSTANCE)), started(None)],
        ..FakeSink::with_node(dead_node(DEAD_UID))
    });
    let mut reconciler = reconciler(
        Mode::Live,
        &sink,
        FakeMembership::dead_node_gone(),
        InstanceAuthority::None,
    );
    reconciler.tick().await;
    tokio::time::advance(WINDOW).await;
    let report = reconciler.tick().await;
    assert_eq!(
        verdicts(&report),
        vec![Verdict::Unknown(UnknownReason::NoInstanceAuthority)]
    );
    assert_eq!(
        report.nodes[0].counts,
        BTreeMap::from([(RowOutcome::Unselected, 2)])
    );
    assert_eq!(sink.writes(), vec![]);
    assert!(sink.deletes.lock().unwrap().is_empty());
}

/// The node's recorded instance decides the node verdict: absent while the
/// reference is listed is dead like `terminated`; live, a missing or
/// terminated reference, a failed call, a malformed recorded ID and no
/// recorded instance each read unknown.
#[tokio::test(start_paused = true)]
async fn the_recorded_instance_decides_the_node_verdict() {
    type Setup = fn(&FakeEc2, &mut CandidateNode);
    let cases: [(&str, Setup, Verdict); 8] = [
        ("terminated", |_, _| {}, LOST),
        ("absent", |ec2, _| ec2.unlist(DEAD_INSTANCE), LOST),
        (
            "stopped is live",
            |ec2, _| ec2.list(DEAD_INSTANCE, &["stopped"]),
            Verdict::Unknown(UnknownReason::InstanceLive),
        ),
        (
            "a live entry beside a terminated one",
            |ec2, _| ec2.list(DEAD_INSTANCE, &["terminated", "pending"]),
            Verdict::Unknown(UnknownReason::InstanceLive),
        ),
        (
            "reference absent",
            |ec2, _| ec2.unlist(REFERENCE_INSTANCE),
            Verdict::Unknown(UnknownReason::ReferenceMissing),
        ),
        (
            "call fails",
            |ec2, _| *ec2.failure.lock().unwrap() = Some("throttled".to_owned()),
            Verdict::Unknown(UnknownReason::InstanceLookupFailed),
        ),
        (
            "malformed recorded instance",
            |_, node| node.instance_id = Some("i-DEAD".to_owned()),
            Verdict::Unknown(UnknownReason::MalformedInstanceId),
        ),
        (
            "no recorded instance",
            |_, node| node.instance_id = None,
            Verdict::Unknown(UnknownReason::NoRecordedInstance),
        ),
    ];
    for (case, setup, expected) in cases {
        let ec2 = FakeEc2::dead_instances();
        let mut node = dead_node(DEAD_UID);
        setup(&ec2, &mut node);
        let sink = Arc::new(FakeSink::with_node(node));
        let mut reconciler = ec2_reconciler(Mode::DryRun, &sink, &ec2);
        reconciler.tick().await;
        tokio::time::advance(WINDOW).await;
        assert_eq!(verdicts(&reconciler.tick().await), vec![expected], "{case}");
    }
}

/// The reference must be a fresh node the reflector lists with a well-formed
/// instance; one lookup serves every node of a tick, and each node's IDs go
/// in one call with the reference, malformed ones left out.
#[tokio::test(start_paused = true)]
async fn one_call_per_node_asks_its_instances_with_the_reference() {
    let sink = Arc::new(FakeSink {
        references: vec![
            ReferenceNode {
                k8s_node_uid: "uid-unlisted".to_owned(),
                instance_id: "i-00000000000000bad".to_owned(),
            },
            ReferenceNode {
                k8s_node_uid: REFERENCE_UID.to_owned(),
                instance_id: "i-not-hex".to_owned(),
            },
            ReferenceNode {
                k8s_node_uid: REFERENCE_UID.to_owned(),
                instance_id: REFERENCE_INSTANCE.to_owned(),
            },
        ],
        rows: vec![
            started(Some(OLDER_INSTANCE)),
            started(Some(OLDER_INSTANCE)),
            started(Some("i-MALFORMED")),
        ],
        ..FakeSink::with_node(dead_node(DEAD_UID))
    });
    let mut second = dead_node("uid-second");
    second.node_id = NodeId::from_uuid(uuid::Uuid::from_u128(2));
    sink.nodes.lock().unwrap().push(second);
    let ec2 = FakeEc2::dead_instances();
    let mut reconciler = ec2_reconciler(Mode::DryRun, &sink, &ec2);

    let report = reconciler.tick().await;

    let expected_call: Vec<String> = [OLDER_INSTANCE, DEAD_INSTANCE, REFERENCE_INSTANCE]
        .map(str::to_owned)
        .to_vec();
    assert_eq!(ec2.calls(), vec![expected_call.clone(), expected_call]);
    assert_eq!(
        report.nodes[0].instances,
        BTreeMap::from([
            (
                DEAD_INSTANCE.to_owned(),
                InstanceVerdict::Dead(DeadReason::Terminated)
            ),
            (
                OLDER_INSTANCE.to_owned(),
                InstanceVerdict::Dead(DeadReason::Terminated)
            ),
            (
                "i-MALFORMED".to_owned(),
                InstanceVerdict::Unknown(UnknownReason::MalformedInstanceId)
            ),
        ])
    );

    // With no usable reference no call is made and every instance is
    // unknown.
    let unusable = Arc::new(FakeSink {
        references: sink.references[..2].to_vec(),
        rows: sink.rows.clone(),
        ..FakeSink::with_node(dead_node(DEAD_UID))
    });
    let ec2 = FakeEc2::dead_instances();
    let report = ec2_reconciler(Mode::DryRun, &unusable, &ec2).tick().await;
    assert_eq!(ec2.calls(), Vec::<Vec<String>>::new());
    assert_eq!(
        report.nodes[0].instances[OLDER_INSTANCE],
        InstanceVerdict::Unknown(UnknownReason::ReferenceUnavailable)
    );
}

// -----------------------------------------------------------------
// Selection
// -----------------------------------------------------------------

/// On a replaced node (same id and name, fresh heartbeat, a new recorded
/// instance) only rows stamped with a dead instance are selected. The
/// replacement's own rows, rows stamped with a live or an unknown instance,
/// and rows with no stamp are left alone, and the recorded instance is not
/// asked about: the heartbeat comes from it.
#[tokio::test(start_paused = true)]
async fn a_replaced_node_reconciles_only_rows_stamped_with_a_dead_instance() {
    let dead_stamped = started(Some(DEAD_INSTANCE));
    let older_stamped = row(SandboxLifecycle::Paused, Some(Some(OLDER_INSTANCE)));
    let replacement_stamped = started(Some(REPLACEMENT_INSTANCE));
    let live_elsewhere = started(Some("i-000000000000000aa"));
    let null_stamped = started(None);
    let no_execution = row(SandboxLifecycle::Started, None);
    let sink = Arc::new(FakeSink {
        rows: vec![
            dead_stamped.clone(),
            older_stamped.clone(),
            replacement_stamped,
            live_elsewhere,
            null_stamped,
            no_execution,
        ],
        ..FakeSink::with_node(replaced_node())
    });
    let ec2 = FakeEc2::dead_instances();
    ec2.list("i-000000000000000aa", &["running"]);
    let report = ec2_reconciler(Mode::Live, &sink, &ec2).tick().await;

    let node = &report.nodes[0];
    assert_eq!(
        node.verdict,
        Verdict::Unknown(UnknownReason::HeartbeatFresh)
    );
    assert_eq!(
        node.counts,
        BTreeMap::from([(RowOutcome::Lost, 2), (RowOutcome::Unselected, 4)])
    );
    assert_eq!(
        sink.writes(),
        [dead_stamped, older_stamped]
            .iter()
            .map(|row| RecordedWrite {
                sandbox_id: row.sandbox_id,
                outcome: RowOutcome::Lost,
                basis: WriteBasis::InstanceDead,
            })
            .collect::<Vec<_>>()
    );
    let asked = ec2.calls();
    assert_eq!(asked.len(), 1);
    assert!(
        !asked[0].contains(&REPLACEMENT_INSTANCE.to_owned()),
        "{asked:?}"
    );
    assert!(sink.deletes.lock().unwrap().is_empty());
}

/// Rows with no stamped execution on the node are selected only when no live
/// machine holds the node id: with the node lost, and not while its
/// heartbeat is fresh or its Node is still listed.
#[tokio::test(start_paused = true)]
async fn unstamped_rows_need_the_node_lost() {
    for (case, heartbeat, node_listed, selected) in [
        ("lost", Heartbeat::Stale, false, true),
        ("fresh heartbeat", Heartbeat::Fresh, false, false),
        ("Node still listed", Heartbeat::Stale, true, false),
    ] {
        let null_stamped = started(None);
        let no_execution = row(SandboxLifecycle::Suspended, None);
        let sink = Arc::new(FakeSink {
            rows: vec![null_stamped.clone(), no_execution.clone()],
            ..FakeSink::with_node(CandidateNode {
                heartbeat,
                ..dead_node(DEAD_UID)
            })
        });
        let membership = FakeMembership::dead_node_gone();
        if node_listed {
            membership.set_uids(Some(&[REFERENCE_UID, DEAD_UID]));
        }
        let mut reconciler = reconciler(
            Mode::Live,
            &sink,
            membership,
            InstanceAuthority::Ec2(FakeEc2::dead_instances()),
        );
        reconciler.tick().await;
        tokio::time::advance(WINDOW).await;
        sink.writes.lock().unwrap().clear();
        let report = reconciler.tick().await;
        let expected_writes: Vec<RecordedWrite> = if selected {
            [
                (&null_stamped, RowOutcome::Lost),
                (&no_execution, RowOutcome::Lost),
            ]
            .iter()
            .map(|(row, outcome)| RecordedWrite {
                sandbox_id: row.sandbox_id,
                outcome: *outcome,
                basis: WriteBasis::NodeLost,
            })
            .collect()
        } else {
            vec![]
        };
        assert_eq!(sink.writes(), expected_writes, "{case}");
        assert_eq!(report.nodes[0].verdict.is_lost(), selected, "{case}");
        assert_eq!(report.nodes[0].deleted, selected, "{case}");
    }
}

// -----------------------------------------------------------------
// Live writes and caps
// -----------------------------------------------------------------

/// A dry run decides the same outcomes and writes nothing; live mode writes
/// only the outcomes that write, deletes a lost node once its writes
/// succeeded, and keeps no status for a deleted node.
#[tokio::test(start_paused = true)]
async fn live_mode_writes_what_a_dry_run_only_reports() {
    let mut held = started(Some(DEAD_INSTANCE));
    held.reported_by_live_node = true;
    let mut restoring = row(SandboxLifecycle::Restoring, Some(Some(DEAD_INSTANCE)));
    restoring.restore_owned = true;
    let lost = started(Some(DEAD_INSTANCE));
    let rows = vec![held, restoring, lost.clone()];
    let mut counts = None;
    for mode in [Mode::DryRun, Mode::Live] {
        let sink = Arc::new(FakeSink {
            rows: rows.clone(),
            ..FakeSink::with_node(dead_node(DEAD_UID))
        });
        let mut reconciler = ec2_reconciler(mode, &sink, &FakeEc2::dead_instances());
        reconciler.tick().await;
        tokio::time::advance(WINDOW).await;
        sink.writes.lock().unwrap().clear();
        sink.statuses.lock().unwrap().clear();
        let report = reconciler.tick().await;
        let node = &report.nodes[0];
        assert_eq!(node.verdict, LOST, "{mode:?}");
        let decided = BTreeMap::from([
            (RowOutcome::HeldByPublication, 1),
            (RowOutcome::HeldByRestore, 1),
            (RowOutcome::Lost, 1),
        ]);
        assert_eq!(node.counts, decided, "{mode:?}");
        counts.get_or_insert(node.counts.clone());
        let live = mode == Mode::Live;
        assert_eq!(
            sink.writes(),
            if live {
                vec![RecordedWrite {
                    sandbox_id: lost.sandbox_id,
                    outcome: RowOutcome::Lost,
                    basis: WriteBasis::InstanceDead,
                }]
            } else {
                vec![]
            },
            "{mode:?}"
        );
        assert_eq!(
            node.applied,
            if live {
                BTreeMap::from([(RowOutcome::Lost, 1)])
            } else {
                BTreeMap::new()
            },
            "{mode:?}"
        );
        assert_eq!(node.deleted, live, "{mode:?}");
        assert_eq!(sink.statuses.lock().unwrap().len(), usize::from(!live));
    }
}

/// A failed or refused write leaves the node row for the next sweep: a
/// failure skips the delete, a refusal counts nothing applied.
#[tokio::test(start_paused = true)]
async fn a_failed_write_keeps_the_node_and_a_refusal_applies_nothing() {
    for (result, delete_attempted) in [
        (Err(()), false),
        (Ok(WriteResult::RowChanged), true),
        (Ok(WriteResult::NodeChanged), true),
        (Ok(WriteResult::NothingToChange), true),
    ] {
        let sink = Arc::new(FakeSink {
            rows: vec![started(Some(DEAD_INSTANCE))],
            write_result: result,
            ..FakeSink::with_node(dead_node(DEAD_UID))
        });
        let mut reconciler = ec2_reconciler(Mode::Live, &sink, &FakeEc2::dead_instances());
        reconciler.tick().await;
        tokio::time::advance(WINDOW).await;
        sink.deletes.lock().unwrap().clear();
        let report = reconciler.tick().await;
        assert_eq!(report.nodes[0].applied, BTreeMap::new(), "{result:?}");
        assert_eq!(report.errors, u32::from(result.is_err()), "{result:?}");
        assert_eq!(
            !sink.deletes.lock().unwrap().is_empty(),
            delete_attempted,
            "{result:?}"
        );
    }
}

/// No more rows than the write cap are written in a tick, across nodes; the
/// rest are written on the next.
#[tokio::test(start_paused = true)]
async fn writes_past_the_cap_wait_for_the_next_tick() {
    let sink = Arc::new(FakeSink {
        rows: (0..3).map(|_| started(Some(DEAD_INSTANCE))).collect(),
        ..FakeSink::with_node(replaced_node())
    });
    let mut second = replaced_node();
    second.node_id = NodeId::from_uuid(uuid::Uuid::from_u128(2));
    sink.nodes.lock().unwrap().push(second);
    let mut reconciler = Reconciler::new(
        ReconcilerConfig {
            max_writes_per_tick: 4,
            ..config(Mode::Live)
        },
        sink.clone(),
        FakeMembership::dead_node_gone(),
        InstanceAuthority::Ec2(FakeEc2::dead_instances()),
        SubstrateKind::Kubernetes,
    );
    let report = reconciler.tick().await;
    assert_eq!(sink.writes().len(), 4);
    let applied: Vec<u64> = report
        .nodes
        .iter()
        .map(|node| node.applied.values().sum())
        .collect();
    assert_eq!(applied, [3, 1]);
    reconciler.tick().await;
    assert_eq!(sink.writes().len(), 8);
}

/// A node holding more rows than the cap reports the cap's worth, says it
/// was truncated, and is not deleted; each outcome lists at most its sample
/// count.
#[tokio::test(start_paused = true)]
async fn rows_past_the_cap_are_reported_truncated_and_block_the_delete() {
    let cap = MAX_ROWS_PER_NODE as usize;
    let sink = Arc::new(FakeSink {
        rows: (0..=cap).map(|_| started(Some(DEAD_INSTANCE))).collect(),
        ..FakeSink::with_node(dead_node(DEAD_UID))
    });
    let mut reconciler = ec2_reconciler(Mode::Live, &sink, &FakeEc2::dead_instances());
    reconciler.tick().await;
    tokio::time::advance(WINDOW).await;
    sink.deletes.lock().unwrap().clear();
    let report = reconciler.tick().await;
    let node = &report.nodes[0];
    assert_eq!(node.verdict, LOST);
    assert!(node.rows_truncated);
    assert_eq!(
        node.counts,
        BTreeMap::from([(RowOutcome::Lost, cap as u64)])
    );
    assert_eq!(node.samples[&RowOutcome::Lost].len(), SAMPLES_PER_OUTCOME);
    assert!(sink.deletes.lock().unwrap().is_empty());

    let exact = Arc::new(FakeSink {
        rows: sink.rows[..cap].to_vec(),
        ..FakeSink::with_node(dead_node(DEAD_UID))
    });
    let mut reconciler = ec2_reconciler(Mode::DryRun, &exact, &FakeEc2::dead_instances());
    assert!(!reconciler.tick().await.nodes[0].rows_truncated);
}

/// No more than the node cap is judged in a tick, and the cursor moves on:
/// the next tick judges only the rest, and its short page ends the sweep.
#[tokio::test(start_paused = true)]
async fn nodes_past_the_cap_wait_for_the_next_tick() {
    let sink = Arc::new(FakeSink::with_node(dead_node(DEAD_UID)));
    *sink.nodes.lock().unwrap() = (0..=MAX_NODES_PER_TICK)
        .map(|n| {
            let mut node = dead_node(DEAD_UID);
            node.node_id = NodeId::from_uuid(uuid::Uuid::from_u128(u128::from(n) + 1));
            node
        })
        .collect();
    let mut reconciler = ec2_reconciler(Mode::DryRun, &sink, &FakeEc2::dead_instances());
    let report = reconciler.tick().await;
    assert_eq!(report.nodes.len(), MAX_NODES_PER_TICK as usize);
    assert_eq!(
        reconciler.scan_after,
        report.nodes.last().map(|node| node.node_id)
    );

    let ninth = sink.nodes.lock().unwrap()[MAX_NODES_PER_TICK as usize].node_id;
    let next = reconciler.tick().await;
    let judged: Vec<NodeId> = next.nodes.iter().map(|node| node.node_id).collect();
    assert_eq!(judged, vec![ninth]);
    assert_eq!(reconciler.scan_after, None);
}
