//! The coordinator against a fake nft that keeps one owned table per pinned
//! namespace, so the real renderer, outcome classification and checker run
//! without privilege. Slots pin distinct `/proc/self/ns` objects through
//! symlinks. `linux` repeats one case against nftables.

use std::collections::HashMap;
use std::os::unix::process::ExitStatusExt as _;
use std::process::{ExitStatus, Output};
use std::sync::Mutex;

use pplx_space_firewall_rules_protocol::decode_revision;
use rusqlite::params;
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::sync::mpsc;

use super::*;
use crate::firewall::assignment::IncomingAssignment;
use crate::slot_pool::NetworkPoolConfig;
use crate::slot_pool::reset::ResetClaim;

mod linux;

const NODE: NodeId = NodeId::from_uuid(Uuid::from_u128(1));
const SANDBOX: &str = "sandbox-a";
const DNS: &str = r#"{"id":"dns","all_of":[{"left":{"payload":{"protocol":"udp","field":"dport"}},"op":"==","right":53}]}"#;
const WEB: &str = r#"{"id":"web","all_of":[{"left":{"payload":{"protocol":"tcp","field":"dport"}},"op":"==","right":80}]}"#;

fn document(rule: &str) -> String {
    format!(r#"{{"schema_version":1,"rules":[{rule}]}}"#)
}

/// A store whose accepted assignment is `generation` with `rule`, if any.
fn store(accepted: Option<(u64, &str)>) -> (TempDir, Arc<FirewallAssignments>) {
    let dir = tempfile::tempdir().unwrap();
    let store = FirewallAssignments::open(&dir.path().join("assignment.json")).unwrap();
    if let Some((generation, rule)) = accepted {
        accept(&store, generation, rule);
    }
    (dir, Arc::new(store))
}

fn accept(store: &FirewallAssignments, generation: u64, rule: &str) {
    let document = document(rule);
    store
        .accept(
            NODE,
            IncomingAssignment {
                node_id: NODE,
                generation,
                revision_id: Uuid::from_u128(generation.into()),
                document: &document,
            },
        )
        .unwrap();
}

/// A pool whose reset wakeups are countable.
fn pool(slots: u32) -> (Arc<NetworkPool>, mpsc::Receiver<()>) {
    let mut pool = NetworkPool::open_in_memory_for_test(NetworkPoolConfig::new(slots));
    let (sender, wakeups) = mpsc::channel(8);
    Arc::get_mut(&mut pool).unwrap().job_tx = sender;
    (pool, wakeups)
}

fn set_slot(
    pool: &NetworkPool,
    slot_id: u32,
    state: SlotState,
    health: SlotHealth,
    sandbox: Option<&str>,
    generation: i64,
) {
    pool.db
        .lock()
        .unwrap()
        .execute(
            "UPDATE network_slots SET state = ?1, health = ?2, sandbox_id = ?3, generation = ?4
             WHERE slot_id = ?5",
            params![
                state.as_str(),
                health.as_str(),
                sandbox,
                generation,
                slot_id
            ],
        )
        .unwrap();
}

fn leased(pool: &NetworkPool, slot_id: u32) {
    set_slot(
        pool,
        slot_id,
        SlotState::Leased,
        SlotHealth::Healthy,
        Some(SANDBOX),
        1,
    );
}

fn ready(pool: &NetworkPool, slot_id: u32) {
    set_slot(
        pool,
        slot_id,
        SlotState::Ready,
        SlotHealth::Healthy,
        None,
        1,
    );
}

#[derive(Clone, Copy, Debug)]
enum Report {
    Exit(i32),
    TimedOut,
}

#[derive(Clone, Copy, Debug)]
enum Listing {
    Kernel,
    Fails,
}

/// Holds the next install after it commits until released.
struct Gate {
    entered: tokio::sync::oneshot::Sender<()>,
    release: std::sync::mpsc::Receiver<()>,
}

struct FakeHost {
    links: TempDir,
    commits: bool,
    report: Report,
    listing: Listing,
    tables: Mutex<HashMap<NamespaceIdentity, Vec<Value>>>,
    installs: Mutex<Vec<NamespaceIdentity>>,
    calls: Mutex<Vec<NamespaceIdentity>>,
    gate: Mutex<Option<Gate>>,
    looked_up: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

impl FakeHost {
    fn new() -> Arc<Self> {
        Self::reporting(true, Report::Exit(0), Listing::Kernel)
    }

    fn reporting(commits: bool, report: Report, listing: Listing) -> Arc<Self> {
        Arc::new(Self {
            links: tempfile::tempdir().unwrap(),
            commits,
            report,
            listing,
            tables: Mutex::default(),
            installs: Mutex::default(),
            calls: Mutex::default(),
            gate: Mutex::default(),
            looked_up: Mutex::default(),
        })
    }

    fn link(&self, slot_id: u32) -> PathBuf {
        self.links.path().join(format!("slot-{slot_id}"))
    }

    /// Points the slot's namespace entry at one of this process's namespaces.
    fn name(&self, slot_id: u32, kind: &str) -> NamespaceIdentity {
        let link = self.link(slot_id);
        let _ = std::fs::remove_file(&link);
        std::os::unix::fs::symlink(format!("/proc/self/ns/{kind}"), &link).unwrap();
        identity(std::fs::metadata(&link)).unwrap()
    }

    fn gate(
        &self,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        std::sync::mpsc::Sender<()>,
    ) {
        let (entered, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release) = std::sync::mpsc::channel();
        *self.gate.lock().unwrap() = Some(Gate { entered, release });
        (entered_rx, release_tx)
    }

    fn on_lookup(&self, hook: impl FnOnce() + Send + 'static) {
        *self.looked_up.lock().unwrap() = Some(Box::new(hook));
    }

    fn table(&self, namespace: NamespaceIdentity) -> Option<Vec<Value>> {
        self.tables.lock().unwrap().get(&namespace).cloned()
    }

    fn installs(&self) -> Vec<NamespaceIdentity> {
        self.installs.lock().unwrap().clone()
    }

    fn calls(&self) -> Vec<NamespaceIdentity> {
        self.calls.lock().unwrap().clone()
    }

    fn listed(&self, objects: Vec<Value>) -> Output {
        let stdout = match self.listing {
            Listing::Kernel => {
                let metainfo = json!({"metainfo": {"json_schema_version": 1}});
                let objects: Vec<_> = std::iter::once(metainfo).chain(objects).collect();
                json!({"nftables": objects}).to_string().into_bytes()
            }
            Listing::Fails => return output(1 << 8, b"", b"Error: listing failed"),
        };
        output(0, &stdout, b"")
    }

    /// nft's reply, or None once it timed out.
    fn reply(&self, netns: &File, args: &[&str], stdin: &[u8]) -> Option<Output> {
        let namespace = identity(netns.metadata()).unwrap();
        self.calls.lock().unwrap().push(namespace);
        let table = self.table(namespace);
        match args {
            ["-j", "-f", "-"] => {
                self.installs.lock().unwrap().push(namespace);
                if self.commits {
                    self.tables.lock().unwrap().insert(namespace, added(stdin));
                }
                let gate = self.gate.lock().unwrap().take();
                if let Some(gate) = gate {
                    gate.entered.send(()).unwrap();
                    gate.release.recv().unwrap();
                }
                match self.report {
                    Report::Exit(code) => Some(output(code << 8, b"", b"")),
                    Report::TimedOut => None,
                }
            }
            ["-j", "list", "tables", "ip"] => Some(
                self.listed(
                    table
                        .and_then(|objects| objects.first().cloned())
                        .into_iter()
                        .collect(),
                ),
            ),
            ["-j", "list", "table", "ip", "space_firewall_rules"] => {
                Some(self.listed(table.unwrap_or_default()))
            }
            other => panic!("unexpected nft {other:?}"),
        }
    }
}

/// What a submitted batch adds after the add and delete that make the
/// replacement valid.
fn added(batch: &[u8]) -> Vec<Value> {
    let batch: Value = serde_json::from_slice(batch).unwrap();
    let construction = batch["nftables"].as_array().unwrap()[2..].iter();
    construction.map(|command| command["add"].clone()).collect()
}

/// `status` is a raw wait status.
fn output(status: i32, stdout: &[u8], stderr: &[u8]) -> Output {
    Output {
        status: ExitStatus::from_raw(status),
        stdout: stdout.to_vec(),
        stderr: stderr.to_vec(),
    }
}

impl SlotHost for FakeHost {
    fn namespace_path(&self, slot: &SlotInfo) -> PathBuf {
        self.link(slot.slot)
    }

    fn looked_up(&self) {
        let hook = self.looked_up.lock().unwrap().take();
        if let Some(hook) = hook {
            hook();
        }
    }
}

async fn apply(
    pool: &Arc<NetworkPool>,
    store: &Arc<FirewallAssignments>,
    host: &Arc<FakeHost>,
    generation: u64,
    slot_id: u32,
) -> SlotFirewallReport {
    let request = SlotFirewallRequest {
        node_id: NODE,
        generation,
        slot_id,
    };
    let fake = host.clone();
    let nft = move |netns: &File, args: &[&str], stdin: &[u8], _| {
        fake.reply(netns, args, stdin)
            .ok_or_else(|| io::ErrorKind::TimedOut.into())
    };
    pool.apply_firewall_rules_with(store, request, host.clone(), Arc::new(nft))
        .await
        .unwrap()
}

fn attempted(report: SlotFirewallReport) -> SlotFirewallAttempt {
    match report {
        SlotFirewallReport::Attempted(attempt) => *attempt,
        SlotFirewallReport::NotRun(not_run) => panic!("not run: {not_run}"),
    }
}

/// The owned table the fake holds after `rule` was installed.
fn installed(rule: &str) -> Vec<Value> {
    let revision = decode_revision(&document(rule)).unwrap();
    render(&revision)
        .commands()
        .iter()
        .map(|command| command["add"].clone())
        .collect()
}

fn wakeups(receiver: &mut mpsc::Receiver<()>) -> usize {
    std::iter::from_fn(|| receiver.try_recv().ok()).count()
}

// Catches an unavailable or empty store, another node's assignment, or any
// generation but the requested one treated as runnable.
#[test]
fn only_the_requested_accepted_assignment_is_runnable() {
    let accepted = Arc::new(DesiredAssignment {
        node_id: NODE,
        generation: 12,
        revision_id: Uuid::from_u128(12),
        revision: decode_revision(&document(DNS)).unwrap(),
    });
    let other = NodeId::from_uuid(Uuid::from_u128(2));
    assert!(matches!(
        runnable(Err(Unavailable), NODE, 12),
        Err(NotRunnable::Unavailable(_))
    ));
    assert!(matches!(
        runnable(Ok(None), NODE, 12),
        Err(NotRunnable::NoAssignment)
    ));
    assert!(matches!(
        runnable(Ok(Some(accepted.clone())), other, 12),
        Err(NotRunnable::WrongNode { accepted }) if accepted == NODE
    ));
    for requested in [11, 13] {
        assert!(
            matches!(
                runnable(Ok(Some(accepted.clone())), NODE, requested),
                Err(NotRunnable::NotCurrent { accepted: 12 })
            ),
            "requested {requested}"
        );
    }
    let runnable = runnable(Ok(Some(accepted.clone())), NODE, 12).unwrap();
    assert!(Arc::ptr_eq(&runnable, &accepted));
}

// Catches an eligibility rule that admits any slot but a healthy ready one
// without a sandbox ID or a healthy leased one with one, including the inverse
// ID shapes.
#[test]
fn only_healthy_ready_or_leased_slots_are_eligible() {
    use SlotHealth::*;
    use SlotState::*;
    let mut passed = Vec::new();
    for state in [
        Missing,
        Creating,
        Ready,
        Leasing,
        Leased,
        Dirty,
        Resetting,
        Quarantined,
        Destroying,
    ] {
        for health in [Unknown, Healthy, Degraded, Invalid] {
            for sandbox in ["", SANDBOX] {
                if eligible(state, health, sandbox) {
                    passed.push((state, health, sandbox));
                }
            }
        }
    }
    assert_eq!(passed, [(Ready, Healthy, ""), (Leased, Healthy, SANDBOX)]);
}

// Catches nft run for a missing, ineligible, busy or changed slot, an
// unpinnable namespace or no accepted assignment, the assignment read before
// ownership, or a slot left owned after a refusal.
#[tokio::test]
async fn refusals_never_run_nft() {
    let (pool, _jobs) = pool(1);
    let (_dir, store) = store(None);
    let host = FakeHost::new();
    host.name(0, "net");
    leased(&pool, 0);
    let refused = async |slot_id| match apply(&pool, &store, &host, 12, slot_id).await {
        SlotFirewallReport::NotRun(not_run) => not_run,
        SlotFirewallReport::Attempted(attempt) => panic!("attempted: {attempt:?}"),
    };

    let guard = SlotGuard::try_acquire(&pool, 0).unwrap();
    assert!(matches!(refused(0).await, NotRun::Busy));
    drop(guard);
    assert!(matches!(
        refused(0).await,
        NotRun::Assignment(NotRunnable::NoAssignment)
    ));

    accept(&store, 12, DNS);
    assert!(matches!(refused(1).await, NotRun::NoSlot));
    set_slot(
        &pool,
        0,
        SlotState::Dirty,
        SlotHealth::Healthy,
        Some(SANDBOX),
        1,
    );
    assert!(matches!(
        refused(0).await,
        NotRun::Ineligible {
            state: SlotState::Dirty,
            health: SlotHealth::Healthy
        }
    ));
    leased(&pool, 0);
    // Only the generation moves, as when the slot is reset and leased again.
    let later = pool.clone();
    host.on_lookup(move || {
        set_slot(
            &later,
            0,
            SlotState::Leased,
            SlotHealth::Healthy,
            Some(SANDBOX),
            2,
        );
    });
    assert!(matches!(refused(0).await, NotRun::Changed));

    std::fs::remove_file(host.link(0)).unwrap();
    assert!(matches!(
        refused(0).await,
        NotRun::Namespace(error) if error.kind() == io::ErrorKind::NotFound
    ));
    std::fs::write(host.link(0), b"").unwrap();
    assert!(matches!(refused(0).await, NotRun::Namespace(_)));
    assert!(
        SlotGuard::try_acquire(&pool, 0).is_some(),
        "a refusal kept the slot"
    );
    assert_eq!(host.calls(), []);
}

// Catches a readback skipped or trusted less than the submit outcome, a blind
// retry, or a result reported as verified from nft's exit alone.
#[tokio::test]
async fn submit_outcome_and_readback_are_reported_separately() {
    let rows = [
        (true, Report::Exit(1), Listing::Kernel, "rejected", "match"),
        (true, Report::TimedOut, Listing::Kernel, "unknown", "match"),
        (
            false,
            Report::Exit(0),
            Listing::Kernel,
            "committed",
            "mismatch",
        ),
        (
            true,
            Report::Exit(0),
            Listing::Fails,
            "committed",
            "inconclusive",
        ),
    ];
    for (commits, report, listing, submitted, readback) in rows {
        let context = format!("commits {commits}, {report:?}, {listing:?}");
        let (pool, _jobs) = pool(1);
        let (_dir, store) = store(Some((12, DNS)));
        let host = FakeHost::reporting(commits, report, listing);
        host.name(0, "net");
        leased(&pool, 0);
        let attempt = attempted(apply(&pool, &store, &host, 12, 0).await);
        let observed = (
            match attempt.submitted {
                InstallOutcome::Committed => "committed",
                InstallOutcome::Rejected { .. } => "rejected",
                InstallOutcome::Unknown { .. } => "unknown",
            },
            match attempt.readback {
                Verification::Match => "match",
                Verification::Mismatch(_) => "mismatch",
                Verification::Inconclusive(_) => "inconclusive",
            },
        );
        assert_eq!(observed, (submitted, readback), "{context}");
        assert_eq!(attempt.verified(), readback == "match", "{context}");
        assert_eq!(host.installs().len(), 1, "{context}: submitted once");
        assert!(host.calls().len() > 1, "{context}: read back");
    }
}

#[derive(Clone, Copy, Debug)]
enum Drift {
    AssignmentAdvanced,
    SlotReset,
    NamespaceReplaced,
    NamespaceRemoved,
}

// Catches a result published as current after the assignment, the slot's
// lifecycle or the namespace entry moved during nft, a result relabelled with
// the newer assignment, or in-flight work redirected to the new entry.
#[tokio::test]
async fn changes_while_nft_runs_are_never_reported_current() {
    for drift in [
        Drift::AssignmentAdvanced,
        Drift::SlotReset,
        Drift::NamespaceReplaced,
        Drift::NamespaceRemoved,
    ] {
        let (pool, _jobs) = pool(1);
        let (_dir, store) = store(Some((12, DNS)));
        let host = FakeHost::new();
        let pinned = host.name(0, "net");
        leased(&pool, 0);
        let (entered, release) = host.gate();
        let task = tokio::spawn({
            let (pool, store, host) = (pool.clone(), store.clone(), host.clone());
            async move { apply(&pool, &store, &host, 12, 0).await }
        });
        entered.await.unwrap();
        let mut replacement = None;
        match drift {
            Drift::AssignmentAdvanced => accept(&store, 13, WEB),
            Drift::SlotReset => set_slot(
                &pool,
                0,
                SlotState::Dirty,
                SlotHealth::Unknown,
                Some(SANDBOX),
                1,
            ),
            Drift::NamespaceReplaced => replacement = Some(host.name(0, "ipc")),
            Drift::NamespaceRemoved => std::fs::remove_file(host.link(0)).unwrap(),
        }
        release.send(()).unwrap();
        let attempt = attempted(task.await.unwrap());

        assert_eq!(attempt.readback, Verification::Match, "{drift:?}");
        assert!(!attempt.verified(), "{drift:?}");
        let stale = attempt.current.as_ref().unwrap_err();
        let expected = match drift {
            Drift::AssignmentAdvanced => matches!(
                stale,
                Stale::Assignment(NotRunnable::NotCurrent { accepted: 13 })
            ),
            Drift::SlotReset => matches!(stale, Stale::Slot),
            Drift::NamespaceReplaced => {
                matches!(stale, Stale::Namespace(Ok(now)) if Some(*now) == replacement)
            }
            Drift::NamespaceRemoved => matches!(
                stale,
                Stale::Namespace(Err(error)) if error.kind() == io::ErrorKind::NotFound
            ),
        };
        assert!(expected, "{drift:?}: {stale:?}");
        assert_eq!(
            (attempt.assignment_generation, attempt.revision_id),
            (12, Uuid::from_u128(12)),
            "{drift:?}: labelled with what was installed"
        );
        assert_eq!(attempt.namespace, pinned, "{drift:?}");
        assert!(
            host.calls().iter().all(|called| *called == pinned),
            "{drift:?}: work followed the entry"
        );
    }
}

// Catches an older invocation that installs its generation once a newer one
// was accepted before it took ownership, or over the newer one afterwards.
#[tokio::test]
async fn an_older_invocation_never_reinstalls_over_a_newer_one() {
    let (pool, _jobs) = pool(1);
    let (_dir, store) = store(Some((12, DNS)));
    let host = FakeHost::new();
    let namespace = host.name(0, "net");
    leased(&pool, 0);
    let later = store.clone();
    host.on_lookup(move || accept(&later, 13, WEB));
    assert!(matches!(
        apply(&pool, &store, &host, 12, 0).await,
        SlotFirewallReport::NotRun(NotRun::Assignment(NotRunnable::NotCurrent { accepted: 13 }))
    ));
    assert_eq!(host.calls(), []);

    let attempt = attempted(apply(&pool, &store, &host, 13, 0).await);
    assert!(attempt.verified(), "{attempt:?}");
    assert_eq!(
        (
            (
                attempt.node_id,
                attempt.assignment_generation,
                attempt.revision_id
            ),
            (attempt.slot_id, attempt.slot_generation, attempt.sandbox_id),
            attempt.namespace,
        ),
        (
            (NODE, 13, Uuid::from_u128(13)),
            (0, 1, Some(SANDBOX.into())),
            namespace
        )
    );
    assert!(matches!(
        apply(&pool, &store, &host, 12, 0).await,
        SlotFirewallReport::NotRun(NotRun::Assignment(NotRunnable::NotCurrent { accepted: 13 }))
    ));
    assert_eq!(host.installs(), [namespace]);
    assert_eq!(host.table(namespace), Some(installed(WEB)));
}

// Catches ownership released with the async caller while nft runs or kept once
// the job finishes, one slot's apply blocking another's, or a neutral release
// that nudges reset.
#[tokio::test]
async fn a_dropped_caller_keeps_the_slot_while_nft_runs() {
    let (pool, mut jobs) = pool(2);
    let (_dir, store) = store(Some((12, DNS)));
    let host = FakeHost::new();
    host.name(0, "net");
    let other = host.name(1, "uts");
    leased(&pool, 0);
    ready(&pool, 1);
    let (entered, release) = host.gate();
    let caller = tokio::spawn({
        let (pool, store, host) = (pool.clone(), store.clone(), host.clone());
        async move { apply(&pool, &store, &host, 12, 0).await }
    });
    entered.await.unwrap();
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());

    assert!(ResetClaim::acquire(&pool, 0).is_none());
    let progressed = attempted(apply(&pool, &store, &host, 12, 1).await);
    assert!(progressed.verified());
    assert_eq!(progressed.namespace, other);

    release.send(()).unwrap();
    pool.task_tracker.close();
    pool.task_tracker.wait().await;
    assert_eq!(
        wakeups(&mut jobs),
        0,
        "neutral release must not nudge reset"
    );
    let reset = ResetClaim::acquire(&pool, 0).expect("released once the job finished");
    reset.disarm();
}
