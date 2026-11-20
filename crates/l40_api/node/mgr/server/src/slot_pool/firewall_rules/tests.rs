//! The coordinator against a fake nft that keeps one owned table per pinned
//! namespace, so the real renderer, outcome classification and checker run
//! without privilege. Slots pin distinct `/proc/self/ns` objects through
//! symlinks. `linux` repeats one case against nftables.

use std::collections::HashMap;
use std::os::unix::process::ExitStatusExt as _;
use std::process::{ExitStatus, Output};
use std::sync::Mutex;
use std::time::Duration;

use pplx_space_firewall_rules_protocol::decode_revision;
use rusqlite::params;
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::sync::{mpsc, oneshot};

use super::*;
use crate::firewall::assignment::IncomingAssignment;
use crate::slot_pool::NetworkPoolConfig;

pub(crate) mod linux;

pub(crate) const NODE: NodeId = NodeId::from_uuid(Uuid::from_u128(1));
/// Far beyond any wait a passing run makes, so it never trips on a slow runner.
pub(crate) const BOUND: Duration = Duration::from_secs(10);

/// Fails the test instead of letting a wait that should end hang it.
pub(crate) async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(BOUND, future)
        .await
        .expect("waited past the bound")
}

/// Generation `generation` drops TCP to the port of that number.
pub(crate) fn document(generation: u64) -> String {
    let rule = json!({"id": "port", "all_of": [{
        "left": {"payload": {"protocol": "tcp", "field": "dport"}}, "op": "==", "right": generation,
    }]});
    json!({"schema_version": 1, "rules": [rule]}).to_string()
}

/// A store with the `accepted` generation, if any.
fn store(accepted: Option<u64>) -> (TempDir, Arc<FirewallAssignments>) {
    let dir = tempfile::tempdir().unwrap();
    let store = FirewallAssignments::open(&dir.path().join("assignment.json")).unwrap();
    if let Some(generation) = accepted {
        accept(&store, generation);
    }
    (dir, Arc::new(store))
}

fn accept(store: &FirewallAssignments, generation: u64) {
    let document = document(generation);
    let incoming = IncomingAssignment {
        node_id: NODE,
        generation,
        revision_id: Uuid::from_u128(generation.into()),
        document: &document,
    };
    store.accept(NODE, incoming).unwrap();
}

/// The owned table the fake holds once `generation` is installed.
pub(crate) fn installed(generation: u64) -> Vec<Value> {
    let revision = decode_revision(&document(generation)).unwrap();
    render(&revision)
        .commands()
        .iter()
        .map(|command| command["add"].clone())
        .collect()
}

/// A pool whose reset wakeups are countable.
fn pool(slots: u32) -> (Arc<NetworkPool>, mpsc::Receiver<()>) {
    let mut pool = NetworkPool::open_in_memory_for_test(NetworkPoolConfig::new(slots));
    let (sender, wakeups) = mpsc::channel(8);
    Arc::get_mut(&mut pool).unwrap().job_tx = sender;
    (pool, wakeups)
}

/// A healthy slot, held by `sandbox-{slot_id}` unless it is ready.
pub(crate) fn set_slot(pool: &NetworkPool, slot_id: u32, state: SlotState, generation: i64) {
    let update = "UPDATE network_slots SET state = ?1, generation = ?2, health = 'healthy',
                  sandbox_id = CASE ?1 WHEN 'ready' THEN NULL ELSE 'sandbox-' || ?3 END
                  WHERE slot_id = ?3";
    let params = params![state.as_str(), generation, slot_id];
    pool.db.lock().unwrap().execute(update, params).unwrap();
}

/// Holds the slot as other work would, until dropped.
pub(crate) fn busy(pool: &Arc<NetworkPool>, slot_id: u32) -> impl Send {
    SlotGuard::try_acquire(pool, slot_id).unwrap()
}

/// How the fake nft departs from a healthy one.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum Fault {
    /// Commits, then reports failure, as nft can after an ACK overflow.
    Rejects,
    /// Commits, then times out.
    TimesOut,
    /// Reports success without committing.
    Drops,
    /// Commits, then fails every listing.
    Unlistable,
    /// Commits, then panics, failing the apply's job.
    Panics,
}

pub(crate) type Hook = Mutex<Option<Box<dyn FnOnce() + Send>>>;

pub(crate) fn hook(slot: &Hook, hook: impl FnOnce() + Send + 'static) {
    *slot.lock().unwrap() = Some(Box::new(hook));
}

fn fire(slot: &Hook) {
    let hook = slot.lock().unwrap().take();
    if let Some(hook) = hook {
        hook();
    }
}

pub(crate) struct FakeHost {
    links: TempDir,
    fault: Option<Fault>,
    pub(crate) tables: Mutex<HashMap<NamespaceIdentity, Vec<Value>>>,
    installs: Mutex<Vec<NamespaceIdentity>>,
    calls: Mutex<Vec<NamespaceIdentity>>,
    /// Fired once, after the next lookup.
    pub(crate) looked_up: Hook,
    /// Fired once, after the next install commits.
    pub(crate) installed: Hook,
}

impl FakeHost {
    pub(crate) fn new(fault: Option<Fault>) -> Arc<Self> {
        Arc::new(Self {
            links: tempfile::tempdir().unwrap(),
            fault,
            tables: Mutex::default(),
            installs: Mutex::default(),
            calls: Mutex::default(),
            looked_up: Mutex::default(),
            installed: Mutex::default(),
        })
    }

    fn link(&self, slot_id: u32) -> PathBuf {
        self.links.path().join(format!("slot-{slot_id}"))
    }

    /// Points the slot's namespace entry at one of this process's namespaces.
    pub(crate) fn name(&self, slot_id: u32, kind: &str) -> NamespaceIdentity {
        let link = self.link(slot_id);
        let _ = std::fs::remove_file(&link);
        std::os::unix::fs::symlink(format!("/proc/self/ns/{kind}"), &link).unwrap();
        identity(std::fs::metadata(&link)).unwrap()
    }

    pub(crate) fn installs(&self) -> Vec<NamespaceIdentity> {
        self.installs.lock().unwrap().clone()
    }

    pub(crate) fn calls(&self) -> Vec<NamespaceIdentity> {
        self.calls.lock().unwrap().clone()
    }

    /// nft's reply, or None once it timed out.
    pub(crate) fn reply(&self, netns: &File, args: &[&str], stdin: &[u8]) -> Option<Output> {
        let namespace = identity(netns.metadata()).unwrap();
        self.calls.lock().unwrap().push(namespace);
        if args == ["-j", "-f", "-"] {
            self.installs.lock().unwrap().push(namespace);
            if self.fault != Some(Fault::Drops) {
                self.tables.lock().unwrap().insert(namespace, added(stdin));
            }
            fire(&self.installed);
            return match self.fault {
                Some(Fault::Panics) => panic!("the fake nft panics after committing"),
                Some(Fault::Rejects) => Some(output(1, Vec::new())),
                Some(Fault::TimesOut) => None,
                _ => Some(output(0, Vec::new())),
            };
        }
        if self.fault == Some(Fault::Unlistable) {
            return Some(output(1, Vec::new()));
        }
        let table = self.tables.lock().unwrap().get(&namespace).cloned();
        let table = table.unwrap_or_default();
        let objects = match args {
            ["-j", "list", "tables", "ip"] => table.into_iter().take(1).collect(),
            ["-j", "list", "table", "ip", "space_firewall_rules"] => table,
            other => panic!("unexpected nft {other:?}"),
        };
        let metainfo = json!({"metainfo": {"json_schema_version": 1}});
        let objects: Vec<_> = std::iter::once(metainfo).chain(objects).collect();
        let listing = json!({"nftables": objects}).to_string();
        Some(output(0, listing.into_bytes()))
    }
}

/// What a submitted batch adds after the add and delete that make the
/// replacement valid.
fn added(batch: &[u8]) -> Vec<Value> {
    let batch: Value = serde_json::from_slice(batch).unwrap();
    let construction = batch["nftables"].as_array().unwrap()[2..].iter();
    construction.map(|command| command["add"].clone()).collect()
}

fn output(code: i32, stdout: Vec<u8>) -> Output {
    Output {
        status: ExitStatus::from_raw(code << 8),
        stdout,
        stderr: Vec::new(),
    }
}

impl SlotHost for FakeHost {
    fn namespace_path(&self, slot: &SlotInfo) -> PathBuf {
        self.link(slot.slot)
    }

    fn looked_up(&self) {
        fire(&self.looked_up);
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
    bounded(pool.apply_firewall_rules_with(store, request, host.clone(), Arc::new(nft)))
        .await
        .unwrap()
}

fn attempted(report: SlotFirewallReport) -> SlotFirewallAttempt {
    match report {
        SlotFirewallReport::Attempted(attempt) => *attempt,
        SlotFirewallReport::NotRun(not_run) => panic!("not run: {not_run}"),
    }
}

// Catches an unavailable store, another node, or a newer generation run.
#[test]
fn only_the_requested_accepted_assignment_is_runnable() {
    let accepted = Arc::new(DesiredAssignment {
        node_id: NODE,
        generation: 12,
        revision_id: Uuid::from_u128(12),
        revision: decode_revision(&document(12)).unwrap(),
    });
    let run = |node, generation| runnable(Ok(Some(accepted.clone())), node, generation);
    assert!(matches!(
        runnable(Err(Unavailable), NODE, 12),
        Err(NotRunnable::Unavailable(_))
    ));
    assert!(matches!(
        run(NodeId::from_uuid(Uuid::from_u128(2)), 12),
        Err(NotRunnable::WrongNode { accepted }) if accepted == NODE
    ));
    assert!(matches!(
        run(NODE, 13),
        Err(NotRunnable::NotCurrent { accepted: 12 })
    ));
}

// Catches any slot eligible but healthy Ready without an ID or Leased with one
// at any health.
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
            for sandbox in ["", "sandbox"] {
                if eligible(state, health, sandbox) {
                    passed.push((state, health, sandbox));
                }
            }
        }
    }
    let expected = [
        (Ready, Healthy, ""),
        (Leased, Unknown, "sandbox"),
        (Leased, Healthy, "sandbox"),
        (Leased, Degraded, "sandbox"),
        (Leased, Invalid, "sandbox"),
    ];
    assert_eq!(passed, expected);
}

// Catches nft run, ownership skipped or kept, the assignment read early, or
// work started during shutdown.
#[tokio::test]
async fn refusals_never_run_nft() {
    let (pool, _jobs) = pool(1);
    let (_dir, store) = store(None);
    let host = FakeHost::new(None);
    host.name(0, "net");
    set_slot(&pool, 0, SlotState::Leased, 1);
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

    accept(&store, 12);
    assert!(matches!(refused(1).await, NotRun::NoSlot));
    set_slot(&pool, 0, SlotState::Dirty, 1);
    assert!(matches!(
        refused(0).await,
        NotRun::Ineligible {
            state: SlotState::Dirty,
            ..
        }
    ));
    set_slot(&pool, 0, SlotState::Leased, 1);
    // Only the generation moves, as when the slot is reset and leased again.
    let later = pool.clone();
    hook(&host.looked_up, move || {
        set_slot(&later, 0, SlotState::Leased, 2);
    });
    assert!(matches!(refused(0).await, NotRun::Changed));

    std::fs::remove_file(host.link(0)).unwrap();
    assert!(matches!(
        refused(0).await,
        NotRun::Namespace(error) if error.kind() == io::ErrorKind::NotFound
    ));
    std::fs::write(host.link(0), b"").unwrap();
    assert!(matches!(refused(0).await, NotRun::Namespace(_)));
    pool.shutdown_token.cancel();
    assert!(matches!(refused(0).await, NotRun::ShuttingDown));
    assert!(
        SlotGuard::try_acquire(&pool, 0).is_some(),
        "a refusal kept the slot"
    );
    assert_eq!(host.calls(), []);
}

// Catches a skipped readback, a retry, or verification from nft's exit.
#[tokio::test]
async fn submit_outcome_and_readback_are_reported_separately() {
    for (fault, submitted, readback) in [
        (Fault::Rejects, "rejected", "match"),
        (Fault::TimesOut, "unknown", "match"),
        (Fault::Drops, "committed", "mismatch"),
        (Fault::Unlistable, "committed", "inconclusive"),
    ] {
        let (pool, _jobs) = pool(1);
        let (_dir, store) = store(Some(12));
        let host = FakeHost::new(Some(fault));
        host.name(0, "net");
        set_slot(&pool, 0, SlotState::Leased, 1);
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
        assert_eq!(observed, (submitted, readback), "{fault:?}");
        assert_eq!(attempt.verified(), readback == "match", "{fault:?}");
        assert_eq!(host.installs().len(), 1, "{fault:?}: submitted once");
        assert!(host.calls().len() > 1, "{fault:?}: read back");
    }
}

#[derive(Clone, Copy, Debug)]
enum Drift {
    AssignmentAdvanced,
    SlotReset,
    NamespaceReplaced,
    NamespaceRemoved,
}

// Catches a result current, relabelled or redirected after a change during nft.
#[tokio::test]
async fn changes_while_nft_runs_are_never_reported_current() {
    for drift in [
        Drift::AssignmentAdvanced,
        Drift::SlotReset,
        Drift::NamespaceReplaced,
        Drift::NamespaceRemoved,
    ] {
        let (pool, _jobs) = pool(1);
        let (_dir, store) = store(Some(12));
        let host = FakeHost::new(None);
        let pinned = host.name(0, "net");
        set_slot(&pool, 0, SlotState::Leased, 1);
        let (later, slots, fake) = (store.clone(), pool.clone(), host.clone());
        hook(&host.installed, move || match drift {
            Drift::AssignmentAdvanced => accept(&later, 13),
            Drift::SlotReset => set_slot(&slots, 0, SlotState::Dirty, 1),
            Drift::NamespaceReplaced => {
                fake.name(0, "ipc");
            }
            Drift::NamespaceRemoved => std::fs::remove_file(fake.link(0)).unwrap(),
        });
        let attempt = attempted(apply(&pool, &store, &host, 12, 0).await);

        assert_eq!(attempt.readback, Verification::Match, "{drift:?}");
        assert!(!attempt.verified(), "{drift:?}");
        let ipc = identity(std::fs::metadata("/proc/self/ns/ipc")).unwrap();
        let stale = attempt.current.as_ref().unwrap_err();
        let expected = match drift {
            Drift::AssignmentAdvanced => matches!(
                stale,
                Stale::Assignment(NotRunnable::NotCurrent { accepted: 13 })
            ),
            Drift::SlotReset => matches!(stale, Stale::Slot),
            Drift::NamespaceReplaced => matches!(stale, Stale::Namespace(Ok(now)) if *now == ipc),
            Drift::NamespaceRemoved => matches!(
                stale,
                Stale::Namespace(Err(error)) if error.kind() == io::ErrorKind::NotFound
            ),
        };
        assert!(expected, "{drift:?}: {stale:?}");
        assert_eq!(
            (attempt.assignment_generation, attempt.namespace),
            (12, pinned),
            "{drift:?}: labelled with what was attempted"
        );
        assert!(
            host.calls().iter().all(|called| *called == pinned),
            "{drift:?}: work followed the entry"
        );
    }
}

// Catches an older generation installed after a newer one was accepted.
#[tokio::test]
async fn an_older_invocation_never_reinstalls_over_a_newer_one() {
    let (pool, _jobs) = pool(1);
    let (_dir, store) = store(Some(12));
    let host = FakeHost::new(None);
    let namespace = host.name(0, "net");
    set_slot(&pool, 0, SlotState::Leased, 1);
    let later = store.clone();
    hook(&host.looked_up, move || accept(&later, 13));
    assert!(matches!(
        apply(&pool, &store, &host, 12, 0).await,
        SlotFirewallReport::NotRun(NotRun::Assignment(NotRunnable::NotCurrent { accepted: 13 }))
    ));
    assert_eq!(host.calls(), []);

    let attempt = attempted(apply(&pool, &store, &host, 13, 0).await);
    assert!(attempt.verified(), "{attempt:?}");
    let assignment = (
        attempt.node_id,
        attempt.assignment_generation,
        attempt.revision_id,
    );
    assert_eq!(assignment, (NODE, 13, Uuid::from_u128(13)));
    let slot = (
        attempt.slot_id,
        attempt.slot_generation,
        attempt.sandbox_id.as_deref(),
    );
    assert_eq!(slot, (0, 1, Some("sandbox-0")));
    assert_eq!(attempt.namespace, namespace);
}

// Catches ownership tied to the caller, kept, blocking other slots, or nudging reset.
#[tokio::test]
async fn a_dropped_caller_keeps_the_slot_while_nft_runs() {
    let (pool, mut jobs) = pool(2);
    let (_dir, store) = store(Some(12));
    let host = FakeHost::new(None);
    host.name(0, "net");
    let other = host.name(1, "uts");
    set_slot(&pool, 0, SlotState::Leased, 1);
    set_slot(&pool, 1, SlotState::Ready, 1);
    let (enter, entered) = oneshot::channel();
    let (release, released) = std::sync::mpsc::channel::<()>();
    hook(&host.installed, move || {
        let _ = enter.send(());
        // Also ends once a failed test drops `release`.
        let _ = released.recv_timeout(BOUND);
    });
    let caller = tokio::spawn({
        let (pool, store, host) = (pool.clone(), store.clone(), host.clone());
        async move { apply(&pool, &store, &host, 12, 0).await }
    });
    bounded(entered).await.unwrap();
    caller.abort();
    assert!(bounded(caller).await.unwrap_err().is_cancelled());

    assert!(SlotGuard::try_acquire(&pool, 0).is_none(), "released early");
    let progressed = attempted(apply(&pool, &store, &host, 12, 1).await);
    assert!(progressed.verified());
    assert_eq!((progressed.namespace, progressed.sandbox_id), (other, None));

    release.send(()).unwrap();
    pool.task_tracker.close();
    bounded(pool.task_tracker.wait()).await;
    assert!(jobs.try_recv().is_err(), "a neutral release nudged reset");
    assert!(SlotGuard::try_acquire(&pool, 0).is_some(), "never released");
}
