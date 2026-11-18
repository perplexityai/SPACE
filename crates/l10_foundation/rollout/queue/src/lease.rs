//! Claiming items under a header's pacing, and renewing a claim.

use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::time::Duration;

use crate::clamp_lease;

/// How many items a canary leases: one at a time, until one is done.
const CANARY_IN_FLIGHT: u64 = 1;

/// What a claim read of its header under the header's lock.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Pacing {
    /// The header is in canary.
    pub canary: bool,
    /// Items in flight on any one node.
    pub per_node: u64,
    /// Items in flight in all.
    pub per_cluster: u64,
    /// Failed items at which nothing more is leased.
    pub failure_budget: u64,
}

/// The items a claim weighs: those in flight, counted by node, and the
/// pending ones it may lease, each with its node, in the order to lease them.
/// An item with no node counts against the cluster cap only.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Candidates<S, N> {
    pub in_flight: Vec<(Option<N>, u64)>,
    pub pending: Vec<(S, Option<N>)>,
}

/// The steps of [`claim`], each a statement or two on the store's open
/// transaction. The transaction must run at `READ COMMITTED`, so each step
/// reads what committed before it began, the waits for earlier steps' locks
/// included.
pub trait ClaimStore {
    /// What the store read of the header that its later steps need, such as
    /// the kind of subject its items name.
    type Header;
    type Subject: Copy + Eq + Hash;
    type Node: Copy + Eq + Hash;
    /// A leased item, as the store reports it.
    type Claimed;
    type Error;

    /// Lock the header row exclusively, if it's in a phase that leases
    /// ([`crate::HeaderPhase::leases`]), and read its pacing. `None` leases
    /// nothing.
    fn lock_leasable(
        &mut self,
    ) -> impl Future<Output = Result<Option<(Self::Header, Pacing)>, Self::Error>> + Send;

    /// Whether any item is done, and how many are in flight, from one
    /// snapshot: read in two, an item that finishes in between could look
    /// neither, and a second canary would be leased.
    fn canary_progress(&mut self) -> impl Future<Output = Result<(bool, u64), Self::Error>> + Send;

    /// Take the store's lease handoff lock exclusively: the lock every claim
    /// and renewal of subjects that `header` claims could share takes, so a
    /// claim reads another header's lease either before a renewal or after it
    /// commits.
    fn lock_handoff(
        &mut self,
        header: &Self::Header,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// The subjects another header holds in flight under a live lease, on the
    /// database clock. The claim skips them, so a subject has one worker at a
    /// time whichever headers name it.
    fn held_elsewhere(
        &mut self,
        header: &Self::Header,
    ) -> impl Future<Output = Result<HashSet<Self::Subject>, Self::Error>> + Send;

    /// Up to `scan_limit` pending items and the in-flight counts, from one
    /// snapshot, so each item's node is the same in both.
    fn candidates(
        &mut self,
        header: &Self::Header,
        scan_limit: u32,
    ) -> impl Future<Output = Result<Candidates<Self::Subject, Self::Node>, Self::Error>> + Send;

    /// How many items have failed.
    fn failed(&mut self) -> impl Future<Output = Result<u64, Self::Error>> + Send;

    /// Lease `subject` if it's still pending: in flight under `token`, until
    /// `lease` after the database clock reads at the write, with one more
    /// attempt. `None` when it's no longer pending.
    fn claim(
        &mut self,
        header: &Self::Header,
        subject: Self::Subject,
        node: Option<Self::Node>,
        token: uuid::Uuid,
        lease: Duration,
    ) -> impl Future<Output = Result<Option<Self::Claimed>, Self::Error>> + Send;
}

/// Lease pending items for `lease` (clamped by [`clamp_lease`]), up to the
/// header's pacing:
///
/// * nothing unless the header leases and its failed items, counted last,
///   haven't reached its failure budget;
/// * in canary, one item at a time, and nothing once one is done;
/// * at most `per_cluster` items in flight in all, and `per_node` on any
///   node, counting those already in flight;
/// * never a subject another header holds under a live lease.
///
/// Steps, in order: the header lock, the canary read, the handoff lock, the
/// held subjects and the candidates (after the handoff lock, so as current as
/// the leases it hands over), then the failed count. The count comes last,
/// each step reading its own snapshot, so an item that failed after it was
/// counted in flight is counted failed; read first, it would be in neither
/// and free a slot. Outcomes take no header lock, so failures among items
/// still in flight can overshoot the budget by up to their number.
pub async fn claim<S: ClaimStore>(
    store: &mut S,
    lease: Duration,
    scan_limit: u32,
) -> Result<Vec<S::Claimed>, S::Error> {
    let lease = clamp_lease(lease);
    let Some((header, mut pacing)) = store.lock_leasable().await? else {
        return Ok(Vec::new());
    };
    if pacing.canary {
        let (done, in_flight) = store.canary_progress().await?;
        if done || in_flight >= CANARY_IN_FLIGHT {
            return Ok(Vec::new());
        }
        pacing.per_cluster = pacing.per_cluster.min(CANARY_IN_FLIGHT);
    }
    store.lock_handoff(&header).await?;
    let held = store.held_elsewhere(&header).await?;
    let candidates = store.candidates(&header, scan_limit).await?;
    let pending: Vec<_> = candidates
        .pending
        .into_iter()
        .filter(|(subject, _)| !held.contains(subject))
        .collect();
    let failed = store.failed().await?;
    if failed >= pacing.failure_budget {
        return Ok(Vec::new());
    }
    let mut on_node: HashMap<S::Node, u64> = HashMap::new();
    let mut in_cluster = 0_u64;
    for (node, count) in candidates.in_flight {
        in_cluster += count;
        if let Some(node) = node {
            *on_node.entry(node).or_default() += count;
        }
    }
    let mut claimed = Vec::new();
    for (subject, node) in pending {
        if in_cluster >= pacing.per_cluster {
            break;
        }
        if let Some(node) = node
            && on_node.get(&node).copied().unwrap_or(0) >= pacing.per_node
        {
            continue;
        }
        let token = uuid::Uuid::now_v7();
        // The header lock serializes claims, so an item read as pending is
        // still pending; a miss means some other writer moved it.
        let Some(item) = store.claim(&header, subject, node, token, lease).await? else {
            continue;
        };
        in_cluster += 1;
        if let Some(node) = node {
            *on_node.entry(node).or_default() += 1;
        }
        claimed.push(item);
    }
    Ok(claimed)
}

/// The steps of [`renew`], on one item, each a statement on the store's open
/// transaction, which must run at `READ COMMITTED`.
pub trait RenewStore {
    type Error;

    /// Take a share lock on the item's header row if the header hasn't
    /// finished: a finished header's leases aren't renewed, and the lock
    /// keeps it from finishing until the renewal commits. `false` renews
    /// nothing.
    fn lock_unfinished(&mut self) -> impl Future<Output = Result<bool, Self::Error>> + Send;

    /// Take the lease handoff lock [`ClaimStore::lock_handoff`] takes, shared,
    /// so renewals don't wait on each other, and a claim waits for this
    /// renewal to commit and then sees it.
    fn lock_handoff(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Lock the item row, whatever its state. A single conditional update
    /// that waits on a row another transaction only locked resumes without
    /// reading the clock again, and would renew a lease that lapsed while it
    /// waited; [`Self::extend`] runs after this wait.
    fn lock_item(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Extend the lease to `lease` after the database clock reads, if the
    /// item is still in flight under the caller's token and the lease hasn't
    /// lapsed on that clock. `false` when it isn't.
    fn extend(&mut self, lease: Duration)
    -> impl Future<Output = Result<bool, Self::Error>> + Send;
}

/// Extend an item's lease to `lease` (clamped by [`clamp_lease`]) from now.
/// `false` when the lease is no longer the caller's: it lapsed, the item left
/// flight, or its header finished. Steps, in order: the header share lock,
/// the shared handoff lock, the item lock, the extension.
pub async fn renew<S: RenewStore>(store: &mut S, lease: Duration) -> Result<bool, S::Error> {
    let lease = clamp_lease(lease);
    if !store.lock_unfinished().await? {
        return Ok(false);
    }
    store.lock_handoff().await?;
    store.lock_item().await?;
    store.extend(lease).await
}
