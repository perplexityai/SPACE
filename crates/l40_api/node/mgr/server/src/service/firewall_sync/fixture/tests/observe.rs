//! Verification rounds through the binding, against the slot apply's fake nft.

use super::super::rounds::{Observed, RoundRefused};
use super::*;
use crate::proto::{
    FirewallRulesCensus as Census, FirewallRulesCensusGap, FirewallRulesCensusGapKind as Gap,
    FirewallRulesObservation, FirewallRulesSlotEvidence, FirewallRulesSlotOutcome as Slot,
    FirewallRulesSlotReason as Reason,
};
use crate::slot_pool::firewall_rules::observe::{
    self, CensusGap, Failure, Refusal, SlotEvidence, Unknown,
};
use crate::slot_pool::{FirewallObservation, SlotOutcome};

const VERIFIED: (Slot, Reason) = (Slot::Verified, Reason::Unspecified);
const MISMATCHED: (Slot, Reason) = (Slot::Failed, Reason::RulesMismatch);

/// Round-only: asks `round` about `generation`.
fn asking(round: u64, generation: u64) -> SyncFirewallRulesRequest {
    SyncFirewallRulesRequest {
        node_id: NODE.to_string(),
        assignment: None,
        question: ask(round, generation),
    }
}

/// A complete census of the bound slots, at lifecycle generation 1, in order.
fn complete(round: u64, generation: u64, slots: &[(Slot, Reason)]) -> FirewallRulesObservation {
    let slot = |(slot_id, (outcome, reason)): (u32, &(Slot, Reason))| FirewallRulesSlotEvidence {
        slot_id: Some(slot_id),
        slot_generation: Some(1),
        outcome: (*outcome).into(),
        reason: (*reason).into(),
    };
    FirewallRulesObservation {
        node_id: NODE.to_string(),
        round: Some(round),
        assignment_generation: Some(generation),
        revision_id: Uuid::from_u128(generation.into()).to_string(),
        manager_epoch: Some(EPOCH),
        census: Census::Complete.into(),
        census_gaps: Vec::new(),
        slots: (0..).zip(slots).map(slot).collect(),
        extra_namespaces: Some(0),
    }
}

fn outcomes(observed: &Observed) -> Vec<SlotOutcome> {
    let observation = observed.answer.as_ref().unwrap();
    observation.slots.iter().map(|slot| slot.outcome).collect()
}

// Catches a round swept twice, a final round answered afresh, a new round
// served from the cache, a stale or conflicting round answered, other than
// the last eight rounds kept, or a round evicted while queued still swept.
#[tokio::test]
async fn a_round_is_observed_once_and_never_recreated() {
    use SlotState::{Leased, Ready};
    let bound = bind_slots(BoundSlots::All, None, None, WAIT, &[Leased, Ready]).await;
    sync_all(&bound.service, 12).await;
    passed(&bound.service, 12).await.unwrap();
    let (sync, pool) = (
        bound.service.firewall_sync.clone().unwrap(),
        &bound.service.pool,
    );
    let calls = || bound.host.calls().len();
    let before = calls();

    let (entered, release) = gate(&bound.host.observed);
    let first = sync.observe(pool, NODE, 12, 7).unwrap();
    bounded(entered).await.unwrap();
    let joined = sync.observe(pool, NODE, 12, 7).unwrap();
    release.send(()).unwrap();
    let (round_7, joined) = (bounded(first).await, bounded(joined).await);
    assert!(Arc::ptr_eq(&round_7, &joined));
    let observation = round_7.answer.as_ref().unwrap();
    assert!(observation.census.complete(), "{observation:?}");
    assert_eq!(outcomes(&round_7), [SlotOutcome::Verified; 2]);
    // Two listings for each slot.
    let swept = calls();
    assert_eq!(swept - before, 4);

    let idle = bound.namespaces[1];
    bound
        .host
        .tables
        .lock()
        .unwrap()
        .insert(idle, installed(11));
    let retried = bounded(sync.observe(pool, NODE, 12, 7).unwrap()).await;
    assert!(Arc::ptr_eq(&round_7, &retried));
    assert_eq!(calls(), swept);
    let refused = |generation, round| sync.observe(pool, NODE, generation, round).err();
    assert_eq!(
        refused(13, 7),
        Some(RoundRefused::Conflict { generation: 12 })
    );
    let other = NodeId::from_uuid(Uuid::from_u128(2));
    assert_eq!(
        sync.observe(pool, other, 12, 9).err(),
        Some(RoundRefused::WrongNode)
    );

    let round_8 = bounded(sync.observe(pool, NODE, 12, 8).unwrap()).await;
    let mismatch = SlotOutcome::Failed(Failure::RulesMismatch);
    assert_eq!(outcomes(&round_8), [SlotOutcome::Verified, mismatch]);
    assert_eq!(round_8.round, 8);
    // Never seen, but older than the newest.
    assert_eq!(refused(12, 6), Some(RoundRefused::Stale));
    let through_8 = calls();

    // 10 queues behind 9's sweep, and 11 to 18 evict both before it runs.
    let (entered, release) = gate(&bound.host.observed);
    let round_9 = sync.observe(pool, NODE, 12, 9).unwrap();
    bounded(entered).await.unwrap();
    let queued: Vec<_> = (10..=18)
        .map(|round| sync.observe(pool, NODE, 12, round).unwrap())
        .collect();
    release.send(()).unwrap();
    bounded(round_9).await;
    let queued = bounded(futures_util::future::join_all(queued)).await;
    assert!(
        matches!(queued[0].answer, Err(Refusal::Unfinished)),
        "{:?}",
        queued[0]
    );
    // Every round from 9 to 18 but 10.
    let evicted = calls();
    assert_eq!(evicted - through_8, 9 * 4);
    assert_eq!(refused(12, 10), Some(RoundRefused::Stale));
    let kept = bounded(sync.observe(pool, NODE, 12, 11).unwrap()).await;
    assert!(Arc::ptr_eq(&kept, &queued[1]));
    assert_eq!(calls(), evicted);
    assert_eq!(bound.host.installs().len(), 2);
}

// Catches Applying claimed without pending apply work for the target or for a
// matching table, or a mismatch the pass will still apply reported as Failed.
#[tokio::test]
async fn only_pending_apply_work_is_applying() {
    use SlotState::Leased;
    let bound = bind_slots(BoundSlots::All, None, None, WAIT, &[Leased, Leased]).await;
    let (sync, pool) = (
        bound.service.firewall_sync.clone().unwrap(),
        &bound.service.pool,
    );
    let target = |slot_generation| SlotTarget {
        slot_generation,
        sandbox_id: Some("sandbox-0".into()),
        namespace: None,
    };
    // Armed and not yet listed: the worker hasn't run on this runtime.
    sync.wanted.send_modify(|wanted| {
        arm(wanted, 12);
    });
    assert!(sync.applying(12, 0, &target(1)));
    assert!(!sync.applying(11, 0, &target(1)));
    sync.wanted.send_replace(None);

    // 12 pauses on slot 0's lookup, its pass listed and nothing applied.
    let (entered, release) = gate(&bound.host.looked_up);
    sync_all(&bound.service, 12).await;
    bounded(entered).await.unwrap();
    assert!(!sync.applying(12, 0, &target(2)), "another occupant");
    // Slot 1 already matches: pending, but verified.
    let matching = bound.namespaces[1];
    bound
        .host
        .tables
        .lock()
        .unwrap()
        .insert(matching, installed(12));
    let pending = bounded(sync.observe(pool, NODE, 12, 1).unwrap()).await;
    assert_eq!(
        outcomes(&pending),
        [SlotOutcome::Applying, SlotOutcome::Verified]
    );
    release.send(()).unwrap();
}

// Catches a question alone that installs or arms, a retried round answered
// afresh, a new round served from the cache, or a refusal returned as evidence.
#[tokio::test]
async fn a_question_alone_reads_the_accepted_generation_once_per_round() {
    use SlotState::{Leased, Ready};
    let bound = bind_slots(BoundSlots::All, None, Some(12), WAIT, &[Leased, Ready]).await;
    let install = |slot: usize, generation| {
        let mut tables = bound.host.tables.lock().unwrap();
        tables.insert(bound.namespaces[slot], installed(generation));
    };
    install(0, 12);
    install(1, 12);

    let round_7 = sync(&bound.service, asking(7, 12)).await.unwrap();
    let answered = SyncFirewallRulesResponse {
        observation: Some(complete(7, 12, &[VERIFIED, VERIFIED])),
        ..SyncFirewallRulesResponse::default()
    };
    assert_eq!(round_7, answered);
    install(1, 11);
    let retried = sync(&bound.service, asking(7, 12)).await.unwrap();
    assert_eq!(retried, round_7);
    let round_8 = sync(&bound.service, asking(8, 12)).await.unwrap();
    assert_eq!(
        round_8.observation,
        Some(complete(8, 12, &[VERIFIED, MISMATCHED]))
    );

    let unaccepted = sync(&bound.service, asking(9, 13)).await.unwrap();
    assert_eq!(unaccepted.observation, None);
    assert!(!unaccepted.unanswered.is_empty());
    assert_eq!(bound.host.installs(), []);
    assert!(nothing_armed(&bound.service));
}

// Catches a question observed before its assignment is accepted and applied,
// or a complete census with a failed slot reported as anything else.
#[tokio::test]
async fn a_question_with_its_assignment_is_observed_after_acceptance() {
    let bound = bind(None, None, WAIT).await;
    let request = SyncFirewallRulesRequest {
        question: ask(7, 12),
        ..delivery(12)
    };
    let reply = sync(&bound.service, request).await.unwrap();
    assert_eq!(reply.accepted_generation, Some(12));
    assert_eq!(reply.acceptance(), FirewallAssignmentAcceptance::Committed);
    assert_eq!(reply.dispatch.unwrap().outcome(), Outcome::Matched);
    // Only the bound slot was applied.
    let observation = complete(7, 12, &[MISMATCHED, VERIFIED]);
    assert_eq!(reply.observation, Some(observation));
}

// Catches evidence returned for an older question beside a newer assignment,
// that question sent to the sweeps, or the newer assignment not delivered.
#[tokio::test]
async fn a_question_about_another_generation_delivers_only_the_assignment() {
    use SlotState::Leased;
    let bound = bind_slots(BoundSlots::All, None, Some(12), WAIT, &[Leased, Leased]).await;
    let binding = bound.service.firewall_sync.clone().unwrap();
    // Held, so a reply that waits on an observation never comes.
    let sweeping = binding.sweeps.lock().await;
    let request = SyncFirewallRulesRequest {
        question: ask(7, 12),
        ..delivery(13)
    };
    let reply = sync(&bound.service, request).await.unwrap();
    drop(sweeping);
    assert_eq!(reply.accepted_generation, Some(13));
    assert_eq!(reply.acceptance(), FirewallAssignmentAcceptance::Committed);
    assert_eq!(reply.observation, None);
    assert!(!reply.unanswered.is_empty());

    let matched = Some(Outcome::Matched);
    assert_eq!(
        passed(&bound.service, 13).await,
        Ok(vec![(0, matched), (1, matched)])
    );
    let installed_13 = installed(13);
    let submissions = bound.host.submissions();
    assert_eq!(submissions.len(), 2);
    assert!(submissions.iter().all(|(_, table)| *table == installed_13));
}

// Catches a dropped or swapped gap, an extra or truncated namespace left
// uncounted, a lost slot generation, outcome or reason, or a census judged by
// its slots.
#[test]
fn evidence_is_the_observation_in_the_reports_terms() {
    let unknown = SlotOutcome::Unknown;
    let outcomes = [
        (SlotOutcome::Verified, VERIFIED),
        (SlotOutcome::Applying, (Slot::Applying, Reason::Unspecified)),
        (SlotOutcome::Failed(Failure::RulesMismatch), MISMATCHED),
        (
            unknown(Unknown::Ineligible),
            (Slot::Unknown, Reason::Ineligible),
        ),
        (
            unknown(Unknown::NamespaceMissing),
            (Slot::Unknown, Reason::NamespaceMissing),
        ),
        (
            unknown(Unknown::NamespaceUnreadable),
            (Slot::Unknown, Reason::NamespaceUnreadable),
        ),
        (
            unknown(Unknown::TableUnreadable),
            (Slot::Unknown, Reason::TableUnreadable),
        ),
        (
            unknown(Unknown::Unconfirmed),
            (Slot::Unknown, Reason::Unconfirmed),
        ),
    ];
    // Each gap and its Report form; extras are only counted.
    let gaps = [
        (
            CensusGap::InventoryFailed,
            Some((Gap::InventoryFailed, None)),
        ),
        (CensusGap::Unowned, Some((Gap::Unowned, None))),
        (CensusGap::ListingFailed, Some((Gap::ListingFailed, None))),
        (
            CensusGap::Missing { slot_id: 4 },
            Some((Gap::Missing, Some(4))),
        ),
        (
            CensusGap::Unread { slot_id: 5 },
            Some((Gap::Unread, Some(5))),
        ),
        (
            CensusGap::Extra {
                entry: "/run/netns/stray".into(),
            },
            None,
        ),
        (CensusGap::Truncated { omitted: 7 }, None),
    ];
    // Slot i at lifecycle generation i + 5, in both forms.
    let slots = |outcomes: &[(SlotOutcome, (Slot, Reason))]| {
        let domain = (0..)
            .zip(outcomes)
            .map(|(slot_id, (outcome, _))| SlotEvidence {
                slot_id,
                target: SlotTarget {
                    slot_generation: u64::from(slot_id) + 5,
                    sandbox_id: Some(format!("sandbox-{slot_id}")),
                    namespace: None,
                },
                outcome: *outcome,
            });
        let wire = (0..)
            .zip(outcomes)
            .map(
                |(slot_id, (_, (outcome, reason)))| FirewallRulesSlotEvidence {
                    slot_id: Some(slot_id),
                    slot_generation: Some(u64::from(slot_id) + 5),
                    outcome: (*outcome).into(),
                    reason: (*reason).into(),
                },
            );
        (domain.collect(), wire.collect())
    };
    let observed = |round, gaps, slots| Observed {
        round,
        answer: Ok(FirewallObservation {
            node_id: NODE,
            generation: 12,
            revision_id: Uuid::from_u128(12),
            census: observe::Census { gaps },
            slots,
        }),
    };

    // Complete, though slots failed.
    let (domain, wire) = slots(&outcomes);
    let mut expected = complete(7, 12, &[]);
    expected.slots = wire;
    let failed = observed(7, Vec::new(), domain);
    assert_eq!(failed.evidence(EPOCH), Ok(expected.clone()));

    // Incomplete, though every slot matched.
    let (domain, wire) = slots(&outcomes[..1]);
    expected.round = Some(8);
    expected.census = Census::Incomplete.into();
    expected.census_gaps = gaps
        .iter()
        .filter_map(|(_, wire)| *wire)
        .map(|(kind, slot_id)| FirewallRulesCensusGap {
            kind: kind.into(),
            slot_id,
        })
        .collect();
    expected.extra_namespaces = Some(1 + 7);
    expected.slots = wire;
    let gaps = gaps.into_iter().map(|(gap, _)| gap).collect();
    assert_eq!(observed(8, gaps, domain).evidence(EPOCH), Ok(expected));
}
