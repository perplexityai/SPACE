use std::convert::Infallible;

use super::*;

const LEASE: Duration = Duration::from_secs(60);
const FAILURE_BUDGET: u64 = 2;
const ROLLING_READS: [&str; 5] = [
    "lock_leasable",
    "lock_handoff",
    "held_elsewhere",
    "candidates",
    "failed",
];

/// A header and its items, recording the steps a claim or renewal ran on
/// them. A pending item's placement is whether it fits the store's caps.
#[derive(Clone)]
struct Fake {
    canary: bool,
    /// Whether an item is done, and how many are in flight, as the canary
    /// read sees them.
    progress: (bool, u64),
    /// How many are in flight as the candidates read sees them.
    in_flight: u64,
    pending: Vec<(u8, bool)>,
    failed: u64,
    steps: Vec<&'static str>,
}

fn rolling() -> Fake {
    Fake {
        canary: false,
        progress: (false, 0),
        in_flight: 0,
        pending: vec![(1, true), (2, true)],
        failed: 0,
        steps: Vec::new(),
    }
}

impl FailedItems for Fake {
    type Error = Infallible;

    async fn failed(&mut self) -> Result<u64, Infallible> {
        self.steps.push("failed");
        Ok(self.failed)
    }
}

impl ClaimStore for Fake {
    type Header = ();
    type Subject = u8;
    type Placement = bool;
    type Claimed = u8;

    async fn lock_leasable(&mut self) -> Result<Option<((), Pacing)>, Infallible> {
        self.steps.push("lock_leasable");
        let pacing = Pacing {
            canary: self.canary,
            per_cluster: 2,
            failure_budget: FAILURE_BUDGET,
        };
        Ok(Some(((), pacing)))
    }

    async fn canary_progress(&mut self) -> Result<(bool, u64), Infallible> {
        self.steps.push("canary_progress");
        Ok(self.progress)
    }

    async fn lock_handoff(&mut self, _: &()) -> Result<(), Infallible> {
        self.steps.push("lock_handoff");
        Ok(())
    }

    async fn held_elsewhere(&mut self, _: &()) -> Result<HashSet<u8>, Infallible> {
        self.steps.push("held_elsewhere");
        Ok(HashSet::new())
    }

    async fn candidates(&mut self, _: &(), _: u32) -> Result<Candidates<u8, bool>, Infallible> {
        self.steps.push("candidates");
        Ok(Candidates {
            in_flight: self.in_flight,
            pending: self.pending.clone(),
        })
    }

    async fn claim(
        &mut self,
        _: &(),
        subject: u8,
        fits: &bool,
        _: uuid::Uuid,
        _: Duration,
    ) -> Result<Option<u8>, Infallible> {
        self.steps.push("claim");
        Ok(fits.then_some(subject))
    }
}

impl RenewStore for Fake {
    type Error = Infallible;

    async fn lock_unfinished(&mut self) -> Result<bool, Infallible> {
        self.steps.push("lock_unfinished");
        Ok(true)
    }

    async fn lock_handoff(&mut self) -> Result<(), Infallible> {
        self.steps.push("lock_handoff");
        Ok(())
    }

    async fn lock_item(&mut self) -> Result<(), Infallible> {
        self.steps.push("lock_item");
        Ok(())
    }

    async fn extend(&mut self, _: Duration) -> Result<bool, Infallible> {
        self.steps.push("extend");
        Ok(true)
    }
}

/// Each case names a regression the store's Postgres suite misses: a read
/// that races an item changing between two steps, items claimed and then
/// dropped, left in flight with nobody holding them, or a miss that takes a
/// slot.
#[tokio::test(flavor = "current_thread")]
async fn a_claim_reads_in_order_and_leases_only_what_it_returns() {
    let canary = Fake {
        canary: true,
        ..rolling()
    };
    let cases = [
        (
            "canary progress read under the header lock, failures counted after the candidates",
            canary.clone(),
            &[1][..],
            vec![
                "lock_leasable",
                "canary_progress",
                "lock_handoff",
                "held_elsewhere",
                "candidates",
                "failed",
                "claim",
            ],
        ),
        (
            "a canary in flight claims nothing, though it finishes before the candidates read",
            Fake {
                progress: (false, 1),
                ..canary
            },
            &[][..],
            vec!["lock_leasable", "canary_progress"],
        ),
        (
            "at the failure budget nothing is claimed, not even to be dropped",
            Fake {
                failed: FAILURE_BUDGET,
                ..rolling()
            },
            &[][..],
            ROLLING_READS.to_vec(),
        ),
        (
            "a candidate that doesn't fit uses no slot",
            Fake {
                in_flight: 1,
                pending: vec![(1, false), (2, true)],
                ..rolling()
            },
            &[2][..],
            [&ROLLING_READS[..], &["claim", "claim"]].concat(),
        ),
    ];
    for (case, mut store, claimed, steps) in cases {
        assert_eq!(
            claim(&mut store, LEASE, 10).await,
            Ok(claimed.to_vec()),
            "{case}"
        );
        assert_eq!(store.steps, steps, "{case}");
    }
}

/// A renewal takes the header lock before the handoff lock, in the order a
/// claim does, or the two deadlock.
#[tokio::test(flavor = "current_thread")]
async fn a_renewal_locks_the_header_before_the_handoff() {
    let mut store = rolling();
    assert_eq!(renew(&mut store, LEASE).await, Ok(true));
    assert_eq!(
        store.steps,
        ["lock_unfinished", "lock_handoff", "lock_item", "extend"]
    );
}
