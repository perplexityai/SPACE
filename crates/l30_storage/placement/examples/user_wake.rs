//! The user-wake path, end to end: claim, do the real work, complete.
//!
//! Compiled by `cargo check --all-targets` (a CI gate), so it cannot drift
//! from the interface. A narrative, not a runnable service — the substrate
//! step is a stub.

use pplx_space_common_core::types::SandboxId;
use pplx_space_placement::{
    Attempt, BlockedReason, ClaimKind, ClaimOutcome, PlacementError, PlacementOutcome,
    PlacementProof, PlacementStore, PostgresPlacementStore,
};

/// How this caller's wake fails. A real handler maps these onto its own
/// surface (a 409 with a retry-after, a conflict, a failed-wake event); the
/// shape the example pins is that the losing dispositions are *errors the
/// caller reports*, not successes to swallow.
#[derive(Debug, thiserror::Error)]
enum WakeFailure {
    #[error(transparent)]
    Placement(#[from] PlacementError),
    #[error("another attempt owns the wake until {lease_expires_at}; retry after it")]
    AlreadyOwned { lease_expires_at: jiff::Timestamp },
    #[error("the sandbox is not wakeable: {reason:?}")]
    Blocked { reason: BlockedReason },
    #[error("the completion lost its CAS; report the failed wake, never retry with this fence")]
    CompletionLost,
}

/// Stand-in for the caller's substrate driver (create the pod, drive the
/// restore, persist evidence). Minutes may pass here — which is why placement
/// is two database operations with real work in between.
async fn drive_restore_on_the_node(_attempt: &Attempt) {
    // The substrate driver lives in the caller's crate, not in placement.
}

async fn wake(store: &PostgresPlacementStore, sandbox: SandboxId) -> Result<(), WakeFailure> {
    // Phase 1 — claim. Exactly one concurrent caller wins; every loser gets
    // a typed answer saying what to do. Nothing here talks to Kubernetes.
    let attempt = match store
        .claim_placement(sandbox, ClaimKind::UserRestore)
        .await?
    {
        ClaimOutcome::Claimed(attempt) => attempt,
        // Another attempt is waking it — report the deadline; the caller
        // retries after it.
        ClaimOutcome::AlreadyOwned { lease_expires_at } => {
            return Err(WakeFailure::AlreadyOwned { lease_expires_at });
        }
        // Evacuation-only cohort throttling — a user claim never takes this
        // path; matched exhaustively so new outcomes are a compile error.
        ClaimOutcome::Backoff { next_attempt_at: _ } => return Ok(()),
        // Not wakeable (deleting, failed, …); retrying changes nothing —
        // report the conflict.
        ClaimOutcome::Blocked(reason) => return Err(WakeFailure::Blocked { reason }),
    };

    // Phase 2 — the actual restore, owned by the caller. The fence is the
    // proof of ownership; `attempt.origin` says fresh wake vs re-drive of a
    // lapsed attempt.
    drive_restore_on_the_node(&attempt).await;

    // Phase 3 — complete, presenting the fence. One CAS re-checks everything
    // at flip time: fence current, no teardown landed, execution row
    // recording routable evidence.
    match store
        .complete_placement(PlacementProof::Fence(&attempt.fence))
        .await?
    {
        PlacementOutcome::Completed => Ok(()),
        // A newer suspend owns the row; the node work succeeded — continue.
        PlacementOutcome::SupersededBySuspend => Ok(()),
        // Teardown or newer claim won. Surface the loss; never retry with
        // this fence.
        PlacementOutcome::CasLost => Err(WakeFailure::CompletionLost),
    }
}

// current_thread, so the example compiles from this crate's own feature set
// (`rt`) rather than relying on another workspace crate unifying
// `rt-multi-thread` into the graph.
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = sqlx::PgPool::connect_lazy("postgres://localhost/space")?;
    let store = PostgresPlacementStore::new(pool, "sandboxes".to_string());
    wake(&store, SandboxId::new()).await?;
    Ok(())
}
