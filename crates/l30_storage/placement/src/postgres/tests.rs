//! Tests for the Postgres placement store, split at the operation seams:
//! [`claim`] covers `claim_placement`, [`completion`] covers
//! `complete_placement` under both proofs, and [`support`] holds the shared
//! fixtures. The SQL-render and typed-refusal unit tests below reach no
//! database; the Postgres suites in the submodules skip gracefully when no
//! Docker daemon is reachable — a green run that skipped them is not a green
//! run.

mod claim;
mod completion;
mod support;

use futures_util::future::join_all;
use pplx_space_common_core::sql;
use pplx_space_common_core::types::{ExecutionGeneration, SandboxId, SandboxLifecycle};
use pplx_space_db_test_fixtures::{TestDb, try_pg_test_db, try_pg_test_db_with_max_connections};
use sqlx::PgPool;
use uuid::Uuid;

use super::{
    PLACEMENT_COMPLETION_BLOCKING_LIFECYCLES, PLACEMENT_SUPERSEDING_SUSPEND_LIFECYCLES,
    PostgresPlacementStore, USER_RESTORE_CLAIM_SQL, recovery_lease_ttl_secs,
    restore_execution_ownership_ttl_secs,
};
use crate::lifecycle::StoredSandboxLifecycle;
use crate::model::{Attempt, ClaimOrigin, LeaseToken, PlacementFence, PlacementProof};
use crate::outcome::{BlockedReason, ClaimKind, ClaimOutcome, PlacementOutcome};
use crate::store::{PlacementError, PlacementStore};

use support::*;

// --- Unit tests over the store's stored vocabulary and fast rejection. These
// --- do not reach a live Postgres.

#[test]
fn every_blocking_entry_round_trips_through_the_stored_vocabulary() {
    for entry in PLACEMENT_COMPLETION_BLOCKING_LIFECYCLES
        .into_iter()
        .chain(PLACEMENT_SUPERSEDING_SUSPEND_LIFECYCLES)
    {
        assert_eq!(
            entry.as_str().parse::<StoredSandboxLifecycle>(),
            Ok(entry),
            "guard-set entries round-trip through the stored vocabulary"
        );
    }
}

#[tokio::test]
async fn unfenced_claim_kinds_are_rejected_without_touching_the_database() {
    // A lazy pool never connects; reaching the database would error
    // differently than the typed rejection asserted here.
    let pool = PgPool::connect_lazy("postgres://localhost:1/unreachable")
        .expect("lazy pool construction does not connect");
    let store = PostgresPlacementStore::new(pool, "sandboxes".to_string());
    for kind in [ClaimKind::OnDemandCreate, ClaimKind::PooledClaim] {
        let err = store
            .claim_placement(SandboxId::new(), kind)
            .await
            .expect_err("unfenced claim kinds must be rejected");
        assert!(
            matches!(err, PlacementError::UnfencedClaimKind(rejected) if rejected == kind),
            "unexpected error for {kind}: {err}"
        );
    }
}
