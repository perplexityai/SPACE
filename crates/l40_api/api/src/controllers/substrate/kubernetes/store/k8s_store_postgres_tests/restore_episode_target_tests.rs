//! The Kubernetes store's restore gate reads a restore episode target
//! (`sandboxes.restore_episode_target`) as it reads the node drain's marker:
//! a user restore neither claims nor joins a system restore in flight, and a
//! `stopped` target refuses a user wake as a stopped re-placement does.

use pplx_space_sandbox_store::test_support::restore_target::{
    HeldTarget, assert_episode_target_reads_as_marker, hold_restore_target,
};

use super::*;

/// The error's variant, without the ids and messages that differ between
/// fixtures.
fn error_variant(err: &SubstrateError) -> String {
    let rendered = format!("{err:?}");
    let variant = rendered.split([' ', '{', '(']).next().unwrap_or_default();
    format!("Err({variant})")
}

fn claim_variant(claim: &RestoreClaim) -> &'static str {
    match claim {
        RestoreClaim::FreshFromSuspended { .. } => "FreshFromSuspended",
        RestoreClaim::FreshFromStopped => "FreshFromStopped",
        RestoreClaim::RedroveRestoring { .. } => "RedroveRestoring",
    }
}

/// A `restoring` row holding `held`, under a restore lease that is live for
/// `lease` (`"-1 seconds"` for one that expired).
async fn restoring_row(
    fixture: &PostgresStore,
    held: Option<HeldTarget>,
    lease: &str,
) -> TestResult<SandboxId> {
    let id = SandboxId::new();
    insert_sandbox(&fixture.pool, id, "restoring").await?;
    set_restore_lease(&fixture.pool, id, lease).await?;
    hold_restore_target(&fixture.pool, id, held).await?;
    Ok(id)
}

/// The user restore claim and a user wake of a `restoring` row, under a live
/// lease and once it lapsed, and the user restore observation under a live
/// one.
#[tokio::test]
async fn the_restore_gate_reads_the_episode_target_as_the_marker() -> TestResult {
    let fixture = postgres_store().await?;

    for lease in ["90 seconds", "-1 seconds"] {
        assert_episode_target_reads_as_marker(
            &format!("begin_restore_lifecycle (lease {lease})"),
            async |held| -> TestResult<_> {
                let id = restoring_row(&fixture, held, lease).await?;
                let claim = fixture.store.begin_restore_lifecycle(id, false).await;
                Ok(match &claim {
                    Ok(claim) => claim_variant(claim).to_owned(),
                    Err(err) => error_variant(err),
                })
            },
        )
        .await?;

        assert_episode_target_reads_as_marker(
            &format!("prepare_wake_sandbox (lease {lease})"),
            async |held| -> TestResult<_> {
                let id = restoring_row(&fixture, held, lease).await?;
                let sandbox = fixture.store.require_sandbox(id).await?;
                let prepared = fixture.store.prepare_wake_sandbox(&sandbox, id, None).await;
                Ok(match &prepared {
                    Ok(prepared) => format!(
                        "Ok({:?})",
                        prepared.restore_claim.as_ref().map(claim_variant)
                    ),
                    Err(err) => error_variant(err),
                })
            },
        )
        .await?;
    }
    // A lapsed lease is claimable whoever's restore it was, so only a live
    // one can tell them apart.
    assert_episode_target_reads_as_marker("observe_user_restore", async |held| -> TestResult<_> {
        let id = restoring_row(&fixture, held, "90 seconds").await?;
        Ok(match fixture.store.observe_user_restore(id).await? {
            UserRestoreObservation::InFlight { .. } => "InFlight".to_owned(),
            observation => format!("{observation:?}"),
        })
    })
    .await?;
    Ok(())
}
