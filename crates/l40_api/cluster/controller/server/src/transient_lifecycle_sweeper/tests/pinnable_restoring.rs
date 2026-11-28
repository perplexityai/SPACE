//! Unpinned `restoring` rows whose automatic retry selector refuses — an
//! unordered pre-claim capture history, or a ready suspend capture outranked
//! only by a failed upload — still carry durable bytes an operator can pin via
//! `POST /admin/api/v1/sandboxes/{id}/restore-pin` once the row returns to
//! `suspended`. Neither the unrestorable-`restoring` sweep nor the aged
//! transient sweep may mark them `failed`; once unowned (or past the episode
//! deadline without a pod IP), the sweeper reverts them to `suspended` unless
//! a post-claim full snapshot makes resetting `lifecycle_changed_at` unsafe.

use std::time::Duration;

use pplx_space_accounts::NodeId;
use pplx_space_db_test_fixtures::pg_test_db;
use pplx_space_durations::PositiveDuration;
use pplx_space_node_model::SubstrateKind;
use pplx_space_sandbox_model::{SandboxId, SandboxLifecycle};
use pplx_space_sandbox_store::restore_pin::{pin_restore_source, restore_pin_state};
use pplx_space_sandbox_store::restore_source::{
    RestoreSourceMode, RestoreSourcePlacement, RestoreSourceSelection, select_restore_source,
};
use pplx_space_sandbox_store::sweeper::{
    AgedTransientRow, fail_unrestorable_restoring_query, list_aged_transient_query,
    list_unrestorable_restoring_query, revert_to_suspended,
};
use pplx_space_snapshot_model::LocalArtifactId;
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use uuid::Uuid;

use super::activation_owner::lifecycle_of;

const SEED_USER: Uuid = Uuid::from_u128(0x10);
const SEED_ORG: Uuid = Uuid::from_u128(0x01);

fn secs(value: u64) -> PositiveDuration {
    PositiveDuration::new(Duration::from_secs(value)).expect("non-zero")
}

async fn insert_unpinned_restoring_sandbox(
    pool: &PgPool,
    sandbox_id: SandboxId,
    managed_by: &str,
    lifecycle_age_secs: i64,
    placement_age_secs: i64,
    lease_active: bool,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, managed_by, lifecycle, network_policy_generation,
             lifecycle_changed_at, placement_changed_at,
             restore_lease_expires_at, restore_lease_token
         )
         VALUES (
             $1, $2, $3, $4, 'restoring', 1,
             NOW() - make_interval(secs => $5::BIGINT),
             NOW() - make_interval(secs => $6::BIGINT),
             CASE WHEN $7 THEN NOW() + INTERVAL '120 seconds' END,
             CASE WHEN $7 THEN space_uuid_v7() END
         )"
    ))
    .bind(sandbox_id)
    .bind(SEED_USER)
    .bind(SEED_ORG)
    .bind(managed_by)
    .bind(lifecycle_age_secs)
    .bind(placement_age_secs)
    .bind(lease_active)
    .execute(pool)
    .await?;
    Ok(())
}

/// A full suspend capture of `artifact`, described as a Direct restore loads
/// a pinned one (`pin_keeps_restore_loadable`).
async fn insert_suspend_capture(
    pool: &PgPool,
    sandbox_id: SandboxId,
    artifact: Uuid,
    state: &str,
    generation: Option<i64>,
    offset_from_now_secs: i64,
) -> TestResult {
    let snapshot_id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO snapshots (
             id, sandbox_id, type, state, timestamp, source_node_id,
             local_artifact_id, artifact_kind, artifact_origin, generation, org_id,
             runtime, runtime_version, guest_os, guest_arch, state_format_name,
             state_format_version
         )
         VALUES (
             $1, $2, 'full', $3, NOW() + make_interval(secs => $4::BIGINT), $5,
             $6, 'full', 'suspend', $7, $8,
             'fc', '1', 'linux', 'x86_64', 'fc-snapshot', '1.0'
         )"
    ))
    .bind(snapshot_id)
    .bind(sandbox_id)
    .bind(state)
    .bind(offset_from_now_secs)
    .bind(NodeId::new())
    .bind(artifact)
    .bind(generation)
    .bind(SEED_ORG)
    .execute(pool)
    .await?;
    if state == "ready" {
        sqlx::query(sql!(
            "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string)
             VALUES ($1, $2)"
        ))
        .bind(snapshot_id)
        .bind(format!("s3://test-bucket/{snapshot_id}?region=us-east-1"))
        .execute(pool)
        .await?;
    }
    Ok(())
}

async fn listed_unrestorable(
    pool: &PgPool,
    managed_by: &str,
    sandbox_id: SandboxId,
) -> TestResult<bool> {
    let rows: Vec<(SandboxId, i64)> =
        sqlx::query_as(sqlx::AssertSqlSafe(list_unrestorable_restoring_query()))
            .bind(managed_by)
            .bind(60_i64)
            .bind(1000_i64)
            .fetch_all(pool)
            .await?;
    Ok(rows.iter().any(|(id, _)| *id == sandbox_id))
}

async fn claim_unrestorable(
    pool: &PgPool,
    managed_by: &str,
    sandbox_id: SandboxId,
) -> TestResult<bool> {
    let claimed: Option<Uuid> =
        sqlx::query_scalar(sqlx::AssertSqlSafe(fail_unrestorable_restoring_query()))
            .bind(managed_by)
            .bind(60_i64)
            .bind(sandbox_id)
            .fetch_optional(pool)
            .await?;
    Ok(claimed.is_some())
}

async fn aged_candidate(
    pool: &PgPool,
    managed_by: &str,
    sandbox_id: SandboxId,
    episode_deadline_secs: Option<i64>,
) -> TestResult<Option<AgedTransientRow>> {
    let rows: Vec<AgedTransientRow> = sqlx::query_as(sqlx::AssertSqlSafe(
        list_aged_transient_query(SandboxLifecycle::Restoring),
    ))
    .bind(managed_by)
    .bind("restoring")
    .bind(60_i64)
    .bind(1000_i64)
    .bind(SubstrateKind::Kubernetes)
    .bind(episode_deadline_secs)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().find(|row| row.id == sandbox_id))
}

#[tokio::test(flavor = "current_thread")]
async fn unpinned_pinnable_pre_claim_captures_revert_to_suspended_instead_of_failing() -> TestResult
{
    let db = pg_test_db().await?;
    let pool = &db.pool;

    // 1. Unordered pre-claim history in the retry window (`gen = 5` + `gen =
    //    None`, both ready) and 2. pre-claim ready capture (`gen = 5`, older
    //    than pre-claim `placement_changed_at`) outranked only by a pre-claim
    //    `failed` upload (`gen = 6`): automatic retry refuses both, yet both
    //    spare under an active lease, list `ready_snapshot` once unowned,
    //    revert to `suspended`, and accept `pin_restore_source`.
    for (name, first_offset, second_state, second_gen) in [
        ("unordered pre-claim captures", -500_i64, "ready", None),
        (
            "ready capture outranked by failed upload",
            -900,
            "failed",
            Some(6),
        ),
    ] {
        let sandbox = SandboxId::new();
        let ready_artifact = Uuid::now_v7();
        insert_unpinned_restoring_sandbox(pool, sandbox, "space", 300, 600, true).await?;
        insert_suspend_capture(
            pool,
            sandbox,
            ready_artifact,
            "ready",
            Some(5),
            first_offset,
        )
        .await?;
        insert_suspend_capture(
            pool,
            sandbox,
            Uuid::now_v7(),
            second_state,
            second_gen,
            -450,
        )
        .await?;

        assert!(
            !listed_unrestorable(pool, "space", sandbox).await?,
            "{name}: unrestorable select must spare pinnable pre-claim capture"
        );
        assert!(
            !claim_unrestorable(pool, "space", sandbox).await?,
            "{name}: unrestorable fail CAS must spare pinnable pre-claim capture"
        );

        sqlx::query(sql!(
            "UPDATE sandboxes
                SET restore_lease_expires_at = NULL,
                    restore_lease_token = NULL
              WHERE id = $1"
        ))
        .bind(sandbox)
        .execute(pool)
        .await?;

        let candidate = aged_candidate(pool, "space", sandbox, None)
            .await?
            .expect("aged restoring row listed");
        assert!(
            candidate.ready_snapshot,
            "{name}: aged candidate must classify as ready_snapshot so decide reverts"
        );
        assert!(
            revert_to_suspended(
                pool,
                sandbox,
                "space",
                SandboxLifecycle::Restoring,
                secs(60),
                None,
            )
            .await?
            .is_some(),
            "{name}: revert_to_suspended must revert to suspended"
        );
        assert_eq!(lifecycle_of(pool, sandbox).await?, "suspended", "{name}");

        let pin_state = restore_pin_state(pool, sandbox).await?.expect("sandbox");
        let token = pin_state
            .lifecycle_changed_at
            .expect("lifecycle_changed_at");
        assert!(
            pin_restore_source(
                pool,
                sandbox,
                LocalArtifactId::from_uuid(ready_artifact),
                token,
                pin_state.restore_node_id,
            )
            .await?,
            "{name}: reverted suspended row must accept restore-pin"
        );
    }

    // 3. Overdue K8s restoring episode with a live lease and no pod IP also
    //    waives ownership and reverts an unordered pre-claim history to
    //    `suspended`.
    let overdue = SandboxId::new();
    insert_unpinned_restoring_sandbox(pool, overdue, "k8s", 2400, 3000, true).await?;
    insert_suspend_capture(pool, overdue, Uuid::now_v7(), "ready", Some(5), -2700).await?;
    insert_suspend_capture(pool, overdue, Uuid::now_v7(), "ready", None, -2500).await?;
    let overdue_candidate = aged_candidate(pool, "k8s", overdue, Some(1800))
        .await?
        .expect("overdue k8s restoring row listed");
    assert!(overdue_candidate.episode_overdue);
    assert!(overdue_candidate.ready_snapshot);
    assert!(
        revert_to_suspended(
            pool,
            overdue,
            "k8s",
            SandboxLifecycle::Restoring,
            secs(60),
            Some(secs(1800)),
        )
        .await?
        .is_some()
    );
    assert_eq!(lifecycle_of(pool, overdue).await?, "suspended");

    // 4. Contrasts:
    //    a) Pre-claim unordered captures + post-claim full snapshot: spared
    //       from `failed` (`ready_snapshot = true`), not reverted (`restoring`
    //       kept so `lifecycle_changed_at` continues fencing the post-claim
    //       row), and once unowned, `restore-pin` lists and pins only the
    //       pre-claim captures without touching `lifecycle_changed_at`.
    let post_claim = SandboxId::new();
    let pre_stamped = Uuid::now_v7();
    let pre_unstamped = Uuid::now_v7();
    let post_artifact = Uuid::now_v7();
    insert_unpinned_restoring_sandbox(pool, post_claim, "space", 300, 600, false).await?;
    insert_suspend_capture(pool, post_claim, pre_stamped, "ready", Some(5), -500).await?;
    insert_suspend_capture(pool, post_claim, pre_unstamped, "ready", None, -400).await?;
    insert_suspend_capture(pool, post_claim, post_artifact, "ready", Some(6), -60).await?;
    assert!(!listed_unrestorable(pool, "space", post_claim).await?);
    assert!(
        aged_candidate(pool, "space", post_claim, None)
            .await?
            .expect("post-claim row listed")
            .ready_snapshot
    );
    assert_eq!(
        revert_to_suspended(
            pool,
            post_claim,
            "space",
            SandboxLifecycle::Restoring,
            secs(60),
            None,
        )
        .await?,
        None
    );
    assert_eq!(lifecycle_of(pool, post_claim).await?, "restoring");

    let pin_state = restore_pin_state(pool, post_claim)
        .await?
        .expect("post-claim restoring row readable");
    assert_eq!(pin_state.lifecycle, "restoring");
    assert!(pin_state.unordered);
    assert!(pin_state.restoring_pinnable);
    let listed_pre_claim: Vec<Uuid> = pin_state
        .captures
        .iter()
        .map(|capture| capture.local_artifact_id.as_uuid())
        .collect();
    assert_eq!(
        listed_pre_claim,
        vec![pre_unstamped, pre_stamped],
        "restore-pin on a restoring row must list only pre-claim pinnable captures"
    );
    let claim_token = pin_state
        .lifecycle_changed_at
        .expect("lifecycle_changed_at");
    let expected_node = pin_state.restore_node_id;
    assert!(
        !pin_restore_source(
            pool,
            post_claim,
            LocalArtifactId::from_uuid(post_artifact),
            claim_token,
            expected_node,
        )
        .await?,
        "post-claim capture must not be pinnable while restoring"
    );
    // While a restore lease is live, `restore-pin` refuses on the restoring row.
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET restore_lease_expires_at = NOW() + INTERVAL '60 seconds',
                restore_lease_token = $2
          WHERE id = $1"
    ))
    .bind(post_claim)
    .bind(Uuid::now_v7())
    .execute(pool)
    .await?;
    assert!(
        !restore_pin_state(pool, post_claim)
            .await?
            .expect("row")
            .restoring_pinnable
    );
    assert!(
        !pin_restore_source(
            pool,
            post_claim,
            LocalArtifactId::from_uuid(pre_stamped),
            claim_token,
            expected_node,
        )
        .await?
    );
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET restore_lease_expires_at = NULL,
                restore_lease_token = NULL
          WHERE id = $1"
    ))
    .bind(post_claim)
    .execute(pool)
    .await?;
    assert!(
        pin_restore_source(
            pool,
            post_claim,
            LocalArtifactId::from_uuid(pre_stamped),
            claim_token,
            expected_node,
        )
        .await?,
        "unowned restoring row must accept a pre-claim pin"
    );
    let after_pin = restore_pin_state(pool, post_claim).await?.expect("row");
    assert_eq!(after_pin.lifecycle, "restoring");
    assert_eq!(
        after_pin.lifecycle_changed_at,
        Some(claim_token),
        "pinning a restoring row must preserve lifecycle_changed_at"
    );
    assert_eq!(
        after_pin.pinned_artifact,
        Some(LocalArtifactId::from_uuid(pre_stamped))
    );
    assert!(matches!(
        select_restore_source(
            pool,
            post_claim,
            RestoreSourceMode::Retry,
            RestoreSourcePlacement::Any,
        )
        .await?,
        RestoreSourceSelection::Selected(_)
    ));
    assert!(
        revert_to_suspended(
            pool,
            post_claim,
            "space",
            SandboxLifecycle::Restoring,
            secs(60),
            None,
        )
        .await?
        .is_some(),
        "once pinned to a pre-claim capture, sweeper revert to suspended succeeds and keeps the pin"
    );
    assert_eq!(lifecycle_of(pool, post_claim).await?, "suspended");

    //    b) Stale prior-cycle ordered ready capture with no newer failed row
    //       (`timestamp < placement_changed_at < lifecycle_changed_at`):
    //       clearing `placement_changed_at` on revert would let unpinned
    //       `suspended` restore silently load pre-wake bytes, so it stays
    //       unrecoverable (`ready_snapshot = false`, no revert, swept to
    //       `failed`).
    let stale_prior = SandboxId::new();
    insert_unpinned_restoring_sandbox(pool, stale_prior, "space", 300, 600, false).await?;
    insert_suspend_capture(pool, stale_prior, Uuid::now_v7(), "ready", Some(5), -900).await?;
    assert!(
        !aged_candidate(pool, "space", stale_prior, None)
            .await?
            .expect("stale prior-cycle row listed")
            .ready_snapshot
    );
    assert_eq!(
        revert_to_suspended(
            pool,
            stale_prior,
            "space",
            SandboxLifecycle::Restoring,
            secs(60),
            None,
        )
        .await?,
        None
    );
    assert!(claim_unrestorable(pool, "space", stale_prior).await?);
    assert_eq!(lifecycle_of(pool, stale_prior).await?, "failed");

    //    c) Only a failed pre-claim upload (no ready capture): swept to
    //       `failed`.
    let only_failed = SandboxId::new();
    insert_unpinned_restoring_sandbox(pool, only_failed, "space", 300, 600, true).await?;
    insert_suspend_capture(pool, only_failed, Uuid::now_v7(), "failed", Some(5), -450).await?;
    assert!(listed_unrestorable(pool, "space", only_failed).await?);
    assert!(claim_unrestorable(pool, "space", only_failed).await?);
    assert_eq!(lifecycle_of(pool, only_failed).await?, "failed");

    Ok(())
}
