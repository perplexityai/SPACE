//! Concurrency coverage for [`claim_user_restore`], [`claim_evacuation_restore`],
//! and [`crate::direct::claim_restore_lease`]: when a fresh `suspended ->
//! restoring` claim waits for `FOR UPDATE` behind an in-flight backup
//! registration's `FOR KEY SHARE`, the claim's committed `lifecycle_changed_at`
//! advances past the lock wait and any pre-claim backup row, while a
//! `restoring -> restoring` retry preserves that cutoff so post-claim backups
//! remain fenced out. A fresh claim is refused while a registered capture is
//! stamped ahead of the database clock, except the evacuation claim of a
//! released `stopped` row, which reads no cutoff.

use std::time::Duration;

use pplx_space_db_test_fixtures::{
    SEED_ORG_ID as SEED_ORG, SEED_USER_ID as SEED_USER, try_pg_test_db_with_max_connections,
};
use pplx_space_sandbox_model::{SandboxId, SandboxLifecycle};
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use uuid::Uuid;

use super::{
    RestoreClaimError, advance_fresh_restore_claim_cutoff, claim_evacuation_restore,
    claim_user_restore,
};
use crate::direct::claim_restore_lease;
use crate::restore_source::{
    RestoreSourceMode, RestoreSourcePlacement, RestoreSourceSelection, select_restore_source,
    stopped_evacuation_restore_source,
};

#[derive(Clone, Copy, Debug)]
enum ClaimKind {
    User,
    Evacuation,
    Direct,
}

async fn insert_claimable_sandbox(pool: &PgPool, kind: ClaimKind) -> TestResult<SandboxId> {
    let id = SandboxId::new();
    let (managed_by, evacuation_target) = match kind {
        ClaimKind::User | ClaimKind::Evacuation => (
            "k8s",
            matches!(kind, ClaimKind::Evacuation).then_some("started"),
        ),
        ClaimKind::Direct => ("space", None),
    };
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, managed_by, lifecycle, template_id,
             network_policy_generation, evacuation_restore_target,
             lifecycle_changed_at, placement_changed_at
         )
         VALUES (
             $1, $2, $3, $4, 'suspended', 'tmpl-restore-lease-test',
             1, $5,
             NOW() - INTERVAL '10 minutes', NOW() - INTERVAL '20 minutes'
         )"
    ))
    .bind(id)
    .bind(SEED_USER)
    .bind(SEED_ORG)
    .bind(managed_by)
    .bind(evacuation_target)
    .execute(pool)
    .await?;
    Ok(id)
}

/// Run `kind`'s claim and return whether it claimed.
async fn try_claim(
    pool: &PgPool,
    kind: ClaimKind,
    sandbox_id: SandboxId,
) -> Result<bool, RestoreClaimError> {
    Ok(match kind {
        ClaimKind::User => claim_user_restore(pool, sandbox_id, 60, Uuid::now_v7(), 60, None)
            .await?
            .is_some(),
        ClaimKind::Evacuation => {
            claim_evacuation_restore(pool, sandbox_id, 60, 60, Some(SandboxLifecycle::Started))
                .await?
                .is_some()
        }
        ClaimKind::Direct => claim_restore_lease(pool, sandbox_id, 60, Uuid::now_v7())
            .await?
            .is_some(),
    })
}

async fn run_claim(pool: &PgPool, kind: ClaimKind, sandbox_id: SandboxId) -> TestResult {
    assert!(
        try_claim(pool, kind, sandbox_id).await?,
        "{kind:?} claim must claim"
    );
    Ok(())
}

async fn insert_backup_at_clock(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    generation: i64,
) -> Result<(Uuid, time::OffsetDateTime), sqlx::Error> {
    let snapshot_id = Uuid::now_v7();
    let artifact_id = Uuid::now_v7();
    let ts: time::OffsetDateTime = sqlx::query_scalar(sql!(
        "INSERT INTO snapshots (
             id, sandbox_id, type, state, \"timestamp\",
             local_artifact_id, artifact_kind, artifact_origin, generation, org_id
         )
         VALUES (
             $1, $2, 'full', 'ready', clock_timestamp(),
             $3, 'full', 'suspend', $4, $5
         )
         RETURNING \"timestamp\""
    ))
    .bind(snapshot_id)
    .bind(sandbox_id)
    .bind(artifact_id)
    .bind(generation)
    .bind(SEED_ORG)
    .fetch_one(executor)
    .await?;
    Ok((snapshot_id, ts))
}

async fn attach_storage_location(pool: &PgPool, snapshot_id: Uuid, uri: &str) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string, org_id)
         VALUES ($1, $2, $3)"
    ))
    .bind(snapshot_id)
    .bind(uri)
    .bind(SEED_ORG)
    .execute(pool)
    .await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn fresh_restore_claims_advance_cutoff_past_pre_claim_lock_wait_and_retries_preserve_it()
-> TestResult {
    // Three concurrent pool consumers run during the lock-wait probe:
    // `key_share_tx`, `claim_fut`, and the `pg_stat_activity` poll.
    let Some(db) = try_pg_test_db_with_max_connections(4).await? else {
        return Ok(());
    };
    let pool = &db.pool;

    for kind in [ClaimKind::User, ClaimKind::Evacuation, ClaimKind::Direct] {
        let sandbox_id = insert_claimable_sandbox(pool, kind).await?;
        let pre_claim_uri = format!("s3://test/pre-claim-{sandbox_id}?region=us-east-1");
        let post_claim_uri = format!("s3://test/post-claim-{sandbox_id}?region=us-east-1");

        // Hold `FOR KEY SHARE` on `sandboxes`, as `snapshot::store::insert` does
        // while registering a pre-claim backup.
        let mut key_share_tx = pool.begin().await?;
        let key_share_pid: i32 = sqlx::query_scalar(sql!("SELECT pg_backend_pid()"))
            .fetch_one(&mut *key_share_tx)
            .await?;
        sqlx::query(sql!("SELECT 1 FROM sandboxes WHERE id = $1 FOR KEY SHARE"))
            .bind(sandbox_id)
            .execute(&mut *key_share_tx)
            .await?;

        let claim_fut = run_claim(pool, kind, sandbox_id);
        let backup_fut = async {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            loop {
                let blocking_claim: bool = sqlx::query_scalar(sql!(
                    "SELECT EXISTS (
                         SELECT 1
                           FROM pg_stat_activity a
                          WHERE $1 = ANY(pg_blocking_pids(a.pid))
                     )"
                ))
                .bind(key_share_pid)
                .fetch_one(pool)
                .await?;
                if blocking_claim {
                    break;
                }
                if tokio::time::Instant::now() > deadline {
                    return Err(format!("{kind:?} claim never blocked on FOR KEY SHARE").into());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            // Guarantee wall-clock advancement past the claim transaction's `NOW()`.
            tokio::time::sleep(Duration::from_millis(25)).await;
            let (pre_id, pre_ts) =
                insert_backup_at_clock(&mut *key_share_tx, sandbox_id, 5).await?;
            key_share_tx.commit().await?;
            attach_storage_location(pool, pre_id, &pre_claim_uri).await?;
            TestResult::Ok(pre_ts)
        };

        let (claim_res, backup_res) = tokio::time::timeout(Duration::from_secs(10), async {
            tokio::join!(claim_fut, backup_fut)
        })
        .await
        .map_err(|_| format!("{kind:?} claim + pre-claim backup timed out"))?;
        claim_res?;
        let pre_claim_ts = backup_res?;

        let claim_cutoff: time::OffsetDateTime = sqlx::query_scalar(sql!(
            "SELECT lifecycle_changed_at FROM sandboxes WHERE id = $1"
        ))
        .bind(sandbox_id)
        .fetch_one(pool)
        .await?;
        assert!(
            claim_cutoff > pre_claim_ts,
            "{kind:?}: claim cutoff ({claim_cutoff}) must exceed pre-claim backup timestamp ({pre_claim_ts})"
        );

        let selected = select_restore_source(
            pool,
            sandbox_id,
            RestoreSourceMode::Retry,
            RestoreSourcePlacement::Any,
        )
        .await?;
        match selected {
            RestoreSourceSelection::Selected(source) => {
                assert_eq!(
                    source.storage_connection_string, pre_claim_uri,
                    "{kind:?}: retry must select the pre-claim backup"
                );
            }
            other => {
                return Err(
                    format!("{kind:?}: expected Selected({pre_claim_uri}), got {other:?}").into(),
                );
            }
        }

        // Expire any restore lease, register a post-claim backup at a higher
        // generation, and verify a `restoring -> restoring` retry preserves
        // `claim_cutoff` so the post-claim backup stays fenced out.
        tokio::time::sleep(Duration::from_millis(10)).await;
        let (post_id, post_claim_ts) = insert_backup_at_clock(pool, sandbox_id, 6).await?;
        attach_storage_location(pool, post_id, &post_claim_uri).await?;
        assert!(post_claim_ts > claim_cutoff);

        sqlx::query(sql!(
            "UPDATE sandboxes
                SET restore_lease_expires_at = NOW() - INTERVAL '1 second'
              WHERE id = $1
                AND restore_lease_token IS NOT NULL"
        ))
        .bind(sandbox_id)
        .execute(pool)
        .await?;
        run_claim(pool, kind, sandbox_id).await?;

        let retry_cutoff: time::OffsetDateTime = sqlx::query_scalar(sql!(
            "SELECT lifecycle_changed_at FROM sandboxes WHERE id = $1"
        ))
        .bind(sandbox_id)
        .fetch_one(pool)
        .await?;
        assert_eq!(
            retry_cutoff, claim_cutoff,
            "{kind:?}: restoring -> restoring retry must preserve lifecycle_changed_at"
        );
        let after_retry = select_restore_source(
            pool,
            sandbox_id,
            RestoreSourceMode::Retry,
            RestoreSourcePlacement::Any,
        )
        .await?;
        match after_retry {
            RestoreSourceSelection::Selected(source) => {
                assert_eq!(
                    source.storage_connection_string, pre_claim_uri,
                    "{kind:?}: post-claim backup must stay excluded across retry"
                );
            }
            other => {
                return Err(format!(
                    "{kind:?}: expected Selected({pre_claim_uri}) after retry, got {other:?}"
                )
                .into());
            }
        }
    }

    Ok(())
}

/// What a refused claim must leave alone: lifecycle, `lifecycle_changed_at`,
/// the lease pair and the evacuation marker.
type RowState = (
    String,
    time::OffsetDateTime,
    Option<Uuid>,
    Option<time::OffsetDateTime>,
    Option<String>,
);

/// A disk backup stamped ahead of the database clock, as one left by an
/// earlier stop is after the clock steps backward, doesn't refuse a fresh
/// claim: a suspended restore orders only full snapshots, so no cutoff below
/// it changes what the retry restores.
#[tokio::test(flavor = "current_thread")]
async fn a_disk_backup_stamped_ahead_of_the_clock_doesnt_refuse_a_claim() -> TestResult {
    let Some(db) = try_pg_test_db_with_max_connections(4).await? else {
        return Ok(());
    };
    let pool = &db.pool;
    for kind in [ClaimKind::User, ClaimKind::Evacuation, ClaimKind::Direct] {
        let sandbox_id = insert_claimable_sandbox(pool, kind).await?;
        let (full_id, _) = insert_backup_at_clock(pool, sandbox_id, 5).await?;
        attach_storage_location(
            pool,
            full_id,
            &format!("s3://test/full-{sandbox_id}?region=us-east-1"),
        )
        .await?;
        sqlx::query(sql!(
            "INSERT INTO snapshots (
                 id, sandbox_id, type, state, \"timestamp\",
                 local_artifact_id, artifact_kind, artifact_origin, org_id
             )
             VALUES ($1, $2, 'disk', 'ready', clock_timestamp() + INTERVAL '1 hour', $3,
                     'disk', 'stop', $4)"
        ))
        .bind(Uuid::now_v7())
        .bind(sandbox_id)
        .bind(Uuid::now_v7())
        .bind(SEED_ORG)
        .execute(pool)
        .await?;
        let claimed = try_claim(pool, kind, sandbox_id)
            .await
            .unwrap_or_else(|error| panic!("{kind:?}: the claim isn't refused: {error:?}"));
        assert!(claimed, "{kind:?}: the claim lands");
    }
    Ok(())
}

/// A placement stamped ahead of the database clock, as one is after the
/// clock steps backward, refuses a fresh claim: a cutoff at or before it
/// would read the placement as one during the restore, and the retry window
/// would admit the capture from before it. Once the placement is behind the
/// clock, the claim lands.
#[tokio::test(flavor = "current_thread")]
async fn a_placement_ahead_of_the_clock_refuses_a_claim() -> TestResult {
    let Some(db) = try_pg_test_db_with_max_connections(4).await? else {
        return Ok(());
    };
    let pool = &db.pool;
    for kind in [ClaimKind::User, ClaimKind::Evacuation, ClaimKind::Direct] {
        let sandbox_id = insert_claimable_sandbox(pool, kind).await?;
        let (old_id, _) = insert_backup_at_clock(pool, sandbox_id, 5).await?;
        attach_storage_location(
            pool,
            old_id,
            &format!("s3://test/old-{sandbox_id}?region=us-east-1"),
        )
        .await?;
        sqlx::query(sql!(
            "UPDATE snapshots SET \"timestamp\" = clock_timestamp() - INTERVAL '1 hour'
              WHERE id = $1"
        ))
        .bind(old_id)
        .execute(pool)
        .await?;
        sqlx::query(sql!(
            "UPDATE sandboxes SET placement_changed_at = clock_timestamp() + INTERVAL '1 hour'
              WHERE id = $1"
        ))
        .bind(sandbox_id)
        .execute(pool)
        .await?;
        let refused = try_claim(pool, kind, sandbox_id).await;
        assert!(
            matches!(refused, Err(RestoreClaimError::CaptureAheadOfClock { .. })),
            "{kind:?}: {refused:?}"
        );
        sqlx::query(sql!(
            "UPDATE sandboxes SET placement_changed_at = clock_timestamp() - INTERVAL '30 minutes'
              WHERE id = $1"
        ))
        .bind(sandbox_id)
        .execute(pool)
        .await?;
        assert!(
            try_claim(pool, kind, sandbox_id)
                .await
                .unwrap_or_else(|error| panic!("{kind:?}: refused: {error:?}")),
            "{kind:?}: the claim lands once the placement is behind the clock"
        );
    }
    Ok(())
}

/// A row pinned to an artifact with a full backup restores exactly that
/// artifact on retry, so a named backup of another artifact stamped ahead of
/// the clock doesn't refuse its claim. The pinned backup itself ahead of the
/// clock still does.
#[tokio::test(flavor = "current_thread")]
async fn only_the_pinned_backup_ahead_of_the_clock_refuses_a_pinned_claim() -> TestResult {
    let Some(db) = try_pg_test_db_with_max_connections(4).await? else {
        return Ok(());
    };
    let pool = &db.pool;
    // (the pinned backup is ahead of the clock, the claim lands)
    for (pinned_ahead, lands) in [(false, true), (true, false)] {
        for kind in [ClaimKind::User, ClaimKind::Evacuation, ClaimKind::Direct] {
            let sandbox_id = insert_claimable_sandbox(pool, kind).await?;
            let (pinned_id, _) = insert_backup_at_clock(pool, sandbox_id, 5).await?;
            attach_storage_location(
                pool,
                pinned_id,
                &format!("s3://test/pinned-{sandbox_id}?region=us-east-1"),
            )
            .await?;
            sqlx::query(sql!(
                "UPDATE sandboxes s
                    SET current_local_artifact_id = sn.local_artifact_id,
                        current_local_artifact_kind = 'full',
                        current_local_artifact_origin = 'suspend'
                   FROM snapshots sn
                  WHERE s.id = $1 AND sn.id = $2"
            ))
            .bind(sandbox_id)
            .bind(pinned_id)
            .execute(pool)
            .await?;
            sqlx::query(sql!(
                "INSERT INTO snapshots (
                     id, sandbox_id, type, state, \"timestamp\",
                     local_artifact_id, artifact_kind, artifact_origin,
                     artifact_snapshot_name, org_id
                 )
                 VALUES ($1, $2, 'full', 'ready', clock_timestamp() + INTERVAL '1 hour',
                         $3, 'full', 'snapshot', 'named', $4)"
            ))
            .bind(Uuid::now_v7())
            .bind(sandbox_id)
            .bind(Uuid::now_v7())
            .bind(SEED_ORG)
            .execute(pool)
            .await?;
            if pinned_ahead {
                sqlx::query(sql!(
                    "UPDATE snapshots
                        SET \"timestamp\" = clock_timestamp() + INTERVAL '1 hour'
                      WHERE id = $1"
                ))
                .bind(pinned_id)
                .execute(pool)
                .await?;
            }
            let outcome = try_claim(pool, kind, sandbox_id).await;
            if lands {
                assert!(
                    outcome.unwrap_or_else(|error| panic!("{kind:?}: refused: {error:?}")),
                    "{kind:?}: the claim lands"
                );
            } else {
                assert!(
                    matches!(outcome, Err(RestoreClaimError::CaptureAheadOfClock { .. })),
                    "{kind:?}: the pinned backup ahead of the clock refuses the claim"
                );
            }
        }
    }
    Ok(())
}

/// A capture stamped ahead of the database clock, as one is after the clock
/// steps backward, has no cutoff that both covers it and stays at the clock.
/// A fresh claim is refused as retryable and leaves the row as it was, lease
/// included, so no restore takes the older generation. Once the clock passes
/// the capture, the claim lands and its retry restores that capture.
#[tokio::test(flavor = "current_thread")]
async fn fresh_restore_claims_are_refused_while_a_capture_is_stamped_ahead_of_the_clock()
-> TestResult {
    let Some(db) = try_pg_test_db_with_max_connections(4).await? else {
        return Ok(());
    };
    let pool = &db.pool;

    for kind in [ClaimKind::User, ClaimKind::Evacuation, ClaimKind::Direct] {
        let sandbox_id = insert_claimable_sandbox(pool, kind).await?;
        let older_uri = format!("s3://test/older-{sandbox_id}?region=us-east-1");
        let newest_uri = format!("s3://test/newest-{sandbox_id}?region=us-east-1");
        let (older_id, _) = insert_backup_at_clock(pool, sandbox_id, 4).await?;
        attach_storage_location(pool, older_id, &older_uri).await?;
        let (newest_id, _) = insert_backup_at_clock(pool, sandbox_id, 5).await?;
        attach_storage_location(pool, newest_id, &newest_uri).await?;
        sqlx::query(sql!(
            "UPDATE snapshots
                SET \"timestamp\" = clock_timestamp() + INTERVAL '1 hour'
              WHERE id = $1"
        ))
        .bind(newest_id)
        .execute(pool)
        .await?;
        let row_state = sql!(
            "SELECT lifecycle, lifecycle_changed_at, restore_lease_token,
                    restore_lease_expires_at, evacuation_restore_target
               FROM sandboxes
              WHERE id = $1"
        );
        let before: RowState = sqlx::query_as(row_state)
            .bind(sandbox_id)
            .fetch_one(pool)
            .await?;

        // The refused cutoff statement writes nothing itself, whatever its
        // caller then does with the transaction.
        let mut tx = pool.begin().await?;
        let refused = advance_fresh_restore_claim_cutoff(&mut *tx, sandbox_id)
            .await
            .expect_err("a cutoff must not land below a registered capture");
        assert!(
            matches!(&refused, RestoreClaimError::CaptureAheadOfClock { .. }),
            "{kind:?}: {refused:?}"
        );
        let in_transaction: RowState = sqlx::query_as(row_state)
            .bind(sandbox_id)
            .fetch_one(&mut *tx)
            .await?;
        assert_eq!(in_transaction, before, "{kind:?}: the refused statement");
        tx.rollback().await?;

        let refused = try_claim(pool, kind, sandbox_id)
            .await
            .expect_err("a claim must not land below a registered capture");
        assert!(
            matches!(
                &refused,
                RestoreClaimError::CaptureAheadOfClock { sandbox_id: refused_id }
                    if *refused_id == sandbox_id
            ),
            "{kind:?}: {refused:?}"
        );
        let after: RowState = sqlx::query_as(row_state)
            .bind(sandbox_id)
            .fetch_one(pool)
            .await?;
        assert_eq!(after, before, "{kind:?}: a refused claim writes nothing");

        // The clock passes the capture.
        let newest_ts: time::OffsetDateTime = sqlx::query_scalar(sql!(
            "UPDATE snapshots
                SET \"timestamp\" = clock_timestamp()
              WHERE id = $1
          RETURNING \"timestamp\""
        ))
        .bind(newest_id)
        .fetch_one(pool)
        .await?;
        run_claim(pool, kind, sandbox_id).await?;
        let claim_cutoff: time::OffsetDateTime = sqlx::query_scalar(sql!(
            "SELECT lifecycle_changed_at FROM sandboxes WHERE id = $1"
        ))
        .bind(sandbox_id)
        .fetch_one(pool)
        .await?;
        assert!(claim_cutoff > newest_ts, "{kind:?}");
        let selected = select_restore_source(
            pool,
            sandbox_id,
            RestoreSourceMode::Retry,
            RestoreSourcePlacement::Any,
        )
        .await?;
        assert!(
            matches!(
                &selected,
                RestoreSourceSelection::Selected(source)
                    if source.storage_connection_string == newest_uri
            ),
            "{kind:?}: {selected:?}"
        );
    }
    Ok(())
}

/// A released stopped evacuation is re-placed from the ready disk artifact its
/// row pins, and that read uses no cutoff. So a snapshot stamped ahead of the
/// database clock, here a named one that has nothing to do with the evacuation,
/// doesn't refuse the claim the way it refuses a claim from `suspended`: the
/// claim lands, the pin stays, and the restore source is the pinned disk.
#[tokio::test(flavor = "current_thread")]
async fn a_stopped_evacuation_claim_is_not_refused_by_a_snapshot_ahead_of_the_clock() -> TestResult
{
    let Some(db) = try_pg_test_db_with_max_connections(4).await? else {
        return Ok(());
    };
    let pool = &db.pool;

    let sandbox_id = SandboxId::new();
    let pinned_artifact = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, managed_by, lifecycle, template_id,
             network_policy_generation,
             current_local_artifact_id, current_local_artifact_kind,
             current_local_artifact_origin
         )
         VALUES (
             $1, $2, $3, 'k8s', 'stopped', 'tmpl-restore-lease-test',
             1, $4, 'disk', 'stop'
         )"
    ))
    .bind(sandbox_id)
    .bind(SEED_USER)
    .bind(SEED_ORG)
    .bind(pinned_artifact)
    .execute(pool)
    .await?;
    // The drain writes the marker while the row stays `stopped`.
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET evacuation_restore_target = 'stopped'
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(pool)
    .await?;

    let disk_uri = format!("s3://test/disk-{sandbox_id}?region=us-east-1");
    let disk_id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO snapshots (
             id, sandbox_id, type, state, \"timestamp\",
             local_artifact_id, artifact_kind, artifact_origin, org_id
         )
         VALUES ($1, $2, 'disk', 'ready', clock_timestamp(), $3, 'disk', 'stop', $4)"
    ))
    .bind(disk_id)
    .bind(sandbox_id)
    .bind(pinned_artifact)
    .bind(SEED_ORG)
    .execute(pool)
    .await?;
    attach_storage_location(pool, disk_id, &disk_uri).await?;

    let named_id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO snapshots (
             id, sandbox_id, type, state, \"timestamp\",
             local_artifact_id, artifact_kind, artifact_origin,
             artifact_snapshot_name, org_id
         )
         VALUES (
             $1, $2, 'full', 'ready', clock_timestamp() + INTERVAL '1 hour',
             $3, 'full', 'snapshot', 'named', $4
         )"
    ))
    .bind(named_id)
    .bind(sandbox_id)
    .bind(Uuid::now_v7())
    .bind(SEED_ORG)
    .execute(pool)
    .await?;
    attach_storage_location(
        pool,
        named_id,
        &format!("s3://test/named-{sandbox_id}?region=us-east-1"),
    )
    .await?;

    // The same rows refuse the cutoff a claim from `suspended` advances, so
    // the claim below lands because it's a stopped one, not because nothing
    // is ahead of the clock.
    let mut tx = pool.begin().await?;
    let refused = advance_fresh_restore_claim_cutoff(&mut *tx, sandbox_id)
        .await
        .expect_err("the named snapshot is ahead of the clock");
    assert!(
        matches!(&refused, RestoreClaimError::CaptureAheadOfClock { .. }),
        "{refused:?}"
    );
    tx.rollback().await?;

    let claimed =
        claim_evacuation_restore(pool, sandbox_id, 60, 60, Some(SandboxLifecycle::Stopped))
            .await?
            .expect("a released stopped evacuation is claimable");
    assert_eq!(claimed.lifecycle, "stopped");
    assert_eq!(claimed.restore_lease_token, None);

    let (lifecycle, marker, pin, pin_kind): (String, Option<String>, Option<Uuid>, Option<String>) =
        sqlx::query_as(sql!(
            "SELECT lifecycle, evacuation_restore_target,
                    current_local_artifact_id, current_local_artifact_kind
               FROM sandboxes
              WHERE id = $1"
        ))
        .bind(sandbox_id)
        .fetch_one(pool)
        .await?;
    assert_eq!(lifecycle, "restoring");
    assert_eq!(marker.as_deref(), Some("stopped"));
    assert_eq!(pin, Some(pinned_artifact), "the claim leaves the pin alone");
    assert_eq!(pin_kind.as_deref(), Some("disk"));

    let source = stopped_evacuation_restore_source(pool, sandbox_id)
        .await?
        .expect("the claimed row still resolves its pinned disk artifact");
    assert_eq!(source.storage_connection_string, disk_uri);
    assert_eq!(source.current_local_artifact_id.as_uuid(), pinned_artifact);
    Ok(())
}
