//! The drain restore's claim, commit and gate read ([`crate::drain_restore`]).
//!
//! A refused claim must leave the row exactly as it was: each refusal compares
//! the whole row before and after, beside a control row that the same claim
//! takes.

use std::time::Duration;

use pplx_space_db_test_fixtures::try_pg_test_db_with_max_connections;

use super::*;
use crate::delete::mark_deleting;
use crate::drain_restore::{
    DrainRestoreGateRow, DrainRestoreTarget, claim_drain_restore, complete_drain_restore,
    drain_restore_episode_open, read_drain_restore_gate,
};
use crate::restore_lease::RestoreClaimRow;

const LEASE_TTL_SECS: i64 = 60;
const EXECUTION_OWNERSHIP_TTL_SECS: i64 = 300;

/// When S, the drain's suspend snapshot, was taken in these fixtures.
const S_TAKEN: &str = "2026-01-01T00:00:00Z";
const NEWER: &str = "2026-01-02T00:00:00Z";
const OLDER: &str = "2025-12-01T00:00:00Z";

/// A full snapshot of `sandbox` in `state`, taken at `taken`, carrying
/// `artifact` when given, stored at a location that is orphaned when asked.
async fn snapshot(
    pool: &PgPool,
    sandbox: SandboxId,
    artifact: Option<Uuid>,
    state: &str,
    taken: &str,
    orphaned: bool,
) -> TestResult<Uuid> {
    let id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO snapshots (
             id, sandbox_id, type, state, timestamp, local_artifact_id, artifact_kind,
             artifact_origin, org_id
         )
         VALUES ($1, $2, 'full', $3, $4::TIMESTAMPTZ, $5,
                 CASE WHEN $5::UUID IS NULL THEN NULL ELSE 'full' END,
                 CASE WHEN $5::UUID IS NULL THEN NULL ELSE 'suspend' END, $6)"
    ))
    .bind(id)
    .bind(sandbox)
    .bind(state)
    .bind(taken)
    .bind(artifact)
    .bind(SEED_ORG)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations (
             id, snapshot_id, storage_connection_string, org_id, orphaned_at
         ) VALUES (
             $1, $2, 's3://test-bucket/snapshots/drained?region=us-east-1', $3,
             CASE WHEN $4 THEN NOW() END
         )"
    ))
    .bind(Uuid::now_v7())
    .bind(id)
    .bind(SEED_ORG)
    .bind(orphaned)
    .execute(pool)
    .await?;
    Ok(id)
}

/// Point the row's current local artifact at `artifact`, as the suspend that
/// published it does.
async fn mark_artifact(pool: &PgPool, sandbox: SandboxId, artifact: Uuid) -> TestResult {
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET current_local_artifact_id = $2,
                current_local_artifact_kind = 'full',
                current_local_artifact_origin = 'suspend'
          WHERE id = $1"
    ))
    .bind(sandbox)
    .bind(artifact)
    .execute(pool)
    .await?;
    Ok(())
}

/// Whether the row names its suspend artifact, as a current suspend does, or
/// is a legacy row that names none.
#[derive(Clone, Copy, Debug)]
enum Artifact {
    Marked,
    Legacy,
}

/// A drained sandbox: `lifecycle`, with S, a ready and stored full snapshot,
/// which the row's artifact marker names when `artifact` is `Marked`.
async fn drained(
    pool: &PgPool,
    lifecycle: &str,
    artifact: Artifact,
) -> TestResult<(SandboxId, Uuid, Uuid)> {
    let id = SandboxId::from_uuid(insert_sandbox(pool, None, lifecycle).await?);
    let s_artifact = Uuid::now_v7();
    let s = snapshot(pool, id, Some(s_artifact), "ready", S_TAKEN, false).await?;
    if matches!(artifact, Artifact::Marked) {
        mark_artifact(pool, id, s_artifact).await?;
    }
    Ok((id, s, s_artifact))
}

async fn claim(
    executor: impl sqlx::PgExecutor<'_>,
    id: SandboxId,
    snapshot: Uuid,
    target: DrainRestoreTarget,
    token: Uuid,
) -> TestResult<Option<RestoreClaimRow>> {
    Ok(claim_drain_restore(
        executor,
        id,
        snapshot,
        target,
        LEASE_TTL_SECS,
        token,
        EXECUTION_OWNERSHIP_TTL_SECS,
    )
    .await?)
}

/// The whole row as JSON text: equal before and after a statement iff it
/// wrote nothing.
async fn row_image(pool: &PgPool, id: SandboxId) -> TestResult<String> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT row_to_json(s)::TEXT FROM sandboxes s WHERE s.id = $1"
    ))
    .bind(id)
    .fetch_one(pool)
    .await?)
}

/// The episode columns: lifecycle, episode target, lease token, and whether
/// the lease is live.
async fn episode(
    pool: &PgPool,
    id: SandboxId,
) -> TestResult<(String, Option<String>, Option<Uuid>, bool)> {
    Ok(sqlx::query_as(sql!(
        "SELECT lifecycle, restore_episode_target, restore_lease_token,
                COALESCE(restore_lease_expires_at > NOW(), FALSE)
           FROM sandboxes
          WHERE id = $1"
    ))
    .bind(id)
    .fetch_one(pool)
    .await?)
}

async fn gate(pool: &PgPool, id: SandboxId, snapshot: Uuid) -> TestResult<DrainRestoreGateRow> {
    Ok(read_drain_restore_gate(pool, id, snapshot)
        .await?
        .expect("the row exists"))
}

/// A `suspended` row whose restore selector loads S is claimed toward either
/// target, with the episode target and a live lease under the claim's token,
/// whether or not the row names its artifact.
#[tokio::test(flavor = "current_thread")]
async fn a_suspended_row_whose_selector_loads_s_is_claimed() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    for artifact in [Artifact::Marked, Artifact::Legacy] {
        for target in DrainRestoreTarget::ALL {
            let (id, s, _) = drained(pool, "suspended", artifact).await?;
            // An older ready snapshot the selector passes over.
            snapshot(pool, id, None, "ready", OLDER, false).await?;
            let token = Uuid::now_v7();
            let claimed = claim(pool, id, s, target, token)
                .await?
                .unwrap_or_else(|| panic!("{artifact:?} {target}: claim refused"));
            assert_eq!(claimed.lifecycle, "suspended", "{artifact:?} {target}");
            assert_eq!(claimed.restore_lease_token, Some(token));
            assert_eq!(
                episode(pool, id).await?,
                (
                    "restoring".to_owned(),
                    Some(target.as_str().to_owned()),
                    Some(token),
                    true
                ),
                "{artifact:?} {target}"
            );
        }
    }
    Ok(())
}

/// A row in any lifecycle but `suspended`, with no episode target, is
/// refused and left unchanged: `restoring` included, which only the re-drive
/// arm takes, and only with the target set.
#[tokio::test(flavor = "current_thread")]
async fn every_other_lifecycle_is_refused_unchanged() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    for lifecycle in SandboxLifecycle::all() {
        let (id, s, _) = drained(pool, lifecycle.as_str(), Artifact::Marked).await?;
        let before = row_image(pool, id).await?;
        let claimed = claim(pool, id, s, DrainRestoreTarget::Started, Uuid::now_v7()).await?;
        if *lifecycle == SandboxLifecycle::Suspended {
            assert!(claimed.is_some(), "the suspended control row is claimed");
        } else {
            assert!(claimed.is_none(), "{lifecycle}: claimed");
            assert_eq!(
                row_image(pool, id).await?,
                before,
                "{lifecycle}: row changed"
            );
        }
    }
    Ok(())
}

/// One way to make an otherwise claimable `suspended` row refuse the claim.
#[derive(Clone, Copy, Debug)]
enum Refusal {
    /// No refusal: the control row.
    None,
    /// The v1 drain's marker is set.
    EvacuationMarker,
    /// A previous episode left its target on the row.
    EpisodeTarget,
    /// A full snapshot newer than S, in `state`, on a legacy row.
    NewerLegacy(&'static str),
    /// A later suspend's artifact is the row's current one.
    NewerArtifact,
    /// S is in `state`.
    SourceState(&'static str),
    /// S's only location is orphaned.
    SourceOrphaned,
    /// The operation names an older ready snapshot, not S.
    OlderNamed,
    /// The operation names another sandbox's snapshot.
    ForeignNamed,
    /// An active execution did work just now.
    FreshExecution,
}

impl Refusal {
    const ALL: [Self; 12] = [
        Self::None,
        Self::EvacuationMarker,
        Self::EpisodeTarget,
        Self::NewerLegacy("creating"),
        Self::NewerLegacy("failed"),
        Self::NewerArtifact,
        Self::SourceState("creating"),
        Self::SourceState("failed"),
        Self::SourceOrphaned,
        Self::OlderNamed,
        Self::ForeignNamed,
        Self::FreshExecution,
    ];

    /// Whether the restore selector still loads S once applied. The gate
    /// read reports the same choice the claim makes.
    const fn selector_loads_s(self) -> bool {
        matches!(
            self,
            Self::None | Self::EvacuationMarker | Self::EpisodeTarget | Self::FreshExecution
        )
    }

    /// A drained `suspended` row with this refusal applied, and the snapshot
    /// the operation names.
    async fn apply(self, pool: &PgPool) -> TestResult<(SandboxId, Uuid)> {
        let artifact = if matches!(self, Self::NewerLegacy(_)) {
            Artifact::Legacy
        } else {
            Artifact::Marked
        };
        let (id, s, s_artifact) = drained(pool, "suspended", artifact).await?;
        let mut named = s;
        match self {
            Self::None => {}
            Self::EvacuationMarker => {
                sqlx::query(sql!(
                    "UPDATE sandboxes SET evacuation_restore_target = 'started' WHERE id = $1"
                ))
                .bind(id)
                .execute(pool)
                .await?;
            }
            Self::EpisodeTarget => {
                sqlx::query(sql!(
                    "UPDATE sandboxes SET restore_episode_target = 'started' WHERE id = $1"
                ))
                .bind(id)
                .execute(pool)
                .await?;
            }
            Self::NewerLegacy(state) => {
                snapshot(pool, id, None, state, NEWER, false).await?;
            }
            Self::NewerArtifact => {
                let newer = Uuid::now_v7();
                snapshot(pool, id, Some(newer), "ready", NEWER, false).await?;
                mark_artifact(pool, id, newer).await?;
            }
            Self::SourceState(state) => {
                sqlx::query(sql!("UPDATE snapshots SET state = $2 WHERE id = $1"))
                    .bind(s)
                    .bind(state)
                    .execute(pool)
                    .await?;
            }
            Self::SourceOrphaned => {
                sqlx::query(sql!(
                    "UPDATE snapshot_storage_locations SET orphaned_at = NOW()
                      WHERE snapshot_id = $1"
                ))
                .bind(s)
                .execute(pool)
                .await?;
            }
            Self::OlderNamed => {
                named = snapshot(pool, id, Some(s_artifact), "ready", OLDER, false).await?;
            }
            Self::ForeignNamed => {
                let (_, foreign, _) = drained(pool, "suspended", Artifact::Marked).await?;
                named = foreign;
            }
            Self::FreshExecution => {
                sqlx::query(sql!(
                    "INSERT INTO sandbox_k8s_executions
                         (sandbox_id, generation, pod_name, pod_namespace, state)
                     VALUES ($1, 1, $2, 'space', 'running')"
                ))
                .bind(id)
                .bind(format!("pod-{id}"))
                .execute(pool)
                .await?;
            }
        }
        Ok((id, named))
    }
}

/// Each refusal leaves the row unchanged, and the gate read selects S
/// exactly when the claim's own selector does. The control row is claimed.
#[tokio::test(flavor = "current_thread")]
async fn each_refusal_leaves_the_row_unchanged() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    for refusal in Refusal::ALL {
        let (id, named) = refusal.apply(pool).await?;
        let s_selected = gate(pool, id, named).await?.selected_snapshot == Some(named);
        assert_eq!(s_selected, refusal.selector_loads_s(), "{refusal:?}: gate");
        let before = row_image(pool, id).await?;
        let claimed = claim(pool, id, named, DrainRestoreTarget::Paused, Uuid::now_v7()).await?;
        if matches!(refusal, Refusal::None) {
            assert!(claimed.is_some(), "the control row is claimed");
        } else {
            assert!(claimed.is_none(), "{refusal:?}: claimed");
            assert_eq!(
                row_image(pool, id).await?,
                before,
                "{refusal:?}: row changed"
            );
        }
    }
    Ok(())
}

/// Two claims of one row at once open one episode. The second waits on the
/// first's row lock, then finds its lease live.
#[tokio::test(flavor = "current_thread")]
async fn concurrent_claims_open_exactly_one_episode() -> TestResult {
    // The open claim, the waiting claim and the lock probe each hold a
    // connection.
    let Some(db) = try_pg_test_db_with_max_connections(3).await? else {
        return Ok(());
    };
    let pool = db.pool.clone();
    let (id, s, _) = drained(&pool, "suspended", Artifact::Marked).await?;
    let first_token = Uuid::now_v7();
    let mut first = pool.begin().await?;
    assert!(
        claim(&mut *first, id, s, DrainRestoreTarget::Started, first_token)
            .await?
            .is_some()
    );
    let second = tokio::spawn({
        let pool = pool.clone();
        async move { claim(&pool, id, s, DrainRestoreTarget::Started, Uuid::now_v7()).await }
    });
    wait_for_lock_waiter(&pool).await?;
    first.commit().await?;
    assert!(second.await??.is_none(), "the second claim took the row");
    assert_eq!(episode(&pool, id).await?.2, Some(first_token));
    Ok(())
}

/// Wait until some session of this database waits on a lock.
async fn wait_for_lock_waiter(pool: &PgPool) -> TestResult {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        let waiting: bool = sqlx::query_scalar(sql!(
            "SELECT EXISTS (
                 SELECT 1 FROM pg_stat_activity
                  WHERE datname = current_database() AND wait_event_type = 'Lock'
             )"
        ))
        .fetch_one(pool)
        .await?;
        if waiting {
            return Ok(());
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the second claim never waited on the row lock"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// The re-drive arm takes back a `restoring` row in its own target's episode
/// once the lease has lapsed, under a new token and without moving the
/// lifecycle clock. A live lease, the other target, or no target refuses it
/// unchanged.
#[tokio::test(flavor = "current_thread")]
async fn the_redrive_arm_takes_back_its_episode_after_the_lease_lapses() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    for (episode_target, lease_live, claimable) in [
        (Some("started"), false, true),
        (Some("started"), true, false),
        (Some("paused"), false, false),
        (None, false, false),
    ] {
        let case = format!("target {episode_target:?}, lease live {lease_live}");
        let (id, s, _) = drained(pool, "restoring", Artifact::Marked).await?;
        sqlx::query(sql!(
            "UPDATE sandboxes
                SET restore_episode_target = $2,
                    restore_lease_token = $3,
                    restore_lease_expires_at = NOW() + CASE WHEN $4 THEN INTERVAL '1 minute'
                                                            ELSE INTERVAL '-1 minute' END,
                    lifecycle_changed_at = NOW() - INTERVAL '1 hour'
              WHERE id = $1"
        ))
        .bind(id)
        .bind(episode_target)
        .bind(Uuid::now_v7())
        .bind(lease_live)
        .execute(pool)
        .await?;
        let before = row_image(pool, id).await?;
        let clock = || async {
            sqlx::query_scalar::<_, time::OffsetDateTime>(sql!(
                "SELECT lifecycle_changed_at FROM sandboxes WHERE id = $1"
            ))
            .bind(id)
            .fetch_one(pool)
            .await
        };
        let clock_before = clock().await?;
        let token = Uuid::now_v7();
        let claimed = claim(pool, id, s, DrainRestoreTarget::Started, token).await?;
        if claimable {
            assert_eq!(claimed.map(|row| row.lifecycle), Some("restoring".into()));
            assert_eq!(
                episode(pool, id).await?,
                (
                    "restoring".into(),
                    Some("started".into()),
                    Some(token),
                    true
                ),
                "{case}"
            );
            assert_eq!(clock().await?, clock_before, "{case}: clock moved");
        } else {
            assert!(claimed.is_none(), "{case}: claimed");
            assert_eq!(row_image(pool, id).await?, before, "{case}: row changed");
        }
    }
    Ok(())
}

/// The commit lands the claim's target only under the claim's token and
/// target, on its node, and ends the episode: the target and the lease clear.
#[tokio::test(flavor = "current_thread")]
async fn the_commit_lands_its_own_target_and_ends_the_episode() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    for target in DrainRestoreTarget::ALL {
        let (id, s, _) = drained(pool, "suspended", Artifact::Marked).await?;
        let token = Uuid::now_v7();
        claim(pool, id, s, target, token).await?.expect("claimed");
        let other_target = DrainRestoreTarget::ALL
            .into_iter()
            .find(|other| *other != target)
            .expect("two targets");
        let before = row_image(pool, id).await?;
        assert!(!complete_drain_restore(pool, id, Uuid::now_v7(), target).await?);
        assert!(!complete_drain_restore(pool, id, token, other_target).await?);
        assert_eq!(row_image(pool, id).await?, before, "{target}: a miss wrote");

        assert!(complete_drain_restore(pool, id, token, target).await?);
        let landed: (String, String, Option<String>, Option<Uuid>, bool) = sqlx::query_as(sql!(
            "SELECT lifecycle, placement, restore_episode_target, restore_lease_token,
                        restore_lease_expires_at IS NULL
                   FROM sandboxes
                  WHERE id = $1"
        ))
        .bind(id)
        .fetch_one(pool)
        .await?;
        assert_eq!(
            landed,
            (
                target.as_str().to_owned(),
                "on_node".to_owned(),
                None,
                None,
                true
            ),
            "{target}"
        );
        // A second commit finds the episode over.
        assert!(!complete_drain_restore(pool, id, token, target).await?);
    }
    Ok(())
}

/// The gate read reports the named snapshot's state, whether it is stored,
/// whether anything is newer, and whether it is the row's current artifact.
#[tokio::test(flavor = "current_thread")]
async fn the_gate_reads_the_named_snapshot() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let (id, s, _) = drained(pool, "suspended", Artifact::Marked).await?;
    let read = gate(pool, id, s).await?;
    assert_eq!(
        read,
        DrainRestoreGateRow {
            lifecycle: "suspended".into(),
            evacuation_marked: false,
            restore_episode_target: None,
            selected_snapshot: Some(s),
            source_state: Some("ready".into()),
            source_located: true,
            newer_full_snapshot: false,
            source_is_current_artifact: true,
        }
    );

    // A later suspend moves the artifact past S, and S's location goes.
    let newer = Uuid::now_v7();
    let later = snapshot(pool, id, Some(newer), "creating", NEWER, false).await?;
    mark_artifact(pool, id, newer).await?;
    sqlx::query(sql!(
        "UPDATE snapshot_storage_locations SET orphaned_at = NOW() WHERE snapshot_id = $1"
    ))
    .bind(s)
    .execute(pool)
    .await?;
    let read = gate(pool, id, s).await?;
    assert_eq!(
        (
            read.selected_snapshot,
            read.source_located,
            read.newer_full_snapshot,
            read.source_is_current_artifact
        ),
        (None, false, true, false)
    );
    assert_eq!(
        gate(pool, id, later).await?.source_state,
        Some("creating".into())
    );

    // A snapshot of another sandbox reads as none, and a missing row as no
    // row.
    let (_, foreign, _) = drained(pool, "suspended", Artifact::Marked).await?;
    assert_eq!(gate(pool, id, foreign).await?.source_state, None);
    assert!(
        read_drain_restore_gate(pool, SandboxId::new(), s)
            .await?
            .is_none()
    );
    Ok(())
}

/// A delete that lands while a drain restore's episode is open ends the
/// episode: the target goes with the `deleting` transition, the restore's
/// commit then finds nothing to land, and no claim reopens it.
#[tokio::test(flavor = "current_thread")]
async fn a_delete_during_the_episode_ends_it() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let (id, s, _) = drained(pool, "suspended", Artifact::Marked).await?;
    let token = Uuid::now_v7();
    claim(pool, id, s, DrainRestoreTarget::Started, token)
        .await?
        .expect("claimed");

    let mut tx = pool.begin().await?;
    mark_deleting(&mut tx, id).await?;
    tx.commit().await?;

    assert!(!complete_drain_restore(pool, id, token, DrainRestoreTarget::Started).await?);
    assert!(
        claim(pool, id, s, DrainRestoreTarget::Started, Uuid::now_v7())
            .await?
            .is_none()
    );
    let (lifecycle, target, _, _) = episode(pool, id).await?;
    assert_eq!((lifecycle.as_str(), target), ("deleting", None));
    assert!(!drain_restore_episode_open(pool, id).await?);
    Ok(())
}

/// An episode is open only on a `restoring` row with a `started` or
/// `paused` target: not a `stopped` one, not a user restore, not another
/// lifecycle, not a missing row.
#[tokio::test(flavor = "current_thread")]
async fn an_episode_is_open_only_while_restoring_toward_a_drain_target() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    for (lifecycle, target, open) in [
        ("restoring", Some("started"), true),
        ("restoring", Some("paused"), true),
        ("restoring", Some("stopped"), false),
        ("restoring", None, false),
        ("suspended", None, false),
    ] {
        let id = SandboxId::from_uuid(insert_sandbox(pool, None, lifecycle).await?);
        sqlx::query(sql!(
            "UPDATE sandboxes SET restore_episode_target = $2 WHERE id = $1"
        ))
        .bind(id)
        .bind(target)
        .execute(pool)
        .await?;
        assert_eq!(
            drain_restore_episode_open(pool, id).await?,
            open,
            "{lifecycle} {target:?}"
        );
    }
    assert!(!drain_restore_episode_open(pool, SandboxId::new()).await?);
    Ok(())
}
