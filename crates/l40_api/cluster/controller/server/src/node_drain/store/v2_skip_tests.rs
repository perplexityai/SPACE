//! Every v1 writer keyed on drain phase skips a v2 drain's node, and a node
//! under an engine this build doesn't know: the node drain's draining,
//! drained and terminating lists and its phase write, the drained-node
//! terminator's list, its reap and its orphan delete, the terminating delete
//! with its tombstone, the forced drain's suspend claim, the record of a
//! stopped evacuation's artifact, and the three sandbox give-ups keyed on
//! the node's phase: the stale-resident sweep, the gone node's bulk fail and
//! the drained gone node's mark-lost. Each writer runs against a v1 row and
//! a row under each other engine in the same phase: the v1 row is the
//! control the writer still takes.
//!
//! Two more writers carry the same `drain_engine IS NULL` guard and aren't
//! driven here: the stopped evacuation's claim
//! (`pplx_space_sandbox_store::drain::claim_stopped_evacuation`), whose
//! claim also needs the sandbox's current disk backed up, and the pod-less
//! give-up's row check (`pplx_space_sandbox_store::pod_less_giveup::read_guards`),
//! a read whose `row_matches` carries the guard.

use pplx_space_accounts::NodeId;
use pplx_space_db_test_fixtures::try_pg_test_db;
use pplx_space_node_model::NodeDrainPhase;
use pplx_space_node_registration_store::drain_reads;
use pplx_space_sandbox_residency::ResidencyEvidence;
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;

use super::{
    DRAIN_RECONCILER_SUBSTRATE, GuardScope, delete_terminating_node_and_record_tombstone,
    set_drain_phase_query,
};
use crate::drained_node_terminator::DrainedNodeTerminatorSink as _;
use crate::drained_node_terminator::store::NodeDbStore;
use pplx_space_node_registration_store::drained_node;

const UID: &str = "uid-1";
/// A `nodes.drain_engine` value this build doesn't know.
const UNKNOWN_ENGINE: &str = "v9";

/// A sandbox in `lifecycle` active on `node_id`, with `marker` as its
/// `evacuation_restore_target`.
async fn sandbox_on(
    pool: &PgPool,
    node_id: NodeId,
    lifecycle: &str,
    marker: Option<&str>,
) -> TestResult<pplx_space_sandbox_model::SandboxId> {
    let id = pplx_space_sandbox_model::SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, managed_by, active_node_id,
             evacuation_restore_target, network_policy_generation
         ) VALUES ($1, '00000000-0000-0000-0000-000000000010',
                   '00000000-0000-0000-0000-000000000001', $2, 'on_node', 'k8s', $3, $4, 1)"
    ))
    .bind(id)
    .bind(lifecycle)
    .bind(node_id)
    .bind(marker)
    .execute(pool)
    .await?;
    Ok(id)
}

/// A Kubernetes node in `phase` under `engine` (`nodes.drain_engine`, NULL
/// for v1), with its `node_k8s` row.
async fn node(pool: &PgPool, phase: &str, engine: Option<&str>) -> TestResult<NodeId> {
    let node_id = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state, drain_phase, drain_mode,
                            drain_started_at, drain_engine, drain_status)
         VALUES ($1, '10.0.0.60', 'worker', 'k8s', 'registered', $2, 'forced', NOW(), $3,
                 CASE WHEN $3::TEXT IS NULL THEN NULL ELSE '{\"generation\": 1, \"deadline_at\": 0}'::JSONB END)"
    ))
    .bind(node_id)
    .bind(phase)
    .bind(engine)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name, k8s_node_uid) VALUES ($1, $2, $3)"
    ))
    .bind(node_id)
    .bind(format!("node-{node_id}"))
    .bind(UID)
    .execute(pool)
    .await?;
    Ok(node_id)
}

async fn phase(pool: &PgPool, node_id: NodeId) -> TestResult<Option<String>> {
    Ok(
        sqlx::query_scalar(sql!("SELECT drain_phase FROM nodes WHERE id = $1"))
            .bind(node_id)
            .fetch_optional(pool)
            .await?
            .flatten(),
    )
}

/// A v1 writer keyed on drain phase.
#[derive(Clone, Copy, Debug)]
enum Writer {
    DrainingList,
    DrainingIds,
    PhaseWrite,
    SuspendClaim,
    StoppedArtifact,
    TerminatingList,
    DrainedIds,
    DrainedList,
    Reap,
    OrphanDelete,
    TerminatingDelete,
    AbandonStale,
    GoneNodeFail,
    UnrecoverableLost,
}

impl Writer {
    const ALL: [Self; 14] = [
        Self::DrainingList,
        Self::DrainingIds,
        Self::PhaseWrite,
        Self::SuspendClaim,
        Self::StoppedArtifact,
        Self::TerminatingList,
        Self::DrainedIds,
        Self::DrainedList,
        Self::Reap,
        Self::OrphanDelete,
        Self::TerminatingDelete,
        Self::AbandonStale,
        Self::GoneNodeFail,
        Self::UnrecoverableLost,
    ];

    /// The phase the writer reads its node in.
    fn phase(self) -> &'static str {
        match self {
            Self::DrainingList
            | Self::DrainingIds
            | Self::PhaseWrite
            | Self::SuspendClaim
            | Self::StoppedArtifact
            | Self::AbandonStale
            | Self::GoneNodeFail => "draining",
            Self::TerminatingList | Self::TerminatingDelete => "terminating",
            Self::DrainedIds
            | Self::DrainedList
            | Self::Reap
            | Self::OrphanDelete
            | Self::UnrecoverableLost => "drained",
        }
    }

    /// Run the writer over the node; whether it took it.
    async fn takes(self, pool: &PgPool, node_id: NodeId) -> TestResult<bool> {
        Ok(match self {
            Self::DrainingList => drain_reads::nodes_in_drain_phase_with_mode(
                pool,
                NodeDrainPhase::Draining,
                DRAIN_RECONCILER_SUBSTRATE,
            )
            .await?
            .iter()
            .any(|row| row.id == node_id),
            Self::DrainingIds | Self::DrainedIds => drain_reads::nodes_in_drain_phase(
                pool,
                if matches!(self, Self::DrainingIds) {
                    NodeDrainPhase::Draining
                } else {
                    NodeDrainPhase::Drained
                },
                DRAIN_RECONCILER_SUBSTRATE,
            )
            .await?
            .contains(&node_id),
            Self::SuspendClaim => {
                let id = sandbox_on(pool, node_id, "started", None).await?;
                pplx_space_sandbox_store::drain::claim_evacuation_suspend(
                    pool,
                    id,
                    node_id,
                    pplx_space_sandbox_model::SandboxLifecycle::Started,
                )
                .await?
            }
            Self::StoppedArtifact => {
                let id = sandbox_on(pool, node_id, "stopped", Some("stopped")).await?;
                pplx_space_sandbox_store::drain::record_stopped_evacuation_artifact(
                    pool,
                    id,
                    node_id,
                    pplx_space_snapshot_model::LocalArtifactId::from_uuid(uuid::Uuid::now_v7()),
                )
                .await?
            }
            Self::PhaseWrite => {
                sqlx::query(sqlx::AssertSqlSafe(set_drain_phase_query(
                    ResidencyEvidence::LifecycleOnly,
                    GuardScope::None,
                )))
                .bind(node_id)
                .bind("drained")
                .bind(None::<String>)
                .bind("draining")
                .execute(pool)
                .await?;
                phase(pool, node_id).await?.as_deref() == Some("drained")
            }
            Self::TerminatingList => drain_reads::nodes_in_drain_phase(
                pool,
                NodeDrainPhase::Terminating,
                DRAIN_RECONCILER_SUBSTRATE,
            )
            .await?
            .contains(&node_id),
            Self::DrainedList => NodeDbStore::new(pool.clone(), ResidencyEvidence::default())
                .list_drained_nodes()
                .await?
                .contains(&node_id),
            Self::Reap => {
                drained_node::mark_terminating(pool, node_id, ResidencyEvidence::default()).await?;
                phase(pool, node_id).await?.as_deref() == Some("terminating")
            }
            Self::OrphanDelete => {
                drained_node::delete_if_orphan(
                    pool,
                    node_id,
                    Some(UID),
                    ResidencyEvidence::default(),
                )
                .await?;
                phase(pool, node_id).await?.is_none()
            }
            Self::TerminatingDelete => {
                let deleted = delete_terminating_node_and_record_tombstone(pool, node_id).await?;
                let tombstoned: bool = sqlx::query_scalar(sql!(
                    "SELECT EXISTS (SELECT 1 FROM node_k8s_tombstones WHERE node_id = $1)"
                ))
                .bind(node_id)
                .fetch_one(pool)
                .await?;
                deleted || tombstoned || phase(pool, node_id).await?.is_none()
            }
            Self::AbandonStale => {
                let id = sandbox_on(pool, node_id, "starting", None).await?;
                sqlx::query(sql!(
                    "UPDATE sandboxes SET lifecycle_changed_at = NOW() - INTERVAL '1 hour'
                      WHERE id = $1"
                ))
                .bind(id)
                .execute(pool)
                .await?;
                !pplx_space_sandbox_store::drain::abandon_stale_resident_sandboxes(
                    pool,
                    node_id,
                    pplx_space_durations::PositiveDuration::new(std::time::Duration::from_secs(60))
                        .expect("non-zero"),
                    &["starting"],
                )
                .await?
                .is_empty()
            }
            Self::GoneNodeFail => {
                sandbox_on(pool, node_id, "started", None).await?;
                !pplx_space_sandbox_store::drain::fail_resident_sandboxes_on_gone_node(
                    pool,
                    node_id,
                    &["started"],
                    Some(UID),
                )
                .await?
                .is_empty()
            }
            Self::UnrecoverableLost => {
                sqlx::query(sql!(
                    "INSERT INTO sandboxes (
                         id, user_id, org_id, lifecycle, placement, managed_by,
                         target_node_id, network_policy_generation
                     ) VALUES ($1, '00000000-0000-0000-0000-000000000010',
                               '00000000-0000-0000-0000-000000000001', 'suspended',
                               'unplaced', 'k8s', $2, 1)"
                ))
                .bind(pplx_space_sandbox_model::SandboxId::new())
                .bind(node_id)
                .execute(pool)
                .await?;
                !NodeDbStore::new(pool.clone(), ResidencyEvidence::default())
                    .mark_unrecoverable_sandboxes_lost(node_id, Some(UID.to_owned()))
                    .await?
                    .is_empty()
            }
        })
    }
}

/// Each v1 writer takes the v1 node and leaves the node under any other
/// engine in the same phase: a v2 node's row stays, with no tombstone and no
/// sandbox written off, for the v2 loop, and so does a row under an engine
/// this build doesn't know.
#[tokio::test(flavor = "current_thread")]
async fn every_v1_writer_skips_a_node_under_another_engine() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    for writer in Writer::ALL {
        for engine in ["v2", UNKNOWN_ENGINE] {
            let other = node(&db.pool, writer.phase(), Some(engine)).await?;
            assert!(
                !writer.takes(&db.pool, other).await?,
                "{writer:?} took the {engine} node"
            );
        }
        let v1 = node(&db.pool, writer.phase(), None).await?;
        assert!(
            writer.takes(&db.pool, v1).await?,
            "{writer:?} left the v1 control"
        );
    }
    Ok(())
}
