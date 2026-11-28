//! Whether a sandbox still needs the node a v2 drain is draining, for an
//! evacuation the node reported: the one answer the controller records an
//! evacuation by, which lets `draining` become `drained`.
//!
//! `decide` is a pure function over the facts `read_facts` reads of the
//! row in one statement. It is default-deny: a row is released only for a
//! reason [`ReleaseReason`] names, and every other row holds the node,
//! including a row in a lifecycle this build doesn't know and an unplaced
//! row in a lifecycle that isn't settled. Durable means what the rest of the
//! drain means by it: a ready, stored backup of the artifact as a full
//! snapshot ([`artifact_has_durable_copy`]), and for a row that pins no
//! artifact, also the terminate's own source-release test
//! ([`snapshot_durable_for_source_release`]), so the verdict and the
//! terminate's release agree on whether such a row's state is durable. The
//! verdict doesn't read the rest of what the terminate checks: the release
//! also skips a row whose restore or activation is in flight, and the
//! terminate's reference check ([`node_references`](super::node_references))
//! refuses the node while any row or active execution still names it. A
//! recorded row can therefore still hold `:terminate` until it no longer
//! names the node.

use std::sync::LazyLock;

use pplx_space_accounts::NodeId;
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sandbox_model::{SandboxId, SandboxLifecycle, SandboxLifecycleParseError};
use pplx_space_sandbox_portability::{
    artifact_has_durable_copy, snapshot_durable_for_source_release,
};
use pplx_space_snapshot_model::ArtifactKind;
use pplx_space_sql_locator::sql;
use uuid::Uuid;

use crate::StoredSandboxLifecycle;

/// What the verdict answers for one reported evacuation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EvacuationVerdict {
    /// The sandbox no longer needs the node, for this reason.
    Released(ReleaseReason),
    /// The sandbox may still need the node; the text says why, for the
    /// operator.
    Holds(String),
}

/// Why a sandbox no longer needs the draining node.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReleaseReason {
    /// The row is gone.
    Gone,
    /// The row is `failed`, or the `deleted` tombstone.
    Terminal,
    /// The row is homed on another node: its `target_node_id`, else its
    /// `active_node_id`, is some other node.
    HomedElsewhere,
    /// The row is `started`, `paused` or `suspending`, with an active
    /// execution bound to another node by name.
    RunsElsewhere,
    /// The row is `suspended` and a durable snapshot off the node holds it.
    Durable,
    /// The row is `stopped` and placed on no node. A v2 drain doesn't move
    /// a stopped sandbox; one homed on no node has nothing on this one.
    StoppedUnplaced,
}

/// How the row's pinned artifact (`current_local_artifact_id`) relates to
/// the reported one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Pin {
    /// The row pins no artifact.
    None,
    /// The row pins the reported artifact.
    Reported,
    /// The row pins another artifact.
    Other,
}

/// The facts of one row [`decide`] answers from, as [`read_facts`] reads
/// them.
#[derive(Clone, Debug, PartialEq, Eq)]
struct EvacuationFacts {
    /// The stored lifecycle, or why it doesn't parse.
    lifecycle: Result<StoredSandboxLifecycle, SandboxLifecycleParseError>,
    active_node_id: Option<NodeId>,
    target_node_id: Option<NodeId>,
    pin: Pin,
    /// An active execution of the row is bound to a node by name, and that
    /// name isn't the draining node's. An execution no node is bound to yet
    /// doesn't count, and neither does any execution while the draining
    /// node has no recorded name.
    runs_on_named_other_node: bool,
    /// The catalog holds the reported artifact as a ready, stored full
    /// snapshot.
    reported_artifact_durable: bool,
    /// The row pins an artifact, and the catalog holds it as a ready,
    /// stored full snapshot.
    pinned_artifact_durable: bool,
    /// [`snapshot_durable_for_source_release`] holds for the row.
    durable_for_source_release: bool,
}

/// Why a `stopped` row homed on the draining node holds it.
pub const STOPPED_HELD: &str = "a stopped sandbox: drain v2 doesn't move stopped sandboxes";

/// Whether the sandbox whose facts are `facts` (`None`: the row is gone)
/// still needs `node_id`, for an evacuation the node reported.
///
/// Released, and nothing else:
/// - the row is gone, `failed`, or the `deleted` tombstone;
/// - `started`, `paused` or `suspending`, not active on the node, with an
///   active execution bound to a named other node. Such a row homed on
///   another node but running nowhere the control plane knows of holds;
/// - `suspended` and durable off the node: it pins the reported artifact,
///   durable; or it pins none, the reported artifact is durable, and the
///   terminate's source-release test holds for it; or it pins another
///   artifact, durable. Or, pinning nothing or another artifact, it is
///   homed on another node. A row still
///   pinning the reported artifact waits for it wherever it is homed: the
///   node keeps the sandbox's cache after a restore elsewhere, so it can
///   report an artifact the row no longer pins, but one it still pins is
///   the only copy until its upload;
/// - `stopped` and not homed on the node ([`STOPPED_HELD`] otherwise);
/// - any other lifecycle homed on another node.
///
/// Every other row holds the node, an unplaced one and one whose lifecycle
/// doesn't parse included.
fn decide(node_id: NodeId, facts: Option<&EvacuationFacts>) -> EvacuationVerdict {
    use EvacuationVerdict::{Holds, Released};
    let Some(facts) = facts else {
        return Released(ReleaseReason::Gone);
    };
    let lifecycle = match &facts.lifecycle {
        Ok(StoredSandboxLifecycle::Deleted) => return Released(ReleaseReason::Terminal),
        Ok(StoredSandboxLifecycle::Live(lifecycle)) => *lifecycle,
        Err(error) => {
            return Holds(format!(
                "the row's lifecycle {:?} is unknown to this controller",
                error.value
            ));
        }
    };
    let home = facts.target_node_id.or(facts.active_node_id);
    let homed_elsewhere = home.is_some_and(|home| home != node_id);
    match lifecycle {
        SandboxLifecycle::Failed => Released(ReleaseReason::Terminal),
        SandboxLifecycle::Suspended => suspended(facts, homed_elsewhere),
        SandboxLifecycle::Started | SandboxLifecycle::Paused | SandboxLifecycle::Suspending => {
            if facts.active_node_id == Some(node_id) {
                Holds("the node's suspend isn't projected onto the row yet".to_owned())
            } else if facts.runs_on_named_other_node {
                // Running again, on an execution elsewhere: a user's call
                // moved it.
                Released(ReleaseReason::RunsElsewhere)
            } else {
                Holds("the row runs nowhere the control plane knows of yet".to_owned())
            }
        }
        SandboxLifecycle::Stopped => match home {
            Some(home) if home == node_id => Holds(STOPPED_HELD.to_owned()),
            Some(_) => Released(ReleaseReason::HomedElsewhere),
            None => Released(ReleaseReason::StoppedUnplaced),
        },
        SandboxLifecycle::Created
        | SandboxLifecycle::Creating
        | SandboxLifecycle::Starting
        | SandboxLifecycle::Restoring
        | SandboxLifecycle::Pausing
        | SandboxLifecycle::Resuming
        | SandboxLifecycle::Stopping
        | SandboxLifecycle::Deleting
        | SandboxLifecycle::Checkpointing
        | SandboxLifecycle::Quarantined => {
            if homed_elsewhere {
                Released(ReleaseReason::HomedElsewhere)
            } else {
                let place = if home.is_some() {
                    "homed on this node"
                } else {
                    "placed on no node"
                };
                Holds(format!(
                    "the row is {lifecycle} and {place}: it may still need the node"
                ))
            }
        }
    }
}

/// [`decide`]'s `suspended` branch.
fn suspended(facts: &EvacuationFacts, homed_elsewhere: bool) -> EvacuationVerdict {
    use EvacuationVerdict::{Holds, Released};
    match facts.pin {
        Pin::Reported if facts.reported_artifact_durable => Released(ReleaseReason::Durable),
        Pin::Reported => Holds("the catalog doesn't hold the artifact as ready yet".to_owned()),
        Pin::None | Pin::Other if homed_elsewhere => Released(ReleaseReason::HomedElsewhere),
        // A markerless row's next restore takes the snapshot the
        // source-release test finds, so both halves must hold: the reported
        // snapshot is durable, and the terminate would release the row.
        Pin::None if !facts.reported_artifact_durable => Holds(
            "the row pins no artifact and the catalog doesn't hold the reported one as ready yet"
                .to_owned(),
        ),
        Pin::None if facts.durable_for_source_release => Released(ReleaseReason::Durable),
        Pin::None => Holds(
            "the row pins no artifact and no snapshot durable enough to release its node is ready yet"
                .to_owned(),
        ),
        Pin::Other if facts.pinned_artifact_durable => Released(ReleaseReason::Durable),
        Pin::Other => Holds(
            "the row pins another artifact than the one the node reported, not yet durable"
                .to_owned(),
        ),
    }
}

/// The row as [`read_facts`] decodes it.
#[derive(sqlx::FromRow)]
struct FactsRow {
    lifecycle: String,
    active_node_id: Option<NodeId>,
    target_node_id: Option<NodeId>,
    current_local_artifact_id: Option<Uuid>,
    runs_on_named_other_node: bool,
    reported_artifact_durable: bool,
    pinned_artifact_durable: bool,
    durable_for_source_release: bool,
}

/// The facts [`decide`] needs of `sandbox_id`, for an evacuation of
/// `reported` (a full snapshot's artifact) that `node_id` reported; `None`
/// when the row is gone. One read: it takes no lock and writes nothing.
async fn read_facts(
    executor: impl Executor<'_>,
    node_id: NodeId,
    sandbox_id: SandboxId,
    reported: Uuid,
) -> Result<Option<EvacuationFacts>, DbError> {
    /// `$1` = sandbox id, `$2` = node id, `$3` = the reported artifact,
    /// `$4` = the full artifact kind.
    static FACTS: LazyLock<String> = LazyLock::new(|| {
        let reported = artifact_has_durable_copy("s.id", "$3", "$4");
        let pinned = artifact_has_durable_copy("s.id", "s.current_local_artifact_id", "$4");
        let source_release = snapshot_durable_for_source_release("s");
        sql!(format!(
            "SELECT s.lifecycle, s.active_node_id, s.target_node_id,
                    s.current_local_artifact_id,
                    EXISTS (
                      SELECT 1 FROM sandbox_k8s_executions e
                       WHERE e.sandbox_id = s.id
                         AND e.active
                         AND e.k8s_node_name IS NOT NULL
                         AND e.k8s_node_name <>
                             (SELECT k8s_node_name FROM node_k8s WHERE node_id = $2)
                    ) AS runs_on_named_other_node,
                    {reported} AS reported_artifact_durable,
                    (s.current_local_artifact_id IS NOT NULL AND {pinned})
                        AS pinned_artifact_durable,
                    {source_release} AS durable_for_source_release
               FROM sandboxes s
              WHERE s.id = $1"
        ))
    });
    let row: Option<FactsRow> = sqlx::query_as(sqlx::AssertSqlSafe(FACTS.as_str()))
        .bind(sandbox_id)
        .bind(node_id)
        .bind(reported)
        .bind(ArtifactKind::Full.as_str())
        .fetch_optional(executor.into_sqlx())
        .await?;
    Ok(row.map(|row| EvacuationFacts {
        lifecycle: row.lifecycle.parse(),
        active_node_id: row.active_node_id,
        target_node_id: row.target_node_id,
        pin: match row.current_local_artifact_id {
            None => Pin::None,
            Some(pin) if pin == reported => Pin::Reported,
            Some(_) => Pin::Other,
        },
        runs_on_named_other_node: row.runs_on_named_other_node,
        reported_artifact_durable: row.reported_artifact_durable,
        pinned_artifact_durable: row.pinned_artifact_durable,
        durable_for_source_release: row.durable_for_source_release,
    }))
}

/// Whether `sandbox_id` still needs `node_id`, for the evacuation of the
/// full snapshot `reported` the node reported: `read_facts`, then
/// `decide`.
pub async fn evacuation_verdict(
    executor: impl Executor<'_>,
    node_id: NodeId,
    sandbox_id: SandboxId,
    reported: Uuid,
) -> Result<EvacuationVerdict, DbError> {
    let facts = read_facts(executor, node_id, sandbox_id, reported).await?;
    Ok(decide(node_id, facts.as_ref()))
}

#[cfg(test)]
mod tests;
