//! Whether a template campaign's twins would land only on nodes that read
//! them.

use std::sync::LazyLock;

use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sql_locator::sql;

/// How many of the substrate's workers [`workers_not_reading_twins`] counted,
/// and which of them hold a template campaign.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TwinReaders {
    /// The substrate's workers.
    pub workers: u64,
    /// The workers whose status isn't evidence or that don't report reading
    /// twin metadata.
    pub holding: u64,
    /// Up to [`HOLDING_SAMPLE`] of the holding workers' ids, lowest first, for
    /// an operator to look at.
    pub sample: Vec<uuid::Uuid>,
}

/// The most holding worker ids [`TwinReaders::sample`] carries.
pub const HOLDING_SAMPLE: usize = 5;

impl TwinReaders {
    /// Whether a template campaign holds: a worker holds it, or the substrate
    /// has none. No registered worker isn't evidence that every worker reads
    /// twins: a node that joins later takes creates whatever it supports.
    pub fn holds(&self) -> bool {
        self.workers == 0 || self.holding > 0
    }
}

/// The substrate's (`managed_by`) worker nodes, and those that would hold a
/// template campaign: those whose status isn't evidence of their
/// environments (no snapshot, an incomplete inventory, or a stale one), and
/// those whose template-manager doesn't report reading a twin's own metadata
/// ([`pplx_space_node_status_store::reads_twin_metadata`]).
///
/// A twin wins resolution once it commits, so a create of its template can
/// land on any worker that can run the twin's environment and pull a build it
/// can't read. Which workers can is decided by node labels that node-ctl sets
/// from its own inventory read, not by the snapshot's usable environments,
/// and a draining node can still admit pods while its cordon is re-asserted.
/// So every worker counts, whatever its environments or drain phase: the
/// support is a property of the node's binaries, not of an environment. A
/// node with no `nodes` row yet isn't seen, and a deleted one's row isn't
/// counted. Takes no lock.
pub async fn workers_not_reading_twins(
    executor: impl Executor<'_>,
    managed_by: &str,
) -> Result<TwinReaders, DbError> {
    /// Built once: the node status predicates are the only interpolation.
    static COUNT: LazyLock<String> = LazyLock::new(|| {
        let holding = format!(
            "NOT {evidence} OR NOT {reads}",
            evidence = pplx_space_node_status_store::environment_evidence("nss"),
            reads = pplx_space_node_status_store::reads_twin_metadata("nss"),
        );
        sql!(format!(
            "SELECT count(*),
                    count(*) FILTER (WHERE {holding}),
                    COALESCE(
                        (array_agg(n.id ORDER BY n.id) FILTER (WHERE {holding}))[1:{HOLDING_SAMPLE}],
                        '{{}}'
                    )
             FROM nodes n
             LEFT JOIN node_status_snapshots nss ON nss.node_id = n.id
             WHERE n.managed_by = $1
               AND n.role = 'worker'
               AND COALESCE(n.state, 'registered') != 'deleted'"
        ))
    });
    let (workers, holding, sample) =
        sqlx::query_as::<_, (i64, i64, Vec<uuid::Uuid>)>(sqlx::AssertSqlSafe(COUNT.as_str()))
            .bind(managed_by)
            .fetch_one(executor.into_sqlx())
            .await?;
    Ok(TwinReaders {
        workers: u64::try_from(workers).unwrap_or_default(),
        holding: u64::try_from(holding).unwrap_or_default(),
        sample,
    })
}
