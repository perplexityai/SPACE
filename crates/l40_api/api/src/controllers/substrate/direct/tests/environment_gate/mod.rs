//! The environment placement gate on Direct. Each statement runs against
//! nodes with and without evidence, for work bound to E1, work bound to E0
//! and unbound work, in every mode.
//!
//! Every seeded set also holds a node only the last-known create fallback
//! can return (degraded, or stale), whose snapshot the gate would admit or
//! refuse differently from the fresh nodes. A cached-ready statement that
//! wrongly comes back empty falls through to that fallback, and the extra
//! node shows it.
use std::collections::{BTreeMap, BTreeSet};

use pplx_space_node_status_store::EnvironmentRefusal;
use pplx_space_node_status_store::test_support::{SnapshotSeed, upsert_snapshot};

use super::*;
use crate::controllers::substrate::environment_placement::EnvironmentPlacementGate;

const E0: &str = "sha256:0000000000000000000000000000000000000000000000000000000000000000";
const E1: &str = "sha256:1111111111111111111111111111111111111111111111111111111111111111";

const BOUND_E1: &str = "bound-e1";
const BOUND_E0: &str = "bound-e0";
const UNBOUND: &str = "unbound";
const TEMPLATES: [(&str, Option<&str>); 3] =
    [(BOUND_E1, Some(E1)), (BOUND_E0, Some(E0)), (UNBOUND, None)];
const HASH: &str = "hash-seed";

use EnvironmentPlacementGate::{Off, On, Shadow};
use EnvironmentRefusal::{EnvironmentNotUsable, Incomplete, NoEvidence, Stale};

mod bindings;
mod legacy_checks;
mod misses;
mod modes;

/// The seeded ready nodes, named by what their snapshot says, and a degraded
/// node with evidence for both environments.
struct Nodes {
    /// Evidence listing E1.
    good_e1: NodeId,
    /// Evidence listing E0.
    good_e0: NodeId,
    /// A partial inventory that lists both.
    incomplete: NodeId,
    /// A build that publishes no environments.
    old_build: NodeId,
    /// No inventory report at all: a second node without evidence, so a
    /// reason's count can be more than one.
    unreported: NodeId,
    /// Arrays listing both, received before the node's freshness window.
    stale_arrays: NodeId,
    /// Not cached-ready, so only the last-known fallback returns it.
    degraded: NodeId,
}

impl Nodes {
    /// The ready nodes.
    fn ready(&self) -> BTreeSet<NodeId> {
        [
            self.good_e1,
            self.good_e0,
            self.incomplete,
            self.old_build,
            self.unreported,
            self.stale_arrays,
        ]
        .into()
    }

    /// Why on refuses `node` for work bound to `environment`.
    fn refusal(&self, node: NodeId, environment: Option<&str>) -> Option<EnvironmentRefusal> {
        let environment = environment?;
        if node == self.incomplete {
            Some(Incomplete)
        } else if node == self.old_build || node == self.unreported {
            Some(NoEvidence)
        } else if node == self.stale_arrays {
            Some(Stale)
        } else if (node == self.good_e1 && environment == E1)
            || (node == self.good_e0 && environment == E0)
            || node == self.degraded
        {
            None
        } else {
            Some(EnvironmentNotUsable)
        }
    }

    /// The ready nodes on admits for work bound to `environment`.
    fn admitted(&self, environment: Option<&str>) -> BTreeSet<NodeId> {
        self.ready()
            .into_iter()
            .filter(|node| self.refusal(*node, environment).is_none())
            .collect()
    }

    /// What shadow counts on `path` for `candidates` returned off.
    fn refusals(
        &self,
        path: &str,
        candidates: &BTreeSet<NodeId>,
        environment: Option<&str>,
    ) -> Refusals {
        let mut expected = Refusals::new();
        for node in candidates {
            if let Some(refusal) = self.refusal(*node, environment) {
                // A restore refused only for want of cold-boot evidence has a
                // label of its own.
                let label = if path.starts_with("restore_") && refusal == EnvironmentNotUsable {
                    "restore_not_cold_bootable"
                } else {
                    refusal.as_str()
                };
                *expected
                    .entry((path.to_string(), label.to_string()))
                    .or_default() += 1;
            }
        }
        expected
    }
}

/// Shadow's refusals by (statement, reason).
type Refusals = BTreeMap<(String, String), u64>;

/// What `substrate`'s shadow recorder classified, by (statement, reason).
fn counted(substrate: &DirectSubstrate) -> Refusals {
    let mut counted = Refusals::new();
    for (path, label, _) in substrate.recorded_refusals() {
        *counted
            .entry((path.as_str().to_string(), label.to_string()))
            .or_default() += 1;
    }
    counted
}

fn gated(fixture: &PostgresFixture, gate: EnvironmentPlacementGate) -> DirectSubstrate {
    DirectSubstrate::new(fixture.pool.clone()).with_environment_placement_gate(gate)
}

/// A worker whose snapshot is `seed`, with `usable` as its usable
/// environments when the seed has arrays.
async fn seed_node(
    pool: &PgPool,
    endpoint: &str,
    seed: SnapshotSeed,
    usable: &[&str],
    state_format_version: &str,
) -> TestResult<NodeId> {
    let node = NodeId::new();
    insert_ready_worker(pool, node, endpoint, state_format_version).await?;
    upsert_snapshot(pool, node, seed).await?;
    if seed.environment_refs_secs_ago.is_some() {
        sqlx::query(sql!(
            "UPDATE node_status_snapshots
             SET usable_environment_refs = $2, known_environment_refs = $2
             WHERE node_id = $1"
        ))
        .bind(node)
        .bind(usable)
        .execute(pool)
        .await?;
    }
    Ok(node)
}

async fn degrade(pool: &PgPool, node: NodeId) -> TestResult {
    sqlx::query(sql!(
        "UPDATE node_status_snapshots SET scheduling_status = 'degraded' WHERE node_id = $1"
    ))
    .bind(node)
    .execute(pool)
    .await?;
    Ok(())
}

async fn seed_nodes(pool: &PgPool, state_format_version: &str) -> TestResult<Nodes> {
    let fresh = SnapshotSeed::default();
    let both = [E1, E0];
    let nodes = Nodes {
        good_e1: seed_node(pool, "127.0.0.1:1", fresh, &[E1], state_format_version).await?,
        good_e0: seed_node(pool, "127.0.0.1:2", fresh, &[E0], state_format_version).await?,
        incomplete: seed_node(
            pool,
            "127.0.0.1:3",
            SnapshotSeed {
                inventory: Some("partial"),
                ..fresh
            },
            &both,
            state_format_version,
        )
        .await?,
        old_build: seed_node(
            pool,
            "127.0.0.1:4",
            SnapshotSeed {
                environment_refs_secs_ago: None,
                ..fresh
            },
            &[],
            state_format_version,
        )
        .await?,
        unreported: seed_node(
            pool,
            "127.0.0.1:7",
            SnapshotSeed {
                inventory: None,
                ..fresh
            },
            &both,
            state_format_version,
        )
        .await?,
        stale_arrays: seed_node(
            pool,
            "127.0.0.1:5",
            SnapshotSeed {
                observed_secs_ago: 50.0,
                stale_in_secs: 10.0,
                environment_refs_secs_ago: Some(65.0),
                ..fresh
            },
            &both,
            state_format_version,
        )
        .await?,
        degraded: seed_node(pool, "127.0.0.1:6", fresh, &both, state_format_version).await?,
    };
    degrade(pool, nodes.degraded).await?;
    Ok(nodes)
}

/// Inventory `template` on `node`, bound to `environment` when given, with
/// a warm pool member.
async fn inventory(
    pool: &PgPool,
    node: NodeId,
    template: &str,
    environment: Option<&str>,
    state_format_version: &str,
) -> TestResult {
    insert_template_inventory(pool, node, template, HASH, state_format_version).await?;
    sqlx::query(sql!(
        "UPDATE node_template_inventory
         SET execution_metadata = CASE WHEN $3::TEXT IS NULL THEN NULL
                 ELSE jsonb_build_object('environment', $3::TEXT) END,
             pool_current = 1
         WHERE node_id = $1 AND template_name = $2"
    ))
    .bind(node)
    .bind(template)
    .bind(environment)
    .execute(pool)
    .await?;
    Ok(())
}

/// The three templates on every one of `nodes`, and their releases.
async fn inventory_templates(
    pool: &PgPool,
    nodes: &BTreeSet<NodeId>,
    state_format_version: &str,
) -> TestResult {
    for (template, environment) in TEMPLATES {
        for node in nodes {
            inventory(pool, *node, template, environment, state_format_version).await?;
        }
        sqlx::query(sql!(
            "INSERT INTO template_releases (template_name, type, hash, execution_metadata)
             VALUES ($1, 'fc', $2, CASE WHEN $3::TEXT IS NULL THEN NULL
                 ELSE jsonb_build_object('environment', $3::TEXT) END)"
        ))
        .bind(template)
        .bind(HASH)
        .bind(environment)
        .execute(pool)
        .await?;
    }
    Ok(())
}

async fn cached_create(
    substrate: &DirectSubstrate,
    template: &str,
    boot_mode: CreateBootMode,
) -> TestResult<BTreeSet<NodeId>> {
    match substrate
        .cached_worker_create_candidates(template, Some(HASH), "fc", boot_mode)
        .await
    {
        Ok(nodes) => Ok(nodes.into_iter().map(|node| node.id).collect()),
        Err(SubstrateError::Unavailable { .. }) => Ok(BTreeSet::new()),
        Err(error) => Err(error.into()),
    }
}

async fn pull_create(substrate: &DirectSubstrate, template: &str) -> TestResult<BTreeSet<NodeId>> {
    Ok(substrate
        .pull_on_create_worker_candidates(template, Some(HASH), "fc")
        .await?
        .into_iter()
        .map(|node| node.id)
        .collect())
}

async fn pooled(substrate: &DirectSubstrate, template: &str) -> TestResult<BTreeSet<NodeId>> {
    Ok(substrate
        .pooled_worker_candidates(template, Some(HASH), "fc")
        .await?
        .into_iter()
        .map(|candidate| candidate.id)
        .collect())
}

/// Each create statement's candidates for `template` under `gate`, and what
/// it counted.
async fn creates(
    fixture: &PostgresFixture,
    gate: EnvironmentPlacementGate,
    template: &str,
) -> TestResult<(BTreeMap<&'static str, BTreeSet<NodeId>>, Refusals)> {
    let substrate = gated(fixture, gate);
    let candidates = BTreeMap::from([
        (
            "cached_create",
            cached_create(&substrate, template, CreateBootMode::Cold).await?,
        ),
        ("pull_create", pull_create(&substrate, template).await?),
        ("pool_adoption", pooled(&substrate, template).await?),
    ]);
    Ok((candidates, counted(&substrate)))
}

/// A suspended sandbox whose source is `source`, with a ready checkpoint
/// produced under `environment` in `state_format_version` and stored
/// remotely.
async fn suspended_sandbox(
    pool: &PgPool,
    source: NodeId,
    environment: Option<&str>,
    state_format_version: &str,
) -> TestResult<SandboxId> {
    let sandbox_id = SandboxId::new();
    insert_sandbox(pool, sandbox_id, Some(source)).await?;
    insert_observed_source(pool, sandbox_id, source).await?;
    let snapshot_id = insert_ready_snapshot(
        pool,
        sandbox_id,
        source,
        state_format_version,
        Some("s3://synthetic-bucket/checkpoint"),
    )
    .await?;
    sqlx::query(sql!(
        "UPDATE snapshots
         SET execution_metadata = CASE WHEN $2::TEXT IS NULL THEN NULL
                 ELSE jsonb_build_object('environment', $2::TEXT, 'components', '[]'::JSONB) END
         WHERE id = $1"
    ))
    .bind(snapshot_id)
    .bind(environment)
    .execute(pool)
    .await?;
    Ok(sandbox_id)
}

/// The restore's source candidate, if any, its remote candidates, and what
/// it counted.
async fn restore(
    fixture: &PostgresFixture,
    gate: EnvironmentPlacementGate,
    sandbox_id: SandboxId,
) -> TestResult<((Option<NodeId>, BTreeSet<NodeId>), Refusals)> {
    let substrate = gated(fixture, gate);
    let mut source = None;
    let mut remote = BTreeSet::new();
    for candidate in substrate.direct_restore_candidates(sandbox_id).await? {
        match candidate.kind {
            DirectRestoreCandidateKind::SourceNode => source = Some(candidate.node.id),
            DirectRestoreCandidateKind::RemoteSnapshot => {
                remote.insert(candidate.node.id);
            }
        }
    }
    Ok(((source, remote), counted(&substrate)))
}
