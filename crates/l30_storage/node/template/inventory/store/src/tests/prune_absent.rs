//! [`prune_absent_templates`] against Postgres: each case seeds inventory
//! rows, each with one component, and observed-status rows, each with one
//! condition, runs the prune, and checks what is left of all four tables.

use pplx_space_db_test_fixtures::{TestDbResult, insert_ready_worker_node, pg_test_db};
use pplx_space_state_reconciliation::FACET_NETWORK_POLICY;
use sqlx::PgPool;

use crate::prune_absent_templates;
use pplx_space_accounts::NodeId;

const NP: &str = FACET_NETWORK_POLICY;

/// Which seeded node a row belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum On {
    Node,
    Other,
}

#[derive(Clone, Copy)]
struct Nodes {
    node: NodeId,
    other: NodeId,
}

impl Nodes {
    fn id(self, on: On) -> NodeId {
        match on {
            On::Node => self.node,
            On::Other => self.other,
        }
    }

    fn on(self, node_id: NodeId) -> On {
        if node_id == self.node {
            On::Node
        } else {
            assert_eq!(node_id, self.other);
            On::Other
        }
    }
}

type Key = (On, String, String);

struct Case {
    inventory: &'static [(On, &'static str, &'static str)],
    facets: &'static [(On, &'static str, &'static str)],
    present: &'static [(&'static str, &'static str)],
    /// The listed templates the publication reports a network-policy
    /// generation for.
    with_policy: &'static [&'static str],
    /// The inventory and observed-status rows left afterwards; their
    /// components and conditions are left with them.
    kept_inventory: &'static [(On, &'static str, &'static str)],
    kept_facets: &'static [(On, &'static str, &'static str)],
}

async fn seed(pool: &PgPool, nodes: Nodes, case: &Case) -> TestDbResult<()> {
    insert_ready_worker_node(pool, nodes.node).await?;
    insert_ready_worker_node(pool, nodes.other).await?;
    for (on, template, hash) in case.inventory {
        sqlx::query(
            "INSERT INTO templates (id, name)
             VALUES (space_uuid_v7(), $1)
             ON CONFLICT (name) DO NOTHING",
        )
        .bind(template)
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO node_template_inventory (node_id, template_name, content_hash)
             VALUES ($1, $2, $3)",
        )
        .bind(nodes.id(*on))
        .bind(template)
        .bind(hash)
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO node_template_components (node_id, template_name, content_hash, kind)
             VALUES ($1, $2, $3, 'kernel')",
        )
        .bind(nodes.id(*on))
        .bind(template)
        .bind(hash)
        .execute(pool)
        .await?;
    }
    for (on, facet, element) in case.facets {
        sqlx::query(
            "INSERT INTO node_facet_observed_status (node_id, facet, element, observed_at)
             VALUES ($1, $2, $3, NOW())",
        )
        .bind(nodes.id(*on))
        .bind(facet)
        .bind(element)
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO node_facet_conditions (
                 node_id, facet, element, condition_type, condition_status, last_transition_at
             )
             VALUES ($1, $2, $3, 'ApplyFailed', 'True', NOW())",
        )
        .bind(nodes.id(*on))
        .bind(facet)
        .bind(element)
        .execute(pool)
        .await?;
    }
    Ok(())
}

/// Three-column keys of `statement`'s rows, by seeded node, sorted.
async fn keys(pool: &PgPool, nodes: Nodes, statement: &'static str) -> TestDbResult<Vec<Key>> {
    let rows = sqlx::query_as::<_, (NodeId, String, String)>(statement)
        .fetch_all(pool)
        .await?;
    let mut rows = rows
        .into_iter()
        .map(|(node_id, a, b)| (nodes.on(node_id), a, b))
        .collect::<Vec<_>>();
    rows.sort();
    Ok(rows)
}

fn owned(rows: &[(On, &str, &str)]) -> Vec<Key> {
    let mut rows = rows
        .iter()
        .map(|(on, a, b)| (*on, a.to_string(), b.to_string()))
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

/// Seed `case`, prune the first node with its listed builds, and check every
/// table the statement reaches.
async fn check(case: Case) -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let nodes = Nodes {
        node: NodeId::new(),
        other: NodeId::new(),
    };
    seed(pool, nodes, &case).await?;
    let present = case
        .present
        .iter()
        .map(|(template, hash)| (template.to_string(), hash.to_string()))
        .collect::<Vec<_>>();

    let with_policy = case
        .with_policy
        .iter()
        .map(|template| template.to_string())
        .collect::<Vec<_>>();

    prune_absent_templates(pool, nodes.node, &present, &with_policy).await?;

    let builds = owned(case.kept_inventory);
    let facets = owned(case.kept_facets);
    assert_eq!(
        keys(
            pool,
            nodes,
            "SELECT node_id, template_name, content_hash FROM node_template_inventory"
        )
        .await?,
        builds
    );
    assert_eq!(
        keys(
            pool,
            nodes,
            "SELECT node_id, template_name, content_hash FROM node_template_components"
        )
        .await?,
        builds
    );
    assert_eq!(
        keys(
            pool,
            nodes,
            "SELECT node_id, facet, element FROM node_facet_observed_status"
        )
        .await?,
        facets
    );
    assert_eq!(
        keys(
            pool,
            nodes,
            "SELECT node_id, facet, element FROM node_facet_conditions"
        )
        .await?,
        facets
    );
    Ok(())
}

/// Listed builds stay; the others go with their network-policy rows.
#[tokio::test(flavor = "current_thread")]
async fn some_templates_removed() -> TestDbResult<()> {
    check(Case {
        inventory: &[
            (On::Node, "alpha", "sha256:a1"),
            (On::Node, "beta", "sha256:b1"),
            (On::Node, "gamma", "sha256:c1"),
        ],
        facets: &[
            (On::Node, NP, "template:alpha"),
            (On::Node, NP, "template:beta"),
            (On::Node, NP, "template:gamma"),
        ],
        present: &[("alpha", "sha256:a1"), ("gamma", "sha256:c1")],
        with_policy: &["alpha", "gamma"],
        kept_inventory: &[
            (On::Node, "alpha", "sha256:a1"),
            (On::Node, "gamma", "sha256:c1"),
        ],
        kept_facets: &[
            (On::Node, NP, "template:alpha"),
            (On::Node, NP, "template:gamma"),
        ],
    })
    .await
}

/// A publication listing every build changes nothing.
#[tokio::test(flavor = "current_thread")]
async fn nothing_removed() -> TestDbResult<()> {
    check(Case {
        inventory: &[
            (On::Node, "alpha", "sha256:a1"),
            (On::Node, "beta", "sha256:b1"),
        ],
        facets: &[
            (On::Node, NP, "template:alpha"),
            (On::Node, NP, "template:beta"),
        ],
        present: &[
            ("alpha", "sha256:a1"),
            ("beta", "sha256:b1"),
            ("unknown", "sha256:u1"),
        ],
        with_policy: &["alpha", "beta", "unknown"],
        kept_inventory: &[
            (On::Node, "alpha", "sha256:a1"),
            (On::Node, "beta", "sha256:b1"),
        ],
        kept_facets: &[
            (On::Node, NP, "template:alpha"),
            (On::Node, NP, "template:beta"),
        ],
    })
    .await
}

/// A publication listing nothing removes every build of the node and their
/// network-policy rows, and nothing of another node's.
#[tokio::test(flavor = "current_thread")]
async fn everything_removed_on_empty_input() -> TestDbResult<()> {
    check(Case {
        inventory: &[
            (On::Node, "alpha", "sha256:a1"),
            (On::Node, "beta", "sha256:b1"),
            (On::Other, "alpha", "sha256:a1"),
        ],
        facets: &[
            (On::Node, NP, "template:alpha"),
            (On::Node, NP, "template:beta"),
            (On::Other, NP, "template:alpha"),
        ],
        present: &[],
        with_policy: &[],
        kept_inventory: &[(On::Other, "alpha", "sha256:a1")],
        kept_facets: &[(On::Other, NP, "template:alpha")],
    })
    .await
}

/// A removed template with no network-policy row still loses its inventory
/// row.
#[tokio::test(flavor = "current_thread")]
async fn removed_template_without_facet_row() -> TestDbResult<()> {
    check(Case {
        inventory: &[
            (On::Node, "alpha", "sha256:a1"),
            (On::Node, "beta", "sha256:b1"),
        ],
        facets: &[(On::Node, NP, "template:alpha")],
        present: &[("alpha", "sha256:a1")],
        with_policy: &["alpha"],
        kept_inventory: &[(On::Node, "alpha", "sha256:a1")],
        kept_facets: &[(On::Node, NP, "template:alpha")],
    })
    .await
}

/// Only the node's network-policy rows of a removed template go: the same
/// element under another facet, a bare-name element, another node's row,
/// and a sandbox element all stay.
#[tokio::test(flavor = "current_thread")]
async fn other_facets_elements_and_nodes_survive() -> TestDbResult<()> {
    check(Case {
        inventory: &[
            (On::Node, "beta", "sha256:b1"),
            (On::Other, "beta", "sha256:b1"),
        ],
        facets: &[
            (On::Node, NP, "template:beta"),
            (On::Node, "template-desired-state", "beta"),
            (On::Node, "template-desired-state", "template:beta"),
            (On::Node, NP, "beta"),
            (On::Node, NP, "sandbox:00000000-0000-7000-8000-000000000001"),
            (On::Other, NP, "template:beta"),
        ],
        present: &[],
        with_policy: &[],
        kept_inventory: &[(On::Other, "beta", "sha256:b1")],
        kept_facets: &[
            (On::Node, "template-desired-state", "beta"),
            (On::Node, "template-desired-state", "template:beta"),
            (On::Node, NP, "beta"),
            (On::Node, NP, "sandbox:00000000-0000-7000-8000-000000000001"),
            (On::Other, NP, "template:beta"),
        ],
    })
    .await
}

/// Several builds of one template go together; listed keys may repeat; a
/// template listed under another build with a network-policy generation
/// loses the build it no longer lists and keeps its network-policy row.
#[tokio::test(flavor = "current_thread")]
async fn duplicate_names() -> TestDbResult<()> {
    check(Case {
        inventory: &[
            (On::Node, "alpha", "sha256:a1"),
            (On::Node, "alpha", "sha256:a2"),
            (On::Node, "beta", "sha256:b1"),
            (On::Node, "beta", "sha256:b2"),
            (On::Node, "gamma", "sha256:c1"),
        ],
        facets: &[
            (On::Node, NP, "template:alpha"),
            (On::Node, NP, "template:beta"),
            (On::Node, NP, "template:gamma"),
        ],
        present: &[
            ("beta", "sha256:b2"),
            ("gamma", "sha256:c1"),
            ("gamma", "sha256:c1"),
        ],
        with_policy: &["beta", "gamma"],
        kept_inventory: &[
            (On::Node, "beta", "sha256:b2"),
            (On::Node, "gamma", "sha256:c1"),
        ],
        kept_facets: &[
            (On::Node, NP, "template:beta"),
            (On::Node, NP, "template:gamma"),
        ],
    })
    .await
}

/// A rebuilt template listed with a network-policy generation, only under a
/// build the node has no row for, loses its old build and keeps its
/// network-policy row.
#[tokio::test(flavor = "current_thread")]
async fn rebuilt_template_keeps_network_policy_row() -> TestDbResult<()> {
    check(Case {
        inventory: &[(On::Node, "alpha", "sha256:a1")],
        facets: &[(On::Node, NP, "template:alpha")],
        present: &[("alpha", "sha256:a2")],
        with_policy: &["alpha"],
        kept_inventory: &[],
        kept_facets: &[(On::Node, NP, "template:alpha")],
    })
    .await
}

/// A rebuilt template listed with no network-policy generation loses its old
/// build and its network-policy row.
#[tokio::test(flavor = "current_thread")]
async fn rebuilt_template_without_policy_loses_network_policy_row() -> TestDbResult<()> {
    check(Case {
        inventory: &[(On::Node, "alpha", "sha256:a1")],
        facets: &[(On::Node, NP, "template:alpha")],
        present: &[("alpha", "sha256:a2")],
        with_policy: &[],
        kept_inventory: &[],
        kept_facets: &[],
    })
    .await
}
