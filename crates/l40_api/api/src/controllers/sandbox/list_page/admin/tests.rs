use std::collections::BTreeMap;

use pplx_space_common_core::types::{NodeId, SubstrateKind};
use pplx_space_db_test_fixtures::{TestDbResult, insert_ready_worker_node, pg_test_db};
use time::Duration;

use super::*;
use crate::controllers::sandbox::SandboxController;
use crate::models::SandboxResponse;

/// An org and one of its users, for seeded sandboxes to belong to.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Owner {
    pub(crate) org_id: OrgId,
    pub(crate) user_id: UserId,
}

pub(crate) async fn seed_org(pool: &PgPool) -> TestDbResult<OrgId> {
    let id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO organizations (id, name, slug, byok_enabled)
         VALUES ($1, $2, $3, false)"
    ))
    .bind(id)
    .bind(format!("Org {id}"))
    .bind(format!("org-{}", id.simple()))
    .execute(pool)
    .await?;
    Ok(OrgId::from_uuid(id))
}

pub(crate) async fn seed_user(pool: &PgPool, org_id: OrgId) -> TestDbResult<Owner> {
    let user_id = UserId::new();
    sqlx::query(sql!(
        "INSERT INTO users (id, email, password_hash, display_name, org_id, role)
         VALUES ($1, $2, 'synthetic-password-hash', 'Sandbox list tests', $3, 'member')"
    ))
    .bind(user_id)
    .bind(format!("{user_id}@example.invalid"))
    .bind(org_id)
    .execute(pool)
    .await?;
    Ok(Owner { org_id, user_id })
}

pub(crate) async fn seed_owner(pool: &PgPool) -> TestDbResult<Owner> {
    let org_id = seed_org(pool).await?;
    seed_user(pool, org_id).await
}

/// A sandbox row to seed; unset fields take the column defaults.
#[derive(Debug, Clone, Default)]
pub(crate) struct SeedSandbox {
    pub(crate) id: Option<SandboxId>,
    pub(crate) name: Option<&'static str>,
    pub(crate) lifecycle: Option<&'static str>,
    pub(crate) template: Option<&'static str>,
    pub(crate) active_node_id: Option<NodeId>,
    pub(crate) created_at: Option<OffsetDateTime>,
}

pub(crate) async fn seed_sandbox(
    pool: &PgPool,
    owner: Owner,
    seed: SeedSandbox,
) -> TestDbResult<SandboxId> {
    let id = seed.id.unwrap_or_default();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, name, user_id, org_id, managed_by, lifecycle, placement,
             network_policy_generation, template_id, active_node_id, created_at
         )
         VALUES ($1, $2, $3, $4, $5, COALESCE($6, 'started'), 'placed', 1, $7, $8,
                 COALESCE($9, NOW()))"
    ))
    .bind(id)
    .bind(seed.name)
    .bind(owner.user_id)
    .bind(owner.org_id)
    .bind(SubstrateKind::Kubernetes)
    .bind(seed.lifecycle)
    .bind(seed.template)
    .bind(seed.active_node_id)
    .bind(seed.created_at)
    .execute(pool)
    .await?;
    Ok(id)
}

/// What a node reports for a sandbox, which need not have a sandbox row.
#[derive(Debug, Clone, Default)]
pub(crate) struct SeedObservation {
    pub(crate) status: Option<&'static str>,
    pub(crate) template: Option<&'static str>,
    pub(crate) pool_template: Option<&'static str>,
    pub(crate) image_hash: Option<&'static str>,
    pub(crate) spaced_version: Option<&'static str>,
}

pub(crate) async fn seed_observation(
    pool: &PgPool,
    sandbox_id: SandboxId,
    node_id: NodeId,
    seed: SeedObservation,
) -> TestDbResult<()> {
    sqlx::query(sql!(
        "INSERT INTO sandbox_observed_states (
             sandbox_id, node_id, status, template, pool_template, image_hash, spaced_version
         )
         VALUES ($1, $2, $3, $4, $5, $6, $7)"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .bind(seed.status)
    .bind(seed.template)
    .bind(seed.pool_template)
    .bind(seed.image_hash)
    .bind(seed.spaced_version)
    .execute(pool)
    .await?;
    Ok(())
}

fn controller(pool: &PgPool) -> SandboxController {
    SandboxController::new(crate::test_support::direct_substrate(pool))
}

/// One page of the list, with no byte budget unless `budget` says so.
async fn page(
    pool: &PgPool,
    filter: &AdminSandboxFilter,
    after: Option<SandboxPageCursor>,
    limit: u32,
) -> Result<ListPage<Sandbox>, sqlx::Error> {
    controller(pool)
        .admin_sandbox_page(pool, filter, after, limit, PageBudget::Bounded)
        .await
}

async fn listed_ids(pool: &PgPool, filter: &AdminSandboxFilter) -> TestDbResult<Vec<SandboxId>> {
    let page = page(pool, filter, None, 100).await?;
    Ok(page.rows.iter().map(|sandbox| sandbox.id).collect())
}

/// The legacy unpaged list's IDs, which run their own statement.
async fn unpaged_ids(pool: &PgPool, filter: &AdminSandboxFilter) -> TestDbResult<Vec<SandboxId>> {
    let page = controller(pool)
        .admin_sandbox_page(pool, filter, None, 100, PageBudget::Unbounded)
        .await?;
    Ok(page.rows.iter().map(|sandbox| sandbox.id).collect())
}

/// A current Direct route for `sandbox_id`, on `node_id`.
async fn seed_route(pool: &PgPool, sandbox_id: SandboxId, node_id: NodeId) -> TestDbResult<()> {
    sqlx::query(sql!(
        "INSERT INTO sandbox_direct_route_identities (
             sandbox_id, route_generation, current_node_id, substrate_kind,
             execution_generation, activation_attempt_id
         )
         VALUES ($1, 1, $2, $3, 1, $4)"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .bind(SubstrateKind::Space)
    .bind(Uuid::now_v7())
    .execute(pool)
    .await?;
    Ok(())
}

fn base_time() -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(1_790_000_000).expect("fixture timestamp")
}

fn cursor_after(sandbox: &Sandbox) -> SandboxPageCursor {
    SandboxPageCursor {
        created_at: sandbox.created_at,
        id: sandbox.id,
    }
}

/// Walking the list a page at a time returns every row that existed when the
/// walk began exactly once, in list order, although rows sharing a
/// `created_at` straddle page boundaries and rows created later land between
/// pages.
#[tokio::test]
async fn pages_return_every_row_once_while_rows_are_inserted() -> TestDbResult<()> {
    const PAGE: u32 = 2;
    let db = pg_test_db().await?;
    let owner = seed_owner(&db.pool).await?;
    let mut existing = Vec::new();
    for index in 0..9 {
        // Three rows per timestamp, so the order between them is the id's.
        let created_at = base_time() - Duration::minutes(index / 3);
        let id = seed_sandbox(
            &db.pool,
            owner,
            SeedSandbox {
                created_at: Some(created_at),
                ..SeedSandbox::default()
            },
        )
        .await?;
        existing.push((created_at, id));
    }
    existing.sort_unstable_by(|left, right| right.cmp(left));

    let filter = AdminSandboxFilter {
        org_id: Some(owner.org_id),
        ..AdminSandboxFilter::default()
    };
    let mut seen = Vec::new();
    let mut after = None;
    loop {
        let page = page(&db.pool, &filter, after, PAGE).await?;
        seed_sandbox(&db.pool, owner, SeedSandbox::default()).await?;
        assert!(page.rows.len() <= PAGE as usize);
        seen.extend(page.rows.iter().map(|sandbox| sandbox.id));
        if !page.has_more {
            break;
        }
        after = page.rows.last().map(cursor_after);
    }
    let expected = existing.iter().map(|(_, id)| *id).collect::<Vec<_>>();
    assert_eq!(seen, expected);
    db.pool.close().await;
    Ok(())
}

/// With no filter beyond `org_id` and no cursor, the list is the rows the
/// route's statement returned before paging and filters existed.
#[tokio::test]
async fn the_first_page_is_the_list_from_before_paging() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let first = seed_owner(&db.pool).await?;
    let second = seed_owner(&db.pool).await?;
    for (index, owner) in [first, second, first, second, first]
        .into_iter()
        .enumerate()
    {
        seed_sandbox(
            &db.pool,
            owner,
            SeedSandbox {
                created_at: Some(base_time() - Duration::seconds(index as i64 / 2)),
                ..SeedSandbox::default()
            },
        )
        .await?;
    }
    for org_id in [None, Some(first.org_id), Some(second.org_id)] {
        for limit in [1, 3, 1000] {
            let legacy: Vec<SandboxId> = sqlx::query_scalar(sql!(
                "SELECT s.id
                 FROM sandboxes s
                 WHERE ($2::uuid IS NULL OR s.org_id = $2)
                 ORDER BY s.created_at DESC, s.id DESC
                 LIMIT $1"
            ))
            .bind(i64::from(limit))
            .bind(org_id)
            .fetch_all(&db.pool)
            .await?;
            let filter = AdminSandboxFilter {
                org_id,
                ..AdminSandboxFilter::default()
            };
            let listed = controller(&db.pool)
                .admin_sandbox_page(&db.pool, &filter, None, limit, PageBudget::Unbounded)
                .await?;
            let listed = listed
                .rows
                .iter()
                .map(|sandbox| sandbox.id)
                .collect::<Vec<_>>();
            assert_eq!(listed, legacy, "org {org_id:?}, limit {limit}");
        }
    }
    db.pool.close().await;
    Ok(())
}

fn fixed_id(value: &str) -> SandboxId {
    SandboxId::from_uuid(Uuid::parse_str(value).expect("fixture uuid"))
}

/// Each filter, alone and combined, returns exactly the rows it describes.
#[tokio::test]
async fn each_filter_returns_exactly_its_rows() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let first_org = seed_org(&db.pool).await?;
    let first_user = seed_user(&db.pool, first_org).await?;
    let second_user = seed_user(&db.pool, first_org).await?;
    let other_org = seed_owner(&db.pool).await?;
    let (node_one, node_two) = (NodeId::new(), NodeId::new());
    let at = |minutes: i64| base_time() + Duration::minutes(minutes);
    let rows = [
        (
            first_user,
            SeedSandbox {
                id: Some(fixed_id("10000000-0000-7000-8000-000000000000")),
                name: Some("alpha-one"),
                lifecycle: Some("started"),
                template: Some("py"),
                active_node_id: Some(node_one),
                created_at: Some(at(0)),
            },
        ),
        (
            first_user,
            SeedSandbox {
                id: Some(fixed_id("10000000-0000-7000-8000-000000000001")),
                name: Some("alpha-two"),
                lifecycle: Some("paused"),
                template: Some("py"),
                active_node_id: Some(node_one),
                created_at: Some(at(1)),
            },
        ),
        (
            second_user,
            SeedSandbox {
                id: Some(fixed_id("2abc0000-0000-7000-8000-000000000000")),
                name: Some("beta"),
                lifecycle: Some("suspended"),
                template: Some("node"),
                active_node_id: None,
                created_at: Some(at(2)),
            },
        ),
        (
            first_user,
            SeedSandbox {
                id: Some(fixed_id("30000000-0000-7000-8000-000000000000")),
                name: None,
                lifecycle: Some("deleted"),
                template: Some("py"),
                active_node_id: None,
                created_at: Some(at(3)),
            },
        ),
        (
            other_org,
            SeedSandbox {
                id: Some(fixed_id("40000000-0000-7000-8000-000000000000")),
                name: Some("gamma"),
                lifecycle: Some("started"),
                template: None,
                active_node_id: Some(node_one),
                created_at: Some(at(4)),
            },
        ),
        (
            second_user,
            SeedSandbox {
                id: Some(fixed_id("50000000-0000-7000-8000-000000000000")),
                name: Some("Alpha-caps"),
                lifecycle: Some("cleanup_required"),
                template: Some("node"),
                active_node_id: Some(node_two),
                created_at: Some(at(5)),
            },
        ),
    ];
    let mut ids = Vec::new();
    for (owner, seed) in rows {
        ids.push(seed_sandbox(&db.pool, owner, seed).await?);
    }
    let lifecycles = |values: &[&str]| {
        Some(
            values
                .iter()
                .map(|value| value.parse().expect("stored lifecycle"))
                .collect(),
        )
    };
    let search = |prefix: &str| Some(SandboxSearch::new(prefix.to_string()));
    let template = |name: &str| Some(TemplateName::parse(name).expect("template name"));
    let cases: Vec<(&str, AdminSandboxFilter, &[usize])> = vec![
        (
            "no filter",
            AdminSandboxFilter::default(),
            &[0, 1, 2, 3, 4, 5],
        ),
        (
            "org",
            AdminSandboxFilter {
                org_id: Some(other_org.org_id),
                ..AdminSandboxFilter::default()
            },
            &[4],
        ),
        (
            "one lifecycle",
            AdminSandboxFilter {
                lifecycles: lifecycles(&["started"]),
                ..AdminSandboxFilter::default()
            },
            &[0, 4],
        ),
        (
            "two lifecycles",
            AdminSandboxFilter {
                lifecycles: lifecycles(&["paused", "suspended"]),
                ..AdminSandboxFilter::default()
            },
            &[1, 2],
        ),
        (
            "tombstones",
            AdminSandboxFilter {
                lifecycles: lifecycles(&["deleted", "cleanup_required"]),
                ..AdminSandboxFilter::default()
            },
            &[3, 5],
        ),
        (
            "template",
            AdminSandboxFilter {
                template: template("py"),
                ..AdminSandboxFilter::default()
            },
            &[0, 1, 3],
        ),
        (
            "node",
            AdminSandboxFilter {
                node: Some(NodeAssignment::Node(node_one)),
                ..AdminSandboxFilter::default()
            },
            &[0, 1, 4],
        ),
        (
            "assigned",
            AdminSandboxFilter {
                node: Some(NodeAssignment::Assigned),
                ..AdminSandboxFilter::default()
            },
            &[0, 1, 4, 5],
        ),
        (
            "unassigned",
            AdminSandboxFilter {
                node: Some(NodeAssignment::Unassigned),
                ..AdminSandboxFilter::default()
            },
            &[2, 3],
        ),
        (
            "user",
            AdminSandboxFilter {
                user_id: Some(first_user.user_id),
                ..AdminSandboxFilter::default()
            },
            &[0, 1, 3],
        ),
        (
            "created after, exclusive",
            AdminSandboxFilter {
                created_after: Some(at(2)),
                ..AdminSandboxFilter::default()
            },
            &[3, 4, 5],
        ),
        (
            "created before, exclusive",
            AdminSandboxFilter {
                created_before: Some(at(2)),
                ..AdminSandboxFilter::default()
            },
            &[0, 1],
        ),
        (
            "created between",
            AdminSandboxFilter {
                created_after: Some(at(0)),
                created_before: Some(at(3)),
                ..AdminSandboxFilter::default()
            },
            &[1, 2],
        ),
        (
            "name prefix is case-sensitive",
            AdminSandboxFilter {
                search: search("alpha"),
                ..AdminSandboxFilter::default()
            },
            &[0, 1],
        ),
        (
            "public id prefix",
            AdminSandboxFilter {
                search: search("sbx_2abc"),
                ..AdminSandboxFilter::default()
            },
            &[2],
        ),
        (
            "dashed id prefix in upper case",
            AdminSandboxFilter {
                search: search("2ABC0000-00"),
                ..AdminSandboxFilter::default()
            },
            &[2],
        ),
        (
            "undashed id prefix",
            AdminSandboxFilter {
                search: search("100000000000700080000000000000"),
                ..AdminSandboxFilter::default()
            },
            &[0, 1],
        ),
        (
            "whole id",
            AdminSandboxFilter {
                search: search("10000000-0000-7000-8000-000000000001"),
                ..AdminSandboxFilter::default()
            },
            &[1],
        ),
        (
            "no match",
            AdminSandboxFilter {
                search: search("zeta"),
                ..AdminSandboxFilter::default()
            },
            &[],
        ),
        // Without its node filter the unassigned case lists rows 0, 1 and 3,
        // and the on-a-node case rows 1 and 3, so the node filter decides each
        // result. On a node `created_after` decides it too (rows 0 and 1
        // without it), so a statement that binds it in another timestamp's
        // place lists other rows.
        (
            "combined, unassigned",
            AdminSandboxFilter {
                org_id: Some(first_org),
                lifecycles: lifecycles(&["started", "paused", "deleted"]),
                template: template("py"),
                node: Some(NodeAssignment::Unassigned),
                user_id: Some(first_user.user_id),
                ..AdminSandboxFilter::default()
            },
            &[3],
        ),
        (
            "combined, on a node",
            AdminSandboxFilter {
                org_id: Some(first_org),
                lifecycles: lifecycles(&["started", "paused", "deleted"]),
                template: template("py"),
                node: Some(NodeAssignment::Node(node_one)),
                user_id: Some(first_user.user_id),
                created_after: Some(at(0)),
                ..AdminSandboxFilter::default()
            },
            &[1],
        ),
    ];
    for (case, filter, expected) in cases {
        // Newest first, so the expected rows in reverse seed order.
        let expected = expected
            .iter()
            .rev()
            .map(|index| ids[*index])
            .collect::<Vec<_>>();
        let listed = listed_ids(&db.pool, &filter).await?;
        assert_eq!(listed, expected, "{case}: page");
        let unpaged = unpaged_ids(&db.pool, &filter).await?;
        assert_eq!(unpaged, expected, "{case}: unpaged list");
    }
    db.pool.close().await;
    Ok(())
}

/// The node filters match the node the response reports: a sandbox with a
/// current Direct route is on its route's node, whatever its row's
/// `active_node_id` says, and any other sandbox is on its row's node. Paging
/// keeps them, cursor and all.
#[tokio::test]
async fn the_node_filters_match_the_node_the_response_reports() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let owner = seed_owner(&db.pool).await?;
    let (row_node, route_node) = (NodeId::new(), NodeId::new());
    let at = |minutes: i64| base_time() + Duration::minutes(minutes);
    // (substrate, row's active node, route's node), oldest first.
    let rows = [
        (SubstrateKind::Kubernetes, Some(row_node), None),
        (SubstrateKind::Space, Some(row_node), Some(route_node)),
        (SubstrateKind::Space, None, Some(route_node)),
        (SubstrateKind::Space, Some(row_node), None),
        (SubstrateKind::Space, None, None),
        (SubstrateKind::Kubernetes, None, None),
        (SubstrateKind::Space, Some(route_node), Some(row_node)),
    ];
    let mut ids = Vec::new();
    for (minutes, (substrate, active_node_id, route)) in (0..).zip(rows) {
        let sandbox_id = seed_sandbox(
            &db.pool,
            owner,
            SeedSandbox {
                active_node_id,
                created_at: Some(at(minutes)),
                ..SeedSandbox::default()
            },
        )
        .await?;
        sqlx::query(sql!("UPDATE sandboxes SET managed_by = $2 WHERE id = $1"))
            .bind(sandbox_id)
            .bind(substrate)
            .execute(&db.pool)
            .await?;
        if let Some(route_node) = route {
            seed_route(&db.pool, sandbox_id, route_node).await?;
        }
        ids.push(sandbox_id);
    }
    let newest_first =
        |indexes: &[usize]| indexes.iter().rev().map(|i| ids[*i]).collect::<Vec<_>>();
    let filter = |node| AdminSandboxFilter {
        org_id: Some(owner.org_id),
        node: Some(node),
        ..AdminSandboxFilter::default()
    };
    let cases = [
        (
            "route node",
            NodeAssignment::Node(route_node),
            newest_first(&[1, 2]),
        ),
        (
            "row node",
            NodeAssignment::Node(row_node),
            newest_first(&[0, 3, 6]),
        ),
        (
            "assigned",
            NodeAssignment::Assigned,
            newest_first(&[0, 1, 2, 3, 6]),
        ),
        (
            "unassigned",
            NodeAssignment::Unassigned,
            newest_first(&[4, 5]),
        ),
    ];
    let everything = page(
        &db.pool,
        &AdminSandboxFilter {
            org_id: Some(owner.org_id),
            ..AdminSandboxFilter::default()
        },
        None,
        100,
    )
    .await?;
    for (label, node, expected) in cases {
        let listed = page(&db.pool, &filter(node), None, 100).await?;
        assert_eq!(
            listed
                .rows
                .iter()
                .map(|sandbox| sandbox.id)
                .collect::<Vec<_>>(),
            expected,
            "{label}"
        );
        if let NodeAssignment::Node(node_id) = node {
            // The independent oracle: the response's own `active_node_id`.
            let reported = everything
                .rows
                .iter()
                .filter(|sandbox| sandbox.active_node_id == Some(node_id))
                .map(|sandbox| sandbox.id)
                .collect::<Vec<_>>();
            assert_eq!(
                reported, expected,
                "{label}: the response reports that node"
            );
        }

        // One row a page, the cursor carrying the filter's position.
        let mut walked = Vec::new();
        let mut after = None;
        loop {
            let one = page(&db.pool, &filter(node), after, 1).await?;
            walked.extend(one.rows.iter().map(|sandbox| sandbox.id));
            match (one.has_more, one.rows.last()) {
                (true, Some(last)) => after = Some(cursor_after(last)),
                _ => break,
            }
        }
        assert_eq!(walked, expected, "{label}: walked a row at a time");

        // The unpaged list runs its own statement with the same filters.
        let unpaged = unpaged_ids(&db.pool, &filter(node)).await?;
        assert_eq!(unpaged, expected, "{label}: unpaged list");
    }
    // Never NULL, so negated it keeps the sandboxes on no node (4 and 5).
    let off_route_node: Vec<SandboxId> = sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(format!(
        "SELECT s.id FROM sandboxes s WHERE s.org_id = $1 AND NOT {}
         ORDER BY s.created_at DESC",
        reported_on_node("s", "$2")
    ))))
    .bind(owner.org_id)
    .bind(route_node)
    .fetch_all(&db.pool)
    .await?;
    assert_eq!(off_route_node, newest_first(&[0, 3, 4, 5, 6]));
    db.pool.close().await;
    Ok(())
}

/// The filter takes every value `sandboxes.lifecycle` holds, as the stored
/// vocabulary normalizes it, and nothing else.
#[test]
fn the_lifecycle_filter_takes_exactly_the_stored_lifecycles() {
    for stored in pplx_space_placement::StoredSandboxLifecycle::all() {
        let value = stored.as_str();
        for spelling in [
            value.to_string(),
            format!(" {} ", value.to_ascii_uppercase()),
        ] {
            assert_eq!(
                spelling
                    .parse::<AdminStoredLifecycle>()
                    .map(AdminStoredLifecycle::as_str),
                Ok(value),
                "{spelling:?}"
            );
            assert_eq!(
                spelling.parse::<pplx_space_placement::StoredSandboxLifecycle>(),
                Ok(*stored),
                "{spelling:?}"
            );
        }
    }
    for unknown in ["", "delete", "cleanup-required", "live"] {
        assert!(
            unknown.parse::<AdminStoredLifecycle>().is_err(),
            "{unknown:?}"
        );
        assert!(
            unknown
                .parse::<pplx_space_placement::StoredSandboxLifecycle>()
                .is_err(),
            "{unknown:?}"
        );
    }
}

#[test]
fn an_id_prefix_is_the_range_of_ids_it_starts() {
    let range = |low: &str, high: &str| Some((fixed_id(low), fixed_id(high)));
    let cases = [
        (
            "",
            range(
                "00000000-0000-0000-0000-000000000000",
                "ffffffff-ffff-ffff-ffff-ffffffffffff",
            ),
        ),
        (
            "sbx_AB",
            range(
                "ab000000-0000-0000-0000-000000000000",
                "abffffff-ffff-ffff-ffff-ffffffffffff",
            ),
        ),
        (
            "0123abcd-ef",
            range(
                "0123abcd-ef00-0000-0000-000000000000",
                "0123abcd-efff-ffff-ffff-ffffffffffff",
            ),
        ),
        (
            "0123abcdef",
            range(
                "0123abcd-ef00-0000-0000-000000000000",
                "0123abcd-efff-ffff-ffff-ffffffffffff",
            ),
        ),
        ("0123-abcd", None),
        ("0123abcd-ef-", None),
        ("sandbox", None),
        ("0123abcd-ef01-2345-6789-abcdef0123456", None),
        ("0123abcdef0123456789abcdef01234567", None),
    ];
    for (prefix, expected) in cases {
        assert_eq!(id_prefix_range(prefix), expected, "{prefix:?}");
    }
}

type Bucket = (
    Option<String>,
    Option<SemVer>,
    Option<String>,
    SandboxResourceType,
);

/// The tally the console draws from the list: every row whose status (else
/// lifecycle) is not `deleted`, by the fields the list response shows.
fn tally(responses: &[SandboxResponse]) -> BTreeMap<String, i64> {
    let mut buckets = BTreeMap::new();
    for response in responses {
        if response.status.as_deref().or(response.lifecycle.as_deref()) == Some("deleted") {
            continue;
        }
        let bucket: Bucket = (
            response.template.clone(),
            response.spaced_version.clone(),
            response.status.clone(),
            response.resource_type,
        );
        *buckets.entry(format!("{bucket:?}")).or_insert(0) += 1;
    }
    buckets
}

fn counted(counts: &[SpacedVersionCount]) -> BTreeMap<String, i64> {
    counts
        .iter()
        .map(|count| {
            let bucket: Bucket = (
                count.template.clone(),
                count.spaced_version.clone(),
                count.status.clone(),
                count.resource_type,
            );
            (format!("{bucket:?}"), count.count)
        })
        .collect()
}

/// The count is the console's tally of the full list: the same template
/// (including the node inventory's), version, status and resource type the
/// list response shows, the node-reported rows with no sandbox row in the
/// unscoped count only, and nothing whose status is `deleted`. The oracle is
/// the list's own projection (`list_responses`) over every listed row.
#[tokio::test]
async fn the_count_is_the_tally_of_the_full_list() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let owner = seed_owner(&db.pool).await?;
    let other = seed_owner(&db.pool).await?;
    let node_id = NodeId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    sqlx::query(sql!(
        "INSERT INTO templates (id, name) VALUES ($1, 'inventoried')"
    ))
    .bind(Uuid::now_v7())
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_template_inventory (node_id, template_name, content_hash)
         VALUES ($1, 'inventoried', 'sha256:inventoried')"
    ))
    .bind(node_id)
    .execute(&db.pool)
    .await?;
    let started = |spaced_version| SeedObservation {
        status: Some("started"),
        spaced_version,
        ..SeedObservation::default()
    };
    let rows: Vec<(
        Owner,
        Option<&'static str>,
        &'static str,
        Option<SeedObservation>,
    )> = vec![
        (owner, Some("py"), "started", Some(started(Some("1.2.0")))),
        (
            owner,
            Some("py"),
            "started",
            Some(SeedObservation {
                template: Some("ignored"),
                ..started(Some("1.2.0"))
            }),
        ),
        (owner, Some("py"), "suspended", None),
        (
            owner,
            None,
            "started",
            Some(SeedObservation {
                image_hash: Some("sha256:inventoried"),
                ..started(Some("1.3.0"))
            }),
        ),
        (
            owner,
            None,
            "started",
            Some(SeedObservation {
                template: Some("reported"),
                ..started(None)
            }),
        ),
        (owner, Some("py"), "deleted", None),
        // A tombstone whose VM its node still reports: the list shows it
        // `started`, so the console's tally counts it, and so must the count.
        (owner, Some("py"), "deleted", Some(started(Some("1.2.0")))),
        // The node's status wins over the stored lifecycle, as in the list.
        (
            owner,
            Some("py"),
            "cleanup_required",
            Some(started(Some("1.2.0"))),
        ),
        (
            owner,
            Some("py"),
            "deleted",
            Some(SeedObservation {
                status: Some("deleted"),
                ..SeedObservation::default()
            }),
        ),
        (other, Some("py"), "started", Some(started(Some("1.2.0")))),
    ];
    for (row_owner, template, lifecycle, observed) in rows {
        let id = seed_sandbox(
            &db.pool,
            row_owner,
            SeedSandbox {
                template,
                lifecycle: Some(lifecycle),
                ..SeedSandbox::default()
            },
        )
        .await?;
        if let Some(observed) = observed {
            seed_observation(&db.pool, id, node_id, observed).await?;
        }
    }
    // Rowless: a warm-pool slot and an observed-local sandbox.
    seed_observation(
        &db.pool,
        SandboxId::new(),
        node_id,
        SeedObservation {
            pool_template: Some("pooled"),
            ..started(Some("1.2.0"))
        },
    )
    .await?;
    seed_observation(
        &db.pool,
        SandboxId::new(),
        node_id,
        SeedObservation::default(),
    )
    .await?;

    let controller = controller(&db.pool);
    for org_id in [None, Some(owner.org_id), Some(other.org_id)] {
        let filter = AdminSandboxFilter {
            org_id,
            ..AdminSandboxFilter::default()
        };
        let listed = controller
            .admin_sandbox_page(&db.pool, &filter, None, 1000, PageBudget::Unbounded)
            .await?;
        let responses = controller
            .list_responses(&db.pool, &listed.rows, filter.is_unfiltered())
            .await
            .map_err(|error| format!("{error:?}"))?;
        let counts = controller
            .admin_spaced_version_counts(&db.pool, org_id)
            .await
            .map_err(|error| format!("{error:?}"))?;
        assert_eq!(counted(&counts), tally(&responses), "org {org_id:?}");
    }
    let everywhere = controller
        .admin_spaced_version_counts(&db.pool, None)
        .await
        .map_err(|error| format!("{error:?}"))?;
    let has = |template: &str, resource_type| {
        everywhere.iter().any(|count| {
            count.template.as_deref() == Some(template) && count.resource_type == resource_type
        })
    };
    assert!(has("inventoried", SandboxResourceType::Sandbox));
    assert!(has("pooled", SandboxResourceType::PoolSlot));
    assert!(
        everywhere
            .iter()
            .any(|count| count.resource_type == SandboxResourceType::ObservedLocal)
    );
    db.pool.close().await;
    Ok(())
}
