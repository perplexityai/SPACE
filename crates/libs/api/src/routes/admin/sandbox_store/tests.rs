use pplx_space_common_core::types::SubstrateKind;
use pplx_space_db_test_fixtures::{TestDbResult, insert_ready_worker_node, pg_test_db};
use sqlx::PgPool;
use time::Duration;

use super::*;

/// An org and one of its users, for seeded sandboxes to belong to.
#[derive(Debug, Clone, Copy)]
pub(in crate::routes::admin) struct Owner {
    pub(in crate::routes::admin) org_id: OrgId,
    pub(in crate::routes::admin) user_id: UserId,
}

pub(in crate::routes::admin) async fn seed_org(pool: &PgPool) -> TestDbResult<OrgId> {
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

pub(in crate::routes::admin) async fn seed_user(
    pool: &PgPool,
    org_id: OrgId,
) -> TestDbResult<Owner> {
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

pub(in crate::routes::admin) async fn seed_owner(pool: &PgPool) -> TestDbResult<Owner> {
    let org_id = seed_org(pool).await?;
    seed_user(pool, org_id).await
}

/// A sandbox row to seed; unset fields take the column defaults.
#[derive(Debug, Clone, Default)]
pub(in crate::routes::admin) struct SeedSandbox {
    pub(in crate::routes::admin) id: Option<SandboxId>,
    pub(in crate::routes::admin) name: Option<&'static str>,
    pub(in crate::routes::admin) lifecycle: Option<&'static str>,
    pub(in crate::routes::admin) template: Option<&'static str>,
    pub(in crate::routes::admin) active_node_id: Option<NodeId>,
    pub(in crate::routes::admin) created_at: Option<OffsetDateTime>,
}

pub(in crate::routes::admin) async fn seed_sandbox(
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

/// What a node reports for `sandbox_id`, which need not have a sandbox row.
pub(in crate::routes::admin) async fn seed_observation(
    pool: &PgPool,
    sandbox_id: SandboxId,
    node_id: NodeId,
    template: Option<&str>,
    spaced_version: Option<&str>,
) -> TestDbResult<()> {
    sqlx::query(sql!(
        "INSERT INTO sandbox_observed_states (sandbox_id, node_id, status, template, spaced_version)
         VALUES ($1, $2, 'started', $3, $4)"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .bind(template)
    .bind(spaced_version)
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
/// `created_at` straddle page boundaries and new rows land between pages.
#[tokio::test]
async fn pages_return_every_row_once_while_rows_are_inserted() -> TestDbResult<()> {
    const PAGE: usize = 2;
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
        let mut page = list_sandboxes(&db.pool, &filter, after, PAGE as i64 + 1).await?;
        seed_sandbox(&db.pool, owner, SeedSandbox::default()).await?;
        let more = page.len() > PAGE;
        page.truncate(PAGE);
        seen.extend(page.iter().map(|sandbox| sandbox.id));
        if !more {
            break;
        }
        after = page.last().map(cursor_after);
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
            .bind(limit)
            .bind(org_id)
            .fetch_all(&db.pool)
            .await?;
            let filter = AdminSandboxFilter {
                org_id,
                ..AdminSandboxFilter::default()
            };
            let listed = list_sandboxes(&db.pool, &filter, None, limit).await?;
            let listed = listed.iter().map(|sandbox| sandbox.id).collect::<Vec<_>>();
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
                active_node_id: Some(node_two),
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
            &[0, 4],
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
        (
            "combined",
            AdminSandboxFilter {
                org_id: Some(first_org),
                lifecycles: lifecycles(&["started", "deleted"]),
                template: template("py"),
                node: Some(NodeAssignment::Unassigned),
                ..AdminSandboxFilter::default()
            },
            &[3],
        ),
    ];
    for (case, filter, expected) in cases {
        let listed = list_sandboxes(&db.pool, &filter, None, 100).await?;
        let listed = listed.iter().map(|sandbox| sandbox.id).collect::<Vec<_>>();
        // Newest first, so the expected rows in reverse seed order.
        let expected = expected
            .iter()
            .rev()
            .map(|index| ids[*index])
            .collect::<Vec<_>>();
        assert_eq!(listed, expected, "{case}");
    }
    db.pool.close().await;
    Ok(())
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

/// A node report seeded with a row: its template and `spaced` version.
type SeedObservation = (Option<&'static str>, &'static str);

/// The count groups every sandbox that is not deleted by template, reported
/// `spaced` version and stored lifecycle, and narrows to one org on request.
#[tokio::test]
async fn sandboxes_count_by_template_version_and_lifecycle() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let owner = seed_owner(&db.pool).await?;
    let other = seed_owner(&db.pool).await?;
    let node_id = NodeId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    let rows: [(Owner, Option<&str>, &str, Option<SeedObservation>); 6] = [
        (owner, Some("py"), "started", Some((None, "1.2.0"))),
        (
            owner,
            Some("py"),
            "started",
            Some((Some("ignored"), "1.2.0")),
        ),
        (owner, Some("py"), "suspended", None),
        (owner, None, "started", Some((Some("observed"), "1.3.0"))),
        (owner, Some("py"), "deleted", Some((None, "1.2.0"))),
        (other, Some("py"), "started", Some((None, "1.2.0"))),
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
        if let Some((observed_template, version)) = observed {
            seed_observation(&db.pool, id, node_id, observed_template, Some(version)).await?;
        }
    }
    let count =
        |template: &str, version: Option<&str>, lifecycle: &str, count| SpacedVersionCount {
            template: Some(template.to_string()),
            spaced_version: version.map(str::to_string),
            lifecycle: lifecycle.to_string(),
            count,
        };
    let everywhere = count_by_spaced_version(&db.pool, None).await?;
    assert_eq!(
        everywhere,
        [
            count("observed", Some("1.3.0"), "started", 1),
            count("py", Some("1.2.0"), "started", 3),
            count("py", None, "suspended", 1),
        ]
    );
    let in_other = count_by_spaced_version(&db.pool, Some(other.org_id)).await?;
    assert_eq!(in_other, [count("py", Some("1.2.0"), "started", 1)]);
    let none_seen = count_by_spaced_version(&db.pool, Some(seed_org(&db.pool).await?)).await?;
    assert!(none_seen.is_empty());
    db.pool.close().await;
    Ok(())
}
