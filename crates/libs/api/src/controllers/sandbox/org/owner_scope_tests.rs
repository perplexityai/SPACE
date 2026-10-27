use pplx_space_common_core::types::{OrgId, SandboxId, UserId};

use pplx_space_common_core::types::{UpstreamIssuer, UpstreamOrgId, UpstreamSubjectId};
use pplx_space_db_test_fixtures::{
    SEED_ORG_ID as LOCAL_ORG_ID, SEED_USER_ID as LOCAL_USER_ID, TestDb, pg_test_db,
};

use super::*;

use pplx_space_test_env::TestResult;

const UPSTREAM_ORG_ID: &str = "example-org-not-a-space-uuid";
const UPSTREAM_SUBJECT_ID: &str = "123";

struct ScopeFixture {
    controller: SandboxController,
    db: TestDb,
}

async fn scope_fixture() -> TestResult<ScopeFixture> {
    let db = pg_test_db().await?;
    let controller = SandboxController::from_env(&db.pool).await?;
    Ok(ScopeFixture { controller, db })
}

async fn insert_local_sandbox(pool: &PgPool) -> TestResult<SandboxId> {
    let sandbox_id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (id, user_id, org_id, network_policy_generation)
         VALUES ($1, $2, $3, 1)"
    ))
    .bind(sandbox_id)
    .bind(LOCAL_USER_ID)
    .bind(LOCAL_ORG_ID)
    .execute(pool)
    .await?;
    Ok(sandbox_id)
}

async fn insert_federated_sandbox(pool: &PgPool, subject_id: &str) -> TestResult<SandboxId> {
    let proposed_org_id = OrgId::new();
    let sandbox_id = SandboxId::new();
    let (org_id,): (OrgId,) = sqlx::query_as(sql!(
        "INSERT INTO organizations (
             id, name, slug, owner_source, upstream_issuer, upstream_org_id
         )
         VALUES ($1, $2, $3, 'federated_identity', 'example-issuer', $4)
         ON CONFLICT (upstream_issuer, upstream_org_id)
             WHERE owner_source = 'federated_identity'
         DO UPDATE SET name = organizations.name
         RETURNING id"
    ))
    .bind(proposed_org_id)
    .bind(format!("Federated Org {UPSTREAM_ORG_ID}"))
    .bind(format!("federated-{proposed_org_id}"))
    .bind(UPSTREAM_ORG_ID)
    .fetch_one(pool)
    .await?;
    let proposed_user_id = UserId::new();
    let (user_id,): (UserId,) = sqlx::query_as(sql!(
        "INSERT INTO users (
             id, email, password_hash, display_name, org_id, role,
             owner_source, upstream_issuer, upstream_subject_id
         )
         VALUES (
             $1, $2, 'federated-user-no-local-password', $3, $4, 'member',
             'federated_identity', 'example-issuer', $5
         )
         ON CONFLICT (org_id, upstream_issuer, upstream_subject_id)
             WHERE owner_source = 'federated_identity'
         DO UPDATE SET display_name = users.display_name
         RETURNING id"
    ))
    .bind(proposed_user_id)
    .bind(format!("federated-{proposed_user_id}@space.invalid"))
    .bind(format!("Federated User {subject_id}"))
    .bind(org_id)
    .bind(subject_id)
    .fetch_one(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandboxes (id, user_id, org_id, network_policy_generation)
         VALUES ($1, $2, $3, 1)"
    ))
    .bind(sandbox_id)
    .bind(user_id)
    .bind(org_id)
    .execute(pool)
    .await?;
    Ok(sandbox_id)
}

fn local_scope() -> SandboxAccessScope {
    SandboxAccessScope::LocalOrg {
        org_id: OrgId::from_uuid(LOCAL_ORG_ID),
    }
}

/// Federated principal whose API key belongs to the seed org.
fn federated_scope(subject_id: &str) -> SandboxAccessScope {
    SandboxAccessScope::FederatedSubject {
        owner: FederatedOwner {
            issuer: UpstreamIssuer::parse("example-issuer").unwrap(),
            org_id: UpstreamOrgId::parse(UPSTREAM_ORG_ID).unwrap(),
            subject_id: UpstreamSubjectId::parse(subject_id).unwrap(),
        },
        legacy_org_id: OrgId::from_uuid(LOCAL_ORG_ID),
    }
}

async fn insert_local_sandbox_in_other_org(pool: &PgPool) -> TestResult<SandboxId> {
    let org_id = OrgId::new();
    sqlx::query(sql!(
        "INSERT INTO organizations (id, name, slug) VALUES ($1, $2, $3)"
    ))
    .bind(org_id)
    .bind(format!("Other Org {org_id}"))
    .bind(format!("other-{org_id}"))
    .execute(pool)
    .await?;
    let user_id = UserId::new();
    sqlx::query(sql!(
        "INSERT INTO users (id, email, password_hash, display_name, org_id, role)
         VALUES ($1, $2, 'other-user-no-password', 'Other User', $3, 'member')"
    ))
    .bind(user_id)
    .bind(format!("other-{user_id}@space.invalid"))
    .bind(org_id)
    .execute(pool)
    .await?;
    let sandbox_id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (id, user_id, org_id, network_policy_generation)
         VALUES ($1, $2, $3, 1)"
    ))
    .bind(sandbox_id)
    .bind(user_id)
    .bind(org_id)
    .execute(pool)
    .await?;
    Ok(sandbox_id)
}

#[tokio::test]
async fn scoped_sandbox_resolves_owner_rows_and_key_org_legacy_rows() -> TestResult {
    let fixture = scope_fixture().await?;
    let local_id = insert_local_sandbox(&fixture.db.pool).await?;
    let other_org_local_id = insert_local_sandbox_in_other_org(&fixture.db.pool).await?;
    let federated_id = insert_federated_sandbox(&fixture.db.pool, UPSTREAM_SUBJECT_ID).await?;

    let local = &local_scope();
    let federated = &federated_scope(UPSTREAM_SUBJECT_ID);
    let other_subject = &federated_scope("456");

    // Local org sees only its local row.
    assert!(
        fixture
            .controller
            .scoped_sandbox(&fixture.db.pool, local, local_id)
            .await?
            .is_some()
    );
    assert!(
        fixture
            .controller
            .scoped_sandbox(&fixture.db.pool, local, federated_id)
            .await?
            .is_none()
    );
    // The owning federated subject sees its row; other subjects do not.
    assert!(
        fixture
            .controller
            .scoped_sandbox(&fixture.db.pool, federated, federated_id)
            .await?
            .is_some()
    );
    assert!(
        fixture
            .controller
            .scoped_sandbox(&fixture.db.pool, other_subject, federated_id)
            .await?
            .is_none()
    );
    // Federated subjects reach the key org's legacy local rows, not other orgs'.
    assert!(
        fixture
            .controller
            .scoped_sandbox(&fixture.db.pool, federated, local_id)
            .await?
            .is_some()
    );
    assert!(
        fixture
            .controller
            .scoped_sandbox(&fixture.db.pool, federated, other_org_local_id)
            .await?
            .is_none()
    );
    Ok(())
}

#[tokio::test]
async fn scoped_sandbox_page_filters_by_owner() -> TestResult {
    let fixture = scope_fixture().await?;
    let local_id = insert_local_sandbox(&fixture.db.pool).await?;
    let federated_id = insert_federated_sandbox(&fixture.db.pool, UPSTREAM_SUBJECT_ID).await?;
    let other_federated_id = insert_federated_sandbox(&fixture.db.pool, "456").await?;
    let other_org_local_id = insert_local_sandbox_in_other_org(&fixture.db.pool).await?;

    let local_page = fixture
        .controller
        .scoped_sandbox_page(&fixture.db.pool, &local_scope(), None, 10, None)
        .await?;
    let federated_page = fixture
        .controller
        .scoped_sandbox_page(
            &fixture.db.pool,
            &federated_scope(UPSTREAM_SUBJECT_ID),
            None,
            10,
            None,
        )
        .await?;

    let local_ids: Vec<SandboxId> = local_page.rows.iter().map(|sandbox| sandbox.id).collect();
    let federated_ids: Vec<SandboxId> = federated_page
        .rows
        .iter()
        .map(|sandbox| sandbox.id)
        .collect();
    assert!(local_ids.contains(&local_id));
    assert!(!local_ids.contains(&federated_id));
    // Own federated rows plus the key org's legacy rows, merged newest-first.
    assert!(federated_ids.contains(&federated_id));
    assert!(federated_ids.contains(&local_id));
    assert!(!federated_ids.contains(&other_federated_id));
    assert!(!federated_ids.contains(&other_org_local_id));
    let keys: Vec<_> = federated_page
        .rows
        .iter()
        .map(|sandbox| (sandbox.created_at, sandbox.id))
        .collect();
    assert!(keys.windows(2).all(|pair| pair[0] > pair[1]));

    // The cursor applies to both arms: the newest visible row is the federated
    // one, and the page after it still reaches the legacy row.
    let first = fixture
        .controller
        .scoped_sandbox_page(
            &fixture.db.pool,
            &federated_scope(UPSTREAM_SUBJECT_ID),
            None,
            1,
            None,
        )
        .await?;
    assert_eq!(first.rows[0].id, federated_id);
    assert!(first.has_more);
    let cursor = SandboxPageCursor {
        created_at: first.rows[0].created_at,
        id: first.rows[0].id,
    };
    let rest: Vec<SandboxId> = fixture
        .controller
        .scoped_sandbox_page(
            &fixture.db.pool,
            &federated_scope(UPSTREAM_SUBJECT_ID),
            Some(cursor),
            10,
            None,
        )
        .await?
        .rows
        .iter()
        .map(|sandbox| sandbox.id)
        .collect();
    assert!(rest.contains(&local_id));
    assert!(!rest.contains(&federated_id));
    Ok(())
}

/// Moves a seeded row to `created_at` and gives it `bytes` of metadata or,
/// with `in_env`, of env, as rows ECW created without size limits can hold.
async fn set_listed_row(
    pool: &PgPool,
    sandbox_id: SandboxId,
    created_at: time::OffsetDateTime,
    bytes: usize,
    in_env: bool,
) -> TestResult {
    let (metadata, env) = if in_env {
        (
            serde_json::json!({}),
            serde_json::json!({ "LEGACY": "e".repeat(bytes) }),
        )
    } else {
        (
            serde_json::json!({ "legacy": "m".repeat(bytes) }),
            serde_json::json!({}),
        )
    };
    sqlx::query(sql!(
        "UPDATE sandboxes SET created_at = $2, metadata = $3, env = $4 WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(created_at)
    .bind(sqlx::types::Json(metadata))
    .bind(sqlx::types::Json(env))
    .execute(pool)
    .await?;
    Ok(())
}

/// Follows an ECW list from the first page to the last, as the route does:
/// the cursor is the last row of a page that has more. Returns each page's
/// sandbox IDs, and checks that a page of more than one row stays within the
/// byte budget.
async fn walk_scoped_pages(
    fixture: &ScopeFixture,
    scope: &SandboxAccessScope,
) -> TestResult<Vec<Vec<SandboxId>>> {
    let mut pages = Vec::new();
    let mut cursor = None;
    loop {
        let page = fixture
            .controller
            .scoped_sandbox_page(&fixture.db.pool, scope, cursor, 1000, None)
            .await?;
        let row_bytes = page
            .rows
            .iter()
            .map(|sandbox| {
                serde_json::to_vec(&sandbox.metadata).unwrap().len()
                    + serde_json::to_vec(&sandbox.env).unwrap().len()
            })
            .sum::<usize>();
        if page.rows.len() > 1 {
            assert!(
                row_bytes <= MAX_SANDBOX_LIST_PAGE_BYTES as usize,
                "{row_bytes}"
            );
        }
        pages.push(page.rows.iter().map(|sandbox| sandbox.id).collect());
        assert!(pages.len() <= 16, "the cursor stopped advancing");
        if !page.has_more {
            return Ok(pages);
        }
        let last = page.rows.last().expect("a page with more is not empty");
        cursor = Some(SandboxPageCursor {
            created_at: last.created_at,
            id: last.id,
        });
    }
}

#[tokio::test]
async fn scoped_sandbox_page_bounds_the_bytes_it_returns() -> TestResult {
    const MIB: usize = 1024 * 1024;
    let fixture = scope_fixture().await?;
    let newest = time::OffsetDateTime::from_unix_timestamp(1_800_000_000)?;
    // Newest first: one row over the whole budget, twelve 1 MiB rows, then
    // three small ones, alternating between a legacy local row and the
    // federated subject's own row so the federated walk crosses both arms.
    // The federated rows carry their bytes in env, which the page holds too.
    let sizes = std::iter::once(9 * MIB)
        .chain(std::iter::repeat_n(MIB, 12))
        .chain(std::iter::repeat_n(16, 3));
    let mut all = Vec::new();
    let mut local = Vec::new();
    for (offset, size) in sizes.enumerate() {
        let sandbox_id = if offset % 2 == 0 {
            let sandbox_id = insert_local_sandbox(&fixture.db.pool).await?;
            local.push(sandbox_id);
            sandbox_id
        } else {
            insert_federated_sandbox(&fixture.db.pool, UPSTREAM_SUBJECT_ID).await?
        };
        let created_at = newest - time::Duration::seconds(offset as i64);
        set_listed_row(
            &fixture.db.pool,
            sandbox_id,
            created_at,
            size,
            offset % 2 == 1,
        )
        .await?;
        all.push(sandbox_id);
    }

    // The oversized row comes back alone; 1 MiB rows fit seven to a page.
    let federated = walk_scoped_pages(&fixture, &federated_scope(UPSTREAM_SUBJECT_ID)).await?;
    assert_eq!(
        federated.iter().map(Vec::len).collect::<Vec<_>>(),
        vec![1, 7, 8]
    );
    assert_eq!(federated.concat(), all);
    // The local rows are the oversized one, six 1 MiB rows and one small.
    let local_pages = walk_scoped_pages(&fixture, &local_scope()).await?;
    assert_eq!(
        local_pages.iter().map(Vec::len).collect::<Vec<_>>(),
        vec![1, 7]
    );
    assert_eq!(local_pages.concat(), local);
    Ok(())
}
