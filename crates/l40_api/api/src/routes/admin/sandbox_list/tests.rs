use axum::Router;
use axum::body::Body;
use axum::http::{Method, Request};
use pplx_space_common_core::sql;
use pplx_space_common_core::types::NodeId;
use pplx_space_db_test_fixtures::{TestDb, TestDbResult, insert_ready_worker_node, pg_test_db};
use pplx_space_public_api::PublicSandboxId;
use serde_json::{Value, json};
use sqlx::PgPool;
use tower::ServiceExt;
use uuid::Uuid;

use super::*;
use crate::controllers::sandbox::list_page::admin::tests::{
    SeedObservation, SeedSandbox, seed_observation, seed_owner, seed_sandbox,
};

const SEED_ORG: Uuid = pplx_space_db_test_fixtures::SEED_ORG_ID;

// ---------------------------------------------------------------------------
// Route tests: the real admin router, session auth included, against a
// migrated database.

struct Harness {
    _db: TestDb,
    pool: PgPool,
    app: Router,
    admin_token: String,
    member_token: String,
}

async fn insert_session_user(pool: &PgPool, role: &str) -> TestDbResult<String> {
    let user_id = UserId::new();
    sqlx::query(sql!(
        "INSERT INTO users (id, email, password_hash, display_name, org_id, role)
         VALUES ($1, $2, 'synthetic-password-hash', 'sandbox list tests', $3, $4)"
    ))
    .bind(user_id)
    .bind(format!("{user_id}@example.invalid"))
    .bind(OrgId::from_uuid(SEED_ORG))
    .bind(role)
    .execute(pool)
    .await?;
    let (token, token_hash) = crate::auth::generate_session_token();
    sqlx::query(sql!(
        "INSERT INTO sessions (user_id, token_hash, expires_at)
         VALUES ($1, $2, NOW() + INTERVAL '1 hour')"
    ))
    .bind(user_id)
    .bind(token_hash)
    .execute(pool)
    .await?;
    Ok(token)
}

/// `None` in the parent of the child process that pins the environment
/// `AppState::from_pool` reads; only the child, which runs the test, needs
/// a database.
async fn harness() -> TestDbResult<Option<Harness>> {
    // `AppState::from_pool` picks its substrate from the environment; keep it
    // Direct so construction never reaches a cluster.
    if !pplx_space_test_env::enter_test_process(&[(
        "SPACE_K8S_CONTROL_PLANE",
        Some(std::ffi::OsStr::new("false")),
    )]) {
        return Ok(None);
    }
    let db = pg_test_db().await?;
    let pool = db.pool.clone();
    let admin_token = insert_session_user(&pool, "organization_admin").await?;
    let member_token = insert_session_user(&pool, "member").await?;
    let state = AppState::from_pool(pool.clone()).await?;
    let app = crate::routes::admin::shared_host_router(state.clone()).with_state(state);
    Ok(Some(Harness {
        _db: db,
        pool,
        app,
        admin_token,
        member_token,
    }))
}

impl Harness {
    async fn request(
        &self,
        token: &str,
        method: Method,
        path: &str,
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        let request = Request::builder()
            .method(method)
            .uri(format!("/admin/api/v1{path}"))
            .header("authorization", format!("Bearer {token}"))
            .header("content-type", "application/json")
            .header("idempotency-key", Uuid::now_v7().to_string())
            .body(body.map_or_else(Body::empty, |body| Body::from(body.to_string())))
            .expect("request");
        let response = self.app.clone().oneshot(request).await.expect("response");
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let value = serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned()));
        (status, value)
    }

    async fn get(&self, path: &str) -> (StatusCode, Value) {
        self.request(&self.admin_token, Method::GET, path, None)
            .await
    }
}

fn ids(rows: &Value) -> Vec<String> {
    rows.as_array()
        .expect("sandbox array")
        .iter()
        .map(|row| row["id"].as_str().expect("id").to_string())
        .collect()
}

/// Which part of the list [`the_list_pages_by_cursor_and_keeps_the_unpaged_array`]
/// reads: a scope it walks by cursor, or the filters.
#[derive(Clone, Copy, Debug)]
enum ListView {
    OrgScoped,
    Unscoped,
    Filtered,
}

/// Without `cursor` the list is the bare array it always was. With one, it
/// is a page with `next_cursor`, and following the cursors returns every row
/// once. The node-reported rows with no sandbox row belong to no org: they
/// come on the first page of the unfiltered list only, never under `org_id`
/// or another filter.
#[rstest::rstest]
#[tokio::test]
async fn the_list_pages_by_cursor_and_keeps_the_unpaged_array(
    #[values(ListView::OrgScoped, ListView::Unscoped, ListView::Filtered)] view: ListView,
) -> TestDbResult<()> {
    let Some(harness) = harness().await? else {
        return Ok(());
    };
    let owner = seed_owner(&harness.pool).await?;
    let mut seeded = Vec::new();
    for _ in 0..5 {
        seeded.push(seed_sandbox(&harness.pool, owner, SeedSandbox::default()).await?);
    }
    seeded.reverse();
    let seeded = seeded.iter().map(ToString::to_string).collect::<Vec<_>>();
    let node_id = NodeId::new();
    insert_ready_worker_node(&harness.pool, node_id).await?;
    let node_only = SandboxId::new().to_string();
    seed_observation(
        &harness.pool,
        node_only.parse().expect("sandbox id"),
        node_id,
        SeedObservation {
            status: Some("started"),
            pool_template: Some("py"),
            spaced_version: Some("1.0.0"),
            ..SeedObservation::default()
        },
    )
    .await?;
    let with_node_only = |rows: &[String]| {
        let mut rows = rows.to_vec();
        rows.push(node_only.clone());
        rows
    };
    let org = owner.org_id.as_uuid();

    let (scope, first_page, walk) = match view {
        ListView::OrgScoped => (
            format!("org_id={org}&"),
            seeded[..2].to_vec(),
            seeded.clone(),
        ),
        ListView::Unscoped => (String::new(), with_node_only(&seeded[..2]), {
            let mut walk = with_node_only(&seeded[..2]);
            walk.extend_from_slice(&seeded[2..]);
            walk
        }),
        ListView::Filtered => {
            let (status, filtered) = harness.get("/sandboxes?lifecycle=started&cursor=").await;
            assert_eq!(status, StatusCode::OK, "{filtered}");
            assert_eq!(ids(&filtered["sandboxes"]), seeded);
            assert_eq!(filtered["next_cursor"], Value::Null);

            // The node filters reach the list, and they and `prefix` leave out the
            // node-reported row as every filter does, even its own ID's prefix.
            for (label, query, expected) in [
                ("node_id", format!("node_id={node_id}"), Vec::new()),
                ("prefix", format!("prefix={node_only}"), Vec::new()),
                ("assigned", "assigned=true".to_string(), Vec::new()),
                ("unassigned", "assigned=false".to_string(), seeded.clone()),
            ] {
                let (status, unpaged) = harness.get(&format!("/sandboxes?{query}")).await;
                assert_eq!(status, StatusCode::OK, "{label}: unpaged list status");
                assert_eq!(ids(&unpaged), expected, "{label}: unpaged list");
                let (status, paged) = harness.get(&format!("/sandboxes?{query}&cursor=")).await;
                assert_eq!(status, StatusCode::OK, "{label}: page status");
                assert_eq!(ids(&paged["sandboxes"]), expected, "{label}: page");
            }
            harness.pool.close().await;
            return Ok(());
        }
    };
    let label = format!("{view:?}");
    let (status, unpaged) = harness.get(&format!("/sandboxes?{scope}limit=2")).await;
    assert_eq!(status, StatusCode::OK, "{label}: unpaged list status");
    assert_eq!(ids(&unpaged), first_page, "{label}");

    let mut walked = Vec::new();
    let mut cursor = String::new();
    let mut pages = 0;
    loop {
        let (status, page) = harness
            .get(&format!("/sandboxes?{scope}limit=2&cursor={cursor}"))
            .await;
        assert_eq!(status, StatusCode::OK, "{label}: page status");
        let rows = ids(&page["sandboxes"]);
        if pages == 0 {
            assert_eq!(
                rows, first_page,
                "{label}: the first page is the unpaged list"
            );
        }
        walked.extend(rows);
        pages += 1;
        match page.get("next_cursor") {
            Some(Value::String(next)) => cursor = next.clone(),
            // `null`, never absent, on the last page.
            Some(Value::Null) => break,
            _ => panic!("{label}: unexpected next_cursor"),
        }
    }
    assert_eq!(pages, 3, "{label}");
    assert_eq!(walked, walk, "{label}");
    harness.pool.close().await;
    Ok(())
}

/// Every malformed parameter, typed ones included, answers 400 with the
/// admin JSON `error`; with `cursor`, so does an unknown parameter.
#[tokio::test]
async fn malformed_list_parameters_answer_400() -> TestDbResult<()> {
    let Some(harness) = harness().await? else {
        return Ok(());
    };
    // A well-formed cursor from around 5000 BC, before Postgres's range.
    let out_of_range_cursor = format!(
        "cursor={}",
        URL_SAFE_NO_PAD.encode(format!(
            "{CURSOR_VERSION}:-220000000000000000000:{}",
            SandboxId::new()
        ))
    );
    for (case, (query, needle)) in [
        ("limit=many", "limit"),
        ("org_id=not-a-uuid", "org_id"),
        ("node_id=not-a-uuid", "node_id"),
        ("user_id=not-a-uuid", "user_id"),
        ("assigned=maybe", "assigned"),
        (
            "cursor=&lifecycel=started",
            "unknown query parameter \"lifecycel\"",
        ),
        ("cursor=&node=x", "unknown query parameter \"node\""),
        ("prefix=a%00b", "NUL"),
        ("cursor=bm90LWEtY3Vyc29y", "invalid cursor"),
        ("cursor=%%%", "invalid"),
        (&out_of_range_cursor, "invalid cursor"),
        ("lifecycle=started,napping", "unknown lifecycle"),
        // An empty parameter is unset, but an empty item in a list is not.
        ("lifecycle=started,", "unknown lifecycle"),
        ("template=..", "invalid template"),
        (
            "node_id=00000000-0000-7000-8000-000000000001&assigned=false",
            "assigned=false",
        ),
        ("created_after=yesterday", "created_after"),
        // Valid RFC 3339, but before the earliest timestamp Postgres stores.
        ("created_after=-005000-01-01T00:00:00Z", "outside"),
        ("created_before=-005000-01-01T00:00:00Z", "outside"),
        (
            "created_after=2026-01-02T00:00:00Z&created_before=2026-01-01T00:00:00Z",
            "earlier",
        ),
        // Inverted inside one microsecond, which rounding `created_before`
        // up would otherwise hide.
        (
            "created_after=2026-01-01T00:00:00.0000006Z&created_before=2026-01-01T00:00:00.0000003Z",
            "earlier",
        ),
    ]
    .into_iter()
    .enumerate()
    {
        // Labelled by position and needle: a query can carry an ID.
        let (status, body) = harness.get(&format!("/sandboxes?{query}")).await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "case {case} ({needle}): status"
        );
        let message = body["error"].as_str().unwrap_or_default();
        assert!(
            message.contains(needle),
            "case {case} ({needle}): error message"
        );
    }
    for query in ["org_id=not-a-uuid", "template=py"] {
        let (status, body) = harness
            .get(&format!("/sandboxes/spaced-versions?{query}"))
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{query}: {body}");
        assert!(body["error"].is_string(), "{query}: {body}");
    }
    // Without `cursor`, an unknown parameter is ignored, as before paging.
    let (status, body) = harness.get("/sandboxes?lifecycel=started").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body.is_array(), "{body}");
    harness.pool.close().await;
    Ok(())
}

/// An empty filter parameter is unset, paged or not: the unpaged list ignored
/// these names before they were filters.
#[rstest::rstest]
#[tokio::test]
async fn an_empty_filter_parameter_is_unset(
    #[values(
        "lifecycle",
        "template",
        "node_id",
        "assigned",
        "user_id",
        "created_after",
        "created_before",
        "prefix"
    )]
    name: &str,
) -> TestDbResult<()> {
    let Some(harness) = harness().await? else {
        return Ok(());
    };
    let owner = seed_owner(&harness.pool).await?;
    for _ in 0..2 {
        seed_sandbox(&harness.pool, owner, SeedSandbox::default()).await?;
    }
    let org = owner.org_id.as_uuid();
    // A body's `age` counts whole seconds on the wall clock, so two requests'
    // bodies can differ by a tick; the IDs and the cursor are the list.
    let listed = |body: &Value, paging: &str| {
        if paging.is_empty() {
            (ids(body), Value::Null)
        } else {
            (ids(&body["sandboxes"]), body["next_cursor"].clone())
        }
    };
    for paging in ["", "&cursor="] {
        let (status, unset) = harness
            .get(&format!("/sandboxes?org_id={org}{paging}"))
            .await;
        assert_eq!(status, StatusCode::OK, "{name}{paging}: without it");
        let (status, empty) = harness
            .get(&format!("/sandboxes?org_id={org}&{name}={paging}"))
            .await;
        assert_eq!(status, StatusCode::OK, "{name}{paging}: empty");
        assert_eq!(
            listed(&empty, paging),
            listed(&unset, paging),
            "{name}{paging}: empty is unset"
        );
    }
    harness.pool.close().await;
    Ok(())
}

/// Postgres keeps microseconds, so a `created_before` inside a microsecond
/// still includes a row created at its start.
#[tokio::test]
async fn created_before_keeps_a_row_inside_its_microsecond() -> TestDbResult<()> {
    let Some(harness) = harness().await? else {
        return Ok(());
    };
    let owner = seed_owner(&harness.pool).await?;
    let created_at = OffsetDateTime::from_unix_timestamp(1_790_000_000).expect("timestamp");
    let sandbox_id = seed_sandbox(
        &harness.pool,
        owner,
        SeedSandbox {
            created_at: Some(created_at),
            ..SeedSandbox::default()
        },
    )
    .await?;
    let org = owner.org_id.as_uuid();
    for (before, expected) in [
        (
            "2026-09-21T14:13:20.000000500Z",
            vec![sandbox_id.to_string()],
        ),
        ("2026-09-21T14:13:20.000001Z", vec![sandbox_id.to_string()]),
        ("2026-09-21T14:13:20Z", vec![]),
    ] {
        let (status, body) = harness
            .get(&format!("/sandboxes?org_id={org}&created_before={before}"))
            .await;
        assert_eq!(status, StatusCode::OK, "{before}: {body}");
        assert_eq!(ids(&body), expected, "{before}");
    }
    harness.pool.close().await;
    Ok(())
}

#[tokio::test]
async fn the_list_and_the_count_require_a_managing_org_admin() -> TestDbResult<()> {
    let Some(harness) = harness().await? else {
        return Ok(());
    };
    for path in [
        "/sandboxes",
        "/sandboxes?cursor=",
        "/sandboxes/spaced-versions",
    ] {
        let (status, body) = harness
            .request(&harness.member_token, Method::GET, path, None)
            .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{path}: {body}");
    }
    harness.pool.close().await;
    Ok(())
}

/// The count is its own route, not a sandbox ID, and answers the
/// controller's buckets for one org on request. Their contents are pinned
/// against the list in `list_page::admin::tests`.
#[tokio::test]
async fn the_spaced_version_count_is_served() -> TestDbResult<()> {
    let Some(harness) = harness().await? else {
        return Ok(());
    };
    let owner = seed_owner(&harness.pool).await?;
    let node_id = NodeId::new();
    insert_ready_worker_node(&harness.pool, node_id).await?;
    let sandbox_id = seed_sandbox(
        &harness.pool,
        owner,
        SeedSandbox {
            template: Some("py"),
            ..SeedSandbox::default()
        },
    )
    .await?;
    seed_observation(
        &harness.pool,
        sandbox_id,
        node_id,
        SeedObservation {
            status: Some("started"),
            spaced_version: Some("1.4.0"),
            ..SeedObservation::default()
        },
    )
    .await?;
    let (status, body) = harness
        .get(&format!(
            "/sandboxes/spaced-versions?org_id={}",
            owner.org_id.as_uuid()
        ))
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body,
        json!({"counts": [
            {
                "template": "py",
                "spaced_version": "1.4.0",
                "status": "started",
                "resource_type": "sandbox",
                "count": 1
            }
        ]})
    );
    let (status, body) = harness.get("/sandboxes/spaced-versions?template=py").await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    harness.pool.close().await;
    Ok(())
}

/// The ID forms the admin sandbox routes take, each rendering one sandbox.
#[derive(Clone, Copy, Debug)]
enum IdForm {
    Public,
    Dashed,
    DashedUpper,
    Undashed,
    UndashedUpper,
    PodGeneration,
    Pod,
}

impl IdForm {
    fn render(self, sandbox_id: SandboxId) -> String {
        let undashed = sandbox_id.as_uuid().simple().to_string();
        match self {
            Self::Public => PublicSandboxId::from(sandbox_id).to_string(),
            Self::Dashed => sandbox_id.to_string(),
            Self::DashedUpper => sandbox_id.to_string().to_uppercase(),
            Self::Undashed => undashed,
            Self::UndashedUpper => undashed.to_uppercase(),
            Self::PodGeneration => format!("space-sbx-{undashed}-g1"),
            Self::Pod => format!("space-sbx-{undashed}"),
        }
    }
}

/// Every admin sandbox route and the debug route resolve each ID form to the
/// same sandbox.
#[rstest::rstest]
#[tokio::test]
async fn every_admin_sandbox_route_takes_every_id_form(
    #[values(
        IdForm::Public,
        IdForm::Dashed,
        IdForm::DashedUpper,
        IdForm::Undashed,
        IdForm::UndashedUpper,
        IdForm::PodGeneration,
        IdForm::Pod
    )]
    form: IdForm,
) -> TestDbResult<()> {
    let Some(harness) = harness().await? else {
        return Ok(());
    };
    let owner = seed_owner(&harness.pool).await?;
    let sandbox_id = seed_sandbox(&harness.pool, owner, SeedSandbox::default()).await?;
    let public_id = PublicSandboxId::from(sandbox_id).to_string();
    let case = format!("{form:?}");
    let form = form.render(sandbox_id);
    let (status, body) = harness.get(&format!("/sandboxes/{form}")).await;
    assert_eq!(status, StatusCode::OK, "GET form {case}");
    assert_eq!(body["id"], sandbox_id.to_string(), "GET form {case}");

    let (status, _) = harness.get(&format!("/debug/sandbox/{form}")).await;
    assert_eq!(status, StatusCode::OK, "debug form {case}");

    let (status, body) = harness
        .request(
            &harness.admin_token,
            Method::POST,
            &format!("/sandboxes/{form}:force-stop"),
            None,
        )
        .await;
    assert_eq!(status, StatusCode::ACCEPTED, "force-stop form {case}");
    assert_eq!(body["sandbox_id"], public_id, "force-stop form {case}");

    let (status, body) = harness
        .request(
            &harness.admin_token,
            Method::PATCH,
            &format!("/sandboxes/{form}"),
            Some(json!({"metadata": {"listed": "yes"}})),
        )
        .await;
    assert_eq!(status, StatusCode::ACCEPTED, "PATCH form {case}");
    assert_eq!(body["sandbox_id"], public_id, "PATCH form {case}");

    let (status, _) = harness
        .request(
            &harness.admin_token,
            Method::POST,
            &format!("/sandboxes/{form}/commands"),
            Some(json!({"command": ["true"]})),
        )
        .await;
    assert_ne!(status, StatusCode::BAD_REQUEST, "commands form {case}");
    assert_ne!(status, StatusCode::NOT_FOUND, "commands form {case}");
    harness.pool.close().await;
    Ok(())
}

/// Every admin sandbox route and the debug route refuse an unknown ID form
/// with 400.
#[tokio::test]
async fn every_admin_sandbox_route_refuses_an_unknown_id_form() -> TestDbResult<()> {
    let Some(harness) = harness().await? else {
        return Ok(());
    };
    let owner = seed_owner(&harness.pool).await?;
    let sandbox_id = seed_sandbox(&harness.pool, owner, SeedSandbox::default()).await?;
    let unknown = format!("{{{sandbox_id}}}");
    for (method, path, body) in [
        (Method::GET, format!("/sandboxes/{unknown}"), None),
        (Method::GET, format!("/debug/sandbox/{unknown}"), None),
        (
            Method::POST,
            format!("/sandboxes/{unknown}:force-stop"),
            None,
        ),
        (
            Method::PATCH,
            format!("/sandboxes/{unknown}"),
            Some(json!({"metadata": {}})),
        ),
        (
            Method::POST,
            format!("/sandboxes/{unknown}/commands"),
            Some(json!({"command": ["true"]})),
        ),
    ] {
        let (status, response) = harness
            .request(&harness.admin_token, method.clone(), &path, body)
            .await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "{method} {path}: {response}"
        );
    }
    harness.pool.close().await;
    Ok(())
}
