use pplx_space_common_core::types::NodeId;
use pplx_space_common_core::types::{OrgId, UserId};

use super::*;
use pplx_space_db_test_fixtures::pg_test_db;

use pplx_space_test_env::TestResult;

mod native_build;
mod substrate_scope;

const KERNEL_VERSION: &str = "vmlinux-6.1.200-custom";
const KERNEL_DIGEST: &str =
    "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const EXACT_SEED_BASE: &str = "seed-bk-smoke-contract@sha256:1111111111111111111111111111111111111111111111111111111111111111";

fn scratch_template_build_request_json(version: &str, digest: &str) -> serde_json::Value {
    serde_json::json!({
        "recipe": {
            "metadata": {},
            "origin": {
                "from_scratch": {
                    "source": {
                        "ubuntu_minimal_cloud_squashfs": {
                            "release": "resolute",
                            "version": "26.04",
                            "date": "release"
                        }
                    },
                    "guest_kernel": {
                        "version": version,
                        "digest": digest
                    }
                }
            },
            "steps": []
        }
    })
}

async fn insert_template(pool: &sqlx::PgPool, template: &TemplateName) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO templates (id, name, description, org_id)
         VALUES ($1, $2, NULL, NULL)"
    ))
    .bind(Uuid::now_v7())
    .bind(template.as_str())
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_node(pool: &sqlx::PgPool, node_id: NodeId, role: &str) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '10.0.0.10'::INET, $2, 'space', 'ready')"
    ))
    .bind(node_id)
    .bind(role)
    .execute(pool)
    .await?;
    Ok(())
}

struct InventoryInput<'a> {
    node_id: NodeId,
    template: &'a TemplateName,
    content_hash: &'a str,
    pool_target: Option<i32>,
    pool_current: i32,
    pool_pending: i32,
    sandbox_ids: &'a [String],
}

async fn insert_inventory(pool: &sqlx::PgPool, input: InventoryInput<'_>) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO node_template_inventory (
             node_id, template_name, content_hash, runtime,
             pool_target, pool_current, pool_pending, pool_sandbox_ids
         )
         VALUES ($1, $2, $3, 'fc', $4, $5, $6, $7)"
    ))
    .bind(input.node_id)
    .bind(input.template.as_str())
    .bind(input.content_hash)
    .bind(input.pool_target)
    .bind(input.pool_current)
    .bind(input.pool_pending)
    .bind(serde_json::json!(input.sandbox_ids))
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_base_snapshot_inventory(
    pool: &sqlx::PgPool,
    node_id: NodeId,
    template: &TemplateName,
    content_hash: &str,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO node_template_inventory (
             node_id, template_name, content_hash, runtime, arch,
             has_base_snapshot, base_snapshot_size_bytes, snapshots,
             observed_at, built_at
         )
         VALUES (
             $1, $2, $3, 'fc', 'aarch64',
             TRUE, 4096, '[]'::JSONB,
             NOW(), '2026-06-01T00:00:00Z'
         )"
    ))
    .bind(node_id)
    .bind(template.as_str())
    .bind(content_hash)
    .execute(pool)
    .await?;
    Ok(())
}

#[test]
fn api_error_uses_public_error_envelope() {
    let (status, Json(body)) = api_error(StatusCode::NOT_FOUND, "Template not found");

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body.code, PublicErrorCode::NotFound);
    assert_eq!(body.message, "Template not found");
    assert!(!body.retryable);
    assert!(body.details.is_empty());
}

#[tokio::test]
async fn template_rows_include_observed_base_snapshot_inventory() -> TestResult {
    let db = pg_test_db().await?;
    let template = TemplateName::parse("seed")?;
    insert_template(&db.pool, &template).await?;

    let builder_id = NodeId::new();
    insert_node(&db.pool, builder_id, "builder").await?;
    insert_base_snapshot_inventory(&db.pool, builder_id, &template, "sha256:current").await?;

    let worker_id = NodeId::new();
    insert_node(&db.pool, worker_id, "worker").await?;
    insert_base_snapshot_inventory(&db.pool, worker_id, &template, "sha256:current").await?;

    let templates = fetch_template_rows(&db.pool, None).await?;
    let response = templates
        .into_iter()
        .map(template_response)
        .find(|template| template.name == "seed")
        .expect("seed template should be listed");

    assert_eq!(response.content_hash.as_deref(), Some("sha256:current"));
    assert_eq!(response.has_base_snapshot, Some(true));
    assert_eq!(response.base_snapshot_size_bytes, Some(4096));

    Ok(())
}

#[tokio::test]
async fn named_release_tag_moves_existing_release_into_warm_retention() -> TestResult {
    let db = pg_test_db().await?;
    let state = AppState::from_pool(db.pool.clone()).await?;
    let (actor_id, managing_org_id, actor_email, actor_password_hash, actor_display_name) =
        sqlx::query_as::<_, (Uuid, Uuid, String, String, String)>(sql!(
            "SELECT u.id, c.managing_org_id, u.email, u.password_hash, u.display_name
             FROM clusters c
             JOIN users u ON u.org_id = c.managing_org_id
             WHERE c.singleton
               AND u.role = 'organization_admin'
             ORDER BY u.id
             LIMIT 1"
        ))
        .fetch_one(&db.pool)
        .await?;
    let actor = User {
        id: UserId::from_uuid(actor_id),
        email: actor_email,
        password_hash: actor_password_hash,
        display_name: actor_display_name,
        org_id: OrgId::from_uuid(managing_org_id),
        role: crate::user_role::UserRole::OrganizationAdmin,
        owner_source: pplx_space_common_core::types::SandboxOwnerSource::Local,
        created_at: time::OffsetDateTime::UNIX_EPOCH,
        updated_at: time::OffsetDateTime::UNIX_EPOCH,
    };
    let template = TemplateName::parse("named-retag")?;
    let republished_release_id = Uuid::now_v7();
    let newer_release_id = Uuid::now_v7();
    let republished_hash =
        "sha256:1414141414141414141414141414141414141414141414141414141414141414";
    let newer_hash = "sha256:1515151515151515151515151515151515151515151515151515151515151515";
    insert_template(&db.pool, &template).await?;
    sqlx::query(sql!(
        "UPDATE templates
         SET org_id = $2
         WHERE name = $1"
    ))
    .bind(template.as_str())
    .bind(managing_org_id)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO template_releases (id, template_name, type, hash, created_at, org_id)
         VALUES
             ($1, $3, 'fc', $4, NOW() - INTERVAL '2 days', $6),
             ($2, $3, 'fc', $5, NOW() - INTERVAL '1 day', $6)"
    ))
    .bind(republished_release_id)
    .bind(newer_release_id)
    .bind(template.as_str())
    .bind(republished_hash)
    .bind(newer_hash)
    .bind(managing_org_id)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO template_release_tags (
             template_release_id, template_name, tag, created_at, modified_at
         ) VALUES ($1, $2, 'prod', NOW() - INTERVAL '1 day', NOW() - INTERVAL '1 day')"
    ))
    .bind(newer_release_id)
    .bind(template.as_str())
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO warm_pool_policies (
             template_name, runtime, vcpu, memory_mib, min_ready_slots,
             max_ready_slots, retain_releases
         ) VALUES ($1, 'fc', 2, 1024, 1, 10, 1)"
    ))
    .bind(template.as_str())
    .execute(&db.pool)
    .await?;

    let response = tag_template_release(
        State(state),
        Extension(AuthUser(actor)),
        Path(republished_release_id),
        Json(TemplateReleaseTagRequest {
            tag: "prod".to_string(),
        }),
    )
    .await
    .into_response();
    let protected = pplx_space_cluster_pool::WarmPoolStore::new(db.pool.clone())
        .protected_template_releases()
        .await?;

    assert_eq!(response.status(), StatusCode::OK);
    assert!(protected.iter().any(|release| {
        release.template_name == template.as_str() && release.content_hash == republished_hash
    }));
    assert!(!protected.iter().any(|release| {
        release.template_name == template.as_str() && release.content_hash == newer_hash
    }));

    Ok(())
}

#[test]
fn template_build_request_converts_to_build_proto() -> TestResult {
    let request: TemplateBuildRequest = serde_json::from_value(serde_json::json!({
        "force": true,
        "publish_tags": [],
        "recipe": {
            "metadata": { "description": "demo" },
            "build_config": {
                "vcpu": 2,
                "memory_bytes": 2147483648u64,
                "disk_bytes": 10737418240u64
            },
            "origin": {
                "from_template": { "base": EXACT_SEED_BASE }
            },
            "steps": [
                {
                    "display_name": "install",
                    "env_refs": ["APT_TOKEN"],
                    "package_install": {
                        "manager": "apt",
                        "packages": ["curl"],
                        "extra_args": ["--no-install-recommends"]
                    }
                },
                {
                    "copy": {
                        "inline_bytes_base64": "aGVsbG8=",
                        "dest": "/tmp/hello.txt",
                        "source_path": "payload/hello.txt"
                    }
                }
            ]
        }
    }))?;

    let tags = normalize_publish_tags(request.publish_tags)?;
    let proto = template_build_recipe_proto(request.recipe)?;

    assert!(tags.is_empty());
    assert_eq!(
        proto
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.description.as_deref()),
        Some("demo")
    );
    assert_eq!(proto.steps.len(), 2);
    assert!(matches!(
        proto.origin.as_ref().and_then(|origin| origin.kind.as_ref()),
        Some(build_proto::recipe::origin::Kind::FromTemplate(from_template))
            if from_template.base == EXACT_SEED_BASE
    ));
    assert!(matches!(
        proto.steps[0].body.as_ref(),
        Some(build_proto::recipe::step::Body::PackageInstall(step))
            if step.manager == build_proto::recipe::PackageManager::Apt as i32
                && step.packages == ["curl"]
    ));
    assert_eq!(proto.steps[0].env_refs, ["APT_TOKEN"]);
    assert!(matches!(
        proto.steps[1].body.as_ref(),
        Some(build_proto::recipe::step::Body::Copy(step))
            if matches!(
                step.payload.as_ref(),
                Some(build_proto::recipe::copy::Payload::InlineBytes(bytes))
                    if bytes == b"hello"
            ) && step.source_path.as_deref() == Some("payload/hello.txt")
    ));

    Ok(())
}

#[test]
fn scratch_template_build_request_preserves_exact_guest_kernel() -> TestResult {
    let request: TemplateBuildRequest = serde_json::from_value(
        scratch_template_build_request_json(KERNEL_VERSION, KERNEL_DIGEST),
    )?;

    let proto = template_build_recipe_proto(request.recipe)?;

    assert!(matches!(
        proto.origin.and_then(|origin| origin.kind),
        Some(build_proto::recipe::origin::Kind::PinnedFromScratch(from_scratch))
            if matches!(
                from_scratch.guest_kernel.as_ref(),
                Some(guest_kernel)
                    if guest_kernel.version == KERNEL_VERSION
                        && guest_kernel.digest == KERNEL_DIGEST
            )
    ));

    Ok(())
}

#[test]
fn unpinned_scratch_template_build_request_uses_legacy_wire_variant() -> TestResult {
    let mut request_json = scratch_template_build_request_json(KERNEL_VERSION, KERNEL_DIGEST);
    request_json["recipe"]["origin"]["from_scratch"]
        .as_object_mut()
        .expect("from_scratch should be an object")
        .remove("guest_kernel");
    let request: TemplateBuildRequest = serde_json::from_value(request_json)?;

    let proto = template_build_recipe_proto(request.recipe)?;

    assert!(matches!(
        proto.origin.and_then(|origin| origin.kind),
        Some(build_proto::recipe::origin::Kind::FromScratch(_))
    ));

    Ok(())
}

#[test]
fn scratch_template_build_request_rejects_malformed_guest_kernel() {
    for (version, digest) in [
        ("bad/version", KERNEL_DIGEST),
        (KERNEL_VERSION, "sha256:not-a-full-digest"),
    ] {
        let request = serde_json::from_value::<TemplateBuildRequest>(
            scratch_template_build_request_json(version, digest),
        );

        assert!(
            request.is_err(),
            "malformed guest kernel should fail JSON deserialization: version={version:?}, digest={digest:?}"
        );
    }
}

#[tokio::test]
async fn template_pool_observed_does_not_multiply_counts_by_sandbox_ids() -> TestResult {
    let db = pg_test_db().await?;
    let template = TemplateName::parse("seed")?;
    insert_template(&db.pool, &template).await?;

    let builder_id = NodeId::new();
    insert_node(&db.pool, builder_id, "builder").await?;
    insert_inventory(
        &db.pool,
        InventoryInput {
            node_id: builder_id,
            template: &template,
            content_hash: "sha256:current",
            pool_target: None,
            pool_current: 0,
            pool_pending: 0,
            sandbox_ids: &[],
        },
    )
    .await?;

    let worker_id = NodeId::new();
    insert_node(&db.pool, worker_id, "worker").await?;
    let sandbox_ids = (0..15)
        .map(|index| format!("sandbox-{index}"))
        .collect::<Vec<_>>();
    insert_inventory(
        &db.pool,
        InventoryInput {
            node_id: worker_id,
            template: &template,
            content_hash: "sha256:current",
            pool_target: Some(15),
            pool_current: 15,
            pool_pending: 0,
            sandbox_ids: &sandbox_ids,
        },
    )
    .await?;

    let stale_worker_id = NodeId::new();
    insert_node(&db.pool, stale_worker_id, "worker").await?;
    insert_inventory(
        &db.pool,
        InventoryInput {
            node_id: stale_worker_id,
            template: &template,
            content_hash: "sha256:old",
            pool_target: Some(100),
            pool_current: 100,
            pool_pending: 0,
            sandbox_ids: &["stale-sandbox".to_string()],
        },
    )
    .await?;

    let observed = fetch_template_pool_observed(&db.pool, &template)
        .await?
        .expect("pool should be observed");

    assert_eq!(observed.pool_target, Some(15));
    assert_eq!(observed.pool_current, 15);
    assert_eq!(observed.pool_pending, 0);
    assert_eq!(observed.pool_sandbox_ids.0.len(), 15);
    assert!(
        !observed
            .pool_sandbox_ids
            .0
            .iter()
            .any(|id| id == "stale-sandbox")
    );

    Ok(())
}

/// The template surface's envelope must not contradict the disposition it
/// carries, and its wrappers must forward the scope.
///
/// `api_error` derives `retryable` from the status, and 409 is absent from
/// that set, so all six other 409s this module returns render
/// `retryable: false` — see `contention_envelope`'s own doc for the list and
/// for the caveat that a couple of them do clear on their own. What matters
/// here is that a contention 409's message tells the caller to retry, so
/// `contention_envelope` sets the flag rather than inheriting it; without that
/// an SDK branching on `retryable` gives up on the one 409 that is certain to
/// resolve.
///
/// The SQLSTATE table itself lives once in
/// `routes::admin::tests::a_read_only_statement_is_never_blamed_for_a_write_interleave`,
/// including the store-wrapped arm; the scope assertions below cover only that
/// this module's two wrappers forward what they are given.
#[tokio::test]
async fn a_contention_conflict_is_advertised_as_retryable() -> TestResult {
    let db = pg_test_db().await?;

    // The default this overrides, asserted first: without it, the `retryable`
    // assertion below would pass whether or not the override exists.
    let (_, Json(permanent)) = api_error(StatusCode::CONFLICT, "Template already registered");
    assert!(
        !permanent.retryable,
        "an ordinary template 409 stays permanent"
    );

    let deadlock = pplx_space_db_test_fixtures::raise_sqlstate(&db.pool, "40P01").await;
    let (status, Json(contended)) = contention_response(ContentionScope::Writing, &deadlock)
        .expect("a deadlock on a template write is contention");
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(contended.code, PublicErrorCode::Conflict);
    assert!(
        contended.retryable,
        "a lost lock race is the one retryable 409 on this surface"
    );
    // Same error, other scope: the wrapper forwards it rather than pinning one,
    // and a read takes the generic retryable 503 instead of the 409.
    assert_eq!(
        contention_response(ContentionScope::ReadOnly, &deadlock).map(|(status, _)| status),
        Some(StatusCode::SERVICE_UNAVAILABLE),
        "a template read is not the caller's write losing a race"
    );

    // The store-backed arm reaches the same envelope as the bare `sqlx` arms.
    let wrapped = PublicOperationStoreError::Sql(
        pplx_space_db_test_fixtures::raise_sqlstate(&db.pool, "55P03").await,
    );
    let (status, Json(contended)) = store_contention_response(ContentionScope::Writing, &wrapped)
        .expect("a lock timeout through the store is contention");
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(contended.retryable);

    // Closed explicitly because nextest reports this test as leaky otherwise:
    // `raise_sqlstate` deliberately fails a statement on each connection it
    // borrows, and the pool's are still outstanding at drop. Cosmetic rather
    // than load-bearing — `DatabaseCleanup::drop` issues
    // `DROP DATABASE … WITH (FORCE)`, which terminates any leftover backend —
    // so the sibling tests that do not close are not thereby wrong.
    db.pool.close().await;
    Ok(())
}

/// A post-commit failure must not advertise a retry, in the envelope any more
/// than in the status.
///
/// `update_template`'s reload and `tag_template_release`'s hydrate both run
/// after `tx.commit()` returned, so the write landed and a replay is wrong. The
/// contention disposition is withheld from them for that reason — but
/// `api_error` marks *every* 500 `retryable: true`, so withholding the status
/// alone left the envelope inviting exactly the replay the exemption exists to
/// prevent: on the `If-Match` path the caller's generation has already been
/// incremented, so the replay answers 412 and reads as "your update did not
/// apply" for an update that did.
///
/// Asserts `post_commit_error` itself, not the two arms that call it, so it
/// does not catch either call being dropped — reverting both to plain
/// `api_error` leaves this and every sibling test green. Closing that needs a
/// driven test on the pattern of
/// `routes::debug::sandbox::tests::the_debug_handler_answers_a_contended_read_with_a_503`.
#[test]
fn a_post_commit_failure_does_not_advertise_a_retry() {
    // The default this overrides, asserted first: without it the assertion
    // below would pass whether or not `post_commit_error` exists.
    let (status, Json(default_500)) = api_error(
        StatusCode::INTERNAL_SERVER_ERROR,
        "failed to update template",
    );
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(
        default_500.retryable,
        "this module marks every ordinary 500 retryable, which is what makes \
         the post-commit override load-bearing"
    );

    let (status, Json(post_commit)) = post_commit_error("failed to update template");
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(
        !post_commit.retryable,
        "the write already landed; replaying it answers 412 on the If-Match path"
    );
}

/// A `PATCH` carrying no field reaches the reconcile fanout having written
/// nothing, which is why `update_template` classifies by what the request has
/// done rather than by the handler it sits in.
///
/// All three `UpdateTemplateRequest` fields are `Option` with a serde default,
/// so `{}` deserializes with none set and every write between the preflight
/// `SELECT` and the fanout is skipped. The transaction still runs the fanout's
/// node-discovery read, so that read executes having written nothing — the
/// input `ContentionScope::ReadOnly` exists for. One `Writing` for the whole
/// block would answer a `40001`/`40P01` there with the 409's "conflicted with a
/// concurrent admin write", about a write the request never made.
///
/// Two phases. The first pins that the write-free path exists and runs the
/// fanout: a 200 is only reachable through the enqueue and the commit, the
/// queue and trigger rows show both enqueue writes executed, and the template
/// and its policy table are untouched — so the request wrote nothing of its
/// own on the way there. A seeded node is load-bearing for that; with none, the
/// discovery read comes back empty and the enqueue returns before writing.
/// The second phase holds `nodes` under `ACCESS EXCLUSIVE` so the discovery
/// read aborts, and pins that the abort is answered rather than reported as an
/// internal failure.
///
/// **What this does not pin, precisely, because the distinction is easy to
/// misread:** not the 409-versus-503 choice, and not the scope this handler
/// passes. `55P03` is the only contention SQLSTATE a held lock raises
/// deterministically, and `contention_disposition` gates the 409 on the error
/// *not* being a lock wait — so both scopes answer this abort 503, and phase
/// two stays green if the discovery read's scope is changed back to `Writing`.
/// It catches the arm dropping classification altogether, which is the state
/// before this series, not the scope wiring within it. Pinning that needs a
/// `40001`/`40P01` on the read, which a held lock cannot produce on demand.
/// The scope-to-status table is asserted once, over both scopes and every
/// SQLSTATE, in
/// `routes::admin::tests::a_read_only_statement_is_never_blamed_for_a_write_interleave`.
#[tokio::test]
async fn a_fieldless_template_update_reaches_the_fanout_without_writing()
-> pplx_space_db_test_fixtures::TestDbResult<()> {
    use pplx_space_db_test_fixtures::SEED_ORG_ID;

    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let template = TemplateName::parse("fieldless-update-probe").expect("valid template name");
    insert_template(&db.pool, &template).await?;
    // A migrated database has no `nodes` rows, and without one the discovery
    // read returns empty and `enqueue_template_refs_for_nodes_tx` returns
    // before either write — so the fanout assertion below would hold with the
    // enqueue call deleted. `managed_by = 'k8s'` satisfies the discovery
    // predicate's `!= 'space'` branch without needing a `node_direct` row.
    let node_id = NodeId::new();
    pplx_space_db_test_fixtures::insert_ready_worker_node(&db.pool, node_id).await?;

    // The role-level bound, so the blocked read aborts instead of waiting out
    // the lock.
    let bounded = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .connect_with(
            db.database_url
                .parse::<sqlx::postgres::PgConnectOptions>()?
                .options([("lock_timeout", "250ms")]),
        )
        .await?;
    let state = AppState::from_pool(bounded)
        .await
        .expect("app state from the test pool");

    let actor = User {
        id: UserId::new(),
        email: "fieldless-update-probe@example.test".to_string(),
        password_hash: String::new(),
        display_name: "fieldless update probe".to_string(),
        org_id: OrgId::from_uuid(SEED_ORG_ID),
        role: crate::user_role::UserRole::OrganizationAdmin,
        owner_source: pplx_space_common_core::types::SandboxOwnerSource::Local,
        created_at: OffsetDateTime::now_utc(),
        updated_at: OffsetDateTime::now_utc(),
    };
    let fieldless = || UpdateTemplateRequest {
        description: None,
        default_auto_lifecycle: None,
        network_policy: None,
    };

    let response = update_template(
        State(state.clone()),
        Extension(AuthUser(actor.clone())),
        Path(template.clone()),
        HeaderMap::new(),
        Json(fieldless()),
    )
    .await
    .into_response();
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "a fieldless update still runs the fanout and commits"
    );

    let policies: i64 = sqlx::query_scalar(sql!(
        "SELECT COUNT(*) FROM template_default_auto_lifecycle_policies WHERE template = $1"
    ))
    .bind(template.as_str())
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(
        policies, 0,
        "no field was sent, so the policy write must have been skipped"
    );
    let description: Option<String> =
        sqlx::query_scalar(sql!("SELECT description FROM templates WHERE name = $1"))
            .bind(template.as_str())
            .fetch_one(&db.pool)
            .await?;
    assert!(
        description.is_none(),
        "no field was sent, so the template row must be untouched"
    );

    // The fanout's writes really ran, on a request that wrote nothing of its
    // own. Without this the test would pass with the enqueue call deleted.
    let queued: i64 = sqlx::query_scalar(sql!(
        "SELECT COUNT(*) FROM node_reconcile_queue WHERE node_id = $1"
    ))
    .bind(node_id)
    .fetch_one(&db.pool)
    .await?;
    assert!(
        queued > 0,
        "the fieldless update must still enqueue the reconcile fanout"
    );
    let triggers: i64 = sqlx::query_scalar(sql!(
        "SELECT COUNT(*) FROM node_reconcile_triggers WHERE node_id = $1"
    ))
    .bind(node_id)
    .fetch_one(&db.pool)
    .await?;
    assert!(
        triggers > 0,
        "the fieldless update must still enqueue the reconcile trigger"
    );

    // The same handler, one field set, so the two absences above are a write
    // this request withheld rather than a fixture that cannot write at all.
    let response = update_template(
        State(state.clone()),
        Extension(AuthUser(actor.clone())),
        Path(template.clone()),
        HeaderMap::new(),
        Json(UpdateTemplateRequest {
            description: Some("written on purpose".to_string()),
            default_auto_lifecycle: None,
            network_policy: None,
        }),
    )
    .await
    .into_response();
    assert_eq!(response.status(), StatusCode::OK);
    let description: Option<String> =
        sqlx::query_scalar(sql!("SELECT description FROM templates WHERE name = $1"))
            .bind(template.as_str())
            .fetch_one(&db.pool)
            .await?;
    assert_eq!(
        description.as_deref(),
        Some("written on purpose"),
        "the write path works, so the fieldless run above really did skip it"
    );

    // `ACCESS EXCLUSIVE` is what DDL takes and conflicts with the discovery
    // read's `ACCESS SHARE`, so that read blocks and then aborts.
    let mut blocker = db.pool.begin().await?;
    sqlx::query(sql!("LOCK TABLE nodes IN ACCESS EXCLUSIVE MODE"))
        .execute(&mut *blocker)
        .await?;

    let response = update_template(
        State(state.clone()),
        Extension(AuthUser(actor.clone())),
        Path(template.clone()),
        HeaderMap::new(),
        Json(fieldless()),
    )
    .await
    .into_response();

    blocker.rollback().await?;

    assert_eq!(
        response.status(),
        StatusCode::SERVICE_UNAVAILABLE,
        "a contended discovery read on the write-free path is retryable, not internal"
    );

    db.pool.close().await;
    Ok(())
}

/// `reconcile_base_snapshot_policy`'s discovery read is answered as the read it
/// is, not as an internal failure.
///
/// This is the other handler the scope split changes, and the one where the
/// change is unconditional: it writes nothing before its fanout, so its
/// discovery read is `ReadOnly` on every request rather than only on a
/// fieldless one. It had no test at all.
///
/// `nodes` is held under `ACCESS EXCLUSIVE` — what DDL takes — so the read
/// blocks and aborts against the pool's `lock_timeout`. Reverting the arm to
/// log-and-500 fails this.
///
/// The same limit as
/// `a_fieldless_template_update_reaches_the_fanout_without_writing` applies: a
/// held lock raises `55P03`, which takes the 503 under either scope, so this
/// pins that the read is classified, not that it is classified `ReadOnly`.
#[tokio::test]
async fn a_contended_base_snapshot_reconcile_read_is_retryable()
-> pplx_space_db_test_fixtures::TestDbResult<()> {
    use pplx_space_db_test_fixtures::SEED_ORG_ID;

    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let template = TemplateName::parse("reconcile-read-probe").expect("valid template name");
    insert_template(&db.pool, &template).await?;

    let bounded = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .connect_with(
            db.database_url
                .parse::<sqlx::postgres::PgConnectOptions>()?
                .options([("lock_timeout", "250ms")]),
        )
        .await?;
    let state = AppState::from_pool(bounded)
        .await
        .expect("app state from the test pool");
    let actor = User {
        id: UserId::new(),
        email: "reconcile-read-probe@example.test".to_string(),
        password_hash: String::new(),
        display_name: "reconcile read probe".to_string(),
        org_id: OrgId::from_uuid(SEED_ORG_ID),
        role: crate::user_role::UserRole::OrganizationAdmin,
        owner_source: pplx_space_common_core::types::SandboxOwnerSource::Local,
        created_at: OffsetDateTime::now_utc(),
        updated_at: OffsetDateTime::now_utc(),
    };

    let mut blocker = db.pool.begin().await?;
    sqlx::query(sql!("LOCK TABLE nodes IN ACCESS EXCLUSIVE MODE"))
        .execute(&mut *blocker)
        .await?;

    let response = reconcile_base_snapshot_policy(
        State(state.clone()),
        Extension(AuthUser(actor.clone())),
        Path(template.clone()),
    )
    .await
    .into_response();

    blocker.rollback().await?;

    assert_eq!(
        response.status(),
        StatusCode::SERVICE_UNAVAILABLE,
        "a contended discovery read is retryable, not internal"
    );

    db.pool.close().await;
    Ok(())
}

#[test]
fn system_code_runtime_uses_embedded_steps_and_requested_seed() -> TestResult {
    let base = format!("seed@{KERNEL_DIGEST}");

    let recipe = system_template_recipe(SystemTemplate::CodeRuntime, Some(&base))?;

    assert!(matches!(recipe.origin.and_then(|origin| origin.kind),
        Some(build_proto::recipe::origin::Kind::FromTemplate(origin)) if origin.base == base));
    assert!(
        !recipe.steps.is_empty(),
        "code-runtime must install its runtime"
    );
    Ok(())
}

#[test]
fn native_runtime_http_steps_preserve_required_wire_variants() {
    let declaration = serde_json::json!({"schema_version":1,"apps":[{"name":"app","command":["/bin/sleep","infinity"],
        "user":"root","working_directory":"/","env":{},"readiness":{"type":"launch"},"startup_timeout_secs":10}]});
    let step: TemplateBuildStep =
        serde_json::from_value(serde_json::json!({"set_runtime":declaration})).unwrap();
    let wire = template_build_step_proto(1, step).unwrap();
    let Some(build_proto::recipe::step::Body::SetRuntime(runtime)) = wire.body else {
        panic!("native runtime body missing")
    };
    pplx_space_build_mgr_client::runtime::decode(runtime).unwrap();
    let step: TemplateBuildStep =
        serde_json::from_value(serde_json::json!({"clear_runtime":{}})).unwrap();
    assert!(matches!(
        template_build_step_proto(1, step).unwrap().body,
        Some(build_proto::recipe::step::Body::ClearRuntime(_))
    ));
    let ambiguous: TemplateBuildStep =
        serde_json::from_value(serde_json::json!({"set_runtime":declaration,"clear_runtime":{}}))
            .unwrap();
    template_build_step_proto(1, ambiguous).unwrap_err();
    serde_json::from_value::<TemplateBuildStep>(
        serde_json::json!({"clear_runtime":{"unexpected":true}}),
    )
    .unwrap_err();
}

/// A template left in `deleting` by a delete operation that already failed
/// must accept a new delete request. Before the fix the handler returned the
/// failed operation's id for every later request, so the template could never
/// leave `deleting` through the API.
#[tokio::test]
async fn a_delete_after_a_failed_delete_operation_starts_a_replacement()
-> pplx_space_db_test_fixtures::TestDbResult<()> {
    use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID};

    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let template = TemplateName::parse("failed-delete-retry-probe").expect("valid template name");
    insert_template(&db.pool, &template).await?;
    let state = AppState::from_pool(db.pool.clone())
        .await
        .expect("app state from the test pool");
    let actor = User {
        id: UserId::from_uuid(SEED_USER_ID),
        email: "failed-delete-retry-probe@example.test".to_string(),
        password_hash: String::new(),
        display_name: "failed delete retry probe".to_string(),
        org_id: OrgId::from_uuid(SEED_ORG_ID),
        role: crate::user_role::UserRole::OrganizationAdmin,
        owner_source: pplx_space_common_core::types::SandboxOwnerSource::Local,
        created_at: OffsetDateTime::now_utc(),
        updated_at: OffsetDateTime::now_utc(),
    };
    let delete = |key: &'static str| {
        let state = state.clone();
        let actor = actor.clone();
        let template = template.clone();
        async move {
            let mut headers = HeaderMap::new();
            headers.insert(IDEMPOTENCY_KEY_HEADER, key.parse().expect("header value"));
            let response = delete_template(
                State(state),
                Extension(AuthUser(actor)),
                headers,
                Path(template),
            )
            .await
            .into_response();
            assert_eq!(
                response.status(),
                StatusCode::ACCEPTED,
                "delete with key {key}"
            );
            let body: serde_json::Value = serde_json::from_slice(
                &axum::body::to_bytes(response.into_body(), 100_000)
                    .await
                    .expect("response body"),
            )
            .expect("json response");
            body["operation_id"]
                .as_str()
                .expect("operation id in response")
                .to_string()
        }
    };
    let recorded_operation = || async {
        sqlx::query_scalar::<_, Option<Uuid>>(sql!(
            "SELECT delete_operation_id FROM templates WHERE name = $1"
        ))
        .bind(template.as_str())
        .fetch_one(&db.pool)
        .await
        .expect("template row")
        .expect("delete operation recorded")
    };

    let first = delete("failed-delete-retry-first").await;
    let first_id = recorded_operation().await;
    assert_eq!(
        first,
        pplx_space_public_api::OperationId::from_uuid(first_id).to_string()
    );

    // While the first operation is active, a new request joins it.
    assert_eq!(
        delete("failed-delete-retry-join").await,
        first,
        "an active delete is joined, not replaced"
    );

    // The dispatcher fails the operation, e.g. after exhausting its retry
    // budget while a sandbox still used the template on a node.
    sqlx::query(sql!(
        "UPDATE public_operations
         SET state = 'failed', completed_at = NOW()
         WHERE operation_id = $1"
    ))
    .bind(first_id)
    .execute(&db.pool)
    .await?;

    let second = delete("failed-delete-retry-second").await;
    assert_ne!(second, first, "a failed delete must be replaced");
    let second_id = recorded_operation().await;
    assert_eq!(
        second,
        pplx_space_public_api::OperationId::from_uuid(second_id).to_string()
    );
    let (desired_state, operation_state): (String, String) = sqlx::query_as(sql!(
        "SELECT t.desired_state, po.state
         FROM templates t
         JOIN public_operations po ON po.operation_id = t.delete_operation_id
         WHERE t.name = $1"
    ))
    .bind(template.as_str())
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(desired_state, TemplateDesiredState::Deleting.as_str());
    assert_eq!(operation_state, "queued", "the replacement is dispatchable");

    // Replaying any key joins the active replacement rather than reviving
    // the failed operation.
    assert_eq!(delete("failed-delete-retry-first").await, second);
    assert_eq!(recorded_operation().await, second_id);

    // The replacement fails too. Replaying the first key answers with its
    // original, failed operation and must not reinstall it as the current
    // delete; a fresh key then starts a dispatchable third attempt.
    sqlx::query(sql!(
        "UPDATE public_operations
         SET state = 'failed', completed_at = NOW()
         WHERE operation_id = $1"
    ))
    .bind(second_id)
    .execute(&db.pool)
    .await?;
    assert_eq!(delete("failed-delete-retry-first").await, first);
    assert_eq!(
        recorded_operation().await,
        second_id,
        "a terminal replayed operation leaves the current delete untouched"
    );
    let third = delete("failed-delete-retry-third").await;
    assert_ne!(third, first);
    assert_ne!(third, second);
    let third_id = recorded_operation().await;
    assert_eq!(
        third,
        pplx_space_public_api::OperationId::from_uuid(third_id).to_string()
    );
    let third_state: String = sqlx::query_scalar(sql!(
        "SELECT state FROM public_operations WHERE operation_id = $1"
    ))
    .bind(third_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(third_state, "queued", "the third attempt is dispatchable");
    Ok(())
}

/// A template policy write that binds a credential reference waits for that
/// reference's in-flight retirement, so retirement cannot report success after
/// a gateway hydrated the new binding.
#[tokio::test(flavor = "multi_thread")]
async fn a_template_policy_bind_waits_for_credential_reference_retirement()
-> pplx_space_db_test_fixtures::TestDbResult<()> {
    use pplx_space_db_test_fixtures::SEED_ORG_ID;

    let db = pplx_space_db_test_fixtures::pg_test_db_with_max_connections(4).await?;
    let template = TemplateName::parse("credential-bind-probe").expect("valid template name");
    insert_template(&db.pool, &template).await?;
    let reference = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO credential_references (id, org_id, upstream_provider, upstream_provider_config)
         VALUES ($1, $2, 'aws_secrets_manager',
                 '{\"secret_arn\": \"arn:aws:secretsmanager:us-west-2:123456789012:secret:example-AbCdEf\"}')"
    ))
    .bind(reference)
    .bind(SEED_ORG_ID)
    .execute(&db.pool)
    .await?;
    let policy: NetworkPolicyV1 = serde_json::from_value(serde_json::json!({
        "version": "v1",
        "egress": {"default_action": "allow"},
        "tls_interception": "on",
        "network_credential_rules": [{
            "rule_id": "example-api",
            "destination": {"exact_host": "api.example.com"},
            "credential_header_bindings": [{
                "header_name": "Authorization",
                "credential_reference_id": reference.to_string()
            }],
            "supported_http_methods": ["GET"]
        }]
    }))?;
    let state = AppState::from_pool(db.pool.clone())
        .await
        .expect("app state from the test pool");
    let actor = User {
        id: UserId::new(),
        email: "credential-bind-probe@example.test".to_string(),
        password_hash: String::new(),
        display_name: "credential bind probe".to_string(),
        org_id: OrgId::from_uuid(SEED_ORG_ID),
        role: crate::user_role::UserRole::OrganizationAdmin,
        owner_source: pplx_space_common_core::types::SandboxOwnerSource::Local,
        created_at: OffsetDateTime::now_utc(),
        updated_at: OffsetDateTime::now_utc(),
    };

    let mut retirement = db.pool.begin().await?;
    sqlx::query(sql!(
        "SELECT id FROM credential_references WHERE id = $1 FOR UPDATE"
    ))
    .bind(reference)
    .fetch_one(&mut *retirement)
    .await?;
    let update = tokio::spawn(update_template(
        State(state),
        Extension(AuthUser(actor)),
        Path(template.clone()),
        HeaderMap::new(),
        Json(UpdateTemplateRequest {
            description: None,
            default_auto_lifecycle: None,
            network_policy: Some(policy),
        }),
    ));
    pplx_space_db_test_fixtures::wait_for_lock_waiter(&db.pool).await?;
    assert!(
        !update.is_finished(),
        "the bind must wait for the retirement"
    );
    retirement.commit().await?;
    assert_eq!(update.await?.into_response().status(), StatusCode::OK);
    Ok(())
}
