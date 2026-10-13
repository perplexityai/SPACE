use pplx_space_common_core::types::{OrgId, UserId};
use uuid::Uuid;

use pplx_space_db_test_fixtures::try_pg_test_db;

use super::*;

use pplx_space_test_env::TestResult;

#[test]
fn admin_create_fingerprint_omits_node_pool_key_when_absent() {
    let org_id = OrgId::from_uuid(Uuid::from_u128(3));
    let owner_id = Uuid::from_u128(7);
    let request: AdminCreateSandboxRequest = serde_json::from_value(serde_json::json!({
        "org_id": org_id.as_uuid(),
        "sandbox": {
            "name": null,
            "template": "seed",
            "runtime": "fc",
            "env": {},
            "metadata": {},
            "resources": null,
        },
    }))
    .unwrap();

    // None case must hash the exact legacy JSON shape (no `node_pool` key),
    // so retries of pre-placement requests keep the same fingerprint.
    let legacy = serde_json::to_vec(&serde_json::json!({
        "org_id": org_id.as_uuid(),
        "user_id": owner_id,
        "sandbox": request.sandbox,
    }))
    .unwrap();
    let expected_none = RequestFingerprint::parse(hex::encode(Sha256::digest(legacy))).unwrap();
    let actual_none =
        admin_create_fingerprint(org_id, UserId::from_uuid(owner_id), &request.sandbox, None)
            .unwrap();
    assert_eq!(actual_none, expected_none);

    // A pin changes the fingerprint (and only then adds the key).
    let pool = NodePoolName::parse("isolated-test-pool").unwrap();
    let with_pool = admin_create_fingerprint(
        org_id,
        UserId::from_uuid(owner_id),
        &request.sandbox,
        Some(&pool),
    )
    .unwrap();
    assert_ne!(with_pool, expected_none);
}

#[test]
fn admin_node_pool_requires_enabled_gate_and_kubernetes_substrate() {
    let node_pool = NodePoolName::parse("isolated-test-pool").unwrap();

    let disabled = validate_admin_node_pool(Some(&node_pool), false, true).unwrap_err();
    assert_eq!(disabled.status, StatusCode::BAD_REQUEST);
    assert_eq!(disabled.message, "sandbox node-pool placement is disabled");

    let unsupported = validate_admin_node_pool(Some(&node_pool), true, false).unwrap_err();
    assert_eq!(unsupported.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        unsupported.message,
        "sandbox node-pool placement requires the Kubernetes substrate"
    );

    validate_admin_node_pool(Some(&node_pool), true, true).unwrap();
    validate_admin_node_pool(None, false, false).unwrap();
}

#[test]
fn admin_sandbox_mutations_reject_v2_policy_documents() {
    let v1_create = serde_json::json!({
        "org_id": "00000000-0000-0000-0000-000000000003",
        "sandbox": {
            "name": null,
            "template": "seed",
            "runtime": "fc",
            "env": {},
            "metadata": {},
            "resources": null,
            "network_policy": {
                "version": "v1",
                "egress": {"default_action": "deny"}
            }
        }
    });
    serde_json::from_value::<AdminCreateSandboxRequest>(v1_create.clone()).unwrap();
    let mut v2_create = v1_create;
    v2_create["sandbox"]["network_policy"] = serde_json::json!({
        "version": "v2",
        "egress": {"deny": [], "allow": []}
    });
    serde_json::from_value::<AdminCreateSandboxRequest>(v2_create).unwrap_err();

    let v1_update = serde_json::json!({
        "network_policy": {
            "mode": "set",
            "policy": {
                "version": "v1",
                "egress": {"default_action": "deny"}
            }
        }
    });
    serde_json::from_value::<PublicUpdateSandboxRequest>(v1_update.clone()).unwrap();
    let mut v2_update = v1_update;
    v2_update["network_policy"]["policy"] = serde_json::json!({
        "version": "v2",
        "egress": {"deny": [], "allow": []}
    });
    serde_json::from_value::<PublicUpdateSandboxRequest>(v2_update).unwrap_err();
}

#[test]
fn admin_create_request_parses_console_shape_and_rejects_unknown_fields() {
    let request: AdminCreateSandboxRequest = serde_json::from_value(serde_json::json!({
        "org_id": "00000000-0000-0000-0000-000000000003",
        "sandbox": {
            "name": null,
            "template": "seed",
            "runtime": "fc",
            "env": {},
            "metadata": {},
            "resources": null,
        },
    }))
    .unwrap();
    assert!(request.user_id.is_none());
    assert_eq!(request.sandbox.template.name().as_str(), "seed");

    // The sandbox spec must be nested, not flattened into the top level.
    let err = serde_json::from_value::<AdminCreateSandboxRequest>(serde_json::json!({
        "org_id": "00000000-0000-0000-0000-000000000003",
        "template": "seed",
    }))
    .unwrap_err();
    assert!(err.to_string().contains("field"), "{err}");
}

#[tokio::test]
async fn invalid_admin_node_pool_is_bad_request() -> TestResult {
    use axum::{body::Body, http::Request};

    let request = Request::builder()
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::json!({
                "org_id": "00000000-0000-0000-0000-000000000003",
                "sandbox": {
                    "name": null,
                    "template": "seed",
                    "runtime": "fc",
                    "env": {},
                    "metadata": {},
                    "resources": null
                },
                "node_pool": "-invalid"
            })
            .to_string(),
        ))?;

    let rejection = AdminCreateJson::from_request(request, &())
        .await
        .expect_err("invalid node_pool must be rejected");

    assert_eq!(rejection.into_response().status(), StatusCode::BAD_REQUEST);
    Ok(())
}

#[tokio::test]
async fn admin_create_preserves_missing_json_content_type_status() -> TestResult {
    use axum::{body::Body, http::Request};

    let request = Request::builder().body(Body::from("{}"))?;
    let rejection = AdminCreateJson::from_request(request, &())
        .await
        .expect_err("missing JSON content type must be rejected");

    assert_eq!(
        rejection.into_response().status(),
        StatusCode::UNSUPPORTED_MEDIA_TYPE
    );
    Ok(())
}

#[test]
fn parse_spec_accepts_force_lifecycle_action() {
    let sandbox_id = PublicSandboxId::new();
    let (target, action) = parse_spec(&format!("{sandbox_id}:force-suspend")).unwrap();

    assert_eq!(target.public_id, sandbox_id);
    assert_eq!(target.domain_id, SandboxId::from(sandbox_id));
    assert_eq!(action, SandboxLifecycleAction::Suspend);
}

#[test]
fn parse_spec_requires_public_id_and_force_prefix() {
    parse_spec("not-a-sandbox:force-suspend").unwrap_err();
    let sandbox_id = PublicSandboxId::new();
    parse_spec(&format!("{sandbox_id}:suspend")).unwrap_err();
    parse_spec(&format!("{sandbox_id}:force-migrate")).unwrap_err();
}

#[test]
fn admin_action_templates_are_per_action_for_idempotency() {
    assert_eq!(
        admin_action_path_template(SandboxLifecycleAction::Pause).as_str(),
        "/v1/sandboxes/{sandbox_id}:force-pause"
    );
    assert_eq!(
        admin_action_label(SandboxLifecycleAction::Delete),
        "force-delete"
    );
}

/// The console edits network policy on sandboxes across every org. The
/// org-scoped public `PATCH /v1/sandboxes/{id}` rejects a sandbox owned by
/// another org with "sandbox not found"; the admin update surface must
/// enqueue the update under the sandbox's owning org instead, attributed
/// to the operator who issued it.
#[tokio::test]
async fn admin_update_enqueues_under_sandbox_org_for_cross_org_caller() -> TestResult {
    use pplx_space_net_common::NetworkPolicyV1;
    use pplx_space_public_api::SandboxNetworkPolicyPatch;

    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };

    // Sandbox owned by a customer org.
    let owner_org = seed_org(&db.pool).await?;
    let owner_user = seed_user(&db.pool, OrgId::from_uuid(owner_org)).await?;
    let sandbox_id = seed_sandbox(
        &db.pool,
        OrgId::from_uuid(owner_org),
        UserId::from_uuid(owner_user),
    )
    .await?;
    let public_sandbox_id = PublicSandboxId::from_uuid(sandbox_id);
    let target = AdminSandboxTarget::from_public_id(public_sandbox_id);

    // The operator belongs to a different (managing) org — the org-scoped
    // ownership predicate the public PATCH runs does not match, which is
    // the "sandbox not found" the console used to surface.
    let managing_org = seed_org(&db.pool).await?;
    let managing_admin = seed_user(&db.pool, OrgId::from_uuid(managing_org)).await?;
    let owned_by_operator_org: bool = sqlx::query_scalar(sql!(
        "SELECT EXISTS(SELECT 1 FROM sandboxes WHERE id = $1 AND org_id = $2)"
    ))
    .bind(sandbox_id)
    .bind(managing_org)
    .fetch_one(&db.pool)
    .await?;
    assert!(!owned_by_operator_org);

    let request = PublicUpdateSandboxRequest {
        metadata: None,
        resources: None,
        network_policy: Some(SandboxNetworkPolicyPatch::Set {
            policy: NetworkPolicyV1::allow_all(),
        }),
        auto_lifecycle: None,
    };
    let idempotency_key = IdempotencyKey::parse(Uuid::now_v7().to_string())?;
    let response = enqueue_admin_update_operation(
        &db.pool,
        OrgId::from_uuid(owner_org),
        UserId::from_uuid(managing_admin),
        target,
        request,
        &idempotency_key,
    )
    .await
    .map_err(|err| format!("enqueue failed: {}", err.message))?;
    assert_eq!(response.sandbox_id, public_sandbox_id);

    let (op_org, op_actor, op_kind, op_target): (Uuid, Option<Uuid>, String, Option<String>) =
        sqlx::query_as(sql!(
            "SELECT org_id, actor_user_id, kind, target_id
             FROM public_operations WHERE operation_id = $1"
        ))
        .bind(response.operation_id.as_uuid())
        .fetch_one(&db.pool)
        .await?;
    assert_eq!(op_org, owner_org);
    assert_eq!(op_actor, Some(managing_admin));
    assert_eq!(op_kind, OperationKind::UpdateSandbox.as_str());
    assert_eq!(
        op_target.as_deref(),
        Some(public_sandbox_id.to_string().as_str())
    );
    Ok(())
}

async fn seed_org(pool: &sqlx::PgPool) -> Result<Uuid, sqlx::Error> {
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
    Ok(id)
}

async fn seed_user(pool: &sqlx::PgPool, org_id: OrgId) -> Result<Uuid, sqlx::Error> {
    let id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO users (id, email, password_hash, display_name, org_id, role)
         VALUES ($1, $2, 'hash', 'Test User', $3, 'member')"
    ))
    .bind(id)
    .bind(format!("{id}@example.test"))
    .bind(org_id)
    .execute(pool)
    .await?;
    Ok(id)
}

async fn seed_sandbox(
    pool: &sqlx::PgPool,
    org_id: OrgId,
    user_id: UserId,
) -> Result<Uuid, sqlx::Error> {
    let id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, managed_by, lifecycle, placement,
             network_policy_generation
         )
         VALUES ($1, $2, $3, 'k8s', 'started', 'placed', 1)"
    ))
    .bind(id)
    .bind(user_id)
    .bind(org_id)
    .execute(pool)
    .await?;
    Ok(id)
}

/// The admin sandbox surface's `public_operations` arms carry the disposition
/// into this module's own error type, split by scope.
///
/// Every admin create, update, and lifecycle action inserts an operation row,
/// and the idempotent-replay path reads one back; all four arms turned any
/// store error into a flat 500. The three writes take the 409 for a
/// serialization abort — the operation is keyed on the caller's
/// `Idempotency-Key`, so a replay of the same request adopts the existing
/// operation instead of opening a second one — while the replay read does not,
/// because it wrote nothing for the 409's wording to describe.
///
/// The SQLSTATE table itself lives once in
/// `routes::admin::tests::a_read_only_statement_is_never_blamed_for_a_write_interleave`;
/// what is asserted here is that `store_action_error` forwards the scope and
/// returns an `AdminSandboxActionError` carrying the right status.
///
/// Asserts the wrapper, not the handler arm that calls it, so it does not catch
/// that call being dropped — only
/// `routes::debug::sandbox::tests::the_debug_handler_answers_a_contended_read_with_a_503`
/// and
/// `controllers::sandbox::lifecycle::tests::a_contended_delete_precheck_stays_retryable`
/// drive production code far enough for that.
#[tokio::test]
async fn the_operation_store_arms_carry_the_disposition_by_scope()
-> pplx_space_db_test_fixtures::TestDbResult<()> {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let wrap = PublicOperationStoreError::Sql;

    let lock_wait = wrap(pplx_space_db_test_fixtures::raise_sqlstate(&db.pool, "55P03").await);
    for scope in [ContentionScope::ReadOnly, ContentionScope::Writing] {
        assert_eq!(
            store_action_error(scope, &lock_wait).map(|err| err.status),
            Some(StatusCode::SERVICE_UNAVAILABLE),
            "{scope:?}: a lock wait is retryable on any statement"
        );
    }

    let deadlock = wrap(pplx_space_db_test_fixtures::raise_sqlstate(&db.pool, "40P01").await);
    assert_eq!(
        store_action_error(ContentionScope::Writing, &deadlock).map(|err| err.status),
        Some(StatusCode::CONFLICT),
        "the operation insert did race another admin write"
    );
    assert_eq!(
        store_action_error(ContentionScope::ReadOnly, &deadlock).map(|err| err.status),
        Some(StatusCode::SERVICE_UNAVAILABLE),
        "the idempotent replay read wrote nothing that could have interleaved, \
         but the retry still succeeds"
    );

    // The contention 409 and the permanent idempotency 409 are the only two
    // 409s these handlers return, so the status cannot separate them. The
    // body's `retryable` is what does — asserted against the permanent one so
    // the flag is provably discriminating rather than merely always set.
    let contended = store_action_error(ContentionScope::Writing, &deadlock)
        .expect("a deadlock on the operation insert is contention");
    assert!(
        contended.retryable,
        "the identical retry resolves a lock race"
    );

    // Built through the constructor the three production sites use, so this
    // covers the response a reused key actually produces rather than a
    // stand-in with the same fields.
    let reused_key = AdminSandboxActionError::idempotency_conflict(None);
    assert_eq!(reused_key.status, contended.status);
    assert!(
        !reused_key.retryable,
        "a reused idempotency key is permanent, and shares its status with the \
         contention 409 — which is why the flag has to carry the difference"
    );

    // And the default is derived from the status, not hardcoded: a plain 500
    // from a read that wrote nothing stays retryable, matching
    // `PublicApiError::internal` and `templates::api_error`.
    assert!(
        AdminSandboxActionError::new(StatusCode::INTERNAL_SERVER_ERROR, "boom").retryable,
        "a 500 on this surface is retryable like every other 500 in the crate"
    );
    assert!(
        !AdminSandboxActionError::new(StatusCode::NOT_FOUND, "gone").retryable,
        "a 404 is not"
    );

    // The admission-denied refusal is the same condition on three routes. All
    // three build it through `new` so they cannot diverge in the source — but
    // this assertion only pins `new`'s answer, not those three call sites:
    // reverting any one of them to a struct literal compiles and leaves this
    // green. (They diverged exactly that way earlier in this branch, when the
    // `retryable` field was added and one site was still a literal. On `main`
    // all three were identical, because none of them carried the field.)
    let admission_denied = AdminSandboxActionError::new(
        StatusCode::INTERNAL_SERVER_ERROR,
        "operation unexpectedly refused admission",
    );
    assert!(
        admission_denied.retryable,
        "derived from its 500 like every other 500 on this surface"
    );

    db.pool.close().await;
    Ok(())
}

/// `create_sandbox_for_org`'s three prechecks answer a lost lock race as
/// retryable, not as an internal failure.
///
/// The `organizations` existence check and both `users` owner lookups are
/// plain reads that run before the create writes anything. They mapped through
/// a `&dyn Display` closure, which erased the `sqlx::Error` at the map site —
/// so a `55P03` on any of them answered "Internal server error" while the
/// `create_operation` write further down the same function answered 503 for
/// the identical abort — and 409 for a serialization abort, which these reads
/// never take. Preserve the database error through classification.
#[tokio::test]
async fn the_create_prechecks_answer_a_lost_lock_race_as_retryable()
-> pplx_space_db_test_fixtures::TestDbResult<()> {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let org_id = OrgId::from_uuid(pplx_space_db_test_fixtures::SEED_ORG_ID);

    let lock_wait = pplx_space_db_test_fixtures::raise_sqlstate(&db.pool, "55P03").await;
    let contended = create_precheck_failure(org_id, &lock_wait);
    assert_eq!(contended.status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(contended.retryable);

    // A read wrote nothing, so it never takes the write-specific 409 — but the
    // abort is still retryable.
    let deadlock = pplx_space_db_test_fixtures::raise_sqlstate(&db.pool, "40P01").await;
    assert_eq!(
        create_precheck_failure(org_id, &deadlock).status,
        StatusCode::SERVICE_UNAVAILABLE
    );

    // Outside the contention set: a real failure, and the caller's 500.
    let violation = pplx_space_db_test_fixtures::raise_sqlstate(&db.pool, "23505").await;
    assert_eq!(
        create_precheck_failure(org_id, &violation).status,
        StatusCode::INTERNAL_SERVER_ERROR
    );

    db.pool.close().await;
    Ok(())
}

/// The admin read arms must take the controller's contention disposition rather
/// than flattening it to a 500.
///
/// `list_responses` and `fetch_admin_sandbox_response` classify contention, so
/// a lost lock race reaches these arms already marked. They previously answered
/// a hardcoded `INTERNAL_SERVER_ERROR`, which threw the classification away one
/// layer above the statement that produced it — and a status-only check would
/// not have noticed, because the un-flattened error is a 503 either way.
///
/// Asserts both directions: the flatten is what was wrong, so a test that only
/// pinned the contention case would pass on a helper that answered 503 for
/// every failure.
#[test]
fn admin_sandbox_reads_keep_the_controller_contention_disposition() {
    let contended = super::super::support::controller_read_error(
        &crate::controllers::ControllerError::lock_contention(),
        || panic!("the contention path must not run the caller's error log"),
    );
    assert_eq!(
        contended.status(),
        StatusCode::SERVICE_UNAVAILABLE,
        "an admin read that lost a lock race is not a 500"
    );
    assert!(
        contended
            .headers()
            .contains_key(axum::http::header::RETRY_AFTER),
        "the retryable answer must carry its pacing hint, or the caller retries immediately \
         against the row that is already contended"
    );

    let mut logged = false;
    let genuine = super::super::support::controller_read_error(
        &crate::controllers::ControllerError::internal("projection blew up"),
        || logged = true,
    );
    assert_eq!(
        genuine.status(),
        StatusCode::INTERNAL_SERVER_ERROR,
        "a real projection failure stays a 500"
    );
    assert!(
        logged,
        "the non-contention path must still run the call site's log; it is the only record"
    );
}

/// With placement enabled, an admin create pinned to a node pool is refused
/// on a substrate without node-pool placement (Direct) and accepted, with the
/// pin persisted in the queued create, on one that has it (Kubernetes).
#[tokio::test]
async fn admin_pinned_create_follows_the_running_substrate() -> TestResult {
    // Turn the placement gate on, and keep the substrate `AppState::from_pool`
    // chooses from the process environment Direct whatever the host sets.
    if !pplx_space_test_env::enter_test_process(&[
        (
            pplx_space_common_core::env::SANDBOX_ALLOW_PLACEMENT_ENV,
            Some(std::ffi::OsStr::new("true")),
        ),
        (
            "SPACE_K8S_CONTROL_PLANE",
            Some(std::ffi::OsStr::new("false")),
        ),
    ]) {
        return Ok(());
    }
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let actor = sqlx::query_as::<_, crate::models::User>(sql!(
        "SELECT u.*
         FROM users u
         JOIN clusters c ON c.managing_org_id = u.org_id
         WHERE c.singleton
           AND u.role = 'organization_admin'
         ORDER BY u.id
         LIMIT 1"
    ))
    .fetch_one(&db.pool)
    .await?;
    let direct = AppState::from_pool(db.pool.clone()).await?;
    assert_eq!(direct.sandbox_controller.managed_by(), "space");

    #[cfg_attr(not(feature = "kubernetes"), allow(unused_mut))]
    let mut cases = vec![(direct, StatusCode::BAD_REQUEST)];
    #[cfg(feature = "kubernetes")]
    {
        let mut kubernetes = AppState::from_pool(db.pool.clone()).await?;
        kubernetes.sandbox_controller = crate::controllers::sandbox::SandboxController::new(
            crate::controllers::substrate::Substrate::Kubernetes(Box::new(
                crate::controllers::substrate::kubernetes::tests::substrate_with_store(
                    db.pool.clone(),
                ),
            )),
        );
        cases.push((kubernetes, StatusCode::ACCEPTED));
    }

    for (state, expected_status) in cases {
        let substrate = state.sandbox_controller.managed_by();
        let org_id = seed_org(&db.pool).await?;
        seed_user(&db.pool, OrgId::from_uuid(org_id)).await?;
        let request = serde_json::from_value(serde_json::json!({
            "org_id": org_id,
            "sandbox": {
                "name": null,
                "template": "seed",
                "runtime": "fc",
                "env": {},
                "metadata": {},
                "resources": null
            },
            "node_pool": "isolated-test-pool"
        }))?;
        let mut headers = HeaderMap::new();
        headers.insert("idempotency-key", Uuid::now_v7().to_string().parse()?);

        let response = create_sandbox(
            State(state.clone()),
            Extension(AuthUser(actor.clone())),
            headers,
            AdminCreateJson(request),
        )
        .await;

        assert_eq!(response.status(), expected_status, "{substrate} substrate");
        let persisted_pins: Vec<Option<String>> = sqlx::query_scalar(sql!(
            "SELECT payload_json->>'node_pool' FROM public_operations WHERE org_id = $1"
        ))
        .bind(org_id)
        .fetch_all(&db.pool)
        .await?;
        if expected_status == StatusCode::ACCEPTED {
            assert_eq!(
                persisted_pins,
                [Some("isolated-test-pool".to_owned())],
                "{substrate} substrate"
            );
        } else {
            let body: serde_json::Value = serde_json::from_slice(
                &axum::body::to_bytes(response.into_body(), 100_000).await?,
            )?;
            assert_eq!(
                body["error"], "sandbox node-pool placement requires the Kubernetes substrate",
                "{substrate} substrate"
            );
            assert!(persisted_pins.is_empty(), "{substrate} substrate");
        }
    }
    Ok(())
}
