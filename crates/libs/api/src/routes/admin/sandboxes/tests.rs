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
            crate::controllers::substrate::Substrate::from_kubernetes(
                crate::controllers::substrate::kubernetes::tests::substrate_with_store(
                    db.pool.clone(),
                ),
            ),
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

/// Admin sandbox actions take the same `Idempotency-Key` rule as the native
/// routes.
#[test]
fn admin_idempotency_key_is_header_text_within_the_cap() {
    let with_key = |value: &[u8]| {
        let mut headers = HeaderMap::new();
        headers.insert(
            IDEMPOTENCY_KEY_HEADER,
            axum::http::HeaderValue::from_bytes(value).unwrap(),
        );
        headers
    };
    let max = "k".repeat(1024);
    assert_eq!(
        idempotency_key(&with_key(max.as_bytes())).unwrap().as_str(),
        max
    );

    for (headers, message) in [
        (HeaderMap::new(), "Idempotency-Key header is required"),
        (
            with_key(format!("{max}k").as_bytes()),
            "Idempotency-Key exceeds 1024 bytes",
        ),
        (
            with_key("clé".as_bytes()),
            "Idempotency-Key must contain only printable ASCII characters, spaces and tabs",
        ),
    ] {
        let error = idempotency_key(&headers).unwrap_err();
        assert_eq!(error.status, StatusCode::BAD_REQUEST, "{message}");
        assert_eq!(error.message, message);
    }
}

/// An admin create keeps the owner as the operation's actor and, when the
/// requester is recorded, reports the administrator as `requested_by_user_id`
/// in the admin operations API; a create without it reports none.
#[tokio::test]
async fn an_admin_create_reports_its_requester_apart_from_the_owner() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let admin = UserId::from_uuid(pplx_space_db_test_fixtures::SEED_USER_ID);
    let org_id = OrgId::from_uuid(seed_org(&db.pool).await?);
    let owner = UserId::from_uuid(seed_user(&db.pool, org_id).await?);
    let store = PublicOperationStore::new(db.pool.clone());
    for (key, requested_by) in [
        ("requested-by-recorded", Some(admin)),
        ("requested-by-off", None),
    ] {
        let request = serde_json::from_value(serde_json::json!({
            "org_id": org_id,
            "user_id": owner,
            "sandbox": {
                "name": null,
                "template": "seed",
                "runtime": "fc",
                "env": {},
                "metadata": {},
                "resources": null
            },
        }))?;
        let response = create_sandbox_for_org(
            &db.pool,
            request,
            &IdempotencyKey::parse_header(key.as_bytes())?,
            requested_by,
        )
        .await
        .map_err(|error| error.message)?;
        let operation = store
            .fetch_operation_admin(response.operation_id)
            .await?
            .expect("the create's operation exists");
        assert_eq!(operation.actor_user_id, Some(owner), "{key}");
        assert_eq!(operation.requested_by_user_id, requested_by, "{key}");
        let listed = store
            .list_operations_admin(crate::public_operations::ListAdminPublicOperations {
                org_id: Some(org_id),
                state: None,
                kind: None,
                target_type: None,
                target_id: Some(response.sandbox_id.to_string()),
                limit: 10,
            })
            .await?;
        assert_eq!(listed.len(), 1, "{key}");
        assert_eq!(listed[0].requested_by_user_id, requested_by, "{key}");
    }
    db.pool.close().await;
    Ok(())
}

/// A replay of an action sent before rollbacks carried a body must match the
/// operation that request created, so only a rollback body changes the
/// fingerprint.
#[test]
fn only_a_rollback_body_changes_the_lifecycle_fingerprint() -> TestResult {
    let public_id = PublicSandboxId::from_uuid(Uuid::from_u128(7));
    let target = AdminSandboxTarget::from_public_id(public_id);
    let earlier = |action: SandboxLifecycleAction| -> TestResult<RequestFingerprint> {
        let bytes = serde_json::to_vec(&serde_json::json!({
            "sandbox_id": public_id.to_string(),
            "action": action.as_str(),
            "admin": true,
        }))?;
        Ok(RequestFingerprint::parse(hex::encode(Sha256::digest(
            bytes,
        )))?)
    };
    for action in [
        SandboxLifecycleAction::Stop,
        SandboxLifecycleAction::Rollback,
    ] {
        assert_eq!(
            lifecycle_request_fingerprint(target, action, None).map_err(|err| err.message)?,
            earlier(action)?,
            "{action:?} without a body"
        );
    }
    let rollback = |snapshot_name: &str| RollbackSandboxRequest {
        snapshot_name: snapshot_name.to_owned(),
        auto_restart: false,
    };
    let first = lifecycle_request_fingerprint(
        target,
        SandboxLifecycleAction::Rollback,
        Some(&rollback("checkpoint-1")),
    )
    .map_err(|err| err.message)?;
    assert_ne!(first, earlier(SandboxLifecycleAction::Rollback)?);
    assert_ne!(
        first,
        lifecycle_request_fingerprint(
            target,
            SandboxLifecycleAction::Rollback,
            Some(&rollback("checkpoint-2")),
        )
        .map_err(|err| err.message)?
    );
    Ok(())
}

mod cross_org_routes {
    use axum::http::Method;
    use serde_json::json;

    use super::*;
    use crate::routes::admin::test_fixtures::{
        AdminRouterHarness, admin_router_harness, admin_router_harness_with,
    };
    use crate::routes::public::hidden_operations::HiddenOperationsGate;

    async fn tenant_sandbox(harness: &AdminRouterHarness) -> TestResult<PublicSandboxId> {
        let id = seed_sandbox(&harness.db.pool, harness.tenant_org, harness.tenant_admin).await?;
        Ok(PublicSandboxId::from_uuid(id))
    }

    #[track_caller]
    fn assert_names_sandbox(
        event: &serde_json::Value,
        harness: &AdminRouterHarness,
        sandbox: PublicSandboxId,
    ) {
        assert_eq!(
            event["actor_user_id"],
            harness.operator.id.to_string(),
            "{event}"
        );
        assert_eq!(
            event["target_org_id"],
            harness.tenant_org.to_string(),
            "{event}"
        );
        assert_eq!(event["target_type"], "sandbox", "{event}");
        assert_eq!(event["target_id"], sandbox.to_string(), "{event}");
    }

    /// The session belongs to the sandbox's organization, is attributed to the
    /// operator, and its attach token is the public route's: it connects to
    /// this one terminal session and expires within the attach window.
    #[tokio::test]
    async fn an_operator_opens_a_terminal_in_another_organizations_sandbox() -> TestResult {
        let Some(harness) = admin_router_harness().await? else {
            return Ok(());
        };
        let sandbox = tenant_sandbox(&harness).await?;
        let path = format!("/sandboxes/{sandbox}/terminal-sessions");
        let body = json!({"size": {"rows": 24, "cols": 80}});
        harness
            .assert_refused_to_non_operators(Method::POST, &path, Some(body.clone()))
            .await;

        let (status, session) = harness
            .as_operator(
                Method::POST,
                &path,
                &[("idempotency-key", "terminal-1")],
                Some(body),
            )
            .await;
        assert_eq!(status, StatusCode::CREATED, "{session}");
        assert_eq!(session["sandbox_id"], sandbox.to_string());
        assert_eq!(session["kind"], "terminal");
        assert_eq!(session["scopes"], json!(["terminal:connect"]));
        let token = session["attach"]["token"].as_str().expect("attach token");
        let rows: Vec<(String, Uuid, Option<Uuid>, String, bool)> = sqlx::query_as(sql!(
            "SELECT session_id, org_id, actor_user_id, kind,
                    token_expires_at <= NOW() + INTERVAL '5 minutes'
             FROM data_plane_sessions
             WHERE token_hash = $1"
        ))
        .bind(crate::auth::hash_token(token))
        .fetch_all(&harness.db.pool)
        .await?;
        assert_eq!(
            rows,
            vec![(
                session["id"].as_str().expect("session id").to_owned(),
                harness.tenant_org.as_uuid(),
                Some(harness.operator.id.as_uuid()),
                "terminal".to_owned(),
                true,
            )],
            "the token opens only this session, under the sandbox's org"
        );

        let events = harness.audit_events(&path).await;
        assert_eq!(events.len(), 1, "{events:?}");
        assert_eq!(events[0]["status_code"], 201);
        assert_names_sandbox(&events[0], &harness, sandbox);

        let (status, error) = harness
            .as_operator(
                Method::POST,
                &path,
                &[("idempotency-key", "terminal-2")],
                Some(json!({"size": {"rows": 0, "cols": 80}})),
            )
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{error}");
        let missing = PublicSandboxId::from_uuid(Uuid::now_v7());
        let (status, _) = harness
            .as_operator(
                Method::POST,
                &format!("/sandboxes/{missing}/terminal-sessions"),
                &[("idempotency-key", "terminal-3")],
                Some(json!({})),
            )
            .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        let (status, _) = harness
            .as_operator(Method::POST, &path, &[], Some(json!({})))
            .await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "Idempotency-Key is required"
        );

        // The two refusals came after the sandbox resolved, so they name it too.
        let events = harness.audit_events(&path).await;
        let statuses: Vec<_> = events
            .iter()
            .map(|event| event["status_code"].clone())
            .collect();
        assert_eq!(statuses, vec![json!(400), json!(400), json!(201)]);
        for event in &events {
            assert_names_sandbox(event, &harness, sandbox);
        }
        let missing_events = harness
            .audit_events(&format!("/sandboxes/{missing}/terminal-sessions"))
            .await;
        assert_eq!(missing_events.len(), 1);
        assert_eq!(missing_events[0]["target_id"], serde_json::Value::Null);
        Ok(())
    }

    /// The admin route scopes its idempotency keys apart from the public
    /// route's: the tenant sending the operator's key and body gets a session
    /// of its own, and the operator's session and attach token are untouched.
    #[tokio::test]
    async fn admin_and_public_terminal_sessions_never_replay_each_other() -> TestResult {
        let Some(harness) = admin_router_harness().await? else {
            return Ok(());
        };
        let sandbox = tenant_sandbox(&harness).await?;
        let path = format!("/sandboxes/{sandbox}/terminal-sessions");
        let key = "shared-terminal-key";
        let body = json!({"size": {"rows": 24, "cols": 80}});
        let (status, admin_session) = harness
            .as_operator(
                Method::POST,
                &path,
                &[("idempotency-key", key)],
                Some(body.clone()),
            )
            .await;
        assert_eq!(status, StatusCode::CREATED, "{admin_session}");
        let admin_session_id = admin_session["id"].as_str().expect("session id").to_owned();
        let token_hash = |session_id: String| {
            let pool = harness.db.pool.clone();
            async move {
                sqlx::query_scalar::<_, Option<String>>(sql!(
                    "SELECT token_hash FROM data_plane_sessions WHERE session_id = $1"
                ))
                .bind(session_id)
                .fetch_one(&pool)
                .await
            }
        };
        let admin_token_hash = token_hash(admin_session_id.clone()).await?;

        let tenant_session = crate::routes::public::sessions::create_terminal_session_for_sandbox(
            &harness.state,
            harness.tenant_org,
            harness.tenant_admin,
            sandbox,
            serde_json::from_value(body.clone())?,
            IdempotencyKey::parse(key.to_owned())?,
            crate::public_operations::OperationPathTemplate::new(
                pplx_space_public_api::routes::CREATE_TERMINAL_SESSION.path(),
            ),
        )
        .await
        .map_err(|error| format!("public create failed: {error:?}"))?;
        assert_ne!(tenant_session.id.to_string(), admin_session_id);
        assert_eq!(
            token_hash(admin_session_id.clone()).await?,
            admin_token_hash,
            "the operator's attach token was not rotated"
        );

        let (status, replay) = harness
            .as_operator(Method::POST, &path, &[("idempotency-key", key)], Some(body))
            .await;
        assert_eq!(status, StatusCode::CREATED, "{replay}");
        assert_eq!(
            replay["id"],
            admin_session_id.as_str(),
            "the admin route replays its own"
        );
        Ok(())
    }

    #[tokio::test]
    async fn an_operator_lists_another_organizations_snapshots_and_the_read_is_audited()
    -> TestResult {
        let Some(harness) = admin_router_harness().await? else {
            return Ok(());
        };
        let sandbox = tenant_sandbox(&harness).await?;
        let snapshot_id = Uuid::now_v7();
        sqlx::query(sql!(
            "INSERT INTO snapshots (
                 id, sandbox_id, type, state, local_artifact_id,
                 artifact_kind, artifact_origin, artifact_snapshot_name, label
             ) VALUES ($1, $2, 'disk', 'ready', $3, 'disk', 'snapshot', 'checkpoint', 'before-upgrade')"
        ))
        .bind(snapshot_id)
        .bind(sandbox.as_uuid())
        .bind(Uuid::now_v7())
        .execute(&harness.db.pool)
        .await?;
        let path = format!("/sandboxes/{sandbox}/snapshots");
        harness
            .assert_refused_to_non_operators(Method::GET, &path, None)
            .await;

        let (status, page) = harness.as_operator(Method::GET, &path, &[], None).await;
        assert_eq!(status, StatusCode::OK, "{page}");
        let data = page["data"].as_array().expect("snapshot page");
        assert_eq!(data.len(), 1, "{page}");
        assert_eq!(
            data[0]["id"],
            pplx_space_public_api::PublicSnapshotId::from(
                pplx_space_common_core::types::SnapshotId::from_uuid(snapshot_id)
            )
            .to_string()
        );
        assert_eq!(data[0]["label"], "before-upgrade");
        assert_eq!(page["next_cursor"], serde_json::Value::Null);

        let events = harness.audit_events(&path).await;
        assert_eq!(
            events.len(),
            1,
            "a read of another org's snapshots is audited"
        );
        assert_eq!(events[0]["method"], "GET");
        assert_names_sandbox(&events[0], &harness, sandbox);

        for query in ["cursor=not-a-cursor", "limit=many"] {
            let (status, _) = harness
                .as_operator(Method::GET, &format!("{path}?{query}"), &[], None)
                .await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{query}");
            let events = harness.audit_events(&format!("{path}?{query}")).await;
            assert!(events.is_empty(), "the path is recorded without its query");
        }
        let events = harness.audit_events(&path).await;
        let statuses: Vec<_> = events
            .iter()
            .map(|event| event["status_code"].clone())
            .collect();
        assert_eq!(
            statuses,
            vec![json!(400), json!(400), json!(200)],
            "a refused read of a resolved sandbox is audited"
        );
        for event in &events {
            assert_names_sandbox(event, &harness, sandbox);
        }
        let missing = PublicSandboxId::from_uuid(Uuid::now_v7());
        let (status, _) = harness
            .as_operator(
                Method::GET,
                &format!("/sandboxes/{missing}/snapshots"),
                &[],
                None,
            )
            .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        Ok(())
    }

    /// With rollback enabled for the sandbox's organization, `:force-rollback`
    /// carries the public `:rollback` body to the operation, under that
    /// organization, and the body is part of the request's idempotency
    /// fingerprint.
    #[tokio::test]
    async fn an_operator_rolls_back_another_organizations_sandbox() -> TestResult {
        let Some(harness) = admin_router_harness_with(|state| {
            state.native_hidden_operations = HiddenOperationsGate::all_orgs();
        })
        .await?
        else {
            return Ok(());
        };
        let sandbox = tenant_sandbox(&harness).await?;
        let path = format!("/sandboxes/{sandbox}:force-rollback");
        let body = json!({"snapshot_name": "checkpoint-1", "auto_restart": true});
        harness
            .assert_refused_to_non_operators(Method::POST, &path, Some(body.clone()))
            .await;

        let key = [("idempotency-key", "rollback-1")];
        let (status, accepted) = harness
            .as_operator(Method::POST, &path, &key, Some(body.clone()))
            .await;
        assert_eq!(status, StatusCode::ACCEPTED, "{accepted}");
        let operation_id = accepted["operation_id"]
            .as_str()
            .expect("operation id")
            .to_owned();
        let (org_id, actor, kind, payload): (Uuid, Option<Uuid>, String, serde_json::Value) =
            sqlx::query_as(sql!(
                "SELECT org_id, actor_user_id, kind, payload_json
                 FROM public_operations
                 WHERE operation_id = $1"
            ))
            .bind(OperationId::from_str(&operation_id)?.as_uuid())
            .fetch_one(&harness.db.pool)
            .await?;
        assert_eq!(org_id, harness.tenant_org.as_uuid());
        assert_eq!(actor, Some(harness.operator.id.as_uuid()));
        assert_eq!(kind, OperationKind::RollbackSandbox.as_str());
        assert_eq!(payload["rollback"], body, "{payload}");

        let (status, replay) = harness
            .as_operator(Method::POST, &path, &key, Some(body))
            .await;
        assert_eq!(status, StatusCode::ACCEPTED, "{replay}");
        assert_eq!(replay["operation_id"], operation_id.as_str());
        let (status, conflict) = harness
            .as_operator(
                Method::POST,
                &path,
                &key,
                Some(json!({"snapshot_name": "checkpoint-2"})),
            )
            .await;
        assert_eq!(status, StatusCode::CONFLICT, "{conflict}");

        for missing_body in [None, Some(json!({"auto_restart": true}))] {
            let (status, error) = harness
                .as_operator(
                    Method::POST,
                    &path,
                    &[("idempotency-key", "rollback-2")],
                    missing_body,
                )
                .await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{error}");
        }
        let events = harness.audit_events(&path).await;
        assert_eq!(events.len(), 5, "{events:?}");
        for event in &events {
            assert_names_sandbox(event, &harness, sandbox);
        }
        Ok(())
    }

    /// Rollback stays behind the public `:rollback`'s hidden-operation gate:
    /// with it closed for the sandbox's organization, `:force-rollback` enqueues
    /// nothing, and the refusal is audited against the sandbox.
    #[tokio::test]
    async fn rollback_is_refused_while_its_gate_is_closed() -> TestResult {
        let Some(harness) = admin_router_harness_with(|state| {
            state.native_hidden_operations = HiddenOperationsGate::default();
        })
        .await?
        else {
            return Ok(());
        };
        let sandbox = tenant_sandbox(&harness).await?;
        let path = format!("/sandboxes/{sandbox}:force-rollback");
        let (status, error) = harness
            .as_operator(
                Method::POST,
                &path,
                &[("idempotency-key", "rollback-closed")],
                Some(json!({"snapshot_name": "checkpoint-1"})),
            )
            .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{error}");
        assert!(
            error["error"]
                .as_str()
                .unwrap_or_default()
                .contains("rollback is not enabled"),
            "{error}"
        );
        let operations: i64 = sqlx::query_scalar(sql!(
            "SELECT count(*) FROM public_operations WHERE target_id = $1"
        ))
        .bind(sandbox.to_string())
        .fetch_one(&harness.db.pool)
        .await?;
        assert_eq!(operations, 0);
        let events = harness.audit_events(&path).await;
        assert_eq!(events.len(), 1, "{events:?}");
        assert_names_sandbox(&events[0], &harness, sandbox);

        let (status, accepted) = harness
            .as_operator(
                Method::POST,
                &format!("/sandboxes/{sandbox}:force-stop"),
                &[("idempotency-key", "stop-open")],
                None,
            )
            .await;
        assert_eq!(
            status,
            StatusCode::ACCEPTED,
            "other actions are ungated: {accepted}"
        );
        Ok(())
    }

    /// Once the sandbox or organization resolves, a refused admin sandbox write
    /// names it in the audit event, as a successful one does.
    #[tokio::test]
    async fn refused_sandbox_writes_name_the_resolved_target() -> TestResult {
        let Some(harness) = admin_router_harness().await? else {
            return Ok(());
        };
        let sandbox = tenant_sandbox(&harness).await?;
        let command = json!({"command": ["true"], "cwd": null, "env": {}, "pty": null, "stdin": false, "timeout": null});
        let refusals = [
            (
                Method::POST,
                format!("/sandboxes/{sandbox}/commands"),
                Some(command),
            ),
            (
                Method::POST,
                format!("/sandboxes/{sandbox}:force-stop"),
                None,
            ),
            (
                Method::PATCH,
                format!("/sandboxes/{sandbox}"),
                Some(json!({"resources": {"disk_mib": 2048}})),
            ),
        ];
        for (method, path, body) in refusals {
            // No Idempotency-Key, or an immutable field: refused after lookup.
            let (status, error) = harness.as_operator(method.clone(), &path, &[], body).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{method} {path}: {error}");
            let events = harness.audit_events(&path).await;
            assert_eq!(events.len(), 1, "{method} {path}: {events:?}");
            assert_names_sandbox(&events[0], &harness, sandbox);
        }

        let create = |user_id: Option<UserId>| {
            json!({
                "org_id": harness.tenant_org,
                "user_id": user_id,
                "sandbox": {"name": null, "template": "seed", "runtime": "fc", "env": {}, "metadata": {}, "resources": null},
            })
        };
        let (status, error) = harness
            .as_operator(
                Method::POST,
                "/sandboxes",
                &[("idempotency-key", "create-foreign-owner")],
                Some(create(Some(harness.operator.id))),
            )
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{error}");
        let (status, accepted) = harness
            .as_operator(
                Method::POST,
                "/sandboxes",
                &[("idempotency-key", "create-tenant-owner")],
                Some(create(None)),
            )
            .await;
        assert_eq!(status, StatusCode::ACCEPTED, "{accepted}");
        let events = harness.audit_events("/sandboxes").await;
        assert_eq!(events.len(), 2, "{events:?}");
        assert_eq!(events[0]["target_id"], accepted["sandbox_id"]);
        assert_eq!(events[1]["target_id"], serde_json::Value::Null);
        for event in &events {
            assert_eq!(event["target_org_id"], harness.tenant_org.to_string());
            assert_eq!(event["target_type"], "sandbox");
        }
        Ok(())
    }
}
