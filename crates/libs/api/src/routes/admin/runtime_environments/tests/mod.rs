use axum::Router;
use axum::body::Body;
use axum::http::{Method, Request};
use pplx_space_common_core::sql;
use pplx_space_common_core::types::{Digest, OrgId, SandboxId, SubstrateKind};
use serde_json::{Value, json};
use sqlx::PgPool;
use tower::ServiceExt;
use uuid::Uuid;

use super::*;
use pplx_space_db_test_fixtures::insert_ready_worker_node;

use crate::routes::admin::test_fixtures::{Fixture, TestResult, postgres_fixture};

/// The managing org the migrations seed.
const SEED_ORG: Uuid = Uuid::from_u128(0x1);

fn environment(byte: u8) -> EnvironmentReference {
    EnvironmentReference::new(Digest::sha256([byte; 32]))
}

fn observation(
    allowed: &[EnvironmentReference],
    default: Option<EnvironmentReference>,
    legacy_unbound_allowed: bool,
) -> ExecutionPolicyObservation {
    ExecutionPolicyObservation::new(
        1,
        allowed.to_vec(),
        default,
        legacy_unbound_allowed,
        None,
        false,
    )
    .expect("valid observation")
}

/// An observation from a node that applied cluster policy `generation`.
fn applied_observation(
    allowed: &[EnvironmentReference],
    default: Option<EnvironmentReference>,
    generation: u64,
) -> ExecutionPolicyObservation {
    ExecutionPolicyObservation::new(2, allowed.to_vec(), default, false, Some(generation), false)
        .expect("valid applied observation")
}

fn policy(
    default: Option<EnvironmentReference>,
    states: &[(EnvironmentReference, RuntimeEnvironmentState)],
    legacy_unbound_allowed: Option<bool>,
) -> RuntimeEnvironmentPolicy {
    RuntimeEnvironmentPolicy {
        generation: 7,
        default_environment: default,
        legacy_unbound_allowed,
        environments: states.iter().copied().collect(),
    }
}

#[test]
fn conformance_compares_the_published_policy_with_the_cluster_policy() {
    use RuntimeEnvironmentState::{Allowed, Blocked, Deprecated};
    let (e0, e1, e2) = (environment(0), environment(1), environment(2));
    let cluster = policy(
        Some(e1),
        &[(e0, Deprecated), (e1, Allowed), (e2, Blocked)],
        Some(false),
    );

    assert_eq!(conformance(&cluster, None), NodePolicyConformance::Unknown);
    // Exactly the executing set.
    assert_eq!(
        conformance(&cluster, Some(&observation(&[e0, e1], Some(e1), false))),
        NodePolicyConformance::Conforms
    );
    // A build that certifies less narrows further, which still conforms.
    assert_eq!(
        conformance(&cluster, Some(&observation(&[e1], Some(e1), false))),
        NodePolicyConformance::Conforms
    );
    // Still executing a blocked environment, on the old default.
    assert_eq!(
        conformance(&cluster, Some(&observation(&[e0, e2], Some(e0), false))),
        NodePolicyConformance::Diverges {
            default_differs: true,
            executes_outside_policy: vec![e2],
            allows_revoked_legacy_unbound: false,
        }
    );
    // An environment with no state row doesn't execute either.
    let e3 = environment(3);
    assert_eq!(
        conformance(&cluster, Some(&observation(&[e1, e3], Some(e1), false))),
        NodePolicyConformance::Diverges {
            default_differs: false,
            executes_outside_policy: vec![e3],
            allows_revoked_legacy_unbound: false,
        }
    );
    assert_eq!(
        conformance(&cluster, Some(&observation(&[e1], Some(e1), true))),
        NodePolicyConformance::Diverges {
            default_differs: false,
            executes_outside_policy: vec![],
            allows_revoked_legacy_unbound: true,
        }
    );
}

#[test]
fn without_a_cluster_default_only_a_revoked_legacy_permission_can_diverge() {
    let (e0, e1) = (environment(0), environment(1));
    // A deprecated state with no default narrows nothing on a node.
    let bootstrap = policy(None, &[(e1, RuntimeEnvironmentState::Deprecated)], None);
    for published in [
        observation(&[e0], Some(e0), false),
        observation(&[e0, e1], Some(e0), true),
        observation(&[], None, false),
    ] {
        assert_eq!(
            conformance(&bootstrap, Some(&published)),
            NodePolicyConformance::Conforms
        );
    }

    // The cluster can't grant the legacy permission: a node that keeps
    // refusing it is stricter, not divergent.
    let granted = policy(None, &[], Some(true));
    assert_eq!(
        conformance(&granted, Some(&observation(&[e0], Some(e0), false))),
        NodePolicyConformance::Conforms
    );
    let revoked = policy(None, &[], Some(false));
    assert_eq!(
        conformance(&revoked, Some(&observation(&[e0], Some(e0), false))),
        NodePolicyConformance::Conforms
    );
    assert_eq!(
        conformance(&revoked, Some(&observation(&[e0], Some(e0), true))),
        NodePolicyConformance::Diverges {
            default_differs: false,
            executes_outside_policy: vec![],
            allows_revoked_legacy_unbound: true,
        }
    );
}

#[test]
fn an_applied_policy_without_the_cluster_default_diverges() {
    use RuntimeEnvironmentState::Allowed;
    let (e0, e1) = (environment(0), environment(1));
    let cluster = policy(Some(e1), &[(e0, Allowed), (e1, Allowed)], None);
    // Schema 2 allows no default: the node refused the cluster's.
    assert_eq!(
        conformance(&cluster, Some(&applied_observation(&[e0], None, 7))),
        NodePolicyConformance::Diverges {
            default_differs: true,
            executes_outside_policy: vec![],
            allows_revoked_legacy_unbound: false,
        }
    );
    // A node that can't read the policy it persisted refuses everything, which
    // says nothing of what it would enforce: unknown, with or without a default.
    let unreadable = ExecutionPolicyObservation::new(2, vec![], None, false, None, true)
        .expect("valid unreadable observation");
    assert_eq!(
        conformance(&cluster, Some(&unreadable)),
        NodePolicyConformance::Unknown
    );
    assert_eq!(
        conformance(&policy(None, &[], None), Some(&unreadable)),
        NodePolicyConformance::Unknown
    );
    assert_eq!(
        conformance(&cluster, Some(&applied_observation(&[e0, e1], Some(e1), 7))),
        NodePolicyConformance::Conforms
    );
}

#[test]
fn conformance_serializes_as_a_tagged_flat_object() {
    let value = serde_json::to_value(NodePolicyConformance::Diverges {
        default_differs: true,
        executes_outside_policy: vec![environment(2)],
        allows_revoked_legacy_unbound: false,
    })
    .unwrap();
    assert_eq!(
        value,
        json!({
            "conformance": "diverges",
            "default_differs": true,
            "executes_outside_policy": [environment(2).to_string()],
            "allows_revoked_legacy_unbound": false,
        })
    );
    assert_eq!(
        serde_json::to_value(NodePolicyConformance::Unknown).unwrap(),
        json!({"conformance": "unknown"})
    );
}

#[test]
fn a_binding_record_classifies_its_rows() {
    let e1 = environment(1);
    assert_eq!(
        binding(Some(&e1.to_string()), true, false),
        Binding::Bound(e1)
    );
    assert_eq!(binding(None, true, false), Binding::Unbound);
    assert_eq!(binding(None, false, false), Binding::Unrecorded);
    assert_eq!(binding(Some("sha256:nope"), true, false), Binding::Invalid);
    assert_eq!(binding(Some(""), true, false), Binding::Invalid);
    // Metadata the query found malformed is invalid, not legacy.
    assert_eq!(binding(None, true, true), Binding::Invalid);
}

fn release_row(
    environment_ref: Option<String>,
    in_use: bool,
    template_releases: i64,
) -> store::TemplateReleasePopulationRow {
    store::TemplateReleasePopulationRow {
        environment_ref,
        in_use,
        malformed: false,
        template_releases,
    }
}

fn sandbox_row(
    environment_ref: Option<String>,
    recorded: bool,
    lifecycle: &str,
    sandboxes: i64,
) -> store::SandboxPopulationRow {
    store::SandboxPopulationRow {
        environment_ref,
        recorded,
        lifecycle: lifecycle.to_owned(),
        // A recorded failed row is always runnable (live observation or
        // repair snapshot). An unrecorded one is runnable only with a live
        // execution, which the route tests cover; here it's the dead case.
        runnable: match lifecycle {
            "cleanup_required" | "deleting" => false,
            "failed" => recorded,
            _ => true,
        },
        malformed: false,
        sandboxes,
    }
}

#[test]
fn population_adds_up_per_binding_and_lists_environments_without_work() {
    let (e0, e1, e2) = (environment(0), environment(1), environment(2));
    let population = Population::from_rows(
        vec![
            sandbox_row(Some(e1.to_string()), true, "started", 3),
            sandbox_row(Some(e1.to_string()), true, "suspended", 2),
            sandbox_row(Some(e0.to_string()), true, "stopped", 1),
            sandbox_row(None, true, "started", 4),
            sandbox_row(None, false, "creating", 5),
            sandbox_row(Some("garbage".into()), true, "paused", 6),
            // Never negative from `COUNT(*)`, and clamped if it somehow is.
            sandbox_row(None, true, "stopped", -1),
        ],
        vec![
            release_row(Some(e1.to_string()), true, 8),
            release_row(None, false, 9),
        ],
    );
    assert_eq!(population.environments[&e1].sandboxes, 5);
    assert_eq!(
        population.environments[&e1].sandboxes_by_lifecycle,
        BTreeMap::from([("started".into(), 3), ("suspended".into(), 2)])
    );
    assert_eq!(population.environments[&e1].template_releases, 8);
    assert_eq!(population.unbound.sandboxes, 4);
    assert_eq!(population.unbound.template_releases, 9);
    assert_eq!(population.unrecorded.sandboxes, 5);
    assert_eq!(population.invalid.sandboxes, 6);

    let cluster = policy(
        Some(e1),
        &[
            (e1, RuntimeEnvironmentState::Allowed),
            (e2, RuntimeEnvironmentState::Blocked),
        ],
        None,
    );
    let response = serde_json::to_value(population_response(&cluster, population)).unwrap();
    let environments = response["environments"].as_array().unwrap();
    let refs: Vec<_> = environments
        .iter()
        .map(|entry| entry["environment_ref"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(refs, [e0, e1, e2].map(|e| e.to_string()));
    // Work bound to an environment with no state row: listed, state null.
    assert_eq!(environments[0]["state"], Value::Null);
    assert_eq!(environments[0]["sandboxes"], 1);
    assert_eq!(environments[1]["state"], "allowed");
    assert_eq!(environments[1]["is_default"], true);
    // A state row with no work is still listed, at zero.
    assert_eq!(environments[2]["state"], "blocked");
    assert_eq!(environments[2]["sandboxes"], 0);
    assert_eq!(environments[2]["template_releases"], 0);
    assert_eq!(response["generation"], 7);
}

#[test]
fn a_block_counts_work_bound_to_it_and_sandboxes_whose_environment_is_unknown() {
    let (e0, e1) = (environment(0), environment(1));
    // Unused template releases with an unknown binding are reported, not
    // counted.
    let only_other = Population::from_rows(
        vec![sandbox_row(Some(e0.to_string()), true, "started", 2)],
        vec![
            release_row(None, false, 4),
            release_row(Some("x".into()), false, 1),
        ],
    );
    assert!(AtRisk::of_block(&only_other, e1).is_empty());
    assert!(!AtRisk::of_block(&only_other, e0).is_empty());

    for at_risk in [
        Population::from_rows(vec![sandbox_row(None, true, "stopped", 1)], vec![]),
        Population::from_rows(vec![sandbox_row(None, false, "started", 1)], vec![]),
        Population::from_rows(
            vec![sandbox_row(Some("x".into()), true, "stopped", 1)],
            vec![],
        ),
        Population::from_rows(vec![], vec![release_row(Some(e1.to_string()), true, 1)]),
    ] {
        assert!(!AtRisk::of_block(&at_risk, e1).is_empty(), "{at_risk:?}");
    }

    // An in-use release whose environment can't be told counts: new creates
    // from its tag may run on the environment that stops.
    for tagged in [
        Population::from_rows(vec![], vec![release_row(None, true, 1)]),
        Population::from_rows(vec![], vec![release_row(Some("x".into()), true, 1)]),
    ] {
        assert!(!AtRisk::of_block(&tagged, e1).is_empty(), "{tagged:?}");
    }
    assert!(
        AtRisk::of_block(
            &Population::from_rows(vec![], vec![release_row(None, false, 4)]),
            e1
        )
        .is_empty()
    );

    // A superseded release, untagged and unpooled, is reported, not counted.
    let superseded =
        Population::from_rows(vec![], vec![release_row(Some(e1.to_string()), false, 3)]);
    let at_risk = AtRisk::of_block(&superseded, e1);
    assert!(at_risk.is_empty(), "{at_risk:?}");
    assert_eq!(at_risk.bound.template_releases, 3);
    assert_eq!(superseded.environments[&e1].template_releases, 3);
}

#[test]
fn the_first_default_stops_every_environment_with_no_state() {
    let (e0, e1, e2, e3) = (
        environment(0),
        environment(1),
        environment(2),
        environment(3),
    );
    let population = Population::from_rows(
        vec![
            sandbox_row(Some(e0.to_string()), true, "started", 2),
            sandbox_row(Some(e1.to_string()), true, "started", 3),
            sandbox_row(Some(e2.to_string()), true, "stopped", 4),
            sandbox_row(Some(e3.to_string()), true, "paused", 1),
        ],
        vec![release_row(Some(e3.to_string()), true, 5)],
    );
    // E1 becomes the default and E2 is already allowed: E0 and E3 stop.
    let cluster = policy(None, &[(e2, RuntimeEnvironmentState::Allowed)], None);
    let at_risk = AtRisk::of_first_default(&population, &cluster, e1);
    assert_eq!(at_risk.environments, [e0, e3]);
    // A block left by direct SQL takes effect with the first default too.
    let stray_block = policy(
        None,
        &[
            (e0, RuntimeEnvironmentState::Allowed),
            (e2, RuntimeEnvironmentState::Blocked),
            (e3, RuntimeEnvironmentState::Allowed),
        ],
        None,
    );
    assert_eq!(
        AtRisk::of_first_default(&population, &stray_block, e1).environments,
        [e2]
    );
    assert_eq!(at_risk.bound.sandboxes, 3);
    assert_eq!(
        at_risk.bound.sandboxes_by_lifecycle,
        BTreeMap::from([("paused".into(), 1), ("started".into(), 2)])
    );
    assert_eq!(at_risk.bound.template_releases, 5);
    assert!(!at_risk.is_empty());

    // With every environment that has work given a state, nothing stops.
    let stated = policy(
        None,
        &[
            (e0, RuntimeEnvironmentState::Allowed),
            (e2, RuntimeEnvironmentState::Allowed),
            (e3, RuntimeEnvironmentState::Deprecated),
        ],
        None,
    );
    let at_risk = AtRisk::of_first_default(&population, &stated, e1);
    assert!(at_risk.environments.is_empty());
    assert!(at_risk.is_empty());
}

#[test]
fn sandboxes_that_cant_run_are_reported_but_never_gate() {
    let (e0, e1) = (environment(0), environment(1));
    let dead = Population::from_rows(
        vec![
            sandbox_row(Some(e0.to_string()), true, "cleanup_required", 2),
            sandbox_row(None, false, "cleanup_required", 1),
            sandbox_row(Some("x".into()), true, "cleanup_required", 1),
        ],
        vec![],
    );
    let at_risk = AtRisk::of_block(&dead, e0);
    assert!(at_risk.is_empty(), "{at_risk:?}");
    // Reported all the same, for an operator deciding on `strand`.
    assert_eq!(at_risk.environments, [e0]);
    assert_eq!(at_risk.bound.sandboxes, 2);
    assert_eq!(at_risk.unrecorded.sandboxes, 1, "still reported");
    assert_eq!(at_risk.unrecorded.sandboxes_not_runnable, 1);

    // A runnable one next to dead ones still gates. (Which lifecycles can
    // run is the query's rule; the population route test covers it.)
    let mixed = Population::from_rows(
        vec![
            sandbox_row(Some(e0.to_string()), true, "cleanup_required", 2),
            sandbox_row(Some(e0.to_string()), true, "stopped", 1),
        ],
        vec![],
    );
    assert_eq!(AtRisk::of_block(&mixed, e0).environments, [e0]);
    assert!(!AtRisk::of_block(&mixed, e0).is_empty());
    assert!(AtRisk::of_block(&mixed, e1).is_empty());
}

#[test]
fn an_unknown_state_names_every_accepted_one() {
    for state in RuntimeEnvironmentState::ALL {
        assert_eq!(parse_state(state.as_str()).ok(), Some(state));
    }
    let (status, _) = parse_state("Blocked").unwrap_err();
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[test]
fn a_snapshot_is_stale_from_its_stale_after() {
    let now = OffsetDateTime::now_utc();
    assert!(status_fresh(now + time::Duration::SECOND, now));
    assert!(!status_fresh(now, now));
    assert!(!status_fresh(now - time::Duration::SECOND, now));
}

#[tokio::test(flavor = "current_thread")]
async fn a_failed_read_is_retryable_only_when_it_lost_a_race() -> TestResult {
    use pplx_space_db_test_fixtures::raise_sqlstate;
    let Some(db) = postgres_fixture().await? else {
        return Ok(());
    };
    for (sqlstate, expected) in [
        ("55P03", StatusCode::SERVICE_UNAVAILABLE),
        // A read wrote nothing, so it never gets the write-race 409.
        ("40P01", StatusCode::SERVICE_UNAVAILABLE),
        ("23505", StatusCode::INTERNAL_SERVER_ERROR),
    ] {
        let error = raise_sqlstate(&db.pool, sqlstate).await;
        assert_eq!(read_db_error(error).status(), expected, "{sqlstate}");
    }
    // Each `raise_sqlstate` fails a statement on the connection it borrows;
    // close the pool so nextest doesn't report them as leaked.
    db.pool.close().await;
    Ok(())
}

// ---------------------------------------------------------------------------
// Route tests: the real admin router, session auth and audit middleware
// included, against a migrated database.

struct Harness {
    db: Fixture,
    app: Router,
    token: String,
    user_id: UserId,
}

async fn insert_session_user(pool: &PgPool, role: &str) -> TestResult<(UserId, String)> {
    let user_id = UserId::new();
    sqlx::query(sql!(
        "INSERT INTO users (id, email, password_hash, display_name, org_id, role)
         VALUES ($1, $2, 'synthetic-password-hash', 'runtime env tests', $3, $4)"
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
    Ok((user_id, token))
}

async fn harness() -> TestResult<Option<Harness>> {
    let Some(db) = postgres_fixture().await? else {
        return Ok(None);
    };
    harness_on(db).await
}

/// A harness whose pool fits a test holding a transaction open while a
/// request runs: the default pool has two connections.
async fn harness_with_connections(max_connections: u32) -> TestResult<Option<Harness>> {
    let Some(db) =
        pplx_space_db_test_fixtures::try_pg_test_db_with_max_connections(max_connections).await?
    else {
        return Ok(None);
    };
    harness_on(db).await
}

async fn harness_on(db: Fixture) -> TestResult<Option<Harness>> {
    let (user_id, token) = insert_session_user(&db.pool, "organization_admin").await?;
    let state = AppState::from_pool(db.pool.clone()).await?;
    let app = crate::routes::admin::shared_host_router(state.clone()).with_state(state);
    Ok(Some(Harness {
        db,
        app,
        token,
        user_id,
    }))
}

impl Harness {
    async fn call(&self, method: Method, path: &str, body: Option<Value>) -> (StatusCode, Value) {
        call(&self.app, &self.token, method, path, body).await
    }

    async fn get(&self, path: &str) -> (StatusCode, Value) {
        self.call(Method::GET, path, None).await
    }

    async fn put(&self, path: &str, body: Value) -> (StatusCode, Value) {
        self.call(Method::PUT, path, Some(body)).await
    }

    async fn set_default(&self, environment: EnvironmentReference) -> (StatusCode, Value) {
        self.put(
            "/admin/api/v1/runtime-environments/default",
            json!({"environment_ref": environment.to_string(), "reason": "roll forward"}),
        )
        .await
    }

    async fn set_state(
        &self,
        environment: EnvironmentReference,
        body: Value,
    ) -> (StatusCode, Value) {
        self.put(&state_path(environment), body).await
    }

    async fn put_if_match(&self, path: &str, body: Value, if_match: &str) -> (StatusCode, Value) {
        call_with_if_match(
            &self.app,
            &self.token,
            Method::PUT,
            path,
            Some(body),
            Some(if_match),
        )
        .await
    }

    async fn generation(&self) -> TestResult<u64> {
        Ok(policy_store::get(&self.db.pool).await?.generation)
    }
}

fn state_path(environment: EnvironmentReference) -> String {
    format!("/admin/api/v1{}", state_path_suffix(environment))
}

async fn call(
    app: &Router,
    token: &str,
    method: Method,
    path: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    call_with_if_match(app, token, method, path, body, None).await
}

async fn call_with_if_match(
    app: &Router,
    token: &str,
    method: Method,
    path: &str,
    body: Option<Value>,
    if_match: Option<&str>,
) -> (StatusCode, Value) {
    let mut request = Request::builder()
        .method(method)
        .uri(path)
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/json");
    if let Some(if_match) = if_match {
        request = request.header("if-match", if_match);
    }
    let body = body.map_or_else(Body::empty, |body| Body::from(body.to_string()));
    let response = app
        .clone()
        .oneshot(request.body(body).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned()))
    };
    (status, value)
}

#[track_caller]
fn assert_error_contains(body: &Value, needle: &str) {
    let error = body["error"].as_str().unwrap_or_default();
    assert!(error.contains(needle), "expected {needle:?} in {body}");
}

fn state_path_suffix(environment: EnvironmentReference) -> String {
    format!("/runtime-environments/{environment}/state")
}

/// Realistic execution metadata, bound to `environment` when given.
fn metadata(environment: Option<EnvironmentReference>) -> Value {
    let mut value = json!({
        "runtime": {"kind": "firecracker", "version": "1.15.1"},
        "guest": {"os": "linux", "architecture": "x86_64"},
        "state_format": {"format": "firecracker-snapshot", "version": "9.0.0"},
    });
    if let Some(environment) = environment {
        value["environment"] = json!(environment.to_string());
    }
    value
}

/// A Kubernetes-managed sandbox in `lifecycle`.
async fn insert_sandbox(pool: &PgPool, lifecycle: &str) -> TestResult<SandboxId> {
    insert_sandbox_on(pool, SubstrateKind::Kubernetes, lifecycle).await
}

async fn insert_sandbox_on(
    pool: &PgPool,
    substrate: SubstrateKind,
    lifecycle: &str,
) -> TestResult<SandboxId> {
    let sandbox_id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (id, user_id, org_id, managed_by, lifecycle, placement,
                                network_policy_generation)
         VALUES ($1, '00000000-0000-0000-0000-000000000010', $2, $3, $4, 'unplaced', 1)"
    ))
    .bind(sandbox_id)
    .bind(OrgId::from_uuid(SEED_ORG))
    .bind(substrate)
    .bind(lifecycle)
    .execute(pool)
    .await?;
    Ok(sandbox_id)
}

/// An observation of `sandbox_id` as failed: its node no longer reports a
/// live VM.
async fn observe_failed(
    pool: &PgPool,
    sandbox_id: SandboxId,
    node_id: NodeId,
    execution_metadata: Option<Value>,
) -> TestResult {
    observe_sandbox(pool, sandbox_id, node_id, execution_metadata).await?;
    sqlx::query(sql!(
        "UPDATE sandbox_observed_states SET status = 'failed' WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .execute(pool)
    .await?;
    Ok(())
}

async fn observe_sandbox(
    pool: &PgPool,
    sandbox_id: SandboxId,
    node_id: NodeId,
    execution_metadata: Option<Value>,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO sandbox_observed_states (sandbox_id, node_id, status, execution_metadata)
         VALUES ($1, $2, 'running', $3)"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .bind(execution_metadata.map(sqlx::types::Json))
    .execute(pool)
    .await?;
    Ok(())
}

/// A `full` snapshot of `sandbox_id`, `age_secs` old. `located` gives it the
/// storage location every uploaded snapshot has.
async fn insert_snapshot(
    pool: &PgPool,
    sandbox_id: SandboxId,
    (state, origin, age_secs, located): (&str, Option<&str>, i64, bool),
    execution_metadata: Value,
) -> TestResult<Option<Uuid>> {
    // An origin needs an artifact kind, and every origin but `fork` a local
    // artifact identity (`snapshots_artifact_identity_check`).
    let local_artifact_id = origin
        .filter(|origin| *origin != "fork")
        .map(|_| Uuid::now_v7());
    let snapshot_id: Uuid = sqlx::query_scalar(sql!(
        "INSERT INTO snapshots (sandbox_id, type, state, artifact_origin, artifact_kind,
                                local_artifact_id, timestamp, execution_metadata)
         VALUES ($1, 'full', $2, $3, CASE WHEN $3::TEXT IS NULL THEN NULL ELSE 'full' END,
                 $4, NOW() - make_interval(secs => $5), $6)
         RETURNING id"
    ))
    .bind(sandbox_id)
    .bind(state)
    .bind(origin)
    .bind(local_artifact_id)
    .bind(age_secs as f64)
    .bind(sqlx::types::Json(execution_metadata))
    .fetch_one(pool)
    .await?;
    if located {
        sqlx::query(sql!(
            "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string)
             VALUES ($1, 's3://synthetic-bucket/snapshot')"
        ))
        .bind(snapshot_id)
        .execute(pool)
        .await?;
    }
    Ok(local_artifact_id)
}

/// Point `sandbox_id`'s current local artifact marker at a suspend artifact.
async fn mark_current_artifact(
    pool: &PgPool,
    sandbox_id: SandboxId,
    local_artifact_id: Option<Uuid>,
) -> TestResult {
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET current_local_artifact_id = $2,
                current_local_artifact_kind = 'full',
                current_local_artifact_origin = 'suspend'
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(local_artifact_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// A release of template `name`, created if it's new. Returns the release id.
async fn insert_template_release(
    pool: &PgPool,
    name: &str,
    execution_metadata: Option<Value>,
) -> TestResult<Uuid> {
    sqlx::query(sql!(
        "INSERT INTO templates (name, id) VALUES ($1, $2) ON CONFLICT DO NOTHING"
    ))
    .bind(name)
    .bind(Uuid::now_v7())
    .execute(pool)
    .await?;
    Ok(sqlx::query_scalar(sql!(
        "INSERT INTO template_releases (template_name, type, hash, execution_metadata)
         VALUES ($1, 'fc', $2, $3)
         RETURNING id"
    ))
    .bind(name)
    .bind(format!("sha256:{}", Uuid::now_v7().simple()))
    .bind(execution_metadata.map(sqlx::types::Json))
    .fetch_one(pool)
    .await?)
}

/// Point `tag` of template `name` at `release`.
async fn tag_release(pool: &PgPool, release: Uuid, name: &str, tag: &str) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO template_release_tags (template_release_id, template_name, tag)
         VALUES ($1, $2, $3)"
    ))
    .bind(release)
    .bind(name)
    .bind(tag)
    .execute(pool)
    .await?;
    Ok(())
}

/// A warm-pool config pinning `release`. Returns the config id.
async fn pool_release(pool: &PgPool, release: Uuid, enabled: bool) -> TestResult<Uuid> {
    Ok(sqlx::query_scalar(sql!(
        "INSERT INTO kubernetes_pool_configs
             (template_release_id, vcpu, memory_mib, target_ready_slots, refill_per_tick,
              max_warming_slots, namespace, enabled)
         VALUES ($1, 1, 512, 1, 1, 1, 'synthetic', $2)
         RETURNING id"
    ))
    .bind(release)
    .bind(enabled)
    .fetch_one(pool)
    .await?)
}

async fn publish_policy(
    pool: &PgPool,
    node_id: NodeId,
    stale: bool,
    execution_policy: Option<Value>,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO node_status_snapshots (
             node_id, observed_at, stale_after, reachable, services, updated_at,
             execution_policy, execution_policy_received_at
         )
         VALUES (
             $1, NOW(),
             CASE WHEN $2 THEN NOW() - INTERVAL '1 minute' ELSE NOW() + INTERVAL '1 hour' END,
             TRUE, '[]'::jsonb, NOW(),
             $3, CASE WHEN $3::JSONB IS NULL THEN NULL ELSE NOW() END
         )"
    ))
    .bind(node_id)
    .bind(stale)
    .bind(execution_policy.map(sqlx::types::Json))
    .execute(pool)
    .await?;
    Ok(())
}

fn published(observation: &ExecutionPolicyObservation) -> Value {
    serde_json::to_value(observation).unwrap()
}

mod nodes;
mod population;
mod writes;
