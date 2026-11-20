//! Postgres-backed tests that drive `apply_drain_action` end to end.
//!
//! Every other test on this handler calls a pure helper. That is deliberate —
//! eight values were found mutation-silent inline and extracted into named,
//! tested functions — but it closed each instance and none of the class. What
//! it left uncovered, until this module, was the *wiring*: which helper feeds
//! which argument, what the call site does with the result, and in what order
//! the guards run. Those mutations survive the entire pure-function suite:
//!
//! - inverting the inversion **at the composition** rather than inside
//!   `persisted_decrement_capacity`,
//! - widening the `transition_starts_a_drain` gate at the call site,
//! - dropping the action gate on the written mode,
//! - passing `None` as `phase_after`'s current phase.
//!
//! Each is a silent data defect: a reverted blue-green shrink, a forced drain
//! presenting as gentle one step from ASG termination, a 502 promising a repair
//! nothing will perform. This module asserts them against a real row.
//!
//! Every test runs against a Kubernetes substrate whose API client refuses
//! every request ([`kubernetes_state`]). A Direct substrate would answer 501
//! before the row is read, so the handler's transition logic would never run.
//! The 409 paths return before the substrate call. The paths that reach
//! `converge_substrate_schedulability` fail there deterministically *after*
//! committing — a `managed_by = 'space'` row for want of a `node_k8s` join, a
//! Kubernetes row because the client refuses the patch — which is what lets the
//! committed columns be asserted without a working API server.

use axum::Extension;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use pplx_space_accounts::{NodeId, OrgId, SandboxOwnerSource, UserId};
use pplx_space_admin_protocol::DrainRequest;
use pplx_space_db_test_fixtures::try_pg_test_db;
use pplx_space_node_model::NodeDrainMode;
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use time::OffsetDateTime;

use super::tests::enter_direct_test_process;
use super::{DrainAction, DrainPreviewError, V2DrainPreview, V2DrainProbe, apply_drain_action};
use crate::controllers::sandbox::SandboxController;
use crate::controllers::substrate::Substrate;
use crate::controllers::substrate::kubernetes::tests::substrate_with_store_and_client;
use crate::middleware::auth::AuthUser;
use crate::models::User;
use crate::routes::AppState;
use crate::user_role::UserRole;

/// `AppState` on a Kubernetes substrate whose API client fails every request.
///
/// `AppState::from_pool` picks its substrate from the process environment, so
/// each test runs in a child process pinned to Direct (see
/// [`super::tests::enter_direct_test_process`]) and swaps this substrate in.
async fn kubernetes_state(pool: &PgPool) -> TestResult<AppState> {
    use axum::http::{Request, Response};
    use kube::client::{Body, ClientBuilder};

    let client = ClientBuilder::new(
        tower::service_fn(|_req: Request<Body>| async {
            Err::<Response<Body>, Box<dyn std::error::Error + Send + Sync>>(
                "drain_handler_tests: no Kubernetes API server is reachable".into(),
            )
        }),
        "space",
    )
    .build();
    let mut state = AppState::from_pool(pool.clone()).await?;
    state.sandbox_controller = SandboxController::new(Substrate::from_kubernetes(
        substrate_with_store_and_client(pool.clone(), client),
    ));
    state.v2_drain_probe = std::sync::Arc::new(NoPreview);
    Ok(state)
}

/// A [`V2DrainProbe`] that answers every preview unreachable: no admin
/// action asks the node anything.
struct NoPreview;

impl V2DrainProbe for NoPreview {
    fn preview<'a>(
        &'a self,
        _state: &'a AppState,
        _target: &'a crate::controllers::substrate::OwnedNodeApiTarget,
    ) -> V2DrainPreview<'a> {
        Box::pin(std::future::ready(Err(DrainPreviewError::Unreachable(
            "no preview is scripted".into(),
        ))))
    }
}

/// An actor the admin authz accepts.
///
/// `require_managing_org_admin` reads only `role` and `org_id` off the struct
/// and compares the latter to `clusters.managing_org_id`, so this needs no
/// `users` row — just the cluster's own org id.
async fn managing_org_admin(pool: &PgPool) -> TestResult<User> {
    let org_id: OrgId =
        sqlx::query_scalar(sql!("SELECT managing_org_id FROM clusters WHERE singleton"))
            .fetch_one(pool)
            .await?;
    Ok(User {
        id: UserId::new(),
        email: "drain-handler-tests@example.invalid".to_string(),
        password_hash: String::new(),
        display_name: "drain handler tests".to_string(),
        org_id,
        role: UserRole::OrganizationAdmin,
        owner_source: SandboxOwnerSource::Local,
        created_at: OffsetDateTime::UNIX_EPOCH,
        updated_at: OffsetDateTime::UNIX_EPOCH,
    })
}

/// A node with the drain columns set as a prior `:drain` would have left them.
///
/// `managed_by = 'space'` so the substrate patch fails deterministically after
/// the commit — see the module docs. `drain_started_at` is backdated so a
/// re-stamp is detectable.
async fn insert_node(
    pool: &PgPool,
    drain_phase: Option<&str>,
    drain_mode: Option<&str>,
    replace_after_terminate: bool,
) -> TestResult<NodeId> {
    let node_id = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (
             id, address, role, managed_by, state,
             drain_phase, drain_mode, drain_started_at, terminate_decrement_capacity
         )
         VALUES (
             $1, '10.0.0.44'::INET, 'worker', 'space', 'registered',
             $2, $3,
             CASE WHEN $2::TEXT IS NULL THEN NULL ELSE NOW() - INTERVAL '4 hours' END,
             $4
         )"
    ))
    .bind(node_id)
    .bind(drain_phase)
    .bind(drain_mode)
    .bind(!replace_after_terminate)
    .execute(pool)
    .await?;
    Ok(node_id)
}

/// A `managed_by = 'k8s'` node with a `node_k8s` row, so `substrate_node_id`
/// resolves and `converge_substrate_schedulability` gets as far as the substrate
/// call (which then fails, with no kube apiserver in a unit test).
///
/// Needed for the retry hint: on a `managed_by = 'space'` row
/// `reconciler_reasserts_cordon` is `false` whatever the phase, so such a row
/// cannot distinguish a correct hint from one computed off the wrong phase.
async fn insert_k8s_draining_node(pool: &PgPool) -> TestResult<NodeId> {
    let node_id = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (
             id, address, role, managed_by, state,
             drain_phase, drain_mode, drain_started_at, terminate_decrement_capacity
         )
         VALUES (
             $1, '10.0.0.45'::INET, 'worker', 'k8s', 'registered',
             'draining', 'gentle', NOW() - INTERVAL '4 hours', false
         )"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name) VALUES ($1, $2)"
    ))
    .bind(node_id)
    .bind(format!("ip-10-0-0-45-{node_id}"))
    .execute(pool)
    .await?;
    Ok(node_id)
}

#[derive(Debug, PartialEq, Eq)]
struct DrainColumns {
    phase: Option<String>,
    mode: Option<String>,
    started_at: Option<OffsetDateTime>,
    decrement_capacity: bool,
}

async fn drain_columns(pool: &PgPool, node_id: NodeId) -> TestResult<DrainColumns> {
    let row: (Option<String>, Option<String>, Option<OffsetDateTime>, bool) = sqlx::query_as(sql!(
        "SELECT drain_phase, drain_mode, drain_started_at, terminate_decrement_capacity
               FROM nodes WHERE id = $1"
    ))
    .bind(node_id)
    .fetch_one(pool)
    .await?;
    Ok(DrainColumns {
        phase: row.0,
        mode: row.1,
        started_at: row.2,
        decrement_capacity: row.3,
    })
}

async fn post_drain_response(
    state: &AppState,
    actor: &User,
    node_id: NodeId,
    action: DrainAction,
    request: DrainRequest,
) -> TestResult<(StatusCode, serde_json::Value)> {
    let response = apply_drain_action(
        State(state.clone()),
        Extension(AuthUser(actor.clone())),
        node_id,
        action,
        request,
    )
    .await
    .into_response();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024).await?;
    Ok((status, serde_json::from_slice(&bytes)?))
}

async fn post_drain(
    state: &AppState,
    actor: &User,
    node_id: NodeId,
    action: DrainAction,
    request: DrainRequest,
) -> StatusCode {
    apply_drain_action(
        State(state.clone()),
        Extension(AuthUser(actor.clone())),
        node_id,
        action,
        request,
    )
    .await
    .into_response()
    .status()
}

/// A same-mode repeat must forward the transition's `None`, not the request's
/// mode, so an unparseable `drain_mode` survives untouched.
///
/// The composition under test is one argument: the handler's in-flight arm
/// passes `DrainTransition::UpdateInFlightDrain(mode)` straight through, and
/// `mode` is `None` on a re-assert. Passing `requested_mode` there instead
/// type-checks, and every existing test stays green — the validator test asserts
/// the transition is `UpdateInFlightDrain(None)`, the store test asserts the
/// store honors `None`, and nothing joined the two.
///
/// The live effect of that mutation is a silent downgrade of the most dangerous
/// column on the row. A node whose `drain_mode` holds a token this build cannot
/// parse — a newer pod's during a rollout, the column has no `CHECK` — meets a
/// bodiless repeat. `from_column_str` resolves the stored token to `Gentle`, the
/// request is also gentle, so this is the same-mode arm; writing the request's
/// `Gentle` would normalize the column to `'gentle'` and disarm a drain the
/// writing pod treats as destructive. Answering 200 while doing it.
///
/// `'forced_fast'` stands in for that token: reachable, not hypothetical, since
/// nothing constrains the column's domain in SQL.
#[tokio::test(flavor = "current_thread")]
async fn a_same_mode_repeat_leaves_an_unparseable_mode_alone() -> TestResult {
    if !enter_direct_test_process() {
        return Ok(());
    }
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let state = kubernetes_state(&db.pool).await?;
    let actor = managing_org_admin(&db.pool).await?;
    let node_id = insert_node(&db.pool, Some("draining"), Some("forced_fast"), false).await?;

    let before = drain_columns(&db.pool, node_id).await?;
    assert_eq!(
        before.mode.as_deref(),
        Some("forced_fast"),
        "the fixture must actually hold an unparseable token, or the assertion \
         below passes on a column that was never at risk"
    );

    // Bodiless: `mode` defaults to gentle, which is what the unparseable column
    // also resolves to — so this reaches the same-mode arm rather than the
    // escalation arm. (An explicit `--forced` here would be a *legitimate*
    // escalation and would normalize the column on purpose.)
    let status = post_drain(
        &state,
        &actor,
        node_id,
        DrainAction::Drain,
        DrainRequest::default(),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_GATEWAY,
        "the row is `managed_by = 'space'`, so the substrate patch fails after \
         the commit — which is what lets the committed columns be read below"
    );

    let after = drain_columns(&db.pool, node_id).await?;
    assert_eq!(
        after.mode.as_deref(),
        Some("forced_fast"),
        "a same-mode repeat must not rewrite the mode column; normalizing it to \
         'gentle' would silently disarm a drain another build considers forced"
    );
    assert_eq!(
        after.phase.as_deref(),
        Some("draining"),
        "and the drain itself is untouched"
    );
    assert_eq!(after.started_at, before.started_at, "including its age");
    Ok(())
}

/// A fresh `:drain --no-replace-after-terminate` must persist
/// `terminate_decrement_capacity = true`.
///
/// This is the composition, not the helper: `persisted_decrement_capacity` has
/// its own test, and inverting the `!` *at the call site* survived it. The
/// column is what the drained-node terminator passes to ASG
/// `TerminateInstanceInAutoScalingGroup` as `ShouldDecrementDesiredCapacity`, so
/// getting it backwards means every instance a monotonic shrink terminates is
/// auto-replaced and the source MNG never shrinks — first visible hours later as
/// `source_desired` rising.
///
/// Both request shapes are driven, so the assertion cannot be satisfied by a
/// constant.
#[tokio::test(flavor = "current_thread")]
async fn a_fresh_drain_persists_the_requested_replacement_intent() -> TestResult {
    if !enter_direct_test_process() {
        return Ok(());
    }
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let state = kubernetes_state(&db.pool).await?;
    let actor = managing_org_admin(&db.pool).await?;

    for replace_after_terminate in [true, false] {
        // Seed the *opposite* stored value, so a write that never happened is
        // distinguishable from one that wrote the right thing.
        let node_id = insert_node(&db.pool, None, None, !replace_after_terminate).await?;
        let status = post_drain(
            &state,
            &actor,
            node_id,
            DrainAction::Drain,
            DrainRequest {
                replace_after_terminate: Some(replace_after_terminate),
                mode: NodeDrainMode::Forced,
                ..DrainRequest::default()
            },
        )
        .await;
        // 502: the DB transaction committed, then the substrate patch failed for
        // want of a `node_k8s` row. The commit is what this test is about.
        assert_eq!(
            status,
            StatusCode::BAD_GATEWAY,
            "expected a post-commit 502"
        );

        let after = drain_columns(&db.pool, node_id).await?;
        assert_eq!(
            after.decrement_capacity, !replace_after_terminate,
            "replace_after_terminate={replace_after_terminate}: the column is the \
             inverse of the request field"
        );
        assert_eq!(after.phase.as_deref(), Some("draining"));
        assert_eq!(after.mode.as_deref(), Some("forced"));
        assert!(after.started_at.is_some(), "a fresh drain stamps its start");
    }
    Ok(())
}

/// `:terminate` must not write the drain columns.
///
/// Two mutations that survive the pure-function suite land here: widening
/// `transition_starts_a_drain`'s gate at the call site makes `:terminate` write
/// `terminate_decrement_capacity`, and dropping the action gate on
/// `requested_mode` makes it write `drain_mode = 'gentle'` through
/// `COALESCE($6, drain_mode)`.
///
/// The second is the worse one: the user-API drain gates and the reconciler both
/// re-read `drain_mode`, so a node one step from ASG termination would present
/// as gentle and reopen user calls against whatever is still attached to it.
#[tokio::test(flavor = "current_thread")]
async fn terminate_does_not_rewrite_the_drains_own_columns() -> TestResult {
    if !enter_direct_test_process() {
        return Ok(());
    }
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let state = kubernetes_state(&db.pool).await?;
    let actor = managing_org_admin(&db.pool).await?;

    // A forced drain that reached `drained`, started with
    // `--no-replace-after-terminate` — the blue-green shrink shape.
    let node_id = insert_node(&db.pool, Some("drained"), Some("forced"), false).await?;
    let before = drain_columns(&db.pool, node_id).await?;
    assert!(before.decrement_capacity, "fixture is a monotonic shrink");

    let status = post_drain(
        &state,
        &actor,
        node_id,
        DrainAction::Terminate,
        // The defaulted body every non-`:drain` action arrives with. Its `mode`
        // is `gentle` and its `replace_after_terminate` is `None` — neither
        // chosen by anyone, and neither may reach the row.
        DrainRequest::default(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "`:terminate` patches no substrate");

    let after = drain_columns(&db.pool, node_id).await?;
    assert_eq!(
        after.mode.as_deref(),
        Some("forced"),
        "the mode must survive: a `gentle` here reopens the user-API gates on a \
         node about to be terminated"
    );
    assert!(
        after.decrement_capacity,
        "the replacement intent must survive, or the ASG auto-replaces the \
         instance this terminate is shrinking away"
    );
    assert_eq!(after.started_at, before.started_at);
    assert_eq!(after.phase.as_deref(), Some("terminating"));
    // The guard must count target-only ownership through the handler, not
    // only through the store's occupancy helper. No Kubernetes action is
    // reachable on this refusal, so a real cluster adds no evidence.
    let held = insert_node(&db.pool, Some("drained"), Some("forced"), false).await?;
    super::super::node_store::terminate_gate_agreement_tests::insert_target_pinned(
        &db.pool,
        held.as_uuid(),
        "suspending",
        None,
    )
    .await?;
    let held_before = drain_columns(&db.pool, held).await?;
    let (status, body) = post_drain_response(
        &state,
        &actor,
        held,
        DrainAction::Terminate,
        DrainRequest::default(),
    )
    .await?;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["sandboxes_active"], 0);
    assert_eq!(body["sandboxes_drain_blockers"], 1);
    assert_eq!(body["sandboxes_local_snapshots"], 1);
    assert_eq!(drain_columns(&db.pool, held).await?, held_before);
    Ok(())
}

/// The escalation commits `forced` and keeps everything that describes the
/// drain it is escalating.
///
/// The whole edge, through the handler: the mode matrix, the replacement-intent
/// check, the write, and the phase the row is left in.
#[tokio::test(flavor = "current_thread")]
async fn escalating_commits_the_mode_and_keeps_the_drain() -> TestResult {
    if !enter_direct_test_process() {
        return Ok(());
    }
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let state = kubernetes_state(&db.pool).await?;
    let actor = managing_org_admin(&db.pool).await?;

    let node_id = insert_node(&db.pool, Some("draining"), Some("gentle"), false).await?;
    let before = drain_columns(&db.pool, node_id).await?;

    let status = post_drain(
        &state,
        &actor,
        node_id,
        DrainAction::Drain,
        DrainRequest {
            // Omitted, which is what `space-admin-cli drain-node --forced` sends.
            replace_after_terminate: None,
            mode: NodeDrainMode::Forced,
            ..DrainRequest::default()
        },
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_GATEWAY,
        "expected a post-commit 502"
    );

    let after = drain_columns(&db.pool, node_id).await?;
    assert_eq!(after.mode.as_deref(), Some("forced"));
    assert_eq!(
        after.started_at, before.started_at,
        "an escalation must not restart the drain's clock"
    );
    assert_eq!(
        after.decrement_capacity, before.decrement_capacity,
        "and must not touch the replacement intent"
    );
    assert_eq!(after.phase.as_deref(), Some("draining"));
    Ok(())
}

/// `require_undrained` refuses the request an orchestrator would otherwise
/// turn into an escalation, and still starts a drain on an undrained node.
///
/// The refused node is the destructive case: a human's gentle drain started
/// with `--no-replace-after-terminate`, met by a forced `:drain` whose
/// replacement intent agrees. Without the precondition that request passes the
/// mode matrix and the intent check and escalates the drain.
#[tokio::test(flavor = "current_thread")]
async fn require_undrained_refuses_an_existing_drain_and_starts_a_new_one() -> TestResult {
    if !enter_direct_test_process() {
        return Ok(());
    }
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let state = kubernetes_state(&db.pool).await?;
    let actor = managing_org_admin(&db.pool).await?;
    let start_only = || DrainRequest {
        replace_after_terminate: Some(false),
        mode: NodeDrainMode::Forced,
        require_undrained: true,
        engine: None,
        hold_seconds: None,
    };

    // A gentle drain whose stored intent agrees with the posted `false`.
    let draining = insert_node(&db.pool, Some("draining"), Some("gentle"), false).await?;
    let before = drain_columns(&db.pool, draining).await?;
    let (status, body) =
        post_drain_response(&state, &actor, draining, DrainAction::Drain, start_only()).await?;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["current_drain_phase"], "draining");
    assert_eq!(
        drain_columns(&db.pool, draining).await?,
        before,
        "a refused start-only request must write nothing"
    );

    // Without the precondition the same request escalates, so the refusal
    // above is the precondition's and not some other guard's.
    let control = insert_node(&db.pool, Some("draining"), Some("gentle"), false).await?;
    let status = post_drain(
        &state,
        &actor,
        control,
        DrainAction::Drain,
        DrainRequest {
            require_undrained: false,
            ..start_only()
        },
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_GATEWAY,
        "expected a post-commit 502"
    );
    assert_eq!(
        drain_columns(&db.pool, control).await?.mode.as_deref(),
        Some("forced")
    );

    // An undrained node still drains.
    let fresh = insert_node(&db.pool, None, None, false).await?;
    let status = post_drain(&state, &actor, fresh, DrainAction::Drain, start_only()).await;
    assert_eq!(
        status,
        StatusCode::BAD_GATEWAY,
        "expected a post-commit 502"
    );
    let after = drain_columns(&db.pool, fresh).await?;
    assert_eq!(after.phase.as_deref(), Some("draining"));
    assert_eq!(after.mode.as_deref(), Some("forced"));
    Ok(())
}

/// A `:drain` carrying a `replace_after_terminate` that disagrees with the drain
/// in flight is refused, and nothing is written.
///
/// The guard is gated on the transition, and running it on the drain-*starting*
/// edge would 409 fresh drains — so both halves are driven here: the refusal on
/// the in-flight edge, and a fresh drain of a node carrying a previous drain's
/// intent, which must still be accepted.
#[tokio::test(flavor = "current_thread")]
async fn a_disagreeing_replacement_intent_is_refused_only_in_flight() -> TestResult {
    if !enter_direct_test_process() {
        return Ok(());
    }
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let state = kubernetes_state(&db.pool).await?;
    let actor = managing_org_admin(&db.pool).await?;

    // In flight, started with `--no-replace-after-terminate`. A request asking
    // to auto-replace disagrees.
    let in_flight = insert_node(&db.pool, Some("draining"), Some("gentle"), false).await?;
    let before = drain_columns(&db.pool, in_flight).await?;
    let status = post_drain(
        &state,
        &actor,
        in_flight,
        DrainAction::Drain,
        DrainRequest {
            replace_after_terminate: Some(true),
            mode: NodeDrainMode::Forced,
            ..DrainRequest::default()
        },
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(
        drain_columns(&db.pool, in_flight).await?,
        before,
        "a refused request must write nothing at all, mode included"
    );

    // Undrained, but still carrying the previous drain's intent — the column is
    // NOT NULL and `:undrain` does not clear it. A fresh drain must not be
    // refused for disagreeing with a drain that no longer exists.
    let undrained = insert_node(&db.pool, None, None, false).await?;
    let status = post_drain(
        &state,
        &actor,
        undrained,
        DrainAction::Drain,
        DrainRequest {
            replace_after_terminate: Some(true),
            mode: NodeDrainMode::Forced,
            ..DrainRequest::default()
        },
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_GATEWAY,
        "a fresh drain must be accepted despite the stale intent column"
    );
    assert!(
        !drain_columns(&db.pool, undrained).await?.decrement_capacity,
        "and must persist its own intent"
    );
    Ok(())
}

/// Downgrading a forced drain in place is refused, and the phase table's own
/// edges still answer 409 where they did.
#[tokio::test(flavor = "current_thread")]
async fn the_refusals_reach_the_response_and_write_nothing() -> TestResult {
    if !enter_direct_test_process() {
        return Ok(());
    }
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let state = kubernetes_state(&db.pool).await?;
    let actor = managing_org_admin(&db.pool).await?;

    // forced -> gentle in place.
    let forced = insert_node(&db.pool, Some("draining"), Some("forced"), true).await?;
    let before = drain_columns(&db.pool, forced).await?;
    assert_eq!(
        post_drain(
            &state,
            &actor,
            forced,
            DrainAction::Drain,
            DrainRequest::default(),
        )
        .await,
        StatusCode::CONFLICT
    );
    assert_eq!(drain_columns(&db.pool, forced).await?, before);

    // `:drain` on a node past `draining` is still a phase conflict.
    for phase in ["drained", "terminating"] {
        let node_id = insert_node(&db.pool, Some(phase), Some("forced"), true).await?;
        let before = drain_columns(&db.pool, node_id).await?;
        assert_eq!(
            post_drain(
                &state,
                &actor,
                node_id,
                DrainAction::Drain,
                DrainRequest {
                    replace_after_terminate: None,
                    mode: NodeDrainMode::Forced,
                    ..DrainRequest::default()
                },
            )
            .await,
            StatusCode::CONFLICT,
            "{phase}: re-draining a node past draining is a conflict"
        );
        assert_eq!(drain_columns(&db.pool, node_id).await?, before);
    }
    Ok(())
}

/// A 502 on a `draining` node must promise the retry the reconciler will
/// actually perform — and that promise is computed from the phase the row is
/// left in, which for a cordon-only request is the phase it already had.
///
/// The composition this pins: `phase_after`'s second argument. The helper has
/// its own test, but passing `None` there — a plausible simplification, since
/// three of its four arms ignore it — survived every other test in the crate.
/// Under that mutation `:cordon` on a draining node whose kube patch fails
/// answers `reconciler_will_retry: false`, telling the operator to intervene by
/// hand on the one edge where the reconciler re-cordons every tick.
///
/// Driven against a `managed_by = 'k8s'` row on purpose: a direct-managed one
/// answers `false` whatever the phase, so it could not tell a correct hint from
/// one computed off the wrong phase.
#[tokio::test(flavor = "current_thread")]
async fn a_cordon_on_a_draining_node_promises_the_retry_the_reconciler_performs() -> TestResult {
    if !enter_direct_test_process() {
        return Ok(());
    }
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let state = kubernetes_state(&db.pool).await?;
    let actor = managing_org_admin(&db.pool).await?;
    let node_id = insert_k8s_draining_node(&db.pool).await?;

    let (status, body) = post_drain_response(
        &state,
        &actor,
        node_id,
        DrainAction::Cordon,
        DrainRequest::default(),
    )
    .await?;

    assert_eq!(
        status,
        StatusCode::BAD_GATEWAY,
        "no apiserver in a unit test, so the cordon patch fails; the 502 body is \
         the subject"
    );
    assert_eq!(
        body["reconciler_will_retry"],
        serde_json::Value::Bool(true),
        "the node is left `draining`, which the node-drain reconciler re-cordons \
         every tick — got {body}"
    );
    assert_eq!(
        body["db_state"]["drain_phase"], "draining",
        "and the echoed state must be the phase the row actually holds"
    );
    Ok(())
}

/// An undrained Kubernetes node in `drain_phase` (NULL for none), with its
/// `node_k8s` row, under drain `engine` with `status`.
async fn insert_k8s_node(
    pool: &PgPool,
    drain_phase: Option<&str>,
    engine: Option<&str>,
    status: Option<serde_json::Value>,
) -> TestResult<NodeId> {
    let node_id = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (
             id, address, role, managed_by, state, drain_phase, drain_mode,
             drain_started_at, terminate_decrement_capacity, drain_engine, drain_status
         )
         VALUES (
             $1, '10.0.0.46'::INET, 'worker', 'k8s', 'registered', $2,
             CASE WHEN $2::TEXT IS NULL OR $3::TEXT IS NOT NULL THEN NULL ELSE 'forced' END,
             CASE WHEN $2::TEXT IS NULL THEN NULL ELSE NOW() - INTERVAL '1 hour' END,
             false, $3, $4
         )"
    ))
    .bind(node_id)
    .bind(drain_phase)
    .bind(engine)
    .bind(status)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name, instance_id) VALUES ($1, $2, 'i-0a1b2c3d')"
    ))
    .bind(node_id)
    .bind(format!("ip-10-0-0-46-{node_id}"))
    .execute(pool)
    .await?;
    // A fresh heartbeat: a v2 drain starts only on a node that answers.
    sqlx::query(sql!(
        "INSERT INTO node_status_snapshots (
             node_id, observed_at, stale_after, reachable, services,
             sandbox_inventory_pruned_at, updated_at
         ) VALUES ($1, NOW(), NOW() + INTERVAL '1 hour', TRUE, '[]'::JSONB, NOW(), NOW())"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;
    Ok(node_id)
}

/// The phase, engine, status and the drain's start in microseconds.
async fn v2_columns(
    pool: &PgPool,
    node_id: NodeId,
) -> TestResult<(
    Option<String>,
    Option<String>,
    Option<serde_json::Value>,
    Option<i64>,
)> {
    Ok(sqlx::query_as(sql!(
        "SELECT drain_phase, drain_engine, drain_status,
                (EXTRACT(EPOCH FROM drain_started_at) * 1000000)::BIGINT
           FROM nodes WHERE id = $1"
    ))
    .bind(node_id)
    .fetch_one(pool)
    .await?)
}

/// A v2 `:drain` with `hold_seconds`.
fn v2_request(hold_seconds: Option<u64>) -> DrainRequest {
    DrainRequest {
        replace_after_terminate: None,
        mode: NodeDrainMode::Gentle,
        require_undrained: false,
        engine: Some(pplx_space_node_drain::NodeDrainEngine::V2),
        hold_seconds,
    }
}

/// The drain's `drain_status.deadline_at` and its start in whole Unix
/// seconds.
async fn deadline_and_start(pool: &PgPool, node_id: NodeId) -> TestResult<(i64, i64)> {
    Ok(sqlx::query_as(sql!(
        "SELECT (drain_status->>'deadline_at')::BIGINT,
                FLOOR(EXTRACT(EPOCH FROM drain_started_at))::BIGINT
           FROM nodes WHERE id = $1"
    ))
    .bind(node_id)
    .fetch_one(pool)
    .await?)
}

/// A v2 `:drain` records the engine, the generation, the drain's start in
/// microseconds, and the deadline, `hold_seconds` after that start, in the
/// transaction that moves the phase, and no mode. A repeat without a hold
/// keeps all of it; one with a shorter hold moves the deadline earlier, and
/// one with a longer hold is a 409 that writes nothing.
#[tokio::test(flavor = "current_thread")]
async fn a_v2_drain_records_its_generation_and_deadline_with_the_phase() -> TestResult {
    if !enter_direct_test_process() {
        return Ok(());
    }
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let mut state = kubernetes_state(&db.pool).await?;
    state.drain_v2_enabled = true;
    let actor = managing_org_admin(&db.pool).await?;
    let node_id = insert_k8s_node(&db.pool, None, None, None).await?;

    post_drain(
        &state,
        &actor,
        node_id,
        DrainAction::Drain,
        v2_request(Some(3600)),
    )
    .await;
    let (phase, engine, status, started_us) = v2_columns(&db.pool, node_id).await?;
    assert_eq!(phase.as_deref(), Some("draining"));
    assert_eq!(engine.as_deref(), Some("v2"));
    let (deadline, started) = deadline_and_start(&db.pool, node_id).await?;
    assert_eq!(
        status,
        started_us.map(|generation| {
            serde_json::json!({"generation": generation, "deadline_at": started + 3600})
        }),
    );
    assert_eq!(deadline, started + 3600);
    assert_eq!(
        drain_columns(&db.pool, node_id).await?.mode,
        None,
        "no mode"
    );

    for repeat in [
        v2_request(None),
        DrainRequest {
            engine: None,
            ..v2_request(None)
        },
        v2_request(Some(3600)),
    ] {
        post_drain(&state, &actor, node_id, DrainAction::Drain, repeat).await;
        assert_eq!(
            v2_columns(&db.pool, node_id).await?.2,
            status,
            "a repeat keeps it"
        );
        assert_eq!(
            drain_columns(&db.pool, node_id).await?.mode,
            None,
            "a repeat writes no mode"
        );
    }
    let before = v2_columns(&db.pool, node_id).await?;
    let (answer, body) = post_drain_response(
        &state,
        &actor,
        node_id,
        DrainAction::Drain,
        v2_request(Some(7200)),
    )
    .await?;
    assert_eq!(answer, StatusCode::CONFLICT, "{body}");
    assert_eq!(
        v2_columns(&db.pool, node_id).await?,
        before,
        "a longer hold writes nothing"
    );
    for shorter in [600_u64, 0] {
        post_drain(
            &state,
            &actor,
            node_id,
            DrainAction::Drain,
            DrainRequest {
                engine: None,
                ..v2_request(Some(shorter))
            },
        )
        .await;
        assert_eq!(
            deadline_and_start(&db.pool, node_id).await?,
            (started + i64::try_from(shorter)?, started),
            "a shorter hold moves the deadline earlier"
        );
    }
    Ok(())
}

/// A v2 `:drain` is refused, writing nothing: without the flag, on a Direct
/// node, without a hold, with a hold over a year, with `mode: forced`
/// (`hold_seconds: 0` stands for it), against a v1 drain in flight, on a
/// node with no recorded instance, and on one whose heartbeat is stale
/// (409, `code` `drain_v2_node_heartbeat_stale`). A hold on a v1 drain is
/// refused too. The control, a v1 drain, still starts.
#[tokio::test(flavor = "current_thread")]
async fn a_v2_drain_it_cant_run_is_refused_and_writes_nothing() -> TestResult {
    if !enter_direct_test_process() {
        return Ok(());
    }
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let enabled = {
        let mut state = kubernetes_state(&db.pool).await?;
        state.drain_v2_enabled = true;
        state
    };
    let disabled = kubernetes_state(&db.pool).await?;
    let actor = managing_org_admin(&db.pool).await?;
    let stale = {
        let node_id = insert_k8s_node(&db.pool, None, None, None).await?;
        sqlx::query(sql!(
            "UPDATE node_status_snapshots SET stale_after = NOW() - INTERVAL '1 minute'
              WHERE node_id = $1"
        ))
        .bind(node_id)
        .execute(&db.pool)
        .await?;
        node_id
    };

    let cases = [
        (
            "flag off",
            &disabled,
            insert_k8s_node(&db.pool, None, None, None).await?,
            v2_request(Some(0)),
            StatusCode::BAD_REQUEST,
            None,
        ),
        (
            "direct node",
            &enabled,
            insert_node(&db.pool, None, None, true).await?,
            v2_request(Some(0)),
            StatusCode::CONFLICT,
            None,
        ),
        (
            "no hold",
            &enabled,
            insert_k8s_node(&db.pool, None, None, None).await?,
            v2_request(None),
            StatusCode::BAD_REQUEST,
            None,
        ),
        (
            "a hold over a year",
            &enabled,
            insert_k8s_node(&db.pool, None, None, None).await?,
            v2_request(Some(366 * 24 * 60 * 60)),
            StatusCode::BAD_REQUEST,
            None,
        ),
        (
            "mode forced",
            &enabled,
            insert_k8s_node(&db.pool, None, None, None).await?,
            DrainRequest {
                mode: NodeDrainMode::Forced,
                ..v2_request(Some(0))
            },
            StatusCode::BAD_REQUEST,
            None,
        ),
        (
            "v1 in flight",
            &enabled,
            insert_k8s_node(&db.pool, Some("draining"), None, None).await?,
            v2_request(Some(0)),
            StatusCode::CONFLICT,
            None,
        ),
        (
            "no recorded instance",
            &enabled,
            {
                let node_id = insert_k8s_node(&db.pool, None, None, None).await?;
                sqlx::query(sql!(
                    "UPDATE node_k8s SET instance_id = NULL WHERE node_id = $1"
                ))
                .bind(node_id)
                .execute(&db.pool)
                .await?;
                node_id
            },
            v2_request(Some(0)),
            StatusCode::CONFLICT,
            None,
        ),
        (
            "a stale heartbeat",
            &enabled,
            stale,
            v2_request(Some(0)),
            StatusCode::CONFLICT,
            Some("drain_v2_node_heartbeat_stale"),
        ),
        (
            "a hold on a v1 drain",
            &enabled,
            insert_k8s_node(&db.pool, None, None, None).await?,
            DrainRequest {
                engine: None,
                ..v2_request(Some(0))
            },
            StatusCode::BAD_REQUEST,
            None,
        ),
    ];
    for (case, state, node_id, request, expected, code) in cases {
        let before = (
            v2_columns(&db.pool, node_id).await?,
            drain_columns(&db.pool, node_id).await?,
        );
        let (status, body) =
            post_drain_response(state, &actor, node_id, DrainAction::Drain, request).await?;
        assert_eq!(status, expected, "{case}: {body}");
        if let Some(code) = code {
            assert_eq!(body["code"], code, "{case}: {body}");
        }
        assert_eq!(
            (
                v2_columns(&db.pool, node_id).await?,
                drain_columns(&db.pool, node_id).await?,
            ),
            before,
            "{case}: wrote"
        );
    }

    let control = insert_k8s_node(&db.pool, None, None, None).await?;
    post_drain(
        &disabled,
        &actor,
        control,
        DrainAction::Drain,
        DrainRequest {
            engine: None,
            hold_seconds: None,
            ..v2_request(None)
        },
    )
    .await;
    let (phase, engine, status, _) = v2_columns(&db.pool, control).await?;
    assert_eq!(
        (phase.as_deref(), engine, status),
        (Some("draining"), None, None)
    );
    Ok(())
}

/// A v2 drain's start takes one of the cluster's evacuation slots: with the
/// cap held by other nodes' drains, `draining` or undrained with their
/// cancel unfinished (no phase), holds included, it is a 409 with the stable
/// `code` `drain_v2_evacuation_cap` and a numeric `Retry-After`, and writes
/// nothing. A drained node's and a terminating node's free theirs, and a
/// repeat that shortens a drain in flight takes none.
#[tokio::test(flavor = "current_thread")]
async fn a_v2_drain_at_the_evacuation_cap_is_refused_with_a_retry() -> TestResult {
    if !enter_direct_test_process() {
        return Ok(());
    }
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let mut state = kubernetes_state(&db.pool).await?;
    state.drain_v2_enabled = true;
    state.drain_v2_max_evacuating = 2;
    let actor = managing_org_admin(&db.pool).await?;
    let holding = serde_json::json!({
        "generation": 7, "deadline_at": 4_102_444_800_i64,
        "report": {"reported_at": 1, "evacuated_grew_at": 1,
                   "pending": [{"sandbox_id": "a", "wait": "hold", "running": true}]},
    });
    let draining = insert_k8s_node(
        &db.pool,
        Some("draining"),
        Some("v2"),
        Some(holding.clone()),
    )
    .await?;
    let cancelling = insert_k8s_node(&db.pool, None, Some("v2"), Some(holding.clone())).await?;
    for phase in [Some("drained"), Some("terminating")] {
        insert_k8s_node(&db.pool, phase, Some("v2"), Some(holding.clone())).await?;
    }
    let next = insert_k8s_node(&db.pool, None, None, None).await?;

    let before = v2_columns(&db.pool, next).await?;
    let response = apply_drain_action(
        State(state.clone()),
        Extension(AuthUser(actor.clone())),
        next,
        DrainAction::Drain,
        v2_request(Some(600)),
    )
    .await
    .into_response();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let retry_after = response
        .headers()
        .get(axum::http::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok());
    assert!(
        retry_after.is_some_and(|seconds| seconds > 0),
        "{retry_after:?}"
    );
    let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024).await?;
    let body: serde_json::Value = serde_json::from_slice(&bytes)?;
    assert_eq!(body["code"], "drain_v2_evacuation_cap", "{body}");
    assert_eq!(body["evacuating"], 2, "{body}");
    assert_eq!(
        v2_columns(&db.pool, next).await?,
        before,
        "a refusal writes nothing"
    );

    // A repeat that only shortens a drain in flight takes no other slot.
    post_drain(
        &state,
        &actor,
        draining,
        DrainAction::Drain,
        v2_request(Some(0)),
    )
    .await;
    assert_eq!(
        v2_columns(&db.pool, draining).await?.0.as_deref(),
        Some("draining")
    );

    // The cancel finishes on the node: its slot frees.
    sqlx::query(sql!(
        "UPDATE nodes SET drain_engine = NULL, drain_status = NULL WHERE id = $1"
    ))
    .bind(cancelling)
    .execute(&db.pool)
    .await?;
    post_drain(
        &state,
        &actor,
        next,
        DrainAction::Drain,
        v2_request(Some(600)),
    )
    .await;
    assert_eq!(
        v2_columns(&db.pool, next).await?.1.as_deref(),
        Some("v2"),
        "a free slot admits the drain"
    );
    Ok(())
}

/// `:undrain` of a v2 node clears the phase and keeps the engine and the
/// status: a v2 engine with no phase is the cancel the controller finishes
/// on the node. A v1 node's undrain writes no v2 column.
#[tokio::test(flavor = "current_thread")]
async fn undrain_of_a_v2_node_leaves_its_cancel_pending() -> TestResult {
    if !enter_direct_test_process() {
        return Ok(());
    }
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let state = kubernetes_state(&db.pool).await?;
    let actor = managing_org_admin(&db.pool).await?;
    let v2 = insert_k8s_node(
        &db.pool,
        Some("draining"),
        Some("v2"),
        Some(serde_json::json!({"generation": 42, "deadline_at": 0})),
    )
    .await?;
    let v1 = insert_k8s_node(&db.pool, Some("draining"), None, None).await?;

    for node_id in [v2, v1] {
        post_drain(
            &state,
            &actor,
            node_id,
            DrainAction::Undrain,
            DrainRequest::default(),
        )
        .await;
    }
    let (phase, engine, status, _) = v2_columns(&db.pool, v2).await?;
    assert_eq!(phase, None);
    assert_eq!(engine.as_deref(), Some("v2"));
    assert_eq!(
        status,
        Some(serde_json::json!({"generation": 42, "deadline_at": 0})),
        "no cancel key"
    );
    let (phase, engine, status, _) = v2_columns(&db.pool, v1).await?;
    assert_eq!((phase, engine, status), (None, None, None));
    Ok(())
}

/// `:undrain` of a v2 node leaves the node's cordon for the controller to
/// lift once the node answers unsealed, so it asks the substrate nothing:
/// with no reachable API server it answers 200 where a v1 node's undrain,
/// whose uncordon fails, answers 502.
#[tokio::test(flavor = "current_thread")]
async fn undrain_of_a_v2_node_leaves_its_cordon_to_the_controller() -> TestResult {
    if !enter_direct_test_process() {
        return Ok(());
    }
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let state = kubernetes_state(&db.pool).await?;
    let actor = managing_org_admin(&db.pool).await?;
    let v2 = insert_k8s_node(
        &db.pool,
        Some("draining"),
        Some("v2"),
        Some(serde_json::json!({"generation": 42, "deadline_at": 0})),
    )
    .await?;
    let v1 = insert_k8s_node(&db.pool, Some("draining"), None, None).await?;
    for (node_id, expected) in [(v2, StatusCode::OK), (v1, StatusCode::BAD_GATEWAY)] {
        let (status, body) = post_drain_response(
            &state,
            &actor,
            node_id,
            DrainAction::Undrain,
            DrainRequest::default(),
        )
        .await?;
        assert_eq!(status, expected, "{body}");
        assert_eq!(v2_columns(&db.pool, node_id).await?.0, None);
    }
    Ok(())
}

/// A started sandbox row active on `node_id`.
async fn insert_resident_sandbox(pool: &PgPool, node_id: NodeId) -> TestResult {
    let sandbox_id = pplx_space_sandbox_model::SandboxId::new();
    crate::controllers::substrate::kubernetes::tests::insert_test_sandbox_row(
        pool, sandbox_id, "started",
    )
    .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET active_node_id = $2 WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// `:terminate` of a v2 node moves `drained` to `terminating` without asking
/// the node and whatever its stored report or what still names the node
/// says: the cluster controller's terminating tick makes the fresh check
/// and holds the fence. Any other phase is refused on the phase.
#[tokio::test(flavor = "current_thread")]
async fn terminate_of_a_v2_node_refuses_only_on_its_phase() -> TestResult {
    if !enter_direct_test_process() {
        return Ok(());
    }
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let actor = managing_org_admin(&db.pool).await?;
    let state = kubernetes_state(&db.pool).await?;
    let pending = serde_json::json!({
        "generation": 42, "deadline_at": 0,
        "report": {"reported_at": 1, "evacuated_grew_at": 1,
                   "pending": [{"sandbox_id": "a", "wait": "moving", "running": false}]},
    });
    for (case, phase, status, held, moves) in [
        (
            "drained, no report",
            Some("drained"),
            serde_json::json!({"generation": 42, "deadline_at": 0}),
            false,
            true,
        ),
        (
            "drained, a stale pending report",
            Some("drained"),
            pending.clone(),
            false,
            true,
        ),
        (
            "drained, a row still naming it",
            Some("drained"),
            serde_json::json!({"generation": 42, "deadline_at": 0}),
            true,
            true,
        ),
        ("draining", Some("draining"), pending.clone(), false, false),
        (
            "a cancel pending",
            None,
            serde_json::json!({"generation": 42, "deadline_at": 0}),
            false,
            false,
        ),
    ] {
        let node_id = insert_k8s_node(&db.pool, phase, Some("v2"), Some(status)).await?;
        if held {
            insert_resident_sandbox(&db.pool, node_id).await?;
        }
        let answer = post_drain(
            &state,
            &actor,
            node_id,
            DrainAction::Terminate,
            DrainRequest::default(),
        )
        .await;
        let after = v2_columns(&db.pool, node_id).await?.0;
        if moves {
            assert_eq!(answer, StatusCode::OK, "{case}");
            assert_eq!(after.as_deref(), Some("terminating"), "{case}");
        } else {
            assert_eq!(answer, StatusCode::CONFLICT, "{case}");
            assert_eq!(after.as_deref(), phase, "{case}");
        }
    }
    Ok(())
}

/// A `:drain` of an undrained v2 node whose cancel hasn't finished on the
/// node is a 409, "undrain still finishing", under any engine, and writes
/// nothing: the node may still be sealed under the old generation. Once the
/// controller finished the cancel, the drain starts.
#[tokio::test(flavor = "current_thread")]
async fn a_drain_over_a_pending_v2_cancel_is_refused() -> TestResult {
    if !enter_direct_test_process() {
        return Ok(());
    }
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let mut state = kubernetes_state(&db.pool).await?;
    state.drain_v2_enabled = true;
    let actor = managing_org_admin(&db.pool).await?;
    let node_id = insert_k8s_node(
        &db.pool,
        None,
        Some("v2"),
        Some(serde_json::json!({"generation": 42, "deadline_at": 0})),
    )
    .await?;
    let before = v2_columns(&db.pool, node_id).await?;
    for request in [
        DrainRequest {
            engine: None,
            hold_seconds: None,
            ..v2_request(None)
        },
        DrainRequest {
            engine: Some(pplx_space_node_drain::NodeDrainEngine::V1),
            hold_seconds: None,
            ..v2_request(None)
        },
        v2_request(Some(60)),
    ] {
        let (answer, body) =
            post_drain_response(&state, &actor, node_id, DrainAction::Drain, request).await?;
        assert_eq!(answer, StatusCode::CONFLICT, "{body}");
        assert!(
            body["error"]
                .as_str()
                .is_some_and(|error| error.starts_with("undrain still finishing")),
            "{body}"
        );
        assert_eq!(v2_columns(&db.pool, node_id).await?, before, "wrote");
    }
    sqlx::query(sql!(
        "UPDATE nodes SET drain_engine = NULL, drain_status = NULL WHERE id = $1"
    ))
    .bind(node_id)
    .execute(&db.pool)
    .await?;
    post_drain(
        &state,
        &actor,
        node_id,
        DrainAction::Drain,
        v2_request(Some(60)),
    )
    .await;
    assert_eq!(
        v2_columns(&db.pool, node_id).await?.0.as_deref(),
        Some("draining")
    );
    Ok(())
}

/// The admin node response's `drain`, read from each shape of stored
/// `drain_status` through the store's typed `StoredV2Status`: absent for a v1
/// drain or none, and for a stored status that doesn't read; for a v2 drain
/// with no report yet, one holding, one mid-suspend with a pool member, and
/// a cancel pending (a v2 engine with no phase), each with what the report
/// and the controller's stamps say, `evacuated_total` and the observed set
/// included.
#[test]
fn the_drain_field_reads_each_stored_v2_status() {
    use pplx_space_admin_protocol::{
        AdminNodeDrain, AdminNodeDrainBlocked, AdminNodeDrainEvacuated, AdminNodeDrainPending,
    };
    let node_id = NodeId::new();
    let at = |seconds: i64| {
        OffsetDateTime::from_unix_timestamp(seconds)
            .expect("in range")
            .format(&time::format_description::well_known::Rfc3339)
            .expect("an RFC 3339 timestamp")
    };
    let deadline = 1_800_003_600_i64;
    let report = |pending: serde_json::Value| {
        serde_json::json!({
            "reported_at": 1_800_000_100_i64,
            "evacuated_grew_at": 1_800_000_000_i64,
            "evacuated": [
                {"sandbox_id": "s-running", "artifact_kind": "full", "artifact_id": "a1"},
                {"sandbox_id": "s-stopped", "artifact_kind": "disk", "artifact_id": "a2"},
            ],
            "pending": pending,
            "blocked": [{"reason": "untracked runtime process 42"}],
            "unrecorded": [{"sandbox_id": "s-stopped", "reason": "not yet", "since": 1}],
        })
    };
    let stored = |pending: serde_json::Value| {
        serde_json::json!({
            "generation": 7, "deadline_at": deadline,
            "observed_running": ["s-held", "s-running"],
            "evacuated_total": ["s-gone", "s-running"],
            "report": report(pending),
        })
    };
    let holds = serde_json::json!([{"sandbox_id": "s-held", "wait": "hold", "running": true}]);
    let suspending = serde_json::json!([
        {"sandbox_id": "s-held", "wait": "moving", "running": true,
         "reason": "the window is full", "since": 1_799_999_000_i64},
        {"sandbox_id": "s-pool", "wait": "external", "running": true, "pool_member": true},
    ]);
    let view = |pending: Vec<AdminNodeDrainPending>, cancelling: bool| AdminNodeDrain {
        generation: 7,
        deadline: Some(at(deadline)),
        cancelling,
        fenced: false,
        reported_at: Some(at(1_800_000_100)),
        evacuated_grew_at: Some(at(1_800_000_000)),
        evacuated_total: vec!["s-gone".into(), "s-running".into()],
        evacuated: vec![
            AdminNodeDrainEvacuated {
                sandbox_id: "s-running".into(),
                artifact_kind: "full".into(),
                was_running: true,
                recorded: true,
            },
            AdminNodeDrainEvacuated {
                sandbox_id: "s-stopped".into(),
                artifact_kind: "disk".into(),
                was_running: false,
                recorded: false,
            },
        ],
        pending,
        blocked: vec![AdminNodeDrainBlocked {
            reason: "untracked runtime process 42".into(),
        }],
    };
    let held = || AdminNodeDrainPending {
        sandbox_id: "s-held".into(),
        wait: "hold".into(),
        running: true,
        pool_member: false,
        reason: None,
        since: None,
    };
    for (case, engine, phase, status, expected) in [
        ("no drain", None, None, None, None),
        (
            "a v1 drain",
            None,
            Some("draining"),
            Some(serde_json::json!({"generation": 7, "deadline_at": 0})),
            None,
        ),
        (
            "no report yet",
            Some("v2"),
            Some("draining"),
            Some(serde_json::json!({"generation": 7, "deadline_at": deadline})),
            Some(AdminNodeDrain {
                reported_at: None,
                evacuated_grew_at: None,
                evacuated_total: Vec::new(),
                evacuated: Vec::new(),
                blocked: Vec::new(),
                ..view(Vec::new(), false)
            }),
        ),
        (
            "holding",
            Some("v2"),
            Some("draining"),
            Some(stored(holds.clone())),
            Some(view(vec![held()], false)),
        ),
        (
            "mid-suspend, with a pool member",
            Some("v2"),
            Some("draining"),
            Some(stored(suspending)),
            Some(view(
                vec![
                    AdminNodeDrainPending {
                        wait: "moving".into(),
                        reason: Some("the window is full".into()),
                        since: Some(at(1_799_999_000)),
                        ..held()
                    },
                    AdminNodeDrainPending {
                        sandbox_id: "s-pool".into(),
                        wait: "external".into(),
                        pool_member: true,
                        ..held()
                    },
                ],
                false,
            )),
        ),
        (
            "a cancel pending",
            Some("v2"),
            None,
            Some(stored(holds.clone())),
            Some(view(vec![held()], true)),
        ),
        (
            "malformed",
            Some("v2"),
            Some("draining"),
            Some(serde_json::json!({"generation": "seven", "report": []})),
            None,
        ),
    ] {
        assert_eq!(
            super::drain_v2::v2_drain_view(node_id, engine, phase, status.as_ref()),
            expected,
            "{case}"
        );
    }
}

/// A v2 drain refused on a node whose heartbeat is stale is told to retry
/// after 60 seconds.
#[tokio::test(flavor = "current_thread")]
async fn a_stale_heartbeat_refusal_says_retry_after_a_minute() -> TestResult {
    if !enter_direct_test_process() {
        return Ok(());
    }
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let mut state = kubernetes_state(&db.pool).await?;
    state.drain_v2_enabled = true;
    let actor = managing_org_admin(&db.pool).await?;
    let node_id = insert_k8s_node(&db.pool, None, None, None).await?;
    sqlx::query(sql!(
        "UPDATE node_status_snapshots SET stale_after = NOW() - INTERVAL '1 minute'
          WHERE node_id = $1"
    ))
    .bind(node_id)
    .execute(&db.pool)
    .await?;
    let response = apply_drain_action(
        State(state),
        Extension(AuthUser(actor)),
        node_id,
        DrainAction::Drain,
        v2_request(Some(0)),
    )
    .await
    .into_response();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert_eq!(
        response
            .headers()
            .get(axum::http::header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok()),
        Some("60")
    );
    Ok(())
}

/// A node's listing carries its drain engine and, for a v2 drain, its
/// `drain`; a stored status that doesn't read leaves the field out and the
/// node's read goes on.
#[tokio::test(flavor = "current_thread")]
async fn a_node_reads_back_its_drain_even_when_the_status_is_malformed() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    for (status, readable) in [
        (
            serde_json::json!({"generation": 42, "deadline_at": 0,
                "report": {"reported_at": 1, "evacuated_grew_at": 1}}),
            true,
        ),
        (serde_json::json!({"generation": "forty-two"}), false),
    ] {
        let node_id =
            insert_k8s_node(&db.pool, Some("drained"), Some("v2"), Some(status.clone())).await?;
        let row = super::AdminNodeStore::new(db.pool.clone())
            .fetch(node_id)
            .await?
            .ok_or("the node reads back")?;
        let response = super::admin_node_response(row, None, None);
        assert_eq!(response.drain_engine.as_deref(), Some("v2"));
        assert_eq!(response.drain.is_some(), readable, "{status}");
        if let Some(drain) = response.drain {
            assert_eq!(drain.generation, 42);
            assert!(drain.pending.is_empty() && drain.blocked.is_empty());
        }
    }
    Ok(())
}

/// `:terminate` of a node that doesn't exist answers 404, as every other
/// action does.
#[tokio::test(flavor = "current_thread")]
async fn terminate_of_an_unknown_node_is_not_found() -> TestResult {
    if !enter_direct_test_process() {
        return Ok(());
    }
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let state = kubernetes_state(&db.pool).await?;
    let actor = managing_org_admin(&db.pool).await?;
    let answer = post_drain(
        &state,
        &actor,
        NodeId::new(),
        DrainAction::Terminate,
        DrainRequest::default(),
    )
    .await;
    assert_eq!(answer, StatusCode::NOT_FOUND);
    Ok(())
}

/// `:undrain` of a v2 node in `terminating` moves it to no phase, leaving its
/// cancel pending, as any v2 undrain does, until the controller fences its
/// instance; a fenced one is refused and left as it was. A v1 node in
/// `terminating` is refused as before.
#[tokio::test(flavor = "current_thread")]
async fn undrain_of_a_terminating_v2_node_runs_only_before_the_fence() -> TestResult {
    if !enter_direct_test_process() {
        return Ok(());
    }
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let state = kubernetes_state(&db.pool).await?;
    let actor = managing_org_admin(&db.pool).await?;
    let unfenced = serde_json::json!({"generation": 42, "deadline_at": 0});
    let mut fenced = unfenced.clone();
    fenced["terminate_instance"] = serde_json::json!("i-0a1b2c3d");
    for (case, engine, status, undrains) in [
        ("unfenced v2", Some("v2"), Some(unfenced.clone()), true),
        ("fenced v2", Some("v2"), Some(fenced.clone()), false),
        ("v1", None, None, false),
    ] {
        let node_id =
            insert_k8s_node(&db.pool, Some("terminating"), engine, status.clone()).await?;
        let answer = post_drain(
            &state,
            &actor,
            node_id,
            DrainAction::Undrain,
            DrainRequest::default(),
        )
        .await;
        let (phase, stored_engine, stored, _) = v2_columns(&db.pool, node_id).await?;
        if undrains {
            assert_eq!(phase, None, "{case}: {answer}");
            assert_eq!(stored_engine.as_deref(), Some("v2"), "{case}");
            assert_eq!(stored, status, "{case}: the status stays, no cancel key");
        } else {
            assert_eq!(answer, StatusCode::CONFLICT, "{case}");
            assert_eq!(phase.as_deref(), Some("terminating"), "{case}");
            assert_eq!(stored, status, "{case}");
        }
    }
    Ok(())
}
