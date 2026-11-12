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
use pplx_space_common_core::sql;
use pplx_space_common_core::types::admin::DrainRequest;
use pplx_space_common_core::types::{NodeDrainMode, NodeId, OrgId, SandboxOwnerSource, UserId};
use pplx_space_db_test_fixtures::try_pg_test_db;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use time::OffsetDateTime;

use super::tests::enter_direct_test_process;
use super::{DrainAction, V2DrainProbe, V2DrainReport, apply_drain_action};
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
    state.v2_drain_probe = std::sync::Arc::new(ScriptedProbe(Err("no node answers".into())));
    Ok(state)
}

/// A [`V2DrainProbe`] that answers every `DrainNode` call with its script.
struct ScriptedProbe(Result<V2DrainReport, String>);

impl V2DrainProbe for ScriptedProbe {
    fn drain_node<'a>(
        &'a self,
        _state: &'a AppState,
        _node_id: NodeId,
        _epoch: u64,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<V2DrainReport, String>> + Send + 'a>,
    > {
        Box::pin(std::future::ready(self.0.clone()))
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
             CASE WHEN $2::TEXT IS NULL THEN NULL ELSE 'forced' END,
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

fn v2_request(mode: NodeDrainMode) -> DrainRequest {
    DrainRequest {
        replace_after_terminate: None,
        mode,
        require_undrained: false,
        engine: Some(pplx_space_common_core::types::NodeDrainEngine::V2),
    }
}

/// A v2 `:drain` records the engine and the epoch, the drain's start in
/// microseconds, in the transaction that moves the phase, and starts the
/// drain waiting for an evacuation slot; a repeat keeps the epoch, and
/// doesn't send a drain the controller admitted back to waiting.
#[tokio::test(flavor = "current_thread")]
async fn a_v2_drain_records_its_epoch_with_the_phase() -> TestResult {
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
        v2_request(NodeDrainMode::Forced),
    )
    .await;
    let (phase, engine, status, started_us) = v2_columns(&db.pool, node_id).await?;
    assert_eq!(phase.as_deref(), Some("draining"));
    assert_eq!(engine.as_deref(), Some("v2"));
    let epoch = status
        .as_ref()
        .and_then(|status| status.get("epoch"))
        .and_then(serde_json::Value::as_i64);
    assert_eq!(epoch, started_us, "{status:?}");
    assert_eq!(
        status.as_ref().and_then(|status| status.get("waiting")),
        Some(&serde_json::Value::Bool(true)),
        "{status:?}"
    );

    post_drain(
        &state,
        &actor,
        node_id,
        DrainAction::Drain,
        v2_request(NodeDrainMode::Forced),
    )
    .await;
    assert_eq!(
        v2_columns(&db.pool, node_id).await?.2,
        status,
        "a repeat keeps the epoch"
    );

    sqlx::query(sql!(
        "UPDATE nodes SET drain_status = drain_status || '{\"waiting\": false}'::JSONB
          WHERE id = $1"
    ))
    .bind(node_id)
    .execute(&db.pool)
    .await?;
    post_drain(
        &state,
        &actor,
        node_id,
        DrainAction::Drain,
        v2_request(NodeDrainMode::Forced),
    )
    .await;
    let admitted = v2_columns(&db.pool, node_id).await?.2;
    assert_eq!(
        admitted.as_ref().and_then(|status| status.get("waiting")),
        Some(&serde_json::Value::Bool(false)),
        "a repeat leaves an admitted drain admitted: {admitted:?}"
    );
    Ok(())
}

/// A v2 `:drain` is refused, writing nothing, without the flag, on a Direct
/// node, as a gentle drain, against a v1 drain in flight, and on a node
/// with no recorded instance; the control, a v1 drain, still starts.
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

    let cases = [
        (
            "flag off",
            &disabled,
            insert_k8s_node(&db.pool, None, None, None).await?,
            NodeDrainMode::Forced,
            StatusCode::BAD_REQUEST,
        ),
        (
            "direct node",
            &enabled,
            insert_node(&db.pool, None, None, true).await?,
            NodeDrainMode::Forced,
            StatusCode::CONFLICT,
        ),
        (
            "gentle",
            &enabled,
            insert_k8s_node(&db.pool, None, None, None).await?,
            NodeDrainMode::Gentle,
            StatusCode::BAD_REQUEST,
        ),
        (
            "v1 in flight",
            &enabled,
            insert_k8s_node(&db.pool, Some("draining"), None, None).await?,
            NodeDrainMode::Forced,
            StatusCode::CONFLICT,
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
            NodeDrainMode::Forced,
            StatusCode::CONFLICT,
        ),
    ];
    for (case, state, node_id, mode, expected) in cases {
        let before = v2_columns(&db.pool, node_id).await?;
        let status = post_drain(state, &actor, node_id, DrainAction::Drain, v2_request(mode)).await;
        assert_eq!(status, expected, "{case}");
        assert_eq!(
            v2_columns(&db.pool, node_id).await?,
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
            ..v2_request(NodeDrainMode::Forced)
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

/// `:undrain` of a v2 node clears the phase and keeps the epoch, marked for
/// the controller to cancel on the node; a v1 node's undrain writes no v2
/// column.
#[tokio::test(flavor = "current_thread")]
async fn undrain_marks_a_v2_epoch_for_cancel() -> TestResult {
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
        Some(serde_json::json!({"epoch": 42})),
    )
    .await?;
    let v1 = insert_k8s_node(&db.pool, Some("draining"), None, None).await?;

    post_drain(
        &state,
        &actor,
        v2,
        DrainAction::Undrain,
        DrainRequest::default(),
    )
    .await;
    post_drain(
        &state,
        &actor,
        v1,
        DrainAction::Undrain,
        DrainRequest::default(),
    )
    .await;
    let (phase, engine, status, _) = v2_columns(&db.pool, v2).await?;
    assert_eq!(phase, None);
    assert_eq!(engine.as_deref(), Some("v2"));
    assert_eq!(
        status,
        Some(serde_json::json!({"epoch": 42, "cancel": true}))
    );
    let (phase, engine, status, _) = v2_columns(&db.pool, v1).await?;
    assert_eq!((phase, engine, status), (None, None, None));
    Ok(())
}

/// A drained v2 node is terminable only when its stored report and a fresh
/// `DrainNode` call both say `done` under its drain's epoch, and then a
/// resident row doesn't hold it; otherwise the request is refused and the
/// phase stays `drained`.
#[tokio::test(flavor = "current_thread")]
async fn terminate_of_a_v2_node_needs_a_done_report_for_its_epoch() -> TestResult {
    if !enter_direct_test_process() {
        return Ok(());
    }
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let actor = managing_org_admin(&db.pool).await?;
    let stored_done =
        serde_json::json!({"epoch": 42, "report": {"sealed_epoch": 42, "done": true}});
    let fresh = |sealed_epoch, done| Ok(V2DrainReport { sealed_epoch, done });
    for (case, status, probe, terminable) in [
        (
            "no report",
            serde_json::json!({"epoch": 42}),
            fresh(Some(42), true),
            false,
        ),
        (
            "stale epoch",
            serde_json::json!({"epoch": 42, "report": {"sealed_epoch": 41, "done": true}}),
            fresh(Some(42), true),
            false,
        ),
        (
            "not done",
            serde_json::json!({"epoch": 42, "report": {"sealed_epoch": 42, "done": false}}),
            fresh(Some(42), true),
            false,
        ),
        (
            "fresh not done",
            stored_done.clone(),
            fresh(Some(42), false),
            false,
        ),
        (
            "fresh other epoch",
            stored_done.clone(),
            fresh(Some(43), true),
            false,
        ),
        (
            "fresh unsealed",
            stored_done.clone(),
            fresh(None, true),
            false,
        ),
        (
            "node unreachable",
            stored_done.clone(),
            Err("refused".into()),
            false,
        ),
        ("done", stored_done.clone(), fresh(Some(42), true), true),
    ] {
        let mut state = kubernetes_state(&db.pool).await?;
        state.v2_drain_probe = std::sync::Arc::new(ScriptedProbe(probe));
        let node_id = insert_k8s_node(&db.pool, Some("drained"), Some("v2"), Some(status)).await?;
        let sandbox_id = pplx_space_common_core::types::SandboxId::new();
        crate::controllers::substrate::kubernetes::tests::insert_test_sandbox_row(
            &db.pool, sandbox_id, "started",
        )
        .await?;
        sqlx::query(sql!(
            "UPDATE sandboxes SET active_node_id = $2 WHERE id = $1"
        ))
        .bind(sandbox_id)
        .bind(node_id)
        .execute(&db.pool)
        .await?;
        let answer = post_drain(
            &state,
            &actor,
            node_id,
            DrainAction::Terminate,
            DrainRequest::default(),
        )
        .await;
        let phase = v2_columns(&db.pool, node_id).await?.0;
        if terminable {
            assert_eq!(phase.as_deref(), Some("terminating"), "{case}: {answer}");
        } else {
            assert_eq!(answer, StatusCode::CONFLICT, "{case}");
            assert_eq!(phase.as_deref(), Some("drained"), "{case}");
        }
    }
    Ok(())
}

/// A drain of an undrained v2 node whose cancel hasn't reached the node is
/// refused unless it runs under v2, which starts a new epoch; a default
/// `:drain` would inherit the old epoch and its cancel. The new drain takes
/// over the slot an admitted drain's cancel holds, starting admitted, and
/// waits for one when the cancelled drain was still waiting.
#[tokio::test(flavor = "current_thread")]
async fn a_drain_over_a_pending_v2_cancel_runs_under_v2_only() -> TestResult {
    if !enter_direct_test_process() {
        return Ok(());
    }
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let mut state = kubernetes_state(&db.pool).await?;
    state.drain_v2_enabled = true;
    let actor = managing_org_admin(&db.pool).await?;

    for (cancelled, waiting) in [
        (serde_json::json!({"epoch": 42, "cancel": true}), false),
        (
            serde_json::json!({"epoch": 42, "cancel": true, "waiting": false}),
            false,
        ),
        (
            serde_json::json!({"epoch": 42, "cancel": true, "waiting": true}),
            true,
        ),
    ] {
        let node_id = insert_k8s_node(&db.pool, None, Some("v2"), Some(cancelled.clone())).await?;
        let before = v2_columns(&db.pool, node_id).await?;
        let answer = post_drain(
            &state,
            &actor,
            node_id,
            DrainAction::Drain,
            DrainRequest {
                engine: None,
                ..v2_request(NodeDrainMode::Forced)
            },
        )
        .await;
        assert_eq!(answer, StatusCode::CONFLICT);
        assert_eq!(v2_columns(&db.pool, node_id).await?, before, "wrote");

        post_drain(
            &state,
            &actor,
            node_id,
            DrainAction::Drain,
            v2_request(NodeDrainMode::Forced),
        )
        .await;
        let (phase, engine, status, started_us) = v2_columns(&db.pool, node_id).await?;
        assert_eq!(
            (phase.as_deref(), engine.as_deref()),
            (Some("draining"), Some("v2"))
        );
        assert_eq!(
            status,
            started_us.map(|epoch| serde_json::json!({"epoch": epoch, "waiting": waiting})),
            "a new epoch without the old cancel, over {cancelled}"
        );
    }
    Ok(())
}

/// A node's listing carries its drain engine, the drain's status and the
/// dead-node verb's last result, as the columns hold them.
#[tokio::test(flavor = "current_thread")]
async fn a_node_reads_back_its_drain_engine_status_and_dead_node_result() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let status = serde_json::json!({"epoch": 42, "report": {"sealed_epoch": 42, "done": true}});
    let verdict = serde_json::json!({"verdict": "alive"});
    let node_id =
        insert_k8s_node(&db.pool, Some("drained"), Some("v2"), Some(status.clone())).await?;
    sqlx::query(sql!("UPDATE nodes SET dead_node_status = $2 WHERE id = $1"))
        .bind(node_id)
        .bind(&verdict)
        .execute(&db.pool)
        .await?;
    let row = super::AdminNodeStore::new(db.pool.clone())
        .fetch(node_id)
        .await?
        .ok_or("the node reads back")?;
    let response = super::admin_node_response(row, None, None);
    assert_eq!(response.drain_engine.as_deref(), Some("v2"));
    assert_eq!(response.drain_status, Some(status));
    assert_eq!(response.dead_node_status, Some(verdict));
    Ok(())
}

/// A [`V2DrainProbe`] that answers `done` for `epoch` only if it can take
/// the node row's lock at once: the handler must not hold it while it asks.
struct LockCheckingProbe(u64);

impl V2DrainProbe for LockCheckingProbe {
    fn drain_node<'a>(
        &'a self,
        state: &'a AppState,
        node_id: NodeId,
        _epoch: u64,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<V2DrainReport, String>> + Send + 'a>,
    > {
        Box::pin(async move {
            let mut tx = state.pool.begin().await.map_err(|e| e.to_string())?;
            sqlx::query(sql!("SELECT 1 FROM nodes WHERE id = $1 FOR UPDATE NOWAIT"))
                .bind(node_id)
                .execute(&mut *tx)
                .await
                .map_err(|error| format!("the node row is locked: {error}"))?;
            tx.rollback().await.map_err(|e| e.to_string())?;
            Ok(V2DrainReport {
                sealed_epoch: Some(self.0),
                done: true,
            })
        })
    }
}

/// `:terminate` asks a v2 node before it locks the node's row, so the call
/// holds up no write to the row; the node then moves to `terminating`.
#[tokio::test(flavor = "current_thread")]
async fn terminate_asks_the_node_before_it_locks_the_row() -> TestResult {
    if !enter_direct_test_process() {
        return Ok(());
    }
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let actor = managing_org_admin(&db.pool).await?;
    let mut state = kubernetes_state(&db.pool).await?;
    state.v2_drain_probe = std::sync::Arc::new(LockCheckingProbe(42));
    let node_id = insert_k8s_node(
        &db.pool,
        Some("drained"),
        Some("v2"),
        Some(serde_json::json!({"epoch": 42, "report": {"sealed_epoch": 42, "done": true}})),
    )
    .await?;
    let answer = post_drain(
        &state,
        &actor,
        node_id,
        DrainAction::Terminate,
        DrainRequest::default(),
    )
    .await;
    assert_eq!(
        v2_columns(&db.pool, node_id).await?.0.as_deref(),
        Some("terminating"),
        "{answer}"
    );
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
