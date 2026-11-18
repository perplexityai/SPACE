//! The start of a stopped sandbox a v2 drain drained
//! ([`KubernetesSubstrate::hand_off_stopped_at_start`]).
//!
//! The test client answers every Kubernetes request with an empty body, so
//! a re-placement this start drives fails at its pod create, after the
//! claim: the row is left claimed, `restoring` with v1's `stopped` marker
//! and no lease, for the next start to resume.

use std::time::Duration;

use crate::metrics::sandbox::api::SandboxApiMetricIdentity;
use pplx_space_accounts::NodeId;
use pplx_space_sandbox_model::{SandboxId, SandboxLifecycle};
use pplx_space_sql_locator::sql;
use uuid::Uuid;

use super::*;
use crate::controllers::substrate::{ActivationOptions, StoppedHandoffAtStart};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

const NODE_NAME: &str = "worker-drained.synthetic.internal";

fn options() -> ActivationOptions {
    ActivationOptions {
        blocking: true,
        ready_timeout: Duration::from_secs(2),
    }
}

fn substrate(pool: &sqlx::PgPool, switch: StoppedHandoffAtStart) -> KubernetesSubstrate {
    let mut substrate = kubernetes_tests::substrate_with_store(pool.clone());
    substrate.k8s.stopped_handoff_at_start = switch;
    substrate
}

/// A worker node named [`NODE_NAME`] under drain `engine` in `phase`.
async fn node(
    pool: &sqlx::PgPool,
    engine: Option<&str>,
    phase: Option<&str>,
) -> Result<NodeId, BoxError> {
    let node_id = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state, drain_engine, drain_phase)
         VALUES ($1, '10.0.0.1'::INET, 'worker', 'k8s', 'registered', $2, $3)"
    ))
    .bind(node_id)
    .bind(engine)
    .bind(phase)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name) VALUES ($1, $2)"
    ))
    .bind(node_id)
    .bind(NODE_NAME)
    .execute(pool)
    .await?;
    Ok(node_id)
}

/// A sandbox stopped an hour ago at home on `node_id`, its pod's execution
/// there created before that, and a ready, stored disk of this stop.
async fn stopped_resident(pool: &sqlx::PgPool, node_id: NodeId) -> Result<SandboxId, BoxError> {
    let sandbox_id = SandboxId::new();
    kubernetes_tests::insert_test_sandbox_row(pool, sandbox_id, "stopped").await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET active_node_id = $2, lifecycle_changed_at = NOW() - INTERVAL '1 hour'
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions (
             sandbox_id, generation, pod_name, pod_namespace, k8s_node_name, state, active,
             created_at
         )
         VALUES ($1, 1, $2, 'space', $3, 'running', true, NOW() - INTERVAL '2 hours')"
    ))
    .bind(sandbox_id)
    .bind(format!("pod-{sandbox_id}-g1"))
    .bind(NODE_NAME)
    .execute(pool)
    .await?;
    let snapshot_id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO snapshots (
             id, sandbox_id, type, state, timestamp, local_artifact_id, artifact_kind,
             artifact_origin
         )
         VALUES ($1, $2, 'disk', 'ready', NOW(), $3, 'disk', 'stop')"
    ))
    .bind(snapshot_id)
    .bind(sandbox_id)
    .bind(Uuid::now_v7())
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string)
         VALUES ($1, $2)"
    ))
    .bind(snapshot_id)
    .bind(format!("s3://test-bucket/{snapshot_id}"))
    .execute(pool)
    .await?;
    Ok(sandbox_id)
}

/// The whole row, and its executions, as JSON text.
async fn image(pool: &sqlx::PgPool, sandbox_id: SandboxId) -> Result<String, BoxError> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT (SELECT to_jsonb(s) FROM sandboxes s WHERE s.id = $1)::TEXT
                || (SELECT COALESCE(jsonb_agg(to_jsonb(e) ORDER BY e.generation), '[]'::jsonb)
                      FROM sandbox_k8s_executions e WHERE e.sandbox_id = $1)::TEXT"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?)
}

/// `(lifecycle, marker, active_node_id, lease token, lease live)`. The start
/// claims with v1's fenced claim, which writes no lease, so the last two stay
/// `None` and `false`.
type Claim = (String, Option<String>, Option<NodeId>, Option<Uuid>, bool);

async fn claim_state(pool: &sqlx::PgPool, sandbox_id: SandboxId) -> Result<Claim, BoxError> {
    Ok(sqlx::query_as(sql!(
        "SELECT lifecycle, evacuation_restore_target, active_node_id, restore_lease_token,
                COALESCE(restore_lease_expires_at > NOW(), FALSE)
           FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?)
}

async fn start(
    substrate: &KubernetesSubstrate,
    sandbox_id: SandboxId,
) -> Result<(Sandbox, ActivationOptions), SubstrateError> {
    let sandbox = substrate.store.require_sandbox(sandbox_id).await?;
    substrate
        .hand_off_stopped_at_start(
            SandboxApiMetricIdentity::native("start"),
            sandbox,
            options(),
        )
        .await
}

fn is_retryable_503(error: &SubstrateError) -> bool {
    matches!(
        crate::controllers::ControllerError::from(error.clone()).status(),
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    )
}

/// The evidence rule: a stopped row at home on a drained or terminating v2
/// node with its disk durable since the stop is handed off, claimed and
/// re-placed. Its re-placement fails here after the claim, so the start
/// answers a retryable 503: the row is left handed off and `restoring`, with
/// no lease, for the next start.
#[tokio::test(flavor = "current_thread")]
async fn the_evidence_rule_hands_off_and_claims_a_stopped_row_on_a_drained_v2_node()
-> Result<(), BoxError> {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let pool = &db.pool;
    let substrate = substrate(pool, StoppedHandoffAtStart::Enabled);
    for phase in ["drained", "terminating"] {
        let node_id = node(pool, Some("v2"), Some(phase)).await?;
        let sandbox_id = stopped_resident(pool, node_id).await?;

        let error = start(&substrate, sandbox_id)
            .await
            .expect_err("the re-placement fails at its pod create");

        assert!(
            matches!(error, SubstrateError::RestoreInFlight { .. }),
            "{phase}: {error:?}"
        );
        assert!(is_retryable_503(&error), "{phase}: {error:?}");
        let (lifecycle, marker, active, token, live) = claim_state(pool, sandbox_id).await?;
        assert_eq!(lifecycle, "restoring", "{phase}: claimed");
        assert_eq!(marker.as_deref(), Some("stopped"), "{phase}: handed off");
        assert_eq!(active, None, "{phase}: unplaced");
        assert_eq!((token, live), (None, false), "{phase}: no lease");
        let home_execution_active: bool = sqlx::query_scalar(sql!(
            "SELECT active FROM sandbox_k8s_executions WHERE sandbox_id = $1 AND generation = 1"
        ))
        .bind(sandbox_id)
        .fetch_one(pool)
        .await?;
        assert!(
            !home_execution_active,
            "{phase}: the home execution is retired"
        );
    }
    Ok(())
}

/// Without the evidence the start takes main's path, reading one statement
/// and writing nothing: a stopped row on a node with no drain, a v1 drain,
/// a v2 drain still draining or being cancelled, and one on a drained v2
/// node whose disk isn't durable.
#[tokio::test(flavor = "current_thread")]
async fn without_the_evidence_the_start_takes_mains_path() -> Result<(), BoxError> {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let pool = &db.pool;
    let substrate = substrate(pool, StoppedHandoffAtStart::Enabled);
    for (case, engine, phase, durable) in [
        ("no drain", None, None, true),
        ("a v1 drain", None, Some("drained"), true),
        ("a draining v2 drain", Some("v2"), Some("draining"), true),
        ("a v2 cancel pending", Some("v2"), None, true),
        (
            "a disk that isn't durable",
            Some("v2"),
            Some("drained"),
            false,
        ),
    ] {
        let node_id = node(pool, engine, phase).await?;
        let sandbox_id = stopped_resident(pool, node_id).await?;
        if !durable {
            sqlx::query(sql!(
                "UPDATE snapshots SET state = 'creating' WHERE sandbox_id = $1"
            ))
            .bind(sandbox_id)
            .execute(pool)
            .await?;
        }
        let before = image(pool, sandbox_id).await?;

        let (sandbox, options) = start(&substrate, sandbox_id).await?;

        assert_eq!(sandbox.active_node_id, Some(node_id), "{case}");
        assert_eq!(
            options.ready_timeout,
            self::options().ready_timeout,
            "{case}"
        );
        assert_eq!(image(pool, sandbox_id).await?, before, "{case}: wrote");
    }
    Ok(())
}

/// A handed-off row, marked and unplaced, is resumed by a start with the
/// switch on: claimed and its re-placement driven. With it off, the start
/// takes main's path, writing and reading nothing, and main's start refuses
/// it until v1's sweep re-places it.
#[tokio::test(flavor = "current_thread")]
async fn a_handed_off_row_is_resumed_only_with_the_switch_on() -> Result<(), BoxError> {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let pool = &db.pool;
    let node_id = node(pool, Some("v2"), Some("drained")).await?;
    let sandbox_id = stopped_resident(pool, node_id).await?;
    let handed_off = substrate(pool, StoppedHandoffAtStart::Enabled)
        .store
        .hand_off_stopped(sandbox_id, node_id)
        .await?;
    assert!(matches!(
        handed_off,
        pplx_space_sandbox_store::drain_restore::StoppedHandoff::HandedOff(_)
    ));
    let before = image(pool, sandbox_id).await?;

    let off = substrate(pool, StoppedHandoffAtStart::Disabled);
    let (sandbox, _) = start(&off, sandbox_id).await?;
    assert_eq!(sandbox.lifecycle, "stopped");
    assert_eq!(image(pool, sandbox_id).await?, before, "switch off: wrote");
    // What main's start plan refuses (`stopped_restore_plan`'s retryable
    // 503) until v1's sweep re-places the row.
    let source = off.store.restore_source_plan(sandbox_id).await?;
    assert!(source.stopped_evacuation_unplaced);

    let on = substrate(pool, StoppedHandoffAtStart::Enabled);
    let error = start(&on, sandbox_id)
        .await
        .expect_err("re-placement fails");
    assert!(is_retryable_503(&error), "{error:?}");
    let (lifecycle, _, _, token, _) = claim_state(pool, sandbox_id).await?;
    assert_eq!((lifecycle.as_str(), token), ("restoring", None), "resumed");
    Ok(())
}

/// An API that restarts between the handoff and the claim leaves the row
/// handed off and unclaimed: a start on a fresh process resumes it, taking
/// the claim the dead one never took.
#[tokio::test(flavor = "current_thread")]
async fn a_restart_between_the_handoff_and_the_claim_is_resumed() -> Result<(), BoxError> {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let pool = &db.pool;
    let node_id = node(pool, Some("v2"), Some("drained")).await?;
    let sandbox_id = stopped_resident(pool, node_id).await?;
    // The first process hands off, then dies before its claim.
    {
        let dead = substrate(pool, StoppedHandoffAtStart::Enabled);
        let handed_off = dead.store.hand_off_stopped(sandbox_id, node_id).await?;
        assert!(matches!(
            handed_off,
            pplx_space_sandbox_store::drain_restore::StoppedHandoff::HandedOff(_)
        ));
    }
    assert_eq!(
        claim_state(pool, sandbox_id).await?,
        (
            "stopped".to_owned(),
            Some("stopped".to_owned()),
            None,
            None,
            false
        )
    );

    let restarted = substrate(pool, StoppedHandoffAtStart::Enabled);
    let error = start(&restarted, sandbox_id)
        .await
        .expect_err("re-placement fails");
    assert!(is_retryable_503(&error), "{error:?}");
    let (lifecycle, marker, _, token, _) = claim_state(pool, sandbox_id).await?;
    assert_eq!(lifecycle, "restoring", "the restarted start claimed it");
    assert_eq!(marker.as_deref(), Some("stopped"));
    assert_eq!(token, None, "no lease");
    Ok(())
}

/// A failure after the claim answers a retryable 503, never a 409, and
/// leaves the row claimed with no lease; the next start resumes it: v1's
/// fenced claim takes the `restoring` marked row again (it writes the row)
/// and drives the re-placement once more.
#[tokio::test(flavor = "current_thread")]
async fn a_failure_after_the_claim_answers_503_and_the_next_start_resumes() -> Result<(), BoxError>
{
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let pool = &db.pool;
    let substrate = substrate(pool, StoppedHandoffAtStart::Enabled);
    let node_id = node(pool, Some("v2"), Some("drained")).await?;
    let sandbox_id = stopped_resident(pool, node_id).await?;

    let first = start(&substrate, sandbox_id).await.expect_err("fails");
    assert!(
        matches!(first, SubstrateError::RestoreInFlight { .. }),
        "{first:?}"
    );
    assert!(is_retryable_503(&first));
    assert_eq!(
        claim_state(pool, sandbox_id).await?,
        (
            "restoring".to_owned(),
            Some("stopped".to_owned()),
            None,
            None,
            false
        ),
        "claimed, with no lease"
    );
    let claimed = xmin(pool, sandbox_id).await?;

    let second = start(&substrate, sandbox_id)
        .await
        .expect_err("fails again");
    assert!(
        matches!(second, SubstrateError::RestoreInFlight { .. }),
        "{second:?}"
    );
    assert!(is_retryable_503(&second));
    assert_ne!(
        xmin(pool, sandbox_id).await?,
        claimed,
        "the next start claimed the row again"
    );
    assert_eq!(
        claim_state(pool, sandbox_id).await?,
        (
            "restoring".to_owned(),
            Some("stopped".to_owned()),
            None,
            None,
            false
        ),
        "still claimed, with no lease, for the next start"
    );
    Ok(())
}

/// The row's version: changes iff Postgres wrote the row.
async fn xmin(pool: &sqlx::PgPool, sandbox_id: SandboxId) -> Result<String, BoxError> {
    Ok(
        sqlx::query_scalar(sql!("SELECT xmin::TEXT FROM sandboxes WHERE id = $1"))
            .bind(sandbox_id)
            .fetch_one(pool)
            .await?,
    )
}

/// Switch off, the start's path sends exactly main's statements: the
/// handoff hook sends none, for a stopped row at home, a handed-off one or a
/// restoring one. On, with no v2 row, it sends its one read.
#[tokio::test(flavor = "current_thread")]
async fn switch_off_the_start_sends_no_statement_of_its_own() -> Result<(), BoxError> {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let pool = &db.pool;
    let node_id = node(pool, None, None).await?;
    let at_home = stopped_resident(pool, node_id).await?;
    let marked = stopped_resident(pool, node_id).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET evacuation_restore_target = 'stopped', active_node_id = NULL
          WHERE id = $1"
    ))
    .bind(marked)
    .execute(pool)
    .await?;
    let restoring = stopped_resident(pool, node_id).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'restoring' WHERE id = $1"
    ))
    .bind(restoring)
    .execute(pool)
    .await?;
    for (switch, expected) in [
        (StoppedHandoffAtStart::Disabled, 0),
        (StoppedHandoffAtStart::Enabled, 1),
    ] {
        let substrate = substrate(pool, switch);
        for sandbox_id in [at_home, restoring] {
            let sandbox = substrate.store.require_sandbox(sandbox_id).await?;
            let substrate = &substrate;
            let sent = statements(|| async move {
                substrate
                    .hand_off_stopped_at_start(
                        SandboxApiMetricIdentity::native("start"),
                        sandbox,
                        options(),
                    )
                    .await
                    .expect("no handoff");
            })
            .await?;
            assert_eq!(sent.len(), expected, "{switch:?}: {sent:#?}");
        }
        if switch == StoppedHandoffAtStart::Disabled {
            let sandbox = substrate.store.require_sandbox(marked).await?;
            let substrate = &substrate;
            let sent = statements(|| async move {
                substrate
                    .hand_off_stopped_at_start(
                        SandboxApiMetricIdentity::native("start"),
                        sandbox,
                        options(),
                    )
                    .await
                    .expect("main's path");
            })
            .await?;
            assert!(sent.is_empty(), "{sent:#?}");
        }
    }
    Ok(())
}

/// Keep `sqlx::query` events enabled for the whole test process, so a
/// capture sees every statement whichever test set the interest cache.
fn keep_query_events_enabled() {
    static ALWAYS_ENABLED: std::sync::OnceLock<tracing::Dispatch> = std::sync::OnceLock::new();
    ALWAYS_ENABLED.get_or_init(|| tracing::Dispatch::new(tracing_subscriber::registry()));
    tracing::callsite::rebuild_interest_cache();
}

/// Every statement `run` sends, as the text sqlx logs.
async fn statements<F, Fut>(run: F) -> Result<Vec<String>, BoxError>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    use pplx_space_observability::test_capture::{CapturedEventsLayer, field_value};
    use tracing_subscriber::layer::SubscriberExt;

    let layer = CapturedEventsLayer::new().with_target("sqlx::query");
    let events = layer.events();
    {
        let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));
        keep_query_events_enabled();
        run().await;
    }
    Ok(events
        .events()
        .iter()
        .map(|event| {
            let statement = field_value(event, "db.statement").unwrap_or_default();
            if statement.is_empty() {
                field_value(event, "summary").unwrap_or_default().to_owned()
            } else {
                statement.to_owned()
            }
        })
        .collect())
}

/// Insert execution `generation` of `sandbox_id` as its active one, as a
/// claim's drive does, retiring the one it replaces.
async fn drive_execution(
    pool: &sqlx::PgPool,
    sandbox_id: SandboxId,
    generation: i64,
) -> Result<(), BoxError> {
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET active = FALSE, state = 'replaced', terminated_at = NOW()
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions (
             sandbox_id, generation, pod_name, pod_namespace, k8s_node_name, state, active
         )
         VALUES ($1, $2, $3, 'space', 'worker-target.synthetic.internal', 'running', true)"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .bind(format!("pod-{sandbox_id}-g{generation}"))
    .execute(pool)
    .await?;
    Ok(())
}

/// Two starts race on one handed-off row: v1's claim writes no lease, so
/// both claim it, the earlier one driving execution 2 and the later one
/// replacing it with execution 3. Whichever lands first, exactly one
/// finalizes: the later claim's. The earlier start answers a retryable 503
/// and leaves the row as it found it.
#[tokio::test(flavor = "current_thread")]
async fn of_two_racing_starts_only_the_later_claim_lands_the_row() -> Result<(), BoxError> {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let pool = &db.pool;
    let substrate = substrate(pool, StoppedHandoffAtStart::Enabled);
    for earlier_lands_first in [true, false] {
        let node_id = node(pool, Some("v2"), Some("drained")).await?;
        let sandbox_id = stopped_resident(pool, node_id).await?;
        assert!(matches!(
            substrate
                .store
                .hand_off_stopped(sandbox_id, node_id)
                .await?,
            pplx_space_sandbox_store::drain_restore::StoppedHandoff::HandedOff(_)
        ));

        substrate
            .store
            .begin_evacuation_restore_lifecycle(sandbox_id, SandboxLifecycle::Stopped)
            .await?;
        drive_execution(pool, sandbox_id, 2).await?;
        substrate
            .store
            .begin_evacuation_restore_lifecycle(sandbox_id, SandboxLifecycle::Stopped)
            .await?;
        drive_execution(pool, sandbox_id, 3).await?;

        let case = if earlier_lands_first {
            "earlier first"
        } else {
            "later first"
        };
        if !earlier_lands_first {
            substrate.land_re_placed_stopped(sandbox_id, 3).await?;
        }
        let before = image(pool, sandbox_id).await?;
        let stale = substrate
            .land_re_placed_stopped(sandbox_id, 2)
            .await
            .expect_err("the earlier claim's landing is fenced");
        assert!(
            matches!(stale, SubstrateError::RestoreInFlight { .. }),
            "{case}: {stale:?}"
        );
        assert!(is_retryable_503(&stale), "{case}: {stale:?}");
        assert_eq!(image(pool, sandbox_id).await?, before, "{case}: unchanged");
        if earlier_lands_first {
            substrate.land_re_placed_stopped(sandbox_id, 3).await?;
        }

        let (lifecycle, marker, ..) = claim_state(pool, sandbox_id).await?;
        assert_eq!(
            (lifecycle.as_str(), marker),
            ("stopped", None),
            "{case}: the later claim landed it"
        );
        let placement: String =
            sqlx::query_scalar(sql!("SELECT placement FROM sandboxes WHERE id = $1"))
                .bind(sandbox_id)
                .fetch_one(pool)
                .await?;
        assert_eq!(placement, "on_node", "{case}");
    }
    Ok(())
}

/// v1's finalize, which v1's controller calls, sends main's statement,
/// byte for byte after its locator, and the start's landing never sends it.
#[tokio::test(flavor = "current_thread")]
async fn v1s_finalize_sends_mains_statement_and_the_start_never_does() -> Result<(), BoxError> {
    const MAINS_FINALIZE: &str = "UPDATE sandboxes
                SET lifecycle = $2,
                    placement = 'on_node',
                    evacuation_restore_target = NULL
              WHERE id = $1 AND lifecycle = 'restoring'";
    const LOCATOR: &str = "/*pplx_space_sandbox_store::drain:";
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let pool = &db.pool;
    let substrate = substrate(pool, StoppedHandoffAtStart::Enabled);
    let node_id = node(pool, Some("v2"), Some("drained")).await?;
    let sandbox_id = stopped_resident(pool, node_id).await?;
    substrate
        .store
        .hand_off_stopped(sandbox_id, node_id)
        .await?;
    substrate
        .store
        .begin_evacuation_restore_lifecycle(sandbox_id, SandboxLifecycle::Stopped)
        .await?;
    drive_execution(pool, sandbox_id, 2).await?;

    let substrate = &substrate;
    let landing = statements(|| async move {
        substrate
            .land_re_placed_stopped(sandbox_id, 2)
            .await
            .expect("lands");
    })
    .await?;
    assert!(
        landing
            .iter()
            .all(|statement| !statement.contains(MAINS_FINALIZE)
                && !statement.trim().starts_with(LOCATOR)),
        "{landing:#?}"
    );

    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'restoring' WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(pool)
    .await?;
    let v1 = statements(|| async move {
        assert!(
            pplx_space_sandbox_store::drain::finalize_evacuation_restore(
                pool,
                sandbox_id,
                SandboxLifecycle::Stopped,
            )
            .await
            .expect("v1's finalize")
        );
    })
    .await?;
    let [statement] = v1.as_slice() else {
        panic!("one statement: {v1:#?}");
    };
    let (locator, text) = statement
        .trim()
        .split_once("*/ ")
        .expect("the statement leads with its locator");
    assert!(format!("{locator}*/").starts_with(LOCATOR), "{statement}");
    assert_eq!(text.trim(), MAINS_FINALIZE);
    Ok(())
}
