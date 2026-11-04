//! End-to-end coverage for the PATCH update path: an `UpdateSandboxRequest`
//! through `SandboxController::update`, into the sandbox store's
//! `update_desired_state`, and back out through the api's read path.
//!
//! Scope note: only the `metadata` branch is reachable here. The resources
//! branch calls `control_runtime_client` and drives the guest before it
//! persists, so it needs a live runtime or a substrate mock — its column
//! behaviour is covered at the store verb instead, and its vCPU allocation
//! across a cold restart in the `kubernetes` submodule, against a simulated
//! worker. `auto_lifecycle` and `network_policy` are untouched by this
//! migration and keep their own SQL.

use pplx_space_common_core::types::SandboxId;
use pplx_space_common_core::types::{OrgId, UserId};

use super::*;
use crate::models::{SandboxMetadata, SandboxMetadataValue, User};
use pplx_space_db_test_fixtures::pg_test_db;
use pplx_space_feature_flags::{FeatureFlagClient, SPACE_API_TIMEOUT_LOCK_BYPASS_FLAG};
use pplx_space_test_env::TestResult;

async fn insert_actor(pool: &PgPool) -> TestResult<User> {
    let org_id = OrgId::new();
    let user_id = UserId::new();
    sqlx::query(sql!(
        "INSERT INTO organizations (id, name, slug)
         VALUES ($1, 'Sandbox Update Test', $2)"
    ))
    .bind(org_id)
    .bind(format!("sandbox-update-{org_id}"))
    .execute(pool)
    .await?;
    Ok(sqlx::query_as::<_, User>(sql!(
        "INSERT INTO users (id, email, password_hash, display_name, org_id, role)
         VALUES ($1, $2, 'unused', 'Sandbox Update Test', $3, 'member')
         RETURNING id, email, password_hash, display_name, org_id, role, owner_source,
                   created_at, updated_at"
    ))
    .bind(user_id)
    .bind(format!("sandbox-update-{user_id}@example.com"))
    .bind(org_id)
    .fetch_one(pool)
    .await?)
}

/// Seed a sandbox and return it as the controller sees it.
async fn seed_sandbox(pool: &PgPool, actor: &User) -> TestResult<Sandbox> {
    let id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes
             (id, name, user_id, org_id, managed_by, lifecycle, placement,
              vcpu, memory_mib, network_policy_generation)
         VALUES ($1, 'update-path', $2, $3, 'k8s', 'started', 'unplaced', 2, 512, 1)"
    ))
    .bind(id)
    .bind(actor.id)
    .bind(actor.org_id)
    .execute(pool)
    .await?;
    Ok(fetch_sandbox(pool, id)
        .await?
        .expect("seeded sandbox must be readable"))
}

fn metadata_request(metadata: Option<SandboxMetadata>) -> UpdateSandboxRequest {
    UpdateSandboxRequest {
        metadata,
        vcpu: None,
        memory_mib: None,
        pids_max: None,
        auto_lifecycle: None,
        network_policy: None,
    }
}

#[cfg(feature = "kubernetes")]
mod kubernetes;

// Runtime tests seed their Direct route and simulated worker explicitly.
fn direct_sandbox_controller(pool: &PgPool) -> SandboxController {
    SandboxController::new(crate::controllers::substrate::Substrate::from_direct(
        crate::controllers::substrate::direct::DirectSubstrate::new(pool.clone()),
    ))
}

/// Seeds a sandbox in `lifecycle` with `policy` stored. With a worker, the row
/// is a placed Direct sandbox with route identity and execution generation 7,
/// running on that simulated worker. Without one, it is `seed_sandbox`'s
/// evidence-free unplaced row on `managed_by`.
async fn seed_timeout_sandbox(
    pool: &PgPool,
    actor: &User,
    lifecycle: &str,
    policy: &AutoLifecyclePolicy,
    managed_by: pplx_space_common_core::types::SubstrateKind,
    running_on: Option<(
        pplx_space_common_core::types::NodeId,
        &pplx_space_node_api_sim::SimWorkerHarness,
    )>,
) -> TestResult<Sandbox> {
    let sandbox = seed_sandbox(pool, actor).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = $2, auto_lifecycle_json = $3,
            active_node_id = $4, placement = $5, managed_by = $6 WHERE id = $1"
    ))
    .bind(sandbox.id)
    .bind(lifecycle)
    .bind(sqlx::types::Json(policy))
    .bind(running_on.map(|(node, _)| node))
    .bind(if running_on.is_some() {
        "on_node"
    } else {
        "unplaced"
    })
    .bind(managed_by)
    .execute(pool)
    .await?;
    if let Some((node, worker)) = running_on {
        sqlx::query(sql!(
            "INSERT INTO sandbox_direct_route_identities
             (sandbox_id, route_generation, current_node_id, substrate_kind,
              execution_generation, activation_attempt_id)
             VALUES ($1, 7, $2, 'space', 7, $3)"
        ))
        .bind(sandbox.id)
        .bind(node)
        .bind(uuid::Uuid::now_v7())
        .execute(pool)
        .await?;
        let id = sandbox.id.to_string();
        worker.state().ensure_default_running(&id).await?;
        worker
            .state()
            .set_route_identity_provenance(&id, Some(7), Some(7), None)
            .await?;
        worker
            .state()
            .apply_access_deadline(&id, Some(1_900_000_000), None)
            .await?;
    }
    Ok(sandbox)
}

/// Runs one timeout update while another transaction holds the sandbox row
/// lock. Returns the result, whether the call waited on that lock, and the
/// lock decision the call recorded.
async fn timeout_update_under_row_lock(
    pool: &PgPool,
    controller: &SandboxController,
    sandbox: &Sandbox,
    timeout_secs: u32,
    expected: &AutoLifecyclePolicy,
    budget: Duration,
) -> TestResult<(
    Result<AccessIdleTimeoutOutcome, ControllerError>,
    bool,
    Vec<String>,
)> {
    let mut holder = pool.begin().await?;
    sqlx::query(sql!("SELECT id FROM sandboxes WHERE id = $1 FOR UPDATE"))
        .bind(sandbox.id)
        .execute(&mut *holder)
        .await?;
    let holder_pid: i32 = sqlx::query_scalar(sql!("SELECT pg_backend_pid()"))
        .fetch_one(&mut *holder)
        .await?;
    let mut observer = pool.acquire().await?;
    let capture = pplx_space_metrics::test_capture::capture_counters_guard();
    let deadline = tokio::time::Instant::now() + budget;
    let (result, waited) = tokio::join!(
        controller.set_access_idle_timeout(
            pool,
            sandbox,
            timeout_secs,
            prost_types::Timestamp {
                seconds: 1_800_000_000,
                nanos: 0,
            },
            deadline,
            Some(expected),
        ),
        async {
            while tokio::time::Instant::now() < deadline {
                let waiting: bool = sqlx::query_scalar(sql!(
                    "SELECT EXISTS (SELECT 1 FROM pg_stat_activity
                     WHERE wait_event_type = 'Lock'
                       AND $1 = ANY(pg_blocking_pids(pid)))"
                ))
                .bind(holder_pid)
                .fetch_one(&mut *observer)
                .await?;
                if waiting {
                    return TestResult::Ok(true);
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Ok(false)
        },
    );
    let decisions = capture
        .finish()
        .into_iter()
        .filter(|counter| counter.name == "sandbox.api.timeout_update.lock_decision")
        .flat_map(|counter| counter.tags)
        .collect();
    // Keep the lock until after the controller finishes, even on failure.
    holder.rollback().await?;
    Ok((result, waited?, decisions))
}

#[tokio::test(flavor = "current_thread")]
async fn unchanged_timeout_does_not_wait_for_an_unrelated_row_lock() -> TestResult {
    use pplx_space_common_core::types::SubstrateKind;
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    // The lock holder, the lock-wait observer and the update each need a
    // connection.
    let db = pplx_space_db_test_fixtures::pg_test_db_with_max_connections(3).await?;
    let actor = insert_actor(&db.pool).await?;
    let enabled =
        FeatureFlagClient::fixed_bools([(SPACE_API_TIMEOUT_LOCK_BYPASS_FLAG.key(), true)]);
    let disabled =
        FeatureFlagClient::fixed_bools([(SPACE_API_TIMEOUT_LOCK_BYPASS_FLAG.key(), false)]);
    let absent = FeatureFlagClient::disabled();
    let controller = direct_sandbox_controller(&db.pool).with_feature_flags(enabled.clone());
    let policy = crate::e2b::auto_lifecycle_policy(90, true, false);
    let node = pplx_space_common_core::types::NodeId::new();
    let worker = pplx_space_node_api_sim::SimWorkerHarness::builder(node.to_string())
        .with_internal_api()
        .start()
        .await?;
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '127.0.0.1', 'worker', $2, 'registered')"
    ))
    .bind(node)
    .bind(SubstrateKind::Space)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_direct (node_id, node_api_endpoint) VALUES ($1, $2)"
    ))
    .bind(node)
    .bind(worker.endpoint())
    .execute(&db.pool)
    .await?;

    for (lifecycle, decision) in [
        ("created", "skipped_absent"),
        ("paused", "skipped_absent"),
        ("suspended", "skipped_absent"),
        ("started", "skipped_present"),
    ] {
        let started = lifecycle == "started";
        let sandbox = seed_timeout_sandbox(
            &db.pool,
            &actor,
            lifecycle,
            &policy,
            SubstrateKind::Space,
            started.then_some((node, &worker)),
        )
        .await?;
        let before: serde_json::Value =
            sqlx::query_scalar(sql!("SELECT to_jsonb(s) FROM sandboxes s WHERE id = $1"))
                .bind(sandbox.id)
                .fetch_one(&db.pool)
                .await?;
        // Runtime requests encode whole seconds; keep their budget above one
        // second while the stored-only lock check uses 500ms.
        let budget = if started {
            Duration::from_secs(2)
        } else {
            Duration::from_millis(500)
        };
        let (result, waited, decisions) =
            timeout_update_under_row_lock(&db.pool, &controller, &sandbox, 90, &policy, budget)
                .await?;
        assert!(
            result.is_ok() && !waited,
            "{lifecycle}: unchanged timeout must not wait for the row lock: {result:?}"
        );
        assert_eq!(decisions, [format!("decision:{decision}")], "{lifecycle}");
        assert_eq!(
            matches!(result?, AccessIdleTimeoutOutcome::Applied),
            started
        );
        let after: serde_json::Value =
            sqlx::query_scalar(sql!("SELECT to_jsonb(s) FROM sandboxes s WHERE id = $1"))
                .bind(sandbox.id)
                .fetch_one(&db.pool)
                .await?;
        assert_eq!(after, before);
        if started {
            let record = worker.state().get(&sandbox.id.to_string()).await?;
            assert_eq!(
                record.access_extended_until, None,
                "ResetPolicy clears the floor"
            );
        }
    }
    // Ineligible requests retain locking even with unchanged intent. Gate
    // rollback and missing configuration must also restore that behavior.
    // `Running` rows carry execution evidence; the others carry none, and
    // Direct proves nothing from missing evidence.
    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Seed {
        K8s,
        Direct,
        Running,
    }
    #[derive(Clone, Copy, Debug)]
    enum Call {
        Unchanged,
        FlagOff,
        FlagAbsent,
        Changed,
        Conflicting,
    }
    let conflicting = crate::e2b::auto_lifecycle_policy(120, true, false);
    for (lifecycle, seed, call, decision) in [
        ("created", Seed::K8s, Call::FlagOff, "locked_flag_off"),
        ("created", Seed::K8s, Call::FlagAbsent, "locked_flag_off"),
        ("created", Seed::K8s, Call::Changed, "locked_policy_changed"),
        (
            "created",
            Seed::K8s,
            Call::Conflicting,
            "locked_expected_mismatch",
        ),
        (
            "started",
            Seed::K8s,
            Call::Unchanged,
            "locked_presence_unknown",
        ),
        (
            "started",
            Seed::Direct,
            Call::Unchanged,
            "locked_presence_unknown",
        ),
        (
            "started",
            Seed::Running,
            Call::Changed,
            "locked_policy_changed",
        ),
        (
            "started",
            Seed::Running,
            Call::Conflicting,
            "locked_expected_mismatch",
        ),
        (
            "restoring",
            Seed::Running,
            Call::Unchanged,
            "locked_presence_unknown",
        ),
        (
            "creating",
            Seed::K8s,
            Call::Unchanged,
            "locked_presence_unknown",
        ),
        (
            "starting",
            Seed::K8s,
            Call::Unchanged,
            "locked_presence_unknown",
        ),
        (
            "restoring",
            Seed::K8s,
            Call::Unchanged,
            "locked_presence_unknown",
        ),
        (
            "resuming",
            Seed::K8s,
            Call::Unchanged,
            "locked_presence_unknown",
        ),
        (
            "pausing",
            Seed::K8s,
            Call::Unchanged,
            "locked_presence_unknown",
        ),
        (
            "suspending",
            Seed::K8s,
            Call::Unchanged,
            "locked_presence_unknown",
        ),
        (
            "checkpointing",
            Seed::K8s,
            Call::Unchanged,
            "locked_presence_unknown",
        ),
        (
            "stopping",
            Seed::K8s,
            Call::Unchanged,
            "locked_presence_unknown",
        ),
        (
            "stopped",
            Seed::K8s,
            Call::Unchanged,
            "locked_presence_unknown",
        ),
        (
            "deleting",
            Seed::K8s,
            Call::Unchanged,
            "locked_presence_unknown",
        ),
        (
            "quarantined",
            Seed::K8s,
            Call::Unchanged,
            "locked_presence_unknown",
        ),
        (
            "failed",
            Seed::K8s,
            Call::Unchanged,
            "locked_presence_unknown",
        ),
        (
            "deleted",
            Seed::K8s,
            Call::Unchanged,
            "locked_presence_unknown",
        ),
    ] {
        let case = format!("{lifecycle} {seed:?} {call:?}");
        let flags = match call {
            Call::FlagOff => &disabled,
            Call::FlagAbsent => &absent,
            Call::Unchanged | Call::Changed | Call::Conflicting => &enabled,
        };
        let (timeout, expected) = match call {
            Call::Changed => (120, &policy),
            Call::Conflicting => (90, &conflicting),
            Call::Unchanged | Call::FlagOff | Call::FlagAbsent => (90, &policy),
        };
        let controller = controller.clone().with_feature_flags(flags.clone());
        let sandbox = seed_timeout_sandbox(
            &db.pool,
            &actor,
            lifecycle,
            &policy,
            if seed == Seed::K8s {
                SubstrateKind::Kubernetes
            } else {
                SubstrateKind::Space
            },
            (seed == Seed::Running).then_some((node, &worker)),
        )
        .await?;
        let ownership = pplx_space_sandbox_store::auto_lifecycle_execution(&db.pool, sandbox.id)
            .await?
            .ok_or("seeded sandbox missing")?;
        assert_eq!(ownership.proves_no_execution(), seed == Seed::K8s, "{case}");
        let (result, waited, decisions) = timeout_update_under_row_lock(
            &db.pool,
            &controller,
            &sandbox,
            timeout,
            expected,
            Duration::from_millis(200),
        )
        .await?;
        let error = match result {
            Ok(outcome) => {
                return Err(format!(
                    "{case}: ineligible timeout must still wait for the row lock, got {outcome:?}"
                )
                .into());
            }
            Err(error) => error,
        };
        assert!(waited, "{case}: never waited on the row lock: {error}");
        assert_eq!(error.status().as_u16(), 503, "{case}");
        assert!(
            error
                .to_string()
                .contains("sandbox deadline update expired"),
            "{case}: {error}"
        );
        assert_eq!(decisions, [format!("decision:{decision}")], "{case}");
        if seed == Seed::Running {
            let record = worker.state().get(&sandbox.id.to_string()).await?;
            assert_eq!(
                record.access_extended_until,
                Some(1_900_000_000),
                "{case}: the worker must not be reached"
            );
        }
    }
    Ok(())
}

/// The whole path: request in, row written, and the value visible through
/// the api's own read. Every metadata value variant is exercised, because
/// the document is an untagged enum and this is the layer where a number or
/// boolean would silently become a string.
#[tokio::test(flavor = "current_thread")]
async fn update_persists_metadata_and_reads_it_back() -> TestResult {
    let db = pg_test_db().await?;
    let actor = insert_actor(&db.pool).await?;
    let sandbox = seed_sandbox(&db.pool, &actor).await?;
    let controller = direct_sandbox_controller(&db.pool);

    let metadata: SandboxMetadata = [
        (
            "team".to_string(),
            SandboxMetadataValue::String("core".to_string()),
        ),
        ("retries".to_string(), SandboxMetadataValue::Number(3)),
        ("pinned".to_string(), SandboxMetadataValue::Bool(true)),
    ]
    .into_iter()
    .collect();

    let generation = controller
        .update(
            &db.pool,
            &sandbox,
            sandbox.id,
            metadata_request(Some(metadata.clone())),
            None,
        )
        .await?;
    assert!(
        generation.is_none(),
        "a metadata-only patch must not touch the network policy generation"
    );

    let reread = fetch_sandbox(&db.pool, sandbox.id)
        .await?
        .expect("sandbox still present");
    assert_eq!(reread.metadata.0, metadata);
    // The patch names metadata only; the seeded resources must not move.
    assert_eq!(reread.vcpu, Some(2));
    assert_eq!(reread.memory_mib, Some(512));
    Ok(())
}

/// An empty request writes nothing at all — no branch of `update` runs.
#[tokio::test(flavor = "current_thread")]
async fn update_with_no_fields_leaves_the_row_alone() -> TestResult {
    let db = pg_test_db().await?;
    let actor = insert_actor(&db.pool).await?;
    let sandbox = seed_sandbox(&db.pool, &actor).await?;
    let controller = direct_sandbox_controller(&db.pool);

    let before = fetch_sandbox(&db.pool, sandbox.id).await?.expect("present");
    controller
        .update(&db.pool, &sandbox, sandbox.id, metadata_request(None), None)
        .await?;

    let after = fetch_sandbox(&db.pool, sandbox.id).await?.expect("present");
    assert_eq!(after.metadata.0, before.metadata.0);
    assert_eq!(after.vcpu, before.vcpu);
    assert_eq!(after.memory_mib, before.memory_mib);
    assert_eq!(
        after.updated_at, before.updated_at,
        "a request with no fields must not stamp updated_at"
    );
    Ok(())
}

/// The `updated_at` behaviour change, asserted at the layer callers see.
/// The replaced inline SQL wrote `updated_at = NOW()`, which made
/// `sandboxes_set_updated_at` return early and stamp the row even when the
/// document was identical. Leaving the column to the trigger means an
/// idempotent PATCH no longer moves it, while a real edit still does.
#[tokio::test(flavor = "current_thread")]
async fn update_metadata_bumps_updated_at_only_on_a_real_change() -> TestResult {
    let db = pg_test_db().await?;
    let actor = insert_actor(&db.pool).await?;
    let sandbox = seed_sandbox(&db.pool, &actor).await?;
    let controller = direct_sandbox_controller(&db.pool);

    let metadata: SandboxMetadata = [(
        "team".to_string(),
        SandboxMetadataValue::String("core".to_string()),
    )]
    .into_iter()
    .collect();

    controller
        .update(
            &db.pool,
            &sandbox,
            sandbox.id,
            metadata_request(Some(metadata.clone())),
            None,
        )
        .await?;
    let first = fetch_sandbox(&db.pool, sandbox.id).await?.expect("present");

    // Same document again.
    controller
        .update(
            &db.pool,
            &sandbox,
            sandbox.id,
            metadata_request(Some(metadata)),
            None,
        )
        .await?;
    let repeated = fetch_sandbox(&db.pool, sandbox.id).await?.expect("present");
    assert_eq!(
        repeated.updated_at, first.updated_at,
        "an idempotent PATCH must not bump updated_at"
    );

    // A genuine edit.
    let changed: SandboxMetadata = [(
        "team".to_string(),
        SandboxMetadataValue::String("platform".to_string()),
    )]
    .into_iter()
    .collect();
    controller
        .update(
            &db.pool,
            &sandbox,
            sandbox.id,
            metadata_request(Some(changed)),
            None,
        )
        .await?;
    let edited = fetch_sandbox(&db.pool, sandbox.id).await?.expect("present");
    assert!(
        edited.updated_at > first.updated_at,
        "a real change must bump updated_at"
    );
    Ok(())
}

/// Metadata replaces wholesale rather than merging, and an empty document is
/// a value a caller can actually write.
#[tokio::test(flavor = "current_thread")]
async fn update_metadata_replaces_the_whole_document() -> TestResult {
    let db = pg_test_db().await?;
    let actor = insert_actor(&db.pool).await?;
    let sandbox = seed_sandbox(&db.pool, &actor).await?;
    let controller = direct_sandbox_controller(&db.pool);

    let seeded: SandboxMetadata = [
        (
            "team".to_string(),
            SandboxMetadataValue::String("core".to_string()),
        ),
        (
            "env".to_string(),
            SandboxMetadataValue::String("prod".to_string()),
        ),
    ]
    .into_iter()
    .collect();
    controller
        .update(
            &db.pool,
            &sandbox,
            sandbox.id,
            metadata_request(Some(seeded)),
            None,
        )
        .await?;

    let replacement: SandboxMetadata = [(
        "team".to_string(),
        SandboxMetadataValue::String("platform".to_string()),
    )]
    .into_iter()
    .collect();
    controller
        .update(
            &db.pool,
            &sandbox,
            sandbox.id,
            metadata_request(Some(replacement.clone())),
            None,
        )
        .await?;
    let after = fetch_sandbox(&db.pool, sandbox.id).await?.expect("present");
    assert_eq!(
        after.metadata.0, replacement,
        "the patch replaces the document; `env` must be gone, not merged"
    );

    controller
        .update(
            &db.pool,
            &sandbox,
            sandbox.id,
            metadata_request(Some(SandboxMetadata::new())),
            None,
        )
        .await?;
    let cleared = fetch_sandbox(&db.pool, sandbox.id).await?.expect("present");
    assert!(cleared.metadata.0.is_empty());
    Ok(())
}
