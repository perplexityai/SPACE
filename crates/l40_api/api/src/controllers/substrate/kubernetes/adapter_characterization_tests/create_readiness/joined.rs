//! Joined-create and post-create metadata repair characterization tests.

use super::*;

#[derive(Debug, Clone, Copy)]
enum OwnerPod {
    /// The owner's pod is running and its proxy reachable.
    Reachable,
    /// The owner's pod is reachable, but its guest never answers a create
    /// that waits for it.
    ReachableGuestSilent,
    /// The owner's pod never gets an IP.
    NeverAssigned,
}

/// A create that finds another create's activation in flight joins it rather
/// than creating a pod: it returns the owner's pod once that pod is
/// reachable, without a lease of its own, and when the owner's pod never
/// becomes reachable it times out within the create budget, leaving the
/// owner's execution and activation untouched. A create that waits for the
/// guest, and whose guest does not answer in time, returns the owner's
/// reachable pod as started but not command-ready rather than failing; its
/// caller would delete the sandbox the owner is starting on a failure.
#[rstest::rstest]
#[case::returns_the_owners_reachable_pod(OwnerPod::Reachable)]
#[case::keeps_the_owners_pod_when_its_guest_is_slow(OwnerPod::ReachableGuestSilent)]
#[case::times_out_without_disturbing_the_owner(OwnerPod::NeverAssigned)]
#[tokio::test(flavor = "current_thread")]
async fn joined_create_waits_on_the_owners_pod(#[case] owner_pod: OwnerPod) -> TestResult {
    if !crate::test_support::enter_sim_worker_process_with_owned_token_files() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    insert_test_sandbox_row(&db.pool, sandbox_id, "creating").await?;
    let worker = match owner_pod {
        OwnerPod::ReachableGuestSilent => {
            start_scripted_worker(ScriptedWorkerConfig::new([(
                SPACED_WAIT_READY,
                ScriptedReply::Hang,
            )]))
            .await?
        }
        OwnerPod::Reachable | OwnerPod::NeverAssigned => {
            start_scripted_worker(ScriptedWorkerConfig::new([])).await?
        }
    };
    let pod_name: Arc<Mutex<Option<String>>> = Arc::default();
    let reply_pod_name = Arc::clone(&pod_name);
    let (client, log) = scripted_kube_client_async(move |request| {
        let pod_name = reply_pod_name.lock().unwrap().clone();
        async move {
            match (request.method, pod_name) {
                (Method::GET, Some(pod_name)) if request.path == pod_path(&pod_name) => {
                    match owner_pod {
                        OwnerPod::Reachable | OwnerPod::ReachableGuestSilent => {
                            pod_reply(&running_pod(&pod_name, sandbox_id, Some(WORKER_NODE)))
                        }
                        OwnerPod::NeverAssigned => pod_reply(&sandbox_pod(
                            &pod_name,
                            &format!("{pod_name}-uid"),
                            sandbox_id,
                            1,
                            "Pending",
                            None,
                        )),
                    }
                }
                _ => kube_status(StatusCode::BAD_REQUEST, "BadRequest"),
            }
        }
    });
    let mut substrate = substrate_on(db.pool.clone(), client, &worker);
    // A phase timeout far past the budget, so only the create budget can end
    // the joined wait.
    substrate.k8s.create_data_plane_ready_timeout = Duration::from_secs(60);

    // Another create owns the activation.
    let owner_attempt = SandboxActivationAttemptId::new();
    let owner = substrate
        .store
        .begin_activation_execution_row(
            sandbox_id,
            owner_attempt,
            SandboxTransitionTargetState::Started,
        )
        .await?;
    *pod_name.lock().unwrap() = Some(owner.pod_name.clone());
    if let OwnerPod::Reachable | OwnerPod::ReachableGuestSilent = owner_pod {
        substrate
            .store
            .persist_pod_metadata(
                sandbox_id,
                owner.generation,
                Some(&format!("{}-uid", owner.pod_name)),
                Some("127.0.0.1"),
                None,
            )
            .await?;
    }
    let inputs = CreateInputs::new();
    let template_ref = inputs.template_ref();
    let mut request = inputs.create_request(sandbox_id, &template_ref);
    if let OwnerPod::ReachableGuestSilent = owner_pod {
        request.wait_until_ready = true;
        request.command_ready_wait = Some(Duration::from_millis(200));
    }

    let started = Instant::now();
    let outcome =
        tokio::time::timeout(Duration::from_secs(10), substrate.create_execution(request))
            .await
            .map_err(|_| format!("{owner_pod:?}: the joined create must end"))?;

    assert!(
        requests_matching(&log, &Method::POST).is_empty()
            && requests_matching(&log, &Method::DELETE).is_empty(),
        "{owner_pod:?}: a joined create neither creates nor deletes a pod"
    );
    assert_eq!(
        active_attempt(&db.pool, sandbox_id).await?,
        Some(owner_attempt.as_uuid()),
        "{owner_pod:?}: the owner keeps its activation"
    );
    assert!(
        substrate.task_tracker.is_empty(),
        "{owner_pod:?}: a joined create leaves nothing running in the background"
    );
    match owner_pod {
        OwnerPod::Reachable => {
            let outcome =
                outcome.map_err(|error| format!("expected a joined create: {error:?}"))?;
            assert!(
                outcome.lease_completion.is_none(),
                "the lease is the owner's"
            );
            assert_eq!(outcome.initial_lifecycle, "started");
            assert_eq!(outcome.active_node_id, Some(worker_node_id()));
            assert!(outcome.command_ready);
            let binding = outcome
                .binding
                .ok_or("a joined create returns the owner's binding")?;
            assert_eq!(
                binding.execution_object_id,
                format!("{}-uid", owner.pod_name)
            );
            assert_eq!(binding.execution_generation, 1);
        }
        OwnerPod::ReachableGuestSilent => {
            let outcome = outcome
                .map_err(|error| format!("expected the owner's pod, not yet ready: {error:?}"))?;
            assert!(
                outcome.lease_completion.is_none(),
                "the lease is the owner's"
            );
            assert_eq!(outcome.initial_lifecycle, "started");
            assert_eq!(outcome.active_node_id, Some(worker_node_id()));
            assert!(!outcome.command_ready, "the guest never answered");
            assert!(outcome.binding.is_none());
            assert_eq!(active_execution_count(&db.pool, sandbox_id).await?, 1);
            // A guest probe that outlived the create's 200 ms wait would hold
            // it for a whole probe timeout.
            let elapsed = started.elapsed();
            assert!(
                elapsed < START_WAIT_COMMAND_PROBE_TIMEOUT,
                "the create's own wait ends it: {elapsed:?}"
            );
        }
        OwnerPod::NeverAssigned => {
            let outcome = outcome
                .err()
                .ok_or("expected the joined create to time out")?;
            assert_eq!(
                outcome.wait_ready_timeout_phase(),
                Some(WaitReadyTimeoutPhase::PodNeverReady),
                "{outcome:?}"
            );
            assert!(
                outcome.preserves_create_row(),
                "joined create errors must preserve the owner's sandboxes row: {outcome:?}"
            );
            let elapsed = started.elapsed();
            assert!(
                elapsed < Duration::from_secs(5),
                "the create budget, not the data-plane timeout, must end the wait: {elapsed:?}"
            );
            assert_eq!(active_execution_count(&db.pool, sandbox_id).await?, 1);
        }
    }
    Ok(())
}

/// When `POST /pods` returns an already node-pinned pod (`spec.node_name` set),
/// `remember_pod_node_placement_fenced` advances `sandbox_k8s_executions.updated_at`
/// before `persist_pod_metadata_if_unchanged` runs. If that initial `pod_uid`
/// persistence fails transiently, `create_execution` must thread the post-placement
/// `creating` `updated_at` epoch into `PodProxyWaitTarget` so readiness repairs the
/// row instead of treating its own `creating` row as concurrently reset.
#[tokio::test(flavor = "current_thread")]
async fn pre_scheduled_create_pod_with_transient_metadata_failure_threads_repair_epoch()
-> TestResult {
    if !crate::test_support::enter_sim_worker_process_with_owned_token_files() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    insert_test_sandbox_row(&db.pool, sandbox_id, "creating").await?;

    // Fail only the first `pod_uid` transition (`NULL -> NOT NULL`), after
    // `k8s_node_name` placement has already bumped `updated_at` on the `creating`
    // row; `nextval` is non-transactional so the repair attempt succeeds.
    sqlx::raw_sql(
        "CREATE SEQUENCE fail_first_pod_uid_seq;
         CREATE FUNCTION fail_first_pod_uid_write() RETURNS trigger AS $$
         BEGIN
             IF OLD.pod_uid IS NULL AND NEW.pod_uid IS NOT NULL
                AND nextval('fail_first_pod_uid_seq') = 1 THEN
                 RAISE EXCEPTION 'simulated transient post-POST metadata write failure';
             END IF;
             RETURN NEW;
         END;
         $$ LANGUAGE plpgsql;
         CREATE TRIGGER trg_fail_first_pod_uid_write
             BEFORE UPDATE ON sandbox_k8s_executions
             FOR EACH ROW EXECUTE FUNCTION fail_first_pod_uid_write();",
    )
    .execute(&db.pool)
    .await?;

    let worker = start_scripted_worker(ScriptedWorkerConfig::new([])).await?;
    let created_name: Arc<Mutex<Option<String>>> = Arc::default();
    let reply_name = Arc::clone(&created_name);
    let (client, _log) = scripted_kube_client(move |request| match request.method {
        Method::POST if request.path == PODS_PATH => {
            let name = requested_pod_name(request);
            *reply_name.lock().unwrap() = Some(name.clone());
            pod_reply(&starting_pod(&name, sandbox_id, Some(WORKER_NODE)))
        }
        _ => kube_status(StatusCode::INTERNAL_SERVER_ERROR, "UnexpectedUncachedGet"),
    });

    let mut substrate = substrate_on(db.pool.clone(), client, &worker);
    substrate.budgets = phase_ending_budgets(substrate.budgets);
    substrate.budgets.readiness_quiet_reflector = Duration::from_secs(60);
    let proxy_wait_started = Arc::clone(&substrate.readiness_test_hooks.proxy_wait_started_at);
    let (substrate, mut feed) = substrate.with_pod_cache_feed(Vec::new());

    let inputs = CreateInputs::new();
    let template_ref = inputs.template_ref();
    let mut request = inputs.create_request(sandbox_id, &template_ref);
    request.wait_until_ready = false;

    let reflector = async {
        let name = loop {
            if proxy_wait_started.lock().unwrap().is_some()
                && let Some(name) = created_name.lock().unwrap().clone()
            {
                break name;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        };
        feed.apply(running_pod(&name, sandbox_id, Some(WORKER_NODE)));
        feed
    };

    let (outcome, _feed) = tokio::time::timeout(
        Duration::from_secs(5),
        futures_util::future::join(Box::pin(substrate.create_execution(request)), reflector),
    )
    .await
    .map_err(|_| "create must repair metadata and finish")?;
    let outcome = outcome?;
    let name = created_name.lock().unwrap().clone().expect("created pod");
    let binding = outcome.binding.ok_or("repaired create binds pod")?;
    assert_eq!(binding.execution_object_id, format!("{name}-uid"));
    assert_eq!(outcome.active_node_id, Some(worker_node_id()));

    let row = substrate
        .store
        .active_execution_status(sandbox_id)
        .await?
        .ok_or("active execution row present")?;
    let expected_uid = format!("{name}-uid");
    assert_eq!(
        row.pod_uid.as_ref().map(PodUid::as_str),
        Some(expected_uid.as_str())
    );
    assert_eq!(row.pod_ip.as_deref(), Some("127.0.0.1"));
    Ok(())
}

/// A joined create pinned to `uid-a` waits while only the reflector shows a
/// same-generation replacement `uid-b`, then immediately switches onto `uid-b`
/// once the owner records `uid-b` in Postgres without needing another pod event;
/// and if the owner retires the execution row instead, the joiner's failure is
/// marked `preserves_create_row()` so `create_sandbox_inner` never deletes the
/// owner's `sandboxes` row.
#[tokio::test(flavor = "current_thread")]
async fn joined_create_follows_same_generation_uid_replacement_once_db_confirms_it() -> TestResult {
    if !crate::test_support::enter_sim_worker_process_with_owned_token_files() {
        return Ok(());
    }
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    insert_test_sandbox_row(&db.pool, sandbox_id, "creating").await?;
    let worker = start_scripted_worker(ScriptedWorkerConfig::new([])).await?;
    let (client, _log) = scripted_kube_client(|_req| {
        kube_status(StatusCode::INTERNAL_SERVER_ERROR, "UnexpectedUncachedGet")
    });

    let mut substrate = substrate_on(db.pool.clone(), client, &worker);
    substrate.budgets.create_ready = Duration::from_secs(5);
    substrate.budgets.readiness_quiet_reflector = Duration::from_secs(60);

    let owner_attempt = SandboxActivationAttemptId::new();
    let owner = substrate
        .store
        .begin_activation_execution_row(
            sandbox_id,
            owner_attempt,
            SandboxTransitionTargetState::Started,
        )
        .await?;
    substrate
        .store
        .persist_pod_metadata(sandbox_id, owner.generation, Some("uid-a"), None, None)
        .await?;

    let mut replacement = running_pod(&owner.pod_name, sandbox_id, Some(WORKER_NODE));
    replacement.metadata.uid = Some("uid-b".to_string());
    let recheck_armed = Arc::clone(&substrate.readiness_test_hooks.recheck_armed);
    let ip_iters = Arc::clone(&substrate.readiness_test_hooks.ip_wait_iterations);
    let (substrate, _feed) = substrate.with_pod_cache_feed(vec![replacement]);

    let inputs = CreateInputs::new();
    let template_ref = inputs.template_ref();
    let mut request = inputs.create_request(sandbox_id, &template_ref);
    request.wait_until_ready = false;

    let store = substrate.store.clone();
    let owner_gen = owner.generation;
    let armed = recheck_armed.notified();
    tokio::pin!(armed);
    armed.as_mut().enable();
    let driver = async move {
        // Wait until iteration 1 checks Postgres while it still names `uid-a`
        // and arms `mismatched_recheck`, then advance Postgres to `uid-b` with
        // zero new pod cache events.
        tokio::time::timeout(Duration::from_secs(1), armed)
            .await
            .map_err(|_| "joiner never armed mismatched_recheck against uid-a")?;
        store
            .persist_pod_metadata(sandbox_id, owner_gen, Some("uid-b"), None, None)
            .await?;
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    };

    let started = Instant::now();
    let (outcome, driver_res) = tokio::join!(substrate.create_execution(request), driver);
    driver_res?;
    let outcome = outcome?;
    let binding = outcome.binding.ok_or("joined create binds replacement")?;
    assert_eq!(binding.execution_object_id, "uid-b");
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "joiner must pivot to uid-b without waiting out 5s create_ready: {:?}",
        started.elapsed()
    );

    // Non-timeout joined-create failure (owner retired the execution row) must
    // also carry `preserves_create_row()` so `create_sandbox_inner` will not
    // delete the owner's `sandboxes` row.
    let retired_sbx = SandboxId::new();
    insert_test_sandbox_row(&db.pool, retired_sbx, "creating").await?;
    let retired_owner_attempt = SandboxActivationAttemptId::new();
    let retired_owner = substrate
        .store
        .begin_activation_execution_row(
            retired_sbx,
            retired_owner_attempt,
            SandboxTransitionTargetState::Started,
        )
        .await?;
    let mut join_retired_req = inputs.create_request(retired_sbx, &template_ref);
    join_retired_req.wait_until_ready = false;
    let base_iters = ip_iters.load(Ordering::SeqCst);
    let retire_pool = db.pool.clone();
    let retire_driver = async move {
        let sync_deadline = Instant::now() + Duration::from_secs(1);
        while ip_iters.load(Ordering::SeqCst) <= base_iters {
            if Instant::now() >= sync_deadline {
                return Err::<(), Box<dyn std::error::Error + Send + Sync>>(
                    "retired-owner joiner never entered wait_for_pod_ip_assignment".into(),
                );
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions SET active = FALSE WHERE sandbox_id = $1 AND generation = $2"
        ))
        .bind(retired_sbx)
        .bind(retired_owner.generation)
        .execute(&retire_pool)
        .await?;
        Ok(())
    };
    let (retired_res, retire_db_res) =
        tokio::join!(substrate.create_execution(join_retired_req), retire_driver);
    retire_db_res?;
    let err = retired_res
        .err()
        .ok_or("retired owner row fails joined create")?;
    assert!(
        err.preserves_create_row(),
        "non-timeout joined-create error must preserve owner's sandboxes row: {err:?}"
    );
    Ok(())
}
