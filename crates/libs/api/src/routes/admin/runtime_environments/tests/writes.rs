//! Route tests for the policy read and the two writes: the store's
//! refusals, malformed requests, authorization and the `strand` gate.

use super::*;

#[tokio::test(flavor = "current_thread")]
async fn the_policy_reads_back_each_write_with_its_actor_and_reason() -> TestResult {
    let Some(h) = harness().await? else {
        return Ok(());
    };
    let (e0, e1) = (environment(0), environment(1));

    let (status, body) = h.get("/admin/api/v1/runtime-environments").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body,
        json!({
            "generation": 0,
            "default_environment_ref": null,
            "legacy_unbound_allowed": null,
            "last_change": null,
            "environments": [],
        })
    );

    let (status, body) = h.set_default(e0).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body,
        json!({"generation": 1, "default_environment_ref": e0.to_string()})
    );
    let (status, body) = h
        .set_state(e1, json!({"state": "deprecated", "reason": "superseded"}))
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body,
        json!({"generation": 2, "environment_ref": e1.to_string(), "state": "deprecated"})
    );

    let (status, body) = h.get("/admin/api/v1/runtime-environments").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["generation"], 2);
    assert_eq!(body["default_environment_ref"], e0.to_string());
    assert_eq!(body["last_change"]["reason"], "superseded");
    assert_eq!(
        body["last_change"]["updated_by_user_id"],
        h.user_id.to_string()
    );
    let environments = body["environments"].as_array().unwrap();
    assert_eq!(environments.len(), 2);
    assert_eq!(environments[0]["environment_ref"], e0.to_string());
    assert_eq!(environments[0]["state"], "allowed");
    assert_eq!(environments[0]["executes"], true);
    assert_eq!(environments[0]["is_default"], true);
    assert_eq!(environments[0]["last_change"]["reason"], "roll forward");
    assert_eq!(environments[1]["state"], "deprecated");
    assert_eq!(environments[1]["executes"], true);
    assert_eq!(environments[1]["is_default"], false);
    assert_eq!(environments[1]["last_change"]["reason"], "superseded");

    // A replaced default stays allowed, and blocking it is now possible.
    let (status, _) = h.set_default(e1).await;
    assert_eq!(status, StatusCode::OK);
    let (status, body) = h
        .set_state(e0, json!({"state": "blocked", "reason": "0-day"}))
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["generation"], 4);
    let policy = policy_store::get(&h.db.pool).await?;
    assert_eq!(policy.default_environment, Some(e1));
    assert_eq!(policy.environments[&e0], RuntimeEnvironmentState::Blocked);
    assert_eq!(policy.executing().collect::<Vec<_>>(), [e1]);

    // Every write was audited by the middleware.
    let audited: i64 = sqlx::query_scalar(sql!(
        "SELECT COUNT(*) FROM admin_audit_events
         WHERE path LIKE '%/runtime-environments/%' AND method = 'PUT'"
    ))
    .fetch_one(&h.db.pool)
    .await?;
    assert_eq!(audited, 4);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn writes_the_store_rejects_are_refused_and_change_nothing() -> TestResult {
    let Some(h) = harness().await? else {
        return Ok(());
    };
    let (e0, e1) = (environment(0), environment(1));

    // No default yet: a block would sit inert.
    let (status, body) = h
        .set_state(
            e1,
            json!({"state": "blocked", "reason": "0-day", "strand": true}),
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_error_contains(&body, "requires a cluster default");
    assert_eq!(h.generation().await?, 0);

    h.set_default(e0).await;
    for state in ["blocked", "deprecated"] {
        let (status, body) = h
            .set_state(
                e0,
                json!({"state": state, "reason": "nope", "strand": state == "blocked"}),
            )
            .await;
        assert_eq!(status, StatusCode::CONFLICT, "{state}: {body}");
        assert_error_contains(&body, "is the cluster default and must stay allowed");
    }
    // Re-allowing the default is accepted: it's already what the store keeps.
    let (status, _) = h
        .set_state(e0, json!({"state": "allowed", "reason": "noop"}))
        .await;
    assert_eq!(status, StatusCode::OK);
    let generation = h.generation().await?;

    let (status, _) = h
        .set_state(e1, json!({"state": "blocked", "reason": "0-day"}))
        .await;
    assert_eq!(status, StatusCode::OK);
    let (status, body) = h.set_default(e1).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_error_contains(&body, "is blocked and can't become the default");
    assert_eq!(h.generation().await?, generation + 1);

    // A stored state this build can't read: the default verb refuses it and
    // names the repair, which the state verb then performs.
    let e2 = environment(2);
    sqlx::query(sql!(
        "INSERT INTO runtime_environment_states (environment_ref, state, reason)
         VALUES ($1, 'retired', 'from a newer build')"
    ))
    .bind(e2.to_string())
    .execute(&h.db.pool)
    .await?;
    let (status, body) = h.set_default(e2).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_error_contains(&body, &state_path_suffix(e2));
    // The read fails closed and says why rather than serving a partial policy.
    let (status, body) = h.get("/admin/api/v1/runtime-environments").await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{body}");
    assert_error_contains(&body, "unknown stored state");
    // A later default needs only the default, so the unreadable row doesn't
    // refuse it.
    let (status, body) = h.set_default(e0).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    // Blocking it is a repair too, and with nothing bound needs no strand
    // even though the policy can't be read.
    let (status, body) = h
        .set_state(e2, json!({"state": "blocked", "reason": "repair"}))
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, _) = h
        .set_state(e2, json!({"state": "allowed", "reason": "repair"}))
        .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = h.set_default(e2).await;
    assert_eq!(status, StatusCode::OK);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn if_match_refuses_a_write_from_a_stale_read() -> TestResult {
    let Some(h) = harness().await? else {
        return Ok(());
    };
    let (e0, e1) = (environment(0), environment(1));
    let default_path = "/admin/api/v1/runtime-environments/default";
    let set_default = |reason: &str| json!({"environment_ref": e0.to_string(), "reason": reason});

    // Matching applies, quoted or not, like an unconditional write.
    let (status, body) = h
        .put_if_match(default_path, set_default("first"), "0")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["generation"], 1);
    let (status, body) = h
        .put_if_match(
            &state_path(e1),
            json!({"state": "blocked", "reason": "0-day"}),
            "\"1\"",
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["generation"], 2);

    // A second operator re-allowing from the read at generation 1 would undo
    // the block. It's refused, names the current generation, and changes
    // nothing: the block and its reason stand.
    let (status, body) = h
        .put_if_match(
            &state_path(e1),
            json!({"state": "allowed", "reason": "from an old list"}),
            "1",
        )
        .await;
    assert_eq!(status, StatusCode::PRECONDITION_FAILED, "{body}");
    assert_error_contains(&body, "the policy is at generation 2, not 1");
    // So is one from a generation the policy hasn't reached, and one that
    // can't be reached at all.
    for value in ["1", "9", &u64::MAX.to_string()] {
        let (status, body) = h
            .put_if_match(default_path, set_default("stale"), value)
            .await;
        assert_eq!(status, StatusCode::PRECONDITION_FAILED, "{value}: {body}");
    }
    assert_eq!(h.generation().await?, 2);
    let (_, policy) = h.get("/admin/api/v1/runtime-environments").await;
    let blocked = policy["environments"]
        .as_array()
        .and_then(|environments| {
            environments
                .iter()
                .find(|row| row["environment_ref"] == e1.to_string())
        })
        .cloned()
        .unwrap_or_default();
    assert_eq!(blocked["state"], "blocked", "{policy}");
    assert_eq!(blocked["last_change"]["reason"], "0-day", "{policy}");

    // A stale block that would need strand is answered 412, not with the
    // strand refusal for a policy the caller hasn't seen.
    let e2 = environment(2);
    let (status, _) = h
        .set_state(e2, json!({"state": "allowed", "reason": "canary"}))
        .await;
    assert_eq!(status, StatusCode::OK);
    insert_sandbox(&h.db.pool, "started").await?;
    let block = json!({"state": "blocked", "reason": "0-day"});
    let (status, body) = h.put_if_match(&state_path(e2), block.clone(), "2").await;
    assert_eq!(status, StatusCode::PRECONDITION_FAILED, "{body}");
    assert_error_contains(&body, "the policy is at generation 3, not 2");
    let (status, body) = h.put_if_match(&state_path(e2), block, "3").await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert!(body["at_risk"].is_object(), "{body}");

    // The store's own refusal describes the policy as it is now, so it wins
    // over a stale If-Match.
    let (status, body) = h
        .put_if_match(
            &state_path(e0),
            json!({"state": "deprecated", "reason": "nope"}),
            "0",
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_error_contains(&body, "must stay allowed");

    // A header that isn't a generation is a malformed request.
    for value in ["*", "W/\"2\"", "-1"] {
        let (status, body) = h
            .put_if_match(default_path, set_default("bad"), value)
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}: {body}");
        assert_error_contains(&body, "If-Match must be a generation number");
    }
    assert_eq!(h.generation().await?, 3);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn malformed_writes_are_refused_before_touching_the_policy() -> TestResult {
    let Some(h) = harness().await? else {
        return Ok(());
    };
    let e1 = environment(1);
    let default_path = "/admin/api/v1/runtime-environments/default";
    let long_reason = "x".repeat(1025);
    let cases: Vec<(String, Option<Value>, &str)> = vec![
        (default_path.into(), None, "missing JSON body"),
        (
            default_path.into(),
            Some(json!({"environment_ref": "sha256:short", "reason": "r"})),
            "invalid environment reference",
        ),
        (
            default_path.into(),
            Some(json!({"environment_ref": e1.to_string()})),
            "missing field `reason`",
        ),
        (
            default_path.into(),
            Some(json!({"environment_ref": e1.to_string(), "reason": ""})),
            "must not be empty",
        ),
        (
            default_path.into(),
            Some(json!({"environment_ref": e1.to_string(), "reason": long_reason})),
            "at most 1024 characters",
        ),
        (
            default_path.into(),
            Some(json!({"environment_ref": e1.to_string(), "reason": "r", "force": true})),
            "unknown field `force`",
        ),
        (
            "/admin/api/v1/runtime-environments/not-a-ref/state".into(),
            Some(json!({"state": "allowed", "reason": "r"})),
            "invalid environment reference",
        ),
        (
            state_path(e1),
            Some(json!({"state": "retired", "reason": "r"})),
            "expected one of: allowed, deprecated, blocked",
        ),
        (
            state_path(e1),
            Some(json!({"state": "allowed", "reason": "r", "strand": true})),
            "strand applies only to state 'blocked'",
        ),
        (
            state_path(e1),
            Some(json!({"state": "allowed", "reason": "nul\u{0}"})),
            "NUL",
        ),
    ];
    for (path, body, expected) in cases {
        let (status, response) = h.call(Method::PUT, &path, body.clone()).await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "{path} {body:?}: {response}"
        );
        assert!(
            response["error"]
                .as_str()
                .unwrap_or_default()
                .contains(expected),
            "{path} {body:?}: {response}"
        );
    }
    assert_eq!(h.generation().await?, 0);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn only_a_managing_org_admin_reaches_the_routes() -> TestResult {
    let Some(h) = harness().await? else {
        return Ok(());
    };
    let (_, member_token) = insert_session_user(&h.db.pool, "member").await?;
    for (method, path, body) in [
        (
            Method::GET,
            "/admin/api/v1/runtime-environments".to_owned(),
            None,
        ),
        (
            Method::GET,
            "/admin/api/v1/runtime-environments/nodes".to_owned(),
            None,
        ),
        (
            Method::GET,
            "/admin/api/v1/runtime-environments/population".to_owned(),
            None,
        ),
        (
            Method::PUT,
            "/admin/api/v1/runtime-environments/default".to_owned(),
            Some(json!({"environment_ref": environment(1).to_string(), "reason": "r"})),
        ),
        (
            Method::PUT,
            state_path(environment(1)),
            Some(json!({"state": "allowed", "reason": "r"})),
        ),
    ] {
        let (status, _) = call(&h.app, &member_token, method.clone(), &path, body.clone()).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{method} {path}");
        let (status, _) = call(&h.app, "not-a-session", method.clone(), &path, body).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{method} {path}");
    }
    assert_eq!(h.generation().await?, 0);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn the_first_default_needs_strand_while_it_would_stop_work() -> TestResult {
    let Some(h) = harness().await? else {
        return Ok(());
    };
    let (e0, e1, e3) = (environment(0), environment(1), environment(3));
    let node = NodeId::new();
    insert_ready_worker_node(&h.db.pool, node).await?;
    let sandbox = insert_sandbox(&h.db.pool, "started").await?;
    observe_sandbox(&h.db.pool, sandbox, node, Some(metadata(Some(e0)))).await?;

    // E0 has work and no state, so the first default would stop it.
    let (status, body) = h.set_default(e1).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_error_contains(&body, "strand: true");
    assert_eq!(body["at_risk"]["environments"], json!([e0.to_string()]));
    assert_eq!(body["at_risk"]["bound"]["sandboxes"], 1);
    assert_eq!(h.generation().await?, 0);

    // The store's refusal still comes first.
    sqlx::query(sql!(
        "INSERT INTO runtime_environment_states (environment_ref, state, reason)
         VALUES ($1, 'retired', 'from a newer build')"
    ))
    .bind(e3.to_string())
    .execute(&h.db.pool)
    .await?;
    let (status, body) = h.set_default(e3).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body.get("at_risk"), None, "{body}");
    sqlx::query(sql!(
        "DELETE FROM runtime_environment_states WHERE environment_ref = $1"
    ))
    .bind(e3.to_string())
    .execute(&h.db.pool)
    .await?;

    // An unreadable state row fails the first default closed: it narrows by
    // every row.
    let e4 = environment(4);
    sqlx::query(sql!(
        "INSERT INTO runtime_environment_states (environment_ref, state, reason)
         VALUES ($1, 'retired', 'from a newer build')"
    ))
    .bind(e4.to_string())
    .execute(&h.db.pool)
    .await?;
    let (status, body) = h.set_default(e1).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{body}");
    sqlx::query(sql!(
        "DELETE FROM runtime_environment_states WHERE environment_ref = $1"
    ))
    .bind(e4.to_string())
    .execute(&h.db.pool)
    .await?;
    assert_eq!(h.generation().await?, 0);

    // Keeping E0 allowed first, as the docs advise, needs no acknowledgement.
    let (status, body) = h
        .set_state(e0, json!({"state": "allowed", "reason": "keep E0"}))
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = h.set_default(e1).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    // A later default only adds itself as allowed, so work on an environment
    // with no state (E3, already stopped) doesn't gate it.
    let stopped = insert_sandbox(&h.db.pool, "started").await?;
    observe_sandbox(&h.db.pool, stopped, node, Some(metadata(Some(e3)))).await?;
    let (status, body) = h.set_default(e0).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn strand_acknowledges_what_the_first_default_stops() -> TestResult {
    let Some(h) = harness().await? else {
        return Ok(());
    };
    let (e0, e1) = (environment(0), environment(1));
    let node = NodeId::new();
    insert_ready_worker_node(&h.db.pool, node).await?;
    let sandbox = insert_sandbox(&h.db.pool, "started").await?;
    observe_sandbox(&h.db.pool, sandbox, node, Some(metadata(Some(e0)))).await?;
    let (status, body) = h
        .put(
            "/admin/api/v1/runtime-environments/default",
            json!({"environment_ref": e1.to_string(), "reason": "move on", "strand": true}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        policy_store::get(&h.db.pool).await?.default_environment,
        Some(e1)
    );
    Ok(())
}

/// A block that read its environment as already blocked rechecks once the
/// policy row lock is its own, if another write landed in between: here, one
/// that allowed the environment again.
#[tokio::test(flavor = "current_thread")]
async fn a_repeated_block_rechecks_after_a_concurrent_unblock() -> TestResult {
    let Some(h) = harness_with_connections(8).await? else {
        return Ok(());
    };
    let (e0, e1) = (environment(0), environment(1));
    h.set_default(e1).await;
    let (status, _) = h
        .set_state(e0, json!({"state": "blocked", "reason": "0-day"}))
        .await;
    assert_eq!(status, StatusCode::OK);
    let node = NodeId::new();
    insert_ready_worker_node(&h.db.pool, node).await?;
    let sandbox = insert_sandbox(&h.db.pool, "started").await?;
    observe_sandbox(&h.db.pool, sandbox, node, Some(metadata(Some(e0)))).await?;

    // Another operator allows E0, holding the policy row lock until commit.
    let mut unblock = h.db.pool.begin().await?;
    policy_store::set_environment_state(
        &mut unblock,
        e0,
        RuntimeEnvironmentState::Allowed,
        &PolicyChange {
            actor: Some(h.user_id),
            reason: PolicyChangeReason::new("fixed")?,
        },
    )
    .await?;

    // This block reads E0 as still blocked, then waits for that lock.
    let (app, token) = (h.app.clone(), h.token.clone());
    let block = tokio::spawn(async move {
        call(
            &app,
            &token,
            Method::PUT,
            &state_path(e0),
            Some(json!({"state": "blocked", "reason": "again"})),
        )
        .await
    });
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let waiting: i64 = sqlx::query_scalar(sql!(
            "SELECT COUNT(*) FROM pg_stat_activity
             WHERE datname = current_database() AND wait_event_type = 'Lock'"
        ))
        .fetch_one(&h.db.pool)
        .await?;
        if waiting > 0 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the block never waited for the lock"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    unblock.commit().await?;

    let (status, body) = block.await?;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["at_risk"]["environments"], json!([e0.to_string()]));
    assert_eq!(
        policy_store::get(&h.db.pool).await?.environments[&e0],
        RuntimeEnvironmentState::Allowed
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn blocking_needs_strand_while_work_may_be_bound_to_the_environment() -> TestResult {
    let Some(h) = harness().await? else {
        return Ok(());
    };
    let (e0, e1, e2) = (environment(0), environment(1), environment(2));
    h.set_default(e1).await;
    let node = NodeId::new();
    insert_ready_worker_node(&h.db.pool, node).await?;
    let sandbox = insert_sandbox(&h.db.pool, "started").await?;
    observe_sandbox(&h.db.pool, sandbox, node, Some(metadata(Some(e0)))).await?;
    let on_default = insert_sandbox(&h.db.pool, "started").await?;
    observe_sandbox(&h.db.pool, on_default, node, Some(metadata(Some(e1)))).await?;
    // Under a default, only an environment with a state executes, so only
    // blocking one can stop work.
    for environment in [e0, environment(3)] {
        let (status, _) = h
            .set_state(
                environment,
                json!({"state": "allowed", "reason": "still in use"}),
            )
            .await;
        assert_eq!(status, StatusCode::OK);
    }

    // The store's refusal comes first: blocking the default is refused for
    // being the default, not for the work bound to it.
    let (status, body) = h
        .set_state(e1, json!({"state": "blocked", "reason": "0-day"}))
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_error_contains(&body, "is the cluster default and must stay allowed");
    assert_eq!(body.get("at_risk"), None);

    // Nothing bound to E2, which still executes, and every sandbox's
    // environment known: the full check finds nothing, so no acknowledgement.
    let (status, _) = h
        .set_state(e2, json!({"state": "allowed", "reason": "never shipped"}))
        .await;
    assert_eq!(status, StatusCode::OK);
    let (status, body) = h
        .set_state(e2, json!({"state": "blocked", "reason": "never shipped"}))
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let generation = h.generation().await?;

    let (status, body) = h
        .set_state(e0, json!({"state": "blocked", "reason": "0-day"}))
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_error_contains(&body, "strand: true");
    assert_eq!(body["at_risk"]["bound"]["sandboxes"], 1);
    assert_eq!(
        body["at_risk"]["bound"]["sandboxes_by_lifecycle"]["started"],
        1
    );
    assert_eq!(body["at_risk"]["unbound"]["sandboxes"], 0);
    // The refused write rolled back: no generation, the state unchanged.
    assert_eq!(h.generation().await?, generation);
    assert_eq!(
        policy_store::get(&h.db.pool).await?.environments[&e0],
        RuntimeEnvironmentState::Allowed
    );

    let (status, body) = h
        .set_state(
            e0,
            json!({"state": "blocked", "reason": "0-day", "strand": true}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["generation"], generation + 1);

    // Blocking it again, say to update the reason, stops nothing new.
    let (status, body) = h
        .set_state(
            e0,
            json!({"state": "blocked", "reason": "0-day, confirmed"}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    // Under a default, an environment with no state already doesn't execute,
    // so blocking it stops nothing new either, work bound to it or not.
    let e5 = environment(5);
    let unstated = insert_sandbox(&h.db.pool, "started").await?;
    observe_sandbox(&h.db.pool, unstated, node, Some(metadata(Some(e5)))).await?;
    let (status, body) = h
        .set_state(e5, json!({"state": "blocked", "reason": "never certified"}))
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    // Unbound work may be the blocked environment's, so it needs strand too.
    let legacy = insert_sandbox(&h.db.pool, "stopped").await?;
    observe_sandbox(&h.db.pool, legacy, node, Some(metadata(None))).await?;
    let e3 = environment(3);
    let (status, body) = h
        .set_state(e3, json!({"state": "blocked", "reason": "0-day"}))
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["at_risk"]["bound"]["sandboxes"], 0);
    assert_eq!(body["at_risk"]["unbound"]["sandboxes"], 1);

    // So does a sandbox its node hasn't reported, on its own: it may be bound
    // to E3. (The legacy one goes first, so it can't be what refuses.)
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'deleted' WHERE id = $1"
    ))
    .bind(legacy)
    .execute(&h.db.pool)
    .await?;
    insert_sandbox(&h.db.pool, "started").await?;
    let (status, body) = h
        .set_state(e3, json!({"state": "blocked", "reason": "0-day"}))
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["at_risk"]["unbound"]["sandboxes"], 0);
    assert_eq!(body["at_risk"]["unrecorded"]["sandboxes"], 1);
    assert_eq!(
        body["at_risk"]["unrecorded"]["sandboxes_by_lifecycle"]["started"],
        1
    );

    // Deprecating doesn't strand anything, so it never asks.
    let (status, body) = h
        .set_state(e3, json!({"state": "deprecated", "reason": "superseded"}))
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn only_releases_still_in_use_gate_a_block() -> TestResult {
    let Some(h) = harness().await? else {
        return Ok(());
    };
    let (e0, e1) = (environment(0), environment(1));
    h.set_default(e1).await;
    let allow_e0 = || h.set_state(e0, json!({"state": "allowed", "reason": "back"}));
    let block_e0 = || h.set_state(e0, json!({"state": "blocked", "reason": "0-day"}));
    let (status, _) = allow_e0().await;
    assert_eq!(status, StatusCode::OK);

    // Superseded: untagged and unpooled, so reachable only by its exact hash.
    // Reported, but it doesn't gate.
    insert_template_release(&h.db.pool, "superseded", Some(metadata(Some(e0)))).await?;
    let (status, body) = block_e0().await;
    assert_eq!(status, StatusCode::OK, "{body}");
    allow_e0().await;

    // Untagged, but an enabled pool config pins it and keeps making slots.
    let pooled = insert_template_release(&h.db.pool, "pooled", Some(metadata(Some(e0)))).await?;
    let config = pool_release(&h.db.pool, pooled, true).await?;
    let (status, body) = block_e0().await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["at_risk"]["environments"], json!([e0.to_string()]));
    assert_eq!(body["at_risk"]["bound"]["template_releases"], 2);
    assert_eq!(body["at_risk"]["bound"]["template_releases_in_use"], 1);

    // A disabled pool config creates nothing, so it no longer gates.
    sqlx::query(sql!(
        "UPDATE kubernetes_pool_configs SET enabled = FALSE WHERE id = $1"
    ))
    .bind(config)
    .execute(&h.db.pool)
    .await?;
    let (status, body) = block_e0().await;
    assert_eq!(status, StatusCode::OK, "{body}");
    allow_e0().await;

    // Tagged: creates by tag resolve to it.
    let tagged = insert_template_release(&h.db.pool, "tagged", Some(metadata(Some(e0)))).await?;
    tag_release(&h.db.pool, tagged, "tagged", "latest").await?;
    let (status, body) = block_e0().await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["at_risk"]["bound"]["template_releases_in_use"], 1);
    Ok(())
}
