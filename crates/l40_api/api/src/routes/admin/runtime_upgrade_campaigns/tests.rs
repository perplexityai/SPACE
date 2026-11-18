//! Route tests against Postgres, through the admin router's real auth and
//! audit middleware.

use axum::http::Method;
use pplx_space_sql_locator::sql;
use pplx_space_content_digest::Digest;
use pplx_space_runtime_upgrade_campaign_store::test_support::{CampaignSeed, insert_campaign};
use pplx_space_runtime_upgrade_campaign_store::{CampaignPhase, ItemPhase, SubjectKind};
use serde_json::{Value, json};

use super::*;
use crate::routes::admin::test_fixtures::{
    AdminRouterHarness, TestResult, admin_router_harness, admin_router_harness_with,
};

const BOTH_KINDS: &[CampaignKind] = &[CampaignKind::Templates, CampaignKind::Sandboxes];

fn environment(byte: u8) -> EnvironmentReference {
    EnvironmentReference::new(Digest::sha256([byte; 32]))
}

/// A harness whose API starts campaigns of both kinds, as it will once both
/// drivers land.
async fn driven() -> TestResult<Option<AdminRouterHarness>> {
    admin_router_harness_with(|state| state.runtime_upgrade_driven_kinds = BOTH_KINDS).await
}

fn start_body(kind: &str, from: u8) -> Value {
    json!({
        "kind": kind,
        "from_environment_ref": environment(from).to_string(),
        "to_environment_ref": environment(200).to_string(),
        "max_in_flight_per_cluster": 4,
        "failure_budget": 2,
        "reason": "move off a blocked environment",
    })
}

async fn start(h: &AdminRouterHarness, body: Value) -> (StatusCode, Value) {
    h.as_operator(Method::POST, "/runtime-upgrade-campaigns", &[], Some(body))
        .await
}

async fn action(
    h: &AdminRouterHarness,
    campaign: CampaignId,
    verb: &str,
    if_match: Option<&str>,
    body: Value,
) -> (StatusCode, Value) {
    let headers: Vec<(&str, &str)> = if_match
        .map(|value| ("if-match", value))
        .into_iter()
        .collect();
    h.as_operator(
        Method::POST,
        &format!("/runtime-upgrade-campaigns/{campaign}:{verb}"),
        &headers,
        Some(body),
    )
    .await
}

fn assert_error_contains(body: &Value, needle: &str) {
    let error = body["error"].as_str().unwrap_or_default();
    assert!(error.contains(needle), "expected {needle:?} in {body}");
}

/// Seed `count` items of `campaign` in `phase`.
async fn items(
    h: &AdminRouterHarness,
    campaign: CampaignId,
    kind: SubjectKind,
    phase: ItemPhase,
    count: usize,
) -> TestResult {
    for _ in 0..count {
        sqlx::query(sql!(
            "INSERT INTO runtime_upgrade_items (campaign_id, subject_kind, subject_id, phase)
             VALUES ($1, $2, $3, $4)"
        ))
        .bind(campaign)
        .bind(kind.as_str())
        .bind(uuid::Uuid::now_v7())
        .bind(phase.as_str())
        .execute(&h.db.pool)
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_start_for_a_kind_with_no_driver_is_refused() -> TestResult {
    let Some(h) = admin_router_harness().await? else {
        return Ok(());
    };
    for kind in ["templates", "sandboxes"] {
        let (status, body) = start(&h, start_body(kind, 1)).await;
        assert_eq!(status, StatusCode::CONFLICT, "{kind}: {body}");
        assert_error_contains(&body, "no cluster-controller driver moves");
    }
    let (status, body) = h
        .as_operator(Method::GET, "/runtime-upgrade-campaigns", &[], None)
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body, json!({"campaigns": [], "next_after": null}));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_started_campaign_reads_back_with_its_items_and_generation() -> TestResult {
    let Some(h) = driven().await? else {
        return Ok(());
    };
    let (status, body) = start(&h, start_body("templates", 1)).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(body["phase"], "canary");
    assert_eq!(body["generation"], 0);
    let campaign = CampaignId(body["id"].as_str().unwrap().parse()?);
    items(
        &h,
        campaign,
        SubjectKind::TemplateRelease,
        ItemPhase::Pending,
        2,
    )
    .await?;
    items(
        &h,
        campaign,
        SubjectKind::TemplateRelease,
        ItemPhase::Done,
        1,
    )
    .await?;

    let (status, body) = h
        .as_operator(
            Method::GET,
            &format!("/runtime-upgrade-campaigns/{campaign}"),
            &[],
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["kind"], "templates");
    assert_eq!(body["from_environment_ref"], environment(1).to_string());
    assert_eq!(body["to_environment_ref"], environment(200).to_string());
    assert_eq!(body["phase"], "canary");
    assert_eq!(body["generation"], 0);
    assert_eq!(body["max_in_flight_per_node"], 1);
    assert_eq!(body["max_in_flight_per_cluster"], 4);
    assert_eq!(body["failure_budget"], 2);
    assert_eq!(body["reason"], "move off a blocked environment");
    assert_eq!(body["created_by_user_id"], h.operator.id.to_string());
    assert_eq!(body["finished_at"], Value::Null);
    assert_eq!(
        body["items"],
        json!({"pending": 2, "in_flight": 0, "done": 1, "failed": 0, "skipped": 0})
    );
    assert_eq!(body["stranded"], Value::Null, "not defined for templates");

    let (status, list) = h
        .as_operator(Method::GET, "/runtime-upgrade-campaigns", &[], None)
        .await;
    assert_eq!(status, StatusCode::OK, "{list}");
    assert_eq!(list["campaigns"].as_array().unwrap().len(), 1);
    assert_eq!(list["campaigns"][0], body);
    assert_eq!(list["next_after"], Value::Null);

    // Audited by the middleware.
    assert_eq!(h.audit_events("/runtime-upgrade-campaigns").await.len(), 1);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_start_is_refused_while_a_campaign_moves_the_same_source() -> TestResult {
    let Some(h) = driven().await? else {
        return Ok(());
    };
    let (status, first) = start(&h, start_body("sandboxes", 1)).await;
    assert_eq!(status, StatusCode::CREATED, "{first}");

    let (status, body) = start(&h, start_body("sandboxes", 1)).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_error_contains(&body, first["id"].as_str().unwrap());
    // Another kind or another source is free.
    let (status, _) = start(&h, start_body("templates", 1)).await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = start(&h, start_body("sandboxes", 2)).await;
    assert_eq!(status, StatusCode::CREATED);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn malformed_starts_are_refused() -> TestResult {
    let Some(h) = driven().await? else {
        return Ok(());
    };
    let mut bodies = Vec::new();
    for (field, value) in [
        ("kind", json!("nodes")),
        ("from_environment_ref", json!("sha256:short")),
        ("canary_size", json!(2)),
        ("failure_budget", json!(0)),
        ("reason", json!("")),
        ("include_running", json!(true)),
    ] {
        let mut body = start_body("templates", 1);
        body[field] = value;
        bodies.push((field, body));
    }
    let mut missing = start_body("templates", 1);
    missing.as_object_mut().unwrap().remove("reason");
    bodies.push(("missing reason", missing));
    for (what, body) in bodies {
        let (status, body) = start(&h, body).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{what}: {body}");
    }
    let (status, _) = h
        .as_operator(Method::GET, "/runtime-upgrade-campaigns", &[], None)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        store::list_records(&h.db.pool, false, None, 10)
            .await?
            .is_empty()
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn pause_and_resume_answer_412_on_a_stale_generation() -> TestResult {
    let Some(h) = driven().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(&h.db.pool, CampaignSeed::default()).await?;
    let reason = json!({"reason": "investigating a boot failure"});

    let (status, body) = action(&h, campaign, "pause", Some("\"3\""), reason.clone()).await;
    assert_eq!(status, StatusCode::PRECONDITION_FAILED, "{body}");
    assert_error_contains(&body, "at generation 0, not 3");

    let (status, body) = action(&h, campaign, "pause", Some("0"), reason.clone()).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body,
        json!({"id": campaign.to_string(), "phase": "paused", "generation": 1, "retried": 0, "draining": 0})
    );

    // The read the caller made before the pause is stale now.
    let (status, body) = action(&h, campaign, "resume", Some("0"), reason.clone()).await;
    assert_eq!(status, StatusCode::PRECONDITION_FAILED, "{body}");
    let (status, body) = action(&h, campaign, "resume", Some("1"), reason).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["phase"], "canary", "nothing is done yet");
    assert_eq!(body["generation"], 2);

    let record = store::get_record(&h.db.pool, campaign).await?.unwrap();
    assert_eq!(record.updated_by, Some(h.operator.id));
    assert_eq!(record.campaign.phase, CampaignPhase::Canary);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_resume_at_the_failure_budget_needs_retry_failed() -> TestResult {
    let Some(h) = driven().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(
        &h.db.pool,
        CampaignSeed {
            phase: CampaignPhase::Paused,
            failure_budget: 2,
            ..CampaignSeed::default()
        },
    )
    .await?;
    items(&h, campaign, SubjectKind::Sandbox, ItemPhase::Failed, 2).await?;
    items(&h, campaign, SubjectKind::Sandbox, ItemPhase::Done, 1).await?;

    let (status, body) = action(&h, campaign, "resume", None, json!({"reason": "fixed"})).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_error_contains(&body, "at or over its failure budget");

    let (status, body) = action(
        &h,
        campaign,
        "resume",
        None,
        json!({"reason": "fixed", "retry_failed": true}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["retried"], 2);
    assert_eq!(body["phase"], "rolling", "an item is done");
    let counts = store::item_counts(&h.db.pool, campaign).await?;
    assert_eq!((counts.pending, counts.failed), (2, 0));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn phase_changes_a_campaign_doesnt_allow_are_refused() -> TestResult {
    let Some(h) = driven().await? else {
        return Ok(());
    };
    let completed = insert_campaign(
        &h.db.pool,
        CampaignSeed {
            phase: CampaignPhase::Completed,
            ..CampaignSeed::default()
        },
    )
    .await?;
    let rolling = insert_campaign(
        &h.db.pool,
        CampaignSeed {
            from: environment(9),
            ..CampaignSeed::default()
        },
    )
    .await?;
    let reason = json!({"reason": "r"});

    let (status, body) = action(&h, completed, "pause", None, reason.clone()).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_error_contains(&body, "final");
    let (status, body) = action(&h, rolling, "resume", None, reason.clone()).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    let (status, _) = action(&h, rolling, "pause", None, reason.clone()).await;
    assert_eq!(status, StatusCode::OK);
    let (status, body) = action(&h, rolling, "pause", None, reason.clone()).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_error_contains(&body, "already paused");

    let unknown = CampaignId(uuid::Uuid::now_v7());
    let (status, _) = action(&h, unknown, "pause", None, reason.clone()).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = action(&h, rolling, "cancel", None, reason.clone()).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "an unknown action");
    let (status, _) = action(&h, rolling, "resume", None, json!({"reason": ""})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = action(&h, rolling, "pause", None, json!({"why": "x"})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = h
        .as_operator(
            Method::GET,
            "/runtime-upgrade-campaigns/not-a-uuid",
            &[],
            None,
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = h
        .as_operator(
            Method::GET,
            &format!("/runtime-upgrade-campaigns/{unknown}"),
            &[],
            None,
        )
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_sandbox_campaign_without_an_edge_reports_its_pending_items_stranded() -> TestResult {
    let Some(h) = driven().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(&h.db.pool, CampaignSeed::default()).await?;
    items(&h, campaign, SubjectKind::Sandbox, ItemPhase::Pending, 3).await?;
    let read = || async {
        let (status, body) = h
            .as_operator(
                Method::GET,
                &format!("/runtime-upgrade-campaigns/{campaign}"),
                &[],
                None,
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        body["stranded"].clone()
    };
    assert_eq!(read().await, json!(3), "no catalog row, so no edge");

    let seed = CampaignSeed::default();
    sqlx::query(sql!(
        "INSERT INTO runtime_environments (
             environment_ref, runtime, runtime_version, version_sort_key,
             kernel_version, state_format_name, state_format_version, upgrade_edges
         )
         VALUES ($1, 'firecracker', '1.17.0', '{1,17,0}', '6.1.155',
                 'firecracker-snapshot', '12.0.0', ARRAY[$2])"
    ))
    .bind(seed.from.to_string())
    .bind(seed.to.to_string())
    .execute(&h.db.pool)
    .await?;
    assert_eq!(read().await, json!(0));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn campaigns_list_a_page_at_a_time_and_filter_unfinished() -> TestResult {
    let Some(h) = driven().await? else {
        return Ok(());
    };
    let mut ids = Vec::new();
    for byte in 0..3 {
        ids.push(
            insert_campaign(
                &h.db.pool,
                CampaignSeed {
                    from: environment(10 + byte),
                    phase: if byte == 1 {
                        CampaignPhase::Aborted
                    } else {
                        CampaignPhase::Rolling
                    },
                    ..CampaignSeed::default()
                },
            )
            .await?,
        );
    }
    let ids_of = |body: &Value| -> Vec<String> {
        body["campaigns"]
            .as_array()
            .unwrap()
            .iter()
            .map(|campaign| campaign["id"].as_str().unwrap().to_owned())
            .collect()
    };
    let (_, all) = h
        .as_operator(Method::GET, "/runtime-upgrade-campaigns", &[], None)
        .await;
    assert_eq!(
        ids_of(&all),
        ids.iter().map(ToString::to_string).collect::<Vec<_>>()
    );
    let (_, unfinished) = h
        .as_operator(
            Method::GET,
            "/runtime-upgrade-campaigns?unfinished=true",
            &[],
            None,
        )
        .await;
    assert_eq!(
        ids_of(&unfinished),
        [ids[0].to_string(), ids[2].to_string()]
    );
    let (_, after) = h
        .as_operator(
            Method::GET,
            &format!("/runtime-upgrade-campaigns?after={}", ids[0]),
            &[],
            None,
        )
        .await;
    assert_eq!(ids_of(&after), [ids[1].to_string(), ids[2].to_string()]);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn only_managing_organization_admins_reach_the_campaigns() -> TestResult {
    let Some(h) = driven().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(&h.db.pool, CampaignSeed::default()).await?;
    h.assert_refused_to_non_operators(Method::GET, "/runtime-upgrade-campaigns", None)
        .await;
    h.assert_refused_to_non_operators(
        Method::POST,
        "/runtime-upgrade-campaigns",
        Some(start_body("templates", 1)),
    )
    .await;
    h.assert_refused_to_non_operators(
        Method::GET,
        &format!("/runtime-upgrade-campaigns/{campaign}"),
        None,
    )
    .await;
    h.assert_refused_to_non_operators(
        Method::POST,
        &format!("/runtime-upgrade-campaigns/{campaign}:pause"),
        Some(json!({"reason": "r"})),
    )
    .await;
    assert_eq!(
        store::get_record(&h.db.pool, campaign)
            .await?
            .unwrap()
            .campaign
            .phase,
        CampaignPhase::Rolling
    );
    assert!(
        store::list_records(&h.db.pool, false, None, 10)
            .await?
            .len()
            == 1
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn an_abort_ends_a_running_campaign_and_frees_its_source() -> TestResult {
    let Some(h) = driven().await? else {
        return Ok(());
    };
    let (status, body) = start(&h, start_body("sandboxes", 1)).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let campaign = CampaignId(body["id"].as_str().unwrap().parse()?);
    // One item in flight under a live lease, one done.
    sqlx::query(sql!(
        "INSERT INTO runtime_upgrade_items (
             campaign_id, subject_kind, subject_id, phase, attempts, lease_token,
             lease_expires_at
         )
         VALUES ($1, 'sandbox', $2, 'in_flight', 1, $3, now() + interval '10 minutes')"
    ))
    .bind(campaign)
    .bind(uuid::Uuid::now_v7())
    .bind(uuid::Uuid::now_v7())
    .execute(&h.db.pool)
    .await?;
    items(&h, campaign, SubjectKind::Sandbox, ItemPhase::Done, 1).await?;

    let (status, body) = action(&h, campaign, "abort", Some("0"), json!({"reason": "0-day"})).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body,
        json!({"id": campaign.to_string(), "phase": "aborted", "generation": 1, "retried": 0, "draining": 1})
    );
    let (_, read) = h
        .as_operator(
            Method::GET,
            &format!("/runtime-upgrade-campaigns/{campaign}"),
            &[],
            None,
        )
        .await;
    assert_eq!(read["phase"], "aborted");
    assert_ne!(read["finished_at"], Value::Null);
    assert_eq!(
        read["items"],
        json!({"pending": 0, "in_flight": 1, "done": 1, "failed": 0, "skipped": 0}),
        "the in-flight item drains under its lease"
    );

    // A new start for the same source succeeds at once.
    let (status, body) = start(&h, start_body("sandboxes", 1)).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    // And the aborted one stays aborted.
    let (status, body) = action(&h, campaign, "abort", None, json!({"reason": "again"})).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_error_contains(&body, "final");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn an_abort_of_a_paused_campaign_succeeds_and_a_stale_one_answers_412() -> TestResult {
    let Some(h) = driven().await? else {
        return Ok(());
    };
    let campaign = insert_campaign(&h.db.pool, CampaignSeed::default()).await?;
    let reason = json!({"reason": "stop it"});
    let (status, _) = action(&h, campaign, "pause", None, reason.clone()).await;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = action(&h, campaign, "abort", Some("0"), reason.clone()).await;
    assert_eq!(status, StatusCode::PRECONDITION_FAILED, "{body}");
    assert_eq!(
        store::get_record(&h.db.pool, campaign)
            .await?
            .unwrap()
            .campaign
            .phase,
        CampaignPhase::Paused,
        "a stale abort changes nothing"
    );
    let (status, _) = action(&h, campaign, "abort", None, json!({"reason": ""})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "a reason is required");

    let (status, body) = action(&h, campaign, "abort", Some("1"), reason).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["phase"], "aborted");
    assert_eq!(body["generation"], 2);
    let record = store::get_record(&h.db.pool, campaign).await?.unwrap();
    assert_eq!(record.updated_by, Some(h.operator.id));
    Ok(())
}
