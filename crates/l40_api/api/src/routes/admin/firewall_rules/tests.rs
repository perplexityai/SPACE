//! Route tests against Postgres, through the admin router's real auth and
//! audit middleware, and unit tests of the responses the store can't give in
//! production. The decoder's rejection tables (protocol crate) and the store's
//! start tests cover the rest: unsupported schemas, rules, and writing nothing
//! on a refusal.

use axum::http::Method;
use pplx_space_accounts::NodeId;
use pplx_space_firewall_rules_model::{FirewallRuleRevision, diff};
use pplx_space_firewall_rules_store::RolloutId;
use pplx_space_sql_locator::sql;
use serde_json::{Value, json};
use uuid::Uuid;

use super::*;
use crate::routes::admin::test_fixtures::{
    AdminRouterHarness, TestResult, admin_router_harness, assert_error_contains,
};

const EMPTY: &str = r#"{"schema_version":1,"rules":[]}"#;

/// A push body with these fields spliced in as written.
fn push(expected: &str, revision: &str) -> String {
    format!(r#"{{"expected_adopted_revision_id":{expected},"reason":"r","revision":{revision}}}"#)
}

async fn post(h: &AdminRouterHarness, body: String) -> (StatusCode, Value) {
    h.as_operator_raw(Method::POST, "/firewall-rollouts", &body)
        .await
}

/// The stale-precondition body for `adopted`, with its id or an explicit null.
fn stale(adopted: AdoptedRevision) -> Value {
    let id = match adopted {
        AdoptedRevision::NoRevision => None,
        AdoptedRevision::Revision(RevisionId(id)) => Some(id),
    };
    json!({
        "error": format!(
            "{}; read GET /firewall-rules and review the candidate again",
            StartError::Stale { adopted }
        ),
        "adopted_revision_id": id,
    })
}

/// Store an empty revision and adopt it.
async fn adopt_empty(h: &AdminRouterHarness) -> TestResult<Uuid> {
    let id = Uuid::now_v7();
    sqlx::query(sql!(
        "WITH revision AS (
             INSERT INTO firewall_rules_revisions (id, ruleset_format_version, ruleset)
             VALUES ($1, 1, '[]') RETURNING id)
         UPDATE firewall_rules_control
         SET enrollment_default_revision_id = (SELECT id FROM revision)"
    ))
    .bind(id)
    .execute(&h.db.pool)
    .await?;
    Ok(id)
}

/// Catches a route mounted outside the managing-organization admin gate.
#[tokio::test(flavor = "current_thread")]
async fn only_managing_organization_admins_reach_the_firewall_routes() -> TestResult {
    let Some(h) = admin_router_harness().await? else {
        return Ok(());
    };
    h.assert_refused_to_non_operators(Method::GET, "/firewall-rules", None)
        .await;
    let body = serde_json::from_str(&push("null", EMPTY))?;
    h.assert_refused_to_non_operators(Method::POST, "/firewall-rollouts", Some(body))
        .await;
    Ok(())
}

/// Catches an absent revision answered without explicit nulls, a document
/// embedded as anything but the revision object, and a store fault answered
/// as a first push or with its detail.
#[tokio::test(flavor = "current_thread")]
async fn the_preview_base_is_the_adopted_document_or_explicit_nulls() -> TestResult {
    let Some(h) = admin_router_harness().await? else {
        return Ok(());
    };
    let (status, body) = h
        .as_operator(Method::GET, "/firewall-rules", &[], None)
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body, json!({"adopted_revision_id": null, "revision": null}));

    let adopted = adopt_empty(&h).await?;
    let (status, body) = h
        .as_operator(Method::GET, "/firewall-rules", &[], None)
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body,
        json!({"adopted_revision_id": adopted, "revision": {"schema_version": 1, "rules": []}})
    );

    sqlx::query(sql!("DELETE FROM firewall_rules_control"))
        .execute(&h.db.pool)
        .await?;
    let (status, body) = h
        .as_operator(Method::GET, "/firewall-rules", &[], None)
        .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{body}");
    assert_eq!(body, json!({"error": "Internal server error"}));
    Ok(())
}

/// Catches a precondition that may be omitted or repeated, a candidate
/// reparsed before the strict decoder (dropping its duplicate key), a body
/// that may name more than the request, and a bad reason reported as
/// disabled.
#[tokio::test(flavor = "current_thread")]
async fn an_ambiguous_push_is_a_bad_request() -> TestResult {
    let Some(h) = admin_router_harness().await? else {
        return Ok(());
    };
    let a = Uuid::now_v7();
    for (body, needle) in [
        (
            format!(r#"{{"reason":"r","revision":{EMPTY}}}"#),
            "missing field `expected_adopted_revision_id`",
        ),
        (
            push(r#""not-a-uuid""#, EMPTY),
            "invalid firewall rollout body",
        ),
        (
            format!(
                r#"{{"expected_adopted_revision_id":null,"expected_adopted_revision_id":"{a}","reason":"r","revision":{EMPTY}}}"#
            ),
            "duplicate field `expected_adopted_revision_id`",
        ),
        (
            push("null", r#"{"schema_version":1,"rules":[],"rules":[]}"#),
            "duplicate field `rules`",
        ),
        (
            format!(
                r#"{{"expected_adopted_revision_id":null,"reason":"r","revision":{EMPTY},"actor":"{a}"}}"#
            ),
            "unknown field `actor`",
        ),
        (
            format!(r#"{{"expected_adopted_revision_id":null,"reason":"","revision":{EMPTY}}}"#),
            "the reason must be 1 to 1024 characters",
        ),
    ] {
        let (status, response) = post(&h, body.clone()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}: {response}");
        assert_error_contains(&response, needle);
    }
    Ok(())
}

/// Catches a valid push that doesn't reach the store's production gate, a
/// precondition checked after it, and a conflict that loses the adopted
/// revision (or its absence) or the rollout in progress.
#[tokio::test(flavor = "current_thread")]
async fn a_valid_push_reaches_the_store_and_keeps_its_refusals() -> TestResult {
    let Some(h) = admin_router_harness().await? else {
        return Ok(());
    };
    let (status, body) = post(&h, push("null", EMPTY)).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body, json!({"error": StartError::NotEnabled.to_string()}));

    let unknown = Uuid::now_v7();
    let (status, body) = post(&h, push(&format!(r#""{unknown}""#), EMPTY)).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body, stale(AdoptedRevision::NoRevision));

    let adopted = adopt_empty(&h).await?;
    let (status, body) = post(&h, push("null", EMPTY)).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body, stale(AdoptedRevision::Revision(RevisionId(adopted))));

    let rollout = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO firewall_rules_rollouts (id, to_revision_id, phase, unfinished_slot, reason)
         VALUES ($1, $2, 'canary', 1, 'seeded')"
    ))
    .bind(rollout)
    .bind(adopted)
    .execute(&h.db.pool)
    .await?;
    let (status, body) = post(&h, push(&format!(r#""{adopted}""#), EMPTY)).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(
        body,
        json!({
            "error": StartError::RolloutInProgress(RolloutId(rollout)).to_string(),
            "rollout_id": rollout,
        })
    );
    Ok(())
}

/// A revision of rules each dropping one TCP destination port.
fn revision(rules: &[(&str, u16)]) -> FirewallRuleRevision {
    let rules: Vec<_> = rules
        .iter()
        .map(|(id, port)| {
            json!({
                "id": id,
                "all_of": [{"left": {"payload": {"protocol": "tcp", "field": "dport"}}, "op": "==", "right": port}]
            })
        })
        .collect();
    decode_revision(&json!({"schema_version": 1, "rules": rules}).to_string())
        .expect("a valid revision")
}

/// Catches a first push given a base, a change reported under the wrong
/// kind or rule, and a reorder-only review reported as no change.
#[test]
fn a_started_rollout_keeps_its_review() -> TestResult {
    let started = |review| {
        serde_json::to_value(started_response(StartedRollout {
            revision_id: RevisionId(Uuid::from_u128(1)),
            rollout_id: RolloutId(Uuid::from_u128(2)),
            generation: 0,
            review,
        }))
    };
    let base_id = RevisionId(Uuid::from_u128(3));
    assert_eq!(
        started(Review::FirstPush)?,
        json!({
            "revision_id": Uuid::from_u128(1),
            "rollout_id": Uuid::from_u128(2),
            "generation": 0,
            "review": {"kind": "first_push"},
        })
    );
    let base = revision(&[("dns", 53), ("web", 80), ("ssh", 22)]);
    let candidate = revision(&[("dns", 53), ("web", 8080), ("new", 443)]);
    let review = started(Review::Against {
        base: base_id,
        diff: diff(&base, &candidate),
    })?;
    assert_eq!(
        review["review"],
        json!({
            "kind": "against",
            "base_revision_id": base_id.0,
            "changes": [
                {"change": "replaced", "rule_id": "web"},
                {"change": "removed", "rule_id": "ssh"},
                {"change": "added", "rule_id": "new"},
            ],
            "reordered": false,
        })
    );
    let review = started(Review::Against {
        base: base_id,
        diff: diff(
            &revision(&[("dns", 53), ("web", 80)]),
            &revision(&[("web", 80), ("dns", 53)]),
        ),
    })?;
    assert_eq!(review["review"]["changes"], json!([]));
    assert_eq!(review["review"]["reordered"], true);
    Ok(())
}

/// Catches the refusals production can't reach yet sharing a message or
/// status.
#[tokio::test(flavor = "current_thread")]
async fn unreachable_refusals_stay_distinct() -> TestResult {
    async fn answer(error: StartError) -> TestResult<(StatusCode, Value)> {
        let response = start_error(error);
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
        Ok((status, serde_json::from_slice(&bytes)?))
    }
    let node = NodeId::new();
    for error in [StartError::SlotTaken, StartError::Unqualified(node)] {
        let message = error.to_string();
        assert_eq!(
            answer(error).await?,
            (StatusCode::CONFLICT, json!({"error": message}))
        );
    }
    Ok(())
}
