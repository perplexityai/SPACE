//! `FetchFirewallRules` through the real auth layer and a TokenReview double.
//! The store's tests cover which parts are present and how they decode.

use super::*;
use crate::firewall_rules::Enrollment;
use crate::proto::{
    FetchFirewallRulesRequest, FetchFirewallRulesResponse, FirewallRulesAssignment,
    FirewallRulesPendingRound, cluster_api_client::ClusterApiClient,
};
use pplx_space_rpc_auth::test_support::{
    TokenReviewStatus, pod_bound_service_account_status, projected_token, service_account_status,
};
use tonic::transport::Channel;

const AUDIENCE: &str = "space-cluster-api";
const NODE_CTL: &str = "system:serviceaccount:space:space-node-ctl";

/// Two rules out of name order, the first with two comparisons, in canonical
/// form, which jsonb's own text isn't.
const ORDERED: &str = concat!(
    r#"[{"id":"web","all_of":["#,
    r#"{"left":{"payload":{"protocol":"tcp","field":"sport"}},"op":"==","right":1024},"#,
    r#"{"left":{"payload":{"protocol":"tcp","field":"dport"}},"op":"==","right":80}]},"#,
    r#"{"id":"dns","all_of":["#,
    r#"{"left":{"payload":{"protocol":"udp","field":"dport"}},"op":"==","right":53}]}]"#,
);
const SSH: &str = r#"[{"id":"ssh","all_of":[{"left":{"payload":{"protocol":"tcp","field":"dport"}},"op":"==","right":22}]}]"#;

/// Serve `service`, reviewing each token as its status. With `grant`,
/// node-ctl holds the fetch authority production withholds.
async fn serve(
    service: ClusterApiService,
    reviews: Vec<(String, TokenReviewStatus)>,
    grant: bool,
) -> TestResult<ReviewedClusterApi> {
    serve_reviewed(
        service,
        "space-node-ctl,space-template-manager",
        grant.then_some("space-node-ctl"),
        move |spec| {
            reviews
                .iter()
                .find(|(token, _)| Some(token) == spec.token.as_ref())
                .map(|(_, status)| status.clone())
                .unwrap()
        },
    )
    .await
}

/// Every PERMISSION_DENIED reason looks the same to the caller.
fn denied<T>() -> Result<T, (Code, String)> {
    Err((
        Code::PermissionDenied,
        "the caller may not fetch firewall rules".to_owned(),
    ))
}

/// Fetch with `bearer`, or with no authorization header.
async fn fetch(
    client: &mut ClusterApiClient<Channel>,
    bearer: Option<&str>,
) -> TestResult<Result<FetchFirewallRulesResponse, (Code, String)>> {
    let mut request = Request::new(FetchFirewallRulesRequest {});
    if let Some(bearer) = bearer {
        request
            .metadata_mut()
            .insert("authorization", format!("Bearer {bearer}").parse()?);
    }
    Ok(client
        .fetch_firewall_rules(request)
        .await
        .map(tonic::Response::into_inner)
        .map_err(|status| (status.code(), status.message().to_owned())))
}

/// node-ctl's token, pod-bound to `name` and `uid`.
fn node_ctl_on(name: &str, uid: &str) -> TokenReviewStatus {
    pod_bound_service_account_status(AUDIENCE, NODE_CTL, name, uid)
}

/// A Kubernetes registration with its inventory name and UID.
async fn register(
    pool: &sqlx::PgPool,
    role: &str,
    state: &str,
    address: &str,
    name: &str,
    uid: &str,
) -> TestResult<NodeId> {
    let node = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, $2::INET, $3, 'k8s', $4)"
    ))
    .bind(node)
    .bind(address)
    .bind(role)
    .bind(state)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name, k8s_node_uid) VALUES ($1, $2, $3)"
    ))
    .bind(node)
    .bind(name)
    .bind(uid)
    .execute(pool)
    .await?;
    Ok(node)
}

async fn insert_revision(pool: &sqlx::PgPool, rules: &str) -> TestResult<Uuid> {
    let id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO firewall_rules_revisions (id, ruleset_format_version, ruleset)
         VALUES ($1, 1, $2::jsonb)"
    ))
    .bind(id)
    .bind(rules)
    .execute(pool)
    .await?;
    Ok(id)
}

/// Give `node` an assignment to `revision` and a pending round, returning
/// the response that carries them.
async fn give_work(
    pool: &sqlx::PgPool,
    node: NodeId,
    generation: u64,
    (revision, rules): (Uuid, &str),
    round: u64,
) -> TestResult<FetchFirewallRulesResponse> {
    let issued_at = time::OffsetDateTime::from_unix_timestamp_nanos(
        1_790_000_000_000_000_000 + i128::from(round) * 1_000_123_000,
    )?;
    sqlx::query(sql!(
        "INSERT INTO firewall_rules_node_assignments (node_id, assignment_generation, revision_id)
         VALUES ($1, $2, $3)"
    ))
    .bind(node)
    .bind(i64::try_from(generation)?)
    .bind(revision)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO firewall_rules_node_observations (
             node_id, manager_epoch, pending_round, pending_round_issued_at
         )
         VALUES ($1, 1, $2, $3)"
    ))
    .bind(node)
    .bind(i64::try_from(round)?)
    .bind(issued_at)
    .execute(pool)
    .await?;
    Ok(response(
        node,
        generation,
        (revision, rules),
        round,
        issued_at,
    ))
}

fn response(
    node: NodeId,
    generation: u64,
    (revision, rules): (Uuid, &str),
    round: u64,
    issued_at: time::OffsetDateTime,
) -> FetchFirewallRulesResponse {
    FetchFirewallRulesResponse {
        node_id: node.to_string(),
        assignment: Some(FirewallRulesAssignment {
            generation,
            revision_id: revision.to_string(),
            revision: format!(r#"{{"schema_version":1,"rules":{rules}}}"#),
        }),
        pending_round: Some(FirewallRulesPendingRound {
            round,
            issued_at: Some(std::time::SystemTime::from(issued_at).into()),
        }),
    }
}

/// Only node-ctl, bound by reviewed name and UID to exactly one live,
/// enrolled worker, learns that worker's instructions, and never by its
/// address: the server's peer address is worker B's. A revision that doesn't
/// decode is an error, not no work.
#[tokio::test(flavor = "current_thread")]
async fn fetch_serves_only_node_ctl_on_its_own_enrolled_worker() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let mut service = fixture.service;
    let pool = service.pool.clone();
    let pool = &pool;
    let ordered = insert_revision(pool, ORDERED).await?;
    let ssh = insert_revision(pool, SSH).await?;

    let a = register(pool, "worker", "ready", "192.0.2.10", "worker-a", "uid-a").await?;
    let b = register(pool, "worker", "ready", "127.0.0.1", "worker-b", "uid-b").await?;
    let u = register(pool, "worker", "ready", "192.0.2.11", "worker-u", "uid-u").await?;
    let builder = register(pool, "builder", "ready", "192.0.2.12", "builder-x", "uid-x").await?;
    let deleted = register(pool, "worker", "deleted", "192.0.2.13", "worker-d", "uid-d").await?;
    let twin = register(pool, "worker", "ready", "192.0.2.14", "worker-t", "uid-t").await?;
    let twin_peer = register(pool, "worker", "ready", "192.0.2.15", "worker-t", "uid-t2").await?;
    let unsynced = register(pool, "worker", "ready", "192.0.2.16", "worker-n", "uid-n").await?;
    sqlx::query(sql!(
        "UPDATE node_k8s SET k8s_node_uid = NULL WHERE node_id = $1"
    ))
    .bind(unsynced)
    .execute(pool)
    .await?;
    give_work(pool, b, 9, (ssh, SSH), 70).await?;
    service.firewall_enrollment =
        Enrollment::Fixture(vec![a, b, builder, deleted, twin, twin_peer]);

    let mut malformed = node_ctl_on("worker-a", "uid-a");
    malformed
        .user
        .as_mut()
        .and_then(|user| user.extra.as_mut())
        .unwrap()
        .insert(
            "authentication.kubernetes.io/node-uid".into(),
            vec!["uid-a".into(), "uid-a".into()],
        );
    // `Ok` is A's own instructions. Each token is minted once: its expiry
    // makes a second one differ.
    let other_workload = "system:serviceaccount:space:space-template-manager";
    let reviewed = [
        ("node-ctl-a", node_ctl_on("worker-a", "uid-a"), Ok(())),
        (
            "other-workload-a",
            pod_bound_service_account_status(AUDIENCE, other_workload, "worker-a", "uid-a"),
            denied(),
        ),
        ("stale-uid", node_ctl_on("worker-a", "stale-uid"), denied()),
        ("unsynced", node_ctl_on("worker-n", "uid-n"), denied()),
        (
            "no-claims",
            service_account_status(AUDIENCE, NODE_CTL),
            denied(),
        ),
        ("malformed", malformed, denied()),
        (
            "unenrolled",
            node_ctl_on("worker-u", "uid-u"),
            Err((
                Code::FailedPrecondition,
                format!("firewall rules aren't enabled for node {u}: it isn't enrolled"),
            )),
        ),
        ("builder", node_ctl_on("builder-x", "uid-x"), denied()),
        ("deleted", node_ctl_on("worker-d", "uid-d"), denied()),
        ("ambiguous", node_ctl_on("worker-t", "uid-t"), denied()),
    ]
    .map(|(label, status, expected)| (projected_token(label), status, expected));
    let node_ctl_a = reviewed[0].0.clone();
    let reviews = reviewed
        .iter()
        .map(|(token, status, _)| (token.clone(), status.clone()))
        .collect();
    let mut harness = serve(service, reviews, true).await?;
    let client = &mut harness.client;

    // A has no instructions yet; B, at the peer address, already has work.
    assert_eq!(
        fetch(client, Some(&node_ctl_a)).await?,
        Ok(FetchFirewallRulesResponse {
            node_id: a.to_string(),
            assignment: None,
            pending_round: None,
        })
    );
    let a_work = give_work(pool, a, 4, (ordered, ORDERED), 7).await?;

    let unreviewed = [
        (Some(REVIEWED_STATIC_TOKEN.to_owned()), denied()),
        (
            None,
            Err((
                Code::Unauthenticated,
                "missing authorization header".to_owned(),
            )),
        ),
    ];
    for (bearer, expected) in reviewed
        .into_iter()
        .map(|(token, _, expected)| (Some(token), expected))
        .chain(unreviewed)
    {
        let expected = expected.map(|()| a_work.clone());
        assert_eq!(
            fetch(client, bearer.as_deref()).await?,
            expected,
            "{bearer:?}"
        );
    }

    // Fetching again changes nothing: the round isn't consumed and no row is
    // touched.
    let touched = || {
        sqlx::query_scalar::<_, Vec<time::OffsetDateTime>>(sql!(
            "SELECT ARRAY[a.updated_at, o.updated_at]
             FROM firewall_rules_node_assignments a
             JOIN firewall_rules_node_observations o USING (node_id)
             WHERE node_id = $1"
        ))
        .bind(a)
        .fetch_one(pool)
    };
    let before = touched().await?;
    assert_eq!(fetch(client, Some(&node_ctl_a)).await?, Ok(a_work));
    assert_eq!(touched().await?, before);
    // A denied fetch didn't revive the deleted registration.
    let state = sqlx::query_scalar::<_, String>(sql!("SELECT state FROM nodes WHERE id = $1"))
        .bind(deleted)
        .fetch_one(pool)
        .await?;
    assert_eq!(state, "deleted");

    // Same authorized caller; an unreadable revision is an RPC error, not no work.
    let bad = insert_revision(pool, r#"[{"id":"r","all_of":[],"extra":1}]"#).await?;
    sqlx::query(sql!(
        "UPDATE firewall_rules_node_assignments SET revision_id = $2 WHERE node_id = $1"
    ))
    .bind(a)
    .bind(bad)
    .execute(pool)
    .await?;
    assert_eq!(
        fetch(client, Some(&node_ctl_a))
            .await?
            .map_err(|(code, _)| code),
        Err(Code::Internal)
    );
    Ok(())
}

/// Production's defaults refuse an otherwise valid caller: no node-ctl
/// principal holds the authority, and no worker is enrolled.
#[tokio::test(flavor = "current_thread")]
async fn fetch_is_closed_while_authority_or_enrollment_is_unapproved() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let pool = fixture.service.pool.clone();
    let a = register(&pool, "worker", "ready", "192.0.2.10", "worker-a", "uid-a").await?;

    for (grant, expected) in [
        (false, denied()),
        (
            true,
            Err((
                Code::FailedPrecondition,
                format!(
                    "firewall rules aren't enabled for node {a}: no enrollment predicate is approved"
                ),
            )),
        ),
    ] {
        let mut service = fixture.service.clone();
        if !grant {
            service.firewall_enrollment = Enrollment::Fixture(vec![a]);
        }
        let token = projected_token("node-ctl-a");
        let reviews = vec![(token.clone(), node_ctl_on("worker-a", "uid-a"))];
        let mut harness = serve(service, reviews, grant).await?;
        assert_eq!(
            fetch(&mut harness.client, Some(&token)).await?,
            expected,
            "grant {grant}"
        );
    }
    Ok(())
}

/// A Fetch overlapping a commit that moves the worker to the next (revision,
/// round) pair returns exactly the old pair: its first read fixes the snapshot
/// before the commit. The writer locks the observations, the assignments, then
/// the registrations, so each read in turn waits for the commit. Reading
/// either part, or the binding the last commit deletes, in a later snapshot
/// returns something else.
#[tokio::test(flavor = "current_thread")]
async fn fetch_returns_assignment_and_round_from_one_snapshot() -> TestResult {
    let Some(fixture) = cluster_fixture_with_max_connections(4).await? else {
        return Ok(());
    };
    let mut service = fixture.service;
    let pool = service.pool.clone();
    let pool = &pool;
    let a = register(pool, "worker", "ready", "192.0.2.10", "worker-a", "uid-a").await?;
    service.firewall_enrollment = Enrollment::Fixture(vec![a]);
    let revisions = [
        (insert_revision(pool, ORDERED).await?, ORDERED),
        (insert_revision(pool, SSH).await?, SSH),
    ];
    let mut old = give_work(pool, a, 1, revisions[0], 7).await?;
    let token = projected_token("node-ctl-a");
    let reviews = vec![(token.clone(), node_ctl_on("worker-a", "uid-a"))];
    let mut harness = serve(service, reviews, true).await?;

    for (step, revision) in [revisions[1], revisions[0], revisions[1]]
        .iter()
        .enumerate()
    {
        let delete = step == 2;
        let generation = u64::try_from(step)? + 2;
        let round = 8 + u64::try_from(step)?;
        let issued_at = time::OffsetDateTime::from_unix_timestamp_nanos(
            1_800_000_000_000_000_000 + i128::from(round) * 1_000_456_000,
        )?;
        let new = response(a, generation, *revision, round, issued_at);

        let mut writer = pool.begin().await?;
        sqlx::query(match step {
            0 => sql!("LOCK TABLE firewall_rules_node_observations IN ACCESS EXCLUSIVE MODE"),
            1 => sql!("LOCK TABLE firewall_rules_node_assignments IN ACCESS EXCLUSIVE MODE"),
            _ => sql!("LOCK TABLE nodes IN ACCESS EXCLUSIVE MODE"),
        })
        .execute(&mut *writer)
        .await?;
        let (mut client, bearer) = (harness.client.clone(), token.clone());
        let overlapping =
            tokio::spawn(async move { fetch(&mut client, Some(&bearer)).await.unwrap() });
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        while sqlx::query_scalar::<_, i64>(sql!(
            "SELECT count(*) FROM pg_stat_activity
             WHERE datname = current_database() AND wait_event_type = 'Lock'"
        ))
        .fetch_one(pool)
        .await?
            == 0
        {
            assert!(
                tokio::time::Instant::now() < deadline,
                "Fetch never blocked"
            );
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        sqlx::query(sql!(
            "UPDATE firewall_rules_node_assignments
             SET revision_id = $2, assignment_generation = $3
             WHERE node_id = $1"
        ))
        .bind(a)
        .bind(revision.0)
        .bind(i64::try_from(generation)?)
        .execute(&mut *writer)
        .await?;
        sqlx::query(sql!(
            "UPDATE firewall_rules_node_observations
             SET pending_round = $2, pending_round_issued_at = $3
             WHERE node_id = $1"
        ))
        .bind(a)
        .bind(i64::try_from(round)?)
        .bind(issued_at)
        .execute(&mut *writer)
        .await?;
        if delete {
            sqlx::query(sql!("UPDATE nodes SET state = 'deleted' WHERE id = $1"))
                .bind(a)
                .execute(&mut *writer)
                .await?;
        }
        writer.commit().await?;

        assert_eq!(overlapping.await?, Ok(old.clone()), "step {step}");
        let after = if delete { denied() } else { Ok(new.clone()) };
        assert_eq!(fetch(&mut harness.client, Some(&token)).await?, after);
        old = new;
    }
    Ok(())
}
