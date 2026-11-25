use super::*;
use pplx_space_rpc_auth::test_support::{
    pod_bound_service_account_status, projected_token, service_account_status,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

/// A pod's reviewed node binding, rather than its transport address, determines
/// which placed private templates it can pull. Cached reviews retain the binding.
#[tokio::test]
async fn pod_template_reads_follow_reviewed_node_identity() -> TestResult {
    let fixture = cluster_fixture().await?.expect("Postgres required");
    let pool = &fixture.service.pool;
    let owner = SEEDED_ORG_ID;
    let (_, hash) = seed_private_release(&fixture, "placed-template", owner).await?;
    let (_, peer_hash) = seed_private_release(&fixture, "peer-template", owner).await?;
    let node_uid = Uuid::now_v7().to_string();
    for (name, address, template) in [
        ("catalog-worker", "192.0.2.10", "placed-template"),
        ("transport-worker", "127.0.0.1", "peer-template"),
    ] {
        let node = insert_k8s_node(pool, address.parse()?, "worker").await?;
        sqlx::query(sql!(
            "INSERT INTO node_k8s (node_id, k8s_node_name, k8s_node_uid) VALUES ($1, $2, $3)"
        ))
        .bind(node)
        .bind(name)
        .bind(&node_uid)
        .execute(pool)
        .await?;
        sqlx::query(sql!(
            "INSERT INTO sandboxes (id, user_id, org_id, managed_by, lifecycle,
                placement, target_node_id, network_policy_generation, template_id)
             VALUES ($1, $2, $3, 'k8s', 'started', 'on_node', $4, 1, $5)"
        ))
        .bind(Uuid::now_v7())
        .bind(SEEDED_USER_ID)
        .bind(owner)
        .bind(node)
        .bind(template)
        .execute(pool)
        .await?;
    }

    let tokens = [
        "bound",
        "stale",
        "unbound",
        "missing-node",
        "missing-uid",
        "malformed",
        "missing-and-malformed",
    ]
    .map(projected_token);
    let reviewed_tokens = tokens.clone();
    let reviews = Arc::new(AtomicUsize::new(0));
    let review_count = reviews.clone();
    let mut harness = serve_reviewed(fixture.service, "workers", None, None, move |spec| {
        review_count.fetch_add(1, Ordering::SeqCst);
        let index = reviewed_tokens
            .iter()
            .position(|token| Some(token) == spec.token.as_ref())
            .unwrap();
        let username = "system:serviceaccount:space:workers";
        if index == 2 {
            service_account_status("space-cluster-api", username)
        } else {
            let uid = if index == 0 {
                node_uid.as_str()
            } else {
                "stale-node-uid"
            };
            let mut status = pod_bound_service_account_status(
                "space-cluster-api",
                username,
                "catalog-worker",
                uid,
            );
            let extra = status.user.as_mut().unwrap().extra.as_mut().unwrap();
            match index {
                3 => {
                    extra.remove("authentication.kubernetes.io/node-name");
                    extra.remove("authentication.kubernetes.io/node-uid");
                }
                4 => {
                    extra.remove("authentication.kubernetes.io/node-uid");
                }
                5 => {
                    extra.insert(
                        "authentication.kubernetes.io/node-uid".into(),
                        vec![uid.into(), uid.into()],
                    );
                }
                6 => {
                    extra.remove("authentication.kubernetes.io/node-uid");
                    extra.insert(
                        "authentication.kubernetes.io/pod-name".into(),
                        vec!["invalid/name".into()],
                    );
                }
                _ => {}
            }
            status
        }
    })
    .await?;
    let client = &mut harness.client;
    for (token, expected) in [
        (tokens[0].as_str(), [true, false]),
        (tokens[1].as_str(), [false, false]),
        (tokens[2].as_str(), [false, true]),
        (tokens[3].as_str(), [false, true]),
        (tokens[4].as_str(), [false, true]),
        (tokens[5].as_str(), [false, false]),
        (tokens[6].as_str(), [false, false]),
        (REVIEWED_STATIC_TOKEN, [false, true]),
    ] {
        for _ in 0..2 {
            for ((template, hash), expected) in
                [("placed-template", &hash), ("peer-template", &peer_hash)]
                    .into_iter()
                    .zip(expected)
            {
                let mut request = Request::new(FindTemplateReleaseByHashRequest {
                    template_name: template.into(),
                    hash: hash.clone(),
                    ..Default::default()
                });
                request
                    .metadata_mut()
                    .insert("authorization", format!("Bearer {token}").parse()?);
                let actual = client
                    .find_template_release_by_hash(request)
                    .await
                    .map(|_| Code::Ok)
                    .unwrap_or_else(|status| status.code());
                let expected = if expected { Code::Ok } else { Code::NotFound };
                assert_eq!(actual, expected, "{template}");
            }
        }
    }
    assert_eq!(
        reviews.load(Ordering::SeqCst),
        tokens.len(),
        "each projected token is reviewed once"
    );
    drop(harness);
    Ok(())
}
