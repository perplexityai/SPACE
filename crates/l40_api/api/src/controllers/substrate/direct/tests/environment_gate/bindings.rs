//! Which binding gates a candidate: its own build's, the release's, or the holders' agreed one.

use super::*;

#[tokio::test(flavor = "current_thread")]
async fn a_pull_takes_the_builds_binding_when_its_release_records_none() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    // The holder's inventory says the build is bound to E1, but the holder
    // has no evidence; another node with evidence for E1 holds nothing.
    let holder = seed_node(
        &fixture.pool,
        "127.0.0.1:1",
        SnapshotSeed {
            environment_refs_secs_ago: None,
            ..SnapshotSeed::default()
        },
        &[],
        "9.0.0",
    )
    .await?;
    let puller = seed_node(
        &fixture.pool,
        "127.0.0.1:2",
        SnapshotSeed::default(),
        &[E1],
        "9.0.0",
    )
    .await?;
    inventory_templates(&fixture.pool, &BTreeSet::from([holder]), "9.0.0").await?;
    // A release recorded before bindings.
    sqlx::query(sql!(
        "UPDATE template_releases SET execution_metadata = NULL WHERE template_name = $1"
    ))
    .bind(BOUND_E1)
    .execute(&fixture.pool)
    .await?;

    assert_eq!(
        pull_create(&gated(&fixture, On), BOUND_E1).await?,
        BTreeSet::from([puller]),
        "the refused holder isn't handed the work as unbound"
    );
    assert_eq!(
        pull_create(&gated(&fixture, Off), BOUND_E1).await?,
        BTreeSet::from([holder, puller])
    );
    // A build nothing records a binding for is unbound.
    assert_eq!(
        pull_create(&gated(&fixture, On), UNBOUND).await?,
        BTreeSet::from([holder, puller])
    );

    // A build no node holds yet takes its release's binding.
    sqlx::query(sql!(
        "INSERT INTO templates (id, name, description, org_id)
         VALUES ($1, 'unheld', NULL, '00000000-0000-0000-0000-000000000001')"
    ))
    .bind(Uuid::now_v7())
    .execute(&fixture.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO template_releases (template_name, type, hash, execution_metadata)
         VALUES ('unheld', 'fc', $1, jsonb_build_object('environment', $2::TEXT))"
    ))
    .bind(HASH)
    .bind(E1)
    .execute(&fixture.pool)
    .await?;
    assert_eq!(
        pull_create(&gated(&fixture, On), "unheld").await?,
        BTreeSet::from([puller]),
        "the release's binding gates a build no node holds"
    );

    // An inventory row recorded under a legacy unprefixed hash binds a pull
    // that names the build's `sha256:` spelling.
    let unprefixed = "1111222233334444555566667777888899990000aaaabbbbccccddddeeeeffff";
    insert_template_inventory(&fixture.pool, holder, "legacy-spelled", unprefixed, "9.0.0").await?;
    sqlx::query(sql!(
        "UPDATE node_template_inventory
         SET execution_metadata = jsonb_build_object('environment', $2::TEXT)
         WHERE node_id = $1 AND template_name = 'legacy-spelled'"
    ))
    .bind(holder)
    .bind(E1)
    .execute(&fixture.pool)
    .await?;
    let pulled: BTreeSet<NodeId> = gated(&fixture, On)
        .pull_on_create_worker_candidates(
            "legacy-spelled",
            Some(&format!("sha256:{unprefixed}")),
            "fc",
        )
        .await?
        .into_iter()
        .map(|node| node.id)
        .collect();
    assert_eq!(pulled, BTreeSet::from([puller]));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_candidates_own_report_doesnt_unbind_a_bound_build() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    // Both nodes hold the E1 build but report it without its binding; the
    // release records it. Only one node has evidence for E1.
    let unproven = seed_node(
        &fixture.pool,
        "127.0.0.1:1",
        SnapshotSeed {
            environment_refs_secs_ago: None,
            ..SnapshotSeed::default()
        },
        &[],
        "9.0.0",
    )
    .await?;
    let good = seed_node(
        &fixture.pool,
        "127.0.0.1:2",
        SnapshotSeed::default(),
        &[E1],
        "9.0.0",
    )
    .await?;
    inventory_templates(&fixture.pool, &BTreeSet::from([unproven, good]), "9.0.0").await?;
    sqlx::query(sql!(
        "UPDATE node_template_inventory SET execution_metadata = NULL WHERE template_name = $1"
    ))
    .bind(BOUND_E1)
    .execute(&fixture.pool)
    .await?;

    let on = gated(&fixture, On);
    assert_eq!(
        cached_create(&on, BOUND_E1, CreateBootMode::Cold).await?,
        BTreeSet::from([good]),
        "the build's binding gates every candidate"
    );
    assert_eq!(pooled(&on, BOUND_E1).await?, BTreeSet::from([good]));
    assert_eq!(
        pull_create(&on, BOUND_E1).await?,
        BTreeSet::from([good]),
        "a pull falls back to the release's binding for a holder's row too"
    );
    assert_eq!(
        cached_create(&gated(&fixture, Off), BOUND_E1, CreateBootMode::Cold).await?,
        BTreeSet::from([unproven, good])
    );
    let shadow = gated(&fixture, Shadow);
    cached_create(&shadow, BOUND_E1, CreateBootMode::Cold).await?;
    assert_eq!(
        counted(&shadow),
        Refusals::from([(("cached_create".to_string(), "no_evidence".to_string()), 1)]),
        "shadow classifies the unproven node"
    );

    // With only the unproven node holding the build, the last-known fallback
    // runs too, and the build's binding gates it as well.
    sqlx::query(sql!(
        "DELETE FROM node_template_inventory WHERE node_id = $1"
    ))
    .bind(good)
    .execute(&fixture.pool)
    .await?;
    assert_eq!(
        cached_create(&on, BOUND_E1, CreateBootMode::Cold).await?,
        BTreeSet::new()
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn without_a_release_binding_each_candidate_is_gated_by_its_own_build() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    // One rootfs hash, cached bound to E0 on one node and to E1 on another,
    // each node with evidence for its own environment only, and no release
    // binding. The E1 report is the newer one.
    let on_e0 = seed_node(
        &fixture.pool,
        "127.0.0.1:1",
        SnapshotSeed::default(),
        &[E0],
        "9.0.0",
    )
    .await?;
    let on_e1 = seed_node(
        &fixture.pool,
        "127.0.0.1:2",
        SnapshotSeed::default(),
        &[E1],
        "9.0.0",
    )
    .await?;
    let split = "split";
    inventory(&fixture.pool, on_e0, split, Some(E0), "9.0.0").await?;
    inventory(&fixture.pool, on_e1, split, Some(E1), "9.0.0").await?;
    sqlx::query(sql!(
        "UPDATE node_template_inventory SET observed_at = NOW() + INTERVAL '1 minute'
         WHERE node_id = $1"
    ))
    .bind(on_e1)
    .execute(&fixture.pool)
    .await?;
    let both = BTreeSet::from([on_e0, on_e1]);

    for gate in EnvironmentPlacementGate::ALL {
        let substrate = gated(&fixture, gate);
        assert_eq!(
            cached_create(&substrate, split, CreateBootMode::Cold).await?,
            both,
            "{gate}: each node's own binding gates it, and its evidence lists it"
        );
        assert_eq!(pooled(&substrate, split).await?, both, "{gate}: pool");
        assert_eq!(
            pull_create(&substrate, split).await?,
            both,
            "{gate}: holders disagree, so a pull imposes neither binding"
        );
        assert_eq!(counted(&substrate), Refusals::new(), "{gate}");
    }

    // A node with evidence for E0 only, caching the E1-bound build, is
    // refused for it.
    sqlx::query(sql!(
        "UPDATE node_status_snapshots SET usable_environment_refs = ARRAY[$2],
             known_environment_refs = ARRAY[$2]
         WHERE node_id = $1"
    ))
    .bind(on_e1)
    .bind(E0)
    .execute(&fixture.pool)
    .await?;
    assert_eq!(
        cached_create(&gated(&fixture, On), split, CreateBootMode::Cold).await?,
        BTreeSet::from([on_e0])
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn without_a_release_binding_a_holder_whose_row_names_none_is_unbound() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    // No release binding. One node holds the build bound to E1 and has
    // evidence for it; the other holds it unbound and has evidence for E0
    // only.
    let bound = seed_node(
        &fixture.pool,
        "127.0.0.1:1",
        SnapshotSeed::default(),
        &[E1],
        "9.0.0",
    )
    .await?;
    let unbound = seed_node(
        &fixture.pool,
        "127.0.0.1:2",
        SnapshotSeed::default(),
        &[E0],
        "9.0.0",
    )
    .await?;
    let template = "mixed";
    inventory(&fixture.pool, bound, template, Some(E1), "9.0.0").await?;
    inventory(&fixture.pool, unbound, template, None, "9.0.0").await?;

    let on = gated(&fixture, On);
    assert_eq!(
        cached_create(&on, template, CreateBootMode::Cold).await?,
        BTreeSet::from([bound, unbound]),
        "the other holder's binding isn't imposed on the unbound build"
    );
    assert_eq!(
        pooled(&on, template).await?,
        BTreeSet::from([bound, unbound])
    );
    assert_eq!(
        pull_create(&on, template).await?,
        BTreeSet::from([bound, unbound]),
        "a pull gates a holder by the build it holds, as a create does"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_candidate_is_gated_by_the_build_it_holds_not_the_releases() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    // The release binds the build to E1, but both nodes hold that digest
    // bound to E0, which is what a create with no environment preference
    // runs there. One node has evidence for E0, the other for E1 only.
    let runs_e0 = seed_node(
        &fixture.pool,
        "127.0.0.1:1",
        SnapshotSeed::default(),
        &[E0],
        "9.0.0",
    )
    .await?;
    let lists_e1 = seed_node(
        &fixture.pool,
        "127.0.0.1:2",
        SnapshotSeed::default(),
        &[E1],
        "9.0.0",
    )
    .await?;
    for node in [runs_e0, lists_e1] {
        inventory(&fixture.pool, node, BOUND_E1, Some(E0), "9.0.0").await?;
    }
    sqlx::query(sql!(
        "INSERT INTO template_releases (template_name, type, hash, execution_metadata)
         VALUES ($1, 'fc', $2, jsonb_build_object('environment', $3::TEXT))"
    ))
    .bind(BOUND_E1)
    .bind(HASH)
    .bind(E1)
    .execute(&fixture.pool)
    .await?;

    let on = gated(&fixture, On);
    assert_eq!(
        cached_create(&on, BOUND_E1, CreateBootMode::Cold).await?,
        BTreeSet::from([runs_e0]),
        "each node is checked for the E0 build it would run"
    );
    assert_eq!(pooled(&on, BOUND_E1).await?, BTreeSet::from([runs_e0]));
    let shadow = gated(&fixture, Shadow);
    cached_create(&shadow, BOUND_E1, CreateBootMode::Cold).await?;
    assert_eq!(
        counted(&shadow),
        Refusals::from([(
            (
                "cached_create".to_string(),
                "environment_not_usable".to_string()
            ),
            1
        )])
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_pull_doesnt_hand_a_refused_holder_its_own_build() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    // No release binding, and the holders disagree: one holds the digest
    // bound to E0 but has evidence for E1 only, the other bound to E1 with
    // evidence for E0 only. A third node holds nothing.
    let holds_e0 = seed_node(
        &fixture.pool,
        "127.0.0.1:1",
        SnapshotSeed::default(),
        &[E1],
        "9.0.0",
    )
    .await?;
    let holds_e1 = seed_node(
        &fixture.pool,
        "127.0.0.1:2",
        SnapshotSeed::default(),
        &[E0],
        "9.0.0",
    )
    .await?;
    let empty = seed_node(
        &fixture.pool,
        "127.0.0.1:3",
        SnapshotSeed::default(),
        &[E1],
        "9.0.0",
    )
    .await?;
    let template = "disputed";
    inventory(&fixture.pool, holds_e0, template, Some(E0), "9.0.0").await?;
    inventory(&fixture.pool, holds_e1, template, Some(E1), "9.0.0").await?;

    assert!(
        gated(&fixture, On)
            .create_candidates_from_cache(template, Some(HASH), "fc", CreateBootMode::Cold)
            .await?
            .is_empty(),
        "on refuses both holders, a miss"
    );
    assert_eq!(
        pull_create(&gated(&fixture, On), template).await?,
        BTreeSet::from([empty]),
        "each holder is refused again for the build it holds"
    );
    for gate in [Off, Shadow] {
        assert_eq!(
            pull_create(&gated(&fixture, gate), template).await?,
            BTreeSet::from([holds_e0, holds_e1, empty]),
            "{gate}"
        );
    }
    let shadow = gated(&fixture, Shadow);
    pull_create(&shadow, template).await?;
    assert_eq!(
        counted(&shadow),
        Refusals::from([(
            (
                "pull_create".to_string(),
                "environment_not_usable".to_string()
            ),
            2
        )])
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_pull_gates_a_holder_by_its_own_row_of_the_digest() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    // No release binding, and the holders disagree, so only each node's own
    // row of the digest binds it. Every node has evidence for E1 only.
    // - `legacy`: holds the digest bound to E0 under its legacy unprefixed
    //   spelling.
    // - `both`: the same, and a `sha256:` row with no binding too.
    // - `bound_e1`: holds it bound to E1 under the `sha256:` spelling.
    // - `other_digest`: holds another digest of the template bound to E0.
    // - `two_bound`: holds the digest bound to E0 under the `sha256:`
    //   spelling and to E1 under the legacy one; the exact spelling wins.
    let unprefixed = "1111222233334444555566667777888899990000aaaabbbbccccddddeeeeffff";
    let prefixed = format!("sha256:{unprefixed}");
    let mut nodes = Vec::new();
    for endpoint in [
        "127.0.0.1:1",
        "127.0.0.1:2",
        "127.0.0.1:3",
        "127.0.0.1:4",
        "127.0.0.1:5",
    ] {
        nodes.push(
            seed_node(
                &fixture.pool,
                endpoint,
                SnapshotSeed::default(),
                &[E1],
                "9.0.0",
            )
            .await?,
        );
    }
    let [legacy, both, bound_e1, other_digest, two_bound] = nodes[..] else {
        unreachable!()
    };
    let rows: [(NodeId, &str, Option<&str>); 7] = [
        (two_bound, unprefixed, Some(E1)),
        (two_bound, &prefixed, Some(E0)),
        (both, &prefixed, None),
        (legacy, unprefixed, Some(E0)),
        (both, unprefixed, Some(E0)),
        (bound_e1, &prefixed, Some(E1)),
        (other_digest, "sha256:another", Some(E0)),
    ];
    for (node, hash, environment) in rows {
        insert_template_inventory(&fixture.pool, node, "spelled", hash, "9.0.0").await?;
        sqlx::query(sql!(
            "UPDATE node_template_inventory
             SET execution_metadata = CASE WHEN $4::TEXT IS NULL THEN NULL
                     ELSE jsonb_build_object('environment', $4::TEXT) END
             WHERE node_id = $1 AND template_name = $2 AND content_hash = $3"
        ))
        .bind(node)
        .bind("spelled")
        .bind(hash)
        .bind(environment)
        .execute(&fixture.pool)
        .await?;
    }
    let pull = |gate| {
        let substrate = gated(&fixture, gate);
        let prefixed = prefixed.clone();
        async move {
            TestResult::Ok(
                substrate
                    .pull_on_create_worker_candidates("spelled", Some(&prefixed), "fc")
                    .await?
                    .into_iter()
                    .map(|node| node.id)
                    .collect::<BTreeSet<NodeId>>(),
            )
        }
    };
    assert_eq!(
        pull(On).await?,
        BTreeSet::from([bound_e1, other_digest]),
        "the E0 holders are refused for the build they hold, whatever its \
         spelling; a node holding only another digest pulls this one unbound"
    );
    assert_eq!(pull(Off).await?, nodes.into_iter().collect());
    Ok(())
}
