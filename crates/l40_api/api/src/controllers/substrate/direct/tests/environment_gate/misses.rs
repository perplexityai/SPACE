//! When a create the gate kept off every holder goes on to pull, and when it fails as off does.

use super::*;

async fn cached_or_miss(
    fixture: &PostgresFixture,
    gate: EnvironmentPlacementGate,
) -> TestResult<BTreeSet<NodeId>> {
    Ok(gated(fixture, gate)
        .create_candidates_from_cache(BOUND_E1, Some(HASH), "fc", CreateBootMode::Cold)
        .await?
        .into_iter()
        .map(|node| node.id)
        .collect())
}

#[tokio::test(flavor = "current_thread")]
async fn on_treats_no_cached_candidate_as_a_miss_so_a_pull_can_follow() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    // The only node holding the template has no evidence; another node with
    // evidence for E1 holds nothing and could pull it.
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

    assert_eq!(
        cached_or_miss(&fixture, Off).await?,
        BTreeSet::from([holder])
    );
    assert_eq!(
        cached_or_miss(&fixture, Shadow).await?,
        BTreeSet::from([holder])
    );
    assert_eq!(
        cached_or_miss(&fixture, On).await?,
        BTreeSet::new(),
        "on refuses the holder, and that's a miss, not a failure"
    );
    assert_eq!(
        pull_create(&gated(&fixture, On), BOUND_E1).await?,
        BTreeSet::from([puller]),
        "so the pull-on-create candidates that follow can take it"
    );

    // With no cached worker for unbound work, off and on alike fail as
    // before the gate: on refuses nothing unbound, so there's no miss.
    for gate in [Off, On] {
        assert!(
            matches!(
                gated(&fixture, gate)
                    .create_candidates_from_cache(
                        UNBOUND,
                        Some("other-hash"),
                        "fc",
                        CreateBootMode::Cold
                    )
                    .await,
                Err(SubstrateError::Unavailable { .. })
            ),
            "{gate}"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn on_fails_a_create_no_holder_was_refused_for_as_off_does() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    // A node with evidence for E1 that could pull the build.
    seed_node(
        &fixture.pool,
        "127.0.0.1:1",
        SnapshotSeed::default(),
        &[E1],
        "9.0.0",
    )
    .await?;
    let holder = seed_node(
        &fixture.pool,
        "127.0.0.1:2",
        SnapshotSeed::default(),
        &[E1],
        "9.0.0",
    )
    .await?;
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
    inventory_templates(&fixture.pool, &BTreeSet::from([holder]), "9.0.0").await?;
    sqlx::query(sql!(
        "UPDATE node_status_snapshots SET scheduling_status = 'draining' WHERE node_id = $1"
    ))
    .bind(holder)
    .execute(&fixture.pool)
    .await?;

    // No worker holds `unheld`, and the only holder of the bound E1 build is
    // draining: the gate refused neither, so neither is a miss.
    for template in ["unheld", BOUND_E1] {
        for gate in EnvironmentPlacementGate::ALL {
            assert!(
                matches!(
                    gated(&fixture, gate)
                        .create_candidates_from_cache(
                            template,
                            Some(HASH),
                            "fc",
                            CreateBootMode::Cold
                        )
                        .await,
                    Err(SubstrateError::Unavailable { .. })
                ),
                "{template}: {gate}"
            );
        }
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn on_treats_a_refused_degraded_holder_as_a_miss_too() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    // The only holder is degraded, so only the last-known fallback sees it,
    // and it has no evidence.
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
    degrade(&fixture.pool, holder).await?;
    inventory_templates(&fixture.pool, &BTreeSet::from([holder]), "9.0.0").await?;
    assert_eq!(
        cached_or_miss(&fixture, Off).await?,
        BTreeSet::from([holder])
    );
    assert_eq!(
        cached_or_miss(&fixture, On).await?,
        BTreeSet::new(),
        "on refused the holder, and that's a miss"
    );
    Ok(())
}
