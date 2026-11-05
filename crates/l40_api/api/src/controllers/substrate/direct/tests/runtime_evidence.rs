//! Unbound create capacity needs current, legacy-capable runtime evidence.
use super::*;

#[tokio::test(flavor = "current_thread")]
async fn runtime_candidates_require_fresh_positive_evidence() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let node = NodeId::new();
    insert_ready_worker_for_runtime(
        &fixture.pool,
        node,
        "127.0.0.1:1",
        "ch",
        "54.0",
        "cloud-hypervisor-snapshot",
        "54.0.1",
    )
    .await?;
    insert_template_inventory(&fixture.pool, node, "seed", "hash-seed", "54.0.1").await?;
    sqlx::query(sql!(
        "UPDATE node_template_inventory
        SET runtime = 'ch', state_format_name = 'cloud-hypervisor-snapshot', pool_current = 1
        WHERE node_id = $1"
    ))
    .bind(node)
    .execute(&fixture.pool)
    .await?;

    // A fresh heartbeat and an installed VMM are insufficient. Unavailable,
    // legacy, missing and stale reports all exclude the candidate, including
    // the last-known cache fallback.
    for (status, age_seconds, requires_environment, expected) in [
        (None, None, false, false),
        (Some("legacy"), Some(0), false, false),
        (Some("unavailable"), Some(0), false, false),
        (Some("complete"), Some(7200), false, false),
        (Some("complete"), Some(0), true, false),
        (Some("complete"), Some(0), false, true),
    ] {
        sqlx::query(sql!(
            "UPDATE node_runtime_installations SET requires_environment = $2 WHERE node_id = $1"
        ))
        .bind(node)
        .bind(requires_environment)
        .execute(&fixture.pool)
        .await?;
        sqlx::query(sql!(
            "UPDATE node_status_snapshots
            SET execution_inventory_status = $2,
                execution_inventory_received_at = CASE WHEN $3::INT IS NULL THEN NULL
                    ELSE NOW() - make_interval(secs => $3) END
            WHERE node_id = $1"
        ))
        .bind(node)
        .bind(status)
        .bind(age_seconds)
        .execute(&fixture.pool)
        .await?;
        let cached = fixture
            .substrate
            .cached_worker_create_candidates(
                "seed",
                Some("hash-seed"),
                "ch",
                CreateBootMode::BaseSnapshot,
            )
            .await;
        assert_eq!(
            cached.is_ok(),
            expected,
            "cached: {status:?}, {age_seconds:?}"
        );
        let pull = fixture
            .substrate
            .pull_on_create_worker_candidates("ch")
            .await?;
        assert_eq!(
            !pull.is_empty(),
            expected,
            "pull: {status:?}, {age_seconds:?}"
        );
        let pool = fixture
            .substrate
            .pooled_worker_candidates("seed", Some("hash-seed"), "ch")
            .await?;
        assert_eq!(
            !pool.is_empty(),
            expected || requires_environment,
            "pool: {status:?}, {age_seconds:?}"
        );
    }
    // A managed-only release must not lend its guest or snapshot capability
    // to an independently installed legacy release of the same runtime kind.
    sqlx::query(sql!(
        "INSERT INTO node_runtime_installations
             (node_id, runtime, runtime_version, requires_environment)
         VALUES ($1, 'ch', '99.0', TRUE)"
    ))
    .bind(node)
    .execute(&fixture.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_runtime_guests (node_id, runtime, runtime_version, guest_os, guest_arch)
         SELECT node_id, runtime, '99.0', guest_os, guest_arch
         FROM node_runtime_guests WHERE node_id = $1"
    ))
    .bind(node)
    .execute(&fixture.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_runtime_state_formats (
             node_id, runtime, runtime_version, state_format_name, state_format_version,
             can_create, can_load
         )
         SELECT node_id, runtime, '99.0', state_format_name, state_format_version, TRUE, TRUE
         FROM node_runtime_state_formats WHERE node_id = $1"
    ))
    .bind(node)
    .execute(&fixture.pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE node_runtime_state_formats SET can_load = FALSE
         WHERE node_id = $1 AND runtime_version = '54.0'"
    ))
    .bind(node)
    .execute(&fixture.pool)
    .await?;
    assert!(matches!(
        fixture
            .substrate
            .cached_worker_create_candidates(
                "seed",
                Some("hash-seed"),
                "ch",
                CreateBootMode::BaseSnapshot
            )
            .await,
        Err(SubstrateError::Unavailable { .. })
    ));
    // Cold creation does not require a loadable memory-state format.
    fixture
        .substrate
        .cached_worker_create_candidates("seed", Some("hash-seed"), "ch", CreateBootMode::Cold)
        .await?;
    sqlx::query(sql!(
        "DELETE FROM node_runtime_guests WHERE node_id = $1 AND runtime_version = '54.0'"
    ))
    .bind(node)
    .execute(&fixture.pool)
    .await?;
    assert!(matches!(
        fixture
            .substrate
            .cached_worker_create_candidates("seed", Some("hash-seed"), "ch", CreateBootMode::Cold)
            .await,
        Err(SubstrateError::Unavailable { .. })
    ));
    assert!(
        fixture
            .substrate
            .pull_on_create_worker_candidates("ch")
            .await?
            .is_empty()
    );
    // Pool adoption keeps the resident execution's binding; it does not
    // launch an unbound replacement from these installation rows.
    assert!(
        !fixture
            .substrate
            .pooled_worker_candidates("seed", Some("hash-seed"), "ch")
            .await?
            .is_empty()
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn unbound_firecracker_candidates_exclude_managed_only_inventory() -> TestResult {
    let Some(fixture) = postgres_fixture().await? else {
        return Ok(());
    };
    let node = NodeId::new();
    insert_ready_worker(&fixture.pool, node, "127.0.0.1:1", "9.0.0").await?;
    insert_template_inventory(&fixture.pool, node, "seed", "hash-seed", "9.0.0").await?;
    sqlx::query(sql!(
        "UPDATE node_template_inventory SET pool_current = 1 WHERE node_id = $1"
    ))
    .bind(node)
    .execute(&fixture.pool)
    .await?;
    for requires_environment in [true, false, true] {
        sqlx::query(sql!(
            "UPDATE node_runtime_installations SET requires_environment = $2 WHERE node_id = $1"
        ))
        .bind(node)
        .bind(requires_environment)
        .execute(&fixture.pool)
        .await?;
        for mode in [CreateBootMode::Cold, CreateBootMode::BaseSnapshot] {
            let cached = fixture
                .substrate
                .cached_worker_create_candidates("seed", Some("hash-seed"), "fc", mode)
                .await;
            assert_eq!(cached.is_ok(), !requires_environment);
        }
        assert_eq!(
            !fixture
                .substrate
                .pull_on_create_worker_candidates("fc")
                .await?
                .is_empty(),
            !requires_environment
        );
        assert!(
            !fixture
                .substrate
                .pooled_worker_candidates("seed", Some("hash-seed"), "fc")
                .await?
                .is_empty()
        );
        // FC's historical last-known fallback must not bypass provenance.
        sqlx::query(sql!(
            "UPDATE node_status_snapshots SET stale_after = NOW() - INTERVAL '1 second'
             WHERE node_id = $1"
        ))
        .bind(node)
        .execute(&fixture.pool)
        .await?;
        let fallback = fixture
            .substrate
            .cached_worker_create_candidates("seed", Some("hash-seed"), "fc", CreateBootMode::Cold)
            .await;
        assert_eq!(fallback.is_ok(), !requires_environment);
        sqlx::query(sql!(
            "UPDATE node_status_snapshots SET stale_after = NOW() + INTERVAL '1 hour'
             WHERE node_id = $1"
        ))
        .bind(node)
        .execute(&fixture.pool)
        .await?;
    }
    Ok(())
}
