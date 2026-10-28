//! Heartbeats cannot turn retained or absent execution facts into CH capacity.
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
    for (status, age_seconds, expected) in [
        (None, None, false),
        (Some("legacy"), Some(0), false),
        (Some("unavailable"), Some(0), false),
        (Some("complete"), Some(7200), false),
        (Some("complete"), Some(0), true),
    ] {
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
            expected,
            "pool: {status:?}, {age_seconds:?}"
        );
    }
    sqlx::query(sql!(
        "UPDATE node_runtime_state_formats SET can_load = FALSE WHERE node_id = $1"
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
    sqlx::query(sql!("DELETE FROM node_runtime_guests WHERE node_id = $1"))
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
    assert!(
        fixture
            .substrate
            .pooled_worker_candidates("seed", Some("hash-seed"), "ch")
            .await?
            .is_empty()
    );
    Ok(())
}
