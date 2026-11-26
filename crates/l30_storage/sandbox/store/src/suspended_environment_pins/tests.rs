use std::str::FromStr;
use std::time::Duration;

use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, try_pg_test_db};
use pplx_space_execution_spec::EnvironmentReference;
use pplx_space_node_model::SubstrateKind;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_snapshot_model::{ArtifactKind, LocalArtifactId};
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use serde_json::{Value, json};
use sqlx::PgPool;

use crate::runtime_population::{SandboxPopulationRow, sandbox_population};

use super::{
    PinnedCheckpointExecution, SandboxPinState, SuspendedPinnedCandidate, candidates_page,
    pin_state,
};

fn env(ch: char) -> EnvironmentReference {
    let hex: String = std::iter::repeat_n(ch, 64).collect();
    EnvironmentReference::from_str(&format!("sha256:{hex}")).expect("valid sha256")
}

async fn insert_sandbox(
    pool: &PgPool,
    id: SandboxId,
    managed_by: SubstrateKind,
    lifecycle: &str,
    pin: Option<(LocalArtifactId, &str)>,
) -> TestResult {
    let (artifact_id, artifact_kind, artifact_origin) = match pin {
        Some((artifact_id, kind)) => {
            let origin = if kind == "disk" { "stop" } else { "suspend" };
            (Some(artifact_id), Some(kind), Some(origin))
        }
        None => (None, None, None),
    };
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, managed_by, lifecycle, placement,
             current_local_artifact_id, current_local_artifact_kind,
             current_local_artifact_origin,
             placement_changed_at, network_policy_generation
         ) VALUES (
             $1, $2, $3, $4, $5, 'unplaced',
             $6, $7,
             $8,
             NOW() - INTERVAL '1 hour', 1
         )"
    ))
    .bind(id)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .bind(managed_by)
    .bind(lifecycle)
    .bind(artifact_id)
    .bind(artifact_kind)
    .bind(artifact_origin)
    .execute(pool)
    .await?;
    Ok(())
}

#[derive(Clone, Copy)]
enum StorageRow {
    None,
    Orphaned,
    Live,
}

struct SnapshotSeed<'a> {
    snapshot_type: &'a str,
    state: &'a str,
    artifact: Option<(LocalArtifactId, &'a str)>,
    age: Duration,
    execution_metadata: Option<Value>,
    runtime: Option<&'a str>,
    runtime_version: Option<&'a str>,
    state_format_name: Option<&'a str>,
    state_format_version: Option<&'a str>,
    storage: StorageRow,
}

impl<'a> SnapshotSeed<'a> {
    fn ready_full(artifact: LocalArtifactId, execution_metadata: Option<Value>) -> Self {
        Self {
            snapshot_type: "full",
            state: "ready",
            artifact: Some((artifact, "full")),
            age: Duration::from_secs(30),
            execution_metadata,
            runtime: None,
            runtime_version: None,
            state_format_name: None,
            state_format_version: None,
            storage: StorageRow::Live,
        }
    }
}

async fn insert_snapshot(
    pool: &PgPool,
    sandbox_id: SandboxId,
    seed: SnapshotSeed<'_>,
) -> TestResult {
    let (artifact_id, artifact_kind, artifact_origin) = match seed.artifact {
        Some((artifact_id, kind)) => {
            let origin = if kind == "disk" { "stop" } else { "suspend" };
            (Some(artifact_id), Some(kind), Some(origin))
        }
        None => (None, None, None),
    };
    let (snapshot_id,): (uuid::Uuid,) = sqlx::query_as(sql!(
        "INSERT INTO snapshots (
             sandbox_id, type, state, timestamp,
             local_artifact_id, artifact_kind, artifact_origin, execution_metadata,
             runtime, runtime_version, state_format_name, state_format_version
         ) VALUES (
             $1, $2, $3, NOW() - make_interval(secs => $4),
             $5, $6, $7, $8,
             $9, $10, $11, $12
         )
         RETURNING id"
    ))
    .bind(sandbox_id)
    .bind(seed.snapshot_type)
    .bind(seed.state)
    .bind(seed.age.as_secs_f64())
    .bind(artifact_id)
    .bind(artifact_kind)
    .bind(artifact_origin)
    .bind(seed.execution_metadata)
    .bind(seed.runtime)
    .bind(seed.runtime_version)
    .bind(seed.state_format_name)
    .bind(seed.state_format_version)
    .fetch_one(pool)
    .await?;

    match seed.storage {
        StorageRow::None => {}
        StorageRow::Live => {
            sqlx::query(sql!(
                "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string)
                 VALUES ($1, 's3://synthetic-bucket/checkpoint')"
            ))
            .bind(snapshot_id)
            .execute(pool)
            .await?;
        }
        StorageRow::Orphaned => {
            sqlx::query(sql!(
                "INSERT INTO snapshot_storage_locations (
                     snapshot_id, storage_connection_string, orphaned_at
                 ) VALUES ($1, 's3://synthetic-bucket/checkpoint', NOW())"
            ))
            .bind(snapshot_id)
            .execute(pool)
            .await?;
        }
    }
    Ok(())
}

/// Filters `suspended` rows on both substrates to those whose pinned, ready,
/// unorphaned `full` checkpoint isn't explicitly bound to another environment
/// string, returning raw execution columns (and pinned `LocalArtifactId` in
/// [`pin_state`]) so `RecordedExecution::from_record` + `attribute` is the
/// single judge — excluding disk-re-pinned rows (which [`pin_state`] reports as
/// [`SandboxPinState::SuspendedPinned`] with [`ArtifactKind::Disk`] when
/// `ready` + unorphaned and [`SandboxPinState::NotPinned`] otherwise),
/// markerless rows, unready or orphaned checkpoints, other lifecycles, other
/// substrates, and multi-attempt backups whose newest `ready` copy binds
/// elsewhere.
#[tokio::test(flavor = "current_thread")]
async fn selects_only_suspended_rows_pinned_to_environment_full_checkpoint() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let e0 = env('a');
    let e1 = env('b');
    let e0_meta = Some(json!({ "environment": e0.to_string() }));
    let e1_meta = Some(json!({ "environment": e1.to_string() }));

    // 1. Eligible: suspended Kubernetes row pinned to a ready, unorphaned E0 full checkpoint.
    let eligible = SandboxId::new();
    let eligible_art = LocalArtifactId::new();
    insert_sandbox(
        &db.pool,
        eligible,
        SubstrateKind::Kubernetes,
        "suspended",
        Some((eligible_art, "full")),
    )
    .await?;
    insert_snapshot(
        &db.pool,
        eligible,
        SnapshotSeed::ready_full(eligible_art, e0_meta.clone()),
    )
    .await?;

    // 2. Memory already dropped: row re-pinned to a `disk` artifact while the
    //    retained `full` snapshot bound to E0 stays `ready` and unorphaned.
    let dropped = SandboxId::new();
    let dropped_full_art = LocalArtifactId::new();
    let dropped_disk_art = LocalArtifactId::new();
    insert_sandbox(
        &db.pool,
        dropped,
        SubstrateKind::Kubernetes,
        "suspended",
        Some((dropped_disk_art, "disk")),
    )
    .await?;
    insert_snapshot(
        &db.pool,
        dropped,
        SnapshotSeed {
            age: Duration::from_secs(120),
            ..SnapshotSeed::ready_full(dropped_full_art, e0_meta.clone())
        },
    )
    .await?;
    insert_snapshot(
        &db.pool,
        dropped,
        SnapshotSeed {
            snapshot_type: "disk",
            artifact: Some((dropped_disk_art, "disk")),
            ..SnapshotSeed::ready_full(dropped_disk_art, e0_meta.clone())
        },
    )
    .await?;

    // 3. Markerless `suspended` row with a fresh ready E0 `full` snapshot:
    //    not pinned (`current_local_artifact_id IS NULL`), so excluded here.
    let markerless = SandboxId::new();
    insert_sandbox(
        &db.pool,
        markerless,
        SubstrateKind::Kubernetes,
        "suspended",
        None,
    )
    .await?;
    insert_snapshot(
        &db.pool,
        markerless,
        SnapshotSeed {
            artifact: None,
            ..SnapshotSeed::ready_full(LocalArtifactId::new(), e0_meta.clone())
        },
    )
    .await?;

    // 4. Direct (`SubstrateKind::Space`) requires the same `ready` + unorphaned
    //    object-storage precondition as Kubernetes: `direct_eligible` qualifies,
    //    while `direct_creating` (upload still `creating` with no storage row)
    //    and `direct_orphaned` (orphaned storage row) are excluded.
    let k8s_creating = SandboxId::new();
    let k8s_creating_art = LocalArtifactId::new();
    insert_sandbox(
        &db.pool,
        k8s_creating,
        SubstrateKind::Kubernetes,
        "suspended",
        Some((k8s_creating_art, "full")),
    )
    .await?;
    insert_snapshot(
        &db.pool,
        k8s_creating,
        SnapshotSeed {
            state: "creating",
            ..SnapshotSeed::ready_full(k8s_creating_art, e0_meta.clone())
        },
    )
    .await?;

    let direct_eligible = SandboxId::new();
    let direct_eligible_art = LocalArtifactId::new();
    insert_sandbox(
        &db.pool,
        direct_eligible,
        SubstrateKind::Space,
        "suspended",
        Some((direct_eligible_art, "full")),
    )
    .await?;
    insert_snapshot(
        &db.pool,
        direct_eligible,
        SnapshotSeed::ready_full(direct_eligible_art, e0_meta.clone()),
    )
    .await?;

    let direct_creating = SandboxId::new();
    let direct_creating_art = LocalArtifactId::new();
    insert_sandbox(
        &db.pool,
        direct_creating,
        SubstrateKind::Space,
        "suspended",
        Some((direct_creating_art, "full")),
    )
    .await?;
    insert_snapshot(
        &db.pool,
        direct_creating,
        SnapshotSeed {
            state: "creating",
            storage: StorageRow::None,
            ..SnapshotSeed::ready_full(direct_creating_art, e0_meta.clone())
        },
    )
    .await?;

    let direct_orphaned = SandboxId::new();
    let direct_orphaned_art = LocalArtifactId::new();
    insert_sandbox(
        &db.pool,
        direct_orphaned,
        SubstrateKind::Space,
        "suspended",
        Some((direct_orphaned_art, "full")),
    )
    .await?;
    insert_snapshot(
        &db.pool,
        direct_orphaned,
        SnapshotSeed {
            storage: StorageRow::Orphaned,
            ..SnapshotSeed::ready_full(direct_orphaned_art, e0_meta.clone())
        },
    )
    .await?;

    // Direct row already re-pinned to a `ready`, unorphaned `disk` projection
    // reports `Disk`; one whose `disk` snapshot isn't `ready`/live reports
    // `Other`. Both are excluded from `candidates_page`.
    let direct_dropped = SandboxId::new();
    let direct_dropped_full_art = LocalArtifactId::new();
    let direct_dropped_disk_art = LocalArtifactId::new();
    insert_sandbox(
        &db.pool,
        direct_dropped,
        SubstrateKind::Space,
        "suspended",
        Some((direct_dropped_disk_art, "disk")),
    )
    .await?;
    insert_snapshot(
        &db.pool,
        direct_dropped,
        SnapshotSeed {
            age: Duration::from_secs(120),
            ..SnapshotSeed::ready_full(direct_dropped_full_art, e0_meta.clone())
        },
    )
    .await?;
    insert_snapshot(
        &db.pool,
        direct_dropped,
        SnapshotSeed {
            snapshot_type: "disk",
            artifact: Some((direct_dropped_disk_art, "disk")),
            ..SnapshotSeed::ready_full(direct_dropped_disk_art, e0_meta.clone())
        },
    )
    .await?;

    let direct_dropped_creating = SandboxId::new();
    let direct_dropped_creating_full_art = LocalArtifactId::new();
    let direct_dropped_creating_disk_art = LocalArtifactId::new();
    insert_sandbox(
        &db.pool,
        direct_dropped_creating,
        SubstrateKind::Space,
        "suspended",
        Some((direct_dropped_creating_disk_art, "disk")),
    )
    .await?;
    insert_snapshot(
        &db.pool,
        direct_dropped_creating,
        SnapshotSeed {
            age: Duration::from_secs(120),
            ..SnapshotSeed::ready_full(direct_dropped_creating_full_art, e0_meta.clone())
        },
    )
    .await?;
    insert_snapshot(
        &db.pool,
        direct_dropped_creating,
        SnapshotSeed {
            snapshot_type: "disk",
            state: "creating",
            artifact: Some((direct_dropped_creating_disk_art, "disk")),
            storage: StorageRow::None,
            ..SnapshotSeed::ready_full(direct_dropped_creating_disk_art, e0_meta.clone())
        },
    )
    .await?;

    // 5. Pinned E0 `full` snapshot on Kubernetes with only orphaned storage -> excluded.
    let orphaned = SandboxId::new();
    let orphaned_art = LocalArtifactId::new();
    insert_sandbox(
        &db.pool,
        orphaned,
        SubstrateKind::Kubernetes,
        "suspended",
        Some((orphaned_art, "full")),
    )
    .await?;
    insert_snapshot(
        &db.pool,
        orphaned,
        SnapshotSeed {
            storage: StorageRow::Orphaned,
            ..SnapshotSeed::ready_full(orphaned_art, e0_meta.clone())
        },
    )
    .await?;

    // 6a. Unbound (`{}` / `{"environment": null}`) and pre-metadata (`None`)
    //     checkpoints ARE returned with their execution columns intact so the
    //     driver can attribute them against the runtime catalog.
    let unbound_omitted = SandboxId::new();
    let unbound_omitted_art = LocalArtifactId::new();
    let unbound_omitted_meta = json!({
        "components": [{ "kind": "kernel", "version": "6.1.155" }]
    });
    insert_sandbox(
        &db.pool,
        unbound_omitted,
        SubstrateKind::Kubernetes,
        "suspended",
        Some((unbound_omitted_art, "full")),
    )
    .await?;
    insert_snapshot(
        &db.pool,
        unbound_omitted,
        SnapshotSeed {
            runtime: Some("firecracker"),
            runtime_version: Some("1.15.1"),
            state_format_name: Some("firecracker-snapshot"),
            state_format_version: Some("9.0.0"),
            ..SnapshotSeed::ready_full(unbound_omitted_art, Some(unbound_omitted_meta.clone()))
        },
    )
    .await?;

    let unbound_null = SandboxId::new();
    let unbound_null_art = LocalArtifactId::new();
    insert_sandbox(
        &db.pool,
        unbound_null,
        SubstrateKind::Kubernetes,
        "suspended",
        Some((unbound_null_art, "full")),
    )
    .await?;
    insert_snapshot(
        &db.pool,
        unbound_null,
        SnapshotSeed::ready_full(unbound_null_art, Some(json!({ "environment": null }))),
    )
    .await?;

    let pre_metadata = SandboxId::new();
    let pre_metadata_art = LocalArtifactId::new();
    insert_sandbox(
        &db.pool,
        pre_metadata,
        SubstrateKind::Kubernetes,
        "suspended",
        Some((pre_metadata_art, "full")),
    )
    .await?;
    insert_snapshot(
        &db.pool,
        pre_metadata,
        SnapshotSeed {
            runtime: Some("firecracker"),
            state_format_name: Some("firecracker-snapshot"),
            ..SnapshotSeed::ready_full(pre_metadata_art, None)
        },
    )
    .await?;

    // 6b. Explicitly bound to E1 -> skipped by the SQL prefilter; non-string
    //     `environment` shapes (`Value::Null`, `7`, `{"environment": 7}`) pass
    //     through with raw execution columns so `RecordedExecution::from_record`
    //     is the single lenient judge.
    let bound_e1 = SandboxId::new();
    let bound_e1_art = LocalArtifactId::new();
    insert_sandbox(
        &db.pool,
        bound_e1,
        SubstrateKind::Kubernetes,
        "suspended",
        Some((bound_e1_art, "full")),
    )
    .await?;
    insert_snapshot(
        &db.pool,
        bound_e1,
        SnapshotSeed::ready_full(bound_e1_art, e1_meta.clone()),
    )
    .await?;

    let mut lenient_shapes = Vec::new();
    for meta in [
        Some(Value::Null),
        Some(json!(7)),
        Some(json!({ "environment": 7 })),
    ] {
        let id = SandboxId::new();
        let art = LocalArtifactId::new();
        insert_sandbox(
            &db.pool,
            id,
            SubstrateKind::Kubernetes,
            "suspended",
            Some((art, "full")),
        )
        .await?;
        insert_snapshot(&db.pool, id, SnapshotSeed::ready_full(art, meta)).await?;
        lenient_shapes.push((id, art));
    }

    // 7. Non-`suspended` lifecycle (`stopped`, `started`, `restoring`, `suspending`)
    //    carrying a `full` pin -> excluded from `candidates_page`, and reported
    //    as `NotPinned { lifecycle }` by `pin_state` so mid-transition rows are
    //    released rather than skipped.
    let mut non_suspended = Vec::new();
    for lifecycle in ["stopped", "started", "restoring", "suspending"] {
        let id = SandboxId::new();
        let art = LocalArtifactId::new();
        insert_sandbox(
            &db.pool,
            id,
            SubstrateKind::Kubernetes,
            lifecycle,
            Some((art, "full")),
        )
        .await?;
        insert_snapshot(&db.pool, id, SnapshotSeed::ready_full(art, e0_meta.clone())).await?;
        non_suspended.push((id, lifecycle));
    }

    // 8. Two `ready` unorphaned `full` rows for one pinned artifact where the
    //    newer upload records E1 while the older recorded E0: the newest `ready`
    //    row wins, matching `sandbox_population`, so this sandbox isn't on E0.
    let superseded = SandboxId::new();
    let superseded_art = LocalArtifactId::new();
    insert_sandbox(
        &db.pool,
        superseded,
        SubstrateKind::Kubernetes,
        "suspended",
        Some((superseded_art, "full")),
    )
    .await?;
    insert_snapshot(
        &db.pool,
        superseded,
        SnapshotSeed {
            age: Duration::from_secs(90),
            ..SnapshotSeed::ready_full(superseded_art, e0_meta.clone())
        },
    )
    .await?;
    insert_snapshot(
        &db.pool,
        superseded,
        SnapshotSeed {
            age: Duration::from_secs(10),
            ..SnapshotSeed::ready_full(superseded_art, e1_meta.clone())
        },
    )
    .await?;

    let k8s_candidates: Vec<SandboxId> =
        candidates_page(&db.pool, SubstrateKind::Kubernetes, e0, None, 50)
            .await?
            .into_iter()
            .map(|row| row.sandbox_id)
            .collect();
    let mut expected_k8s = vec![eligible, unbound_omitted, unbound_null, pre_metadata];
    expected_k8s.extend(lenient_shapes.iter().map(|(id, _)| *id));
    expected_k8s.sort();
    assert_eq!(k8s_candidates, expected_k8s);

    let direct_candidates: Vec<SandboxId> =
        candidates_page(&db.pool, SubstrateKind::Space, e0, None, 50)
            .await?
            .into_iter()
            .map(|row| row.sandbox_id)
            .collect();
    assert_eq!(direct_candidates, vec![direct_eligible]);

    let expected_unbound_exec = PinnedCheckpointExecution {
        execution_metadata: Some(sqlx::types::Json(unbound_omitted_meta)),
        runtime: Some("firecracker".to_owned()),
        runtime_version: Some("1.15.1".to_owned()),
        state_format_name: Some("firecracker-snapshot".to_owned()),
        state_format_version: Some("9.0.0".to_owned()),
    };
    assert_eq!(
        pin_state(&db.pool, SubstrateKind::Kubernetes, unbound_omitted).await?,
        SandboxPinState::SuspendedPinned {
            artifact_id: unbound_omitted_art,
            kind: ArtifactKind::Full,
            execution: expected_unbound_exec,
        },
    );
    assert_eq!(
        pin_state(&db.pool, SubstrateKind::Kubernetes, pre_metadata).await?,
        SandboxPinState::SuspendedPinned {
            artifact_id: pre_metadata_art,
            kind: ArtifactKind::Full,
            execution: PinnedCheckpointExecution {
                execution_metadata: None,
                runtime: Some("firecracker".to_owned()),
                runtime_version: None,
                state_format_name: Some("firecracker-snapshot".to_owned()),
                state_format_version: None,
            },
        },
    );

    assert!(matches!(
        pin_state(&db.pool, SubstrateKind::Kubernetes, eligible).await?,
        SandboxPinState::SuspendedPinned {
            artifact_id,
            kind: ArtifactKind::Full,
            ..
        } if artifact_id == eligible_art
    ));
    assert!(matches!(
        pin_state(&db.pool, SubstrateKind::Kubernetes, bound_e1).await?,
        SandboxPinState::SuspendedPinned {
            artifact_id,
            kind: ArtifactKind::Full,
            ..
        } if artifact_id == bound_e1_art
    ));
    assert_eq!(
        pin_state(&db.pool, SubstrateKind::Kubernetes, superseded).await?,
        SandboxPinState::SuspendedPinned {
            artifact_id: superseded_art,
            kind: ArtifactKind::Full,
            execution: PinnedCheckpointExecution {
                execution_metadata: e1_meta.map(sqlx::types::Json),
                runtime: None,
                runtime_version: None,
                state_format_name: None,
                state_format_version: None,
            },
        },
    );
    for (id, art) in lenient_shapes {
        assert!(matches!(
            pin_state(&db.pool, SubstrateKind::Kubernetes, id).await?,
            SandboxPinState::SuspendedPinned {
                artifact_id,
                kind: ArtifactKind::Full,
                ..
            } if artifact_id == art
        ));
    }
    assert_eq!(
        pin_state(&db.pool, SubstrateKind::Space, eligible).await?,
        SandboxPinState::Absent,
    );
    assert!(matches!(
        pin_state(&db.pool, SubstrateKind::Kubernetes, dropped).await?,
        SandboxPinState::SuspendedPinned {
            artifact_id,
            kind: ArtifactKind::Disk,
            ..
        } if artifact_id == dropped_disk_art
    ));
    for (id, lifecycle) in non_suspended {
        assert_eq!(
            pin_state(&db.pool, SubstrateKind::Kubernetes, id).await?,
            SandboxPinState::NotPinned {
                lifecycle: lifecycle.to_owned(),
            },
        );
    }
    for suspended_unpinned in [markerless, k8s_creating, orphaned] {
        assert_eq!(
            pin_state(&db.pool, SubstrateKind::Kubernetes, suspended_unpinned).await?,
            SandboxPinState::NotPinned {
                lifecycle: "suspended".to_owned(),
            },
            "{suspended_unpinned} lacks a ready unorphaned pin on Kubernetes"
        );
    }
    for absent in [
        direct_eligible,
        direct_creating,
        direct_orphaned,
        SandboxId::new(),
    ] {
        assert_eq!(
            pin_state(&db.pool, SubstrateKind::Kubernetes, absent).await?,
            SandboxPinState::Absent,
        );
    }
    assert!(matches!(
        pin_state(&db.pool, SubstrateKind::Space, direct_eligible).await?,
        SandboxPinState::SuspendedPinned {
            artifact_id,
            kind: ArtifactKind::Full,
            ..
        } if artifact_id == direct_eligible_art
    ));
    for direct_other in [direct_creating, direct_orphaned, direct_dropped_creating] {
        assert_eq!(
            pin_state(&db.pool, SubstrateKind::Space, direct_other).await?,
            SandboxPinState::NotPinned {
                lifecycle: "suspended".to_owned(),
            },
            "{direct_other} lacks a ready unorphaned checkpoint on Direct"
        );
    }
    assert!(matches!(
        pin_state(&db.pool, SubstrateKind::Space, direct_dropped).await?,
        SandboxPinState::SuspendedPinned {
            artifact_id,
            kind: ArtifactKind::Disk,
            ..
        } if artifact_id == direct_dropped_disk_art
    ));

    // Cross-check against `sandbox_population`: on Kubernetes, re-pinning
    // `dropped` to `disk` removes it from E0's `suspended` population (`eligible`
    // and unpinned `markerless` remain, while `superseded` binds to E1); on
    // Direct, `sandbox_population` still reads the newest `full` snapshot
    // without consulting `current_local_artifact_*` or upload readiness (`2 + 5 = 7`).
    let pop = sandbox_population(&db.pool).await?;
    let e0_suspended_total: i64 = pop
        .iter()
        .filter(
            |SandboxPopulationRow {
                 environment_ref,
                 lifecycle,
                 ..
             }| {
                environment_ref.as_deref() == Some(&e0.to_string()) && lifecycle == "suspended"
            },
        )
        .map(|row| row.sandboxes)
        .sum();
    assert_eq!(e0_suspended_total, 7);

    Ok(())
}

/// Keyset pagination walks strictly ascending `SandboxId`s across pages with no
/// duplicates or gaps, and `limit == 0` short-circuits to an empty page.
#[tokio::test(flavor = "current_thread")]
async fn candidates_page_walks_ids_in_keyset_order() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let e0 = env('c');
    let meta = Some(json!({ "environment": e0.to_string() }));

    // Seed enough rows with `limit = 1` so `CANDIDATES_AFTER_PAGE_QUERY` runs
    // past PostgreSQL's 5-execution generic-plan threshold on a single prepared
    // statement.
    let mut expected = Vec::new();
    for _ in 0..7 {
        let id = SandboxId::new();
        let art = LocalArtifactId::new();
        insert_sandbox(
            &db.pool,
            id,
            SubstrateKind::Kubernetes,
            "suspended",
            Some((art, "full")),
        )
        .await?;
        insert_snapshot(&db.pool, id, SnapshotSeed::ready_full(art, meta.clone())).await?;
        expected.push(id);
    }
    expected.sort();

    assert!(
        candidates_page(&db.pool, SubstrateKind::Kubernetes, e0, None, 0)
            .await?
            .is_empty()
    );

    let mut after = None;
    for &expected_id in &expected {
        let page: Vec<SuspendedPinnedCandidate> =
            candidates_page(&db.pool, SubstrateKind::Kubernetes, e0, after, 1).await?;
        assert_eq!(
            page.iter().map(|row| row.sandbox_id).collect::<Vec<_>>(),
            vec![expected_id]
        );
        after = Some(expected_id);
    }
    assert!(
        candidates_page(&db.pool, SubstrateKind::Kubernetes, e0, after, 1)
            .await?
            .is_empty()
    );

    Ok(())
}
