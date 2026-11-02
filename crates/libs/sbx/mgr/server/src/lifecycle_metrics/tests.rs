use super::*;
use crate::state::SandboxState;

#[test]
fn disappeared_lifecycle_buckets_are_zeroed_once() {
    let mut sampler = LifecycleGaugeSampler::default();
    let initial = [
        SandboxLifecycleCount {
            state: SandboxState::CleanupFailed,
            count: 2,
        },
        SandboxLifecycleCount {
            state: SandboxState::Started,
            count: 4,
        },
    ];
    let mut emitted = Vec::new();
    sampler.emit_sample(&initial, |bucket, count| {
        emitted.push((bucket.clone(), count));
    });
    sampler.emit_sample(&initial[1..], |bucket, count| {
        emitted.push((bucket.clone(), count));
    });
    sampler.emit_sample(&initial[1..], |bucket, count| {
        emitted.push((bucket.clone(), count));
    });

    assert_eq!(
        emitted,
        vec![
            (
                LifecycleBucket {
                    state: "cleanup_failed".to_owned(),
                },
                2,
            ),
            (
                LifecycleBucket {
                    state: "started".to_owned(),
                },
                4,
            ),
            (
                LifecycleBucket {
                    state: "started".to_owned(),
                },
                4,
            ),
            (
                LifecycleBucket {
                    state: "cleanup_failed".to_owned(),
                },
                0,
            ),
            (
                LifecycleBucket {
                    state: "started".to_owned(),
                },
                4,
            ),
        ]
    );
}

/// A sandbox whose backup has stalled for days reaches the backlog gauges on
/// the sampler's first tick.
#[tokio::test]
async fn the_sampler_reports_a_stalled_backup() {
    let conn = crate::db::test_conn();
    let created_at = crate::db::now_epoch_i64().unwrap() - 3 * 24 * 60 * 60;
    let artifact = "01939e3e-9007-7000-8000-000000000001";
    conn.execute(
        "INSERT INTO local_artifacts (
             local_artifact_id, sandbox_id, artifact_kind, artifact_origin,
             local_path, created_at
         ) VALUES (?1, 'sbx-stalled', 'full', 'suspend', '/tmp/stalled', ?2)",
        crate::db::fixture::params![artifact, created_at.to_string()],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO sandboxes (
             id, uuid8, state, template, user, runtime, created_at, updated_at,
             process_env_json, auto_lifecycle_json, auto_lifecycle_state_json,
             current_local_artifact_id, current_local_artifact_kind,
             current_local_artifact_origin
         ) VALUES ('sbx-stalled', 'uuid8', 'suspended', 'seed', 'u', 'fc', '0', '0',
                   '{}', '{}', '{}', ?1, 'full', 'suspend')",
        crate::db::fixture::params![artifact],
    )
    .unwrap();

    let values = pplx_space_metrics::test_capture::capture_values_guard();
    let shutdown = CancellationToken::new();
    let sampler = tokio::spawn(run(
        crate::db::SharedConnection::new(conn),
        shutdown.clone(),
    ));
    tokio::time::sleep(Duration::from_millis(200)).await;
    shutdown.cancel();
    sampler.await.unwrap();
    let emitted = values.finish();

    let gauge = |name: &str| {
        emitted
            .iter()
            .rfind(|value| value.name == name)
            .map(|value| value.value)
    };
    assert_eq!(
        gauge("sandbox.worker.backup.pending"),
        Some(1.0),
        "{emitted:?}"
    );
    let age = gauge("sandbox.worker.backup.pending_oldest_age_seconds").unwrap();
    assert!(age >= (3 * 24 * 60 * 60) as f64, "{emitted:?}");
}
