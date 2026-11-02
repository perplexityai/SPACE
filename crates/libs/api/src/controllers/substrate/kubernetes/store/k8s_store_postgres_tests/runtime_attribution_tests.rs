//! `sandboxes.activating_client_id` through the pod-tracking writers: each
//! sets the record from the pod's `space.pplx.net/client-id` label when the
//! active execution row moves to a different pod, repairs it from a labeled
//! view of the same pod, and leaves it alone for an unlabeled view of the same
//! pod or a sandbox that's being torn down.

use pplx_space_common_core::types::{ClientId, PodName};

use super::*;
use crate::controllers::substrate::{ObservedPod, RecordedExecution};

fn client(id: &str) -> ClientId {
    ClientId::parse(id).expect("test client id")
}

async fn record(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<Option<ClientId>> {
    Ok(
        pplx_space_sandbox_store::attribute_runtime::activating_client(pool, sandbox_id)
            .await?
            .expect("sandbox row"),
    )
}

async fn set_record(pool: &PgPool, sandbox_id: SandboxId, id: Option<&str>) -> TestResult {
    sqlx::query(sql!(
        "UPDATE sandboxes SET activating_client_id = $2 WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(id)
    .execute(pool)
    .await?;
    Ok(())
}

async fn tracked_pod_uid(
    pool: &PgPool,
    sandbox_id: SandboxId,
    generation: i64,
) -> TestResult<Option<String>> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT pod_uid FROM sandbox_k8s_executions WHERE sandbox_id = $1 AND generation = $2"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .fetch_one(pool)
    .await?)
}

/// One write of the pod metadata: what it shows, the pod UID, its label, and
/// the record it leaves.
type Step<'a> = (
    &'a str,
    Option<&'a str>,
    Option<&'a ClientId>,
    Option<&'a ClientId>,
);

/// The metadata write, step by step on one execution row: each step's pod and
/// label, and the record it leaves.
#[tokio::test]
async fn pod_metadata_sets_the_record_from_the_tracked_pods_label() -> TestResult {
    let fixture = postgres_store().await?;
    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "started").await?;
    let generation = fixture
        .store
        .ensure_active_execution_claim(sandbox_id)
        .await?
        .row
        .generation;
    let alpha = client("alpha");
    let beta = client("beta");

    let steps: [Step<'_>; 5] = [
        (
            "no pod tracked yet writes nothing",
            None,
            Some(&alpha),
            None,
        ),
        (
            "a new labeled pod sets the record",
            Some("pod-1"),
            Some(&alpha),
            Some(&alpha),
        ),
        (
            "an unlabeled view of the same pod keeps it",
            Some("pod-1"),
            None,
            Some(&alpha),
        ),
        (
            "a labeled view of the same pod repairs it",
            Some("pod-1"),
            Some(&beta),
            Some(&beta),
        ),
        ("a new unlabeled pod clears it", Some("pod-2"), None, None),
    ];
    for (case, pod_uid, label, expected) in steps {
        fixture
            .store
            .persist_pod_metadata(sandbox_id, generation, pod_uid, None, label)
            .await?;
        assert_eq!(
            record(&fixture.pool, sandbox_id).await?.as_ref(),
            expected,
            "{case}"
        );
        assert_eq!(
            tracked_pod_uid(&fixture.pool, sandbox_id, generation)
                .await?
                .as_deref(),
            pod_uid,
            "{case}: the pod metadata is written either way"
        );
    }
    Ok(())
}

/// A record that lags behind the pod the row already tracks is repaired by a
/// labeled view of that pod, the write that takes the `sandboxes` lock.
#[tokio::test]
async fn a_labeled_view_repairs_a_lagging_record() -> TestResult {
    let fixture = postgres_store().await?;
    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "started").await?;
    let generation = fixture
        .store
        .ensure_active_execution_claim(sandbox_id)
        .await?
        .row
        .generation;
    fixture
        .store
        .persist_pod_metadata(sandbox_id, generation, Some("pod-1"), None, None)
        .await?;
    set_record(&fixture.pool, sandbox_id, Some("stale")).await?;

    fixture
        .store
        .persist_pod_metadata(
            sandbox_id,
            generation,
            Some("pod-1"),
            None,
            Some(&client("alpha")),
        )
        .await?;
    assert_eq!(
        record(&fixture.pool, sandbox_id).await?,
        Some(client("alpha"))
    );
    Ok(())
}

/// A sandbox in a lifecycle no activation creates a runtime in (a stop, a
/// suspend, a teardown) keeps its record when a tracking observer moves it to
/// a different labeled pod, or a repair observer sees that pod labeled, while
/// the pod metadata itself is still written.
#[tokio::test]
async fn a_torn_down_sandbox_keeps_its_record() -> TestResult {
    let fixture = postgres_store().await?;
    for lifecycle in pplx_space_sandbox_store::attribute_runtime::UNATTRIBUTED_LIFECYCLES {
        let sandbox_id = SandboxId::new();
        insert_sandbox(&fixture.pool, sandbox_id, "started").await?;
        let generation = fixture
            .store
            .ensure_active_execution_claim(sandbox_id)
            .await?
            .row
            .generation;
        set_record(&fixture.pool, sandbox_id, Some("owner")).await?;
        sqlx::query(sql!("UPDATE sandboxes SET lifecycle = $2 WHERE id = $1"))
            .bind(sandbox_id)
            .bind(lifecycle)
            .execute(&fixture.pool)
            .await?;

        fixture
            .store
            .persist_pod_metadata(
                sandbox_id,
                generation,
                Some("other-pod"),
                None,
                Some(&client("intruder")),
            )
            .await?;
        assert_eq!(
            record(&fixture.pool, sandbox_id).await?,
            Some(client("owner")),
            "{lifecycle}"
        );
        assert_eq!(
            tracked_pod_uid(&fixture.pool, sandbox_id, generation)
                .await?
                .as_deref(),
            Some("other-pod"),
            "{lifecycle}: the pod metadata is still written"
        );

        fixture
            .store
            .repair_runtime_attribution(
                sandbox_id,
                generation,
                "other-pod",
                Some(&client("intruder")),
            )
            .await?;
        assert_eq!(
            record(&fixture.pool, sandbox_id).await?,
            Some(client("owner")),
            "{lifecycle}: a repair observer leaves it too"
        );
    }
    Ok(())
}

/// The materialized write and the UID fill both attribute the pod they start
/// tracking, under the `sandboxes` lock they already hold.
#[tokio::test]
async fn materialization_and_uid_fill_attribute_the_new_pod() -> TestResult {
    let fixture = postgres_store().await?;

    let materialized = SandboxId::new();
    insert_sandbox(&fixture.pool, materialized, "started").await?;
    let generation = fixture
        .store
        .ensure_active_execution_claim(materialized)
        .await?
        .row
        .generation;
    set_record(&fixture.pool, materialized, Some("previous")).await?;
    fixture
        .store
        .persist_materialized_pod_metadata(
            materialized,
            generation,
            SandboxLifecycle::Started,
            Some("materialized-pod"),
            None,
            Some(&client("alpha")),
        )
        .await?;
    assert_eq!(
        record(&fixture.pool, materialized).await?,
        Some(client("alpha")),
        "a materialized pod sets the record"
    );

    let backfilled = SandboxId::new();
    let pod_name = K8sStore::execution_pod_name(backfilled, 1);
    insert_sandbox(&fixture.pool, backfilled, "started").await?;
    set_record(&fixture.pool, backfilled, Some("previous")).await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions (sandbox_id, generation, pod_name, pod_namespace)
         VALUES ($1, 1, $2, 'space')"
    ))
    .bind(backfilled)
    .bind(&pod_name)
    .execute(&fixture.pool)
    .await?;
    let identity = ExecutionIdentity {
        pod_name: PodName::parse(&pod_name)?,
        generation: Some(1),
        activation_attempt_id: None,
        recorded: RecordedExecution::Read {
            pod_uid: None,
            route_activation_id: None,
        },
        pod: ObservedPod::Unobserved,
    };
    let beta = client("beta");
    let filled = fixture
        .store
        .backfill_missing_pod_uid(backfilled, &identity, "observed-pod", Some(&beta))
        .await?;
    assert!(filled);
    assert_eq!(
        record(&fixture.pool, backfilled).await?,
        Some(client("beta")),
        "filling the UID of a labeled pod sets the record"
    );
    Ok(())
}

/// A metadata write that waited on the execution-row lock reads the record a
/// concurrent writer committed under that lock, not the one it saw first. Here
/// another transaction moves the row to `pod-2` and records `beta` while the
/// write of `pod-1`, labeled `alpha`, waits; the write must then record
/// `alpha` for `pod-1`, not leave `beta` beside it.
#[tokio::test]
async fn a_write_that_waited_on_the_row_lock_reads_the_committed_record() -> TestResult {
    let fixture = postgres_store_with_max_connections(4).await?;
    let sandbox_id = SandboxId::new();
    insert_sandbox(&fixture.pool, sandbox_id, "started").await?;
    let generation = fixture
        .store
        .ensure_active_execution_claim(sandbox_id)
        .await?
        .row
        .generation;
    let alpha = client("alpha");
    fixture
        .store
        .persist_pod_metadata(sandbox_id, generation, Some("pod-1"), None, Some(&alpha))
        .await?;
    assert_eq!(
        record(&fixture.pool, sandbox_id).await?,
        Some(alpha.clone())
    );

    // The concurrent writer, holding both locks in the established order.
    let mut writer = fixture.pool.begin().await?;
    sqlx::query(sql!("SELECT id FROM sandboxes WHERE id = $1 FOR UPDATE"))
        .bind(sandbox_id)
        .execute(&mut *writer)
        .await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions SET pod_uid = 'pod-2'
          WHERE sandbox_id = $1 AND generation = $2"
    ))
    .bind(sandbox_id)
    .bind(generation)
    .execute(&mut *writer)
    .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET activating_client_id = 'beta' WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&mut *writer)
    .await?;

    let release = async {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let waiting: bool = sqlx::query_scalar(sql!(
                    "SELECT EXISTS (
                         SELECT 1
                           FROM pg_stat_activity
                          WHERE datname = current_database()
                            AND wait_event_type = 'Lock'
                            AND query LIKE '%sandbox_k8s_executions%FOR UPDATE%'
                     )"
                ))
                .fetch_one(&fixture.pool)
                .await?;
                if waiting {
                    return Ok::<_, sqlx::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await??;
        writer.commit().await?;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    };
    let write = fixture.store.persist_pod_metadata(
        sandbox_id,
        generation,
        Some("pod-1"),
        None,
        Some(&alpha),
    );
    let (released, written) = tokio::join!(release, write);
    released?;
    written?;

    assert_eq!(
        tracked_pod_uid(&fixture.pool, sandbox_id, generation)
            .await?
            .as_deref(),
        Some("pod-1")
    );
    assert_eq!(record(&fixture.pool, sandbox_id).await?, Some(alpha));
    Ok(())
}

/// With attribution off, filling a pod UID writes only the execution row:
/// the record stays as it was, whatever the pod's label, and the fill takes
/// no `sandboxes` lock, so a held one doesn't block it.
#[tokio::test]
async fn with_attribution_off_the_uid_fill_leaves_the_record_and_its_lock_alone() -> TestResult {
    let fixture = postgres_store_with_max_connections(4).await?;
    let store = K8sStore::new(
        fixture.pool.clone(),
        "space".to_string(),
        crate::controllers::substrate::ActivationClientAttribution::Disabled,
    );
    let sandbox_id = SandboxId::new();
    let pod_name = K8sStore::execution_pod_name(sandbox_id, 1);
    insert_sandbox(&fixture.pool, sandbox_id, "started").await?;
    set_record(&fixture.pool, sandbox_id, Some("previous")).await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions (sandbox_id, generation, pod_name, pod_namespace)
         VALUES ($1, 1, $2, 'space')"
    ))
    .bind(sandbox_id)
    .bind(&pod_name)
    .execute(&fixture.pool)
    .await?;
    let identity = ExecutionIdentity {
        pod_name: PodName::parse(&pod_name)?,
        generation: Some(1),
        activation_attempt_id: None,
        recorded: RecordedExecution::Read {
            pod_uid: None,
            route_activation_id: None,
        },
        pod: ObservedPod::Unobserved,
    };
    // Another transaction holds the sandbox row lock throughout.
    let mut holder = fixture.pool.begin().await?;
    sqlx::query(sql!("SELECT id FROM sandboxes WHERE id = $1 FOR UPDATE"))
        .bind(sandbox_id)
        .execute(&mut *holder)
        .await?;

    let beta = client("beta");
    let filled = tokio::time::timeout(
        Duration::from_secs(5),
        store.backfill_missing_pod_uid(sandbox_id, &identity, "observed-pod", Some(&beta)),
    )
    .await
    .expect("the fill doesn't wait on the sandbox row lock")?;
    holder.rollback().await?;

    assert!(filled);
    assert_eq!(
        record(&fixture.pool, sandbox_id).await?,
        Some(client("previous"))
    );
    Ok(())
}
