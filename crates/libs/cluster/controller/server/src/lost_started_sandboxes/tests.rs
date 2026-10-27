//! # Test matrix
//!
//! ## C - `ReconcilerConfig`
//! - C1 `c1_disabled_by_default_and_enabled_by_env`
//!
//! ## T - Tick behavior (fake sink, node presence and EC2 lookup)
//! - T1 `t1_node_evidence_table`
//! - T2 `t2_raced_apply_is_not_counted_as_lost`
//! - T3 `t3_per_tick_cap_rate_limits_the_rest`
//!
//! ## R - Postgres-backed sink
//! - R1 `r1_lists_a_refused_recovery_with_its_lost_node`
//! - R2 `r2_mark_lost_fails_the_sandbox_and_keeps_its_snapshot`
//! - R3 `r3_guard_table_keeps_rows_that_are_not_lost`
//! - R4 `r4_mark_lost_refuses_a_row_that_changed_after_the_scan`

use std::collections::HashSet;
use std::sync::Mutex as StdMutex;

use super::*;

#[test]
fn c1_disabled_by_default_and_enabled_by_env() {
    let mut env = pplx_space_test_env::TestEnv::default();
    assert!(!ReconcilerConfig::from_environment(&env).enabled);

    env.set("SPACE_LOST_STARTED_SANDBOX_RECONCILER_ENABLED", "true");
    assert!(ReconcilerConfig::from_environment(&env).enabled);
}

#[derive(Default)]
struct FakeSink {
    pages: StdMutex<Vec<Vec<LostStartedCandidate>>>,
    mark_result: StdMutex<Option<bool>>,
    marked: StdMutex<Vec<SandboxId>>,
}

impl FakeSink {
    fn with_page(candidates: Vec<LostStartedCandidate>) -> Self {
        let sink = Self::default();
        sink.pages.lock().unwrap().push(candidates);
        *sink.mark_result.lock().unwrap() = Some(true);
        sink
    }

    fn marked(&self) -> Vec<SandboxId> {
        self.marked.lock().unwrap().clone()
    }
}

impl LostStartedSink for Arc<FakeSink> {
    async fn list_candidates(
        &self,
        _limit: i64,
        _after: Option<SandboxId>,
        _min_age: Duration,
    ) -> Result<Vec<LostStartedCandidate>, sqlx::Error> {
        Ok(self.pages.lock().unwrap().pop().unwrap_or_default())
    }

    async fn mark_lost(&self, candidate: &LostStartedCandidate) -> Result<bool, sqlx::Error> {
        let result = (*self.mark_result.lock().unwrap()).ok_or(sqlx::Error::PoolTimedOut)?;
        if result {
            self.marked.lock().unwrap().push(candidate.sandbox_id);
        }
        Ok(result)
    }
}

#[derive(Debug, Clone, Copy)]
enum Answer<T> {
    Ok(T),
    Err,
}

#[derive(Debug, thiserror::Error)]
#[error("fake lookup failure")]
struct FakeLookupError;

struct FakePresence(Answer<bool>);

impl KubeNodePresence for FakePresence {
    type LookupError = FakeLookupError;

    fn live_node_names(&self) -> Option<HashSet<String>> {
        None
    }

    async fn node_exists(&self, _node_name: String) -> Result<bool, Self::LookupError> {
        match self.0 {
            Answer::Ok(exists) => Ok(exists),
            Answer::Err => Err(FakeLookupError),
        }
    }
}

#[derive(Clone, Copy)]
enum Ec2 {
    Gone,
    Live,
    Err,
}

struct FakeLookup {
    answer: Ec2,
    calls: StdMutex<usize>,
}

impl InstanceLookup for Arc<FakeLookup> {
    async fn lookup_by_private_dns(
        &self,
        _private_dns_name: String,
    ) -> Result<InstanceLookupOutcome, crate::orphaned_nodes::InstanceLookupError> {
        *self.calls.lock().unwrap() += 1;
        match self.answer {
            Ec2::Gone => Ok(InstanceLookupOutcome::Gone),
            Ec2::Live => Ok(InstanceLookupOutcome::Live {
                instance_id: "i-0000000000example".to_string(),
                state: "running".to_string(),
            }),
            Ec2::Err => Err(crate::orphaned_nodes::InstanceLookupError::Backend(
                "unavailable".to_string(),
            )),
        }
    }
}

fn candidate() -> LostStartedCandidate {
    LostStartedCandidate {
        sandbox_id: SandboxId::new(),
        recovery_generation: 2,
        lost_node_name: "node-a.example.test".to_string(),
    }
}

fn config() -> ReconcilerConfig {
    ReconcilerConfig {
        enabled: true,
        ..ReconcilerConfig::default()
    }
}

#[tokio::test(flavor = "current_thread")]
async fn t1_node_evidence_table() {
    // (kube Node exists, EC2 verdict, failed, EC2 consulted)
    let cases = [
        (Answer::Ok(false), Ec2::Gone, true, true),
        (Answer::Ok(true), Ec2::Gone, false, false),
        (Answer::Err, Ec2::Gone, false, false),
        (Answer::Ok(false), Ec2::Live, false, true),
        (Answer::Ok(false), Ec2::Err, false, true),
    ];
    for (kube, ec2, expect_failed, expect_ec2_call) in cases {
        let row = candidate();
        let sink = Arc::new(FakeSink::with_page(vec![row.clone()]));
        let lookup = Arc::new(FakeLookup {
            answer: ec2,
            calls: StdMutex::new(0),
        });
        let mut reconciler = Reconciler::new(
            config(),
            Arc::clone(&sink),
            FakePresence(kube),
            Arc::clone(&lookup),
        );

        let report = reconciler.tick().await;

        let case = format!("kube {kube:?}");
        assert_eq!(
            sink.marked(),
            if expect_failed {
                vec![row.sandbox_id]
            } else {
                vec![]
            },
            "{case}"
        );
        assert_eq!(report.marked_lost, usize::from(expect_failed), "{case}");
        assert_eq!(*lookup.calls.lock().unwrap() > 0, expect_ec2_call, "{case}");
    }
}

#[tokio::test(flavor = "current_thread")]
async fn t2_raced_apply_is_not_counted_as_lost() {
    let sink = Arc::new(FakeSink::with_page(vec![candidate()]));
    *sink.mark_result.lock().unwrap() = Some(false);
    let lookup = Arc::new(FakeLookup {
        answer: Ec2::Gone,
        calls: StdMutex::new(0),
    });
    let mut reconciler = Reconciler::new(
        config(),
        Arc::clone(&sink),
        FakePresence(Answer::Ok(false)),
        lookup,
    );

    let report = reconciler.tick().await;

    assert_eq!(report.marked_lost, 0);
    assert_eq!(report.raced, 1);
}

#[tokio::test(flavor = "current_thread")]
async fn t3_per_tick_cap_rate_limits_the_rest() {
    let rows = vec![candidate(), candidate(), candidate()];
    let sink = Arc::new(FakeSink::with_page(rows.clone()));
    let lookup = Arc::new(FakeLookup {
        answer: Ec2::Gone,
        calls: StdMutex::new(0),
    });
    let mut reconciler = Reconciler::new(
        ReconcilerConfig {
            max_per_tick: 2,
            ..config()
        },
        Arc::clone(&sink),
        FakePresence(Answer::Ok(false)),
        lookup,
    );

    let report = reconciler.tick().await;

    assert_eq!(sink.marked(), vec![rows[0].sandbox_id, rows[1].sandbox_id]);
    assert_eq!(report.rate_limited, 1);
}

mod postgres_backed {
    use super::*;
    use pplx_space_common_core::types::{LocalArtifactId, SnapshotId};
    use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, try_pg_test_db};
    use pplx_space_snapshot_retention::{RetentionSink, SnapshotRetentionDbStore};
    use pplx_space_test_env::TestResult;
    use sqlx::PgPool;

    const LOST_NODE: &str = "node-a.example.test";
    const MIN_AGE: Duration = Duration::from_secs(600);

    /// A `started` sandbox whose generation-1 execution ran on [`LOST_NODE`]
    /// and was forgotten, and whose generation-2 recovery refused: the shape
    /// the API's started recovery leaves when the only suspend snapshot
    /// predates the last access.
    #[derive(Clone, Copy)]
    struct LostSpec {
        /// The snapshot also covers the last access, so recovery could restore.
        snapshot_covers_activity: bool,
        /// A suspend upload for the current artifact is still `creating`.
        upload_in_flight: bool,
        /// Seconds since the lost execution was retired.
        retired_age_secs: i64,
        /// State of the generation-2 execution.
        recovery_state: &'static str,
    }

    impl LostSpec {
        fn lost() -> Self {
            Self {
                snapshot_covers_activity: false,
                upload_in_flight: false,
                retired_age_secs: 3600,
                recovery_state: "recovery_failed",
            }
        }
    }

    struct Seeded {
        sandbox_id: SandboxId,
        artifact_id: LocalArtifactId,
        snapshot_id: SnapshotId,
    }

    async fn seed(pool: &PgPool, spec: LostSpec) -> TestResult<Seeded> {
        let sandbox_id = SandboxId::new();
        let artifact_id = LocalArtifactId::new();
        let snapshot_id = SnapshotId::new();
        let access_age_secs: i64 = if spec.snapshot_covers_activity {
            7200
        } else {
            1800
        };
        sqlx::query(
            "INSERT INTO sandboxes
                (id, user_id, org_id, managed_by, lifecycle, placement,
                 network_policy_generation, current_local_artifact_id,
                 current_local_artifact_kind, current_local_artifact_origin,
                 last_access_at)
             VALUES ($1, $2::uuid, $3::uuid, 'k8s', 'started', 'unplaced', 1, $4,
                     'full', 'suspend', NOW() - make_interval(secs => $5::BIGINT))",
        )
        .bind(sandbox_id)
        .bind(SEED_USER_ID)
        .bind(SEED_ORG_ID)
        .bind(artifact_id)
        .bind(access_age_secs)
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO snapshots
                (id, sandbox_id, org_id, type, state, timestamp,
                 local_artifact_id, artifact_kind, artifact_origin)
             VALUES ($1, $2, $3::uuid, 'full', 'ready', NOW() - INTERVAL '1 hour',
                     $4, 'full', 'suspend')",
        )
        .bind(snapshot_id)
        .bind(sandbox_id)
        .bind(SEED_ORG_ID)
        .bind(artifact_id)
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string)
             VALUES ($1, 's3://bucket-a/snapshot')",
        )
        .bind(snapshot_id)
        .execute(pool)
        .await?;
        if spec.upload_in_flight {
            sqlx::query(
                "INSERT INTO snapshots
                    (id, sandbox_id, org_id, type, state, timestamp,
                     local_artifact_id, artifact_kind, artifact_origin)
                 VALUES ($1, $2, $3::uuid, 'full', 'creating', NOW(), $4, 'full', 'suspend')",
            )
            .bind(SnapshotId::new())
            .bind(sandbox_id)
            .bind(SEED_ORG_ID)
            .bind(artifact_id)
            .execute(pool)
            .await?;
        }
        let compact = sandbox_id.as_uuid().simple();
        sqlx::query(
            "INSERT INTO sandbox_k8s_executions
                (sandbox_id, generation, pod_name, pod_namespace, pod_uid, k8s_node_name,
                 state, active, terminated_at)
             VALUES ($1, 1, $2, 'sandboxes', 'uid-1', $3, 'forgotten', FALSE,
                     NOW() - make_interval(secs => $4::BIGINT))",
        )
        .bind(sandbox_id)
        .bind(format!("space-sbx-{compact}-g1"))
        .bind(LOST_NODE)
        .bind(spec.retired_age_secs)
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO sandbox_k8s_executions
                (sandbox_id, generation, pod_name, pod_namespace, state, active)
             VALUES ($1, 2, $2, 'sandboxes', $3, TRUE)",
        )
        .bind(sandbox_id)
        .bind(format!("space-sbx-{compact}-g2"))
        .bind(spec.recovery_state)
        .execute(pool)
        .await?;
        Ok(Seeded {
            sandbox_id,
            artifact_id,
            snapshot_id,
        })
    }

    async fn listed(store: &LostStartedDbStore) -> TestResult<Vec<LostStartedCandidate>> {
        Ok(store.list_candidates(100, None, MIN_AGE).await?)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn r1_lists_a_refused_recovery_with_its_lost_node() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let seeded = seed(&db.pool, LostSpec::lost()).await?;

        let candidates = listed(&LostStartedDbStore::new(db.pool.clone())).await?;

        assert_eq!(
            candidates,
            vec![LostStartedCandidate {
                sandbox_id: seeded.sandbox_id,
                recovery_generation: 2,
                lost_node_name: LOST_NODE.to_string(),
            }]
        );
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn r2_mark_lost_fails_the_sandbox_and_keeps_its_snapshot() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let seeded = seed(&db.pool, LostSpec::lost()).await?;
        let lost_node = pplx_space_common_core::types::NodeId::new();
        sqlx::query(
            "INSERT INTO nodes (id, address, role, managed_by, state)
             VALUES ($1, '10.0.0.99'::inet, 'worker', 'k8s', 'registered')",
        )
        .bind(lost_node)
        .execute(&db.pool)
        .await?;
        sqlx::query("INSERT INTO node_k8s (node_id, k8s_node_name) VALUES ($1, $2)")
            .bind(lost_node)
            .bind(LOST_NODE)
            .execute(&db.pool)
            .await?;
        sqlx::query(
            "INSERT INTO sandbox_observed_states (sandbox_id, node_id, status, observed_at, updated_at)
             VALUES ($1, $2, 'started', NOW(), NOW())",
        )
        .bind(seeded.sandbox_id)
        .bind(lost_node)
        .execute(&db.pool)
        .await?;
        let store = LostStartedDbStore::new(db.pool.clone());
        let candidate = listed(&store).await?.remove(0);

        assert!(store.mark_lost(&candidate).await?);

        let observed: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sandbox_observed_states WHERE sandbox_id = $1",
        )
        .bind(seeded.sandbox_id)
        .fetch_one(&db.pool)
        .await?;
        assert_eq!(
            observed, 0,
            "the gone node's report must not keep reads at running"
        );

        let (lifecycle, marker): (String, Option<LocalArtifactId>) = sqlx::query_as(
            "SELECT lifecycle, current_local_artifact_id FROM sandboxes WHERE id = $1",
        )
        .bind(seeded.sandbox_id)
        .fetch_one(&db.pool)
        .await?;
        assert_eq!(lifecycle, "failed");
        assert_eq!(marker, Some(seeded.artifact_id));
        let active: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sandbox_k8s_executions WHERE sandbox_id = $1 AND active",
        )
        .bind(seeded.sandbox_id)
        .fetch_one(&db.pool)
        .await?;
        assert_eq!(active, 0);
        let (state, location): (String, Option<String>) = sqlx::query_as(
            "SELECT sn.state, ssl.storage_connection_string
               FROM snapshots sn
               LEFT JOIN snapshot_storage_locations ssl
                 ON ssl.snapshot_id = sn.id AND ssl.orphaned_at IS NULL
              WHERE sn.id = $1",
        )
        .bind(seeded.snapshot_id)
        .fetch_one(&db.pool)
        .await?;
        assert_eq!(state, "ready");
        assert_eq!(location.as_deref(), Some("s3://bucket-a/snapshot"));

        // Retention never offers the kept snapshot, even keeping only one.
        let page = SnapshotRetentionDbStore::new(db.pool.clone())
            .list_candidates(1, 100, None, None)
            .await?;
        assert!(
            page.candidates
                .iter()
                .all(|candidate| candidate.snapshot_id != seeded.snapshot_id)
        );
        // Lost is terminal for this loop: the row no longer qualifies.
        assert!(listed(&store).await?.is_empty());
        // Nor does the failed-row repair restore the older snapshot once the
        // flip is past its minimum age.
        sqlx::query(
            "UPDATE sandboxes SET lifecycle_changed_at = NOW() - INTERVAL '1 day' WHERE id = $1",
        )
        .bind(seeded.sandbox_id)
        .execute(&db.pool)
        .await?;
        let repairable = crate::failed_restorable_repair::RepairDbStore::new(db.pool.clone())
            .list_candidates(
                100,
                None,
                pplx_space_common_core::types::PositiveDuration::new(Duration::from_secs(1))
                    .expect("positive"),
            )
            .await?;
        assert!(
            repairable
                .iter()
                .all(|candidate| candidate.sandbox_id != seeded.sandbox_id)
        );
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn r3_guard_table_keeps_rows_that_are_not_lost() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let cases = [
            (
                "snapshot covers the last access",
                LostSpec {
                    snapshot_covers_activity: true,
                    ..LostSpec::lost()
                },
            ),
            (
                "suspend upload in flight",
                LostSpec {
                    upload_in_flight: true,
                    ..LostSpec::lost()
                },
            ),
            (
                "lost execution retired too recently",
                LostSpec {
                    retired_age_secs: 60,
                    ..LostSpec::lost()
                },
            ),
            (
                "recovery still in progress",
                LostSpec {
                    recovery_state: "recovering",
                    ..LostSpec::lost()
                },
            ),
        ];
        let store = LostStartedDbStore::new(db.pool.clone());
        for (case, spec) in cases {
            let seeded = seed(&db.pool, spec).await?;

            let listed = listed(&store).await?;

            assert!(
                listed.iter().all(|c| c.sandbox_id != seeded.sandbox_id),
                "{case}"
            );
            // The age gate is a scan-only throttle; every other guard also
            // refuses at apply time.
            if spec.retired_age_secs >= MIN_AGE.as_secs() as i64 {
                let applied = store
                    .mark_lost(&LostStartedCandidate {
                        sandbox_id: seeded.sandbox_id,
                        recovery_generation: 2,
                        lost_node_name: LOST_NODE.to_string(),
                    })
                    .await?;
                assert!(!applied, "{case}");
                let lifecycle: String =
                    sqlx::query_scalar("SELECT lifecycle FROM sandboxes WHERE id = $1")
                        .bind(seeded.sandbox_id)
                        .fetch_one(&db.pool)
                        .await?;
                assert_eq!(lifecycle, "started", "{case}");
            }
        }
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn r4_mark_lost_refuses_a_row_that_changed_after_the_scan() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let store = LostStartedDbStore::new(db.pool.clone());
        let seeded = seed(&db.pool, LostSpec::lost()).await?;
        let candidate = listed(&store).await?.remove(0);

        // A different node recorded for the lost execution.
        let elsewhere = LostStartedCandidate {
            lost_node_name: "node-b.example.test".to_string(),
            ..candidate.clone()
        };
        assert!(!store.mark_lost(&elsewhere).await?);
        // A newer recovery generation claimed in the meantime.
        sqlx::query(
            "UPDATE sandbox_k8s_executions SET active = FALSE
              WHERE sandbox_id = $1 AND generation = 2",
        )
        .bind(seeded.sandbox_id)
        .execute(&db.pool)
        .await?;
        sqlx::query(
            "INSERT INTO sandbox_k8s_executions
                (sandbox_id, generation, pod_name, pod_namespace, state, active)
             VALUES ($1, 3, $2, 'sandboxes', 'recovering', TRUE)",
        )
        .bind(seeded.sandbox_id)
        .bind(format!(
            "space-sbx-{}-g3",
            seeded.sandbox_id.as_uuid().simple()
        ))
        .execute(&db.pool)
        .await?;
        assert!(!store.mark_lost(&candidate).await?);

        let lifecycle: String = sqlx::query_scalar("SELECT lifecycle FROM sandboxes WHERE id = $1")
            .bind(seeded.sandbox_id)
            .fetch_one(&db.pool)
            .await?;
        assert_eq!(lifecycle, "started");
        Ok(())
    }
}
