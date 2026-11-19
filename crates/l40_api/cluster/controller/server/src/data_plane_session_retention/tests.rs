use std::sync::{Arc, Mutex};

use pplx_space_db_test_fixtures::pg_test_db;
use pplx_space_test_env::TestResult;
use uuid::Uuid;

use super::*;

#[derive(Clone, Default)]
struct FakeSink {
    requests: Arc<Mutex<Vec<(Duration, u32)>>>,
    reports: Arc<Mutex<Vec<TickReport>>>,
}

impl FakeSink {
    fn push_report(&self, report: TickReport) {
        self.reports.lock().unwrap().push(report);
    }

    fn requests(&self) -> Vec<(Duration, u32)> {
        self.requests.lock().unwrap().clone()
    }
}

impl RetentionSink for FakeSink {
    async fn gc_expired_sessions(
        &self,
        grace: Duration,
        max_records: u32,
    ) -> Result<TickReport, sqlx::Error> {
        self.requests.lock().unwrap().push((grace, max_records));
        Ok(self.reports.lock().unwrap().pop().unwrap_or_default())
    }
}

#[tokio::test]
async fn tick_uses_configured_batch_and_grace() {
    let sink = FakeSink::default();
    sink.push_report(TickReport {
        sessions_deleted: 7,
    });
    let reconciler = Reconciler::new(
        ReconcilerConfig {
            tick: Duration::from_secs(60),
            grace: Duration::from_secs(900),
            max_records_per_tick: 42,
        },
        sink.clone(),
    );

    let report = reconciler.tick().await.expect("tick should succeed");

    assert_eq!(sink.requests(), vec![(Duration::from_secs(900), 42)]);
    assert_eq!(
        report,
        TickReport {
            sessions_deleted: 7,
        }
    );
}

/// A guest that predates assigned process ids binds a session to an id it
/// minted, and the process routes find the process through that row for as
/// long as it runs, however long ago its session expired or ended. The sweep
/// keeps such a row until its sandbox is deleted, and prunes every other
/// expired or ended row as before.
#[tokio::test]
async fn sweep_keeps_a_binding_to_a_guest_minted_process_id_until_its_sandbox_goes() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let sandbox_id = seed_sandbox(pool).await?;
    let unbound = "proc_00000000-0000-7000-8000-000000000001";
    let self_bound = "proc_00000000-0000-7000-8000-000000000002";
    let guest_bound = "proc_00000000-0000-7000-8000-000000000003";
    let ended_guest_bound = "proc_00000000-0000-7000-8000-000000000004";
    let live = "proc_00000000-0000-7000-8000-000000000005";
    // (session, state, expired, bound process id, kept)
    let rows = [
        (unbound, "open", true, None, false),
        (self_bound, "open", true, Some(self_bound), false),
        (guest_bound, "open", true, Some("guest-minted-1"), true),
        (
            ended_guest_bound,
            "closed",
            false,
            Some("guest-minted-2"),
            true,
        ),
        (live, "open", false, None, true),
    ];
    for (session_id, state, expired, process_id, _) in rows {
        sqlx::query(sql!(
            "INSERT INTO data_plane_sessions (
                 session_id, org_id, sandbox_id, kind, state, expires_at, updated_at,
                 backend_process_id
             )
             SELECT $1, org_id, id, 'process', $2,
                    NOW() + CASE WHEN $3 THEN INTERVAL '-2 hours' ELSE INTERVAL '20 hours' END,
                    NOW() - INTERVAL '2 hours',
                    $4
               FROM sandboxes
              WHERE id = $5"
        ))
        .bind(session_id)
        .bind(state)
        .bind(expired)
        .bind(process_id)
        .bind(sandbox_id)
        .execute(pool)
        .await?;
    }
    let store = DataPlaneSessionRetentionStore::new(pool.clone());

    let report = store
        .gc_expired_sessions(Duration::from_secs(3600), 100)
        .await?;

    let expected: Vec<&str> = rows.iter().filter(|row| row.4).map(|row| row.0).collect();
    assert_eq!(remaining_sessions(pool).await?, expected);
    assert_eq!(
        report.sessions_deleted,
        (rows.len() - expected.len()) as u64
    );

    sqlx::query(sql!("DELETE FROM sandboxes WHERE id = $1"))
        .bind(sandbox_id)
        .execute(pool)
        .await?;
    assert_eq!(remaining_sessions(pool).await?, Vec::<String>::new());
    Ok(())
}

async fn remaining_sessions(pool: &PgPool) -> Result<Vec<String>, sqlx::Error> {
    sqlx::query_scalar(sql!(
        "SELECT session_id FROM data_plane_sessions ORDER BY session_id"
    ))
    .fetch_all(pool)
    .await
}

async fn seed_sandbox(pool: &PgPool) -> Result<Uuid, sqlx::Error> {
    let org_id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO organizations (id, name, slug) VALUES ($1, $2, $3)"
    ))
    .bind(org_id)
    .bind(format!("Org {org_id}"))
    .bind(format!("org-{}", org_id.simple()))
    .execute(pool)
    .await?;
    let user_id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO users (id, email, password_hash, display_name, org_id, role)
         VALUES ($1, $2, 'hash', 'Test User', $3, 'member')"
    ))
    .bind(user_id)
    .bind(format!("{user_id}@example.test"))
    .bind(org_id)
    .execute(pool)
    .await?;
    let sandbox_id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (id, user_id, org_id, network_policy_generation)
         VALUES ($1, $2, $3, 1)"
    ))
    .bind(sandbox_id)
    .bind(user_id)
    .bind(org_id)
    .execute(pool)
    .await?;
    Ok(sandbox_id)
}
