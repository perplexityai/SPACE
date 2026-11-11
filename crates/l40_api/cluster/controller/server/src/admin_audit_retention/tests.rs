use std::sync::{Arc, Mutex};

use pplx_space_db_test_fixtures::try_pg_test_db;
use uuid::Uuid;

use super::*;

use pplx_space_test_env::TestResult;

#[derive(Clone, Default)]
struct FakeSink {
    requests: Arc<Mutex<Vec<(Duration, u32, u32)>>>,
    reports: Arc<Mutex<Vec<TickReport>>>,
}

impl FakeSink {
    fn push_report(&self, report: TickReport) {
        self.reports.lock().unwrap().push(report);
    }

    fn requests(&self) -> Vec<(Duration, u32, u32)> {
        self.requests.lock().unwrap().clone()
    }
}

impl RetentionSink for FakeSink {
    async fn prune_audit_events(
        &self,
        retention: Duration,
        max_records: u32,
        max_delete_per_tick: u32,
    ) -> Result<TickReport, sqlx::Error> {
        self.requests
            .lock()
            .unwrap()
            .push((retention, max_records, max_delete_per_tick));
        Ok(self.reports.lock().unwrap().pop().unwrap_or_default())
    }
}

#[tokio::test(flavor = "current_thread")]
async fn tick_uses_configured_limits() {
    let sink = FakeSink::default();
    sink.push_report(TickReport {
        aged_deleted: 1,
        overflow_deleted: 2,
        retained_rows: 5,
    });
    let reconciler = Reconciler::new(
        ReconcilerConfig {
            tick: Duration::from_secs(10),
            retention: Duration::from_secs(60),
            max_records: 100,
            max_delete_per_tick: 7,
        },
        sink.clone(),
    );

    let report = reconciler.tick().await.unwrap();

    assert_eq!(report.aged_deleted, 1);
    assert_eq!(report.overflow_deleted, 2);
    assert_eq!(sink.requests(), vec![(Duration::from_secs(60), 100, 7)]);
}

#[test]
fn config_defaults_to_bounded_retention() {
    let config = ReconcilerConfig::default();

    assert_eq!(config.tick, DEFAULT_TICK);
    assert_eq!(config.retention, DEFAULT_RETENTION);
    assert_eq!(config.max_records, DEFAULT_MAX_RECORDS);
    assert_eq!(config.max_delete_per_tick, DEFAULT_MAX_DELETE_PER_TICK);
}

#[tokio::test(flavor = "current_thread")]
async fn store_prunes_expired_audit_events() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let store = AdminAuditRetentionStore::new(db.pool.clone());
    insert_audit_event(&db.pool, "GET", "/old", 40).await?;
    insert_audit_event(&db.pool, "DELETE", "/recent", 1).await?;

    let report = store
        .prune_audit_events(Duration::from_secs(30 * 24 * 60 * 60), 100, 100)
        .await?;

    assert_eq!(report.aged_deleted, 1);
    assert_eq!(report.overflow_deleted, 0);
    assert_eq!(report.retained_rows, 1);
    assert_eq!(audit_event_count(&db.pool).await?, 1);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn store_caps_audit_events_by_newest_rows() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let store = AdminAuditRetentionStore::new(db.pool.clone());
    insert_audit_event(&db.pool, "DELETE", "/oldest", 3).await?;
    insert_audit_event(&db.pool, "POST", "/middle", 2).await?;
    insert_audit_event(&db.pool, "PATCH", "/newest", 1).await?;

    let report = store
        .prune_audit_events(Duration::from_secs(90 * 24 * 60 * 60), 2, 100)
        .await?;

    assert_eq!(report.aged_deleted, 0);
    assert_eq!(report.overflow_deleted, 1);
    assert_eq!(report.retained_rows, 2);
    assert_eq!(audit_event_count(&db.pool).await?, 2);
    assert!(!audit_path_exists(&db.pool, "/oldest").await?);
    assert!(audit_path_exists(&db.pool, "/middle").await?);
    assert!(audit_path_exists(&db.pool, "/newest").await?);
    Ok(())
}

async fn insert_audit_event(
    pool: &PgPool,
    method: &str,
    path: &str,
    age_days: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "INSERT INTO admin_audit_events (
             id,
             actor_user_id,
             actor_org_id,
             actor_email,
             method,
             path,
             status_code,
             success,
             duration_ms,
             created_at
         )
         VALUES (
             $1,
             $2,
             $3,
             'admin@example.test',
             $4,
             $5,
             200,
             TRUE,
             1,
             NOW() - ($6::BIGINT * INTERVAL '1 day')
         )"
    ))
    .bind(Uuid::now_v7())
    .bind(Uuid::now_v7())
    .bind(Uuid::now_v7())
    .bind(method)
    .bind(path)
    .bind(age_days)
    .execute(pool)
    .await?;
    Ok(())
}

async fn audit_event_count(pool: &PgPool) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(sql!("SELECT COUNT(*) FROM admin_audit_events"))
        .fetch_one(pool)
        .await
}

async fn audit_path_exists(pool: &PgPool, path: &str) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(sql!(
        "SELECT EXISTS(SELECT 1 FROM admin_audit_events WHERE path = $1)"
    ))
    .bind(path)
    .fetch_one(pool)
    .await
}
