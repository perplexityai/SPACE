use pplx_space_common_core::types::{SandboxLifecycle, SubstrateKind};

use std::collections::BTreeMap;

use pplx_space_common_core::sql;
use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, try_pg_test_db};
use pplx_space_metrics::sandbox::transient_lifecycle::LifecycleAge;
use pplx_space_metrics::test_capture::{CapturedValue, capture_values_guard};
use pplx_space_test_env::TestResult;
use uuid::Uuid;

use super::tick;

const ROWS: &str = "sandbox.transient_lifecycle.rows";
const OLDEST: &str = "sandbox.transient_lifecycle.oldest_age_seconds";

fn tag_value<'a>(value: &'a CapturedValue, key: &str) -> Option<&'a str> {
    value
        .tags
        .iter()
        .find_map(|tag| tag.strip_prefix(key)?.strip_prefix(':'))
}

/// A tick over a table with one `restoring` row two minutes into its
/// lifecycle reports that row in `restoring`/`1m_10m`, and zero for every
/// other transient lifecycle and bucket.
#[tokio::test(flavor = "current_thread")]
async fn a_tick_reports_every_pair_from_the_rows() -> TestResult {
    Box::pin(async {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        sqlx::query(sql!(
            "INSERT INTO sandboxes
                (id, user_id, org_id, managed_by, lifecycle, template_id,
                 network_policy_generation, lifecycle_changed_at)
             VALUES ($1, $2, $3, $4, 'restoring', 'tmpl-census-test', 1,
                     NOW() - INTERVAL '2 minutes')"
        ))
        .bind(Uuid::now_v7())
        .bind(SEED_USER_ID)
        .bind(SEED_ORG_ID)
        .bind(SubstrateKind::Kubernetes)
        .execute(&db.pool)
        .await?;

        let capture = capture_values_guard();
        tick(&db.pool).await?;
        let values = capture.finish();

        let mut rows = BTreeMap::new();
        let mut oldest = BTreeMap::new();
        for value in &values {
            let lifecycle = tag_value(value, "lifecycle").expect("lifecycle tag");
            match value.name.as_str() {
                ROWS => {
                    let age = tag_value(value, "age").expect("age tag");
                    rows.insert((lifecycle, age), value.value);
                }
                OLDEST => {
                    oldest.insert(lifecycle, value.value);
                }
                other => panic!("unexpected metric {other}"),
            }
        }
        let transient = SandboxLifecycle::all()
            .iter()
            .filter(|lifecycle| lifecycle.is_transient())
            .count();
        assert_eq!(rows.len(), transient * LifecycleAge::ALL.len(), "{rows:?}");
        assert_eq!(oldest.len(), transient, "{oldest:?}");
        for ((lifecycle, age), value) in &rows {
            let expected = if (*lifecycle, *age) == ("restoring", "1m_10m") {
                1.0
            } else {
                0.0
            };
            assert_eq!(*value, expected, "{lifecycle}/{age}");
        }
        let restoring = oldest["restoring"];
        assert!((120.0..180.0).contains(&restoring), "{restoring}");
        assert_eq!(oldest["deleting"], 0.0);
        Ok(())
    })
    .await
}

/// A failed read emits nothing, rather than zeros it did not read.
#[tokio::test(flavor = "current_thread")]
async fn a_failed_read_emits_nothing() -> TestResult {
    Box::pin(async {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        db.pool.close().await;

        let capture = capture_values_guard();
        let result = tick(&db.pool).await;
        let values = capture.finish();

        assert!(result.is_err(), "a closed pool must fail the read");
        assert!(values.is_empty(), "{values:?}");
        Ok(())
    })
    .await
}
