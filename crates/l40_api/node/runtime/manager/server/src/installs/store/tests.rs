use super::*;
#[tokio::test]
async fn a_job_result_is_replaced_as_the_job_progresses() {
    let dir = tempfile::tempdir().unwrap();
    let conn = open(dir.path()).await.unwrap();
    let job_id = JobId::new();
    assert_eq!(get_job_result(&conn, &job_id).unwrap(), None);

    let path = PathBuf::from("/space/vendor/kernels/6.1.155");
    let digest = Digest::sha256([0xcd; 32]);
    let publishing = JobResult::InProgress {
        phase: JobPhase::Publishing,
        path: path.clone(),
        digest,
    };
    put_job_result(&conn, &job_id, &publishing, jiff::Timestamp::now()).unwrap();
    assert_eq!(get_job_result(&conn, &job_id).unwrap(), Some(publishing));

    let installed = JobResult::Done {
        outcome: JobOutcome::Installed,
        path,
        digest,
    };
    put_job_result(&conn, &job_id, &installed, jiff::Timestamp::now()).unwrap();
    assert_eq!(get_job_result(&conn, &job_id).unwrap(), Some(installed));

    delete_orphaned_job_results(&conn).unwrap();
    assert_eq!(get_job_result(&conn, &job_id).unwrap(), None);
}

#[tokio::test]
async fn the_schema_refuses_a_result_that_is_both_or_neither() {
    let dir = tempfile::tempdir().unwrap();
    let conn = open(dir.path()).await.unwrap();
    for (phase, outcome) in [(Some("publishing"), Some("installed")), (None, None)] {
        conn.execute(
            "INSERT INTO operation_results (job_id, phase, outcome, path, digest, updated_at_unix_ms)
             VALUES (?1, ?2, ?3, '/p', 'sha256:00', 0)",
            rusqlite::params![JobId::new(), phase, outcome],
        )
        .unwrap_err();
    }
}

#[test]
fn closed_vocabularies_round_trip_through_text() {
    for phase in [JobPhase::Publishing, JobPhase::Removing] {
        assert_eq!(phase.to_string().parse::<JobPhase>().unwrap(), phase);
    }
    for outcome in [
        JobOutcome::Installed,
        JobOutcome::AlreadyInstalled,
        JobOutcome::Uninstalled,
    ] {
        assert_eq!(outcome.to_string().parse::<JobOutcome>().unwrap(), outcome);
    }
    "rolling-back".parse::<JobPhase>().unwrap_err();
    "kept".parse::<JobOutcome>().unwrap_err();
}

#[tokio::test]
async fn results_of_pruned_jobs_are_dropped() {
    let dir = tempfile::tempdir().unwrap();
    let conn = open(dir.path()).await.unwrap();
    let kept = JobId::new();
    pplx_space_common_jobs::db::insert_job(
        &conn,
        pplx_space_common_jobs::db::InsertJob {
            job_id: &kept,
            kind: crate::installs::operations::INSTALL_JOB_KIND,
            payload: "{}",
            subject_id: None,
            dedup_key: None,
            max_attempts: 1,
            traceparent: None,
            priority: pplx_space_common_jobs::JobPriority::default(),
        },
    )
    .unwrap();
    let pruned = JobId::new();
    for job_id in [&kept, &pruned] {
        let result = JobResult::Done {
            outcome: JobOutcome::Uninstalled,
            path: PathBuf::from("/space/vendor/kernels/6.1.155"),
            digest: Digest::sha256([0xab; 32]),
        };
        put_job_result(&conn, job_id, &result, jiff::Timestamp::now()).unwrap();
    }
    assert_eq!(delete_orphaned_job_results(&conn).unwrap(), 1);
    assert!(get_job_result(&conn, &kept).unwrap().is_some());
    assert!(get_job_result(&conn, &pruned).unwrap().is_none());
}

#[tokio::test]
async fn kernel_state_survives_reopening_and_rejected_migration_history() {
    use std::os::unix::fs::PermissionsExt as _;
    let dir = tempfile::tempdir().unwrap();
    let job_id = JobId::new();
    let result = JobResult::InProgress {
        phase: JobPhase::Publishing,
        path: PathBuf::from("/node/kernels/6.1.158"),
        digest: Digest::sha256([7; 32]),
    };
    let conn = open(dir.path()).await.unwrap();
    put_job_result(&conn, &job_id, &result, jiff::Timestamp::now()).unwrap();
    drop(conn);

    let conn = open(dir.path()).await.unwrap();
    assert_eq!(
        get_job_result(&conn, &job_id).unwrap(),
        Some(result.clone())
    );
    let mode = std::fs::metadata(dir.path().join(DATABASE_FILE))
        .unwrap()
        .permissions()
        .mode();
    assert_eq!(mode & 0o777, 0o600);

    conn.execute_batch(
        "UPDATE _space_common_schema_migrations SET checksum = x'00'
         WHERE version = (SELECT MAX(version) FROM _space_common_schema_migrations);",
    )
    .unwrap();
    let history = |conn: &Connection| {
        conn.prepare(
            "SELECT version, checksum FROM _space_common_schema_migrations ORDER BY version",
        )
        .unwrap()
        .query_map([], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
    };
    let before = history(&conn);
    drop(conn);

    open(dir.path()).await.unwrap_err();

    let conn = Connection::open(dir.path().join(DATABASE_FILE)).unwrap();
    assert_eq!(history(&conn), before);
    assert_eq!(get_job_result(&conn, &job_id).unwrap(), Some(result));
}

#[tokio::test]
async fn a_database_that_cannot_be_migrated_fails_instead_of_being_recreated() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(DATABASE_FILE);
    std::fs::write(&path, b"not a database, and not ours to discard").unwrap();
    open(dir.path()).await.unwrap_err();
    // Left in place for an operator: recreating it would lose durable operation intent.
    assert_eq!(
        std::fs::read(&path).unwrap(),
        b"not a database, and not ours to discard"
    );
}

// Use each actually deployed baseline, with real SQLx history, rather than a
// hand-stamped version that could hide a checksum incompatibility.
async fn prior_database(dir: &Path, prototype: bool) -> Connection {
    use sqlx::SqlSafeStr as _;
    use sqlx::migrate::{Migration, MigrationType, Migrator};
    let baseline = if prototype {
        include_str!("../../../../../../../../../db/runtime-manager/history/prototype-baseline.sql")
    } else {
        include_str!("../../../../../../../../../db/runtime-manager/20260930000000_baseline.up.sql")
    };
    let migrator = Migrator {
        migrations: vec![Migration::new(
            20260930000000,
            "baseline".into(),
            MigrationType::ReversibleUp,
            baseline.into_sql_str(),
            false,
        )]
        .into(),
        ..Migrator::DEFAULT
    };
    let path = dir.join(DATABASE_FILE);
    pplx_space_sqlite::migrate_embedded(
        DATABASE_NAME,
        &path,
        pplx_space_sqlite::EmbeddedMigrators {
            service: &migrator,
            common: Some(&COMMON_SCHEMA_MIGRATOR),
        },
    )
    .await
    .unwrap();
    Connection::open(path).unwrap()
}

fn baseline_history(conn: &Connection) -> (String, Vec<u8>, i64) {
    conn.query_row(
        "SELECT installed_on, checksum, execution_time FROM _sqlx_migrations WHERE version = 20260930000000",
        [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    ).unwrap()
}

#[tokio::test]
async fn both_deployed_baselines_reset_prototype_state_once_and_preserve_history() {
    for prototype in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let conn = prior_database(dir.path(), prototype).await;
        let history = baseline_history(&conn);
        let job = JobId::new();
        conn.execute(
            "INSERT INTO jobs (job_id, kind, status, payload) VALUES (?1, 'artifact-install', 'running', 'obsolete request')",
            params![job],
        ).unwrap();
        let result = JobResult::InProgress {
            phase: JobPhase::Publishing,
            path: PathBuf::from("/space/vendor/artifacts/kernels/6.1.172"),
            digest: Digest::sha256([8; 32]),
        };
        if prototype {
            put_job_result(&conn, &job, &result, jiff::Timestamp::now()).unwrap();
        } else {
            conn.execute_batch(
                "INSERT INTO kernel_installs VALUES ('6.1.155', '/old/kernel', 'firecracker', '1.15', 'source', 42, 'md5', 'digest', NULL, 123);
                 INSERT INTO kernel_job_results VALUES ('old-result', 'publishing', NULL, '/old/kernel', 'digest', 123);",
            ).unwrap();
        }
        drop(conn);
        let conn = open(dir.path()).await.unwrap();
        assert_eq!(baseline_history(&conn), history);
        assert_eq!(
            conn.query_row("SELECT COUNT(*) FROM jobs", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            0
        );
        assert_eq!(get_job_result(&conn, &job).unwrap(), None);
        assert_eq!(conn.query_row("SELECT COUNT(*) FROM sqlite_schema WHERE name IN ('kernel_installs', 'kernel_job_results')", [], |r| r.get::<_, i64>(0)).unwrap(), 0);
        put_job_result(&conn, &job, &result, jiff::Timestamp::now()).unwrap();
        conn.execute(
            "INSERT INTO jobs (job_id, kind) VALUES (?1, 'artifact-install')",
            params![job],
        )
        .unwrap();
        drop(conn);
        let conn = open(dir.path()).await.unwrap();
        assert_eq!(baseline_history(&conn), history);
        assert_eq!(get_job_result(&conn, &job).unwrap(), Some(result));
        assert_eq!(
            conn.query_row("SELECT COUNT(*) FROM jobs", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            1
        );
        assert_eq!(
            conn.query_row("SELECT COUNT(*) FROM _sqlx_migrations", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            2
        );
    }
}

#[tokio::test]
async fn rejected_histories_leave_prototype_state_untouched() {
    for prototype in [false, true] {
        for common in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let conn = prior_database(dir.path(), prototype).await;
            conn.execute_batch(
                "INSERT INTO jobs (job_id, kind) VALUES ('preserved', 'artifact-install');",
            )
            .unwrap();
            conn.execute_batch(if common {
                "UPDATE _space_common_schema_migrations SET checksum = x'00';"
            } else {
                "UPDATE _sqlx_migrations SET checksum = x'00';"
            })
            .unwrap();
            let history = baseline_history(&conn);
            drop(conn);
            open(dir.path()).await.unwrap_err();
            let conn = Connection::open(dir.path().join(DATABASE_FILE)).unwrap();
            assert_eq!(baseline_history(&conn), history);
            assert_eq!(
                conn.query_row("SELECT COUNT(*) FROM jobs", [], |r| r.get::<_, i64>(0))
                    .unwrap(),
                1
            );
            assert_eq!(
                conn.query_row("SELECT COUNT(*) FROM _sqlx_migrations", [], |r| r
                    .get::<_, i64>(0))
                    .unwrap(),
                1
            );
        }
    }
}
