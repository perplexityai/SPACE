use super::*;

fn install(version: &str, job_id: JobId) -> KernelInstall {
    KernelInstall {
        version: version.parse().unwrap(),
        path: PathBuf::from(format!("/space/vendor/kernels/{version}")),
        line: crate::installs::test_support::fc_line("1.15"),
        location: format!(
            "s3://fake/firecracker-ci/v1.15/x86_64/vmlinux-{version}?region=us-east-1"
        )
        .parse()
        .unwrap(),
        size: 7,
        md5: "44d1aea84475e5bed0dd4f27ddfca550".parse().unwrap(),
        digest: Digest::sha256([0xab; 32]),
        job_id: Some(job_id),
        installed_at: jiff::Timestamp::from_millisecond(1_700_000_000_000).unwrap(),
    }
}

#[tokio::test]
async fn a_seeded_install_has_no_job() {
    let dir = tempfile::tempdir().unwrap();
    let conn = open(dir.path()).await.unwrap();
    let seeded = KernelInstall {
        job_id: None,
        ..install("6.1.155", JobId::new())
    };
    assert!(insert_kernel_install(&conn, &seeded).unwrap());
    assert_eq!(
        get_kernel_install(&conn, &seeded.version).unwrap(),
        Some(seeded)
    );
}

#[tokio::test]
async fn kernel_installs_are_recorded_listed_and_forgotten() {
    let dir = tempfile::tempdir().unwrap();
    let conn = open(dir.path()).await.unwrap();
    let first = install("6.1.155", JobId::new());
    let second = install("5.10.245", JobId::new());
    assert!(insert_kernel_install(&conn, &first).unwrap());
    assert!(insert_kernel_install(&conn, &second).unwrap());
    // One record per version: a second insert keeps the first.
    let other = KernelInstall {
        job_id: Some(JobId::new()),
        ..first.clone()
    };
    assert!(!insert_kernel_install(&conn, &other).unwrap());

    assert_eq!(
        get_kernel_install(&conn, &first.version).unwrap(),
        Some(first.clone())
    );
    assert_eq!(
        list_kernel_installs(&conn).unwrap(),
        vec![second.clone(), first.clone()]
    );
    assert!(delete_kernel_install(&conn, &first.version).unwrap());
    assert!(!delete_kernel_install(&conn, &first.version).unwrap());
    assert_eq!(list_kernel_installs(&conn).unwrap(), vec![second]);
}

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

    clear_job_result(&conn, &job_id).unwrap();
    assert_eq!(get_job_result(&conn, &job_id).unwrap(), None);
}

#[tokio::test]
async fn the_schema_refuses_a_result_that_is_both_or_neither() {
    let dir = tempfile::tempdir().unwrap();
    let conn = open(dir.path()).await.unwrap();
    for (phase, outcome) in [(Some("publishing"), Some("installed")), (None, None)] {
        conn.execute(
            "INSERT INTO kernel_job_results (job_id, phase, outcome, path, digest, updated_at_unix_ms)
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
        JobOutcome::ProvidedExternally,
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
            kind: "kernel",
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
    let recorded = install("6.1.155", job_id);
    let result = JobResult::InProgress {
        phase: JobPhase::Publishing,
        path: recorded.path.clone(),
        digest: recorded.digest,
    };
    let conn = open(dir.path()).await.unwrap();
    assert!(insert_kernel_install(&conn, &recorded).unwrap());
    put_job_result(&conn, &job_id, &result, jiff::Timestamp::now()).unwrap();
    drop(conn);

    let conn = open(dir.path()).await.unwrap();
    assert_eq!(
        get_kernel_install(&conn, &recorded.version).unwrap(),
        Some(recorded.clone())
    );
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
    assert_eq!(
        get_kernel_install(&conn, &recorded.version).unwrap(),
        Some(recorded)
    );
    assert_eq!(get_job_result(&conn, &job_id).unwrap(), Some(result));
}

#[tokio::test]
async fn a_database_that_cannot_be_migrated_fails_instead_of_being_recreated() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(DATABASE_FILE);
    std::fs::write(&path, b"not a database, and not ours to discard").unwrap();
    open(dir.path()).await.unwrap_err();
    // Left in place for an operator: recreating it would disown every install.
    assert_eq!(
        std::fs::read(&path).unwrap(),
        b"not a database, and not ours to discard"
    );
}
