use super::{
    JobFailureKind, JobFailureReason, JobStatus, ParseJobFailureKindError,
    ParseJobFailureReasonError,
};

#[test]
fn job_status_round_trips_the_persisted_vocabulary() {
    for status in [
        JobStatus::Pending,
        JobStatus::Running,
        JobStatus::Cancelling,
        JobStatus::Completed,
        JobStatus::Failed,
        JobStatus::Cancelled,
    ] {
        assert_eq!(status.as_str().parse(), Ok(status));
        assert_eq!(status.to_string(), status.as_str());
    }
    "typo".parse::<JobStatus>().unwrap_err();
}

#[test]
fn job_failure_kind_round_trips_persisted_values() {
    for kind in [JobFailureKind::Permanent, JobFailureKind::Retriable] {
        assert_eq!(kind.as_str().parse(), Ok(kind));
        assert_eq!(kind.to_string(), kind.as_str());
    }
}

#[test]
fn job_failure_kind_rejects_unknown_persisted_value() {
    let connection = rusqlite::Connection::open_in_memory().expect("open in-memory database");
    let error = connection
        .query_row("SELECT 'exhausted'", [], |row| {
            row.get::<_, JobFailureKind>(0)
        })
        .expect_err("unknown persisted failure kind must be rejected");

    assert!(
        matches!(
            error,
            rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Text,
                ref source,
            ) if source.downcast_ref::<ParseJobFailureKindError>()
                == Some(&ParseJobFailureKindError {
                    value: "exhausted".to_string(),
                })
        ),
        "unexpected SQLite conversion error: {error:?}"
    );
}

#[test]
fn job_failure_reason_round_trips_persisted_values() {
    for reason in [
        JobFailureReason::NotFound,
        JobFailureReason::PermissionDenied,
        JobFailureReason::InvalidInput,
        JobFailureReason::Internal,
    ] {
        assert_eq!(reason.as_str().parse(), Ok(reason));
        assert_eq!(reason.to_string(), reason.as_str());
    }
}

#[test]
fn job_failure_reason_rejects_unknown_persisted_value() {
    let connection = rusqlite::Connection::open_in_memory().expect("open in-memory database");
    let error = connection
        .query_row("SELECT 'gone'", [], |row| row.get::<_, JobFailureReason>(0))
        .expect_err("unknown persisted failure reason must be rejected");

    assert!(
        matches!(
            error,
            rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Text,
                ref source,
            ) if source.downcast_ref::<ParseJobFailureReasonError>()
                == Some(&ParseJobFailureReasonError {
                    value: "gone".to_string(),
                })
        ),
        "unexpected SQLite conversion error: {error:?}"
    );
}
