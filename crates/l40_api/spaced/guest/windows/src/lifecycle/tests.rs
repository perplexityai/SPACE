use super::*;

#[test]
fn host_timestamp_is_utc_and_truncated_to_windows_milliseconds() {
    // 2000-02-29 12:34:56 UTC: exercise leap-day calendar conversion.
    let time = utc_system_time(Some(prost_types::Timestamp {
        seconds: 951_827_696,
        nanos: 987_654_321,
    }))
    .unwrap();
    assert_eq!((time.wYear, time.wMonth, time.wDay), (2000, 2, 29));
    assert_eq!(
        (time.wHour, time.wMinute, time.wSecond, time.wMilliseconds),
        (12, 34, 56, 987)
    );
    for time in [
        None,
        Some(prost_types::Timestamp {
            seconds: 0,
            nanos: -1,
        }),
        Some(prost_types::Timestamp {
            seconds: 0,
            nanos: 1_000_000_000,
        }),
        Some(prost_types::Timestamp {
            seconds: -11_644_473_601,
            nanos: 0,
        }),
        Some(prost_types::Timestamp {
            seconds: i64::MAX,
            nanos: 0,
        }),
    ] {
        assert_eq!(
            utc_system_time(time)
                .err()
                .expect("invalid timestamp accepted")
                .code(),
            tonic::Code::InvalidArgument
        );
    }
}

#[tokio::test]
async fn cancelled_flush_retains_admission_and_failures_release_it() {
    let slots = Arc::new(Semaphore::new(1));
    let (started, wait_started) = tokio::sync::oneshot::channel();
    let (finish, wait_finish) = std::sync::mpsc::channel();
    let task = tokio::spawn(run_flush(slots.clone(), move || {
        started.send(()).unwrap();
        wait_finish.recv().unwrap();
        Ok(())
    }));
    wait_started.await.unwrap();
    task.abort();
    let _ = task.await;
    let error = run_flush(slots.clone(), || panic!("concurrent flush was admitted"))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::ResourceExhausted);
    finish.send(()).unwrap();
    let permit = tokio::time::timeout(std::time::Duration::from_secs(5), slots.acquire())
        .await
        .unwrap()
        .unwrap();
    drop(permit);
    assert_eq!(
        run_flush(slots.clone(), || Err(io::Error::other("flush failure")))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Internal
    );
    run_flush(slots, || Ok(())).await.unwrap();
}
