use super::*;
use futures_util::StreamExt;

#[tokio::test]
async fn maintenance_owns_configuration_and_waits_for_stream_lifetimes() {
    let shutdown = CancellationToken::new();
    let execution = Execution {
        admission: Admission::new(shutdown.clone()),
        filesystem: None,
        processes: None,
    };
    let admission = execution.admission.admit().unwrap();
    let worker = admission.clone();
    let mut stream = retain_stream(Box::pin(futures_util::stream::iter([Ok(7)])), admission);
    let hold = execution.hold_for_update(Uuid::now_v7()).unwrap();
    assert_eq!(
        execution.admission.admit().err().unwrap().code(),
        tonic::Code::Unavailable
    );
    assert_eq!(
        execution.update_defer_reasons().await,
        [DeferReason::Operations]
    );
    assert_eq!(stream.next().await.unwrap().unwrap(), 7);
    // Delivering the last item is not the same as closing the RPC stream.
    assert_eq!(
        execution.update_defer_reasons().await,
        [DeferReason::Operations]
    );
    drop(stream);
    // A cancelled response must not hide a blocking worker that is still
    // finishing the operation under its original admission.
    assert_eq!(
        execution.update_defer_reasons().await,
        [DeferReason::Operations]
    );
    drop(worker);
    assert!(execution.update_defer_reasons().await.is_empty());
    let other = Admission::new(CancellationToken::new());
    let wrong = other.hold(Uuid::now_v7()).unwrap();
    assert_eq!(
        execution
            .configure_process_env_for_update(&wrong, WindowsEnv::parse(HashMap::new()).unwrap())
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    execution
        .configure_process_env_for_update(
            &hold,
            WindowsEnv::parse(HashMap::from([("Path".into(), "configured".into())])).unwrap(),
        )
        .await
        .unwrap();
    let env = execution
        .admission
        .environment(HashMap::from([("PATH".into(), "request".into())]))
        .unwrap()
        .into_map();
    assert_eq!(env, HashMap::from([("PATH".into(), "request".into())]));
    execution
        .configure_process_env_for_update(&hold, WindowsEnv::parse(HashMap::new()).unwrap())
        .await
        .unwrap();
    assert!(
        execution
            .admission
            .environment(HashMap::new())
            .unwrap()
            .into_map()
            .is_empty()
    );
    drop(hold);
    drop(execution.admission.admit().unwrap());
    let hold = execution.hold_for_update(Uuid::now_v7()).unwrap();
    shutdown.cancel();
    drop(hold);
    assert_eq!(
        execution.admission.admit().err().unwrap().code(),
        tonic::Code::Unavailable
    );
}
