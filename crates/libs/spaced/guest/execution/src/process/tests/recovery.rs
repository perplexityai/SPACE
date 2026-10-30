use super::*;
use pplx_space_common_core::types::ProcessRecoveryToken;
use pplx_space_spaced_guest_protocol::proto::operations::{RecoveryStart, process_event};

async fn recovery_request(service: &ExecutionService, key: &str) -> StartRequest {
    service
        .configure_process_recovery(Some(ProcessRecoveryToken::new()))
        .await
        .expect("enable recovery");
    let info = service.process_execution_info();
    StartRequest {
        recovery: Some(RecoveryStart {
            contract_version: 1,
            expected_instance_id: info.instance_id,
            expected_activation_id: info.activation_id.expect("activation enabled"),
            command: vec!["/bin/sh".into(), "-c".into(), "printf recovered".into()],
        }),
        idempotency_key: Some(key.into()),
        process_id: Some("assigned-recovery-process".into()),
        ..Default::default()
    }
}

async fn events(service: &ExecutionService, request: StartRequest) -> Vec<ProcessEvent> {
    Process::start(service, Request::new(request))
        .await
        .expect("start process")
        .into_inner()
        .map(Result::unwrap)
        .collect()
        .await
}
async fn failure(service: &ExecutionService, request: StartRequest) -> Status {
    Process::start(service, Request::new(request))
        .await
        .err()
        .expect("start refused")
}

#[tokio::test]
async fn recovery_start_replays_before_id_collision_or_mutable_defaults() {
    let service = cancellation_service();
    let mut request = recovery_request(&service, "recovery-replay").await;
    request.env.insert("A".into(), "one".into());
    request.env.insert("B".into(), "two".into());
    let (first, duplicate) = tokio::join!(
        events(&service, request.clone()),
        events(&service, request.clone())
    );
    assert_eq!(first, duplicate);
    assert!(
        matches!(&first[0].event, Some(process_event::Event::Started(started)) if started.instance_id.is_some() && started.process_incarnation.is_some())
    );
    service
        .configure_process_env(
            EnvMap::try_from(HashMap::from([("NEW".into(), "default".into())])).unwrap(),
        )
        .await
        .unwrap();
    request.env = HashMap::from([("B".into(), "two".into()), ("A".into(), "one".into())]);
    assert_eq!(events(&service, request.clone()).await, first);
    let mut legacy = request.clone();
    legacy.command = legacy.recovery.take().unwrap().command;
    legacy.process_id = None;
    assert_eq!(
        pplx_space_spaced_guest_protocol::recovery::RecoveryReason::from_status(
            &failure(&service, legacy).await
        ),
        Some(pplx_space_spaced_guest_protocol::recovery::RecoveryReason::StartModeMismatch),
    );
    for mut changed in [request.clone(), request.clone(), request.clone()]
        .into_iter()
        .enumerate()
    {
        match changed.0 {
            0 => changed
                .1
                .recovery
                .as_mut()
                .unwrap()
                .command
                .push("different".into()),
            1 => {
                changed.1.env.insert("A".into(), "different".into());
            }
            _ => changed.1.cwd = Some("/".into()),
        }
        assert_eq!(
            failure(&service, changed.1).await.code(),
            tonic::Code::InvalidArgument
        );
    }
    assert_eq!(service.inner.processes.records.len(), 1);
    service.cancel();
    service.drain().await.unwrap();
}

#[tokio::test]
async fn recovery_start_refuses_invalid_forms_and_stale_lifetimes_without_a_launch() {
    let service = cancellation_service();
    let request = recovery_request(&service, "recovery-fences").await;
    for case in 0..7 {
        let mut changed = request.clone();
        let expected = match case {
            0 => {
                changed.recovery.as_mut().unwrap().expected_instance_id =
                    ProcessRecoveryToken::new().to_string();
                tonic::Code::FailedPrecondition
            }
            1 => {
                changed.recovery.as_mut().unwrap().expected_activation_id =
                    ProcessRecoveryToken::new().to_string();
                tonic::Code::FailedPrecondition
            }
            2 => {
                changed.recovery.as_mut().unwrap().contract_version = 2;
                tonic::Code::FailedPrecondition
            }
            3 => {
                changed.command = vec!["/bin/true".into()];
                tonic::Code::InvalidArgument
            }
            4 => {
                changed.env.insert("INVALID=KEY".into(), "value".into());
                tonic::Code::InvalidArgument
            }
            5 => {
                changed.idempotency_key = None;
                tonic::Code::InvalidArgument
            }
            _ => {
                service.configure_process_recovery(None).await.unwrap();
                tonic::Code::Unavailable
            }
        };
        assert_eq!(
            failure(&service, changed).await.code(),
            expected,
            "case {case}"
        );
        assert!(service.inner.processes.records.is_empty());
    }
    let old = request.clone();
    service
        .configure_process_recovery(Some(ProcessRecoveryToken::new()))
        .await
        .unwrap();
    assert_eq!(
        failure(&service, old).await.code(),
        tonic::Code::FailedPrecondition
    );
    // Disabling recovery does not disable ordinary execution.
    service.configure_process_recovery(None).await.unwrap();
    events(
        &service,
        StartRequest {
            command: vec!["/bin/true".into()],
            idempotency_key: Some("legacy".into()),
            ..Default::default()
        },
    )
    .await;
    service.cancel();
    service.drain().await.unwrap();
}

#[tokio::test]
async fn recovery_retry_never_attaches_to_a_reused_process_id() {
    let service = cancellation_service();
    let request = recovery_request(&service, "retained-key").await;
    let first = events(&service, request.clone()).await;
    let process_id = ProcessId::parse(&first[0].process_id).unwrap();
    // Model record eviction while the independently configured key survives.
    service.inner.processes.records.remove(&process_id);
    events(
        &service,
        StartRequest {
            command: vec!["/bin/true".into()],
            process_id: Some(process_id.to_string()),
            idempotency_key: Some("replacement".into()),
            ..Default::default()
        },
    )
    .await;
    assert_eq!(
        failure(&service, request).await.code(),
        tonic::Code::FailedPrecondition
    );
    service.cancel();
    service.drain().await.unwrap();
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn recovery_snapshot_barrier_settles_cancelled_start_before_disabling() {
    let (service, placement) = stalled_placement_service();
    let request = recovery_request(&service, "snapshot-start").await;
    let pending = tokio::spawn({
        let service = service.clone();
        let request = request.clone();
        async move { Process::start(&service, Request::new(request)).await }
    });
    wait_for_start_owner(&service).await;
    pending.abort();
    let _ = pending.await;
    let mut disabling = Box::pin(service.configure_process_recovery(None));
    assert!(futures_util::poll!(&mut disabling).is_pending());
    let slots = service.inner.process_payload_slots.available_permits();
    let (refusal, remaining) = {
        let refused = failure(&service, request.clone());
        tokio::pin!(refused);
        (
            futures_util::poll!(&mut refused),
            service.inner.process_payload_slots.available_permits(),
        )
    };
    drop(disabling);
    let disabled = service.process_execution_info().activation_id.is_none();
    placement.release();
    assert!(disabled, "abandoned quiescing must remain disabled");
    let mut disabling = Box::pin(service.configure_process_recovery(None));
    assert!(
        matches!(refusal, std::task::Poll::Ready(status) if status.code() == tonic::Code::Unavailable)
    );
    assert_eq!(remaining, slots);
    tokio::time::timeout(Duration::from_secs(5), &mut disabling)
        .await
        .unwrap()
        .unwrap();
    assert!(service.process_execution_info().activation_id.is_none());
    let epoch =
        ProcessRecoveryToken::parse(&request.recovery.as_ref().unwrap().expected_activation_id)
            .unwrap();
    service
        .configure_process_recovery(Some(epoch))
        .await
        .unwrap();
    let replayed = events(&service, request).await;
    assert!(matches!(
        replayed[0].event,
        Some(process_event::Event::Started(_))
    ));
    assert_eq!(service.inner.processes.records.len(), 1);
    service.cancel();
    service.drain().await.unwrap();
}

mod handles;

mod mutations;
