use super::*;

#[test]
fn live_identity_rejects_replaced_or_stopped_daemon_and_wrong_peer() {
    let identity = KubernetesPodIdentity {
        namespace: "space".into(),
        service_account: "worker".into(),
        pod_name: "worker-daemon".parse().unwrap(),
        pod_uid: "pod-a".parse().unwrap(),
        node_name: "worker-a".parse().unwrap(),
        node_uid: "node-a".parse().unwrap(),
    };
    let pod: k8s_openapi::api::core::v1::Pod = serde_json::from_value(serde_json::json!({
        "metadata": {"name": "worker-daemon", "uid": "pod-a"},
        "spec": {"nodeName": "worker-a", "serviceAccountName": "worker"},
        "status": {"phase": "Running", "podIPs": [{"ip": "192.0.2.1"}]}
    }))
    .unwrap();
    let peer = "192.0.2.1".parse().unwrap();
    verify_observed_pod(&identity, &pod, peer).unwrap();
    for change in 0..7 {
        let mut changed = pod.clone();
        let mut actual_peer = peer;
        match change {
            0 => changed.metadata.uid = Some("pod-b".into()),
            1 => changed.spec.as_mut().unwrap().node_name = Some("worker-b".into()),
            2 => changed.spec.as_mut().unwrap().service_account_name = Some("other".into()),
            3 => changed.status.as_mut().unwrap().phase = Some("Succeeded".into()),
            4 => actual_peer = "192.0.2.2".parse().unwrap(),
            5 | 6 => {
                changed.metadata.deletion_timestamp = Some(
                    serde_json::from_value(serde_json::json!("2026-01-01T00:00:00Z")).unwrap(),
                );
                if change == 6 {
                    changed.metadata.uid = Some("pod-b".into());
                }
            }
            _ => unreachable!(),
        }
        assert_eq!(
            verify_observed_pod(&identity, &changed, actual_peer)
                .unwrap_err()
                .code(),
            if change == 5 {
                tonic::Code::Unavailable
            } else {
                tonic::Code::PermissionDenied
            }
        );
    }
}

#[tokio::test]
async fn advisory_identity_verification_never_queues_or_outlives_its_budget() {
    let checks = tokio::sync::Semaphore::new(1);
    let permit = checks.acquire().await.unwrap();
    let error =
        with_verification_budget::<()>(&checks, NodeIdentityVerification::Advisory, async {
            panic!("a busy advisory verifier must not begin I/O");
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::Unavailable);
    drop(permit);
    let error = with_verification_budget(
        &checks,
        NodeIdentityVerification::Advisory,
        std::future::pending::<Result<(), tonic::Status>>(),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), tonic::Code::Unavailable);
    // Cancellation returns the permit; required work remains serviceable.
    with_verification_budget(&checks, NodeIdentityVerification::Required, async {
        Ok(())
    })
    .await
    .unwrap();
}
