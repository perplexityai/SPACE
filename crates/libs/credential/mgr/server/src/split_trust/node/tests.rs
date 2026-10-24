use super::*;

#[tokio::test(start_paused = true)]
async fn ttl_removes_old_credentials_but_keeps_recent_entries() {
    let old = SandboxId::from_uuid(uuid::Uuid::now_v7());
    let recent = SandboxId::from_uuid(uuid::Uuid::now_v7());
    let entry = || {
        Arc::new(Entry {
            value: OnceCell::from(Cached {
                anchor: Anchor {
                    certificate_der: vec![1],
                    private_key_der: zeroize::Zeroizing::new(vec![2]),
                },
                acquired: tokio::time::Instant::now(),
            }),
            created: tokio::time::Instant::now(),
        })
    };
    let mut cache = HashMap::from([(old, entry())]);
    tokio::time::advance(Duration::from_secs(30)).await;
    cache.insert(recent, entry());
    tokio::time::advance(Duration::from_secs(31)).await;
    NodeAnchors::expire(&mut cache);
    assert!(!cache.contains_key(&old));
    assert!(cache.contains_key(&recent));
}

#[tokio::test(start_paused = true)]
async fn a_pending_miss_does_not_block_another_sandbox_and_same_id_is_single_flight() {
    use pplx_space_common_svc::bootstrap::wrap_grpc_client_channel;
    use pplx_space_credmgr_client::{CredentialManagerClient, CredmgrBearerInterceptor};
    use tonic::service::interceptor::InterceptedService;

    let channel = tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
    let node = NodeAnchors::new(CredentialManagerClient::new(wrap_grpc_client_channel(
        InterceptedService::new(channel, CredmgrBearerInterceptor::None),
    )));
    let cold_id = SandboxId::from_uuid(uuid::Uuid::now_v7());
    let hot_id = SandboxId::from_uuid(uuid::Uuid::now_v7());
    let cold = node.entry(cold_id).unwrap();
    assert!(Arc::ptr_eq(&cold, &node.entry(cold_id).unwrap()));
    node.entry(hot_id)
        .unwrap()
        .value
        .set(Cached {
            anchor: Anchor {
                certificate_der: vec![1],
                private_key_der: zeroize::Zeroizing::new(vec![2]),
            },
            acquired: tokio::time::Instant::now(),
        })
        .map_err(|_| ())
        .unwrap();
    let pending = cold.value.get_or_init(std::future::pending);
    tokio::pin!(pending);
    tokio::select! {
        biased;
        _ = &mut pending => panic!("cold acquisition must remain pending"),
        () = std::future::ready(()) => {}
    }
    tokio::time::timeout(Duration::from_millis(1), node.public_anchor(hot_id))
        .await
        .expect("an unrelated KMS miss must not delay a cached result")
        .unwrap();
    assert!(cold.value.get().is_none());
}
