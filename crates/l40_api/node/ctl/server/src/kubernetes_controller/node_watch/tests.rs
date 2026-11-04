use super::*;

#[test]
fn node_metadata_cache_returns_latest_metadata() {
    let (tx, rx) = watch::channel(None);
    let cache = NodeMetadataCache { rx };

    assert_eq!(cache.metadata(), None);

    let metadata = NodeMetadata {
        resource_version: "123".to_string(),
        labels: BTreeMap::from([("space.pplx.net/test".to_string(), "true".to_string())]),
        annotations: BTreeMap::from([(
            "space.pplx.net/test".to_string(),
            "sha256:abc".to_string(),
        )]),
        network_slots_capacity: None,
    };
    tx.send(Some(metadata.clone())).unwrap();

    assert_eq!(cache.metadata(), Some(metadata));
}

#[tokio::test]
async fn node_metadata_cache_wait_ready_blocks_until_metadata_arrives() {
    let (tx, rx) = watch::channel(None);
    let cache = NodeMetadataCache { rx };

    let waiter = tokio::spawn({
        let cache = cache.clone();
        async move { cache.wait_ready().await }
    });

    assert!(!waiter.is_finished());

    tx.send(Some(NodeMetadata::default())).unwrap();

    waiter.await.unwrap().unwrap();
}
