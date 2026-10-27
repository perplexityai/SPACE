use super::super::authority::tests::local_provider;
use super::*;
use crate::state::NetworkStateReason;

#[tokio::test]
async fn cache_refreshes_short_lived_leaves_without_revoking_existing_connections() {
    use super::super::authority::tests::leaf_with_lifetime;
    let (_, owner, provider) = local_provider().await;
    let host: DomainName = "expiry.test".parse().unwrap();
    for lifetime in [120, 15] {
        provider.get(host.clone(), 443).await.unwrap();
        let entry = provider
            .inner
            .cache
            .lock()
            .unwrap()
            .ready
            .pop(&(host.clone(), 443))
            .unwrap();
        let mut entry = Arc::try_unwrap(entry).ok().unwrap();
        // A real independently signed leaf models an aged cache entry without
        // sleeping for its original five-minute lifetime or changing clocks.
        let cached = Arc::new(leaf_with_lifetime(
            &owner,
            host.clone(),
            lifetime,
            entry.fence.clone(),
        ));
        entry.value = OnceCell::new();
        entry.value.set(cached.clone()).unwrap();
        provider
            .inner
            .cache
            .lock()
            .unwrap()
            .ready
            .put((host.clone(), 443), Arc::new(entry));
        let before = provider.inner.rate.lock().unwrap().1;
        let returned = provider.get(host.clone(), 443).await.unwrap();
        assert_eq!(Arc::ptr_eq(&cached, &returned), lifetime == 120);
        assert_eq!(
            provider.inner.rate.lock().unwrap().1,
            before + usize::from(lifetime == 15)
        );
        assert!(
            cached.is_usable(),
            "cache refresh must not revoke existing connections"
        );
        cached
            .certified_key()
            .unwrap()
            .key
            .choose_scheme(&[rustls::SignatureScheme::ECDSA_NISTP256_SHA256])
            .unwrap()
            .sign(b"existing connection")
            .unwrap();
    }
}

#[tokio::test]
async fn local_cache_shares_exact_targets_and_bounds_retention() {
    let (_, _, provider) = local_provider().await;
    let host: DomainName = "shared.test".parse().unwrap();
    let (first, second) = tokio::join!(
        provider.get(host.clone(), 443),
        provider.get(host.clone(), 443),
    );
    let first = first.unwrap();
    assert!(Arc::ptr_eq(&first, &second.unwrap()));
    let other_port = provider.get(host.clone(), 8443).await.unwrap();
    assert!(!Arc::ptr_eq(&first, &other_port));
    assert_eq!(other_port.port(), 8443);

    for index in 0..=MAX_CACHED_CERTIFICATES {
        // Retention is independent of the separately exercised issuance budget.
        *provider.inner.rate.lock().unwrap() = (tokio::time::Instant::now(), 0);
        provider
            .get(format!("host{index}.test").parse().unwrap(), 443)
            .await
            .unwrap();
    }
    {
        let cache = provider.inner.cache.lock().unwrap();
        assert_eq!(cache.ready.len(), MAX_CACHED_CERTIFICATES);
        assert!(!cache.ready.contains(&(host, 443)));
    }
    assert!(
        first.is_usable(),
        "eviction does not revoke an active TLS lease"
    );

    let warm: Vec<_> = provider
        .inner
        .cache
        .lock()
        .unwrap()
        .ready
        .iter()
        .map(|(key, entry)| (key.clone(), entry.value.get().unwrap().clone()))
        .collect();
    for capacity_limited in [true, false] {
        let permits = if capacity_limited {
            Some(
                provider
                    .inner
                    .capacity
                    .clone()
                    .acquire_many_owned(MAX_CONCURRENT_REQUESTS as u32)
                    .await
                    .unwrap(),
            )
        } else {
            None
        };
        for index in 0..=MAX_CACHED_CERTIFICATES {
            // Pin an exhausted window for each miss: a full-cache loop can
            // outlast the one-second window and reopen the signing budget.
            *provider.inner.rate.lock().unwrap() =
                (tokio::time::Instant::now(), MAX_REQUESTS_PER_SECOND);
            assert_eq!(
                provider
                    .get(format!("rejected{index}.test").parse().unwrap(), 443)
                    .await
                    .unwrap_err()
                    .code(),
                tonic::Code::ResourceExhausted,
            );
        }
        for ((host, port), lease) in &warm {
            assert!(
                Arc::ptr_eq(lease, &provider.get(host.clone(), *port).await.unwrap()),
                "failed misses must preserve every warm cache entry"
            );
        }
        drop(permits);
    }

    provider
        .inner
        .context
        .network
        .set_no_identity(NetworkStateReason::NoIdentity)
        .await;
    first.invalidated().await;
    assert!(!first.is_usable());
    provider.prune().unwrap();
    assert!(provider.inner.cache.lock().unwrap().ready.is_empty());
}

#[tokio::test]
async fn local_signing_budgets_reject_excess_work_and_allow_retry() {
    let (_, _, provider) = local_provider().await;
    let permits = provider
        .inner
        .capacity
        .clone()
        .acquire_many_owned(MAX_CONCURRENT_REQUESTS as u32)
        .await
        .unwrap();
    let host: DomainName = "capacity.test".parse().unwrap();
    assert_eq!(
        provider.get(host.clone(), 443).await.unwrap_err().code(),
        tonic::Code::ResourceExhausted
    );
    drop(permits);
    let lease = provider.get(host.clone(), 443).await.unwrap();

    *provider.inner.rate.lock().unwrap() = (tokio::time::Instant::now(), MAX_REQUESTS_PER_SECOND);
    assert!(
        Arc::ptr_eq(&lease, &provider.get(host, 443).await.unwrap()),
        "cache hits do not consume the leaf-signing budget"
    );
    let fresh: DomainName = "rate.test".parse().unwrap();
    assert_eq!(
        provider.get(fresh.clone(), 443).await.unwrap_err().code(),
        tonic::Code::ResourceExhausted
    );
    *provider.inner.rate.lock().unwrap() = (tokio::time::Instant::now(), 0);
    provider.get(fresh, 443).await.unwrap();
}

#[tokio::test]
async fn local_cache_rejects_retired_ca_and_changed_execution() {
    for change_execution in [false, true] {
        let (_, owner, provider) = local_provider().await;
        let host: DomainName = "binding.test".parse().unwrap();
        let lease = provider.get(host.clone(), 443).await.unwrap();
        if change_execution {
            super::super::authority::tests::advance_route(&owner);
        } else {
            provider.inner.context.shutdown.cancel();
        }
        assert!(!owner.is_ready());
        assert!(!lease.is_usable());
        lease.certified_key().unwrap_err();
        tokio::time::timeout(Duration::from_secs(1), lease.invalidated())
            .await
            .unwrap();
        provider.get(host, 443).await.unwrap_err();
        provider.prune().unwrap();
        assert!(provider.inner.cache.lock().unwrap().ready.is_empty());
    }
}
