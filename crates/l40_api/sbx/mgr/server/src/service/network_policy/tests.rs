use super::*;
use crate::template_resolver::fake::FakeTemplateResolver;

/// A cache whose owner lookups are refused, with the resolver that refuses
/// them. Tests that supply their own fetch never reach it.
fn refusing_cache() -> (TemplateNetworkPolicyCache, Arc<FakeTemplateResolver>) {
    let templates = Arc::new(FakeTemplateResolver::refusing(Status::unavailable(
        "template-manager is outside this test",
    )));
    let cache = TemplateNetworkPolicyCache::new(
        tokio_util::sync::CancellationToken::new(),
        tokio_util::task::TaskTracker::new(),
        Arc::clone(&templates) as Arc<dyn crate::template_resolver::TemplateResolver>,
    );
    (cache, templates)
}

#[test]
fn legacy_policy_hash_is_stable() {
    assert_eq!(
        network_policy_hash(&NetworkPolicyV1::deny_all()),
        "dd5502196868b187f6af7595127426118377be9933dc8a666d2d2cc4debe5843"
    );
}

#[test]
fn policy_write_boundary_accepts_tls_interception_on() {
    let mut policy = NetworkPolicyV1::deny_all();
    policy.tls_interception = pplx_space_net_common::TlsInterception::On;

    assert_eq!(
        network_policy_from_proto(&NetworkPolicyProto::from(&policy)).unwrap(),
        policy
    );
}

#[tokio::test(start_paused = true)]
async fn template_policy_refresh_uses_current_owner_across_local_changes_and_recreation() {
    use crate::db::PolicyAuthority::{Cluster, Local};
    for (first_authority, first_generation, next_authority, next_generation) in [
        (Cluster, 9, Cluster, 10),
        (Local, 0, Local, 0),   // Old template-manager local updates reuse zero.
        (Local, 9, Cluster, 1), // Delete/recreate legitimately changes authority.
    ] {
        let cache = refusing_cache().0;
        cache
            .get_or_fetch_with("seed", move |_| async move {
                Ok(EffectiveNetworkPolicy::new(
                    NetworkPolicyV1::deny_all(),
                    first_generation,
                    first_authority,
                    true,
                ))
            })
            .await
            .unwrap();
        let cached = cache
            .get_or_fetch_with("seed", |_| async {
                panic!("fresh owner policy must not refetch")
            })
            .await
            .unwrap();
        assert_eq!(cached.policy, NetworkPolicyV1::deny_all());

        tokio::time::advance(TEMPLATE_NETWORK_POLICY_CACHE_TTL).await;
        assert_eq!(
            cache
                .get_or_fetch_with("seed", |_| async {
                    Err(Status::unavailable("owner unavailable"))
                })
                .await
                .unwrap_err()
                .code(),
            tonic::Code::Unavailable
        );
        let updated = cache
            .get_or_fetch_with("seed", move |_| async move {
                Ok(EffectiveNetworkPolicy::new(
                    NetworkPolicyV1::allow_all(),
                    next_generation,
                    next_authority,
                    true,
                ))
            })
            .await
            .unwrap();
        assert_eq!(updated.policy, NetworkPolicyV1::allow_all());
        assert_eq!(updated.authority, next_authority);
        assert_eq!(updated.generation, next_generation);
        cache.task_tracker.close();
        cache.task_tracker.wait().await;
    }
}

#[tokio::test(start_paused = true)]
async fn template_policy_hints_are_admission_requirements_not_cache_authority() {
    use crate::db::PolicyAuthority::{Cluster, Local};
    for (source_authority, generation, hint_authority, hint_generation, matching, expected) in [
        (Cluster, 4, Cluster, 5, true, Some(tonic::Code::Unavailable)),
        (
            Cluster,
            5,
            Cluster,
            5,
            false,
            Some(tonic::Code::FailedPrecondition),
        ),
        (Cluster, 5, Cluster, 5, true, None),
        (Cluster, 6, Cluster, 5, false, None),
        (Local, 0, Cluster, 50, false, None),
        (Local, 0, Local, 0, true, None),
        (
            Local,
            0,
            Local,
            0,
            false,
            Some(tonic::Code::FailedPrecondition),
        ),
        (
            Cluster,
            5,
            Local,
            0,
            true,
            Some(tonic::Code::FailedPrecondition),
        ),
    ] {
        let (cache, templates) = refusing_cache();
        cache
            .get_or_fetch_with("seed", move |_| async move {
                Ok(EffectiveNetworkPolicy::new(
                    NetworkPolicyV1::deny_all(),
                    generation,
                    source_authority,
                    true,
                ))
            })
            .await
            .unwrap();
        let hint = EffectiveNetworkPolicy::new(
            if matching {
                NetworkPolicyV1::deny_all()
            } else {
                NetworkPolicyV1::allow_all()
            },
            hint_generation,
            hint_authority,
            true,
        );
        let admission_error = cache.require_hint("seed", &hint, true).await.err();
        // The hint is checked against the cached owner answer, not a new lookup.
        assert!(templates.lookups().is_empty());
        assert_eq!(admission_error.as_ref().map(|error| error.code()), expected);
        if let Some(error) = &admission_error {
            assert!(pplx_space_sbx_mgr_client::is_template_policy_dependency(
                error
            ));
        }
        if expected.is_some() {
            // Failed admission invalidates the lagging cache, so the next caller
            // can observe reconciliation instead of waiting for the TTL.
            let fresh = cache
                .get_or_fetch_with("seed", |_| async {
                    Ok(EffectiveNetworkPolicy::new(
                        NetworkPolicyV1::deny_all(),
                        100,
                        Cluster,
                        true,
                    ))
                })
                .await
                .unwrap();
            assert_eq!(fresh.generation, 100);
        } else {
            let retained = cache
                .get_or_fetch_with("seed", |_| async {
                    panic!("a hint must not install a replacement policy")
                })
                .await
                .unwrap();
            assert_eq!(retained.policy, NetworkPolicyV1::deny_all());
            assert_eq!(retained.authority, source_authority);
            assert_eq!(retained.generation, generation);
        }
        cache.task_tracker.close();
        cache.task_tracker.wait().await;
    }
}

#[tokio::test(start_paused = true)]
async fn custom_policy_override_does_not_require_template_owner_progress() {
    let (cache, templates) = refusing_cache();
    let hint = EffectiveNetworkPolicy::new(
        NetworkPolicyV1::deny_all(),
        50,
        crate::db::PolicyAuthority::Cluster,
        true,
    );
    // An unavailable template source cannot block an explicit sandbox override.
    // Holding admission also proves the override path never starts a fetch.
    let _guard = cache.inner.lock().await;
    for inherits_template in [false, true] {
        let result = cache.require_hint("seed", &hint, inherits_template).await;
        assert_eq!(
            result.err().map(|error| error.code()),
            inherits_template.then_some(tonic::Code::DeadlineExceeded)
        );
    }
    assert!(templates.lookups().is_empty());
}

#[tokio::test(start_paused = true)]
async fn template_policy_invalidation_fences_fetch_callers_and_successor_claims() {
    let cache = refusing_cache().0;
    let mut old = Box::pin(cache.get_or_fetch_with("seed", |_| std::future::pending()));
    assert!(futures_util::poll!(&mut old).is_pending());
    let old_claim = Arc::clone(&cache.inner.lock().await.pending_fetches["seed"].claim);
    cache.invalidate("seed").await;
    assert_eq!(old.await.unwrap_err().code(), tonic::Code::Unavailable);
    assert!(cache.inner.lock().await.cached.is_empty());

    let (release, released) = tokio::sync::oneshot::channel();
    let mut successor = Box::pin(cache.get_or_fetch_with("seed", |_| async move {
        released.await.unwrap();
        Ok(EffectiveNetworkPolicy::new(
            NetworkPolicyV1::deny_all(),
            0,
            crate::db::PolicyAuthority::Local,
            true,
        ))
    }));
    assert!(futures_util::poll!(&mut successor).is_pending());
    assert_eq!(
        cache
            .complete_fetch(
                "seed".to_string(),
                &old_claim,
                Ok(EffectiveNetworkPolicy::new(
                    NetworkPolicyV1::allow_all(),
                    100,
                    crate::db::PolicyAuthority::Cluster,
                    true
                ))
            )
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unavailable
    );
    assert!(
        cache
            .inner
            .lock()
            .await
            .pending_fetches
            .contains_key("seed")
    );
    release.send(()).unwrap();
    assert_eq!(successor.await.unwrap().policy, NetworkPolicyV1::deny_all());
    cache.invalidate("seed").await;
    let fresh = cache
        .get_or_fetch_with("seed", |_| async {
            Ok(EffectiveNetworkPolicy::new(
                NetworkPolicyV1::allow_all(),
                1,
                crate::db::PolicyAuthority::Cluster,
                true,
            ))
        })
        .await
        .unwrap();
    assert_eq!(fresh.authority, crate::db::PolicyAuthority::Cluster);
}

#[tokio::test(start_paused = true)]
async fn template_policy_wait_deadline_includes_cache_admission() {
    for release_midway in [false, true] {
        let cache = refusing_cache().0;
        let guard = cache.inner.lock().await;
        let mut waiting = Box::pin(cache.get_or_fetch_with("seed", |_| std::future::pending()));
        assert!(futures_util::poll!(&mut waiting).is_pending());
        let mut guard = Some(guard);
        tokio::time::advance(TEMPLATE_NETWORK_POLICY_WAIT_TIMEOUT / 2).await;
        if release_midway {
            drop(guard.take());
            assert!(futures_util::poll!(&mut waiting).is_pending());
        }
        tokio::time::advance(TEMPLATE_NETWORK_POLICY_WAIT_TIMEOUT / 2).await;
        assert!(matches!(futures_util::poll!(&mut waiting),
            std::task::Poll::Ready(Err(error)) if error.code() == tonic::Code::DeadlineExceeded));
        drop(guard);
        cache.shutdown_token.cancel();
        cache.task_tracker.close();
        cache.task_tracker.wait().await;
    }
}

#[tokio::test(start_paused = true)]
async fn template_policy_fetch_progress_survives_foreground_timeouts_and_cancellation() {
    let cache = refusing_cache().0;
    let (alive, cancelled) = tokio::sync::oneshot::channel::<()>();
    let (release, released) = tokio::sync::oneshot::channel::<()>();
    let mut first = Box::pin(cache.get_or_fetch_with("seed", |_| async move {
        let _alive = alive;
        released.await.unwrap();
        Ok(EffectiveNetworkPolicy::new(
            NetworkPolicyV1::deny_all(),
            1,
            crate::db::PolicyAuthority::Cluster,
            true,
        ))
    }));
    assert!(futures_util::poll!(&mut first).is_pending());
    let mut second = Box::pin(cache.get_or_fetch_with("seed", |_| async {
        panic!("same-key caller must share the original fetch")
    }));
    assert!(futures_util::poll!(&mut second).is_pending());
    drop(first);
    tokio::time::advance(TEMPLATE_NETWORK_POLICY_WAIT_TIMEOUT).await;
    assert_eq!(
        second.await.unwrap_err().code(),
        tonic::Code::DeadlineExceeded,
    );
    let mut cancelled = cancelled;
    assert_eq!(
        cancelled.try_recv(),
        Err(tokio::sync::oneshot::error::TryRecvError::Empty),
        "foreground timeouts must not cancel dependency progress",
    );

    release.send(()).unwrap();
    let policy = cache
        .get_or_fetch_with("seed", |_| async {
            panic!("later callers must reuse the surviving fetch")
        })
        .await
        .unwrap();
    assert_eq!(policy.generation, 1);
}

#[tokio::test(start_paused = true)]
async fn template_policy_fetch_capacity_and_deadline_release_claims() {
    for shutdown in [false, true] {
        let cache = refusing_cache().0;
        let names: Vec<_> = (0..TEMPLATE_NETWORK_POLICY_FETCH_CAPACITY)
            .map(|i| format!("template-{i}"))
            .collect();
        let mut pending = Vec::new();
        for name in &names {
            let mut fetch = Box::pin(cache.get_or_fetch_with(name, |_| std::future::pending()));
            assert!(futures_util::poll!(&mut fetch).is_pending());
            pending.push(fetch);
        }
        tokio::time::advance(TEMPLATE_NETWORK_POLICY_WAIT_TIMEOUT).await;
        for caller in pending {
            assert_eq!(
                caller.await.unwrap_err().code(),
                tonic::Code::DeadlineExceeded
            );
        }
        // Abandoning all foreground waiters does not free the bounded owners.
        assert_eq!(
            cache
                .get_or_fetch_with("overflow", |_| std::future::pending())
                .await
                .unwrap_err()
                .code(),
            tonic::Code::DeadlineExceeded
        );
        assert_eq!(cache.fetch_slots.available_permits(), 0);

        if shutdown {
            cache.shutdown_token.cancel();
        } else {
            tokio::time::advance(
                TEMPLATE_NETWORK_POLICY_FETCH_TIMEOUT - TEMPLATE_NETWORK_POLICY_WAIT_TIMEOUT,
            )
            .await;
        }
        cache.task_tracker.close();
        cache.task_tracker.wait().await;
        assert!(cache.inner.lock().await.pending_fetches.is_empty());
        assert_eq!(
            cache.fetch_slots.available_permits(),
            TEMPLATE_NETWORK_POLICY_FETCH_CAPACITY,
        );

        let next = cache
            .get_or_fetch_with("overflow", |_| async {
                Ok(EffectiveNetworkPolicy::new(
                    NetworkPolicyV1::deny_all(),
                    1,
                    crate::db::PolicyAuthority::Cluster,
                    true,
                ))
            })
            .await;
        if shutdown {
            let error = next.unwrap_err();
            assert_eq!(error.code(), tonic::Code::Unavailable);
            assert!(!pplx_space_sbx_mgr_client::is_template_policy_dependency(
                &error
            ));
        } else {
            next.unwrap();
        }
    }

    // A ready Shared can be completed by its caller before its tracked owner
    // is scheduled. Those queued owner tasks still consume the same hard cap.
    let cache = refusing_cache().0;
    for i in 0..TEMPLATE_NETWORK_POLICY_FETCH_CAPACITY {
        let name = format!("ready-{i}");
        let mut caller = Box::pin(tokio::task::unconstrained(cache.get_or_fetch_with(
            &name,
            |_| async {
                Ok(EffectiveNetworkPolicy::new(
                    NetworkPolicyV1::deny_all(),
                    1,
                    crate::db::PolicyAuthority::Cluster,
                    true,
                ))
            },
        )));
        assert!(matches!(
            futures_util::poll!(&mut caller),
            std::task::Poll::Ready(Ok(_))
        ));
    }
    assert_eq!(cache.fetch_slots.available_permits(), 0);
    let mut overflow = Box::pin(tokio::task::unconstrained(
        cache.get_or_fetch_with("overflow", |_| std::future::pending()),
    ));
    assert!(futures_util::poll!(&mut overflow).is_pending());
    drop(overflow);
    cache.task_tracker.close();
    cache.task_tracker.wait().await;
    assert_eq!(
        cache.fetch_slots.available_permits(),
        TEMPLATE_NETWORK_POLICY_FETCH_CAPACITY,
    );
}

#[tokio::test(start_paused = true)]
async fn template_policy_cache_bounds_churn_without_retiring_pending_claims() {
    let cache = refusing_cache().0;
    let mut waiting = Box::pin(cache.get_or_fetch_with("pending", |_| std::future::pending()));
    assert!(futures_util::poll!(&mut waiting).is_pending());
    let claim = Arc::clone(&cache.inner.lock().await.pending_fetches["pending"].claim);
    for i in 0..TEMPLATE_NETWORK_POLICY_CACHE_CAPACITY + 2 {
        cache
            .get_or_fetch_with(&format!("template-{i}"), |_| async {
                Ok(EffectiveNetworkPolicy::new(
                    NetworkPolicyV1::deny_all(),
                    1,
                    crate::db::PolicyAuthority::Local,
                    true,
                ))
            })
            .await
            .unwrap();
        tokio::task::yield_now().await;
    }
    {
        let state = cache.inner.lock().await;
        assert_eq!(state.cached.len(), TEMPLATE_NETWORK_POLICY_CACHE_CAPACITY);
        assert!(Arc::ptr_eq(&state.pending_fetches["pending"].claim, &claim));
        assert!(!state.cached.contains_key("template-0"));
    }
    let fresh = cache
        .get_or_fetch_with("template-0", |_| async {
            Ok(EffectiveNetworkPolicy::new(
                NetworkPolicyV1::allow_all(),
                0,
                crate::db::PolicyAuthority::Cluster,
                true,
            ))
        })
        .await
        .unwrap();
    assert_eq!(fresh.policy, NetworkPolicyV1::allow_all());
    cache.invalidate("pending").await;
    assert_eq!(waiting.await.unwrap_err().code(), tonic::Code::Unavailable);
    cache.task_tracker.close();
    cache.task_tracker.wait().await;
}

#[test]
fn network_policy_retirement_forgets_only_the_retired_obligation() {
    let queue = NetworkPolicyReconcileQueue::default();
    for id in ["retired", "retained"] {
        for _ in 0..NETWORK_POLICY_RECONCILE_MAX_ATTEMPTS {
            queue.enqueue_retry(id.into(), Duration::from_secs(1), "unavailable");
        }
    }
    queue.retire("retired");
    let state = queue.inner.lock().unwrap();
    assert!(!state.dead.contains_key("retired"));
    assert!(state.dead.contains_key("retained"));
    drop(state);
    queue.enqueue("retired");
    queue.retire("retired");
    assert!(queue.drain_pending().is_empty());

    queue.enqueue("retired");
    let old = queue.take_pending(&Default::default(), 1).pop().unwrap();
    queue.retire("retired");
    queue.enqueue("retired");
    let replacement = queue.take_pending(&Default::default(), 1).pop().unwrap();
    queue.finish_attempt(old, Some("late failure"), Duration::ZERO);
    assert_eq!(
        queue.retry_len(),
        0,
        "retired attempt must not resurrect retry work"
    );
    queue.finish_attempt(replacement, Some("current failure"), Duration::ZERO);
    assert_eq!(
        queue.retry_len(),
        1,
        "replacement obligation must retain its retry owner"
    );
}

#[test]
fn network_policy_reconcile_queue_dedupes_pending_ids() {
    let queue = NetworkPolicyReconcileQueue::default();

    assert!(queue.enqueue("sandbox-1"));
    assert!(!queue.enqueue("sandbox-1"));
    assert_eq!(queue.pending_len(), 1);
    assert_eq!(queue.retry_len(), 0);

    let batch = queue.drain_pending();
    assert_eq!(batch.len(), 1);
    assert!(batch.contains("sandbox-1"));
    assert_eq!(queue.pending_len(), 0);
}

#[test]
fn network_policy_reconcile_queue_retries_without_duplicates() {
    let queue = NetworkPolicyReconcileQueue::default();

    queue.enqueue("sandbox-1");
    queue.enqueue_retry(
        "sandbox-1".to_string(),
        std::time::Duration::from_secs(10),
        "failed",
    );
    assert_eq!(queue.pending_len(), 0);
    assert_eq!(queue.retry_len(), 1);

    queue.enqueue_retry("sandbox-1".to_string(), std::time::Duration::ZERO, "failed");
    queue.enqueue_retry("sandbox-1".to_string(), std::time::Duration::ZERO, "failed");
    assert_eq!(queue.pending_len(), 0);
    assert_eq!(queue.retry_len(), 1);

    assert_eq!(
        queue
            .promote_due_retries(tokio::time::Instant::now() + std::time::Duration::from_millis(1)),
        1
    );
    assert_eq!(queue.pending_len(), 1);
    assert_eq!(queue.retry_len(), 0);
}

#[test]
fn network_policy_reconcile_queue_direct_enqueue_clears_retry() {
    let queue = NetworkPolicyReconcileQueue::default();

    queue.enqueue_retry(
        "sandbox-1".to_string(),
        std::time::Duration::from_secs(10),
        "failed",
    );
    assert_eq!(queue.retry_len(), 1);

    queue.enqueue("sandbox-1");
    assert_eq!(queue.pending_len(), 1);
    assert_eq!(queue.retry_len(), 0);
    assert_eq!(queue.dead_len(), 0);
}

#[test]
fn network_policy_reconcile_queue_dead_letters_and_direct_enqueue_recovers() {
    let queue = NetworkPolicyReconcileQueue::default();

    for _ in 0..NETWORK_POLICY_RECONCILE_MAX_ATTEMPTS {
        queue.enqueue_retry("sandbox-1".to_string(), std::time::Duration::ZERO, "failed");
    }

    assert_eq!(queue.retry_len(), 0);
    assert_eq!(queue.dead_len(), 1);

    queue.enqueue("sandbox-1");

    assert_eq!(queue.pending_len(), 1);
    assert_eq!(queue.dead_len(), 0);
}

#[test]
fn network_policy_proto_rejects_unspecified_default_action() {
    let err = network_policy_from_proto(&NetworkPolicyProto {
        version: pplx_space_net_common::proto::NetworkPolicyVersion::V1.into(),
        egress_default_action: pplx_space_net_common::proto::EgressDefaultAction::Unspecified
            .into(),
        ..Default::default()
    })
    .expect_err("unspecified action must reject");

    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[test]
fn set_network_policy_requires_exactly_one_source() {
    let policy = NetworkPolicyProto::from(&NetworkPolicyV1::allow_all());

    assert!(
        network_policy_override_from_set_request(true, Some(&policy)).is_err(),
        "inherit_template=true with a policy is invalid"
    );
    assert!(
        network_policy_override_from_set_request(false, None).is_err(),
        "inherit_template=false without a policy is invalid"
    );
    assert_eq!(
        network_policy_override_from_set_request(true, None).unwrap(),
        None
    );
    assert_eq!(
        network_policy_override_from_set_request(false, Some(&policy)).unwrap(),
        Some(NetworkPolicyV1::allow_all())
    );
}
#[test]
fn reconcile_queue_selects_only_unclaimed_work_up_to_capacity() {
    let queue = NetworkPolicyReconcileQueue::default();
    for id in ["active", "first", "second"] {
        queue.enqueue(id);
    }
    let first = queue.take_pending(&["active"].into_iter().collect(), 1);
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].sandbox_id, "first");
    let remaining = queue.take_pending(&Default::default(), 8);
    assert_eq!(
        remaining
            .iter()
            .map(|attempt| attempt.sandbox_id.as_str())
            .collect::<Vec<_>>(),
        ["active", "second"]
    );
}
