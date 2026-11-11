use super::*;

use std::sync::atomic::{AtomicUsize, Ordering};

#[tokio::test(flavor = "current_thread")]
async fn concurrent_callers_share_one_listing() {
    let listing = InboundSandboxListing::default();
    let listings = AtomicUsize::new(0);
    let list = || async {
        listings.fetch_add(1, Ordering::SeqCst);
        tokio::task::yield_now().await;
        Ok(vec!["sbx-a".to_string(), "sbx-b".to_string()])
    };

    let (network_policy, sandbox_desired_state) = tokio::join!(
        listing.sandbox_ids_from(list),
        listing.sandbox_ids_from(list),
    );

    assert_eq!(listings.load(Ordering::SeqCst), 1);
    assert_eq!(
        network_policy,
        Ok(vec!["sbx-a".to_string(), "sbx-b".to_string()])
    );
    assert_eq!(sandbox_desired_state, network_policy);
}

#[tokio::test(flavor = "current_thread")]
async fn a_failed_listing_is_retried_by_the_next_facet() {
    let listing = InboundSandboxListing::default();

    let failed = listing
        .sandbox_ids_from(|| async { Err("list sandboxes: unavailable".to_string()) })
        .await;
    let retried = listing
        .sandbox_ids_from(|| async { Ok(vec!["sbx-a".to_string()]) })
        .await;

    assert_eq!(failed, Err("list sandboxes: unavailable".to_string()));
    assert_eq!(retried, Ok(vec!["sbx-a".to_string()]));
}
