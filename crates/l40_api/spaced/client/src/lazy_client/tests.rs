use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;

#[tokio::test]
async fn concurrent_connects_keep_first_cached_client() -> anyhow::Result<()> {
    let lazy = Arc::new(LazyClient::new());
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let connect_count = Arc::new(AtomicUsize::new(0));

    let first = {
        let lazy = Arc::clone(&lazy);
        let barrier = Arc::clone(&barrier);
        let connect_count = Arc::clone(&connect_count);
        tokio::spawn(async move {
            lazy.get_or_connect::<anyhow::Error, _, _>(|| async move {
                let value = connect_count.fetch_add(1, Ordering::SeqCst) + 1;
                barrier.wait().await;
                Ok(value)
            })
            .await
        })
    };
    let second = {
        let lazy = Arc::clone(&lazy);
        let barrier = Arc::clone(&barrier);
        let connect_count = Arc::clone(&connect_count);
        tokio::spawn(async move {
            lazy.get_or_connect::<anyhow::Error, _, _>(|| async move {
                let value = connect_count.fetch_add(1, Ordering::SeqCst) + 1;
                barrier.wait().await;
                Ok(value)
            })
            .await
        })
    };

    let first = first.await??;
    let second = second.await??;
    assert_eq!(connect_count.load(Ordering::SeqCst), 2);
    assert_eq!(first, second);
    assert_eq!(
        lazy.get_or_connect::<anyhow::Error, _, _>(|| async { Ok(99) })
            .await?,
        first
    );
    Ok(())
}

#[tokio::test]
async fn clear_if_generation_does_not_clear_newer_client() -> anyhow::Result<()> {
    let lazy = LazyClient::new();
    let first = lazy
        .get_or_connect_with_generation::<anyhow::Error, _, _>(|| async { Ok(1) })
        .await?;
    let first_generation = first.generation();
    assert!(lazy.clear_if_generation(first_generation).await);

    let second = lazy
        .get_or_connect_with_generation::<anyhow::Error, _, _>(|| async { Ok(2) })
        .await?;
    assert_ne!(first_generation, second.generation());
    assert!(!lazy.clear_if_generation(first_generation).await);
    assert_eq!(
        lazy.get_or_connect::<anyhow::Error, _, _>(|| async { Ok(3) })
            .await?,
        2
    );
    Ok(())
}

#[tokio::test]
async fn closed_pinned_connection_is_replaced_before_the_next_lease() -> anyhow::Result<()> {
    let lazy = LazyClient::new();
    let connection = crate::PinnedConnection::new();
    let transport = crate::uds::PinnedIo::new(tokio::io::duplex(64).0, &connection);
    let first = lazy
        .get_or_connect_pinned_with_generation::<anyhow::Error, _, _>(|| async {
            Ok((1, Some(connection.clone())))
        })
        .await?;
    let reused = lazy
        .get_or_connect_pinned_with_generation::<anyhow::Error, _, _>(|| async { Ok((2, None)) })
        .await?;
    assert_eq!(reused.generation(), first.generation());
    assert_eq!(reused.into_client(), 1);

    drop(transport);
    let replaced = lazy
        .get_or_connect_pinned_with_generation::<anyhow::Error, _, _>(|| async { Ok((3, None)) })
        .await?;
    assert_ne!(replaced.generation(), first.generation());
    assert_eq!(replaced.into_client(), 3);
    Ok(())
}

#[tokio::test]
async fn closed_pinned_connection_is_not_leased_when_reconnect_fails() -> anyhow::Result<()> {
    let lazy = LazyClient::new();
    let connection = crate::PinnedConnection::new();
    drop(crate::uds::PinnedIo::new(
        tokio::io::duplex(64).0,
        &connection,
    ));
    lazy.get_or_connect_pinned_with_generation::<anyhow::Error, _, _>(|| async {
        Ok((1, Some(connection)))
    })
    .await?;

    lazy.get_or_connect_pinned_with_generation::<anyhow::Error, _, _>(|| async {
        Err(anyhow::anyhow!("guest is restarting"))
    })
    .await
    .expect_err("the failed reconnect must be reported");
    assert!(!lazy.is_connected());
    assert_eq!(
        lazy.get_or_connect::<anyhow::Error, _, _>(|| async { Ok(2) })
            .await?,
        2
    );
    Ok(())
}
