use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

#[tokio::test]
async fn historical_seed_is_single_flight_reused_and_recoverable() {
    let ring = SeedKeyring::default();
    let calls = AtomicUsize::new(0);
    let loads = (0..32).map(|_| {
        ring.get(key(1), async {
            calls.fetch_add(1, Ordering::Relaxed);
            tokio::task::yield_now().await;
            Ok(Zeroizing::new([42; 48]))
        })
    });
    let seeds = futures_util::future::try_join_all(loads).await.unwrap();
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    assert!(seeds.iter().all(|seed| ***seed == [42; 48]));
    let cached = ring
        .get(key(1), async { Err(HardwareError::Timeout) })
        .await
        .unwrap();
    assert_eq!(**seeds[0], **cached);
    assert!(matches!(
        ring.get(key(2), async { Err(HardwareError::Timeout) })
            .await,
        Err(HardwareError::Timeout)
    ));
    assert_eq!(
        **ring
            .get(key(2), async { Ok(Zeroizing::new([43; 48])) })
            .await
            .unwrap(),
        [43; 48]
    );
    for index in 3..(CAPACITY as u64 + 3) {
        ring.get(key(index), async { Ok(Zeroizing::new([1; 48])) })
            .await
            .unwrap();
    }
    assert_eq!(ring.slots.lock().await.len(), CAPACITY);
    assert!(matches!(
        ring.get(key(1), async { Err(HardwareError::Timeout) })
            .await,
        Err(HardwareError::Timeout)
    ));
}

#[tokio::test]
async fn cancelled_seed_load_is_retryable_and_does_not_block_other_keys() {
    let ring = SeedKeyring::default();
    {
        let cold = ring.get(key(1), std::future::pending());
        tokio::pin!(cold);
        tokio::select! {
            biased;
            _ = &mut cold => panic!("cold load must remain pending"),
            () = std::future::ready(()) => {}
        }
        ring.get(key(2), async { Ok(Zeroizing::new([2; 48])) })
            .await
            .unwrap();
    }
    let retried = ring
        .get(key(1), async { Ok(Zeroizing::new([1; 48])) })
        .await
        .unwrap();
    assert_eq!(**retried, [1; 48]);
}

fn key(index: u64) -> AnchorKeyId {
    format!("mrk-{index:032x}").parse().unwrap()
}
