use super::*;
use tokio::time::sleep;

#[tokio::test]
async fn drain_server_returns_when_server_completes_before_shutdown() {
    let token = CancellationToken::new();

    let output = drain_server("test-service", &token, Duration::from_secs(1), async { 7 }).await;

    assert_eq!(output, Some(7));
}

#[tokio::test]
async fn drain_server_waits_for_server_after_shutdown() {
    let token = CancellationToken::new();
    let shutdown = token.clone();
    tokio::spawn(async move {
        sleep(Duration::from_millis(5)).await;
        shutdown.cancel();
    });

    let output = drain_server("test-service", &token, Duration::from_secs(1), async {
        sleep(Duration::from_millis(10)).await;
        7
    })
    .await;

    assert_eq!(output, Some(7));
}

#[tokio::test]
async fn drain_server_returns_none_after_drain_timeout() {
    let token = CancellationToken::new();
    token.cancel();

    let output = drain_server("test-service", &token, Duration::from_millis(5), async {
        sleep(Duration::from_secs(1)).await;
        7
    })
    .await;

    assert_eq!(output, None);
}
