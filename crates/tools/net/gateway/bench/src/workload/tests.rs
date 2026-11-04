use super::*;

fn env(scale_percent: u32) -> Env {
    Env {
        upstream: "198.18.1.2:9000".parse().expect("literal address"),
        raw_echo: "198.18.1.2:9001".parse().expect("literal address"),
        denied: "169.254.169.254:80".parse().expect("literal address"),
        scale_percent,
    }
}

#[test]
fn every_registered_workload_has_a_unique_name_and_a_summary() {
    let mut names: Vec<&str> = REGISTRY.iter().map(|workload| workload.name).collect();
    let registered = names.len();
    names.sort_unstable();
    names.dedup();
    assert_eq!(names.len(), registered, "workload names must be unique");
    for workload in REGISTRY {
        assert!(
            !workload.summary.is_empty(),
            "{} has no summary",
            workload.name
        );
        assert_eq!(
            lookup(workload.name).map(|found| found.name),
            Some(workload.name)
        );
    }
}

#[test]
fn lookup_rejects_an_unregistered_name() {
    assert!(lookup("no-such-workload").is_none());
}

/// A refused flow closes at once, so the deny probe's read normally returns
/// immediately. When the destination turns out to be *reachable* and silent, the
/// read has to come back anyway: waiting for the peer to close made the workload
/// hang until the runner's 180s deadline and report "did not finish" instead of
/// naming the real fault, which is that policy was not enforced.
#[tokio::test]
async fn a_reachable_but_silent_destination_is_reported_as_carrying_nothing() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a silent listener");
    let addr = listener.local_addr().expect("read the bound address");
    // Accept and hold: never write, never close.
    let held = tokio::spawn(async move { listener.accept().await.map(|(stream, _peer)| stream) });

    let mut stream = TcpStream::connect(addr).await.expect("connect");
    // The outer deadline is what makes an unbounded read a failure rather than a
    // hung test run.
    let carried = tokio::time::timeout(DENY_VERDICT_WAIT * 3, bytes_carried(&mut stream))
        .await
        .expect("the deny probe must bound its own wait; it never returned");

    assert_eq!(carried, 0, "a silent peer carried no bytes");
    drop(held);
}

#[tokio::test]
async fn a_destination_that_answers_is_reported_as_carrying_bytes() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a talking listener");
    let addr = listener.local_addr().expect("read the bound address");
    tokio::spawn(async move {
        if let Ok((mut stream, _peer)) = listener.accept().await {
            let _ = stream.write_all(b"denied").await;
            let _ = stream.shutdown().await;
        }
    });

    let mut stream = TcpStream::connect(addr).await.expect("connect");
    assert_eq!(bytes_carried(&mut stream).await, 6);
}

#[test]
fn scaling_is_proportional_and_never_reaches_zero() {
    assert_eq!(env(100).scaled(1_000), 1_000);
    assert_eq!(env(50).scaled(1_000), 500);
    assert_eq!(env(400).scaled(1_000), 4_000);
    // A tiny scale must still leave something to run, or a `--scale 1` smoke
    // would silently measure nothing.
    assert_eq!(env(1).scaled(8), 1);
}
