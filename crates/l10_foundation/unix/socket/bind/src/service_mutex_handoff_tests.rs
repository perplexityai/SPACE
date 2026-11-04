//! The local handoff contract between the service mutex and Unix socket takeover.

use std::path::PathBuf;
use std::time::Duration;

use tempfile::tempdir;
use tokio_util::sync::CancellationToken;

use pplx_space_service_mutex::{AcquireOutcome, ServiceMutex};

fn mutex_path(dir: &tempfile::TempDir) -> PathBuf {
    dir.path().join("service.mutex")
}

/// The full local handoff contract, simulated end to end in one process
/// (flock is per-open-file-description, so two instances here contend
/// exactly like two processes): a predecessor serves on the production
/// socket while holding the mutex; a successor blocks passive; on
/// release the successor acquires, takes the socket path over by atomic
/// staging-rename, and serves new connections — while the predecessor's
/// established connections keep working on the unlinked inode.
#[tokio::test]
async fn handoff_moves_new_connections_and_preserves_established_ones() {
    use crate::{UnixSocketOptions, bind_unix_socket};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::UnixStream;

    async fn serve(listener: tokio::net::UnixListener, identity: u8) {
        loop {
            let Ok((mut conn, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                let mut request = [0u8; 1];
                while conn.read_exact(&mut request).await.is_ok() {
                    if conn.write_all(&[identity]).await.is_err() {
                        return;
                    }
                }
            });
        }
    }

    async fn ask(conn: &mut UnixStream) -> u8 {
        conn.write_all(b"?").await.expect("send request");
        let mut reply = [0u8; 1];
        conn.read_exact(&mut reply).await.expect("read reply");
        reply[0]
    }

    let dir = tempdir().unwrap();
    let mutex = mutex_path(&dir);
    let socket = dir.path().join("svc.sock");

    let predecessor_mutex = ServiceMutex::acquire(
        &mutex,
        CancellationToken::new(),
        Duration::from_millis(10),
        None,
    )
    .await
    .expect("predecessor acquire");
    let predecessor_listener =
        bind_unix_socket(&socket, UnixSocketOptions::new(0o600)).expect("predecessor bind");
    tokio::spawn(serve(predecessor_listener, b'A'));

    let mut established = UnixStream::connect(&socket).await.expect("connect");
    assert_eq!(ask(&mut established).await, b'A');

    let successor_token = CancellationToken::new();
    let successor = tokio::spawn({
        let mutex = mutex.clone();
        let token = successor_token.clone();
        async move { ServiceMutex::acquire_with_timeout(&mutex, &token, Duration::from_secs(5)).await }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !successor.is_finished(),
        "successor must stay passive while the predecessor holds the mutex"
    );

    drop(predecessor_mutex);
    let successor_mutex = match successor.await.expect("join").expect("acquire") {
        AcquireOutcome::Acquired(guard) => guard,
        AcquireOutcome::PassiveExit => panic!("successor was not cancelled"),
    };
    let successor_listener =
        bind_unix_socket(&socket, UnixSocketOptions::new(0o600)).expect("successor takeover bind");
    tokio::spawn(serve(successor_listener, b'B'));

    let mut fresh = UnixStream::connect(&socket)
        .await
        .expect("connect after takeover");
    assert_eq!(
        ask(&mut fresh).await,
        b'B',
        "new connections must reach the successor"
    );
    assert_eq!(
        ask(&mut established).await,
        b'A',
        "the predecessor's established connection must survive the takeover"
    );

    drop(successor_mutex);
}
