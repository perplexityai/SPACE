use super::*;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::time::Duration;
use tempfile::tempdir;

fn mutex_path(dir: &tempfile::TempDir) -> PathBuf {
    dir.path().join("service.mutex")
}

#[tokio::test]
async fn acquires_on_fresh_file() {
    // Umask is process-wide, so exercise it in a child running only this test.
    if std::env::var_os("SPACE_MUTEX_MODE_TEST_CHILD").is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "service_mutex::tests::acquires_on_fresh_file",
                "--nocapture",
            ])
            .env("SPACE_MUTEX_MODE_TEST_CHILD", "1")
            .output()
            .expect("run isolated mode test");
        assert!(
            output.status.success() && String::from_utf8_lossy(&output.stdout).contains("1 passed"),
            "mode test failed:\n{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
        return;
    }
    nix::sys::stat::umask(nix::sys::stat::Mode::from_bits_truncate(0o022));
    let dir = tempdir().unwrap();
    let path = mutex_path(&dir);

    let token = CancellationToken::new();
    let lock = ServiceMutex::acquire(&path, token, Duration::from_millis(10), None)
        .await
        .expect("acquire");
    assert_eq!(lock.path(), path.as_path());
    assert_eq!(std::fs::metadata(&path).unwrap().mode() & 0o7777, 0o600);
    drop(lock);

    // Opening retained state must neither republish the inode nor normalize it.
    std::fs::write(&path, b"retained").unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o660)).unwrap();
    let before = std::fs::metadata(&path).unwrap();
    let _lock = ServiceMutex::acquire(
        &path,
        CancellationToken::new(),
        Duration::from_millis(10),
        None,
    )
    .await
    .expect("reacquire retained file");
    let after = std::fs::metadata(&path).unwrap();
    assert_eq!(
        (
            after.dev(),
            after.ino(),
            after.uid(),
            after.gid(),
            after.mode()
        ),
        (
            before.dev(),
            before.ino(),
            before.uid(),
            before.gid(),
            before.mode(),
        ),
    );
    assert_eq!(std::fs::read(&path).unwrap(), b"retained");
}

#[tokio::test]
async fn second_acquire_blocks_until_first_drops() {
    let dir = tempdir().unwrap();
    let path = mutex_path(&dir);

    let first = ServiceMutex::acquire(
        &path,
        CancellationToken::new(),
        Duration::from_millis(10),
        None,
    )
    .await
    .expect("first acquire");

    let second_path = path.clone();
    let second_handle = tokio::spawn(async move {
        ServiceMutex::acquire(
            &second_path,
            CancellationToken::new(),
            Duration::from_millis(10),
            None,
        )
        .await
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !second_handle.is_finished(),
        "second should still be blocked"
    );

    drop(first);

    let second = tokio::time::timeout(Duration::from_secs(2), second_handle)
        .await
        .expect("second acquire timed out")
        .expect("second join")
        .expect("second acquire");
    drop(second);
}

#[tokio::test]
async fn returns_cancelled_when_token_fires_during_wait() {
    let dir = tempdir().unwrap();
    let path = mutex_path(&dir);

    let _guard = ServiceMutex::acquire(
        &path,
        CancellationToken::new(),
        Duration::from_millis(10),
        None,
    )
    .await
    .expect("holder acquire");

    let token = CancellationToken::new();
    let waiter_token = token.clone();
    let waiter_path = path.clone();
    let waiter = tokio::spawn(async move {
        ServiceMutex::acquire(&waiter_path, waiter_token, Duration::from_millis(10), None).await
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    token.cancel();

    let err = tokio::time::timeout(Duration::from_secs(2), waiter)
        .await
        .expect("cancel-wait timed out")
        .expect("waiter join")
        .expect_err("expected Cancelled");
    assert!(matches!(err, AcquireError::Cancelled));
}

#[tokio::test]
async fn returns_cancelled_when_token_already_fired() {
    let dir = tempdir().unwrap();
    let path = mutex_path(&dir);

    let token = CancellationToken::new();
    token.cancel();
    let err = ServiceMutex::acquire(&path, token, Duration::from_millis(10), None)
        .await
        .expect_err("expected Cancelled");
    assert!(matches!(err, AcquireError::Cancelled));
}

#[tokio::test]
async fn returns_timeout_when_deadline_elapses() {
    let dir = tempdir().unwrap();
    let path = mutex_path(&dir);

    let _guard = ServiceMutex::acquire(
        &path,
        CancellationToken::new(),
        Duration::from_millis(10),
        None,
    )
    .await
    .expect("holder acquire");

    let err = ServiceMutex::acquire(
        &path,
        CancellationToken::new(),
        Duration::from_millis(10),
        Some(Duration::from_millis(80)),
    )
    .await
    .expect_err("expected Timeout");
    assert!(matches!(err, AcquireError::Timeout { .. }));
}

#[tokio::test]
async fn acquire_with_timeout_folds_cancellation_into_passive_exit() {
    let dir = tempdir().unwrap();
    let path = mutex_path(&dir);

    let _guard = ServiceMutex::acquire(
        &path,
        CancellationToken::new(),
        Duration::from_millis(10),
        None,
    )
    .await
    .expect("holder acquire");

    let token = CancellationToken::new();
    token.cancel();
    let outcome = ServiceMutex::acquire_with_timeout(&path, &token, Duration::from_secs(1))
        .await
        .expect("expected PassiveExit, not error");
    assert!(matches!(outcome, AcquireOutcome::PassiveExit));
}

#[tokio::test]
async fn acquire_with_timeout_returns_timeout_error_on_deadline() {
    let dir = tempdir().unwrap();
    let path = mutex_path(&dir);

    let _guard = ServiceMutex::acquire(
        &path,
        CancellationToken::new(),
        Duration::from_millis(10),
        None,
    )
    .await
    .expect("holder acquire");

    let token = CancellationToken::new();
    let err = ServiceMutex::acquire_with_timeout(&path, &token, Duration::from_millis(80))
        .await
        .expect_err("expected Timeout error");
    assert!(matches!(err, AcquireError::Timeout { .. }));
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
    use crate::bootstrap::{UnixSocketOptions, bind_unix_socket};
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
