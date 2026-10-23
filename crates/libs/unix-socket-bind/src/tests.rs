use super::*;

#[test]
fn unix_socket_staging_path_preserves_the_published_path_budget() {
    let socket_path = Path::new(
        "/tmp/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.sock",
    );

    let staging_path =
        unix_socket_staging_path_for_attempt(socket_path, Uuid::now_v7(), 0).unwrap();

    assert!(
        staging_path.as_os_str().as_bytes().len() <= socket_path.as_os_str().as_bytes().len(),
        "staging path {} exceeds published path budget {}",
        staging_path.display(),
        socket_path.display()
    );
}

#[test]
fn concurrent_unix_socket_replacements_use_distinct_staging_paths() {
    let socket_path = Path::new("/tmp/handoff.sock");

    let first = unix_socket_staging_path_for_attempt(socket_path, Uuid::from_u128(0), 0).unwrap();
    let second = unix_socket_staging_path_for_attempt(socket_path, Uuid::from_u128(1), 0).unwrap();

    assert_ne!(
        first, second,
        "concurrent binders must not share staging socket ownership"
    );
}

#[test]
fn compact_unix_socket_staging_keeps_a_minimum_nonce_budget() {
    let socket_path = Path::new("/tmp/1234567890");

    let staging_path =
        unix_socket_staging_path_for_attempt(socket_path, Uuid::from_u128(0), 0).unwrap();
    let staging_file_name = staging_path.file_name().unwrap().as_bytes();

    assert_eq!(staging_file_name.len(), 10);
    assert!(
        !staging_file_name.starts_with(b".handoff-"),
        "readable prefix must not consume the compact nonce budget"
    );
}

#[test]
fn compact_unix_socket_staging_attempts_do_not_repeat() {
    let socket_path = Path::new("/tmp/s");
    let seed = Uuid::from_u128(0);
    let paths = (0..64)
        .map(|attempt| unix_socket_staging_path_for_attempt(socket_path, seed, attempt).unwrap())
        .collect::<std::collections::HashSet<_>>();

    assert_eq!(paths.len(), 64);
}

#[test]
fn unix_socket_replacement_does_not_unlink_another_binders_staging_path() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()
        .unwrap();
    let _runtime_guard = runtime.enter();
    let directory = tempfile::tempdir().unwrap();
    let socket_path = directory.path().join("handoff.sock");
    let staging_path = directory.path().join(".handoff.sock.handoff-collision");
    let _published = bind_unix_socket(&socket_path, UnixSocketOptions::new(0o600)).unwrap();
    let competing_listener = std::os::unix::net::UnixListener::bind(&staging_path).unwrap();

    let error =
        bind_unix_socket_replacement_at(&socket_path, UnixSocketOptions::new(0o600), &staging_path)
            .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("failed to bind unix socket staging path"),
        "unexpected replacement error: {error:#}"
    );
    let connection = std::os::unix::net::UnixStream::connect(&staging_path).unwrap();
    let (accepted, _) = competing_listener.accept().unwrap();
    drop((connection, accepted));
}

#[test]
fn unix_socket_rebind_never_removes_published_path() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()
        .unwrap();
    let _runtime_guard = runtime.enter();
    let directory = tempfile::tempdir().unwrap();
    let socket_path = directory.path().join("handoff.sock");
    let listener = bind_unix_socket(&socket_path, UnixSocketOptions::new(0o600)).unwrap();
    let start_acceptor = |listener: tokio::net::UnixListener| {
        let running = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
        let thread_running = running.clone();
        let listener = listener.into_std().unwrap();
        listener.set_nonblocking(true).unwrap();
        let thread = std::thread::spawn(move || {
            while thread_running.load(std::sync::atomic::Ordering::Relaxed) {
                match listener.accept() {
                    Ok((stream, _)) => drop(stream),
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::yield_now();
                    }
                    Err(_) => break,
                }
            }
        });
        (running, thread)
    };
    let (mut accept_running, mut accept_thread) = start_acceptor(listener);

    let running = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
    let successes = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let not_found = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let other = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let connector = {
        let path = socket_path.clone();
        let running = running.clone();
        let successes = successes.clone();
        let not_found = not_found.clone();
        let other = other.clone();
        std::thread::spawn(move || {
            while running.load(std::sync::atomic::Ordering::Relaxed) {
                match std::os::unix::net::UnixStream::connect(&path) {
                    Ok(stream) => {
                        successes.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        drop(stream);
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        not_found.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    // The synthetic connection storm can fill a listener backlog.
                    Err(error) if error.kind() == std::io::ErrorKind::ConnectionRefused => {}
                    Err(_) => {
                        other.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                }
                std::thread::sleep(std::time::Duration::from_micros(200));
            }
        })
    };

    for _ in 0..1_000 {
        let listener = bind_unix_socket(&socket_path, UnixSocketOptions::new(0o600)).unwrap();
        let (next_accept_running, next_accept_thread) = start_acceptor(listener);
        accept_running.store(false, std::sync::atomic::Ordering::Relaxed);
        accept_thread.join().unwrap();
        accept_running = next_accept_running;
        accept_thread = next_accept_thread;
    }

    running.store(false, std::sync::atomic::Ordering::Relaxed);
    connector.join().unwrap();
    accept_running.store(false, std::sync::atomic::Ordering::Relaxed);
    accept_thread.join().unwrap();
    let successes = successes.load(std::sync::atomic::Ordering::Relaxed);
    let not_found = not_found.load(std::sync::atomic::Ordering::Relaxed);
    let other = other.load(std::sync::atomic::Ordering::Relaxed);
    assert!(
        successes > 0,
        "connector must exercise the published socket"
    );
    assert_eq!(
        (not_found, other),
        (0, 0),
        "the published socket path must never disappear while it is rebound",
    );
}
