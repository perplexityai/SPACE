use super::*;
use uuid::Uuid;

#[test]
fn ciphertext_publication_is_readable_without_exposing_private_backings() {
    use std::io::Write;
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    use std::os::unix::process::CommandExt;

    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o755)).unwrap();
    let instance = root.join("instance");
    let published = root.join("published");
    std::fs::create_dir(&instance).unwrap();
    std::fs::create_dir(&published).unwrap();
    let reader_gid = if nix::unistd::geteuid().is_root() {
        65534
    } else {
        nix::unistd::getegid().as_raw()
    };
    for dir in [&instance, &published] {
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    for name in ["snapshot.luks", "cow.luks"] {
        let src = instance.join(name);
        let dst = published.join(name);
        let mut backing = crate::device_backing::Backing::create(&src).unwrap();
        backing.file.write_all(b"sealed ciphertext").unwrap();
        // Repeated staging must replace the copy, not loosen the live backing.
        for _ in 0..2 {
            stage_ciphertext(&root, &src, &dst, reader_gid).unwrap();
            if nix::unistd::geteuid().is_root() {
                // Exercise the uploader's actual DAC boundary on root-run Linux
                // tests, without changing this test process's credentials.
                let read_as_peer = |file: &Path| {
                    std::process::Command::new("/bin/cat")
                        .arg(file)
                        .uid(65534)
                        .gid(reader_gid)
                        .output()
                        .unwrap()
                };
                let output = read_as_peer(&dst);
                assert!(
                    output.status.success(),
                    "{}",
                    String::from_utf8_lossy(&output.stderr)
                );
                assert_eq!(output.stdout, b"sealed ciphertext");
                assert!(!read_as_peer(&src).status.success());
            }
            let source = std::fs::metadata(&src).unwrap();
            let copy = std::fs::metadata(&dst).unwrap();
            assert_eq!(source.mode() & 0o7777, 0o600);
            assert_eq!(copy.mode() & 0o7777, 0o660);
            assert_eq!(copy.gid(), reader_gid);
            assert_ne!(source.ino(), copy.ino());
            assert_eq!(std::fs::read(&dst).unwrap(), b"sealed ciphertext");
        }
    }
}

#[tokio::test]
async fn cancelled_device_waiter_retains_serialization_until_work_finishes() {
    let service = crate::service::tests::service_tests::service_for_unit_tests().await;
    let id = sid().to_string();
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let worker = service.clone();
    let worker_id = id.clone();
    let waiter = tokio::spawn(async move {
        worker
            .run_owned_device_operation(
                &worker_id,
                crate::lifecycle_limiter::LifecycleMethod::PrepareSandbox,
                async move {
                    entered_tx.send(()).unwrap();
                    release_rx.await.unwrap();
                    Ok(())
                },
            )
            .await
    });
    entered_rx.await.unwrap();
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    let device = service.sandbox_device_stack_lock(&sid()).await;
    device.try_lock().unwrap_err();
    let (second_tx, mut second_rx) = tokio::sync::oneshot::channel();
    let second = service.run_owned_device_operation(
        &id,
        crate::lifecycle_limiter::LifecycleMethod::PrepareSandbox,
        async move {
            second_tx.send(()).unwrap();
            Ok(())
        },
    );
    tokio::pin!(second);
    assert!(futures_util::poll!(&mut second).is_pending());
    assert!(matches!(
        second_rx.try_recv(),
        Err(tokio::sync::oneshot::error::TryRecvError::Empty)
    ));
    release_tx.send(()).unwrap();
    second.await.unwrap();
    second_rx.await.unwrap();
    drop(device.try_lock().unwrap());
}

fn sid() -> SandboxId {
    SandboxId::from_uuid(Uuid::parse_str("019d84bd-280d-7331-99c6-b63377f3a3a2").unwrap())
}

#[test]
fn derived_paths_match_the_sandbox_layout() {
    let s = sid();
    assert_eq!(
        volume_backing_path(&s, "snapshot"),
        PathBuf::from(format!(
            "{}/019d84bd-280d-7331-99c6-b63377f3a3a2/instance/snapshot.luks",
            paths::SANDBOX_CACHE_DIR
        ))
    );
    assert_eq!(
        volume_mount_dir(&s, "snapshot"),
        PathBuf::from(format!(
            "{}/019d84bd-280d-7331-99c6-b63377f3a3a2/snapshot-vol",
            paths::SANDBOX_CACHE_DIR
        ))
    );
}

#[test]
fn credmgr_error_context_preserves_machine_readable_reason() {
    let mut metadata = tonic::metadata::MetadataMap::new();
    metadata.insert(
        pplx_space_credmgr_client::CREDMGR_ERROR_REASON_METADATA_KEY,
        pplx_space_credmgr_client::CREDMGR_ERROR_REASON_INVALID_KEY_REFERENCE
            .parse()
            .unwrap(),
    );
    let status = tonic::Status::with_metadata(
        tonic::Code::FailedPrecondition,
        "provider rejected key",
        metadata,
    );

    let contextualized = credmgr_status_with_context(status);

    assert!(pplx_space_credmgr_client::status_is_invalid_key_reference(
        &contextualized
    ));
    assert!(contextualized.message().contains("provider rejected key"));
}

#[tokio::test(start_paused = true)]
async fn dek_placement_retry_is_not_found_only_and_returns_the_final_status() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::Poll;

    assert!(
        DEK_FETCH_TIMEOUT
            > DEK_PLACEMENT_RETRY_DELAY * u32::try_from(DEK_PLACEMENT_MAX_ATTEMPTS - 1).unwrap()
    );

    let calls = Arc::new(AtomicUsize::new(0));
    let fetch_calls = calls.clone();
    let retry = retry_dek_fetch_on_placement_race(move || {
        let attempt = fetch_calls.fetch_add(1, Ordering::SeqCst) + 1;
        async move {
            if attempt == DEK_PLACEMENT_MAX_ATTEMPTS {
                let mut metadata = tonic::metadata::MetadataMap::new();
                metadata.insert("placement-proof", "final".parse().unwrap());
                Err::<(), _>(tonic::Status::with_details_and_metadata(
                    tonic::Code::NotFound,
                    "final placement miss",
                    tonic::codegen::Bytes::from_static(b"final-detail"),
                    metadata,
                ))
            } else {
                Err::<(), _>(tonic::Status::not_found(format!(
                    "placement miss {attempt}"
                )))
            }
        }
    });
    tokio::pin!(retry);

    assert!(matches!(futures_util::poll!(&mut retry), Poll::Pending));
    for completed_sleeps in 0..DEK_PLACEMENT_MAX_ATTEMPTS - 1 {
        assert_eq!(calls.load(Ordering::SeqCst), completed_sleeps + 1);
        tokio::time::advance(DEK_PLACEMENT_RETRY_DELAY - Duration::from_millis(1)).await;
        assert!(matches!(futures_util::poll!(&mut retry), Poll::Pending));
        assert_eq!(calls.load(Ordering::SeqCst), completed_sleeps + 1);
        tokio::time::advance(Duration::from_millis(1)).await;
        if completed_sleeps + 1 < DEK_PLACEMENT_MAX_ATTEMPTS - 1 {
            assert!(matches!(futures_util::poll!(&mut retry), Poll::Pending));
        }
    }
    let error = match futures_util::poll!(&mut retry) {
        Poll::Ready(Err(error)) => error,
        other => panic!("tenth NotFound must be returned unchanged, got {other:?}"),
    };
    assert_eq!(calls.load(Ordering::SeqCst), DEK_PLACEMENT_MAX_ATTEMPTS);
    assert_eq!(error.code(), tonic::Code::NotFound);
    assert_eq!(error.message(), "final placement miss");
    assert_eq!(error.details(), b"final-detail");
    assert_eq!(error.metadata().get("placement-proof").unwrap(), "final");

    let calls = Arc::new(AtomicUsize::new(0));
    let fetch_calls = calls.clone();
    let error = retry_dek_fetch_on_placement_race(move || {
        fetch_calls.fetch_add(1, Ordering::SeqCst);
        async {
            Err::<(), _>(tonic::Status::with_details(
                tonic::Code::PermissionDenied,
                "not a placement race",
                tonic::codegen::Bytes::from_static(b"denied-detail"),
            ))
        }
    })
    .await
    .unwrap_err();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(error.code(), tonic::Code::PermissionDenied);
    assert_eq!(error.message(), "not a placement race");
    assert_eq!(error.details(), b"denied-detail");
}

#[test]
fn volume_name_accepts_the_snapshot_volume_and_rejects_unsafe_names() {
    assert_eq!(validate_volume_name("snapshot").unwrap(), "snapshot");
    assert_eq!(validate_volume_name(" data-1 ").unwrap(), "data-1");

    for bad in ["", "  ", "UPPER", "a/b", "a b", "a\0b", "..", "名前"] {
        validate_volume_name(bad).unwrap_err();
    }
    validate_volume_name(&"x".repeat(MAX_VOLUME_NAME_LEN + 1)).unwrap_err();
}

#[test]
fn sandbox_scoped_path_requires_the_calling_sandbox_prefix() {
    let s = sid();
    let base = paths::sandbox_cache_dir(&s.to_string());
    let ok = validate_sandbox_scoped_path(
        &s,
        &format!("{base}/snapshot/cow.luks"),
        "restore_cow_source",
    )
    .expect("path under the sandbox cache dir must be accepted");
    assert!(ok.ends_with("snapshot/cow.luks"));

    // Another sandbox's dir, relative paths, traversal, and prefix tricks
    // (sibling dir sharing the id as a name prefix) are all rejected.
    let other = paths::sandbox_cache_dir("ffffffff-280d-7331-99c6-b63377f3a3a2");
    for bad in [
        format!("{other}/snapshot/cow.luks"),
        "relative/cow.luks".to_string(),
        format!("{base}/../etc/shadow"),
        format!("{base}-evil/cow.luks"),
        String::new(),
    ] {
        assert!(
            validate_sandbox_scoped_path(&s, &bad, "restore_cow_source").is_err(),
            "path {bad:?} must be rejected"
        );
    }
}

#[test]
fn stage_dst_name_must_be_a_bare_file_name() {
    assert_eq!(
        validate_stage_dst_name("snapshot.luks").unwrap(),
        "snapshot.luks"
    );
    for bad in ["", ".", "..", "a/b", "/abs"] {
        assert!(
            validate_stage_dst_name(bad).is_err(),
            "dst name {bad:?} must be rejected"
        );
    }
}
