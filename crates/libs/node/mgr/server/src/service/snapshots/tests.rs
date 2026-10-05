use super::{
    ENC_SNAPSHOT_MEMORY_NAME, ENC_SNAPSHOT_VMSTATE_NAME, reown_encrypted_snapshot_inputs,
    set_snapshot_export_modes, validate_cache_artifact_path,
};
use std::os::unix::fs::PermissionsExt;

#[test]
fn snapshot_identity_inventory_survives_failed_or_exhausted_measurements() {
    let root = tempfile::tempdir().unwrap();
    let snapshots = root.path().join("snapshots");
    std::fs::create_dir(&snapshots).unwrap();
    for name in [
        "d-1m-001",
        "d-1m-002",
        "d-1m-003",
        "f-20260921T120000Z",
        "invalid",
    ] {
        std::fs::create_dir(snapshots.join(name)).unwrap();
    }
    for budget in [
        std::time::Duration::ZERO,
        std::time::Duration::from_secs(30),
    ] {
        let (paths, token, _) = super::super::snapshot_inventory::SnapshotInventory::default()
            .page(super::SandboxId::new(), root.path(), "")
            .unwrap();
        assert!(token.is_empty());
        let inventory = super::measure_snapshots(paths, budget, |path, _| {
            assert!(
                !budget.is_zero(),
                "exhausted or skipped measurement must not start work"
            );
            if path.ends_with("001") {
                Err("measurement timed out".into())
            } else if path.ends_with("002") {
                Ok(None)
            } else {
                Ok(Some(super::BtrfsDiskUsage {
                    exclusive_bytes: 0,
                    total_bytes: 12,
                }))
            }
        });
        assert_eq!(
            inventory
                .iter()
                .map(|snapshot| snapshot.name.as_str())
                .collect::<Vec<_>>(),
            ["d-1m-001", "d-1m-002", "d-1m-003", "f-20260921T120000Z"]
        );
        assert!(inventory[0].exclusive_bytes.is_none());
        assert!(inventory[1].exclusive_bytes.is_none());
        assert_eq!(
            inventory[2].exclusive_bytes,
            (!budget.is_zero()).then_some(0)
        );
        assert_eq!(
            inventory[2].referenced_bytes,
            (!budget.is_zero()).then_some(12)
        );
    }
    let invalid_root = root.path().join("invalid-root");
    std::fs::create_dir(&invalid_root).unwrap();
    std::fs::write(invalid_root.join("snapshots"), []).unwrap();
    let error = super::super::snapshot_inventory::SnapshotInventory::default()
        .page(super::SandboxId::new(), &invalid_root, "")
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::Unavailable);
}

#[test]
fn plaintext_load_rejects_replaced_source_and_safely_retries_existing_destination() {
    use std::os::unix::fs::symlink;
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let source = root.join("snapshot");
    let jail = root.join("jail");
    std::fs::create_dir(&source).unwrap();
    std::fs::create_dir(&jail).unwrap();
    std::fs::write(source.join("memory"), b"memory").unwrap();
    std::fs::write(source.join("vmstate"), b"snapshot").unwrap();
    let victim = root.join("victim");
    std::fs::write(&victim, b"private").unwrap();
    // Replace an input after the RPC's preliminary pathname validation.
    std::fs::rename(source.join("vmstate"), source.join("original")).unwrap();
    for hard in [false, true] {
        if hard {
            std::fs::hard_link(&victim, source.join("vmstate")).unwrap();
        } else {
            symlink(&victim, source.join("vmstate")).unwrap();
        }
        super::stage_plaintext_snapshot_inputs(&source, &jail).unwrap_err();
        assert!(!jail.join("vmstate").exists());
        std::fs::remove_file(source.join("vmstate")).unwrap();
    }
    std::fs::rename(source.join("original"), source.join("vmstate")).unwrap();
    for destination in ["absent", "regular", "symlink", "hardlink"] {
        if jail.join("vmstate").symlink_metadata().is_ok() {
            std::fs::remove_file(jail.join("vmstate")).unwrap();
        }
        match destination {
            "regular" => std::fs::write(jail.join("vmstate"), b"old").unwrap(),
            "symlink" => symlink(&victim, jail.join("vmstate")).unwrap(),
            "hardlink" => std::fs::hard_link(&victim, jail.join("vmstate")).unwrap(),
            _ => {}
        }
        super::stage_plaintext_snapshot_inputs(&source, &jail).unwrap();
        super::stage_plaintext_snapshot_inputs(&source, &jail).unwrap();
        assert_eq!(std::fs::read(jail.join("vmstate")).unwrap(), b"snapshot");
        assert_eq!(std::fs::read(&victim).unwrap(), b"private");
    }
}

#[test]
fn disk_snapshot_source_is_readable_by_the_shared_cache_group() {
    let temp = tempfile::tempdir().unwrap();
    let instance = temp.path().canonicalize().unwrap().join("instance");
    std::fs::create_dir(&instance).unwrap();
    let rootfs = instance.join("rootfs.ext4");
    std::fs::write(&rootfs, b"disk").unwrap();
    std::fs::set_permissions(&instance, std::fs::Permissions::from_mode(0o700)).unwrap();
    std::fs::set_permissions(&rootfs, std::fs::Permissions::from_mode(0o600)).unwrap();

    set_snapshot_export_modes(&instance).unwrap();

    assert_eq!(
        std::fs::metadata(&instance).unwrap().permissions().mode() & 0o770,
        0o770
    );
    assert_eq!(
        std::fs::metadata(&rootfs).unwrap().permissions().mode() & 0o660,
        0o660
    );
}

/// Validate both restored inputs before changing any ownership. A jailed
/// writer must not turn the privileged ownership transfer into a link escape.
#[test]
fn encrypted_load_rejects_links_before_changing_ownership() {
    use std::os::unix::fs::{MetadataExt, symlink};
    let temp = tempfile::tempdir().unwrap();
    let victim = temp.path().join("victim");
    std::fs::write(&victim, b"outside").unwrap();
    let original = std::fs::metadata(&victim).unwrap();
    for name in [ENC_SNAPSHOT_VMSTATE_NAME, ENC_SNAPSHOT_MEMORY_NAME] {
        for hard in [false, true] {
            let volume = tempfile::tempdir_in(temp.path()).unwrap();
            for input in [ENC_SNAPSHOT_VMSTATE_NAME, ENC_SNAPSHOT_MEMORY_NAME] {
                let path = volume.path().join(input);
                if input != name {
                    std::fs::write(path, b"snapshot").unwrap();
                } else if hard {
                    std::fs::hard_link(&victim, path).unwrap();
                } else {
                    symlink(&victim, path).unwrap();
                }
            }
            let pinned = std::fs::File::open(volume.path()).unwrap();
            let error = reown_encrypted_snapshot_inputs(&pinned, original.uid()).unwrap_err();
            assert!(
                error
                    .message()
                    .contains(if hard { "multiply linked" } else { "ELOOP" }),
                "{error}"
            );
            let after = std::fs::metadata(&victim).unwrap();
            assert_eq!(
                (after.uid(), after.gid(), after.mode()),
                (original.uid(), original.gid(), original.mode())
            );
        }
    }
}

fn cache_root() -> &'static str {
    pplx_space_common_core::host::linux::paths::SPACE_CACHE_DIR
}

#[test]
fn accepts_paths_strictly_inside_the_cache_root() {
    let root = super::CACHE_DIR;
    let id = uuid::Uuid::now_v7();
    for suffix in [
        format!("{id}/snapshot"),
        format!("{id}/snapshots/name.gc-pending-x"),
        format!(".fork-copies/{id}"),
    ] {
        validate_cache_artifact_path(&format!("{root}/{suffix}")).unwrap();
    }
    for path in [
        format!("{root}/{id}"),
        format!("{root}/.fork-copies"),
        format!("{}/firecracker/templates/name/hash", cache_root()),
    ] {
        assert!(validate_cache_artifact_path(&path).is_err());
    }
}

/// `<root>/` and `<root>/.` both resolve to the cache root
/// itself — deleting it would wipe every cached artifact on the
/// node. The validator must reject anything that doesn't name an
/// entry strictly inside the root.
#[test]
fn rejects_the_cache_root_itself() {
    let root = cache_root();
    assert!(validate_cache_artifact_path(root).is_err());
    assert!(validate_cache_artifact_path(&format!("{root}/")).is_err());
    assert!(validate_cache_artifact_path(&format!("{root}/.")).is_err());
}

#[test]
fn rejects_traversal_and_degenerate_components() {
    let root = cache_root();
    assert!(validate_cache_artifact_path(&format!("{root}/foo/../bar")).is_err());
    assert!(validate_cache_artifact_path(&format!("{root}/foo/..")).is_err());
    assert!(validate_cache_artifact_path(&format!("{root}/./foo")).is_err());
    assert!(validate_cache_artifact_path(&format!("{root}//foo")).is_err());
    assert!(validate_cache_artifact_path(&format!("{root}/foo/")).is_err());
    assert!(validate_cache_artifact_path("/other/root/foo").is_err());
}

/// Finding 1: the capture runs bind -> FC -> unmount as one detached unit via
/// `run_detached`, so a deadline-cancelled caller cannot leave a created mount
/// unowned. This models that ordering: the caller's await is dropped WHILE the
/// body is between "mount created" and "unmounted", and the detached body must
/// still run to completion and leave nothing mounted.
#[tokio::test]
async fn detached_capture_finishes_its_cleanup_after_the_caller_cancels() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;

    let mounts = Arc::new(AtomicUsize::new(0));
    let done = Arc::new(AtomicBool::new(false));
    let body = {
        let mounts = Arc::clone(&mounts);
        let done = Arc::clone(&done);
        super::run_detached(
            tokio_util::task::TaskTracker::new(),
            "test-capture",
            async move {
                mounts.fetch_add(1, Ordering::SeqCst); // bind created
                tokio::time::sleep(Duration::from_millis(30)).await; // uncancellable work
                mounts.fetch_sub(1, Ordering::SeqCst); // always unmounted by the same unit
                done.store(true, Ordering::SeqCst);
                Ok::<(), tonic::Status>(())
            },
        )
    };

    // The caller is cancelled (deadline) mid-body: drop its await.
    tokio::time::timeout(Duration::from_millis(1), body)
        .await
        .expect_err("caller should be cancelled by the timeout");

    // The detached unit still runs to completion and leaves no mount behind.
    for _ in 0..200 {
        if done.load(Ordering::SeqCst) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(done.load(Ordering::SeqCst), "detached cleanup did not run");
    assert_eq!(
        mounts.load(Ordering::SeqCst),
        0,
        "a cancelled caller left a mount owned by no one"
    );
}

// A Firecracker API that accepts the connection and never answers fails the
// pause or resume at its deadline instead of holding the VM gate forever.
#[tokio::test]
async fn a_firecracker_that_never_answers_fails_a_pause_state_change_at_its_deadline() {
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("firecracker.sock");
    let listener = tokio::net::UnixListener::bind(&socket).unwrap();
    let silent = tokio::spawn(async move {
        let (connection, _) = listener.accept().await.unwrap();
        // Hold the connection open without answering.
        std::future::pending::<()>().await;
        drop(connection);
    });
    let client = crate::fc_api::FcApiClient::new(&socket);
    for paused in [false, true] {
        let started = std::time::Instant::now();
        let error =
            super::set_firecracker_paused(&client, paused, std::time::Duration::from_millis(200))
                .await
                .expect_err("a silent Firecracker fails the change");
        assert_eq!(error.code(), tonic::Code::DeadlineExceeded, "{paused}");
        assert!(started.elapsed() < std::time::Duration::from_secs(5));
    }
    silent.abort();
}
