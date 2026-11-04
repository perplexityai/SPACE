use std::fs;
use std::sync::Arc;
use std::sync::mpsc;
use std::time::Duration;

use pplx_space_common_core::host::disk_admission::CacheDiskAdmissionConfig;
use rstest::{fixture, rstest};
use tempfile::TempDir;

use super::*;

#[test]
fn handoff_io_rejects_redirected_inputs() {
    use std::os::unix::fs::symlink;

    for redirected_instance in [false, true] {
        let cache = tempfile::tempdir_in(std::env::temp_dir().canonicalize().unwrap()).unwrap();
        let instance = cache
            .path()
            .join(SandboxId::new().to_string())
            .join("instance");
        let outside = tempfile::tempdir_in(std::env::temp_dir().canonicalize().unwrap()).unwrap();
        fs::create_dir_all(instance.parent().unwrap()).unwrap();
        fs::write(outside.path().join(IN_PROGRESS_MARKER), "1").unwrap();
        fs::write(outside.path().join("rootfs.ext4"), "sentinel").unwrap();
        if redirected_instance {
            symlink(outside.path(), &instance).unwrap();
        } else {
            fs::create_dir(&instance).unwrap();
            symlink(
                outside.path().join(IN_PROGRESS_MARKER),
                instance.join(IN_PROGRESS_MARKER),
            )
            .unwrap();
        }
        read_handoff_state(&instance).unwrap_err();
        if redirected_instance {
            assert!(write_marker(&instance, IN_PROGRESS_MARKER, 2).is_err());
            assert!(remove_if_present(&instance.join("rootfs.ext4"), "test removal").is_err());
        }
        assert_eq!(
            fs::read_to_string(outside.path().join(IN_PROGRESS_MARKER)).unwrap(),
            "1"
        );
        assert_eq!(
            fs::read_to_string(outside.path().join("rootfs.ext4")).unwrap(),
            "sentinel"
        );
    }
    for shape in ["directory", "fifo", "hardlink", "oversized"] {
        let cache = tempfile::tempdir_in(std::env::temp_dir().canonicalize().unwrap()).unwrap();
        let instance = cache
            .path()
            .join(SandboxId::new().to_string())
            .join("instance");
        fs::create_dir_all(&instance).unwrap();
        let marker = instance.join(IN_PROGRESS_MARKER);
        match shape {
            "directory" => fs::create_dir(&marker).unwrap(),
            "fifo" => nix::unistd::mkfifo(&marker, nix::sys::stat::Mode::S_IRUSR).unwrap(),
            "hardlink" => {
                let source = cache.path().join("source");
                fs::write(&source, "1").unwrap();
                fs::hard_link(&source, &marker).unwrap();
                fs::hard_link(&source, instance.join("rootfs.ext4")).unwrap();
                validate_rootfs_file(&instance.join("rootfs.ext4"), 1).unwrap_err();
            }
            "oversized" => fs::write(&marker, format!("{}1", " ".repeat(128))).unwrap(),
            _ => unreachable!(),
        }
        read_handoff_state(&instance).unwrap_err();
    }
}

struct PreparedRootfsFixture {
    _tempdir: TempDir,
    rootfs_path: PathBuf,
    sandbox_id: SandboxId,
    reservations: PreparedRootfsReservations,
    disk_admission: Arc<DiskAdmission>,
}

#[fixture]
async fn prepared_rootfs() -> PreparedRootfsFixture {
    let tempdir = tempfile::tempdir_in(std::env::temp_dir().canonicalize().unwrap()).unwrap();
    let sandbox_id = SandboxId::new();
    PreparedRootfsFixture {
        rootfs_path: tempdir
            .path()
            .join(sandbox_id.to_string())
            .join("instance/rootfs.ext4"),
        sandbox_id,
        reservations: PreparedRootfsReservations::default(),
        disk_admission: Arc::new(
            DiskAdmission::new(CacheDiskAdmissionConfig {
                enabled: false,
                ..Default::default()
            })
            .await,
        ),
        _tempdir: tempdir,
    }
}

#[rstest]
#[tokio::test]
async fn oversized_external_rootfs_is_rejected_before_reservation_or_marker(
    #[future] prepared_rootfs: PreparedRootfsFixture,
) {
    let fixture = prepared_rootfs.await;
    let error = fixture
        .reservations
        .reserve(
            &fixture.disk_admission,
            fixture.sandbox_id,
            &fixture.rootfs_path,
            128 * 1024 + 1,
        )
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert_eq!(fixture.disk_admission.pending_reservation_bytes(), 0);
    assert!(!fixture.rootfs_path.parent().unwrap().exists());
}

fn observe_runtime_progress(
    pause: Arc<ReservePause>,
    progressed: mpsc::Receiver<()>,
) -> std::thread::JoinHandle<bool> {
    std::thread::spawn(move || {
        pause.entered.wait();
        let progressed = progressed.recv_timeout(Duration::from_secs(1)).is_ok();
        pause.release.wait();
        progressed
    })
}

#[rstest]
#[tokio::test]
async fn concurrent_reserve_waits_for_the_first_handoff_claim(
    #[future] prepared_rootfs: PreparedRootfsFixture,
) {
    let mut fixture = prepared_rootfs.await;
    let pause = Arc::new(ReservePause::new());
    fixture.reservations.reserve_pause = Some(Arc::clone(&pause));

    std::thread::scope(|scope| {
        let reservations = &fixture.reservations;
        let disk_admission = &fixture.disk_admission;
        let sandbox_id = fixture.sandbox_id;
        let rootfs_path = &fixture.rootfs_path;
        let (first_tx, first_rx) = mpsc::channel();
        scope.spawn(move || {
            first_tx
                .send(reservations.reserve(disk_admission, sandbox_id, rootfs_path, 1))
                .unwrap();
        });
        pause.entered.wait();

        let (second_tx, second_rx) = mpsc::channel();
        scope.spawn(move || {
            second_tx
                .send(reservations.reserve(disk_admission, sandbox_id, rootfs_path, 1))
                .unwrap();
        });
        let second_returned_early = second_rx.recv_timeout(Duration::from_millis(100)).ok();
        let returned_early = second_returned_early.is_some();
        pause.release.wait();

        first_rx.recv().unwrap().unwrap();
        match second_returned_early {
            Some(result) => result.unwrap(),
            None => second_rx.recv().unwrap().unwrap(),
        };
        assert!(!returned_early);
    });

    assert_eq!(fixture.reservations.active.len(), 1);
    assert_eq!(
        fixture.disk_admission.pending_reservation_bytes(),
        fixture
            .disk_admission
            .config()
            .allocation_reservation_bytes
            .max(MIB)
    );
    assert_eq!(
        read_handoff_state(fixture.rootfs_path.parent().unwrap()).unwrap(),
        HandoffState::InProgress(1)
    );
}

#[rstest]
#[tokio::test(flavor = "current_thread")]
async fn reservation_filesystem_work_does_not_block_the_runtime(
    #[future] prepared_rootfs: PreparedRootfsFixture,
) {
    let mut fixture = prepared_rootfs.await;
    let pause = Arc::new(ReservePause::new());
    fixture.reservations.reserve_pause = Some(Arc::clone(&pause));
    let (progressed, progress) = mpsc::channel();
    let observer = observe_runtime_progress(pause, progress);

    let reserve = fixture.reservations.reserve_on_blocking_pool(
        Arc::clone(&fixture.disk_admission),
        fixture.sandbox_id,
        fixture.rootfs_path.clone(),
        1,
    );
    let heartbeat = async {
        tokio::task::yield_now().await;
        progressed.send(()).unwrap();
    };
    let (result, ()) = tokio::join!(reserve, heartbeat);

    result.unwrap();
    assert!(observer.join().unwrap());
}

#[rstest]
#[tokio::test(flavor = "current_thread")]
async fn completion_filesystem_work_does_not_block_the_runtime(
    #[future] prepared_rootfs: PreparedRootfsFixture,
) {
    let mut fixture = prepared_rootfs.await;
    fixture
        .reservations
        .reserve(
            &fixture.disk_admission,
            fixture.sandbox_id,
            &fixture.rootfs_path,
            1,
        )
        .unwrap();
    fs::File::create(&fixture.rootfs_path)
        .unwrap()
        .set_len(MIB)
        .unwrap();
    let pause = Arc::new(ReservePause::new());
    fixture.reservations.commit_pause = Some(Arc::clone(&pause));
    let (progressed, progress) = mpsc::channel();
    let observer = observe_runtime_progress(pause, progress);

    let complete = fixture.reservations.complete(
        &fixture.disk_admission,
        fixture.sandbox_id,
        &fixture.rootfs_path,
        PreparedRootfsDisposition::Commit,
    );
    let heartbeat = async {
        tokio::task::yield_now().await;
        progressed.send(()).unwrap();
    };
    let (result, ()) = tokio::join!(complete, heartbeat);

    result.unwrap();
    assert!(observer.join().unwrap());
}

#[rstest]
#[tokio::test]
async fn commit_rejects_a_handoff_removed_after_rootfs_validation(
    #[future] prepared_rootfs: PreparedRootfsFixture,
) {
    let mut fixture = prepared_rootfs.await;
    fixture
        .reservations
        .reserve(
            &fixture.disk_admission,
            fixture.sandbox_id,
            &fixture.rootfs_path,
            1,
        )
        .unwrap();
    fs::File::create(&fixture.rootfs_path)
        .unwrap()
        .set_len(MIB)
        .unwrap();
    let pause = Arc::new(ReservePause::new());
    fixture.reservations.commit_pause = Some(Arc::clone(&pause));

    let status = std::thread::scope(|scope| {
        let reservations = &fixture.reservations;
        let disk_admission = &fixture.disk_admission;
        let sandbox_id = fixture.sandbox_id;
        let rootfs_path = &fixture.rootfs_path;
        let (complete_tx, complete_rx) = mpsc::channel();
        scope.spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            complete_tx
                .send(runtime.block_on(reservations.complete(
                    disk_admission,
                    sandbox_id,
                    rootfs_path,
                    PreparedRootfsDisposition::Commit,
                )))
                .unwrap();
        });
        pause.entered.wait();
        fs::remove_file(
            fixture
                .rootfs_path
                .parent()
                .unwrap()
                .join(IN_PROGRESS_MARKER),
        )
        .unwrap();
        pause.release.wait();
        complete_rx.recv().unwrap().unwrap_err()
    });

    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    assert!(
        fixture
            .reservations
            .active
            .contains_key(&fixture.sandbox_id)
    );
    assert!(fixture.disk_admission.pending_reservation_bytes() > 0);
}

#[rstest]
#[tokio::test]
async fn reservation_is_held_until_a_valid_rootfs_is_committed(
    #[future] prepared_rootfs: PreparedRootfsFixture,
) {
    let fixture = prepared_rootfs.await;

    fixture
        .reservations
        .reserve(
            &fixture.disk_admission,
            fixture.sandbox_id,
            &fixture.rootfs_path,
            1,
        )
        .unwrap();
    fs::File::create(&fixture.rootfs_path)
        .unwrap()
        .set_len(MIB)
        .unwrap();
    assert!(fixture.disk_admission.pending_reservation_bytes() > 0);
    let error = validate_ready_handoff(
        fixture.rootfs_path.parent().unwrap(),
        &fixture.rootfs_path,
        Some(1),
    )
    .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);

    fixture
        .reservations
        .complete(
            &fixture.disk_admission,
            fixture.sandbox_id,
            &fixture.rootfs_path,
            PreparedRootfsDisposition::Commit,
        )
        .await
        .unwrap();

    assert_eq!(fixture.disk_admission.pending_reservation_bytes(), 0);
    assert!(
        validate_ready_handoff(
            fixture.rootfs_path.parent().unwrap(),
            &fixture.rootfs_path,
            Some(1),
        )
        .unwrap()
    );
}

#[rstest]
#[tokio::test]
async fn repeated_reserve_repairs_a_missing_handoff_marker(
    #[future] prepared_rootfs: PreparedRootfsFixture,
) {
    let fixture = prepared_rootfs.await;
    fixture
        .reservations
        .reserve(
            &fixture.disk_admission,
            fixture.sandbox_id,
            &fixture.rootfs_path,
            1,
        )
        .unwrap();
    fs::remove_file(
        fixture
            .rootfs_path
            .parent()
            .unwrap()
            .join(IN_PROGRESS_MARKER),
    )
    .unwrap();

    fixture
        .reservations
        .reserve(
            &fixture.disk_admission,
            fixture.sandbox_id,
            &fixture.rootfs_path,
            1,
        )
        .unwrap();
    fs::File::create(&fixture.rootfs_path)
        .unwrap()
        .set_len(MIB)
        .unwrap();
    fixture
        .reservations
        .complete(
            &fixture.disk_admission,
            fixture.sandbox_id,
            &fixture.rootfs_path,
            PreparedRootfsDisposition::Commit,
        )
        .await
        .unwrap();

    assert_eq!(
        read_handoff_state(fixture.rootfs_path.parent().unwrap()).unwrap(),
        HandoffState::Ready(1)
    );
    assert_eq!(fixture.disk_admission.pending_reservation_bytes(), 0);
}

#[rstest]
#[tokio::test]
async fn marker_write_atomically_replaces_the_destination(
    #[future] prepared_rootfs: PreparedRootfsFixture,
) {
    let fixture = prepared_rootfs.await;
    let instance_dir = fixture.rootfs_path.parent().unwrap();
    fs::create_dir_all(instance_dir).unwrap();
    let sentinel = fixture._tempdir.path().join("sentinel");
    fs::write(&sentinel, "outside").unwrap();
    std::os::unix::fs::symlink(&sentinel, instance_dir.join(IN_PROGRESS_MARKER)).unwrap();

    write_marker(instance_dir, IN_PROGRESS_MARKER, 1).unwrap();

    assert_eq!(fs::read_to_string(sentinel).unwrap(), "outside");
    assert_eq!(
        read_handoff_state(instance_dir).unwrap(),
        HandoffState::InProgress(1)
    );
    assert!(
        !fs::symlink_metadata(instance_dir.join(IN_PROGRESS_MARKER))
            .unwrap()
            .file_type()
            .is_symlink()
    );
}

#[rstest]
#[tokio::test]
async fn required_handoff_rejects_disappearance_after_initial_validation(
    #[future] prepared_rootfs: PreparedRootfsFixture,
) {
    let fixture = prepared_rootfs.await;
    fixture
        .reservations
        .reserve(
            &fixture.disk_admission,
            fixture.sandbox_id,
            &fixture.rootfs_path,
            1,
        )
        .unwrap();
    fs::File::create(&fixture.rootfs_path)
        .unwrap()
        .set_len(MIB)
        .unwrap();
    fixture
        .reservations
        .complete(
            &fixture.disk_admission,
            fixture.sandbox_id,
            &fixture.rootfs_path,
            PreparedRootfsDisposition::Commit,
        )
        .await
        .unwrap();
    assert!(
        validate_ready_handoff(
            fixture.rootfs_path.parent().unwrap(),
            &fixture.rootfs_path,
            Some(1),
        )
        .unwrap()
    );
    fs::remove_file(fixture.rootfs_path.parent().unwrap().join(READY_MARKER)).unwrap();

    let error = require_ready_handoff(
        fixture.rootfs_path.parent().unwrap(),
        &fixture.rootfs_path,
        Some(1),
    )
    .unwrap_err();

    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
}

#[rstest]
#[tokio::test]
async fn abort_removes_partial_rootfs_and_releases_reservation(
    #[future] prepared_rootfs: PreparedRootfsFixture,
) {
    let fixture = prepared_rootfs.await;
    fixture
        .reservations
        .reserve(
            &fixture.disk_admission,
            fixture.sandbox_id,
            &fixture.rootfs_path,
            1,
        )
        .unwrap();
    fs::write(&fixture.rootfs_path, b"partial").unwrap();

    fixture
        .reservations
        .complete(
            &fixture.disk_admission,
            fixture.sandbox_id,
            &fixture.rootfs_path,
            PreparedRootfsDisposition::Abort,
        )
        .await
        .unwrap();

    assert!(!fixture.rootfs_path.exists());
    assert_eq!(fixture.disk_admission.pending_reservation_bytes(), 0);
    assert_eq!(
        read_handoff_state(fixture.rootfs_path.parent().unwrap()).unwrap(),
        HandoffState::Absent
    );
}

#[rstest]
#[tokio::test]
async fn abort_removes_partial_rootfs_when_handoff_marker_is_missing(
    #[future] prepared_rootfs: PreparedRootfsFixture,
) {
    let fixture = prepared_rootfs.await;
    fixture
        .reservations
        .reserve(
            &fixture.disk_admission,
            fixture.sandbox_id,
            &fixture.rootfs_path,
            1,
        )
        .unwrap();
    fs::write(&fixture.rootfs_path, b"partial").unwrap();
    fs::remove_file(
        fixture
            .rootfs_path
            .parent()
            .unwrap()
            .join(IN_PROGRESS_MARKER),
    )
    .unwrap();

    fixture
        .reservations
        .complete(
            &fixture.disk_admission,
            fixture.sandbox_id,
            &fixture.rootfs_path,
            PreparedRootfsDisposition::Abort,
        )
        .await
        .unwrap();

    assert!(!fixture.rootfs_path.exists());
    assert_eq!(fixture.disk_admission.pending_reservation_bytes(), 0);
    assert!(
        !fixture
            .reservations
            .active
            .contains_key(&fixture.sandbox_id)
    );
}

#[rstest]
#[tokio::test]
async fn abort_holds_directory_shaped_partial_rootfs_for_repair(
    #[future] prepared_rootfs: PreparedRootfsFixture,
) {
    let fixture = prepared_rootfs.await;
    fixture
        .reservations
        .reserve(
            &fixture.disk_admission,
            fixture.sandbox_id,
            &fixture.rootfs_path,
            1,
        )
        .unwrap();
    fs::create_dir(&fixture.rootfs_path).unwrap();
    fs::write(fixture.rootfs_path.join("partial"), b"partial").unwrap();

    fixture
        .reservations
        .complete(
            &fixture.disk_admission,
            fixture.sandbox_id,
            &fixture.rootfs_path,
            PreparedRootfsDisposition::Abort,
        )
        .await
        .unwrap_err();

    assert!(fixture.rootfs_path.exists());
    assert!(fixture.disk_admission.pending_reservation_bytes() > 0);
    assert_eq!(
        read_handoff_state(fixture.rootfs_path.parent().unwrap()).unwrap(),
        HandoffState::InProgress(1)
    );
}

#[rstest]
#[tokio::test]
async fn commit_rejects_a_rootfs_with_the_wrong_size_without_releasing_admission(
    #[future] prepared_rootfs: PreparedRootfsFixture,
) {
    let fixture = prepared_rootfs.await;
    fixture
        .reservations
        .reserve(
            &fixture.disk_admission,
            fixture.sandbox_id,
            &fixture.rootfs_path,
            1,
        )
        .unwrap();
    fs::write(&fixture.rootfs_path, b"short").unwrap();

    let error = fixture
        .reservations
        .complete(
            &fixture.disk_admission,
            fixture.sandbox_id,
            &fixture.rootfs_path,
            PreparedRootfsDisposition::Commit,
        )
        .await
        .unwrap_err();

    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(fixture.disk_admission.pending_reservation_bytes() > 0);
    assert!(matches!(
        read_handoff_state(fixture.rootfs_path.parent().unwrap()).unwrap(),
        HandoffState::InProgress(1)
    ));
}

#[rstest]
#[tokio::test]
async fn restart_retains_unknown_handoffs_until_repair_or_confirmed_cleanup(
    #[future] prepared_rootfs: PreparedRootfsFixture,
) {
    let fixture = prepared_rootfs.await;
    let cache = fixture._tempdir.path();
    let instance = fixture.rootfs_path.parent().unwrap();
    let disk = &fixture.disk_admission;
    let reserved_bytes = (2048 * MIB).max(disk.config().allocation_reservation_bytes);
    let assert_held = || {
        assert!(
            disk.reserve(DiskAllocationOperation::PrepareSandbox, Some(MIB))
                .is_err(),
            "a surviving assembler's unknown promise must block new allocations"
        );
    };
    for shape in ["corrupt", "zero", "over_policy", "conflict", "unreadable"] {
        let before_restart = PreparedRootfsReservations::default();
        before_restart
            .reserve(disk, fixture.sandbox_id, &fixture.rootfs_path, 2048)
            .unwrap();
        let healthy = SandboxId::new();
        let healthy_dir = cache.join(healthy.to_string());
        before_restart
            .reserve(
                disk,
                healthy,
                &healthy_dir.join("instance/rootfs.ext4"),
                2048,
            )
            .unwrap();
        let marker = instance.join(IN_PROGRESS_MARKER);
        match shape {
            "corrupt" => fs::write(&marker, "corrupt").unwrap(),
            "zero" => fs::write(&marker, "0").unwrap(),
            "over_policy" => fs::write(&marker, "131073").unwrap(),
            "conflict" => fs::write(instance.join(READY_MARKER), "2048").unwrap(),
            "unreadable" => {
                fs::remove_file(&marker).unwrap();
                fs::create_dir(&marker).unwrap();
            }
            _ => unreachable!(),
        }
        drop(before_restart);
        let recovered = PreparedRootfsReservations::recover(disk, cache);
        assert_held();
        assert_eq!(disk.pending_reservation_bytes(), reserved_bytes);
        drop(recovered);
        let recovered = PreparedRootfsReservations::recover(disk, cache);
        disk.refresh_after_external_allocation().await;
        assert_held();
        // Completing a different obligation cannot clear this hold.
        fs::remove_dir_all(&healthy_dir).unwrap();
        recovered
            .release_if_removed(disk, healthy, &healthy_dir)
            .await;
        assert_eq!(disk.pending_reservation_bytes(), 0);
        assert_held();
        recovered
            .release_if_removed(disk, fixture.sandbox_id, instance.parent().unwrap())
            .await;
        assert_held();
        if shape == "unreadable" {
            fs::remove_dir(&marker).unwrap();
        } else {
            fs::remove_file(&marker).unwrap();
        }
        if shape == "conflict" {
            fs::remove_file(instance.join(READY_MARKER)).unwrap();
        }
        // Marker deletion alone is not proof of writer completion.
        recovered.reclaim_stale(disk, cache, Timestamp::now()).await;
        assert_held();
        match shape {
            "corrupt" => {
                write_marker(instance, IN_PROGRESS_MARKER, 2048).unwrap();
                recovered.reclaim_stale(disk, cache, Timestamp::now()).await;
                assert_eq!(disk.pending_reservation_bytes(), reserved_bytes);
                disk.reserve(DiskAllocationOperation::PrepareSandbox, Some(MIB))
                    .unwrap();
            }
            "zero" => {
                write_marker(instance, READY_MARKER, 2048).unwrap();
                // A ready marker alone does not validate the completed artifact.
                recovered.reclaim_stale(disk, cache, Timestamp::now()).await;
                assert_held();
                fs::File::create(&fixture.rootfs_path)
                    .unwrap()
                    .set_len(2048 * MIB)
                    .unwrap();
                recovered.reclaim_stale(disk, cache, Timestamp::now()).await;
                disk.reserve(DiskAllocationOperation::PrepareSandbox, Some(MIB))
                    .unwrap();
            }
            _ => {}
        }
        fs::remove_dir_all(instance.parent().unwrap()).unwrap();
        if shape == "unreadable" {
            // The reaper covers a dropped cleanup waiter, without waiting seven hours.
            recovered.reclaim_stale(disk, cache, Timestamp::now()).await;
        } else {
            recovered
                .release_if_removed(disk, fixture.sandbox_id, instance.parent().unwrap())
                .await;
        }
        assert_eq!(disk.pending_reservation_bytes(), 0);
        disk.reserve(DiskAllocationOperation::PrepareSandbox, Some(MIB))
            .unwrap();
    }
}

#[rstest]
#[tokio::test]
async fn incomplete_startup_scan_holds_admission_until_the_reaper_recovers_it(
    #[future] prepared_rootfs: PreparedRootfsFixture,
) {
    let fixture = prepared_rootfs.await;
    let cache = fixture._tempdir.path().join("unreadable-cache");
    fs::write(&cache, "not a directory").unwrap();
    let disk = &fixture.disk_admission;
    let reserved_bytes = (2048 * MIB).max(disk.config().allocation_reservation_bytes);
    let recovered = PreparedRootfsReservations::recover(disk, &cache);
    assert!(
        disk.reserve(DiskAllocationOperation::PrepareSandbox, Some(MIB))
            .is_err()
    );
    fs::remove_file(&cache).unwrap();
    let instance = cache.join(fixture.sandbox_id.to_string()).join("instance");
    fs::create_dir_all(&instance).unwrap();
    write_marker(&instance, IN_PROGRESS_MARKER, 2048).unwrap();
    for _ in 0..2 {
        recovered
            .reclaim_stale(disk, &cache, Timestamp::now())
            .await;
        assert_eq!(disk.pending_reservation_bytes(), reserved_bytes);
        disk.reserve(DiskAllocationOperation::PrepareSandbox, Some(MIB))
            .unwrap();
    }
}

#[rstest]
#[tokio::test]
async fn startup_preserves_handoff_age_and_reclaims_it_after_timeout(
    #[future] prepared_rootfs: PreparedRootfsFixture,
) {
    let fixture = prepared_rootfs.await;
    fixture
        .reservations
        .reserve(
            &fixture.disk_admission,
            fixture.sandbox_id,
            &fixture.rootfs_path,
            1,
        )
        .unwrap();
    let started_at = Timestamp::from_second(1_700_000_000).unwrap();
    fs::File::options()
        .write(true)
        .open(
            fixture
                .rootfs_path
                .parent()
                .unwrap()
                .join(IN_PROGRESS_MARKER),
        )
        .unwrap()
        .set_times(fs::FileTimes::new().set_modified(std::time::SystemTime::from(started_at)))
        .unwrap();
    drop(fixture.reservations);
    assert_eq!(fixture.disk_admission.pending_reservation_bytes(), 0);

    let cache_root = fixture._tempdir.path();
    let recovered = PreparedRootfsReservations::recover(&fixture.disk_admission, cache_root);

    assert!(recovered.active.contains_key(&fixture.sandbox_id));
    assert!(fixture.disk_admission.pending_reservation_bytes() > 0);
    assert_eq!(
        recovered
            .active
            .get(&fixture.sandbox_id)
            .unwrap()
            .started_at,
        started_at
    );

    let reclaimed = recovered
        .reclaim_stale(
            &fixture.disk_admission,
            cache_root,
            started_at.checked_add(STALE_HANDOFF_AFTER).unwrap(),
        )
        .await;

    assert_eq!(reclaimed, 1);
    assert_eq!(fixture.disk_admission.pending_reservation_bytes(), 0);
    assert_eq!(
        read_handoff_state(fixture.rootfs_path.parent().unwrap()).unwrap(),
        HandoffState::Absent
    );
}

#[rstest]
#[tokio::test]
async fn cleanup_keeps_reservation_until_the_sandbox_cache_is_removed(
    #[future] prepared_rootfs: PreparedRootfsFixture,
) {
    let fixture = prepared_rootfs.await;
    fixture
        .reservations
        .reserve(
            &fixture.disk_admission,
            fixture.sandbox_id,
            &fixture.rootfs_path,
            1,
        )
        .unwrap();
    let sandbox_cache_dir = fixture.rootfs_path.parent().unwrap().parent().unwrap();

    fixture
        .reservations
        .release_if_removed(
            &fixture.disk_admission,
            fixture.sandbox_id,
            sandbox_cache_dir,
        )
        .await;

    assert!(fixture.disk_admission.pending_reservation_bytes() > 0);

    fs::remove_dir_all(sandbox_cache_dir).unwrap();
    fixture
        .reservations
        .release_if_removed(
            &fixture.disk_admission,
            fixture.sandbox_id,
            sandbox_cache_dir,
        )
        .await;

    assert_eq!(fixture.disk_admission.pending_reservation_bytes(), 0);
}

#[rstest]
#[tokio::test]
async fn cleanup_retains_reservation_when_cache_metadata_cannot_be_read(
    #[future] prepared_rootfs: PreparedRootfsFixture,
) {
    let fixture = prepared_rootfs.await;
    fixture
        .reservations
        .reserve(
            &fixture.disk_admission,
            fixture.sandbox_id,
            &fixture.rootfs_path,
            1,
        )
        .unwrap();
    let unstatable_path = fixture._tempdir.path().join("x".repeat(1024));
    unstatable_path.try_exists().unwrap_err();

    fixture
        .reservations
        .release_if_removed(
            &fixture.disk_admission,
            fixture.sandbox_id,
            &unstatable_path,
        )
        .await;

    assert!(
        fixture
            .reservations
            .active
            .contains_key(&fixture.sandbox_id)
    );
    assert!(fixture.disk_admission.pending_reservation_bytes() > 0);
}

#[rstest]
#[tokio::test(flavor = "current_thread")]
async fn cleanup_filesystem_work_does_not_block_the_runtime(
    #[future] prepared_rootfs: PreparedRootfsFixture,
) {
    let mut fixture = prepared_rootfs.await;
    fixture
        .reservations
        .reserve(
            &fixture.disk_admission,
            fixture.sandbox_id,
            &fixture.rootfs_path,
            1,
        )
        .unwrap();
    let sandbox_cache_dir = fixture.rootfs_path.parent().unwrap().parent().unwrap();
    fs::remove_dir_all(sandbox_cache_dir).unwrap();
    let pause = Arc::new(ReservePause::new());
    fixture.reservations.release_pause = Some(Arc::clone(&pause));
    let (progressed, progress) = mpsc::channel();
    let observer = observe_runtime_progress(pause, progress);

    let release = fixture.reservations.release_if_removed(
        &fixture.disk_admission,
        fixture.sandbox_id,
        sandbox_cache_dir,
    );
    let heartbeat = async {
        tokio::task::yield_now().await;
        let _ = progressed.send(());
    };
    let ((), ()) = tokio::join!(release, heartbeat);

    assert!(observer.join().unwrap());
    assert_eq!(fixture.disk_admission.pending_reservation_bytes(), 0);
}

#[rstest]
#[tokio::test(flavor = "current_thread")]
async fn stale_handoff_reclamation_does_not_block_the_runtime(
    #[future] prepared_rootfs: PreparedRootfsFixture,
) {
    let mut fixture = prepared_rootfs.await;
    fixture
        .reservations
        .reserve(
            &fixture.disk_admission,
            fixture.sandbox_id,
            &fixture.rootfs_path,
            1,
        )
        .unwrap();
    fs::write(&fixture.rootfs_path, b"partial").unwrap();
    let started_at = fixture
        .reservations
        .active
        .get(&fixture.sandbox_id)
        .unwrap()
        .started_at;
    let pause = Arc::new(ReservePause::new());
    fixture.reservations.reclaim_pause = Some(Arc::clone(&pause));
    let (progressed, progress) = mpsc::channel();
    let observer = observe_runtime_progress(pause, progress);

    let reclaim = fixture.reservations.reclaim_stale(
        &fixture.disk_admission,
        fixture._tempdir.path(),
        started_at.checked_add(STALE_HANDOFF_AFTER).unwrap(),
    );
    let heartbeat = async {
        tokio::task::yield_now().await;
        let _ = progressed.send(());
    };
    let (reclaimed, ()) = tokio::join!(reclaim, heartbeat);

    assert_eq!(reclaimed, 1);
    assert!(observer.join().unwrap());
}

#[rstest]
#[tokio::test]
async fn directory_shaped_handoff_is_held_for_repair_after_timeout(
    #[future] prepared_rootfs: PreparedRootfsFixture,
) {
    let fixture = prepared_rootfs.await;
    fixture
        .reservations
        .reserve(
            &fixture.disk_admission,
            fixture.sandbox_id,
            &fixture.rootfs_path,
            1,
        )
        .unwrap();
    fs::create_dir(&fixture.rootfs_path).unwrap();
    fs::write(fixture.rootfs_path.join("partial"), b"partial").unwrap();
    let started_at = fixture
        .reservations
        .active
        .get(&fixture.sandbox_id)
        .unwrap()
        .started_at;
    let after_timeout = started_at.checked_add(STALE_HANDOFF_AFTER).unwrap();

    let reclaimed = fixture
        .reservations
        .reclaim_stale(
            &fixture.disk_admission,
            fixture._tempdir.path(),
            started_at
                .checked_add(SignedDuration::from_hours(6))
                .unwrap(),
        )
        .await;

    assert_eq!(reclaimed, 0);
    assert!(fixture.rootfs_path.exists());
    assert!(fixture.disk_admission.pending_reservation_bytes() > 0);

    let reclaimed = fixture
        .reservations
        .reclaim_stale(
            &fixture.disk_admission,
            fixture._tempdir.path(),
            after_timeout,
        )
        .await;

    assert_eq!(reclaimed, 0);
    assert!(fixture.rootfs_path.exists());
    assert!(fixture.disk_admission.pending_reservation_bytes() > 0);
    assert_eq!(
        read_handoff_state(fixture.rootfs_path.parent().unwrap()).unwrap(),
        HandoffState::InProgress(1)
    );
}

#[rstest]
#[tokio::test]
async fn stale_handoff_without_a_marker_removes_the_uncommitted_rootfs(
    #[future] prepared_rootfs: PreparedRootfsFixture,
) {
    let fixture = prepared_rootfs.await;
    fixture
        .reservations
        .reserve(
            &fixture.disk_admission,
            fixture.sandbox_id,
            &fixture.rootfs_path,
            1,
        )
        .unwrap();
    fs::write(&fixture.rootfs_path, b"partial").unwrap();
    fs::remove_file(
        fixture
            .rootfs_path
            .parent()
            .unwrap()
            .join(IN_PROGRESS_MARKER),
    )
    .unwrap();
    let started_at = fixture
        .reservations
        .active
        .get(&fixture.sandbox_id)
        .unwrap()
        .started_at;

    let reclaimed = fixture
        .reservations
        .reclaim_stale(
            &fixture.disk_admission,
            fixture._tempdir.path(),
            started_at.checked_add(STALE_HANDOFF_AFTER).unwrap(),
        )
        .await;

    assert_eq!(reclaimed, 1);
    assert!(!fixture.rootfs_path.exists());
    assert_eq!(fixture.disk_admission.pending_reservation_bytes(), 0);
}

#[cfg(target_os = "linux")]
#[test]
#[ignore = "requires root and an explicitly supplied disposable btrfs fixture directory"]
fn prepared_instance_and_clone_use_real_btrfs_subvolumes() {
    let Some(root) = std::env::var_os("SPACE_NODEMGR_TEST_BTRFS_DIR") else {
        eprintln!("not opted in: set SPACE_NODEMGR_TEST_BTRFS_DIR to a disposable btrfs directory");
        return;
    };
    assert!(nix::unistd::geteuid().is_root());
    let fixture = tempfile::tempdir_in(root).unwrap();
    let cache = fixture.path().canonicalize().unwrap();
    let donor = cache.join("donor/instance");
    let instance = files::directory(&donor, true).unwrap();
    assert!(crate::btrfs::is_subvolume(&instance).unwrap());
    fs::write(donor.join("rootfs.ext4"), b"original rootfs").unwrap();
    fs::create_dir(cache.join("donor/snapshot")).unwrap();
    for name in ["vmstate", "memory"] {
        fs::write(cache.join("donor/snapshot").join(name), name).unwrap();
    }
    fs::create_dir(cache.join("target")).unwrap();
    fs::create_dir(cache.join("run")).unwrap();
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    fs::set_permissions(cache.join("target"), fs::Permissions::from_mode(0o700)).unwrap();
    let metadata = fs::metadata(cache.join("target")).unwrap();
    let identity = crate::proto::CloneDirectoryIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
    };
    let tree = crate::clone_tree::CloneTree::open(
        &cache,
        &cache.join("run"),
        "donor",
        "target",
        &identity,
    )
    .unwrap();
    let gid = nix::unistd::getegid().as_raw();
    tree.copy(gid, gid).unwrap();
    let clone = files::directory(&cache.join("target/instance"), false).unwrap();
    assert!(crate::btrfs::is_subvolume(&clone).unwrap());
    assert_eq!(
        crate::btrfs::info(&clone).unwrap().1.parent_uuid,
        crate::btrfs::info(&instance).unwrap().1.uuid,
    );
    fs::write(cache.join("target/instance/rootfs.ext4"), b"clone changes").unwrap();
    assert_eq!(
        fs::read(donor.join("rootfs.ext4")).unwrap(),
        b"original rootfs"
    );
    for name in ["vmstate", "memory"] {
        assert_eq!(
            fs::read(cache.join("target/snapshot").join(name)).unwrap(),
            name.as_bytes()
        );
    }
    // An ordinary directory cannot be snapshotted as its enclosing subvolume.
    let ordinary = fs::File::open(cache.join("donor/snapshot")).unwrap();
    let parent = fs::File::open(&cache).unwrap();
    let error = crate::btrfs::snapshot(&ordinary, &parent, "invalid".as_ref(), false).unwrap_err();
    assert_eq!(error.raw_os_error(), Some(libc::EINVAL));
    assert!(!cache.join("invalid").exists());
    super::super::artifact_fs::remove(&cache.join("target")).unwrap();
    super::super::artifact_fs::remove(&cache.join("donor")).unwrap();
}
