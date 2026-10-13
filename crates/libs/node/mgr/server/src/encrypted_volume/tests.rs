use super::*;
use uuid::Uuid;

fn sid() -> SandboxId {
    SandboxId::from_uuid(Uuid::parse_str("019d84bd-280d-7331-99c6-b63377f3a3a2").unwrap())
}

#[test]
fn mapper_name_is_derived_and_distinct_from_rootfs() {
    let s = sid();
    assert_eq!(
        mapper_name(&s, "snapshot"),
        "sbxvol-019d84bd-280d-7331-99c6-b63377f3a3a2-snapshot"
    );
    // Distinct from the rootfs mapper (sbxroot-<id>) and within the dm
    // 127-char name limit even with a volume name appended.
    assert!(mapper_name(&s, "snapshot").starts_with("sbxvol-"));
    assert!(mapper_name(&s, "snapshot").len() < 128);
}

#[tokio::test]
async fn fresh_and_restore_source_are_mutually_exclusive() {
    // A fresh capture recreates its backing; pairing it with a restore
    // source would silently discard the downloaded snapshot. Rejected
    // before any device work.
    let err = prepare(PrepareParams {
        sandbox_id: sid(),
        volume_name: "snapshot".to_string(),
        backing_path: PathBuf::from("/nonexistent/instance/snapshot.luks"),
        restore_source: Some(PathBuf::from("/nonexistent/snapshot/snapshot.luks")),
        mount_dir: PathBuf::from("/nonexistent/snapshot-vol"),
        size_bytes: 1,
        dek: b"dek",
        fresh: true,
    })
    .await
    .expect_err("fresh + restore_source must be rejected");
    assert!(matches!(err, DmCryptError::InvalidParams(_)), "{err:?}");
}

#[test]
fn backing_size_floors_at_btrfs_minimum() {
    // A tiny request is floored to the btrfs minimum.
    assert_eq!(volume_backing_size_bytes(1), MIN_VOLUME_BYTES);
    // A large request gets headroom on top.
    let big = 4 * 1024 * 1024 * 1024;
    assert_eq!(volume_backing_size_bytes(big), big + VOLUME_HEADROOM_BYTES);
}

/// Fix 1: the unwind enumerates the device's mountpoints from mountinfo —
/// canonical dir AND jail bind, nothing hardcoded — latest-mounted first, and
/// matches by device identity. btrfs reports an anonymous `0:N` filesystem
/// device in mountinfo, so `/dev/dm-N` must be matched to dmsetup's minor.
#[test]
fn unwind_enumerates_device_mounts_latest_first_by_identity() {
    let mapper = "sbxvol-01a0a702-518c-7211-a938-72bfa87751b0-snapshot";
    let dev = format!("/dev/mapper/{mapper}");
    let mountinfo = "\
        801 25 0:851 / /space/cache/sandboxes/01a0a702/snapshot-vol rw - btrfs /dev/dm-5 rw,compress-force=zstd\n\
        802 25 259:1 / /space/cache rw - btrfs /dev/nvme0n1p1 rw\n\
        803 25 0:851 / /space/cache/firecracker/jails/firecracker/01a0a702/root/encsnap rw - btrfs /dev/dm-5 rw,compress-force=zstd\n";
    let expected = vec![
        "/space/cache/firecracker/jails/firecracker/01a0a702/root/encsnap".to_string(),
        "/space/cache/sandboxes/01a0a702/snapshot-vol".to_string(),
    ];
    // btrfs may report an anonymous superblock device rather than the block
    // mapper's major:minor, including on hosts without /dev/dm-N nodes.
    for info in [mountinfo.to_owned(), mountinfo.replace("0:851", "253:5")] {
        assert_eq!(
            mountpoints_for_mapper_identity(&info, mapper, MapperIdentity::Present((253, 5)))
                .unwrap(),
            expected
        );
    }
    // dm-N spelling defeats the path fallback alone...
    assert!(mountpoints_of_device(mountinfo, &dev, None).is_empty());
    // ...which still works when the source was recorded as the mapper path.
    let by_path = mountinfo.replace("/dev/dm-5", &dev);
    assert_eq!(
        mountpoints_for_mapper_identity(&by_path, mapper, MapperIdentity::Absent).unwrap(),
        expected
    );
    // An unresolved identity is unknown, not proof that there are no mapper
    // mounts: teardown must stop before close/force-free.
    assert!(matches!(
        mountpoints_for_mapper_identity(
            mountinfo,
            mapper,
            MapperIdentity::Error(DmCryptError::Command {
                cmd: "dmsetup info".into(),
                stderr: "timed out".into(),
            })
        ),
        Err(DmCryptError::Command { .. })
    ));
}

/// Fix 2: a mapping is debris when its loop's backing file is deleted or is
/// no longer the current backing inode; a healthy mapping is left alone.
#[test]
fn mapper_probe_classifies_absent_vs_error() {
    // "does not exist" is a POSITIVE absence (name freed / renamed away);
    // anything else is an unresolved probe that must never destroy.
    assert!(matches!(
        classify_mapper_probe(Ok((253, 5))),
        MapperIdentity::Present((253, 5))
    ));
    assert!(matches!(
        classify_mapper_probe(Err(DmCryptError::Command {
            cmd: "dmsetup info".into(),
            stderr: "Device sbxvol-x-snapshot does not exist.".into(),
        })),
        MapperIdentity::Absent
    ));
    assert!(matches!(
        classify_mapper_probe(Err(DmCryptError::Command {
            cmd: "dmsetup info".into(),
            stderr: "dmsetup: connection timed out".into(),
        })),
        MapperIdentity::Error(_)
    ));
}

/// The idempotent-arm decision. Accept requires POSITIVE proof (present mapper
/// AND the mount sits on it); a foreign or gone-name mount rebuilds; a vanished
/// mount device (TOCTOU) or a probe error is Retry — never accept an
/// unverified mount (Firecracker would write plaintext into an unmounted dir),
/// never destroy on an unproven probe.
#[test]
fn idempotent_arm_accepts_only_a_positively_verified_mount() {
    // Present + matching + mounted → accept.
    assert_eq!(
        idempotent_arm(&MapperIdentity::Present((253, 5)), Some((253, 5))),
        IdempotentArm::Accept
    );
    // Present + positively foreign → rebuild.
    assert_eq!(
        idempotent_arm(&MapperIdentity::Present((253, 5)), Some((253, 9))),
        IdempotentArm::Rebuild
    );
    // An anonymous filesystem ID is not a different block device. Without
    // backing-device evidence, retry instead of destroying the mounted volume.
    assert_eq!(
        idempotent_arm(&MapperIdentity::Present((253, 5)), Some((0, 42))),
        IdempotentArm::Retry
    );
    // Present but the mount's device vanished/unresolved (TOCTOU): retry, do
    // NOT accept an unverified mount.
    assert_eq!(
        idempotent_arm(&MapperIdentity::Present((253, 5)), None),
        IdempotentArm::Retry
    );
    // Absent mapper name (force-freed away) plus a positively present mount:
    // the surviving mount is stale and must be detached before rebuilding.
    let present_mount = Some((253, 5));
    assert_eq!(
        idempotent_arm(&MapperIdentity::Absent, present_mount),
        IdempotentArm::Rebuild
    );
    assert_eq!(
        idempotent_arm(&MapperIdentity::Absent, None),
        IdempotentArm::Rebuild
    );
    // Probe error: unknown → retry, never destroy.
    assert_eq!(
        idempotent_arm(
            &MapperIdentity::Error(DmCryptError::Command {
                cmd: "dmsetup info".into(),
                stderr: "timed out".into(),
            }),
            Some((253, 9))
        ),
        IdempotentArm::Retry
    );
}

#[test]
fn mount_device_numbers_reads_the_mountinfo_majmin() {
    let info = "\
        801 25 253:5 / /space/cache/sandboxes/01a0a702/snapshot-vol rw - btrfs /dev/dm-5 rw\n\
        802 25 259:1 / /space/cache rw - btrfs /dev/nvme0n1p1 rw\n";
    assert_eq!(
        mount_device_numbers(info, "/space/cache/sandboxes/01a0a702/snapshot-vol"),
        Some((253, 5))
    );
    assert_eq!(
        mount_device_numbers(info, "/space/cache/sandboxes/other"),
        None
    );
    assert!(mountpoint_is_present(
        info,
        Path::new("/space/cache/sandboxes/01a0a702/snapshot-vol")
    ));
    assert!(!mountpoint_is_present(
        info,
        Path::new("/space/cache/sandboxes/other")
    ));
}

#[test]
fn mount_identity_judges_the_visible_topmost_mount() {
    let info = "\
        801 25 253:5 / /space/cache/sandboxes/01a0a702/snapshot-vol rw - btrfs /dev/dm-5 rw\n\
        802 25 259:1 / /space/cache/sandboxes/01a0a702/snapshot-vol rw - ext4 /dev/nvme0n1p1 rw\n";
    assert_eq!(
        mount_device_numbers(info, "/space/cache/sandboxes/01a0a702/snapshot-vol"),
        Some((259, 1)),
        "the last mountinfo entry is the visible stacked mount"
    );
}

#[tokio::test(start_paused = true)]
async fn release_waits_for_the_detached_filesystem_within_a_bound() {
    use std::{cell::Cell, time::Duration};
    let poll = Duration::from_millis(100);
    let budget = Duration::from_secs(10);
    // (probe answers, then repeated forever; observed idle; virtual time spent)
    let cases: [(&[u64], u64, bool, Duration); 3] = [
        (&[], 0, true, Duration::ZERO),
        (&[1, 1, 1], 0, true, 3 * poll),
        (&[], 1, false, budget),
    ];
    for (answers, then, idle, spent) in cases {
        let calls = Cell::new(0);
        let started = tokio::time::Instant::now();
        let observed = wait_until_unopened(
            "sbxvol-test",
            || {
                let call = calls.replace(calls.get() + 1);
                let open = answers.get(call).copied().unwrap_or(then);
                async move { Ok::<_, DmCryptError>(open) }
            },
            budget,
            poll,
        )
        .await;
        assert_eq!(
            (observed, started.elapsed()),
            (idle, spent),
            "{answers:?} then {then}"
        );
    }
}
