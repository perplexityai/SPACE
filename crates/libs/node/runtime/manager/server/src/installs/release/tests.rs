use super::super::test_support::{Member, executable, tgz};
use super::*;
fn place(root: &Path, version: &str, fill: u8) {
    let bin = root.join(format!("v{version}/bin"));
    std::fs::create_dir_all(&bin).unwrap();
    let architecture = crate::installs::host_arch().unwrap();
    std::fs::write(bin.join("firecracker"), executable(architecture, fill)).unwrap();
    std::fs::write(
        bin.join("jailer"),
        executable(architecture, fill.wrapping_add(1)),
    )
    .unwrap();
}

#[test]
fn a_release_digest_covers_each_binary_by_name() {
    let dir = tempfile::tempdir().unwrap();
    place(dir.path(), "1.16.2", 1);
    let release = dir.path().join("v1.16.2");
    let first = release_digest(RuntimeKind::Firecracker, &release, 4096).unwrap();
    let architecture = crate::installs::host_arch().unwrap();
    let text = format!(
        "firecracker {}\njailer {}\n",
        Digest::sha256(Sha256::digest(executable(architecture, 1)).into()).hex_string(),
        Digest::sha256(Sha256::digest(executable(architecture, 2)).into()).hex_string()
    );
    assert_eq!(
        first,
        Digest::sha256(Sha256::digest(text.as_bytes()).into())
    );

    std::fs::write(release.join("bin/jailer"), executable(architecture, 9)).unwrap();
    assert_ne!(
        release_digest(RuntimeKind::Firecracker, &release, 4096).unwrap(),
        first
    );
    std::fs::remove_file(release.join("bin/jailer")).unwrap();
    std::os::unix::fs::symlink("firecracker", release.join("bin/jailer")).unwrap();
    assert!(matches!(
        release_digest(RuntimeKind::Firecracker, &release, 4096),
        Err(ReleaseDigestError::NotRegularFile { .. })
    ));
}

/// The unpack stops once its binaries are written, and a crafted archive
/// cannot keep it decompressing: entries and bytes before the binaries are
/// bounded, and cancellation stops it.
#[test]
fn an_unpack_is_bounded_and_stops_at_its_binaries() {
    let architecture = crate::installs::host_arch().unwrap();
    let binaries = [
        ("bin/firecracker", Member::File(executable(architecture, 1))),
        ("bin/jailer", Member::File(executable(architecture, 2))),
    ];
    let members: Vec<(&str, String)> = binaries
        .iter()
        .map(|(name, _)| (*name, name.to_string()))
        .collect();
    let limits = UnpackLimits {
        max_member_bytes: 4096,
        max_total_bytes: 16 * 1024,
        max_entries: 4,
    };
    let fillers = |count: usize, bytes: usize| -> Vec<(String, Member)> {
        (0..count)
            .map(|index| (format!("extra-{index}"), Member::File(vec![0; bytes])))
            .collect()
    };
    let run = |before: Vec<(String, Member)>,
               after: Vec<(String, Member)>,
               cancellation: &CancellationToken| {
        let named = |(name, member): &(String, Member)| (name.clone(), member.clone());
        let entries: Vec<(String, Member)> = before
            .iter()
            .map(named)
            .chain(
                binaries
                    .iter()
                    .map(|(name, member)| (name.to_string(), member.clone())),
            )
            .chain(after.iter().map(named))
            .collect();
        let members_ref: Vec<(&str, Member)> = entries
            .iter()
            .map(|(name, member)| (name.as_str(), member.clone()))
            .collect();
        let dir = tempfile::tempdir().unwrap();
        let archive = dir.path().join("release.tgz");
        std::fs::write(&archive, tgz(&members_ref)).unwrap();
        let release = dir.path().join("release");
        std::fs::create_dir(&release).unwrap();
        unpack(&archive, &release, &members, limits, cancellation)
    };
    let live = CancellationToken::new();

    // Whatever follows the binaries is never read.
    run(Vec::new(), fillers(16, 64 * 1024), &live).unwrap();

    let error = run(fillers(4, 1), Vec::new(), &live).unwrap_err();
    assert!(
        matches!(error, UnpackError::TooManyEntries { limit: 4 }),
        "{error}"
    );

    let error = run(fillers(1, 64 * 1024), Vec::new(), &live).unwrap_err();
    assert!(
        matches!(error, UnpackError::TooMuchData { limit } if limit == 16 * 1024),
        "{error}"
    );

    // A named binary must be a regular archive member, never a link.
    let dir = tempfile::tempdir().unwrap();
    let archive = dir.path().join("linked.tgz");
    std::fs::write(
        &archive,
        tgz(&[("bin/firecracker", Member::Link("/outside"))]),
    )
    .unwrap();
    let destination = dir.path().join("out");
    std::fs::create_dir(&destination).unwrap();
    assert!(matches!(
        unpack(&archive, &destination, &members, limits, &live),
        Err(UnpackError::NotRegularFile { .. })
    ));

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let error = run(Vec::new(), Vec::new(), &cancelled).unwrap_err();
    assert!(matches!(error, UnpackError::Cancelled), "{error}");
}

#[test]
fn unpack_retries_local_io_but_rejects_malformed_content() {
    for (kind, retriable) in [
        (std::io::ErrorKind::Other, false),
        (std::io::ErrorKind::InvalidData, false),
        (std::io::ErrorKind::UnexpectedEof, false),
    ] {
        let error = DownloadError::Unpack(UnpackError::Read(kind.into())).into_job_error();
        assert_eq!(
            matches!(error, JobError::Retriable(_)),
            retriable,
            "{error:?}"
        );
    }
    for error in [
        std::io::Error::from_raw_os_error(libc::EIO),
        std::io::Error::other(std::io::Error::from_raw_os_error(libc::EIO)),
    ] {
        assert!(matches!(
            DownloadError::Unpack(UnpackError::Read(error)).into_job_error(),
            JobError::Retriable(_)
        ));
    }
    let error = DownloadError::Unpack(UnpackError::Staging(StagingError::new(
        "write",
        Path::new("synthetic/binary"),
        std::io::ErrorKind::StorageFull.into(),
    )))
    .into_job_error();
    assert!(matches!(error, JobError::Retriable(_)));
}
