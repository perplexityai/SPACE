use super::super::test_support::{FakePayloadSource, Member, executable, tgz};
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
    for compression in [ArchiveCompression::Gzip, ArchiveCompression::Xz] {
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
            std::fs::write(&archive, compressed(compression, &members_ref)).unwrap();
            let release = dir.path().join("release");
            std::fs::create_dir(&release).unwrap();
            unpack(
                &archive,
                &release,
                compression,
                &members,
                limits,
                cancellation,
            )
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
            compressed(
                compression,
                &[("bin/firecracker", Member::Link("/outside"))],
            ),
        )
        .unwrap();
        let destination = dir.path().join("out");
        std::fs::create_dir(&destination).unwrap();
        assert!(matches!(
            unpack(&archive, &destination, compression, &members, limits, &live),
            Err(UnpackError::NotRegularFile { .. })
        ));

        let cancelled = CancellationToken::new();
        cancelled.cancel();
        let error = run(Vec::new(), Vec::new(), &cancelled).unwrap_err();
        assert!(matches!(error, UnpackError::Cancelled), "{error}");
    }
}

fn compressed(compression: ArchiveCompression, entries: &[(&str, Member)]) -> Vec<u8> {
    let gzip = tgz(entries);
    match compression {
        ArchiveCompression::Gzip => gzip,
        ArchiveCompression::Xz => {
            let mut tar = Vec::new();
            flate2::read::GzDecoder::new(gzip.as_slice())
                .read_to_end(&mut tar)
                .unwrap();
            let mut encoder = xz2::write::XzEncoder::new(Vec::new(), 1);
            encoder.write_all(&tar).unwrap();
            encoder.finish().unwrap()
        }
    }
}

#[test]
fn xz_dictionary_memory_is_bounded_before_output() {
    // An empty tar encoded with `xz -0 --lzma2=dict=128MiB`. It expands to
    // only 10 KiB, so neither the output-byte nor entry bounds can reject it.
    let payload = [
        0xfd, 0x37, 0x7a, 0x58, 0x5a, 0x00, 0x00, 0x04, 0xe6, 0xd6, 0xb4, 0x46, 0x04, 0xc0, 0x34,
        0x80, 0x50, 0x21, 0x01, 0x1e, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x6a, 0x9a,
        0x4f, 0x5e, 0xe0, 0x27, 0xff, 0x00, 0x2c, 0x5d, 0x00, 0x00, 0x6f, 0xfd, 0xff, 0xff, 0xa3,
        0xb7, 0xff, 0x47, 0x3e, 0x48, 0x15, 0x72, 0x39, 0x61, 0x51, 0xb8, 0x92, 0x28, 0xe6, 0xa3,
        0x86, 0x07, 0xf9, 0xee, 0xe4, 0x1e, 0x82, 0xd3, 0x2f, 0xc5, 0x3a, 0x3c, 0x01, 0x4b, 0xb1,
        0x7e, 0xc9, 0x8a, 0x5c, 0x32, 0x1b, 0x64, 0x00, 0x00, 0xaf, 0x18, 0x12, 0xb3, 0x2f, 0x55,
        0xcc, 0x33, 0x00, 0x01, 0x50, 0x80, 0x50, 0x00, 0x00, 0x00, 0x1a, 0x63, 0x30, 0xb5, 0xb1,
        0xc4, 0x67, 0xfb, 0x02, 0x00, 0x00, 0x00, 0x00, 0x04, 0x59, 0x5a,
    ];
    let root = tempfile::tempdir().unwrap();
    let archive = root.path().join("dictionary.tar.xz");
    std::fs::write(&archive, payload).unwrap();
    let result = unpack(
        &archive,
        root.path(),
        ArchiveCompression::Xz,
        &[("bin/wasmtime", "missing".into())],
        UnpackLimits {
            max_member_bytes: 4096,
            max_total_bytes: 16 * 1024,
            max_entries: 4,
        },
        &CancellationToken::new(),
    );
    assert!(matches!(result, Err(UnpackError::Read(_))), "{result:?}");
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

/// Native engines, whether standalone or archived, must pass both download
/// and installed-file pins and the host architecture check before publication.
#[tokio::test]
async fn native_payloads_use_the_same_verified_release_pipeline() {
    use super::super::executor::ExecutorParts;
    use super::super::test_support::{FakeSource, LocalPublisher, TestPins};
    use pplx_space_node_runtime_core::catalog::PinnedFile;
    use std::sync::Mutex;

    #[derive(Clone, Copy, Debug)]
    enum Case {
        Valid,
        CorruptTransfer,
        WrongBinaryPin,
        WrongArchitecture,
        TooLarge,
        Cancelled,
        InvalidLayout,
    }
    let architecture = crate::installs::host_arch().unwrap();
    let other = match architecture {
        Arch::X86_64 => Arch::Aarch64,
        Arch::Aarch64 => Arch::X86_64,
        Arch::Wasm32 => panic!("native host fixture cannot be WebAssembly"),
    };
    let checksum = |bytes: &[u8]| Digest::sha256(Sha256::digest(bytes).into());
    for (runtime, version, binary, compression) in [
        (
            RuntimeKind::CloudHypervisor,
            "54.0",
            "bin/cloud-hypervisor",
            None,
        ),
        (
            RuntimeKind::Wasmtime,
            "50.0.2",
            "bin/wasmtime",
            Some(ArchiveCompression::Xz),
        ),
    ] {
        for case in [
            Case::Valid,
            Case::CorruptTransfer,
            Case::WrongBinaryPin,
            Case::WrongArchitecture,
            Case::TooLarge,
            Case::Cancelled,
            Case::InvalidLayout,
        ] {
            let root = tempfile::tempdir().unwrap();
            let bytes = executable(
                if matches!(case, Case::WrongArchitecture) {
                    other
                } else {
                    architecture
                },
                7,
            );
            let (download, payload) = match compression {
                Some(compression) => (
                    compressed(
                        compression,
                        &[("upstream/wasmtime", Member::File(bytes.clone()))],
                    ),
                    ReleasePayload::TarXz {
                        members: vec![super::super::release_source::ReleaseArchiveMember {
                            binary: binary.into(),
                            member: "upstream/wasmtime".into(),
                        }],
                    },
                ),
                None => (bytes.clone(), ReleasePayload::Executable),
            };
            let received = download.len() as u64;
            let pin = PinnedRelease {
                release: RuntimeIdentity {
                    kind: if matches!(case, Case::InvalidLayout) {
                        RuntimeKind::Firecracker
                    } else {
                        runtime
                    },
                    version: version.parse().unwrap(),
                },
                architecture,
                archive_sha256: if matches!(case, Case::CorruptTransfer) {
                    checksum(b"different download")
                } else {
                    checksum(&download)
                },
                binaries: vec![PinnedFile {
                    path: binary,
                    sha256: if matches!(case, Case::WrongBinaryPin) {
                        checksum(b"different executable")
                    } else {
                        checksum(&bytes)
                    },
                }],
            };
            let source = Arc::new(FakePayloadSource {
                runtime,
                payload,
                location: "https://releases.example.test/runtime-static"
                    .parse()
                    .unwrap(),
                bytes: download,
            });
            let release = VerifiableRelease {
                release: pin.release.clone(),
                architecture,
                archive: source.location.clone(),
                sha256: pin.archive_sha256,
            };
            let kernels = Arc::new(FakeSource::default());
            let pins = TestPins::new(kernels.clone());
            pins.pin_release(pin.clone());
            let context = ReleaseContext {
                root: root.path().join("releases"),
                source,
            };
            let staging = root.path().join("staging");
            let publisher = LocalPublisher::new(&context.root, root.path(), &staging, pins.clone());
            let executor = InstallExecutor::new(
                ExecutorParts {
                    boot_source: None,
                    kernels_root: root.path().to_path_buf(),
                    staging: staging.clone(),
                    limits: super::super::operations::InstallLimits {
                        max_bytes: if matches!(case, Case::TooLarge) {
                            1
                        } else {
                            4096
                        },
                        ..crate::DEFAULT_INSTALL_LIMITS
                    },
                    host_arch: architecture,
                    source: crate::installs::test_support::kernel_sources(kernels),
                    releases: ReleaseSources::new(vec![context.clone()]).unwrap(),
                    publisher,
                    pins,
                },
                Arc::new(Mutex::new(rusqlite::Connection::open_in_memory().unwrap())),
            );
            let cancellation = CancellationToken::new();
            if matches!(case, Case::Cancelled) {
                cancellation.cancel();
            }
            let result = executor
                .prepare_release(&context, &release, pin.digest(), &staging, &cancellation)
                .await;
            match case {
                Case::Valid => {
                    assert_eq!(result.unwrap(), (pin.digest(), received));
                    let path = staging
                        .join(
                            pplx_space_node_runtime_core::artifact_store::release_directory(
                                &release.release,
                            ),
                        )
                        .join(binary);
                    assert_eq!(std::fs::read(&path).unwrap(), bytes);
                    // Standalone bytes move directly into the release; archive
                    // payload cleanup belongs to the publication operation.
                    if compression.is_none() {
                        assert!(!staging.join(PAYLOAD_FILE).exists());
                    }
                    use std::os::unix::fs::PermissionsExt as _;
                    assert_ne!(
                        std::fs::metadata(path).unwrap().permissions().mode() & 0o100,
                        0
                    );
                }
                Case::CorruptTransfer => assert!(matches!(
                    result,
                    Err(DownloadError::ChecksumMismatch { .. })
                )),
                Case::WrongBinaryPin => {
                    assert!(matches!(result, Err(DownloadError::BinariesDiffer)))
                }
                Case::WrongArchitecture => {
                    assert!(matches!(result, Err(DownloadError::NotExecutable { .. })))
                }
                Case::TooLarge => assert!(matches!(result, Err(DownloadError::Transfer(_)))),
                Case::Cancelled => assert!(matches!(result, Err(DownloadError::Cancelled))),
                Case::InvalidLayout => {
                    assert!(matches!(result, Err(DownloadError::PayloadLayout)));
                    assert!(
                        !staging.exists(),
                        "invalid layout must fail before download"
                    );
                }
            }
        }
    }
}

#[test]
fn source_packaging_cannot_choose_install_paths_or_omit_binaries() {
    use super::super::release_source::ReleaseArchiveMember;
    let layout = ReleaseLayout::of(RuntimeKind::Firecracker).unwrap();
    for binaries in [
        vec!["bin/firecracker"],
        vec!["bin/firecracker", "bin/jailer"],
        vec!["bin/firecracker", "bin/firecracker"],
        vec!["bin/firecracker", "../../outside"],
        vec!["bin/firecracker", "bin/jailer", "extra"],
    ] {
        let members: Vec<_> = binaries
            .into_iter()
            .map(|binary| ReleaseArchiveMember {
                binary: binary.into(),
                member: "source-member".into(),
            })
            .collect();
        for payload in [
            ReleasePayload::TarGz {
                members: members.clone(),
            },
            ReleasePayload::TarXz { members },
        ] {
            assert!(matches!(
                Materialization::for_layout(payload, layout),
                Err(DownloadError::PayloadLayout)
            ));
        }
    }
}

#[test]
fn release_sources_are_unambiguous_and_do_not_fall_back_across_runtimes() {
    use super::super::test_support::FakeReleaseSource;
    let directory = tempfile::tempdir().unwrap();
    let fc = ReleaseContext {
        root: directory.path().join("fc"),
        source: Arc::new(FakeReleaseSource::default()),
    };
    let ch = ReleaseContext {
        root: directory.path().join("ch"),
        source: Arc::new(FakePayloadSource {
            runtime: RuntimeKind::CloudHypervisor,
            payload: ReleasePayload::Executable,
            location: "https://releases.example.test/ch".parse().unwrap(),
            bytes: Vec::new(),
        }),
    };
    let sources = ReleaseSources::new(vec![fc.clone(), ch.clone()]).unwrap();
    assert_eq!(sources.get(RuntimeKind::Firecracker).unwrap().root, fc.root);
    assert_eq!(
        sources.get(RuntimeKind::CloudHypervisor).unwrap().root,
        ch.root
    );
    assert!(sources.get(RuntimeKind::Gvisor).is_none());
    assert!(matches!(
        ReleaseSources::new(vec![fc.clone(), fc.clone()]),
        Err(ReleaseConfigurationError::DuplicateRuntime { .. })
    ));
    for root in [
        fc.root.clone(),
        fc.root.join("nested"),
        directory.path().to_path_buf(),
    ] {
        assert!(matches!(
            ReleaseSources::new(vec![fc.clone(), ReleaseContext { root, ..ch.clone() }]),
            Err(ReleaseConfigurationError::OverlappingStores { .. })
        ));
    }
    let alias = directory.path().join("alias");
    std::fs::create_dir_all(&fc.root).unwrap();
    std::os::unix::fs::symlink(&fc.root, &alias).unwrap();
    assert!(matches!(
        ReleaseSources::new(vec![
            fc.clone(),
            ReleaseContext {
                root: alias.join("not-created"),
                ..ch.clone()
            }
        ]),
        Err(ReleaseConfigurationError::OverlappingStores { .. })
    ));
    for root in [
        PathBuf::from("relative"),
        directory.path().join("../escape"),
    ] {
        assert!(matches!(
            ReleaseSources::new(vec![ReleaseContext { root, ..ch.clone() }]),
            Err(ReleaseConfigurationError::InvalidStore { .. })
        ));
    }
}
