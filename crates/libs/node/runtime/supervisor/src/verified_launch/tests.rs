use super::*;
use pplx_space_common_core::types::Arch;

#[test]
fn adapter_locations_are_not_environment_authority() {
    let environments = CompiledEnvironment::for_architecture(Arch::X86_64).unwrap();
    let environment = &environments[0];
    let release = environment.release();
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join(managed_release_directory(
        &release.release,
        release.digest(),
    ));
    let firecracker = root.join("bin/firecracker");
    let jailer = root.join("bin/jailer");
    // A configured location is accepted without a shared pointer or host I/O.
    // Missing files still fail authentication before execution.
    let launch = VerifiedLaunch::from_environment(environment, &firecracker, &jailer).unwrap();
    assert!(launch.open().is_err());
    for (runtime, launcher) in [
        (PathBuf::from("relative/bin/firecracker"), jailer.clone()),
        (root.join("../bin/firecracker"), jailer.clone()),
        (
            firecracker.clone(),
            temporary.path().join("other/bin/jailer"),
        ),
        (
            temporary.path().join("wrong-release/bin/firecracker"),
            jailer.clone(),
        ),
        (firecracker.clone(), PathBuf::new()),
    ] {
        assert_eq!(
            VerifiedLaunch::from_environment(environment, &runtime, &launcher)
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
    }
}

#[test]
fn source_io_failures_remain_retryable_through_launch_admission() {
    use pplx_space_node_runtime_core::launch_admission::{
        admission_refusal, supervisor_launch_status,
    };
    enum Operation {
        Directory,
        Verity,
        Metadata,
    }
    for operation in [Operation::Directory, Operation::Verity, Operation::Metadata] {
        for (errno, expected) in [
            (libc::EIO, tonic::Code::Internal),
            (libc::EMFILE, tonic::Code::Internal),
            (libc::EINTR, tonic::Code::Internal),
            (libc::EBUSY, tonic::Code::Internal),
            (libc::ETXTBSY, tonic::Code::Internal),
            (libc::ENOENT, tonic::Code::FailedPrecondition),
            (libc::EACCES, tonic::Code::FailedPrecondition),
        ] {
            let source = std::io::Error::from_raw_os_error(errno);
            let path = PathBuf::from("/synthetic/execution/source");
            let error = match operation {
                Operation::Directory => SourceError::Directory(VerifyError::Io {
                    action: "inspect",
                    path,
                    source,
                }),
                Operation::Verity => SourceError::Verity {
                    path,
                    source: VerityError::Io {
                        operation: "measure",
                        source,
                    },
                },
                Operation::Metadata => SourceError::Metadata { path, source },
            };
            let status = admission_refusal(source_status(error));
            if expected == tonic::Code::Internal {
                assert!(status.metadata().is_empty());
            }
            assert_eq!(supervisor_launch_status(status).code(), expected);
        }
    }
    let digest = VerityDigest::new(pplx_space_common_core::types::Digest::sha256([1; 32]));
    for source in [
        VerityError::Unsupported,
        VerityError::Unsealed,
        VerityError::DigestFormat,
        VerityError::Mismatch {
            expected: digest,
            actual: VerityDigest::new(pplx_space_common_core::types::Digest::sha256([2; 32])),
        },
    ] {
        let error = SourceError::Verity {
            path: PathBuf::from("/synthetic/execution/source"),
            source,
        };
        assert_eq!(
            supervisor_launch_status(admission_refusal(source_status(error))).code(),
            tonic::Code::FailedPrecondition
        );
    }
}
