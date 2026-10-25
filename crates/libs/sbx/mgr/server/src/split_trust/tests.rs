use super::*;
use pplx_space_common_core::types::{
    Arch, ExecutionMetadata, SandboxArtifactMetadata, TemplateName,
};
use pplx_space_feature_flags::{FeatureFlagClient, SPACE_SANDBOX_SCOPED_TRUST_FLAG};

#[test]
fn enrollment_is_default_off_and_host_decisions_override_later_flag_changes() {
    let directory = tempfile::tempdir().unwrap();
    let id = "019a1234-5678-7000-8000-000000000001";
    let off = FeatureFlagClient::disabled();
    let on = FeatureFlagClient::fixed_bools([(SPACE_SANDBOX_SCOPED_TRUST_FLAG.key(), true)]);
    assert!(!initial_enrollment(directory.path(), id, &off).unwrap());
    assert!(
        initial_enrollment(directory.path(), id, &on).is_ok_and(|value| value)
            == cfg!(feature = "split-trust-prototype")
    );
    let metadata = SandboxArtifactMetadata::new(
        id.parse().unwrap(),
        ExecutionMetadata::initial_firecracker(Arch::X86_64, vec![]),
        TemplateName::parse("seed").unwrap(),
        None,
    );
    let root = directory.path().join(id);
    std::fs::create_dir(&root).unwrap();
    std::fs::write(
        root.join("metadata.json"),
        serde_json::to_vec(&metadata).unwrap(),
    )
    .unwrap();
    assert!(!initial_enrollment(directory.path(), id, &on).unwrap());
    let pin = "mrk-0123456789abcdef0123456789abcdef";
    let scoped = metadata.with_scoped_tls_key_id(Some(pin.into()));
    std::fs::write(
        root.join("metadata.json"),
        serde_json::to_vec(&scoped).unwrap(),
    )
    .unwrap();
    assert_eq!(
        enrollment(directory.path(), id).unwrap(),
        Some(Enrollment::Scoped(pin.parse().unwrap())),
    );
    assert!(
        initial_enrollment(directory.path(), id, &off).is_ok_and(|value| value)
            == cfg!(feature = "split-trust-prototype")
    );
    // A missing guest certificate is never an enrollment signal. Corrupt host
    // metadata must fail closed, not turn an enrolled sandbox into legacy.
    std::fs::write(root.join("metadata.json"), b"{").unwrap();
    assert_eq!(
        initial_enrollment(directory.path(), id, &off)
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition,
    );
    assert_eq!(
        enrollment(directory.path(), "../outside")
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition,
    );
}
