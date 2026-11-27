use super::*;
use crate::policy::SelectionIntent;

fn policy(bytes: &[u8]) -> InstalledPolicy {
    InstalledPolicy {
        authorization: Uuid::now_v7(),
        promotion: Promotion {
            channel: "stable".into(),
            target: ArtifactDigest::of_bytes(bytes),
            target_version: "0.8.2".parse().unwrap(),
            guest_update_required_since: "0.8.2".parse().unwrap(),
            intent: SelectionIntent::Upgrade,
            minimum_host: "0.8.1".parse().unwrap(),
            maximum_guest: "0.8.2".parse().unwrap(),
        },
    }
}

#[test]
fn catalog_keeps_platform_authorities_independent() {
    let linux = policy(b"linux");
    let windows = policy(b"windows");
    let mut catalog = Catalog {
        targets: BTreeMap::from([
            (
                RecoveryPlatform::Linux,
                BTreeMap::from([(Arch::X86_64, linux.clone())]),
            ),
            (
                RecoveryPlatform::Windows,
                BTreeMap::from([(Arch::X86_64, windows.clone())]),
            ),
        ]),
    };
    let decoded = Catalog::parse(&serde_json::to_vec(&catalog).unwrap()).unwrap();
    assert_eq!(
        decoded.select(RecoveryPlatform::Linux, Arch::X86_64),
        Some(&linux)
    );
    assert_eq!(
        decoded.select(RecoveryPlatform::Windows, Arch::X86_64),
        Some(&windows)
    );
    assert!(
        decoded
            .select(RecoveryPlatform::Windows, Arch::Aarch64)
            .is_none()
    );
    assert_eq!(
        decoded.target(windows.promotion.target).unwrap().0,
        RecoveryPlatform::Windows
    );
    catalog.targets.insert(
        RecoveryPlatform::Linux,
        BTreeMap::from([(Arch::X86_64, policy(b"new-linux"))]),
    );
    let decoded = Catalog::parse(&serde_json::to_vec(&catalog).unwrap()).unwrap();
    assert_eq!(
        decoded.select(RecoveryPlatform::Windows, Arch::X86_64),
        Some(&windows)
    );
    assert!(decoded.target(linux.promotion.target).is_none());
}

#[test]
fn catalog_rejects_ambiguous_or_invalid_authority() {
    let policy = policy(b"target");
    let encoded = serde_json::to_value(&policy).unwrap();
    for invalid in [
        serde_json::json!({"targets":{"unknown-contract":{"x86_64":encoded}}}),
        serde_json::json!({"targets":{"linux-systemd-selector-v1":{"unknown-arch":encoded}}}),
        serde_json::json!({"targets":{"linux-systemd-selector-v1":{"x86_64":encoded},"windows-scm-two-slot-v1":{"x86_64":encoded}}}),
        serde_json::json!({"targets":{},"ignored":true}),
    ] {
        Catalog::parse(&serde_json::to_vec(&invalid).unwrap()).unwrap_err();
    }
    let mut invalid =
        serde_json::json!({"targets":{"linux-systemd-selector-v1":{"x86_64":encoded}}});
    invalid["targets"]["linux-systemd-selector-v1"]["x86_64"]["authorization"] =
        serde_json::Value::String(Uuid::nil().to_string());
    Catalog::parse(&serde_json::to_vec(&invalid).unwrap()).unwrap_err();
    Catalog::parse(&vec![b' '; MAX_METADATA_BYTES + 1]).unwrap_err();
}
