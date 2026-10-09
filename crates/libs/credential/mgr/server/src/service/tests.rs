use pplx_space_common_core::types::{OrgId, SandboxId, UserId};

use std::collections::HashMap;

use pplx_space_common_svc::PeerIdentity;
use uuid::Uuid;
use zeroize::Zeroizing;

use super::*;
use crate::auth::ResolvedCaller;
use crate::in_memory::InMemoryKeyWrapper;
use crate::kek_store::KekRecord;

/// In-memory KEK-row map standing in for Postgres.
struct TestKekStore {
    rows: HashMap<(Uuid, OrgId), KekRecord>,
}

#[tonic::async_trait]
impl KekStore for TestKekStore {
    async fn active_kek_for_org(&self, org: OrgId) -> Result<Option<KekRecord>, KekStoreError> {
        Ok(self
            .rows
            .iter()
            .find(|((_, row_org), record)| *row_org == org && !record.retired)
            .map(|(_, record)| record.clone()))
    }

    async fn kek_for_org(
        &self,
        cmk_id: Uuid,
        org: OrgId,
    ) -> Result<Option<KekRecord>, KekStoreError> {
        Ok(self.rows.get(&(cmk_id, org)).cloned())
    }
}

/// A consistent map entry: the row key and the record's cmk_id match.
fn kek_row(org: OrgId, retired: bool) -> ((Uuid, OrgId), KekRecord) {
    let cmk_id = Uuid::now_v7();
    ((cmk_id, org), in_memory_kek(cmk_id, retired))
}

fn in_memory_kek(cmk_id: Uuid, retired: bool) -> KekRecord {
    KekRecord {
        cmk_id,
        provider: "in-memory".parse().expect("in-memory provider parses"),
        key_identifier: "test-kek".parse().expect("identifier parses"),
        retired,
    }
}

fn service_with(
    wrapper: Arc<InMemoryKeyWrapper>,
    rows: HashMap<(Uuid, OrgId), KekRecord>,
) -> CredentialManagerService {
    CredentialManagerService::new(wrapper, PeerProcessCheckMode::Off)
        .with_kek_store(Arc::new(TestKekStore { rows }))
}

/// A request that passes authorization as the root UDS peer.
fn root_request<T>(message: T) -> Request<T> {
    let mut request = Request::new(message);
    request
        .extensions_mut()
        .insert(PeerIdentity::new(0, 0, Some(1)));
    request
}

fn plaintext_request<T>(message: T) -> Request<T> {
    let mut request = Request::new(message);
    request
        .extensions_mut()
        .insert(crate::tcp_auth::TcpCallerIdentity::Plaintext);
    request
}

trait TakePlaintextDek {
    fn take_plaintext_dek(self) -> Zeroizing<Vec<u8>>;
}

impl TakePlaintextDek for GetOrIssueSandboxVolumeDekResponse {
    fn take_plaintext_dek(mut self) -> Zeroizing<Vec<u8>> {
        Zeroizing::new(std::mem::take(&mut self.plaintext_dek))
    }
}

fn encrypt_request(
    org: OrgId,
    user_id: Option<UserId>,
    plaintext: &[u8],
    aad: &[u8],
) -> EncryptRequest {
    EncryptRequest {
        organization_id: org.to_string(),
        user_id: user_id.map(|u| u.to_string()),
        plaintext: plaintext.to_vec(),
        aad: aad.to_vec(),
        purpose: "test".to_string(),
    }
}

fn decrypt_request(
    org: OrgId,
    user_id: Option<UserId>,
    sealed: &EncryptResponse,
    aad: &[u8],
) -> DecryptRequest {
    DecryptRequest {
        organization_id: org.to_string(),
        user_id: user_id.map(|u| u.to_string()),
        ciphertext: sealed.ciphertext.clone(),
        wrapped_dek: sealed.wrapped_dek.clone(),
        aad: aad.to_vec(),
        purpose: "test".to_string(),
    }
}

#[test]
fn over_long_purpose_is_rejected_not_truncated() {
    // The AAD encodes purpose length in a u16 prefix. A purpose longer
    // than u16::MAX must be rejected, not silently truncated — otherwise
    // two distinct purposes whose lengths differ only above 65535 would
    // serialize to the same AAD, so a payload sealed under one purpose
    // could open under another.
    let org = OrgId::new();
    let too_long = "a".repeat(u16::MAX as usize + 1);
    let err = effective_aad(org, None, &too_long, b"").unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    // A purpose at the boundary still works.
    let ok = "a".repeat(u16::MAX as usize);
    effective_aad(org, None, &ok, b"").unwrap();
}

#[tokio::test]
async fn encrypt_decrypt_round_trips_without_naming_a_key() {
    let org = OrgId::new();
    let user = UserId::new();
    let service = service_with(
        Arc::new(InMemoryKeyWrapper::new()),
        HashMap::from([kek_row(org, false)]),
    );

    let sealed = service
        .encrypt(root_request(encrypt_request(
            org,
            Some(user),
            b"s3cret",
            b"ctx",
        )))
        .await
        .unwrap()
        .into_inner();

    let plaintext = service
        .decrypt(root_request(decrypt_request(
            org,
            Some(user),
            &sealed,
            b"ctx",
        )))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(plaintext.plaintext, b"s3cret");
}

#[tokio::test]
async fn encrypt_fails_when_org_has_no_active_kek() {
    let provisioned = OrgId::new();
    let unprovisioned = OrgId::new();
    let retired_only = OrgId::new();
    let cmk = Uuid::now_v7();
    let service = service_with(
        Arc::new(InMemoryKeyWrapper::new()),
        HashMap::from([
            kek_row(provisioned, false),
            ((cmk, retired_only), in_memory_kek(cmk, true)),
        ]),
    );

    for org in [unprovisioned, retired_only] {
        let err = service
            .encrypt(root_request(encrypt_request(org, None, b"x", b"")))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert!(err.message().contains("no active KEK"));
    }
}

#[tokio::test]
async fn retired_kek_still_decrypts_existing_rows() {
    let org = OrgId::new();
    let cmk = Uuid::now_v7();
    let wrapper = Arc::new(InMemoryKeyWrapper::new());

    // Seal while the KEK is active …
    let before = service_with(
        wrapper.clone(),
        HashMap::from([((cmk, org), in_memory_kek(cmk, false))]),
    );
    let sealed = before
        .encrypt(root_request(encrypt_request(org, None, b"data", b"a")))
        .await
        .unwrap()
        .into_inner();

    // … then retire it (same row, same key material) and decrypt.
    let after = service_with(
        wrapper,
        HashMap::from([((cmk, org), in_memory_kek(cmk, true))]),
    );
    let plaintext = after
        .decrypt(root_request(decrypt_request(org, None, &sealed, b"a")))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(plaintext.plaintext, b"data");
}

#[tokio::test]
async fn decrypt_under_a_foreign_org_is_rejected() {
    let owner = OrgId::new();
    let attacker = OrgId::new();
    let cmk = Uuid::now_v7();
    let service = service_with(
        Arc::new(InMemoryKeyWrapper::new()),
        HashMap::from([
            ((cmk, owner), in_memory_kek(cmk, false)),
            // The attacker org is provisioned too — its KEK is simply a
            // different row, so the envelope's cmk_id resolves to nothing.
            kek_row(attacker, false),
        ]),
    );

    let sealed = service
        .encrypt(root_request(encrypt_request(owner, None, b"data", b"")))
        .await
        .unwrap()
        .into_inner();
    let err = service
        .decrypt(root_request(decrypt_request(attacker, None, &sealed, b"")))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
}

#[tokio::test]
async fn decrypt_with_wrong_user_is_permission_denied() {
    let org = OrgId::new();
    let service = service_with(
        Arc::new(InMemoryKeyWrapper::new()),
        HashMap::from([kek_row(org, false)]),
    );

    let sealed = service
        .encrypt(root_request(encrypt_request(
            org,
            Some(UserId::new()),
            b"data",
            b"",
        )))
        .await
        .unwrap()
        .into_inner();
    for wrong_user in [Some(Uuid::now_v7()), None] {
        let err = service
            .decrypt(root_request(decrypt_request(
                org,
                wrong_user.map(UserId::from_uuid),
                &sealed,
                b"",
            )))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }
}

#[tokio::test]
async fn decrypt_with_wrong_purpose_is_permission_denied() {
    let org = OrgId::new();
    let service = service_with(
        Arc::new(InMemoryKeyWrapper::new()),
        HashMap::from([kek_row(org, false)]),
    );
    let sealed = service
        .encrypt(root_request(encrypt_request(org, None, b"data", b"")))
        .await
        .unwrap()
        .into_inner();
    // Same org/user/aad, different purpose → must not decrypt.
    let err = service
        .decrypt(root_request(DecryptRequest {
            organization_id: org.to_string(),
            user_id: None,
            ciphertext: sealed.ciphertext,
            wrapped_dek: sealed.wrapped_dek,
            aad: Vec::new(),
            purpose: "other".to_string(),
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
}

#[tokio::test]
async fn encrypt_requires_a_purpose() {
    let org = OrgId::new();
    let service = service_with(
        Arc::new(InMemoryKeyWrapper::new()),
        HashMap::from([kek_row(org, false)]),
    );
    let mut req = encrypt_request(org, None, b"data", b"");
    req.purpose = String::new();
    let err = service.encrypt(root_request(req)).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn decrypt_with_wrong_aad_is_permission_denied() {
    let org = OrgId::new();
    let service = service_with(
        Arc::new(InMemoryKeyWrapper::new()),
        HashMap::from([kek_row(org, false)]),
    );

    let sealed = service
        .encrypt(root_request(encrypt_request(org, None, b"data", b"right")))
        .await
        .unwrap()
        .into_inner();
    let err = service
        .decrypt(root_request(decrypt_request(org, None, &sealed, b"wrong")))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
}

#[tokio::test]
async fn tampered_wrapped_dek_envelope_is_invalid_argument() {
    let org = OrgId::new();
    let service = service_with(
        Arc::new(InMemoryKeyWrapper::new()),
        HashMap::from([kek_row(org, false)]),
    );

    let mut sealed = service
        .encrypt(root_request(encrypt_request(org, None, b"data", b"")))
        .await
        .unwrap()
        .into_inner();
    sealed.wrapped_dek[0] = 9;
    let err = service
        .decrypt(root_request(decrypt_request(org, None, &sealed, b"")))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn oversized_plaintext_is_rejected() {
    let org = OrgId::new();
    let service = service_with(
        Arc::new(InMemoryKeyWrapper::new()),
        HashMap::from([kek_row(org, false)]),
    );

    let big = vec![0u8; MAX_ENCRYPT_PLAINTEXT_LEN + 1];
    let err = service
        .encrypt(root_request(encrypt_request(org, None, &big, b"")))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

struct TestCredentialStore {
    owner: crate::credential_store::SandboxOwner,
}

const PERSISTED_WRAP_INVALID_KEY_DETAIL: &str = "provider rejected persisted key";

struct InvalidKeyOnUnwrapWrapper;

#[tonic::async_trait]
impl KeyWrapper for InvalidKeyOnUnwrapWrapper {
    async fn generate_and_wrap_dek(
        &self,
        _org: OrgId,
        _requesting_sandbox: Option<SandboxId>,
        _provider: KekProvider,
        _master_key_identifier: &MasterKeyIdentifier,
        _spec: DekKeySpec,
        _aad: &[u8],
    ) -> Result<GenerateDekOutput, WrapError> {
        Err(WrapError::Backend(
            "persisted-wrap test unexpectedly issued a DEK".into(),
        ))
    }

    async fn unwrap_dek(
        &self,
        _org: OrgId,
        _requesting_sandbox: Option<SandboxId>,
        _provider: KekProvider,
        _master_key_identifier: &MasterKeyIdentifier,
        _wrapped_dek: &[u8],
        _aad: &[u8],
    ) -> Result<Vec<u8>, UnwrapError> {
        Err(UnwrapError::InvalidKey(
            PERSISTED_WRAP_INVALID_KEY_DETAIL.into(),
        ))
    }
}

#[tonic::async_trait]
impl CredentialStore for TestCredentialStore {
    async fn sandbox_owner(
        &self,
        _sandbox_id: SandboxId,
    ) -> Result<
        Option<crate::credential_store::SandboxOwner>,
        crate::credential_store::CredentialStoreError,
    > {
        Ok(Some(self.owner))
    }
}

#[tokio::test]
async fn instance_without_database_refuses_envelope_rpcs() {
    let service = CredentialManagerService::new(
        Arc::new(InMemoryKeyWrapper::new()),
        PeerProcessCheckMode::Off,
    );
    let err = service
        .encrypt(root_request(encrypt_request(OrgId::new(), None, b"x", b"")))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("database"));
}

/// In-memory volume-wrap store standing in for the
/// `sandbox_volume_encryption` table, including its first-writer-wins
/// insert.
#[derive(Default)]
struct TestVolumeWrapStore {
    rows: std::sync::Mutex<HashMap<SandboxId, crate::volume_wrap_store::VolumeWrap>>,
}

#[tonic::async_trait]
impl VolumeWrapStore for TestVolumeWrapStore {
    async fn get(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<
        Option<crate::volume_wrap_store::VolumeWrap>,
        crate::volume_wrap_store::VolumeWrapStoreError,
    > {
        Ok(self.rows.lock().unwrap().get(&sandbox_id).cloned())
    }

    async fn insert_if_absent(
        &self,
        sandbox_id: SandboxId,
        org_id: OrgId,
        cmk_id: Uuid,
        wrapped_dek: &[u8],
    ) -> Result<crate::volume_wrap_store::VolumeWrap, crate::volume_wrap_store::VolumeWrapStoreError>
    {
        let mut rows = self.rows.lock().unwrap();
        Ok(rows
            .entry(sandbox_id)
            .or_insert_with(|| crate::volume_wrap_store::VolumeWrap {
                org_id,
                cmk_id,
                wrapped_dek: wrapped_dek.to_vec(),
            })
            .clone())
    }
}

fn volume_service(
    org: OrgId,
    sandbox_owner_org: Option<OrgId>,
    kek_rows: HashMap<(Uuid, OrgId), KekRecord>,
    store: Arc<TestVolumeWrapStore>,
) -> CredentialManagerService {
    volume_service_byok(org, sandbox_owner_org, true, kek_rows, store)
}

fn volume_service_byok(
    org: OrgId,
    sandbox_owner_org: Option<OrgId>,
    byok_enabled: bool,
    kek_rows: HashMap<(Uuid, OrgId), KekRecord>,
    store: Arc<TestVolumeWrapStore>,
) -> CredentialManagerService {
    let _ = org;
    let mut service = CredentialManagerService::new(
        Arc::new(InMemoryKeyWrapper::new()),
        PeerProcessCheckMode::Off,
    )
    .with_kek_store(Arc::new(TestKekStore { rows: kek_rows }))
    .with_volume_wrap_store(store);
    if let Some(owner_org) = sandbox_owner_org {
        service = service.with_credential_store(Arc::new(TestCredentialStore {
            owner: crate::credential_store::SandboxOwner {
                user_id: UserId::new(),
                org_id: owner_org,
                byok_enabled,
            },
        }));
    }
    service
}

/// The core single-DEK-per-sandbox guarantee: two GetOrIssue calls for the
/// same sandbox return byte-identical plaintext, and exactly one wrap row
/// is persisted. The first call issues; the second loads.
#[tokio::test]
async fn volume_dek_is_one_stable_key_per_sandbox() {
    let org = OrgId::new();
    let store = Arc::new(TestVolumeWrapStore::default());
    let service = volume_service(
        org,
        Some(org),
        HashMap::from([kek_row(org, false)]),
        store.clone(),
    );
    let sandbox_id = SandboxId::new().to_string();

    let first = service
        .get_or_issue_sandbox_volume_dek(root_request(GetOrIssueSandboxVolumeDekRequest {
            sandbox_id: sandbox_id.clone(),
            expected_org_id: String::new(),
        }))
        .await
        .unwrap()
        .into_inner()
        .take_plaintext_dek();
    let second = service
        .get_or_issue_sandbox_volume_dek(root_request(GetOrIssueSandboxVolumeDekRequest {
            sandbox_id: sandbox_id.clone(),
            expected_org_id: String::new(),
        }))
        .await
        .unwrap()
        .into_inner()
        .take_plaintext_dek();

    assert_eq!(first.len(), 32, "AES-256 DEK");
    assert_eq!(first, second, "the same sandbox must resolve to one DEK");
    assert_eq!(
        store.rows.lock().unwrap().len(),
        1,
        "exactly one wrap row per sandbox"
    );
}

#[tokio::test]
async fn persisted_volume_wrap_invalid_key_is_machine_readable() {
    let org = OrgId::new();
    let sandbox_id = SandboxId::new();
    let ((cmk_id, row_org), kek) = kek_row(org, false);
    let store = Arc::new(TestVolumeWrapStore::default());
    store.rows.lock().unwrap().insert(
        sandbox_id,
        crate::volume_wrap_store::VolumeWrap {
            org_id: org,
            cmk_id,
            wrapped_dek: vec![0xA5; 48],
        },
    );
    let service = CredentialManagerService::new(
        Arc::new(InvalidKeyOnUnwrapWrapper),
        PeerProcessCheckMode::Off,
    )
    .with_kek_store(Arc::new(TestKekStore {
        rows: HashMap::from([((cmk_id, row_org), kek)]),
    }))
    .with_volume_wrap_store(store)
    .with_credential_store(Arc::new(TestCredentialStore {
        owner: crate::credential_store::SandboxOwner {
            user_id: UserId::new(),
            org_id: org,
            byok_enabled: true,
        },
    }));

    let status = service
        .get_or_issue_sandbox_volume_dek(root_request(GetOrIssueSandboxVolumeDekRequest {
            sandbox_id: sandbox_id.to_string(),
            expected_org_id: String::new(),
        }))
        .await
        .unwrap_err();

    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    assert!(pplx_space_credmgr_client::status_is_invalid_key_reference(
        &status
    ));
    assert!(status.message().contains(PERSISTED_WRAP_INVALID_KEY_DETAIL));
}

/// No active KEK for the sandbox's org → issuing fails closed.
#[tokio::test]
async fn volume_dek_without_active_kek_fails_precondition() {
    let org = OrgId::new();
    let service = volume_service(
        org,
        Some(org),
        HashMap::new(),
        Arc::new(TestVolumeWrapStore::default()),
    );
    let err = service
        .get_or_issue_sandbox_volume_dek(root_request(GetOrIssueSandboxVolumeDekRequest {
            sandbox_id: Uuid::now_v7().to_string(),
            expected_org_id: String::new(),
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
}

/// Claim-ordering guard: a caller asserting the wrong owner is refused
/// BEFORE any wrap is minted or unwrapped. The wrap is AAD-bound to
/// (org, sandbox) first-writer-wins, so a re-home that ran while the
/// sandbox was still owned by another org would otherwise persist a wrap
/// the real owner can never unwrap — a permanently undecryptable volume.
#[tokio::test]
async fn volume_dek_refuses_a_mismatched_expected_org() {
    let owner = OrgId::new();
    let store = Arc::new(TestVolumeWrapStore::default());
    let service = volume_service(
        owner,
        Some(owner),
        HashMap::from([kek_row(owner, false)]),
        store.clone(),
    );
    let sandbox_id = SandboxId::new().to_string();

    let err = service
        .get_or_issue_sandbox_volume_dek(root_request(GetOrIssueSandboxVolumeDekRequest {
            sandbox_id: sandbox_id.clone(),
            expected_org_id: OrgId::new().to_string(),
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(
        store.rows.lock().unwrap().is_empty(),
        "a refused owner mismatch must persist nothing"
    );

    let err = service
        .get_or_issue_sandbox_volume_dek(root_request(GetOrIssueSandboxVolumeDekRequest {
            sandbox_id: sandbox_id.clone(),
            expected_org_id: "not-a-uuid".to_string(),
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    // A correct assertion behaves exactly like no assertion: same DEK.
    let asserted = service
        .get_or_issue_sandbox_volume_dek(root_request(GetOrIssueSandboxVolumeDekRequest {
            sandbox_id: sandbox_id.clone(),
            expected_org_id: owner.to_string(),
        }))
        .await
        .unwrap()
        .into_inner()
        .take_plaintext_dek();
    let unasserted = service
        .get_or_issue_sandbox_volume_dek(root_request(GetOrIssueSandboxVolumeDekRequest {
            sandbox_id,
            expected_org_id: String::new(),
        }))
        .await
        .unwrap()
        .into_inner()
        .take_plaintext_dek();
    assert_eq!(asserted, unasserted);
}

/// A stored wrap whose org no longer matches the sandbox's resolved owner is
/// permanently undecryptable (the AAD binds the old org), so the service must
/// refuse with FAILED_PRECONDITION — a permanent verdict — rather than fail
/// KEK resolution with an Internal that retry taxonomies read as transient.
#[tokio::test]
async fn volume_dek_refuses_a_wrap_bound_to_a_previous_owner() {
    let owner = OrgId::new();
    let previous_owner = OrgId::new();
    let sandbox_id = SandboxId::new();
    let store = Arc::new(TestVolumeWrapStore::default());
    store.rows.lock().unwrap().insert(
        sandbox_id,
        crate::volume_wrap_store::VolumeWrap {
            org_id: previous_owner,
            cmk_id: Uuid::now_v7(),
            wrapped_dek: vec![0xA5; 48],
        },
    );
    let service = volume_service(
        owner,
        Some(owner),
        HashMap::from([kek_row(owner, false)]),
        store,
    );

    let err = service
        .get_or_issue_sandbox_volume_dek(root_request(GetOrIssueSandboxVolumeDekRequest {
            sandbox_id: sandbox_id.to_string(),
            expected_org_id: String::new(),
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
}

/// `byok_enabled = FALSE` gates encryption OFF even when the org has an
/// active KEK — an org can already have a KEK for credential encryption
/// without being a BYOK org, so keying solely on the KEK would wrongly route
/// its sandboxes onto the encrypted path. GetOrIssue must refuse.
#[tokio::test]
async fn byok_disabled_org_with_active_kek_is_not_encrypted() {
    let org = OrgId::new();
    // Active KEK present, but the org has byok_enabled = false.
    let issue = volume_service_byok(
        org,
        Some(org),
        false,
        HashMap::from([kek_row(org, false)]),
        Arc::new(TestVolumeWrapStore::default()),
    );
    let err = issue
        .get_or_issue_sandbox_volume_dek(root_request(GetOrIssueSandboxVolumeDekRequest {
            sandbox_id: Uuid::now_v7().to_string(),
            expected_org_id: String::new(),
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
}

/// Keys outlive enrollment: once a sandbox has an issued volume wrap, a
/// later GetOrIssue must still LOAD (unwrap) that DEK even after the org
/// disables BYOK. The sandbox's on-disk volumes are already LUKS with no
/// plaintext fallback, so refusing here would permanently strand
/// restore/start of an already-encrypted sandbox. Only *minting a first*
/// DEK requires byok_enabled (covered by
/// `byok_disabled_org_with_active_kek_is_not_encrypted`).
#[tokio::test]
async fn issued_volume_dek_loads_after_byok_disabled() {
    let org = OrgId::new();
    // One shared wrapper + KEK rows + wrap store across both services, so
    // the wrap minted while enrolled can be unwrapped after de-enrollment
    // (InMemoryKeyWrapper mints a per-instance random KEK, so the wrapper
    // must be shared, not rebuilt).
    let wrapper = Arc::new(InMemoryKeyWrapper::new());
    let kek_rows = HashMap::from([kek_row(org, false)]);
    let store = Arc::new(TestVolumeWrapStore::default());
    let build = |byok_enabled: bool| {
        CredentialManagerService::new(wrapper.clone(), PeerProcessCheckMode::Off)
            .with_kek_store(Arc::new(TestKekStore {
                rows: kek_rows.clone(),
            }))
            .with_volume_wrap_store(store.clone())
            .with_credential_store(Arc::new(TestCredentialStore {
                owner: crate::credential_store::SandboxOwner {
                    user_id: UserId::new(),
                    org_id: org,
                    byok_enabled,
                },
            }))
    };
    let sandbox_id = SandboxId::new().to_string();

    // Issue while the org is enrolled — mints + stores the wrap.
    let issued = build(true)
        .get_or_issue_sandbox_volume_dek(root_request(GetOrIssueSandboxVolumeDekRequest {
            sandbox_id: sandbox_id.clone(),
            expected_org_id: String::new(),
        }))
        .await
        .expect("issue while enrolled")
        .into_inner()
        .take_plaintext_dek();

    // Org disables BYOK. Loading the already-stored wrap must still succeed
    // and return the same DEK.
    let loaded = build(false)
        .get_or_issue_sandbox_volume_dek(root_request(GetOrIssueSandboxVolumeDekRequest {
            sandbox_id: sandbox_id.clone(),
            expected_org_id: String::new(),
        }))
        .await
        .expect("an already-encrypted sandbox must load its DEK after BYOK is disabled")
        .into_inner()
        .take_plaintext_dek();

    assert_eq!(issued.len(), 32, "AES-256 DEK");
    assert_eq!(issued, loaded, "keys must outlive enrollment");
}

/// A KEK store whose active row can be provisioned on demand, modeling the
/// production `PgKekStore` + `IntermediateKeyProvisioner` pair: provisioning is
/// idempotent per org (one active KEK, mimicking the partial unique index's
/// `ON CONFLICT`/adopt-winner), and a provisioned row is immediately
/// readable back by `cmk_id` — the same consistency the real Postgres store
/// gives the volume unwrap path. The single struct backs both the
/// `KekStore` reads and the `KekProvisioner` writes over one shared map, so
/// a provisioned KEK is visible to the later `kek_for_org` unwrap lookups.
#[derive(Clone, Default)]
struct ProvisioningKekStore {
    rows: Arc<std::sync::Mutex<HashMap<(Uuid, OrgId), KekRecord>>>,
    issues: Arc<std::sync::atomic::AtomicUsize>,
}

impl ProvisioningKekStore {
    fn seeded(rows: HashMap<(Uuid, OrgId), KekRecord>) -> Self {
        Self {
            rows: Arc::new(std::sync::Mutex::new(rows)),
            issues: Arc::default(),
        }
    }

    fn issues(&self) -> usize {
        self.issues.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn active_count(&self, org: OrgId) -> usize {
        self.rows
            .lock()
            .unwrap()
            .iter()
            .filter(|((_, row_org), record)| *row_org == org && !record.retired)
            .count()
    }
}

#[tonic::async_trait]
impl KekStore for ProvisioningKekStore {
    async fn active_kek_for_org(&self, org: OrgId) -> Result<Option<KekRecord>, KekStoreError> {
        Ok(self
            .rows
            .lock()
            .unwrap()
            .iter()
            .find(|((_, row_org), record)| *row_org == org && !record.retired)
            .map(|(_, record)| record.clone()))
    }

    async fn kek_for_org(
        &self,
        cmk_id: Uuid,
        org: OrgId,
    ) -> Result<Option<KekRecord>, KekStoreError> {
        Ok(self.rows.lock().unwrap().get(&(cmk_id, org)).cloned())
    }
}

#[tonic::async_trait]
impl KekProvisioner for ProvisioningKekStore {
    async fn provision_active_kek(
        &self,
        org: OrgId,
        _requesting_sandbox: Option<SandboxId>,
    ) -> Result<KekRecord, ProvisionError> {
        // No `.await` while the lock is held, so the whole resolve-or-insert
        // is atomic — concurrent callers serialize here, one inserts and the
        // rest adopt, exactly like the real ON CONFLICT / adopt-winner path.
        let mut rows = self.rows.lock().unwrap();
        if let Some(existing) = rows
            .iter()
            .find(|((_, row_org), record)| *row_org == org && !record.retired)
            .map(|(_, record)| record.clone())
        {
            return Ok(existing);
        }
        self.issues
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let cmk_id = Uuid::now_v7();
        let record = in_memory_kek(cmk_id, false);
        rows.insert((cmk_id, org), record.clone());
        Ok(record)
    }
}

/// A provisioner that always fails closed with the provider's revocation
/// classification (org offboarded), persisting nothing.
struct DenyingProvisioner;

#[tonic::async_trait]
impl KekProvisioner for DenyingProvisioner {
    async fn provision_active_kek(
        &self,
        _org: OrgId,
        _requesting_sandbox: Option<SandboxId>,
    ) -> Result<KekRecord, ProvisionError> {
        Err(ProvisionError::Provider(ProviderError::AccessRevoked(
            "org offboarded".into(),
        )))
    }
}

fn volume_service_with_kek_store(
    owner_org: OrgId,
    byok_enabled: bool,
    kek_store: Arc<dyn KekStore>,
    volume_store: Arc<TestVolumeWrapStore>,
) -> CredentialManagerService {
    CredentialManagerService::new(
        Arc::new(InMemoryKeyWrapper::new()),
        PeerProcessCheckMode::Off,
    )
    .with_kek_store(kek_store)
    .with_volume_wrap_store(volume_store)
    .with_credential_store(Arc::new(TestCredentialStore {
        owner: crate::credential_store::SandboxOwner {
            user_id: UserId::new(),
            org_id: owner_org,
            byok_enabled,
        },
    }))
}

fn volume_request() -> GetOrIssueSandboxVolumeDekRequest {
    GetOrIssueSandboxVolumeDekRequest {
        sandbox_id: Uuid::now_v7().to_string(),
        expected_org_id: String::new(),
    }
}

/// The gap: a BYOK org whose provider self-provisions (intermediate-key-broker)
/// but which has no active KEK yet. Without a provisioner installed the
/// first-volume path fails closed (the historical behavior); with the same
/// resolve-or-provision provisioner Encrypt uses, the first sandbox volume
/// enrolls the org and succeeds — so BYOK volume encryption is
/// self-sufficient and no longer needs a credential Encrypt to enroll first.
#[tokio::test]
async fn volume_dek_lazily_provisions_when_a_provisioner_is_installed() {
    let org = OrgId::new();

    // No provisioner: an org with no active KEK still fails closed.
    let bare = volume_service_with_kek_store(
        org,
        true,
        Arc::new(ProvisioningKekStore::default()),
        Arc::new(TestVolumeWrapStore::default()),
    );
    let err = bare
        .get_or_issue_sandbox_volume_dek(root_request(volume_request()))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);

    // With a provisioner, the first volume enrolls the org and succeeds,
    // and a repeat call for the same sandbox loads the stored wrap without
    // re-provisioning.
    let provisioning = ProvisioningKekStore::default();
    let volume_store = Arc::new(TestVolumeWrapStore::default());
    let service = volume_service_with_kek_store(
        org,
        true,
        Arc::new(provisioning.clone()),
        volume_store.clone(),
    )
    .with_kek_provisioner(Arc::new(provisioning.clone()));

    let sandbox_id = SandboxId::new().to_string();
    let first = service
        .get_or_issue_sandbox_volume_dek(root_request(GetOrIssueSandboxVolumeDekRequest {
            sandbox_id: sandbox_id.clone(),
            expected_org_id: String::new(),
        }))
        .await
        .expect("first volume must provision the KEK and succeed")
        .into_inner()
        .take_plaintext_dek();
    assert_eq!(first.len(), 32, "AES-256 DEK");
    assert_eq!(provisioning.issues(), 1, "first volume provisions once");

    let second = service
        .get_or_issue_sandbox_volume_dek(root_request(GetOrIssueSandboxVolumeDekRequest {
            sandbox_id,
            expected_org_id: String::new(),
        }))
        .await
        .expect("the stored wrap must load after provisioning")
        .into_inner()
        .take_plaintext_dek();
    assert_eq!(first, second, "one stable DEK per sandbox");
    assert_eq!(provisioning.issues(), 1, "loading must not re-provision");
    assert_eq!(volume_store.rows.lock().unwrap().len(), 1);
}

/// Any already-provisioned org issues a volume DEK without invoking the
/// provisioner again.
#[tokio::test]
async fn volume_dek_with_active_kek_does_not_provision() {
    let org = OrgId::new();
    let provisioning = ProvisioningKekStore::seeded(HashMap::from([kek_row(org, false)]));
    let service = volume_service_with_kek_store(
        org,
        true,
        Arc::new(provisioning.clone()),
        Arc::new(TestVolumeWrapStore::default()),
    )
    .with_kek_provisioner(Arc::new(provisioning.clone()));

    let dek = service
        .get_or_issue_sandbox_volume_dek(root_request(volume_request()))
        .await
        .expect("an org with an active KEK issues without provisioning")
        .into_inner()
        .take_plaintext_dek();
    assert_eq!(dek.len(), 32);
    assert_eq!(
        provisioning.issues(),
        0,
        "an already-provisioned org must never re-provision"
    );
}

/// A provider that refuses enrollment (org offboarded / not recognized)
/// fails closed with the machine-readable revocation reason and persists
/// no volume wrap — the same fail-closed posture the wrap/unwrap paths use.
#[tokio::test]
async fn volume_dek_provisioner_denial_fails_closed_and_persists_nothing() {
    let org = OrgId::new();
    let volume_store = Arc::new(TestVolumeWrapStore::default());
    let service = volume_service_with_kek_store(
        org,
        true,
        Arc::new(ProvisioningKekStore::default()),
        volume_store.clone(),
    )
    .with_kek_provisioner(Arc::new(DenyingProvisioner));

    let err = service
        .get_or_issue_sandbox_volume_dek(root_request(volume_request()))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(
        pplx_space_credmgr_client::status_is_org_kek_access_revoked(&err),
        "a provider denial must carry the machine-readable revocation reason"
    );
    assert_eq!(
        volume_store.rows.lock().unwrap().len(),
        0,
        "a denied provision must persist no volume wrap"
    );
}

/// A non-BYOK org never reaches provisioning: the `byok_enabled` gate
/// refuses a *new* encryption before any KEK resolve-or-provision, so a
/// credential-injection org (platform-managed KEK, BYOK off) cannot be
/// enrolled onto the encrypted path by a stray volume call.
#[tokio::test]
async fn volume_dek_for_non_byok_org_never_provisions() {
    let org = OrgId::new();
    let provisioning = ProvisioningKekStore::default();
    let service = volume_service_with_kek_store(
        org,
        false,
        Arc::new(provisioning.clone()),
        Arc::new(TestVolumeWrapStore::default()),
    )
    .with_kek_provisioner(Arc::new(provisioning.clone()));

    let err = service
        .get_or_issue_sandbox_volume_dek(root_request(volume_request()))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert_eq!(
        provisioning.issues(),
        0,
        "the byok gate must reject before any provisioning"
    );
}

/// Concurrency: many first-writers for the same sandbox converge on one
/// provisioned KEK (one active row, one issue) and one stored wrap, and all
/// callers observe byte-identical DEKs — the losers unwrap the winner's
/// wrap through the provisioned KEK.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn volume_dek_concurrent_first_writers_converge() {
    let org = OrgId::new();
    let provisioning = ProvisioningKekStore::default();
    let volume_store = Arc::new(TestVolumeWrapStore::default());
    let service = volume_service_with_kek_store(
        org,
        true,
        Arc::new(provisioning.clone()),
        volume_store.clone(),
    )
    .with_kek_provisioner(Arc::new(provisioning.clone()));
    let sandbox_id = SandboxId::new().to_string();

    let mut handles = Vec::new();
    for _ in 0..8 {
        let service = service.clone();
        let sandbox_id = sandbox_id.clone();
        handles.push(tokio::spawn(async move {
            service
                .get_or_issue_sandbox_volume_dek(root_request(GetOrIssueSandboxVolumeDekRequest {
                    sandbox_id,
                    expected_org_id: String::new(),
                }))
                .await
                .expect("every concurrent first-writer must succeed")
                .into_inner()
                .take_plaintext_dek()
        }));
    }
    let mut deks = Vec::new();
    for handle in handles {
        deks.push(handle.await.unwrap());
    }
    let first = &deks[0];
    assert_eq!(first.len(), 32);
    assert!(
        deks.iter().all(|dek| dek == first),
        "all concurrent callers must resolve to one DEK"
    );
    assert_eq!(provisioning.issues(), 1, "the KEK must be provisioned once");
    assert_eq!(provisioning.active_count(org), 1, "one active KEK");
    assert_eq!(
        volume_store.rows.lock().unwrap().len(),
        1,
        "exactly one wrap row per sandbox"
    );
}

/// Revocation must be machine-readable across the process boundary:
/// consumers (org cleanup / sandbox shutdown) decode the reason metadata
/// via the client helper, never by parsing status messages.
#[test]
fn access_revoked_status_is_machine_readable() {
    for status in [
        wrap_error_to_status(WrapError::AccessRevoked),
        unwrap_error_to_status(UnwrapError::AccessRevoked),
    ] {
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert!(pplx_space_credmgr_client::status_is_org_kek_access_revoked(
            &status
        ));
    }
    // Other failed-precondition statuses carry no revocation reason.
    let other = wrap_error_to_status(WrapError::UnsupportedProvider(KekProvider::AwsKms));
    assert_eq!(other.code(), tonic::Code::FailedPrecondition);
    assert!(!pplx_space_credmgr_client::status_is_org_kek_access_revoked(&other));
}

struct FakeProvisioner {
    record: KekRecord,
    calls: std::sync::atomic::AtomicUsize,
}

#[tonic::async_trait]
impl KekProvisioner for FakeProvisioner {
    async fn provision_active_kek(
        &self,
        _org: OrgId,
        _requesting_sandbox: Option<SandboxId>,
    ) -> Result<KekRecord, ProvisionError> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(self.record.clone())
    }
}

/// Without a provisioner an unprovisioned org stays a precondition
/// failure; with one, Encrypt provisions lazily and proceeds.
#[tokio::test]
async fn encrypt_lazily_provisions_when_a_provisioner_is_installed() {
    let wrapper = Arc::new(InMemoryKeyWrapper::new());
    let org = OrgId::new();

    let bare = service_with(wrapper.clone(), HashMap::new());
    let status = bare
        .encrypt(root_request(encrypt_request(org, None, b"secret", b"")))
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);

    let (_, record) = kek_row(org, false);
    let provisioner = Arc::new(FakeProvisioner {
        record,
        calls: std::sync::atomic::AtomicUsize::new(0),
    });
    let service = service_with(wrapper, HashMap::new()).with_kek_provisioner(provisioner.clone());
    let response = service
        .encrypt(root_request(encrypt_request(org, None, b"secret", b"")))
        .await
        .expect("encrypt must succeed after lazy provisioning");
    assert!(!response.into_inner().ciphertext.is_empty());
    assert_eq!(
        provisioner.calls.load(std::sync::atomic::Ordering::SeqCst),
        1
    );
}

/// The node agent proxies these RPCs for sandbox-manager, volume-manager, and
/// template-manager under its own credential, so its identity must clear each
/// one's TCP allow-list.
fn test_k8s_identity(service: ClientService) -> crate::auth::K8sIdentity {
    crate::auth::K8sIdentity::from_token_review(
        service,
        "credential-client",
        "credential-client-uid",
        "worker-node",
        "worker-node-uid",
    )
    .expect("test Kubernetes caller has Pod and node bindings")
}

#[test]
fn node_agent_is_authorized_on_the_rpcs_it_proxies() {
    for (operation, allow_services) in [
        (
            "ResolveNetworkCredentials",
            ALLOW_RESOLVE_NETWORK_CREDENTIAL_SERVICES,
        ),
        ("ExchangeToken", ALLOW_EXCHANGE_TOKEN_SERVICES),
        ("IssueDek", ALLOW_ISSUE_DEK_SERVICES),
        ("UnwrapDek", ALLOW_UNWRAP_DEK_SERVICES),
    ] {
        CallerResolver::new(UdsPeerAuthorizer::new("space-credential-manager", []))
            .authorize_caller(
                &ResolvedCaller::K8sJwt(test_k8s_identity(ClientService::NodeCredentialAgent)),
                operation,
                &[],
                allow_services,
            )
            .unwrap_or_else(|err| panic!("agent must be authorized for {operation}: {err}"));
    }
}

#[test]
fn build_manager_may_exchange_storage_credentials_only() {
    let resolver = CallerResolver::new(UdsPeerAuthorizer::new("space-credential-manager", []));
    let caller = ResolvedCaller::K8sJwt(test_k8s_identity(ClientService::BuildManager));

    resolver
        .authorize_caller(
            &caller,
            "ExchangeToken",
            ALLOW_EXCHANGE_TOKEN,
            ALLOW_EXCHANGE_TOKEN_SERVICES,
        )
        .expect("build-manager must be able to exchange template-storage credentials");

    for (operation, uds_allow, service_allow) in [
        ("IssueDek", ALLOW_ISSUE_DEK, ALLOW_ISSUE_DEK_SERVICES),
        ("UnwrapDek", ALLOW_UNWRAP_DEK, ALLOW_UNWRAP_DEK_SERVICES),
        ("Encrypt", ALLOW_ENVELOPE, ALLOW_ENVELOPE_SERVICES),
        ("Decrypt", ALLOW_ENVELOPE, ALLOW_ENVELOPE_SERVICES),
    ] {
        let error = resolver
            .authorize_caller(&caller, operation, uds_allow, service_allow)
            .expect_err("build-manager must not receive unrelated credential authority");
        assert_eq!(error.code(), tonic::Code::PermissionDenied, "{operation}");
    }
}

#[test]
fn invalid_durable_storage_configuration_is_a_precondition_failure() {
    for error in [
        StorageExchangeError::InvalidStorageConfiguration,
        StorageExchangeError::Store(StorageVendStoreError::UnsupportedProvider),
    ] {
        assert_eq!(
            storage_exchange_error_to_status(error).code(),
            tonic::Code::FailedPrecondition
        );
    }
}

/// Without a storage vend role the authority wires no authorization store,
/// so every exchange fails closed before any request parsing or issuance.
#[tokio::test]
async fn unconfigured_storage_exchange_fails_closed_before_issuance() {
    let service = CredentialManagerService::new(
        Arc::new(InMemoryKeyWrapper::new()),
        PeerProcessCheckMode::Off,
    );
    let mut request = Request::new(ExchangeTokenRequest::default());
    request
        .extensions_mut()
        .insert(crate::tcp_auth::TcpCallerIdentity::K8sJwt(
            test_k8s_identity(ClientService::BuildManager),
        ));

    let status = service.exchange_token(request).await.unwrap_err();
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    assert!(
        status
            .message()
            .starts_with("storage credential exchange is not configured"),
        "{}",
        status.message()
    );
}

/// Encrypt/Decrypt are the control-plane API's credential CRUD. The agent has
/// no reason to call them, so widening its identity must not reach them: with a
/// mapped identity the allow-list denies it, where the shared static token it
/// presents today would be blanket-allowed.
#[test]
fn node_agent_is_denied_encrypt_and_decrypt() {
    assert_eq!(ALLOW_ENVELOPE_SERVICES, [ClientService::ApiSvc]);
    for operation in ["Encrypt", "Decrypt"] {
        let err = CallerResolver::new(UdsPeerAuthorizer::new("space-credential-manager", []))
            .authorize_caller(
                &ResolvedCaller::K8sJwt(test_k8s_identity(ClientService::NodeCredentialAgent)),
                operation,
                ALLOW_ENVELOPE,
                ALLOW_ENVELOPE_SERVICES,
            )
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
        assert!(err.message().contains("space-credential-manager"));
        assert!(err.message().contains(operation));
    }
}

/// Contracting the caller set after migration removes the compromised-node
/// paths through the shared static token and other node services.
#[test]
fn volume_dek_contract_accepts_only_node_manager() {
    let resolver = CallerResolver::new(UdsPeerAuthorizer::new("space-credential-manager", []));
    for caller in [
        ClientService::SandboxManager,
        ClientService::VolumeManager,
        ClientService::NodeCredentialAgent,
    ] {
        let err = resolver
            .authorize_caller(
                &ResolvedCaller::K8sJwt(test_k8s_identity(caller)),
                "GetOrIssueSandboxVolumeDek",
                ALLOW_VOLUME_DEK,
                ALLOW_VOLUME_DEK_SERVICES,
            )
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }
    resolver
        .authorize_caller(
            &ResolvedCaller::K8sJwt(test_k8s_identity(ClientService::NodeManager)),
            "GetOrIssueSandboxVolumeDek",
            ALLOW_VOLUME_DEK,
            ALLOW_VOLUME_DEK_SERVICES,
        )
        .expect("attested node-manager must be able to request its sandbox's volume DEK");
}

#[tokio::test]
async fn volume_rpcs_reject_plaintext_static_token() {
    let service = CredentialManagerService::new(
        Arc::new(InMemoryKeyWrapper::new()),
        PeerProcessCheckMode::Off,
    );

    let dek_err = service
        .get_or_issue_sandbox_volume_dek(plaintext_request(GetOrIssueSandboxVolumeDekRequest {
            sandbox_id: Uuid::now_v7().to_string(),
            expected_org_id: String::new(),
        }))
        .await
        .unwrap_err();
    assert_eq!(dek_err.code(), tonic::Code::PermissionDenied);
}

/// Transient backend outages surface as retriable UNAVAILABLE, not
/// INTERNAL.
#[test]
fn backend_unavailable_maps_to_unavailable_status() {
    let status = wrap_error_to_status(WrapError::Unavailable("provider outage".into()));
    assert_eq!(status.code(), tonic::Code::Unavailable);
    let status = unwrap_error_to_status(UnwrapError::Unavailable("provider outage".into()));
    assert_eq!(status.code(), tonic::Code::Unavailable);
    let status = wrap_error_to_status(WrapError::Backend("bug".into()));
    assert_eq!(status.code(), tonic::Code::Internal);
}

#[test]
fn invalid_key_errors_map_to_marked_failed_precondition_status() {
    for status in [
        wrap_error_to_status(WrapError::InvalidKey("unknown key".into())),
        unwrap_error_to_status(UnwrapError::InvalidKey("unknown key".into())),
        provision_error_to_status(ProvisionError::Provider(ProviderError::InvalidKey(
            "unknown organization".into(),
        ))),
    ] {
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert!(pplx_space_credmgr_client::status_is_invalid_key_reference(
            &status
        ));
    }
}

#[test]
fn authorization_rejection_is_permission_denied_without_revocation_metadata() {
    for status in [
        wrap_error_to_status(WrapError::AuthorizationRejected("expired grant".into())),
        unwrap_error_to_status(UnwrapError::AuthorizationRejected("expired grant".into())),
        provision_error_to_status(ProvisionError::Provider(
            ProviderError::AuthorizationRejected("expired grant".into()),
        )),
    ] {
        assert_eq!(status.code(), tonic::Code::PermissionDenied);
        assert!(!pplx_space_credmgr_client::status_is_org_kek_access_revoked(&status));
    }
}

/// A request that authenticates as the node credential agent over TCP, which
/// is the only caller IssueIntermediateCertificate accepts.
fn agent_request<T>(message: T) -> Request<T> {
    let mut request = Request::new(message);
    request
        .extensions_mut()
        .insert(crate::tcp_auth::TcpCallerIdentity::K8sJwt(
            test_k8s_identity(ClientService::NodeCredentialAgent),
        ));
    request
}

/// A node CSR, produced the way the node agent will produce one.
fn node_csr() -> Vec<u8> {
    let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).expect("node keypair");
    let mut distinguished_name = rcgen::DistinguishedName::new();
    distinguished_name.push(
        rcgen::DnType::CommonName,
        crate::intermediate_ca::csr::CSR_COMMON_NAME,
    );
    let mut params = rcgen::CertificateParams::default();
    params.distinguished_name = distinguished_name;
    params
        .serialize_request(&key)
        .expect("sign the request")
        .der()
        .to_vec()
}

/// Stands in for the endpoint. The handler's job is authorization, validation,
/// and error mapping — not signing — and `AwsPrivateCaIssuer` needs a live CA.
struct StubIssuer {
    outcome: Result<Vec<u8>, IssueIntermediateError>,
}

#[tonic::async_trait]
impl crate::intermediate_ca::IntermediateCaIssuer for StubIssuer {
    async fn issue(
        &self,
        request: &IntermediateCertificateRequest,
    ) -> Result<Vec<u8>, IssueIntermediateError> {
        match &self.outcome {
            // Echo the offered key so the caller can prove the handler passed
            // the request through rather than inventing one.
            Ok(_) => Ok(request.csr.public_key_der().to_vec()),
            Err(error) => Err(IssueIntermediateError::Unavailable(error.to_string())),
        }
    }
}

fn service_with_issuer(
    outcome: Result<Vec<u8>, IssueIntermediateError>,
) -> CredentialManagerService {
    CredentialManagerService::new(
        Arc::new(InMemoryKeyWrapper::new()),
        PeerProcessCheckMode::Off,
    )
    .with_intermediate_ca_issuer(Arc::new(StubIssuer { outcome }))
}

/// The handler hands the validated request to the backend and returns what it
/// produced, unaltered.
#[tokio::test]
async fn a_valid_request_reaches_the_backend_and_its_certificate_is_returned() {
    let csr = node_csr();
    let response = service_with_issuer(Ok(Vec::new()))
        .issue_intermediate_certificate(agent_request(IssueIntermediateCertificateRequest {
            csr: csr.clone(),
        }))
        .await
        .expect("issuance succeeds")
        .into_inner();

    let parsed = crate::intermediate_ca::csr::NodeCsr::parse(&csr).expect("a valid request");
    assert_eq!(response.certificate_der, parsed.public_key_der());
}

/// The node's refresh loop treats FAILED_PRECONDITION as terminal, so a
/// deployment with no signing backend has to say so with that code rather than
/// with a retryable one — otherwise every node retries forever against a
/// cluster that will never answer.
#[tokio::test]
async fn instance_without_an_issuer_refuses_intermediate_issuance() {
    let err = CredentialManagerService::new(
        Arc::new(InMemoryKeyWrapper::new()),
        PeerProcessCheckMode::Off,
    )
    .issue_intermediate_certificate(agent_request(IssueIntermediateCertificateRequest {
        csr: node_csr(),
    }))
    .await
    .expect_err("no backend configured");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
}

/// The authorization decision this RPC makes is "may this caller hold a CA
/// key". Every other credmgr client must be refused it, including ones
/// allow-listed for the credential RPCs, and refused before the request is
/// parsed so a rejected CSR is indistinguishable from an accepted one.
#[tokio::test]
async fn only_the_node_agent_may_request_an_intermediate() {
    let service = service_with_issuer(Ok(Vec::new()));
    for service_identity in [
        ClientService::SandboxManager,
        ClientService::VolumeManager,
        ClientService::TemplateManager,
        ClientService::BuildManager,
        ClientService::ApiSvc,
    ] {
        for csr in [node_csr(), b"not a csr".to_vec()] {
            let mut request = Request::new(IssueIntermediateCertificateRequest { csr });
            request
                .extensions_mut()
                .insert(crate::tcp_auth::TcpCallerIdentity::K8sJwt(
                    test_k8s_identity(service_identity),
                ));
            let err = service
                .issue_intermediate_certificate(request)
                .await
                .expect_err("only the node agent is authorized");
            assert_eq!(
                err.code(),
                tonic::Code::PermissionDenied,
                "{service_identity} was allowed to request an intermediate"
            );
        }
    }
}

/// Both of these used to return before the metric was recorded, so
/// `not_configured` and `invalid_csr` were in the documented contract and
/// never emitted.
#[tokio::test]
async fn precondition_and_validation_failures_are_metered() {
    let no_backend = CredentialManagerService::new(
        Arc::new(InMemoryKeyWrapper::new()),
        PeerProcessCheckMode::Off,
    )
    .issue_intermediate(IssueIntermediateCertificateRequest { csr: node_csr() })
    .await;
    assert_eq!(intermediate_result_tag(&no_backend), "not_configured");

    let rejected = service_with_issuer(Ok(Vec::new()))
        .issue_intermediate(IssueIntermediateCertificateRequest {
            csr: b"not der".to_vec(),
        })
        .await;
    assert_eq!(intermediate_result_tag(&rejected), "invalid_csr");
}

/// The retryable/terminal split the node's refresh loop backs off on. A
/// variant landing in the wrong group either spins forever against a request
/// that will never be accepted, or gives up on a throttle that would clear.
#[test]
fn issuance_errors_map_to_the_status_the_refresh_loop_expects() {
    use std::time::Duration;

    for (error, expected) in [
        (
            IssueIntermediateError::NotConfigured,
            tonic::Code::FailedPrecondition,
        ),
        (
            IssueIntermediateError::Csr(crate::intermediate_ca::csr::CsrError::Malformed),
            tonic::Code::InvalidArgument,
        ),
        (
            IssueIntermediateError::RequestBuild("validity".into()),
            tonic::Code::Internal,
        ),
        (
            IssueIntermediateError::Rejected("bad template".into()),
            tonic::Code::Internal,
        ),
        (
            IssueIntermediateError::Unavailable("throttled".into()),
            tonic::Code::Unavailable,
        ),
        (
            IssueIntermediateError::IssuanceTimedOut {
                timeout: Duration::from_secs(20),
            },
            tonic::Code::Unavailable,
        ),
        (
            IssueIntermediateError::MalformedCertificate("garbage".into()),
            tonic::Code::Unavailable,
        ),
    ] {
        assert_eq!(
            intermediate_error_to_status(error).code(),
            expected,
            "wrong retry posture"
        );
    }
}

/// Every mapper on this authority answers a lost lock race `Unavailable`.
///
/// This is the boundary `space_creds` was held out of the role-level
/// `lock_timeout` for: while these all returned `Internal`, bounding the
/// credential lane would have turned a slow success into a fast
/// permanent-looking failure for a caller that had no reason to retry. The
/// assertion is on the code the caller sees, since that is what the exclusion
/// was reasoned about.
#[tokio::test(flavor = "current_thread")]
async fn every_credential_mapper_answers_a_lost_lock_race_as_unavailable()
-> pplx_space_test_env::TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_raw_pg_test_db().await? else {
        return Ok(());
    };

    for sqlstate in ["55P03", "40001", "40P01"] {
        let raise = || pplx_space_db_test_fixtures::raise_sqlstate(&db.pool, sqlstate);

        assert_eq!(
            kek_store_error_to_status(KekStoreError::Query(raise().await)).code(),
            tonic::Code::Unavailable,
            "{sqlstate} on a KEK lookup"
        );
        assert_eq!(
            provision_error_to_status(ProvisionError::Db(raise().await)).code(),
            tonic::Code::Unavailable,
            "{sqlstate} while provisioning a KEK"
        );
        assert_eq!(
            volume_wrap_store_error_to_status(
                crate::volume_wrap_store::VolumeWrapStoreError::Query(raise().await)
            )
            .code(),
            tonic::Code::Unavailable,
            "{sqlstate} on a volume-wrap lookup"
        );
        assert_eq!(
            credential_store_error_to_status(
                "sandbox owner lookup failed",
                crate::credential_store::CredentialStoreError::Query(raise().await),
            )
            .code(),
            tonic::Code::Unavailable,
            "{sqlstate} on a credential lookup"
        );
    }

    // A real failure keeps `Internal`, and a corrupt KEK row — which is not a
    // `Query` at all — must not be swept into the retryable branch.
    assert_eq!(
        kek_store_error_to_status(KekStoreError::Query(
            pplx_space_db_test_fixtures::raise_sqlstate(&db.pool, "23505").await
        ))
        .code(),
        tonic::Code::Internal
    );
    assert_eq!(
        kek_store_error_to_status(KekStoreError::InvalidProvider {
            cmk_id: Uuid::now_v7(),
            message: "bogus".to_string(),
        })
        .code(),
        tonic::Code::Internal
    );

    // The provider-stated classifications are untouched: the contention branch
    // sits on the `Db` arm only.
    assert_eq!(
        provision_error_to_status(ProvisionError::LostRaceWithoutWinner).code(),
        tonic::Code::Unavailable
    );
    assert_eq!(
        provision_error_to_status(ProvisionError::Provider(ProviderError::Malformed(
            "bad key blob".to_string()
        )))
        .code(),
        tonic::Code::Internal
    );
    Ok(())
}

#[test]
fn volume_principal_has_only_transfer_credential_permissions() {
    let service = CredentialManagerService::new(
        Arc::new(InMemoryKeyWrapper::new()),
        PeerProcessCheckMode::Off,
    );
    let caller = ResolvedCaller::UdsPeer(PeerIdentity::new(users::SPACE_VOLMGR.uid, 951, Some(1)));
    for (operation, allow, expected) in [
        ("ExchangeToken", ALLOW_EXCHANGE_TOKEN, true),
        ("IssueDek", ALLOW_ISSUE_DEK, true),
        ("UnwrapDek", ALLOW_UNWRAP_DEK, true),
        ("Encrypt", ALLOW_ENVELOPE, false),
        ("Decrypt", ALLOW_ENVELOPE, false),
        ("GetOrIssueSandboxVolumeDek", ALLOW_VOLUME_DEK, false),
        (
            "IssueIntermediateCertificate",
            ALLOW_ISSUE_INTERMEDIATE,
            false,
        ),
    ] {
        let result = service
            .caller_resolver
            .authorize_caller(&caller, operation, allow, &[]);
        if expected {
            result.unwrap();
        } else {
            assert_eq!(
                result.unwrap_err().code(),
                tonic::Code::PermissionDenied,
                "{operation}"
            );
        }
    }
}
