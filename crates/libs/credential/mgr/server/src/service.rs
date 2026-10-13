//! CredentialManager gRPC service implementation.

use pplx_space_common_core::types::UserId;

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use aws_credential_types::provider::ProvideCredentials;
use pplx_space_common_svc::core::host::linux::identities::{user_names, users};
use pplx_space_common_svc::core::host::linux::paths;
use pplx_space_common_svc::core::types::{
    KekProvider, MasterKeyIdentifier, OrgId, SandboxId, volume_wrap_aad,
};
use pplx_space_common_svc::sql::contention;
use pplx_space_common_svc::{
    PeerProcessCheckMode, UdsPeerAccount, UdsPeerAuthorizer, UdsPeerPrincipal, capability,
};
use tonic::{Request, Response, Status};

use crate::auth::{CallerResolver, ClientService, ResolvedCaller};
use crate::credential_injection_authority::CredentialInjectionAuthority;
use crate::credential_injection_rpc::CredentialInjectionRpc;
use crate::credential_store::CredentialStore;
use crate::envelope;
use crate::intermediate_ca::csr::NodeCsr;
use crate::intermediate_ca::{
    self, IntermediateCaIssuer, IntermediateCertificateRequest, IssueIntermediateError,
};
use crate::intermediate_key_broker::ProviderError;
use crate::kek_provisioning::{KekProvisioner, ProvisionError};
use crate::kek_store::{KekRecord, KekStore, KekStoreError};
use crate::key_wrapper::{DekKeySpec, GenerateDekOutput, KeyWrapper, UnwrapError, WrapError};
use crate::proto::credential_manager_server::CredentialManager;
use crate::proto::{
    AwsCredentials, DecryptRequest, DecryptResponse, DekSpec, EncryptRequest, EncryptResponse,
    ExchangeTokenRequest, ExchangeTokenResponse, GetOrIssueSandboxVolumeDekRequest,
    GetOrIssueSandboxVolumeDekResponse, IssueDekRequest, IssueDekResponse,
    IssueIntermediateCertificateRequest, IssueIntermediateCertificateResponse,
    ResolveNetworkCredentialsRequest, ResolveNetworkCredentialsResponse, UnwrapDekRequest,
    UnwrapDekResponse, exchange_token_response,
};
use crate::storage_exchange::{
    AuthorizedExchange, StorageExchangeError, VendRoleArn, authorize_exchange,
};
use crate::storage_vend_store::{StorageVendStore, StorageVendStoreError};
use crate::volume_wrap_store::VolumeWrapStore;

const SPACE_SBXMGR_ACCOUNT: &str = user_names::SPACE_SBXMGR;
const SPACE_TMPLMGR_ACCOUNT: &str = user_names::SPACE_TMPLMGR;

// ExchangeToken deliberately excludes root: every issuance needs a service
// actor that can be carried into the authorization matrix and audit record.
const ALLOW_EXCHANGE_TOKEN: &[UdsPeerPrincipal] = &[
    UdsPeerPrincipal::Service(SPACE_SBXMGR_ACCOUNT),
    UdsPeerPrincipal::Service(SPACE_TMPLMGR_ACCOUNT),
    UdsPeerPrincipal::Service(user_names::SPACE_VOLMGR),
];
/// Same callers as `ExchangeToken` for now: sandbox-manager and
/// template-manager are the two on-node services that own data they
/// might want to envelope-encrypt.
const ALLOW_ISSUE_DEK: &[UdsPeerPrincipal] = &[
    UdsPeerPrincipal::Root,
    UdsPeerPrincipal::Service(SPACE_SBXMGR_ACCOUNT),
    UdsPeerPrincipal::Service(SPACE_TMPLMGR_ACCOUNT),
    UdsPeerPrincipal::Service(user_names::SPACE_VOLMGR),
];
const ALLOW_UNWRAP_DEK: &[UdsPeerPrincipal] = &[
    UdsPeerPrincipal::Root,
    UdsPeerPrincipal::Service(SPACE_SBXMGR_ACCOUNT),
    UdsPeerPrincipal::Service(SPACE_TMPLMGR_ACCOUNT),
    UdsPeerPrincipal::Service(user_names::SPACE_VOLMGR),
];

/// TCP service identities mirroring the UDS allow-lists above. On the
/// control-plane deployment vol-mgr presents its own projected ServiceAccount
/// token (resolved to a `ClientService` via TokenReview) rather than sharing
/// sandbox-manager's Linux uid, so it appears here explicitly even though it
/// has no separate UDS principal.
///
/// The node-local agent ([`crate::proxy`]) is in these lists for a different
/// reason: it re-originates every node-local caller's RPC under its own
/// credential, so the authority sees the agent's identity and never the
/// original caller's. ExchangeToken accepts the agent only because that RPC
/// separately validates the locally authenticated Volume/Template actor claim;
/// Sandbox Manager is not in its storage authorization matrix. Build Manager
/// reaches the authority directly and receives only token exchange. The
/// DEK-operation lists remain the union of the node services the agent fronts.
const ALLOW_RESOLVE_NETWORK_CREDENTIAL_SERVICES: &[ClientService] =
    &[ClientService::NodeCredentialAgent];
const ALLOW_EXCHANGE_TOKEN_SERVICES: &[ClientService] = &[
    ClientService::VolumeManager,
    ClientService::TemplateManager,
    ClientService::BuildManager,
    ClientService::NodeCredentialAgent,
];
const ALLOW_ISSUE_DEK_SERVICES: &[ClientService] = &[
    ClientService::SandboxManager,
    ClientService::VolumeManager,
    ClientService::TemplateManager,
    ClientService::NodeCredentialAgent,
];
const ALLOW_UNWRAP_DEK_SERVICES: &[ClientService] = ALLOW_ISSUE_DEK_SERVICES;

/// Encrypt/Decrypt serve the control-plane API's credential CRUD. api-svc
/// is never an on-node UDS peer, so the UDS list is root-only (debugging).
const ALLOW_ENVELOPE: &[UdsPeerPrincipal] = &[UdsPeerPrincipal::Root];
const ALLOW_ENVELOPE_SERVICES: &[ClientService] = &[ClientService::ApiSvc];

/// GetOrIssueSandboxVolumeDek serves the node-local block-volume flows.
/// After the direct-call migration, only node-manager may access a sandbox
/// volume DEK. Root remains available on the local authority socket for
/// operator debugging.
const ALLOW_VOLUME_DEK: &[UdsPeerPrincipal] = &[UdsPeerPrincipal::Root];
const ALLOW_VOLUME_DEK_SERVICES: &[ClientService] = &[ClientService::NodeManager];

/// IssueIntermediateCertificate is the node credential agent's RPC and no one
/// else's. The UDS list is root-only for debugging; the agent reaches the
/// authority over TCP.
const ALLOW_ISSUE_INTERMEDIATE: &[UdsPeerPrincipal] = &[UdsPeerPrincipal::Root];
const ALLOW_ISSUE_INTERMEDIATE_SERVICES: &[ClientService] = &[ClientService::NodeCredentialAgent];
/// Cap on `EncryptRequest.plaintext`. Encrypt is for credential values and
/// other small blobs; bulk data goes through IssueDek + the streaming
/// pipeline, so anything larger here is a caller bug.
const MAX_ENCRYPT_PLAINTEXT_LEN: usize = 64 * 1024;
/// Decrypt-side cap: sealed form adds a version byte, a 12-byte nonce, and
/// a 16-byte GCM tag on top of the plaintext cap.
const MAX_DECRYPT_CIPHERTEXT_LEN: usize = MAX_ENCRYPT_PLAINTEXT_LEN + 1 + 12 + 16;

/// Token-type URI advertised on the issued AWS-credentials variant.
/// Duplicated from `pplx-space-credmgr-client` (a stable public OAuth
/// URI) to keep this crate from depending on the client just for one
/// string — the dep direction would invert the usual server→client one.
const AWS_CREDENTIALS_TOKEN_TYPE: &str = "urn:ietf:params:oauth:token-type:aws-credentials";

#[derive(Clone)]
pub struct CredentialManagerService {
    caller_resolver: CallerResolver,
    /// Validated once at authority startup. Keeping this out of the request
    /// path prevents a Ready authority from discovering a malformed role only
    /// after callers depend on it.
    storage_vend_role: Option<VendRoleArn>,
    /// Pluggable backend that wraps DEKs under per-org KEKs. The
    /// concrete impl (external provider or explicit in-memory stub)
    /// is chosen by the binary at startup so handlers stay backend-
    /// agnostic.
    key_wrapper: Arc<dyn KeyWrapper>,
    /// cmk_id → KEK row resolution for Encrypt/Decrypt. Present only on
    /// instances with database access (the control-plane deployment);
    /// node-local instances refuse those RPCs with FAILED_PRECONDITION.
    kek_store: Option<Arc<dyn KekStore>>,
    /// Shared authority pool used for TCP caller node-registration checks.
    pub(crate) database_pool: Option<sqlx::PgPool>,
    /// Sandbox-owner reads for volume encryption.
    /// Present only on instances with database access.
    credential_store: Option<Arc<dyn CredentialStore>>,
    /// Tenant, placement, template-owner, and per-org role reads for scoped
    /// storage credential exchange. Present only on the authority.
    storage_vend_store: Option<Arc<dyn StorageVendStore>>,
    /// Per-sandbox volume DEK wrap persistence for
    /// GetOrIssueSandboxVolumeDek. Present only on instances with database
    /// access (the control-plane deployment).
    volume_wrap_store: Option<Arc<dyn VolumeWrapStore>>,
    /// Lazy KEK provisioning for orgs with no active KEK, used by Encrypt.
    /// Present only when this deployment's platform-default provider
    /// requires an authority-side round-trip (intermediate-key-broker); absent
    /// means "no active KEK" stays a caller-visible precondition failure.
    kek_provisioner: Option<Arc<dyn KekProvisioner>>,
    /// Signs node intermediate CA certificates. Present only on the
    /// control-plane deployment; absent means IssueIntermediateCertificate is
    /// a caller-visible precondition failure.
    intermediate_ca_issuer: Option<Arc<dyn IntermediateCaIssuer>>,
    /// Reference-based resolution endpoint. Present only on instances with
    /// access to durable credential-injection state.
    credential_injection_rpc: Option<CredentialInjectionRpc>,
}

impl CredentialManagerService {
    /// `peer_process_check_mode` is injected rather than read from the
    /// environment here, so construction stays deterministic in tests and
    /// the binary can fail startup on an invalid value
    /// (see [`PeerProcessCheckMode::from_env`]).
    pub fn new(
        key_wrapper: Arc<dyn KeyWrapper>,
        peer_process_check_mode: PeerProcessCheckMode,
    ) -> Self {
        // Defense in depth on top of `SO_PEERCRED`-based uid matching:
        //
        // 1. **Binary identity** (`require_binary`): the peer's
        //    `/proc/<pid>/exe` must resolve to a known SPACE service
        //    binary. Catches the case where another process owned by
        //    the same Linux user manages to connect to the credmgr
        //    socket — e.g., if `space-sbxmgr` were ever made to spawn
        //    a helper, or if a developer runs an ad-hoc tool as
        //    `space-sbxmgr`. The allowlists cover both `/opt/space/bin`
        //    (Helm) and `/space/bin` (systemd) install layouts.
        // 2. **Forbidden capabilities** (`forbid_capabilities`): the
        //    peer must NOT hold `CAP_SYS_PTRACE`. sandbox-manager and
        //    template-manager have no legitimate need for ptrace, and
        //    a peer that holds it is in a position to attach to /
        //    inspect arbitrary processes (including credmgr's own
        //    keyspace). This is a cheap negative invariant; if it
        //    ever breaks legitimately, swap it for a tighter
        //    positive check.
        //
        // credmgr is the only service opted into the process-inspection
        // layer today. The layer is controlled by the
        // `SPACE_UDS_PEER_PROCESS_CHECK` env switch (`off` | `log-only` |
        // `enforce`, injected by the binary at startup) and defaults to
        // `off` so it never breaks local dev or non-Linux hosts that can't
        // satisfy the `/proc` preconditions; a set-but-invalid value fails
        // startup. No deployment sets the switch, so these declarations are
        // inert until an environment opts in.
        Self {
            caller_resolver: CallerResolver::new(
                UdsPeerAuthorizer::new(
                    "space-credential-manager",
                    [
                        UdsPeerAccount::with_linux_user(users::SPACE_SBXMGR)
                            .require_binary(paths::SPACE_SBXMGR_UID_BINARIES)
                            .forbid_capabilities(capability::CAP_SYS_PTRACE),
                        UdsPeerAccount::with_linux_user(users::SPACE_VOLMGR)
                            .require_binary(paths::SPACE_VOLMGR_SVC_BINARIES)
                            .forbid_capabilities(capability::CAP_SYS_PTRACE),
                        UdsPeerAccount::with_linux_user(users::SPACE_TMPLMGR)
                            .require_binary(paths::SPACE_TMPLMGR_SVC_BINARIES)
                            .forbid_capabilities(capability::CAP_SYS_PTRACE),
                    ],
                )
                .with_process_check_mode(peer_process_check_mode),
            ),
            storage_vend_role: None,
            key_wrapper,
            kek_store: None,
            database_pool: None,
            credential_store: None,
            storage_vend_store: None,
            volume_wrap_store: None,
            kek_provisioner: None,
            intermediate_ca_issuer: None,
            credential_injection_rpc: None,
        }
    }

    /// Configure the startup-validated platform storage vending role.
    pub(crate) fn with_storage_vend_role(mut self, role: VendRoleArn) -> Self {
        self.storage_vend_role = Some(role);
        self
    }

    /// Enable reference-based network-credential resolution.
    pub fn with_credential_injection_authority(
        mut self,
        authority: CredentialInjectionAuthority,
    ) -> Self {
        self.credential_injection_rpc = Some(CredentialInjectionRpc::new(authority));
        self
    }

    /// Enable RPCs that resolve a sandbox's owning organization.
    pub fn with_credential_store(mut self, store: Arc<dyn CredentialStore>) -> Self {
        self.credential_store = Some(store);
        self
    }

    /// Enable resource- and tenant-scoped storage credential exchange.
    pub fn with_storage_vend_store(mut self, store: Arc<dyn StorageVendStore>) -> Self {
        self.storage_vend_store = Some(store);
        self
    }

    /// Enable the Encrypt/Decrypt RPCs by providing cmk_id resolution.
    pub fn with_kek_store(mut self, kek_store: Arc<dyn KekStore>) -> Self {
        self.kek_store = Some(kek_store);
        self
    }

    /// Enable GetOrIssueSandboxVolumeDek by providing per-sandbox volume
    /// wrap persistence.
    pub fn with_volume_wrap_store(mut self, store: Arc<dyn VolumeWrapStore>) -> Self {
        self.volume_wrap_store = Some(store);
        self
    }

    /// Enable lazy KEK provisioning on Encrypt for orgs with no active KEK.
    pub fn with_kek_provisioner(mut self, provisioner: Arc<dyn KekProvisioner>) -> Self {
        self.kek_provisioner = Some(provisioner);
        self
    }

    /// Enable IssueIntermediateCertificate by providing a signing backend.
    pub fn with_intermediate_ca_issuer(mut self, issuer: Arc<dyn IntermediateCaIssuer>) -> Self {
        self.intermediate_ca_issuer = Some(issuer);
        self
    }

    /// The org's active KEK, lazily provisioned when this deployment owns
    /// provisioning (intermediate-key-broker platform default). Without a
    /// provisioner, an unprovisioned org is the caller's precondition
    /// failure it has always been.
    async fn active_or_provisioned_kek(
        &self,
        org: OrgId,
        requesting_sandbox: Option<SandboxId>,
    ) -> Result<KekRecord, Status> {
        if let Some(kek) = self
            .kek_store()?
            .active_kek_for_org(org)
            .await
            .map_err(kek_store_error_to_status)?
        {
            return Ok(kek);
        }
        let Some(provisioner) = &self.kek_provisioner else {
            return Err(Status::failed_precondition(
                "the organization has no active KEK; provision one before encrypting",
            ));
        };
        provisioner
            .provision_active_kek(org, requesting_sandbox)
            .await
            .map_err(provision_error_to_status)
    }

    fn authorize_caller<T>(
        &self,
        request: &Request<T>,
        operation: &'static str,
        allow_uds: &[UdsPeerPrincipal],
        allow_services: &[ClientService],
    ) -> Result<ResolvedCaller, Status> {
        // TCP middleware has already placed a TokenReview result in request
        // extensions. This unifies it with UDS peer identity and applies the
        // operation allow-list without inspecting the request body.
        let caller = self.caller_resolver.resolve(request)?;
        self.caller_resolver
            .authorize_caller(&caller, operation, allow_uds, allow_services)?;
        Ok(caller)
    }

    fn authorize_volume_caller<T>(
        &self,
        request: &Request<T>,
        operation: &'static str,
    ) -> Result<(), Status> {
        let caller = self.authorize_caller(
            request,
            operation,
            ALLOW_VOLUME_DEK,
            ALLOW_VOLUME_DEK_SERVICES,
        )?;
        if matches!(caller, ResolvedCaller::Plaintext) {
            return Err(Status::permission_denied(format!(
                "{operation} requires a mapped node-manager identity"
            )));
        }
        Ok(())
    }
}

#[tonic::async_trait]
impl CredentialManager for CredentialManagerService {
    async fn resolve_network_credentials(
        &self,
        request: Request<ResolveNetworkCredentialsRequest>,
    ) -> Result<Response<ResolveNetworkCredentialsResponse>, Status> {
        let caller = self.authorize_caller(
            &request,
            "ResolveNetworkCredentials",
            &[],
            ALLOW_RESOLVE_NETWORK_CREDENTIAL_SERVICES,
        )?;
        let response = self
            .credential_injection_rpc()?
            .resolve_network_credentials(&caller, request.into_inner())
            .await?;
        Ok(Response::new(response))
    }

    /// RFC 8693 storage token exchange. The authority validates the complete
    /// contract, resolves tenant and placement from durable state, and assumes
    /// only the configured storage role with an exact-resource session policy.
    async fn exchange_token(
        &self,
        request: Request<ExchangeTokenRequest>,
    ) -> Result<Response<ExchangeTokenResponse>, Status> {
        let caller = match self.caller_resolver.resolve(&request) {
            Ok(caller) => caller,
            Err(status) => {
                audit_exchange_caller_denial(None, status.code());
                return Err(status);
            }
        };
        if let Err(status) = self.caller_resolver.authorize_caller(
            &caller,
            "ExchangeToken",
            ALLOW_EXCHANGE_TOKEN,
            ALLOW_EXCHANGE_TOKEN_SERVICES,
        ) {
            audit_exchange_caller_denial(Some(&caller), status.code());
            return Err(status);
        }
        let request = request.into_inner();
        let authorized =
            match authorize_exchange(self.storage_vend_store()?.as_ref(), &caller, request).await {
                Ok(authorized) => authorized,
                Err(error) => {
                    audit_exchange_denial(&caller, &error);
                    return Err(storage_exchange_error_to_status(error));
                }
            };
        let role = match authorized.vend_role(self.storage_vend_role.as_ref()) {
            Ok(role) => role,
            Err(error) => {
                audit_exchange_denial(&caller, &error);
                return Err(storage_exchange_error_to_status(error));
            }
        };
        let policy = match authorized.session_policy() {
            Ok(policy) => policy,
            Err(error) => {
                audit_exchange_denial(&caller, &error);
                return Err(storage_exchange_error_to_status(error));
            }
        };
        let aws_credentials =
            match assume_s3_vend_role(role.as_str(), &policy, authorized.ttl_seconds).await {
                Ok(credentials) => credentials,
                Err(status) => {
                    audit_exchange_failure(&authorized, status.code());
                    return Err(status);
                }
            };
        let expires_in = match credential_expires_in(&aws_credentials) {
            Ok(expires_in) => expires_in,
            Err(status) => {
                audit_exchange_failure(&authorized, status.code());
                return Err(status);
            }
        };
        audit_exchange_issuance(&authorized, expires_in);

        Ok(Response::new(ExchangeTokenResponse {
            issued_token_type: AWS_CREDENTIALS_TOKEN_TYPE.to_string(),
            token_type: "N_A".to_string(),
            expires_in: Some(expires_in),
            scope: Some(authorized.actions.effective_scope()),
            refresh_token: None,
            payload: Some(exchange_token_response::Payload::AwsCredentials(
                aws_credentials,
            )),
        }))
    }

    async fn issue_dek(
        &self,
        request: Request<IssueDekRequest>,
    ) -> Result<Response<IssueDekResponse>, Status> {
        self.authorize_caller(
            &request,
            "IssueDek",
            ALLOW_ISSUE_DEK,
            ALLOW_ISSUE_DEK_SERVICES,
        )?;
        let req = request.into_inner();

        let org = parse_organization_id(&req.organization_id)?;
        let provider: KekProvider = req
            .provider
            .parse()
            .map_err(|e| Status::invalid_argument(format!("provider: {e}")))?;
        let master_key: MasterKeyIdentifier = req
            .master_key_identifier
            .parse()
            .map_err(|e| Status::invalid_argument(format!("master_key_identifier: {e}")))?;
        let spec = dek_spec_to_key_spec(req.spec);

        let GenerateDekOutput {
            plaintext_dek,
            wrapped_dek,
        } = self
            .key_wrapper
            .generate_and_wrap_dek(org, None, provider, &master_key, spec, &req.aad)
            .await
            .map_err(wrap_error_to_status)?;

        Ok(Response::new(IssueDekResponse {
            plaintext_dek,
            wrapped_dek,
        }))
    }

    async fn unwrap_dek(
        &self,
        request: Request<UnwrapDekRequest>,
    ) -> Result<Response<UnwrapDekResponse>, Status> {
        self.authorize_caller(
            &request,
            "UnwrapDek",
            ALLOW_UNWRAP_DEK,
            ALLOW_UNWRAP_DEK_SERVICES,
        )?;
        let req = request.into_inner();

        let org = parse_organization_id(&req.organization_id)?;
        let provider: KekProvider = req
            .provider
            .parse()
            .map_err(|e| Status::invalid_argument(format!("provider: {e}")))?;
        let master_key: MasterKeyIdentifier = req
            .master_key_identifier
            .parse()
            .map_err(|e| Status::invalid_argument(format!("master_key_identifier: {e}")))?;

        let plaintext_dek = self
            .key_wrapper
            .unwrap_dek(org, None, provider, &master_key, &req.wrapped_dek, &req.aad)
            .await
            .map_err(unwrap_error_to_status)?;

        Ok(Response::new(UnwrapDekResponse { plaintext_dek }))
    }

    async fn encrypt(
        &self,
        request: Request<EncryptRequest>,
    ) -> Result<Response<EncryptResponse>, Status> {
        self.authorize_caller(&request, "Encrypt", ALLOW_ENVELOPE, ALLOW_ENVELOPE_SERVICES)?;
        let req = request.into_inner();

        let (org, user_id) = parse_org_and_user(&req.organization_id, req.user_id.as_deref())?;
        if req.plaintext.len() > MAX_ENCRYPT_PLAINTEXT_LEN {
            return Err(Status::invalid_argument(format!(
                "plaintext exceeds the {MAX_ENCRYPT_PLAINTEXT_LEN}-byte Encrypt cap; \
                 use IssueDek and the streaming pipeline for bulk data"
            )));
        }
        if req.purpose.is_empty() {
            return Err(Status::invalid_argument(
                "purpose is required (scopes the seal so Encrypt is not a general oracle)",
            ));
        }

        // Callers never name a KEK; the org→KEK mapping (and, for
        // intermediate-key-broker deployments, lazy provisioning) lives here.
        let kek = self.active_or_provisioned_kek(org, None).await?;

        let aad = effective_aad(org, user_id, &req.purpose, &req.aad)?;
        let GenerateDekOutput {
            plaintext_dek,
            wrapped_dek,
        } = self
            .key_wrapper
            .generate_and_wrap_dek(
                org,
                None,
                kek.provider,
                &kek.key_identifier,
                DekKeySpec::Aes256,
                &aad,
            )
            .await
            .map_err(wrap_error_to_status)?;
        let dek = dek_array(plaintext_dek)?;
        let ciphertext = envelope::seal(&dek, &req.plaintext, &aad);

        Ok(Response::new(EncryptResponse {
            ciphertext,
            wrapped_dek: envelope::encode_wrapped_dek(kek.cmk_id, &wrapped_dek),
        }))
    }

    async fn decrypt(
        &self,
        request: Request<DecryptRequest>,
    ) -> Result<Response<DecryptResponse>, Status> {
        self.authorize_caller(&request, "Decrypt", ALLOW_ENVELOPE, ALLOW_ENVELOPE_SERVICES)?;
        let req = request.into_inner();

        let (org, user_id) = parse_org_and_user(&req.organization_id, req.user_id.as_deref())?;
        if req.ciphertext.len() > MAX_DECRYPT_CIPHERTEXT_LEN {
            return Err(Status::invalid_argument(
                "ciphertext exceeds the Decrypt cap; this was not produced by Encrypt",
            ));
        }

        // The wrapping KEK is named inside the wrapped-DEK envelope, never
        // by the caller. The (cmk_id, org) lookup verifies the row belongs
        // to the asserted org; retired KEKs still decrypt so rotation
        // cannot strand existing rows.
        let (cmk_id, backend_wrap) = envelope::decode_wrapped_dek(&req.wrapped_dek)
            .map_err(|e| Status::invalid_argument(format!("wrapped_dek: {e}")))?;
        let kek = self
            .kek_store()?
            .kek_for_org(cmk_id, org)
            .await
            .map_err(kek_store_error_to_status)?
            .ok_or_else(|| {
                Status::permission_denied(
                    "the wrapped DEK does not belong to a KEK of this organization",
                )
            })?;

        let aad = effective_aad(org, user_id, &req.purpose, &req.aad)?;
        let plaintext_dek = self
            .key_wrapper
            .unwrap_dek(
                org,
                None,
                kek.provider,
                &kek.key_identifier,
                backend_wrap,
                &aad,
            )
            .await
            .map_err(unwrap_error_to_status)?;
        let dek = dek_array(plaintext_dek)?;
        let plaintext = envelope::open(&dek, &req.ciphertext, &aad).map_err(|e| match e {
            envelope::OpenError::AuthFailed => Status::permission_denied(
                "authentication failed (AAD mismatch or tampered ciphertext)",
            ),
            envelope::OpenError::Malformed | envelope::OpenError::UnsupportedVersion(_) => {
                Status::invalid_argument(e.to_string())
            }
        })?;

        Ok(Response::new(DecryptResponse { plaintext }))
    }

    async fn get_or_issue_sandbox_volume_dek(
        &self,
        request: Request<GetOrIssueSandboxVolumeDekRequest>,
    ) -> Result<Response<GetOrIssueSandboxVolumeDekResponse>, Status> {
        self.authorize_volume_caller(&request, "GetOrIssueSandboxVolumeDek")?;
        let req = request.into_inner();
        let sandbox_id: SandboxId = req
            .sandbox_id
            .parse()
            .map_err(|e| Status::invalid_argument(format!("sandbox_id: {e}")))?;

        // The caller named only the sandbox. credmgr resolves the sandbox's
        // organization, the org's active KEK, and the stored per-sandbox wrap
        // itself — the whole KEK→DEK chain stays behind this service.
        let owner = self
            .credential_store()?
            .sandbox_owner(sandbox_id)
            .await
            .map_err(|e| credential_store_error_to_status("sandbox owner lookup failed", e))?
            .ok_or_else(|| Status::not_found("sandbox not found"))?;
        let org = owner.org_id;

        // Caller-asserted owner cross-check, before any wrap is minted or
        // unwrapped: the wrap below is AAD-bound to (org, sandbox) and
        // first-writer-wins, so a caller acting on a stale ownership view
        // (a warm-pool claim racing the ownership transfer) would otherwise
        // persist a wrap the real owner can never unwrap — a permanently
        // undecryptable sandbox volume, not a retryable error.
        if !req.expected_org_id.trim().is_empty() {
            let expected: OrgId = req
                .expected_org_id
                .parse()
                .map_err(|e| Status::invalid_argument(format!("expected_org_id: {e}")))?;
            if expected != org {
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    expected_org_id = %expected,
                    "volume DEK refused: sandbox is not owned by the org the caller expected"
                );
                return Err(Status::failed_precondition(
                    "sandbox is not owned by the organization the caller expected",
                ));
            }
        }
        let aad = volume_wrap_aad(org, &sandbox_id);

        // Already issued: load the stored wrap and unwrap it — allowed
        // regardless of the org's *current* byok_enabled. The sandbox's
        // on-disk volumes are already LUKS with no plaintext fallback, so the
        // DEK must outlive enrollment: refusing here would permanently strand
        // restore/start of an already-encrypted sandbox after BYOK is toggled
        // off. Every volume for the sandbox shares this one DEK.
        if let Some(existing) = self
            .volume_wrap_store()?
            .get(sandbox_id)
            .await
            .map_err(volume_wrap_store_error_to_status)?
        {
            // Ownership moved after the wrap was minted (a claim raced a
            // transfer, or the sandbox changed org): the stored wrap is
            // AAD-bound to the OLD (org, sandbox) pair, so unwrapping under
            // the current owner can never succeed. Refuse permanently and
            // say why, instead of failing deep in KEK resolution with an
            // Internal that callers' retry taxonomies read as transient.
            if existing.org_id != owner.org_id {
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    "volume DEK refused: stored wrap is bound to a different org than the \
                     sandbox's current owner"
                );
                return Err(Status::failed_precondition(
                    "sandbox volume DEK is bound to a different organization than the \
                     sandbox's current owner; the volume can no longer be decrypted",
                ));
            }
            let plaintext_dek = self
                .unwrap_volume_wrap(org, sandbox_id, &existing, &aad)
                .await?;
            return Ok(Response::new(GetOrIssueSandboxVolumeDekResponse {
                plaintext_dek,
            }));
        }

        // First volume for this sandbox = a *new* encryption. Only minting a
        // fresh DEK requires the org to currently have BYOK enabled: defense in
        // depth behind sandbox-manager, which stamps `volume_encryption` at
        // create from the org's enrollment, but vol-mgr can call this directly.
        // Key on `byok_enabled`, not KEK presence — an org may already have a
        // KEK for credential encryption without being a BYOK org, and
        // dashboard-toggling BYOK off must stop *new* volume encryption even if
        // a key row is left behind.
        if !owner.byok_enabled {
            return Err(Status::failed_precondition(
                "sandbox's organization does not have BYOK enabled",
            ));
        }

        // Mint a fresh DEK under the org's active KEK, lazily provisioning one
        // for a BYOK org that has none when this authority owns provisioning
        // (an authority-owned provisioner is wired). Reusing the same
        // resolve-or-provision path Encrypt uses makes a first sandbox volume
        // self-sufficient: it enrolls the org on first use instead of requiring
        // a credential Encrypt to provision the KEK beforehand. Without a
        // provisioner an unprovisioned org stays the precondition failure it
        // has always been.
        let kek = self
            .active_or_provisioned_kek(org, Some(sandbox_id))
            .await?;
        let GenerateDekOutput {
            plaintext_dek,
            wrapped_dek,
        } = self
            .key_wrapper
            .generate_and_wrap_dek(
                org,
                Some(sandbox_id),
                kek.provider,
                &kek.key_identifier,
                DekKeySpec::Aes256,
                &aad,
            )
            .await
            .map_err(wrap_error_to_status)?;

        // First-writer-wins persistence. The returned row is whichever wrap
        // landed first; if a concurrent caller beat us, discard our fresh DEK
        // and unwrap theirs so the sandbox's DEK is stable.
        let canonical = self
            .volume_wrap_store()?
            .insert_if_absent(sandbox_id, org, kek.cmk_id, &wrapped_dek)
            .await
            .map_err(volume_wrap_store_error_to_status)?;
        if canonical.wrapped_dek == wrapped_dek {
            return Ok(Response::new(GetOrIssueSandboxVolumeDekResponse {
                plaintext_dek,
            }));
        }
        // Same previous-owner refusal as the load path above: a concurrent
        // first writer can land between our initial lookup and this insert,
        // and if its wrap belongs to a different org the AAD can never match.
        if canonical.org_id != owner.org_id {
            tracing::warn!(
                sandbox_id = %sandbox_id,
                "volume DEK refused: concurrently persisted wrap is bound to a different org \
                 than the sandbox's current owner"
            );
            return Err(Status::failed_precondition(
                "sandbox volume DEK is bound to a different organization than the \
                 sandbox's current owner; the volume can no longer be decrypted",
            ));
        }
        let plaintext_dek = self
            .unwrap_volume_wrap(org, sandbox_id, &canonical, &aad)
            .await?;
        Ok(Response::new(GetOrIssueSandboxVolumeDekResponse {
            plaintext_dek,
        }))
    }

    /// Certify a node's intermediate CA public key under the SPACE root.
    ///
    /// The TCP layer verifies the caller pod, node registration, and connection
    /// peer before this handler's service-level authorization runs.
    async fn issue_intermediate_certificate(
        &self,
        request: Request<IssueIntermediateCertificateRequest>,
    ) -> Result<Response<IssueIntermediateCertificateResponse>, Status> {
        self.authorize_caller(
            &request,
            "IssueIntermediateCertificate",
            ALLOW_ISSUE_INTERMEDIATE,
            ALLOW_ISSUE_INTERMEDIATE_SERVICES,
        )?;

        // The gRPC layer already logs and traces the call; this adds the
        // failure class and the latency of an issuance that polls for seconds.
        let started = Instant::now();
        let issued = self.issue_intermediate(request.into_inner()).await;
        pplx_space_metrics::credmgr::intermediate_ca_issue(
            intermediate_result_tag(&issued),
            started.elapsed().as_secs_f64() * 1000.0,
        );

        Ok(Response::new(IssueIntermediateCertificateResponse {
            certificate_der: issued.map_err(intermediate_error_to_status)?,
            issuer_certificate_der: Vec::new(),
        }))
    }
}

impl CredentialManagerService {
    /// Everything [`intermediate_ca_issue`](pplx_space_metrics::credmgr::intermediate_ca_issue)
    /// meters: the backend precondition, request validation, and the issuance
    /// itself. Separated from the handler so a refused request is counted like
    /// any other failed issuance instead of returning before the metric.
    async fn issue_intermediate(
        &self,
        request: IssueIntermediateCertificateRequest,
    ) -> Result<Vec<u8>, IssueIntermediateError> {
        let issuer = self
            .intermediate_ca_issuer
            .as_ref()
            .ok_or(IssueIntermediateError::NotConfigured)?;
        let issue_request = IntermediateCertificateRequest {
            csr: NodeCsr::parse(&request.csr)?,
            validity: intermediate_ca::INTERMEDIATE_VALIDITY,
        };
        issuer.issue(&issue_request).await
    }
}

/// Low-cardinality metric `result` tag for an issuance attempt.
fn intermediate_result_tag<T>(result: &Result<T, IssueIntermediateError>) -> &'static str {
    match result {
        Ok(_) => "ok",
        Err(IssueIntermediateError::NotConfigured) => "not_configured",
        Err(IssueIntermediateError::Csr(_)) => "invalid_csr",
        Err(IssueIntermediateError::RequestBuild(_)) => "request_build_failed",
        Err(IssueIntermediateError::Rejected(_)) => "rejected",
        Err(IssueIntermediateError::Unavailable(_)) => "unavailable",
        Err(IssueIntermediateError::IssuanceTimedOut { .. }) => "timed_out",
        Err(IssueIntermediateError::MalformedCertificate(_)) => "malformed_certificate",
    }
}

/// Map an issuance failure onto the status the node's refresh loop keys off:
/// `UNAVAILABLE` to come back soon; `FAILED_PRECONDITION` (no intermediate CA
/// deployed here) and `INVALID_ARGUMENT` to re-probe slowly, since only a
/// deploy changes either. See `node_ca::certify_error`.
fn intermediate_error_to_status(error: IssueIntermediateError) -> Status {
    match error {
        IssueIntermediateError::NotConfigured => Status::failed_precondition(error.to_string()),
        IssueIntermediateError::Csr(_) => Status::invalid_argument(error.to_string()),
        // The request already validated, so a rejection here is about how this
        // authority composed the call. Different bytes from the caller would
        // not help.
        IssueIntermediateError::RequestBuild(_) | IssueIntermediateError::Rejected(_) => {
            Status::internal(error.to_string())
        }
        IssueIntermediateError::Unavailable(_)
        | IssueIntermediateError::IssuanceTimedOut { .. }
        | IssueIntermediateError::MalformedCertificate(_) => Status::unavailable(error.to_string()),
    }
}

impl CredentialManagerService {
    /// Recover the plaintext DEK from a stored volume wrap. The wrapping KEK
    /// is named inside the row (`cmk_id`), never by the caller; the
    /// `(cmk_id, org)` lookup verifies the row belongs to the sandbox's org,
    /// and retired KEKs still unwrap so rotation cannot strand an existing
    /// volume.
    async fn unwrap_volume_wrap(
        &self,
        org: OrgId,
        sandbox: SandboxId,
        wrap: &crate::volume_wrap_store::VolumeWrap,
        aad: &[u8],
    ) -> Result<Vec<u8>, Status> {
        let kek = self
            .kek_store()?
            .kek_for_org(wrap.cmk_id, org)
            .await
            .map_err(kek_store_error_to_status)?
            .ok_or_else(|| {
                Status::internal(
                    "stored volume wrap names a KEK that does not belong to the sandbox's org",
                )
            })?;
        self.key_wrapper
            .unwrap_dek(
                org,
                Some(sandbox),
                kek.provider,
                &kek.key_identifier,
                &wrap.wrapped_dek,
                aad,
            )
            .await
            .map_err(unwrap_error_to_status)
    }

    /// The credential store (sandbox→owner reads), failing closed when this
    /// instance has no database access (node-local deployments).
    fn credential_store(&self) -> Result<&Arc<dyn CredentialStore>, Status> {
        self.credential_store.as_ref().ok_or_else(|| {
            Status::failed_precondition(
                "this RPC requires database access; this credmgr instance \
                 has no database configured",
            )
        })
    }

    fn storage_vend_store(&self) -> Result<&Arc<dyn StorageVendStore>, Status> {
        self.storage_vend_store.as_ref().ok_or_else(|| {
            Status::failed_precondition(
                "storage credential exchange is not configured; it requires a storage vend \
                 role and authority database access",
            )
        })
    }

    fn credential_injection_rpc(&self) -> Result<&CredentialInjectionRpc, Status> {
        self.credential_injection_rpc.as_ref().ok_or_else(|| {
            Status::failed_precondition(
                "credential-injection resolution is not configured on this instance",
            )
        })
    }

    /// The per-sandbox volume wrap store, failing closed when this instance
    /// has no database access (node-local deployments).
    fn volume_wrap_store(&self) -> Result<&Arc<dyn VolumeWrapStore>, Status> {
        self.volume_wrap_store.as_ref().ok_or_else(|| {
            Status::failed_precondition(
                "GetOrIssueSandboxVolumeDek requires database access; this credmgr \
                 instance has no database configured",
            )
        })
    }

    /// The KEK store, failing closed when this instance has no database
    /// access (node-local deployments).
    fn kek_store(&self) -> Result<&Arc<dyn KekStore>, Status> {
        self.kek_store.as_ref().ok_or_else(|| {
            Status::failed_precondition(
                "Encrypt/Decrypt require database access; this credmgr instance \
                 has no database configured",
            )
        })
    }
}

pub(crate) fn parse_organization_id(raw: &str) -> Result<OrgId, Status> {
    raw.parse()
        .map_err(|error| Status::invalid_argument(format!("organization_id: {error}")))
}

fn parse_org_and_user(
    organization_id: &str,
    user_id: Option<&str>,
) -> Result<(OrgId, Option<UserId>), Status> {
    let org = parse_organization_id(organization_id)?;
    let user_id = user_id
        .map(|raw| {
            raw.parse::<UserId>()
                .map_err(|e| Status::invalid_argument(format!("user_id: {e}")))
        })
        .transpose()?;
    Ok((org, user_id))
}

/// Domain-separated AAD binding the payload to its owner. Fixed-width
/// fields (16-byte org, 16-byte user or nil for org-scoped payloads), so
/// the caller-supplied suffix cannot alias the binding.
fn effective_aad(
    org: OrgId,
    user_id: Option<UserId>,
    purpose: &str,
    caller_aad: &[u8],
) -> Result<Vec<u8>, Status> {
    // v2 adds the purpose scope so Encrypt/Decrypt cannot act as a general
    // oracle. The 2-byte length prefix keeps purpose unambiguous since the
    // caller AAD that follows is variable-length.
    const PREFIX: &[u8] = b"space-credential-envelope/v2";
    let purpose = purpose.as_bytes();
    // The length prefix is a u16; a longer purpose would truncate and let two
    // distinct purposes alias to the same AAD. Reject rather than truncate.
    let purpose_len = u16::try_from(purpose.len()).map_err(|_| {
        Status::invalid_argument(format!(
            "purpose is {} bytes; must be at most {}",
            purpose.len(),
            u16::MAX
        ))
    })?;
    let mut aad =
        Vec::with_capacity(PREFIX.len() + 1 + 16 + 16 + 2 + purpose.len() + caller_aad.len());
    aad.extend_from_slice(PREFIX);
    aad.push(0);
    aad.extend_from_slice(org.as_uuid().as_bytes());
    aad.extend_from_slice(
        user_id
            .map(UserId::as_uuid)
            .unwrap_or_else(uuid::Uuid::nil)
            .as_bytes(),
    );
    aad.extend_from_slice(&purpose_len.to_le_bytes());
    aad.extend_from_slice(purpose);
    aad.extend_from_slice(caller_aad);
    Ok(aad)
}

/// Move a backend-issued DEK into a fixed 32-byte buffer that zeroizes on
/// drop. The source `Vec` is zeroized here as well so no copy of the key
/// outlives the request.
fn dek_array(mut plaintext_dek: Vec<u8>) -> Result<zeroize::Zeroizing<[u8; 32]>, Status> {
    use zeroize::Zeroize;
    let result = <[u8; 32]>::try_from(plaintext_dek.as_slice())
        .map(zeroize::Zeroizing::new)
        .map_err(|_| Status::internal("key backend returned a DEK of unexpected length"));
    plaintext_dek.zeroize();
    result
}

/// `Unavailable` when `error` is a contention abort (`40001` / `40P01` /
/// `55P03`), `None` when it is a real failure.
///
/// This authority is the reason `space_creds` was excluded from the role-level
/// `lock_timeout` (see `db/README.md`): every mapper below turned any SQL error
/// into `Internal`, and bounding a lane whose only caller reads that as
/// permanent trades a slow success for a fast failure. `Unavailable` is what
/// makes the bound safe to enable here — the same code
/// `ProvisionError::LostRaceWithoutWinner` already uses for its own lost race.
///
/// Every statement in the lane can reach it, not only the ones that name a
/// lock: the credential and KEK writes are `INSERT ... ON CONFLICT`, which
/// block on a concurrent unfinished insert of the same key, and a plain
/// `SELECT` blocks behind DDL on the table it reads.
///
/// `warn`, not `error`: a bounded lock wait is the expected outcome of enabling
/// the bound, and the callers below skip their own `error!` when this answers.
fn contention_status(what: &'static str, error: &sqlx::Error) -> Option<Status> {
    if !contention::is_lock_contention(error) {
        return None;
    }
    tracing::warn!(what, error = %error, "credmgr statement lost a lock race; retryable");
    Some(Status::unavailable(
        "operation conflicted with a concurrent change; retry the operation",
    ))
}

fn kek_store_error_to_status(e: KekStoreError) -> Status {
    if let KekStoreError::Query(error) = &e
        && let Some(contended) = contention_status("KEK lookup", error)
    {
        return contended;
    }
    match e {
        // Corrupt rows and query failures are server-side problems; none of
        // them should leak schema details to the caller.
        KekStoreError::InvalidProvider { .. }
        | KekStoreError::InvalidKeyIdentifier { .. }
        | KekStoreError::Query(_) => {
            tracing::error!(error = %e, "KEK lookup failed");
            Status::internal("KEK lookup failed")
        }
    }
}

fn volume_wrap_store_error_to_status(e: crate::volume_wrap_store::VolumeWrapStoreError) -> Status {
    let crate::volume_wrap_store::VolumeWrapStoreError::Query(error) = &e;
    if let Some(contended) = contention_status("sandbox volume wrap lookup", error) {
        return contended;
    }
    tracing::error!(error = %e, "sandbox volume wrap lookup failed");
    Status::internal("sandbox volume wrap lookup failed")
}

/// The credential-store lookups' mapper. `what` is the caller's own message,
/// threaded through rather than replaced with one generic string, so an owner
/// lookup stays distinguishable from a credential lookup in both the log and
/// the response — the callers' `Internal` messages already differed and this
/// keeps them.
fn credential_store_error_to_status(
    what: &'static str,
    e: crate::credential_store::CredentialStoreError,
) -> Status {
    let crate::credential_store::CredentialStoreError::Query(error) = &e;
    if let Some(contended) = contention_status(what, error) {
        return contended;
    }
    tracing::error!(error = %e, what, "credential store lookup failed");
    Status::internal(what)
}

/// Resolve a proto [`DekSpec`] discriminant to a typed [`DekKeySpec`].
/// Unknown / `UNSPECIFIED` defaults to AES-256 to keep the API
/// forward-compatible: clients that haven't been recompiled against
/// a newer proto get a sensible 32-byte key.
fn dek_spec_to_key_spec(spec: i32) -> DekKeySpec {
    match DekSpec::try_from(spec).unwrap_or(DekSpec::Aes256) {
        DekSpec::Unspecified | DekSpec::Aes256 => DekKeySpec::Aes256,
        DekSpec::Aes128 => DekKeySpec::Aes128,
    }
}

/// `FAILED_PRECONDITION` carrying the machine-readable revocation reason in
/// trailing metadata, so cross-process consumers (the org cleanup /
/// sandbox-shutdown machinery) can react without parsing status messages.
/// Decode with [`pplx_space_credmgr_client::status_is_org_kek_access_revoked`].
fn org_kek_access_revoked_status() -> Status {
    let mut metadata = tonic::metadata::MetadataMap::new();
    metadata.insert(
        pplx_space_credmgr_client::CREDMGR_ERROR_REASON_METADATA_KEY,
        pplx_space_credmgr_client::CREDMGR_ERROR_REASON_ORG_KEK_ACCESS_REVOKED
            .parse()
            .expect("static ASCII metadata value"),
    );
    Status::with_metadata(
        tonic::Code::FailedPrecondition,
        "access to the organization's KEK was revoked at its external provider; \
         credential operations for this organization are disabled",
        metadata,
    )
}

/// `PERMISSION_DENIED` carrying the machine-readable already-published reason,
/// so a builder whose publication check raced another publisher, or could not
/// see the release, converges on tags-only registration instead of failing.
/// Decode with [`pplx_space_credmgr_client::status_is_release_already_published`].
fn release_already_published_status(message: String) -> Status {
    let mut metadata = tonic::metadata::MetadataMap::new();
    metadata.insert(
        pplx_space_credmgr_client::CREDMGR_ERROR_REASON_METADATA_KEY,
        pplx_space_credmgr_client::CREDMGR_ERROR_REASON_RELEASE_ALREADY_PUBLISHED
            .parse()
            .expect("static ASCII metadata value"),
    );
    Status::with_metadata(tonic::Code::PermissionDenied, message, metadata)
}

fn invalid_key_reference_status(message: String) -> Status {
    let mut metadata = tonic::metadata::MetadataMap::new();
    metadata.insert(
        pplx_space_credmgr_client::CREDMGR_ERROR_REASON_METADATA_KEY,
        pplx_space_credmgr_client::CREDMGR_ERROR_REASON_INVALID_KEY_REFERENCE
            .parse()
            .expect("static ASCII metadata value"),
    );
    Status::with_metadata(tonic::Code::FailedPrecondition, message, metadata)
}

/// Lazy-provisioning failures carry the same classification the provider
/// stated: enrollment denials are terminal (with the machine-readable
/// revocation reason), transients are retriable, contract violations are
/// internal.
fn provision_error_to_status(e: ProvisionError) -> Status {
    match e {
        ProvisionError::Provider(ProviderError::AccessRevoked(_)) => {
            org_kek_access_revoked_status()
        }
        ProvisionError::Provider(ProviderError::InvalidTenant(detail)) => {
            Status::failed_precondition(format!(
                "the intermediate-key broker does not recognize this tenant: {detail}"
            ))
        }
        ProvisionError::Provider(ProviderError::InvalidKey(detail)) => {
            invalid_key_reference_status(format!(
                "the key provider does not recognize this organization: {detail}"
            ))
        }
        ProvisionError::Provider(ProviderError::AuthorizationRejected(detail)) => {
            Status::permission_denied(format!(
                "KEK provisioning authorization was rejected: {detail}"
            ))
        }
        ProvisionError::Provider(ProviderError::Unavailable(msg)) => {
            Status::unavailable(format!("KEK provisioning: {msg}"))
        }
        ProvisionError::Provider(error @ ProviderError::Malformed(_)) => {
            Status::internal(format!("KEK provisioning: {error}"))
        }
        ProvisionError::KekStore(error) => kek_store_error_to_status(error),
        // Before the `Internal`: this transaction inserts the KEK row through
        // an `INSERT ... ON CONFLICT`, so two first-time provisioners for one
        // org contend on the same key. `LostRaceWithoutWinner` below is the
        // same race detected a statement later, and answers `Unavailable`.
        ProvisionError::Db(error) => contention_status("KEK provisioning", &error)
            .unwrap_or_else(|| Status::internal(format!("KEK provisioning: {error}"))),
        ProvisionError::LostRaceWithoutWinner => {
            Status::unavailable("KEK provisioning raced a concurrent writer; retry the operation")
        }
    }
}

fn wrap_error_to_status(e: WrapError) -> Status {
    match e {
        WrapError::UnsupportedProvider(p) => Status::failed_precondition(format!(
            "KEK provider '{p}' is not configured in this credmgr",
        )),
        WrapError::AccessRevoked => org_kek_access_revoked_status(),
        WrapError::Unavailable(msg) => Status::unavailable(format!("wrap: {msg}")),
        WrapError::InvalidKey(msg) => invalid_key_reference_status(format!("wrap: {msg}")),
        WrapError::AuthorizationRejected(msg) => {
            Status::permission_denied(format!("wrap authorization was rejected: {msg}"))
        }
        WrapError::Backend(msg) => Status::internal(format!("wrap: {msg}")),
    }
}

fn unwrap_error_to_status(e: UnwrapError) -> Status {
    match e {
        UnwrapError::UnsupportedProvider(p) => Status::failed_precondition(format!(
            "KEK provider '{p}' is not configured in this credmgr",
        )),
        UnwrapError::UnknownOrgKek => Status::failed_precondition(
            "no KEK exists for the requested organization + master_key_identifier",
        ),
        UnwrapError::AuthFailed => {
            Status::permission_denied("authentication failed (AAD mismatch or tampered ciphertext)")
        }
        UnwrapError::Malformed => Status::invalid_argument("malformed wrapped DEK"),
        UnwrapError::AccessRevoked => org_kek_access_revoked_status(),
        UnwrapError::Unavailable(msg) => Status::unavailable(format!("unwrap: {msg}")),
        UnwrapError::InvalidKey(msg) => invalid_key_reference_status(format!("unwrap: {msg}")),
        UnwrapError::AuthorizationRejected(msg) => {
            Status::permission_denied(format!("unwrap authorization was rejected: {msg}"))
        }
        UnwrapError::Backend(msg) => Status::internal(format!("unwrap: {msg}")),
    }
}

/// Vend a storage-role session limited by the exact-resource inline policy and
/// TTL. Ambient and static credentials are inputs to STS only, never response
/// values.
async fn assume_s3_vend_role(
    role_arn: &str,
    session_policy: &str,
    ttl_seconds: u32,
) -> Result<AwsCredentials, Status> {
    use tracing::Instrument as _;
    let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let span = tracing::info_span!(
        "sts.client",
        otel.kind = "client",
        otel.name = "STS.AssumeRole",
        otel.status_code = tracing::field::Empty,
        otel.status_description = tracing::field::Empty,
        "operation.name" = "sts.client",
        rpc.system = "aws-api",
        rpc.service = "STS",
        rpc.method = "AssumeRole",
    );
    let assumed = async {
        aws_config::sts::AssumeRoleProvider::builder(role_arn)
            .session_name("space-storage-exchange")
            .session_length(Duration::from_secs(u64::from(ttl_seconds)))
            .policy(session_policy)
            .configure(&config)
            .build()
            .await
            .provide_credentials()
            .await
    }
    .instrument(span.clone())
    .await;
    match &assumed {
        Ok(_) => {
            span.record("otel.status_code", "OK");
        }
        // A fixed kind, not the provider's text, which can quote the role ARN.
        Err(error) => {
            let kind = match error {
                aws_credential_types::provider::error::CredentialsError::CredentialsNotLoaded(
                    _,
                ) => "not_loaded",
                aws_credential_types::provider::error::CredentialsError::ProviderTimedOut(_) => {
                    "timeout"
                }
                aws_credential_types::provider::error::CredentialsError::InvalidConfiguration(
                    _,
                ) => "invalid_configuration",
                aws_credential_types::provider::error::CredentialsError::ProviderError(_) => {
                    "provider_error"
                }
                _ => "other",
            };
            span.record("otel.status_code", "ERROR");
            span.record(
                "otel.status_description",
                format!("STS AssumeRole failed: {kind}").as_str(),
            );
        }
    }
    let creds = assumed.map_err(|e| {
        tracing::warn!(error = %e, "storage role assumption failed");
        Status::failed_precondition("failed to assume the configured storage role")
    })?;
    let expires_at_unix = creds
        .expiry()
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_secs())
        .ok_or_else(|| Status::internal("STS returned storage credentials without an expiry"))?;
    Ok(AwsCredentials {
        access_key_id: creds.access_key_id().to_string(),
        secret_access_key: creds.secret_access_key().to_string(),
        session_token: creds.session_token().map(str::to_string),
        expires_at_unix: Some(expires_at_unix),
    })
}

fn credential_expires_in(credentials: &AwsCredentials) -> Result<u32, Status> {
    let expires_at = credentials
        .expires_at_unix
        .ok_or_else(|| Status::internal("scoped storage credentials have no expiry"))?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Status::internal("system clock is before the Unix epoch"))?
        .as_secs();
    let remaining = expires_at
        .checked_sub(now)
        .filter(|remaining| *remaining > 0)
        .ok_or_else(|| Status::internal("STS returned already-expired storage credentials"))?;
    u32::try_from(remaining)
        .map_err(|_| Status::internal("STS returned an invalid storage credential expiry"))
}

fn storage_exchange_error_to_status(error: StorageExchangeError) -> Status {
    if let StorageExchangeError::Store(StorageVendStoreError::Query(query)) = &error
        && let Some(status) = contention_status("storage authorization lookup", query)
    {
        return status;
    }
    match error {
        StorageExchangeError::Missing(_)
        | StorageExchangeError::Malformed(_)
        | StorageExchangeError::Unsupported(_)
        | StorageExchangeError::EmptyScope
        | StorageExchangeError::PolicyTooLarge => Status::invalid_argument(error.to_string()),
        StorageExchangeError::ActorSpoofing
        | StorageExchangeError::UnauthorizedActor
        | StorageExchangeError::UnauthorizedResource
        | StorageExchangeError::UnknownSubject
        | StorageExchangeError::WrongNode => Status::permission_denied(error.to_string()),
        StorageExchangeError::ReleaseAlreadyPublished => {
            release_already_published_status(error.to_string())
        }
        StorageExchangeError::MissingVendRole
        | StorageExchangeError::InvalidVendRole
        | StorageExchangeError::InvalidStorageConfiguration
        | StorageExchangeError::Store(StorageVendStoreError::UnsupportedProvider) => {
            Status::failed_precondition(error.to_string())
        }
        StorageExchangeError::Store(_) => {
            tracing::error!(error = %error, "storage authorization lookup failed");
            Status::internal("storage authorization lookup failed")
        }
    }
}

fn caller_audit_label(caller: &ResolvedCaller) -> String {
    match caller {
        ResolvedCaller::K8sJwt(identity) => identity.service().to_string(),
        ResolvedCaller::UdsPeer(identity) => format!("uds-uid:{}", identity.uid()),
        ResolvedCaller::Plaintext => "internal-token".to_string(),
    }
}

fn audit_exchange_caller_denial(caller: Option<&ResolvedCaller>, code: tonic::Code) {
    tracing::warn!(
        target: "credential_security_audit",
        operation = "ExchangeToken",
        actor = caller.map(caller_audit_label).as_deref().unwrap_or("unresolved"),
        decision = "deny",
        outcome = "rejected",
        error_class = ?code,
        "credential security audit event"
    );
}

fn audit_exchange_denial(caller: &ResolvedCaller, error: &StorageExchangeError) {
    tracing::warn!(
        target: "credential_security_audit",
        operation = "ExchangeToken",
        actor = %caller_audit_label(caller),
        decision = "deny",
        outcome = "rejected",
        error_class = error.audit_class(),
        "credential security audit event"
    );
}

fn audit_exchange_failure(exchange: &AuthorizedExchange, code: tonic::Code) {
    tracing::warn!(
        target: "credential_security_audit",
        operation = "ExchangeToken",
        actor = %exchange.actor,
        tenant = exchange.organization_id.map(|org| org.to_string()).as_deref(),
        subject_kind = exchange.subject.audit_kind(),
        subject = %exchange.subject.audit_id(),
        resource_bucket = exchange.resource.bucket(),
        resource_key = exchange.resource.key(),
        scope = %exchange.actions.effective_scope(),
        decision = "allow",
        outcome = "failed",
        error_class = ?code,
        "credential security audit event"
    );
}

fn audit_exchange_issuance(exchange: &AuthorizedExchange, expires_in: u32) {
    tracing::info!(
        target: "credential_security_audit",
        operation = "ExchangeToken",
        actor = %exchange.actor,
        tenant = exchange.organization_id.map(|org| org.to_string()).as_deref(),
        subject_kind = exchange.subject.audit_kind(),
        subject = %exchange.subject.audit_id(),
        resource_bucket = exchange.resource.bucket(),
        resource_key = exchange.resource.key(),
        scope = %exchange.actions.effective_scope(),
        expires_in,
        decision = "allow",
        outcome = "issued",
        "credential security audit event"
    );
}

#[cfg(test)]
mod tests;
