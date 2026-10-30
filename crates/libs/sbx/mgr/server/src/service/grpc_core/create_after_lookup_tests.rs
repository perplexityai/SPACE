//! Create's paths after template lookup, against a scripted template resolver
//! and an in-process node-manager.
//!
//! Most cases fence the sandbox insert and inspect the attempted row. Successful
//! logical creates instead use a test-owned cache root and assert durable state.

use super::*;
use crate::node_manager_transport::fake::{FakeNodeManager, ServedNodeManager};
use crate::template_resolver::fake::FakeTemplateResolver;
use pplx_space_common_svc::PeerIdentity;
use pplx_space_node_mgr_client::proto::{NetworkPoolStatusRequest, NetworkPoolStatusResponse};
use pplx_space_template_mgr_client::proto::{GetTemplateRequest, GetTemplateResponse, TemplateRef};
use std::collections::HashMap;

pub(super) const TEMPLATE: &str = "seed";
pub(super) const BUILD: &str = "sha256:create-after-lookup";
pub(super) const OWNER: &str = "root@local";
/// Prefix of the insert fence's refusal; the rest is the refused row as JSON.
const FENCE: &str = "create fence refused row: ";
/// Bounds every create on real time, including the node-manager calls it
/// makes, so a regression that waits on node-manager fails instead of hanging.
pub(super) const CREATE_WAIT: Duration = Duration::from_secs(30);

/// Run the test in a child process without the feature-flag provider
/// settings, so service construction cannot start a provider. Call before
/// constructing any service; return when false.
pub(super) fn enter_isolated_test_process() -> bool {
    crate::service::tests::enter_route_test_process(&[
        ("EPPO_SDK_KEY", None),
        ("SPACE_FLAGD_ENABLED", None),
    ])
}

/// The environment rows are seeded with, distinct from every request's.
fn row_env() -> BTreeMap<String, String> {
    BTreeMap::from([("ROW_ENV".to_owned(), "from-row".to_owned())])
}

/// The fields of a refused `sandboxes` insert this module asserts on.
#[derive(Debug, serde::Deserialize, PartialEq)]
struct FencedRow {
    id: String,
    state: String,
    template: String,
    template_hash: Option<String>,
    user: String,
    auto_lifecycle: AutoLifecyclePolicy,
    auto_lifecycle_authority: String,
    network_policy_authority: String,
    process_env: BTreeMap<String, String>,
}

/// Refuse every new `sandboxes` row, reporting the row in the refusal.
fn install_insert_fence(conn: &crate::db::Connection) {
    conn.execute_batch(&format!(
        "CREATE TEMP TRIGGER create_after_lookup_insert_fence
         BEFORE INSERT ON main.sandboxes
         BEGIN
             SELECT RAISE(ABORT, '{FENCE}' || json_object(
                 'id', NEW.id,
                 'state', NEW.state,
                 'template', NEW.template,
                 'template_hash', NEW.template_hash,
                 'user', NEW.user,
                 'auto_lifecycle', json(NEW.auto_lifecycle_json),
                 'auto_lifecycle_authority', NEW.auto_lifecycle_authority,
                 'network_policy_authority', NEW.network_policy_authority,
                 'process_env', json(NEW.process_env_json)
             ));
         END;"
    ))
    .unwrap();
}

/// The row a create tried to insert, from the fence's refusal.
fn fenced_row(error: &Status) -> FencedRow {
    let (_, row) = error
        .message()
        .split_once(FENCE)
        .unwrap_or_else(|| panic!("create did not reach the row insert: {error}"));
    serde_json::from_str(row).unwrap()
}

/// A template build in a test-owned directory, built for this node with a
/// supported spaced guest.
pub(super) struct TemplateBuild {
    pub(super) dir: tempfile::TempDir,
}

impl TemplateBuild {
    pub(super) fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("metadata.json"),
            serde_json::json!({
                "arch": std::env::consts::ARCH,
                "spaced_version": pplx_space_spaced_common::spaced_version(),
                "spaced_compatibility_family": pplx_space_spaced_common::spaced_compatibility_family(),
            })
            .to_string(),
        )
        .unwrap();
        Self { dir }
    }

    pub(super) fn response(&self, build: &str) -> GetTemplateResponse {
        GetTemplateResponse {
            template: Some(TemplateRef {
                name: TEMPLATE.into(),
                content_hash: Some(build.into()),
                tag: None,
            }),
            arch: std::env::consts::ARCH.into(),
            template_dir: self.dir.path().to_str().unwrap().into(),
            default_auto_lifecycle: Some((&access_idle_suspend_policy()).into()),
            network_policy_authority: crate::db::PolicyAuthority::Local.as_proto_i32(),
            auto_lifecycle_authority: crate::db::PolicyAuthority::Local.as_proto_i32(),
            ..Default::default()
        }
    }

    /// A resolver answering every lookup with this template at build `build`.
    pub(super) fn resolver(&self, build: &str) -> Arc<FakeTemplateResolver> {
        let response = self.response(build);
        Arc::new(FakeTemplateResolver::new(move || {
            let mut response = tonic::Response::new(response.clone());
            pplx_space_common_svc::template_lookup::TemplateLookupScope::LocalExecution
                .receipt(&mut response);
            Ok(response)
        }))
    }
}

/// The template's default auto-lifecycle policy, distinct from the disabled
/// policy test rows start with.
fn access_idle_suspend_policy() -> AutoLifecyclePolicy {
    use pplx_space_common_core::types::{
        AutoLifecycleConditionSet, AutoLifecycleConditionSetId, AutoLifecycleStage,
    };
    AutoLifecyclePolicy {
        wake: Default::default(),
        condition_sets: BTreeMap::from([(
            AutoLifecycleConditionSetId::AccessIdle,
            AutoLifecycleConditionSet::access_idle(),
        )]),
        stages: vec![AutoLifecycleStage {
            condition_set: AutoLifecycleConditionSetId::AccessIdle,
            at: 600,
            action: AutoLifecycleIdleAction::Suspend,
        }],
    }
    .normalized()
}

pub(super) struct Fixture {
    pub(super) service: SandboxManagerService,
    pub(super) templates: Arc<FakeTemplateResolver>,
    pub(super) node_manager: ServedNodeManager,
}

impl Fixture {
    /// A service over `conn`, with the insert fence installed, whose template
    /// lookups go to `templates` and whose node-manager is `node_manager`.
    pub(super) fn new(
        conn: crate::db::Connection,
        templates: Arc<FakeTemplateResolver>,
        node_manager: FakeNodeManager,
    ) -> Self {
        install_insert_fence(&conn);
        let node_manager = node_manager.serve();
        let service = SandboxManagerService::new_with_options(
            conn,
            SandboxManagerServiceOptions {
                node_manager: node_manager.connector(),
                template_resolver: Arc::clone(&templates) as _,
                ..SandboxManagerServiceOptions::for_test()
            },
        );
        Self {
            service,
            templates,
            node_manager,
        }
    }

    pub(super) async fn create(&self, request: CreateRequest) -> Result<CreateResponse, Status> {
        let mut request = Request::new(request);
        request
            .extensions_mut()
            .insert(PeerIdentity::new(0, 0, None));
        tokio::time::timeout(CREATE_WAIT, self.service.create(request))
            .await
            .unwrap_or_else(|_| panic!("create waited longer than {CREATE_WAIT:?}"))
            .map(Response::into_inner)
    }

    pub(super) fn row(&self, id: &str) -> Option<crate::db::SandboxRow> {
        crate::db::get_sandbox(&self.service.db_conn().unwrap(), id).unwrap()
    }

    pub(super) fn row_count(&self) -> i64 {
        self.service
            .db_conn()
            .unwrap()
            .query_row("SELECT COUNT(*) FROM sandboxes", [], |row| row.get(0))
            .unwrap()
    }

    /// Create looked the template up once, by name.
    fn assert_one_lookup(&self, context: &str) {
        assert_eq!(
            self.templates.requests(),
            vec![GetTemplateRequest {
                caller_org_id: None,
                template: Some(TemplateRef {
                    name: TEMPLATE.into(),
                    content_hash: None,
                    tag: None,
                }),
                vcpu: None,
                memory_mib: None,
            }],
            "{context}"
        );
    }
}

pub(super) fn create_request() -> CreateRequest {
    CreateRequest {
        template: TEMPLATE.into(),
        owner: OWNER.into(),
        runtime: pplx_space_common_core::types::RUNTIME_FIRECRACKER_ALIAS.into(),
        env: HashMap::from([("CREATE_ENV".to_owned(), "from-request".to_owned())]),
        ..Default::default()
    }
}

/// Insert a row the way create writes one, with [`row_env`], before the fence
/// is installed.
pub(super) fn insert_created_row(
    conn: &crate::db::Connection,
    id: &str,
    state: SandboxState,
    template_hash: &str,
    owner: &SandboxOwner,
    pool_template: Option<&str>,
) {
    let sandbox_id = SandboxId::from_str(id).unwrap();
    crate::db::insert_sandbox(
        conn,
        crate::db::InsertSandboxParams {
            guest_image: Default::default(),
            id,
            uuid8: &SandboxRuntimeLabel::from_sandbox_id(sandbox_id),
            state,
            template: TEMPLATE,
            template_hash: Some(template_hash),
            user: owner,
            runtime: pplx_space_common_core::types::RUNTIME_FIRECRACKER_ALIAS,
            snapshots_enabled: false,
            rootfs_size_mb: None,
            disk_io_limits: Default::default(),
            pool_template,
            volume_encryption: false,
            process_env: &row_env(),
            auto_lifecycle: &AutoLifecyclePolicy::disabled(),
            network_policy_override: None,
            network_policy_generation: 0,
            network_policy_authority: crate::db::PolicyAuthority::Cluster,
            auto_lifecycle_authority: crate::db::PolicyAuthority::Cluster,
        },
    )
    .unwrap();
}

#[tokio::test]
async fn create_reaches_the_row_insert_with_the_templates_build_and_defaults() {
    if !enter_isolated_test_process() {
        return;
    }
    struct Case {
        name: &'static str,
        request: CreateRequest,
        /// The id create must insert, when the request names one.
        id: Option<String>,
        auto_lifecycle: AutoLifecyclePolicy,
        auto_lifecycle_authority: &'static str,
    }
    let requested_id = SandboxId::new().to_string();
    let cases = [
        Case {
            name: "template defaults",
            request: create_request(),
            id: None,
            auto_lifecycle: access_idle_suspend_policy(),
            auto_lifecycle_authority: "local",
        },
        Case {
            name: "requested policy",
            request: CreateRequest {
                auto_lifecycle: Some((&AutoLifecyclePolicy::disabled()).into()),
                auto_lifecycle_authority: crate::db::PolicyAuthority::Cluster.as_proto_i32(),
                ..create_request()
            },
            id: None,
            auto_lifecycle: AutoLifecyclePolicy::disabled(),
            auto_lifecycle_authority: "cluster",
        },
        Case {
            name: "requested id with no row",
            request: CreateRequest {
                sandbox_id: Some(requested_id.clone()),
                ..create_request()
            },
            id: Some(requested_id),
            auto_lifecycle: access_idle_suspend_policy(),
            auto_lifecycle_authority: "local",
        },
        // An empty pool and no base snapshot fall back to a cold create.
        Case {
            name: "empty pool",
            request: CreateRequest {
                boot_mode: BootMode::Pool as i32,
                ..create_request()
            },
            id: None,
            auto_lifecycle: access_idle_suspend_policy(),
            auto_lifecycle_authority: "local",
        },
    ];
    for case in cases {
        let build = TemplateBuild::new();
        let fixture = Fixture::new(
            crate::db::test_conn(),
            build.resolver(BUILD),
            FakeNodeManager::default(),
        );

        let error = fixture.create(case.request).await.unwrap_err();

        assert_eq!(
            error.code(),
            tonic::Code::Internal,
            "{}: {error}",
            case.name
        );
        let row = fenced_row(&error);
        if let Some(id) = &case.id {
            assert_eq!(&row.id, id, "{}", case.name);
        }
        assert_eq!(
            row,
            FencedRow {
                id: row.id.clone(),
                state: "creating".into(),
                template: TEMPLATE.into(),
                template_hash: Some(BUILD.into()),
                user: OWNER.into(),
                auto_lifecycle: case.auto_lifecycle,
                auto_lifecycle_authority: case.auto_lifecycle_authority.into(),
                network_policy_authority: "local".into(),
                process_env: BTreeMap::from([("CREATE_ENV".into(), "from-request".into())]),
            },
            "{}",
            case.name
        );
        fixture.assert_one_lookup(case.name);
        assert!(fixture.node_manager.calls().is_empty(), "{}", case.name);
        assert_eq!(fixture.row_count(), 0, "{}", case.name);
    }
}

#[tokio::test]
async fn autostart_create_checks_ready_network_slots_before_the_row_insert() {
    if !enter_isolated_test_process() {
        return;
    }
    struct Case {
        name: &'static str,
        pool_status: fn() -> Result<NetworkPoolStatusResponse, Status>,
        code: tonic::Code,
        /// `None` when create gets past the check to the row insert.
        refusal: Option<&'static str>,
    }
    let cases = [
        Case {
            name: "no ready slot",
            pool_status: || Ok(NetworkPoolStatusResponse::default()),
            code: tonic::Code::ResourceExhausted,
            refusal: Some("no ready network slots"),
        },
        Case {
            name: "status refused",
            pool_status: || Err(Status::unavailable("scripted pool status refusal")),
            code: tonic::Code::Unavailable,
            refusal: Some("network pool status failed: scripted pool status refusal"),
        },
        Case {
            name: "ready slot",
            pool_status: || {
                Ok(NetworkPoolStatusResponse {
                    network_slots_ready: 1,
                    ..Default::default()
                })
            },
            code: tonic::Code::Internal,
            refusal: None,
        },
    ];
    for case in cases {
        let build = TemplateBuild::new();
        let pool_status = case.pool_status;
        let fixture = Fixture::new(
            crate::db::test_conn(),
            build.resolver(BUILD),
            FakeNodeManager::default().unary(
                "GetNetworkPoolStatus",
                move |_: NetworkPoolStatusRequest| pool_status(),
            ),
        );

        let error = fixture
            .create(CreateRequest {
                autostart: true,
                ..create_request()
            })
            .await
            .unwrap_err();

        assert_eq!(error.code(), case.code, "{}: {error}", case.name);
        match case.refusal {
            Some(refusal) => assert_eq!(error.message(), refusal, "{}", case.name),
            None => assert_eq!(fenced_row(&error).template_hash.as_deref(), Some(BUILD)),
        }
        assert_eq!(
            fixture.node_manager.calls(),
            vec!["GetNetworkPoolStatus".to_owned()],
            "{}",
            case.name
        );
        fixture.assert_one_lookup(case.name);
        assert_eq!(fixture.row_count(), 0, "{}", case.name);
    }
}

/// A paused pool donor at `build`, as the pool leaves it for adoption.
pub(super) fn insert_pool_donor(conn: &crate::db::Connection, build: &str) -> String {
    let donor = SandboxId::new();
    let id = donor.to_string();
    insert_created_row(
        conn,
        &id,
        SandboxState::Paused,
        build,
        &TEMPLATE_MANAGER_SANDBOX_OWNER,
        Some(TEMPLATE),
    );
    crate::db::update_space_api_route_identity(
        conn,
        donor,
        &pplx_space_common_core::types::SpaceApiRouteIdentity {
            sandbox_id: Some(donor),
            route_generation: Some(pplx_space_common_core::types::RouteGeneration::new(1).unwrap()),
            current_node_id: Some(pplx_space_common_core::types::NodeId::new()),
            substrate_kind: Some(NodeManagedBy::Space),
            execution_generation: Some(ExecutionGeneration::new(1).unwrap()),
            activation_attempt_id: Some(
                pplx_space_common_core::types::SandboxActivationAttemptId::new(),
            ),
            ..Default::default()
        },
        crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::Unfenced,
    )
    .unwrap();
    id
}

#[tokio::test]
async fn pool_create_adopts_a_donor_of_the_resolved_build_with_the_templates_defaults() {
    if !enter_isolated_test_process() {
        return;
    }
    let conn = crate::db::test_conn();
    let donor = insert_pool_donor(&conn, BUILD);
    let build = TemplateBuild::new();
    let fixture = Fixture::new(conn, build.resolver(BUILD), FakeNodeManager::default());

    let response = fixture
        .create(CreateRequest {
            boot_mode: BootMode::Pool as i32,
            ..create_request()
        })
        .await
        .unwrap();

    assert_eq!(response.sandbox_id, donor);
    assert_eq!(response.boot_mode_used, BootMode::Pool as i32);
    assert!(!response.reused_existing);
    let adopted = fixture.row(&donor).unwrap();
    assert_eq!(adopted.state, SandboxState::Paused);
    assert_eq!(adopted.user.as_str(), OWNER);
    assert_eq!(
        adopted.process_env,
        BTreeMap::from([("CREATE_ENV".into(), "from-request".into())])
    );
    assert_eq!(adopted.auto_lifecycle, access_idle_suspend_policy());
    assert_eq!(
        adopted.auto_lifecycle_authority,
        crate::db::PolicyAuthority::Local
    );
    fixture.assert_one_lookup("pool adoption");
    assert!(fixture.node_manager.calls().is_empty());
    assert_eq!(fixture.row_count(), 1);
}

#[tokio::test]
async fn pool_create_never_adopts_a_donor_of_another_build() {
    if !enter_isolated_test_process() {
        return;
    }
    let conn = crate::db::test_conn();
    let donor = insert_pool_donor(&conn, "sha256:previous-build");
    let build = TemplateBuild::new();
    let fixture = Fixture::new(conn, build.resolver(BUILD), FakeNodeManager::default());
    let before = fixture.row(&donor).unwrap();

    // Adoption selects donors by the resolved build. A pool holding only stale
    // donors is skipped like any other ineligible one: create falls through to
    // a cold create, which the insert fence refuses with the row it attempted.
    let error = fixture
        .create(CreateRequest {
            boot_mode: BootMode::Pool as i32,
            ..create_request()
        })
        .await
        .unwrap_err();

    assert_eq!(fenced_row(&error).template_hash.as_deref(), Some(BUILD));
    let after = fixture.row(&donor).unwrap();
    assert_eq!(after.state, before.state);
    assert_eq!(after.user, before.user);
    assert_eq!(after.process_env, row_env());
    assert_eq!(after.auto_lifecycle, before.auto_lifecycle);
    fixture.assert_one_lookup("stale donor");
    assert!(fixture.node_manager.calls().is_empty());
    assert_eq!(fixture.row_count(), 1);
}

/// What a requested-id create does with a compatible existing row in
/// `state`, written out per state.
fn requested_create_outcome(state: SandboxState) -> Result<(), (tonic::Code, String)> {
    use SandboxState::*;
    match state {
        Created | Started | Paused | Suspended | Stopped => Ok(()),
        CleanupRequired | CleanupFailed => Err((
            tonic::Code::Unavailable,
            "is still completing activation cleanup; retry".to_owned(),
        )),
        Creating | Starting | Stopping | Deleting | Pausing | Resuming | Checkpointing
        | Suspending | Restoring | Quarantined | Failed => Err((
            tonic::Code::AlreadyExists,
            format!("already exists in state {state}"),
        )),
    }
}

#[tokio::test]
async fn requested_id_create_reuses_only_a_compatible_settled_row() {
    if !enter_isolated_test_process() {
        return;
    }
    struct Case {
        name: String,
        state: SandboxState,
        template_hash: &'static str,
        request_policy: AutoLifecyclePolicy,
        outcome: Result<(), (tonic::Code, String)>,
    }
    // Every state with a request whose policy differs from the row's, so a
    // reuse must update the policy and a refusal must leave it.
    let mut cases: Vec<Case> = SandboxState::all()
        .into_iter()
        .map(|state| Case {
            name: format!("compatible {state} row"),
            state,
            template_hash: BUILD,
            request_policy: access_idle_suspend_policy(),
            outcome: requested_create_outcome(state),
        })
        .collect();
    cases.push(Case {
        name: "compatible started row with the same policy".into(),
        state: SandboxState::Started,
        template_hash: BUILD,
        request_policy: AutoLifecyclePolicy::disabled(),
        outcome: Ok(()),
    });
    // Create checks the build before and, when the policy changes, again
    // under the wake lock; the same-policy case reaches only the first check.
    for (policy, request_policy) in [
        ("the same policy", AutoLifecyclePolicy::disabled()),
        ("a changed policy", access_idle_suspend_policy()),
    ] {
        cases.push(Case {
            name: format!("started row of another build with {policy}"),
            state: SandboxState::Started,
            template_hash: "sha256:previous-build",
            request_policy,
            outcome: Err((
                tonic::Code::AlreadyExists,
                "exists with incompatible create parameters: template_hash".to_owned(),
            )),
        });
    }
    for case in cases {
        let conn = crate::db::test_conn();
        let id = SandboxId::new().to_string();
        crate::db::seed_historical_lifecycle_rows(&conn, |conn| {
            insert_created_row(
                conn,
                &id,
                SandboxState::Created,
                case.template_hash,
                &SandboxOwner::parse(OWNER.to_owned()).unwrap(),
                None,
            );
            conn.execute(
                "UPDATE sandboxes SET state = ?1 WHERE id = ?2",
                crate::db::fixture::params![case.state.as_str(), &id],
            )
            .unwrap();
        });
        let build = TemplateBuild::new();
        let fixture = Fixture::new(conn, build.resolver(BUILD), FakeNodeManager::default());
        let before = fixture.row(&id).unwrap();
        assert_eq!(before.state, case.state, "{}", case.name);

        let result = fixture
            .create(CreateRequest {
                sandbox_id: Some(id.clone()),
                auto_lifecycle: Some((&case.request_policy).into()),
                auto_lifecycle_authority: crate::db::PolicyAuthority::Cluster.as_proto_i32(),
                ..create_request()
            })
            .await;

        let after = fixture.row(&id).unwrap();
        match &case.outcome {
            Err((code, message)) => {
                let error = result.unwrap_err();
                assert_eq!(error.code(), *code, "{}: {error}", case.name);
                assert!(
                    error.message().contains(message.as_str()),
                    "{}: {error}",
                    case.name
                );
                assert_eq!(after.auto_lifecycle, before.auto_lifecycle, "{}", case.name);
            }
            Ok(()) => {
                let response = result.unwrap_or_else(|error| panic!("{}: {error}", case.name));
                assert_eq!(response.sandbox_id, id, "{}", case.name);
                assert!(response.reused_existing, "{}", case.name);
                assert_eq!(after.auto_lifecycle, case.request_policy, "{}", case.name);
            }
        }
        // Reuse keeps the row's own environment over the request's.
        assert_eq!(after.state, before.state, "{}", case.name);
        assert_eq!(after.process_env, row_env(), "{}", case.name);
        fixture.assert_one_lookup(&case.name);
        assert!(fixture.node_manager.calls().is_empty(), "{}", case.name);
        assert_eq!(fixture.row_count(), 1, "{}", case.name);
    }
}

fn protocol_test_environment() -> pplx_space_execution_common::environment::EnvironmentDefinition {
    use pplx_space_common_core::types::{Arch, EnvironmentDescriptor, RuntimeBinding};
    use pplx_space_execution_common::environment::EnvironmentDefinition;
    let initial =
        pplx_space_node_runtime_core::catalog::InitialEnvironment::for_architecture(Arch::X86_64)
            .unwrap()
            .definition()
            .unwrap();
    let architecture = std::env::consts::ARCH.parse().unwrap();
    if architecture == Arch::X86_64 {
        // Keep E0's historical identity. Reconstructing its normalized descriptor
        // would assign a new schema-2 reference with no catalog qualification.
        return initial;
    }
    let mut spec = initial.descriptor().spec().clone();
    spec.host.architecture = architecture;
    spec.guest.architecture = architecture;
    let RuntimeBinding::ManagedRelease(release) = &mut spec.runtime;
    release.target.architecture = architecture;
    EnvironmentDefinition::new(EnvironmentDescriptor::try_from(spec).unwrap())
}

fn qualified_test_policy(request: &mut CreateRequest) {
    // These fake-node tests exercise selection on any test host. Only x86-64
    // has a compiled bound environment qualified for automatic suspend.
    // Keep the default suspend policy there to check admission after selection.
    if std::env::consts::ARCH != "x86_64" {
        request.auto_lifecycle = Some((&AutoLifecyclePolicy::disabled()).into());
        request.auto_lifecycle_authority = crate::db::PolicyAuthority::Local.as_proto_i32();
    }
}

#[tokio::test]
async fn explicit_selection_checks_legacy_base_evidence_before_create() {
    use pplx_space_node_mgr_client::proto::{
        ResolveExecutionEnvironmentRequest, ResolveExecutionEnvironmentResponse,
    };
    if !enter_isolated_test_process() {
        return;
    }
    type Mutation = fn(&mut CreateRequest);
    for mutate in [
        (|r: &mut CreateRequest| r.sandbox_id = Some(Uuid::now_v7().to_string())) as Mutation,
        |r| r.route_identity = Some(Default::default()),
        |r| r.pool_template = Some("pool".into()),
        |r| r.snapshots_enabled = true,
        |r| r.boot_mode = BootMode::BaseSnapshot as i32,
    ] {
        // First, on every path, a node that doesn't execute the selected
        // environment refuses it before the template lookup or any row. The
        // legacy base evidence check follows below, with a node that admits.
        let build = TemplateBuild::new();
        let fixture = Fixture::new(
            crate::db::test_conn(),
            build.resolver(BUILD),
            FakeNodeManager::default().unary(
                "ResolveExecutionEnvironment",
                |_: ResolveExecutionEnvironmentRequest| -> Result<ResolveExecutionEnvironmentResponse, Status> {
                    Err(Status::failed_precondition(
                        "execution environment is not allowed on this node",
                    ))
                },
            ),
        );
        let mut request = create_request();
        request.boot_mode = BootMode::Cold as i32;
        pplx_space_sbx_mgr_client::environment::bind(
            &mut request,
            pplx_space_common_core::types::EnvironmentReference::new(
                pplx_space_common_core::types::Digest::sha256([1; 32]),
            ),
        );
        mutate(&mut request);
        // A selected pool member is refused from the request alone.
        let pool_member = request.pool_template.is_some();
        let error = fixture.create(request).await.unwrap_err();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert_eq!(
            fixture.node_manager.calls(),
            if pool_member {
                Vec::<String>::new()
            } else {
                vec!["ResolveExecutionEnvironment".to_owned()]
            }
        );
        assert!(fixture.templates.requests().is_empty());
        assert_eq!(fixture.row_count(), 0);
    }
    let definition = protocol_test_environment();
    let requirements = definition.execution_requirements().unwrap();
    let mut legacy_wire =
        pplx_space_execution_common::proto::ExecutionMetadata::from(&requirements);
    legacy_wire.environment = None;
    for matches in [true, false] {
        let build = TemplateBuild::new();
        let mut evidence = legacy_wire.clone();
        if !matches {
            evidence.components.clear();
        }
        let evidence =
            pplx_space_common_core::types::ExecutionMetadata::try_from(&evidence).unwrap();
        let mut metadata: serde_json::Value =
            serde_json::from_slice(&std::fs::read(build.dir.path().join("metadata.json")).unwrap())
                .unwrap();
        metadata["schema_version"] = 2.into();
        metadata["execution"] = serde_json::to_value(&evidence).unwrap();
        std::fs::write(
            build.dir.path().join("metadata.json"),
            serde_json::to_vec(&metadata).unwrap(),
        )
        .unwrap();
        let verified = definition.bind_execution(requirements.clone()).unwrap();
        let response = ResolveExecutionEnvironmentResponse {
            accepted_selection: 0,
            definition: Some((&definition).into()),
            execution: Some((&verified).into()),
        };
        let node = FakeNodeManager::default().unary(
            "ResolveExecutionEnvironment",
            move |_: ResolveExecutionEnvironmentRequest| Ok(response.clone()),
        );
        let fixture = Fixture::new(crate::db::test_conn(), build.resolver(BUILD), node);
        let mut request = create_request();
        request.boot_mode = BootMode::Cold as i32;
        qualified_test_policy(&mut request);
        pplx_space_sbx_mgr_client::environment::bind(&mut request, definition.reference());
        let error = fixture.create(request).await.unwrap_err();
        if matches {
            assert_eq!(fenced_row(&error).template, TEMPLATE);
        } else {
            assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        }
        assert_eq!(fixture.row_count(), 0);
    }
}

/// Exercise logical Create and the persisted input used by Start, without a VM
/// or host-owned cache. Bound execution is qualified only for x86-64; changing
/// a descriptor's architecture also changes its identity and is not qualification.
#[cfg(target_arch = "x86_64")]
#[tokio::test]
async fn selected_create_persists_the_binding_used_by_first_start() {
    use pplx_space_node_mgr_client::proto::{
        ResolveExecutionEnvironmentRequest, ResolveExecutionEnvironmentResponse,
    };
    if !enter_isolated_test_process() {
        return;
    }
    for environment in pplx_space_node_runtime_core::catalog::CompiledEnvironment::for_architecture(
        pplx_space_common_core::types::Arch::X86_64,
    )
    .unwrap()
    {
        if !environment.supports_guest_image(Default::default()) {
            continue;
        }
        // Preserve the catalog's historical identity as well as its descriptor.
        let definition = environment.definition().clone();
        let execution = definition.execution_requirements().unwrap();
        let firecracker =
            execution.runtime().kind == pplx_space_common_core::types::RuntimeKind::Firecracker;
        let bound = definition.bind_execution(execution).unwrap();
        let mut wire = pplx_space_execution_common::proto::ExecutionMetadata::from(&bound);
        // Only Firecracker accepts historical, unbound disk evidence. Other
        // adapters require the exact contract persisted by Create.
        if firecracker {
            wire.environment = None;
        }
        // The disk may have been built by an older consumer of the same
        // kernel. The sandbox must keep the newly selected exact consumer.
        if environment
            .kernel()
            .is_some_and(|kernel| kernel.version.as_str() == "6.1.155")
        {
            wire.runtime.as_mut().unwrap().version = "1.15.1".into();
            wire.state_format.as_mut().unwrap().version = "9.0.0".into();
        }
        let legacy = pplx_space_common_core::types::ExecutionMetadata::try_from(&wire).unwrap();
        let build = TemplateBuild::new();
        let mut metadata: serde_json::Value =
            serde_json::from_slice(&std::fs::read(build.dir.path().join("metadata.json")).unwrap())
                .unwrap();
        metadata["schema_version"] = if firecracker { 2 } else { 3 }.into();
        metadata["execution"] = serde_json::to_value(&legacy).unwrap();
        let metadata_bytes = serde_json::to_vec(&metadata).unwrap();
        std::fs::write(build.dir.path().join("metadata.json"), &metadata_bytes).unwrap();
        let response = ResolveExecutionEnvironmentResponse {
            accepted_selection: 0,
            definition: Some((&definition).into()),
            execution: Some((&bound).into()),
        };
        let node = FakeNodeManager::default().unary(
            "ResolveExecutionEnvironment",
            move |_: ResolveExecutionEnvironmentRequest| Ok(response.clone()),
        );
        let database = tempfile::tempdir().unwrap();
        let db_path = database.path().join("sandbox.sqlite");
        crate::db::create_test_schema_on_disk(&db_path);
        let mut fixture = Fixture::new(
            crate::db::Connection::open_at(&db_path).unwrap(),
            build.resolver(BUILD),
            node,
        );
        fixture
            .service
            .db_conn()
            .unwrap()
            .execute_batch("DROP TRIGGER create_after_lookup_insert_fence")
            .unwrap();
        let cache = tempfile::tempdir().unwrap();
        fixture.service.create_cache_dir = Some(cache.path().to_owned());
        let mut request = create_request();
        request.boot_mode = BootMode::Cold as i32;
        request.auto_lifecycle = Some((&AutoLifecyclePolicy::disabled()).into());
        request.auto_lifecycle_authority = crate::db::PolicyAuthority::Local.as_proto_i32();
        pplx_space_sbx_mgr_client::environment::bind(&mut request, definition.reference());
        let created = fixture.create(request).await.unwrap();
        assert_eq!(
            created.environment.as_deref(),
            Some(definition.reference().to_string().as_str())
        );
        let reopened = crate::db::Connection::open_at(&db_path).unwrap();
        let row = crate::db::get_sandbox(&reopened, &created.sandbox_id)
            .unwrap()
            .unwrap();
        assert_eq!(row.state, SandboxState::Created);
        assert_eq!(row.execution_metadata.as_ref(), Some(&bound));
        let prepared = template_source_for_sandbox_prepare_in(
            &fixture.service.node_manager,
            fixture.templates.as_ref(),
            &row,
            cache.path(),
        )
        .await
        .unwrap();
        assert!(!prepared.existing_artifact);
        assert_eq!(prepared.execution, bound);
        assert_eq!(
            std::fs::read(build.dir.path().join("metadata.json")).unwrap(),
            metadata_bytes
        );

        let mut corrupted_template = metadata.clone();
        corrupted_template["execution"]["components"][0]["digest"] =
            format!("sha256:{}", "00".repeat(32)).into();
        std::fs::write(
            build.dir.path().join("metadata.json"),
            serde_json::to_vec(&corrupted_template).unwrap(),
        )
        .unwrap();
        assert_eq!(
            template_source_for_sandbox_prepare_in(
                &fixture.service.node_manager,
                fixture.templates.as_ref(),
                &row,
                cache.path()
            )
            .await
            .unwrap_err()
            .code(),
            tonic::Code::FailedPrecondition,
        );
        std::fs::write(build.dir.path().join("metadata.json"), &metadata_bytes).unwrap();

        // A retained rootfs and its matching legacy evidence also inherit the
        // saved binding. Disk producer annotations cannot select a different
        // consumer; conflicting guest or kernel inputs still fail.
        let sandbox_dir = cache.path().join(&created.sandbox_id);
        std::fs::create_dir_all(sandbox_dir.join("instance")).unwrap();
        std::fs::write(sandbox_dir.join("instance/rootfs.ext4"), b"test rootfs").unwrap();
        #[derive(Clone, Copy, Debug)]
        enum Evidence {
            Matching,
            Alias,
            Runtime,
            RuntimeKind,
            Kernel,
            Reference,
            Missing,
        }
        for evidence in [
            Evidence::Matching,
            Evidence::Alias,
            Evidence::Runtime,
            Evidence::RuntimeKind,
            Evidence::Kernel,
            Evidence::Reference,
            Evidence::Missing,
        ] {
            let mut wire = pplx_space_execution_common::proto::ExecutionMetadata::from(&legacy);
            match evidence {
                Evidence::Matching => {}
                Evidence::Alias => wire.components[0].version = Some("legacy-kernel-label".into()),
                Evidence::Runtime => wire.runtime.as_mut().unwrap().version = "1.16.0".into(),
                Evidence::RuntimeKind => {
                    wire.runtime.as_mut().unwrap().kind = if firecracker {
                        pplx_space_execution_common::proto::RuntimeKind::CloudHypervisor as i32
                    } else {
                        pplx_space_execution_common::proto::RuntimeKind::Firecracker as i32
                    };
                }
                Evidence::Kernel => {
                    wire.components[0].digest = Some(format!("sha256:{}", "00".repeat(32)))
                }
                Evidence::Reference => {
                    wire.environment = Some(format!("sha256:{}", "00".repeat(32)))
                }
                Evidence::Missing => {}
            }
            let observed =
                pplx_space_common_core::types::ExecutionMetadata::try_from(&wire).unwrap();
            let path = sandbox_dir.join("metadata.json");
            if matches!(evidence, Evidence::Missing) {
                std::fs::remove_file(&path).unwrap();
            } else {
                let artifact = pplx_space_common_core::types::SandboxArtifactMetadata::new(
                    created.sandbox_id.parse().unwrap(),
                    observed,
                    TEMPLATE.parse().unwrap(),
                    None,
                );
                std::fs::write(path, serde_json::to_vec(&artifact).unwrap()).unwrap();
            }
            let source = template_source_for_sandbox_prepare_in(
                &fixture.service.node_manager,
                fixture.templates.as_ref(),
                &row,
                cache.path(),
            )
            .await;
            if matches!(evidence, Evidence::Matching)
                || (firecracker
                    && matches!(
                        evidence,
                        Evidence::Alias | Evidence::Runtime | Evidence::Reference
                    ))
            {
                let source = source.unwrap();
                assert!(source.existing_artifact);
                assert_eq!(source.execution.environment(), Some(definition.reference()));
                assert_eq!(source.execution.runtime(), bound.runtime());
                assert_eq!(
                    source.execution.components()[0].digest,
                    bound.components()[0].digest
                );
            } else {
                assert_eq!(
                    source.unwrap_err().code(),
                    tonic::Code::FailedPrecondition,
                    "{evidence:?}"
                );
            }
        }
        fixture.service.shutdown_token.cancel();
        fixture.service.task_tracker.close();
        tokio::time::timeout(CREATE_WAIT, fixture.service.task_tracker.wait())
            .await
            .unwrap();
    }
}

/// A revoked template source must reach compatible selection rather than first
/// requiring exact permission for the revoked producer.
#[tokio::test]
async fn unpinned_create_selects_before_admitting_a_non_default_source() {
    use pplx_space_common_core::types::{Digest, EnvironmentDescriptor, RuntimeBinding};
    use pplx_space_execution_common::environment::EnvironmentDefinition;
    use pplx_space_node_mgr_client::proto::{
        ExecutionEnvironmentSelection, ResolveExecutionEnvironmentRequest,
        ResolveExecutionEnvironmentResponse, resolve_execution_environment_request::Compatible,
    };
    if !enter_isolated_test_process() {
        return;
    }
    let target = protocol_test_environment();
    // A retired producer with the same boot contract. It deliberately has no
    // current memory-state qualification; policy belongs to the selected target.
    let mut source_spec = target.descriptor().spec().clone();
    let RuntimeBinding::ManagedRelease(release) = &mut source_spec.runtime;
    release.runtime.version = "1.17.0".parse().unwrap();
    release.digest = Digest::sha256([7; 32]);
    let source = EnvironmentDefinition::new(EnvironmentDescriptor::try_from(source_spec).unwrap())
        .execution_requirements()
        .unwrap();
    let execution = target.execution_requirements().unwrap();
    let build = TemplateBuild::new();
    let mut metadata: serde_json::Value =
        serde_json::from_slice(&std::fs::read(build.dir.path().join("metadata.json")).unwrap())
            .unwrap();
    metadata["schema_version"] = 3.into();
    metadata["execution"] = serde_json::to_value(&source).unwrap();
    std::fs::write(
        build.dir.path().join("metadata.json"),
        serde_json::to_vec(&metadata).unwrap(),
    )
    .unwrap();
    let expected_consumer = execution.clone();
    let node = FakeNodeManager::default().unary(
        "ResolveExecutionEnvironment",
        move |request: ResolveExecutionEnvironmentRequest| {
            let Some(Compatible::ColdBootSource(observed)) = request.compatible else {
                return Err(Status::failed_precondition("source environment revoked"));
            };
            assert_eq!(
                pplx_space_common_core::types::ExecutionMetadata::try_from(&observed).unwrap(),
                source
            );
            Ok(ResolveExecutionEnvironmentResponse {
                accepted_selection: ExecutionEnvironmentSelection::ColdBoot as i32,
                definition: Some((&target).into()),
                execution: Some((&execution).into()),
            })
        },
    );
    let database = tempfile::tempdir().unwrap();
    let db_path = database.path().join("sandbox.sqlite");
    crate::db::create_test_schema_on_disk(&db_path);
    let mut fixture = Fixture::new(
        crate::db::Connection::open_at(&db_path).unwrap(),
        build.resolver(BUILD),
        node,
    );
    fixture
        .service
        .db_conn()
        .unwrap()
        .execute_batch("DROP TRIGGER create_after_lookup_insert_fence")
        .unwrap();
    let cache = tempfile::tempdir().unwrap();
    fixture.service.create_cache_dir = Some(cache.path().to_owned());
    let mut request = create_request();
    request.boot_mode = BootMode::Cold as i32;
    qualified_test_policy(&mut request);
    let created = fixture.create(request).await.unwrap();
    let reopened = crate::db::Connection::open_at(&db_path).unwrap();
    let row = crate::db::get_sandbox(&reopened, &created.sandbox_id)
        .unwrap()
        .unwrap();
    assert_eq!(row.execution_metadata, Some(expected_consumer));
    assert_eq!(row.state, SandboxState::Created);
    fixture.service.shutdown_token.cancel();
    fixture.service.task_tracker.close();
    tokio::time::timeout(CREATE_WAIT, fixture.service.task_tracker.wait())
        .await
        .unwrap();
}
