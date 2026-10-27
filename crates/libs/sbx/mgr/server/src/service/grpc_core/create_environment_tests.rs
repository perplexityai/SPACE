//! Create's cluster, pool and warm paths for a template bound to the
//! runtime-only upgrade of E0 (Firecracker 1.17 with E0's kernel), against an
//! in-process node-manager that either executes that binding or refuses it.

use super::create_after_lookup_tests::{
    BUILD, CREATE_WAIT, Fixture, OWNER, TEMPLATE, TemplateBuild, create_request,
    enter_isolated_test_process, insert_created_row, insert_pool_donor,
};
use super::*;
use crate::node_manager_transport::fake::FakeNodeManager;
use crate::template_resolver::fake::FakeTemplateResolver;
use pplx_space_common_core::types::{ExecutionMetadata, SnapshotExecutionMetadata};
use pplx_space_execution_common::environment::EnvironmentDefinition;
use pplx_space_node_mgr_client::proto::{
    NetworkPoolStatusRequest, NetworkPoolStatusResponse, ResolveExecutionEnvironmentRequest,
    ResolveExecutionEnvironmentResponse,
};
use pplx_space_template_mgr_client::proto::GetTemplateResponse;

const UPGRADE: &str = "fc-1.17.0-linux-6.1.155";
/// The 6.18 guest kernel: a different environment with the upgrade's runtime.
const UPGRADE_WITH_NEW_KERNEL: &str = "fc-1.17.0-linux-6.18.44";
const RESOLVE: &str = "ResolveExecutionEnvironment";

/// A compiled environment, exactly as the catalog defines it, so its
/// reference is the one sandbox-manager's qualification checks recognize.
fn definition(name: &str) -> EnvironmentDefinition {
    pplx_space_node_runtime_core::catalog::CompiledEnvironment::for_architecture(
        pplx_space_common_core::types::Arch::X86_64,
    )
    .unwrap()
    .into_iter()
    .find(|entry| entry.name() == name)
    .unwrap()
    .definition()
    .clone()
}

fn bound(definition: &EnvironmentDefinition) -> ExecutionMetadata {
    definition
        .bind_execution(definition.execution_requirements().unwrap())
        .unwrap()
}

/// A template build whose stored metadata binds it to `execution`.
fn bound_template(execution: &ExecutionMetadata) -> TemplateBuild {
    let build = TemplateBuild::new();
    let path = build.dir.path().join("metadata.json");
    let mut metadata: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    metadata["schema_version"] = 3.into();
    metadata["execution"] = serde_json::to_value(execution).unwrap();
    std::fs::write(&path, serde_json::to_vec(&metadata).unwrap()).unwrap();
    build
}

/// Whether the fake node executes the environment it's asked to admit.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Node {
    Executes,
    Refuses,
    /// Fails to answer, as an unreachable node-manager does.
    Unavailable,
}

/// A node-manager answering exact resolution of `definition`, and the memory
/// policy a base-snapshot create reads. Compatible selection fails the test:
/// cluster, pool and warm paths keep the template's exact binding.
fn node_manager(definition: &EnvironmentDefinition, node: Node) -> FakeNodeManager {
    let reference = definition.reference().to_string();
    let response = ResolveExecutionEnvironmentResponse {
        accepted_selection: 0,
        definition: Some(definition.into()),
        execution: Some((&bound(definition)).into()),
    };
    FakeNodeManager::default()
        .unary(
            RESOLVE,
            move |request: ResolveExecutionEnvironmentRequest| {
                assert!(
                    request.compatible.is_none(),
                    "a non-local create selected a compatible consumer"
                );
                assert_eq!(request.reference.as_deref(), Some(reference.as_str()));
                assert_eq!(
                    request.definition_schema_version,
                    pplx_space_execution_common::environment::RUNTIME_NEUTRAL_ENVIRONMENT_SCHEMA_VERSION,
                    "admission must request the runtime-neutral definition"
                );
                match node {
                    Node::Executes => Ok(response.clone()),
                    Node::Refuses => Err(Status::failed_precondition(
                        "execution environment is not allowed on this node",
                    )),
                    Node::Unavailable => Err(Status::unavailable("node-manager is restarting")),
                }
            },
        )
        .unary("GetNetworkPoolStatus", |_: NetworkPoolStatusRequest| {
            Ok(NetworkPoolStatusResponse {
                sandbox_cgroup_memory_mib: 4096,
                sandbox_guest_memory_max_mib: 3968,
                sandbox_memory_headroom_mib: 128,
                ..Default::default()
            })
        })
}

fn single_build(response: GetTemplateResponse) -> Arc<FakeTemplateResolver> {
    Arc::new(FakeTemplateResolver::new(move || {
        let mut response = tonic::Response::new(response.clone());
        pplx_space_common_svc::template_lookup::TemplateLookupScope::LocalExecution
            .receipt(&mut response);
        Ok(response)
    }))
}

/// A fixture whose creates complete: no insert fence, and a test-owned cache.
struct Created {
    fixture: Fixture,
    cache: tempfile::TempDir,
}

impl Created {
    fn new(
        conn: crate::db::Connection,
        templates: Arc<FakeTemplateResolver>,
        node: FakeNodeManager,
    ) -> Self {
        let mut fixture = Fixture::new(conn, templates, node);
        fixture
            .service
            .db_conn()
            .unwrap()
            .execute_batch("DROP TRIGGER create_after_lookup_insert_fence")
            .unwrap();
        let cache = tempfile::tempdir().unwrap();
        fixture.service.create_cache_dir = Some(cache.path().to_owned());
        Self { fixture, cache }
    }

    fn resolutions(&self) -> usize {
        resolutions(&self.fixture)
    }

    async fn shutdown(self) {
        self.fixture.service.shutdown_token.cancel();
        self.fixture.service.task_tracker.close();
        tokio::time::timeout(CREATE_WAIT, self.fixture.service.task_tracker.wait())
            .await
            .unwrap();
    }
}

fn resolutions(fixture: &Fixture) -> usize {
    fixture
        .node_manager
        .calls()
        .iter()
        .filter(|call| *call == RESOLVE)
        .count()
}

type Mutation = fn(&mut CreateRequest);

const CLUSTER_OWNED: (&str, Mutation) = ("cluster-owned sandbox id", |request| {
    request.sandbox_id = Some(Uuid::now_v7().to_string())
});
const ROUTE_IDENTITY: (&str, Mutation) = ("cluster route identity", |request| {
    request.route_identity = Some(Default::default())
});
const POOL_MEMBER: (&str, Mutation) = ("pool member", |request| {
    request.pool_template = Some(TEMPLATE.into())
});
const SNAPSHOTS: (&str, Mutation) = ("snapshots at create", |request| {
    request.snapshots_enabled = true
});

/// The create paths the selection gate used to refuse, other than warm boot.
fn cluster_and_pool_paths() -> [(&'static str, Mutation); 4] {
    [CLUSTER_OWNED, ROUTE_IDENTITY, POOL_MEMBER, SNAPSHOTS]
}

fn cold_request(mutate: Mutation, selected: Option<&EnvironmentDefinition>) -> CreateRequest {
    let mut request = CreateRequest {
        boot_mode: BootMode::Cold as i32,
        ..create_request()
    };
    if let Some(selected) = selected {
        pplx_space_sbx_mgr_client::environment::bind(&mut request, selected.reference());
    }
    mutate(&mut request);
    request
}

/// Create on each of `paths` from a template bound to `template`, against a
/// node executing `node`, and check the persisted row keeps that exact
/// binding. Returns how many exact resolutions each path made.
async fn create_on_every_path(
    paths: &[(&'static str, Mutation)],
    template: &ExecutionMetadata,
    node: &EnvironmentDefinition,
    selected: Option<&EnvironmentDefinition>,
) -> Vec<usize> {
    let mut counts = Vec::new();
    for &(name, mutate) in paths {
        let build = bound_template(template);
        let created = Created::new(
            crate::db::test_conn(),
            build.resolver(BUILD),
            node_manager(node, Node::Executes),
        );
        let request = cold_request(mutate, selected);
        let response = created
            .fixture
            .create(request.clone())
            .await
            .unwrap_or_else(|error| panic!("{name}: {error}"));
        assert_eq!(response.boot_mode_used, BootMode::Cold as i32, "{name}");
        assert_eq!(
            response.environment,
            selected.map(|selected| selected.reference().to_string()),
            "{name}"
        );
        if let Some(requested) = &request.sandbox_id {
            assert_eq!(&response.sandbox_id, requested, "{name}");
        }
        let row = created.fixture.row(&response.sandbox_id).unwrap();
        assert_eq!(row.state, SandboxState::Created, "{name}");
        assert_eq!(row.execution_metadata.as_ref(), Some(template), "{name}");
        assert_eq!(row.pool_template, request.pool_template, "{name}");
        assert_eq!(row.snapshots_enabled, request.snapshots_enabled, "{name}");
        counts.push(created.resolutions());
        created.shutdown().await;
    }
    counts
}

/// Every cluster and pool path refuses before a row, after asking the node
/// to admit the binding once.
async fn assert_refused_on_every_path(template: &ExecutionMetadata, node: Node) {
    for (name, mutate) in cluster_and_pool_paths() {
        let build = bound_template(template);
        let fixture = Fixture::new(
            crate::db::test_conn(),
            build.resolver(BUILD),
            node_manager(&definition(UPGRADE), node),
        );
        let error = fixture
            .create(cold_request(mutate, None))
            .await
            .unwrap_err();
        assert_eq!(
            error.code(),
            tonic::Code::FailedPrecondition,
            "{name}: {error}"
        );
        assert_eq!(resolutions(&fixture), 1, "{name}");
        assert_eq!(fixture.row_count(), 0, "{name}");
    }
}

#[tokio::test]
async fn an_upgrade_template_creates_on_cluster_pool_and_snapshot_paths_with_its_exact_binding() {
    if !enter_isolated_test_process() {
        return;
    }
    let upgrade = definition(UPGRADE);
    assert_eq!(
        create_on_every_path(&cluster_and_pool_paths(), &bound(&upgrade), &upgrade, None).await,
        [1, 1, 1, 1],
        "the node admits the exact binding once per create"
    );
}

#[tokio::test]
async fn explicit_upgrade_selection_takes_cluster_and_snapshot_paths_but_never_makes_a_pool_member()
{
    if !enter_isolated_test_process() {
        return;
    }
    let upgrade = definition(UPGRADE);
    assert_eq!(
        create_on_every_path(
            &[CLUSTER_OWNED, SNAPSHOTS],
            &bound(&upgrade),
            &upgrade,
            Some(&upgrade)
        )
        .await,
        [1, 1],
        "selection is the admission; nothing resolves twice"
    );
    // Pool donors are matched by build, so a member can't carry a binding
    // selected apart from its build's.
    let build = bound_template(&bound(&upgrade));
    let fixture = Fixture::new(
        crate::db::test_conn(),
        build.resolver(BUILD),
        node_manager(&upgrade, Node::Executes),
    );
    let error = fixture
        .create(cold_request(POOL_MEMBER.1, Some(&upgrade)))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{error}");
    assert_eq!(resolutions(&fixture), 0, "refused from the request alone");
    assert!(fixture.templates.requests().is_empty());
    assert_eq!(fixture.row_count(), 0);
    // Selection still can't relabel the template as another environment.
    let other = definition(UPGRADE_WITH_NEW_KERNEL);
    let build = bound_template(&bound(&upgrade));
    let fixture = Fixture::new(
        crate::db::test_conn(),
        build.resolver(BUILD),
        node_manager(&other, Node::Executes),
    );
    let request = cold_request(CLUSTER_OWNED.1, Some(&other));
    assert_eq!(
        fixture.create(request).await.unwrap_err().code(),
        tonic::Code::FailedPrecondition
    );
    assert_eq!(fixture.row_count(), 0);
}

/// E0 bindings keep their existing admission at launch: no node round trip.
#[tokio::test]
async fn an_e0_template_takes_the_cluster_paths_without_node_admission() {
    if !enter_isolated_test_process() {
        return;
    }
    let e0 = definition("e0");
    assert_eq!(
        create_on_every_path(&cluster_and_pool_paths(), &bound(&e0), &e0, None).await,
        [0, 0, 0, 0]
    );
}

#[tokio::test]
async fn a_node_that_does_not_execute_the_upgrade_refuses_every_path_before_a_row() {
    if !enter_isolated_test_process() {
        return;
    }
    let upgrade = definition(UPGRADE);
    let execution = bound(&upgrade);
    assert_refused_on_every_path(&execution, Node::Refuses).await;

    // A donor of a binding the node doesn't execute stays in the pool, and the
    // create goes on without it: here the template's own binding is refused too.
    let conn = crate::db::test_conn();
    let donor = insert_pool_donor(&conn, BUILD);
    crate::db::upsert_sandbox_execution_metadata(&conn, &donor, &execution).unwrap();
    let build = bound_template(&execution);
    let fixture = Fixture::new(
        conn,
        build.resolver(BUILD),
        node_manager(&upgrade, Node::Refuses),
    );
    let before = fixture.row(&donor).unwrap();
    let error = fixture
        .create(CreateRequest {
            boot_mode: BootMode::Pool as i32,
            ..create_request()
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    // Once for the donor, once for the template on the path without the pool.
    assert_eq!(resolutions(&fixture), 2);
    assert_donor_untouched(&fixture, &before);
}

/// The donor is still paused, unclaimed and the build's next donor.
fn assert_donor_untouched(fixture: &Fixture, before: &crate::db::SandboxRow) {
    let after = fixture.row(&before.id).unwrap();
    assert_eq!(after.user, before.user);
    assert_eq!(after.state, before.state);
    let db = fixture.service.db_conn().unwrap();
    assert_eq!(
        crate::db::scan::pool_donor(&db, TEMPLATE, Some(BUILD), None)
            .unwrap()
            .map(|row| row.id),
        Some(before.id.clone())
    );
}

/// Template evidence that names the upgrade but records another kernel can't
/// borrow the node's admission of the upgrade.
#[tokio::test]
async fn upgrade_admission_checks_the_template_evidence_against_the_definition() {
    if !enter_isolated_test_process() {
        return;
    }
    let upgrade = definition(UPGRADE);
    let mut wire = pplx_space_execution_common::proto::ExecutionMetadata::from(&bound(&upgrade));
    wire.components[0].digest = Some(format!("sha256:{}", "00".repeat(32)));
    let forged = ExecutionMetadata::try_from(&wire).unwrap();
    assert_eq!(forged.environment(), Some(upgrade.reference()));
    assert_refused_on_every_path(&forged, Node::Executes).await;
}

#[tokio::test]
async fn an_upgrade_template_adopts_its_pool_donor_but_explicit_selection_does_not() {
    if !enter_isolated_test_process() {
        return;
    }
    let upgrade = definition(UPGRADE);
    let execution = bound(&upgrade);
    for selected in [false, true] {
        let conn = crate::db::test_conn();
        let donor = insert_pool_donor(&conn, BUILD);
        crate::db::upsert_sandbox_execution_metadata(&conn, &donor, &execution).unwrap();
        let build = bound_template(&execution);
        let created = Created::new(
            conn,
            build.resolver(BUILD),
            node_manager(&upgrade, Node::Executes),
        );
        let mut request = CreateRequest {
            boot_mode: BootMode::Pool as i32,
            ..create_request()
        };
        if selected {
            pplx_space_sbx_mgr_client::environment::bind(&mut request, upgrade.reference());
        }
        let response = created.fixture.create(request).await.unwrap();
        assert_eq!(created.resolutions(), 1, "selected={selected}");
        if selected {
            // Donors are matched by build, not binding, so selection boots
            // its own sandbox and reports the binding it verified.
            assert_ne!(response.sandbox_id, donor);
            assert_eq!(response.boot_mode_used, BootMode::Cold as i32);
            assert_eq!(
                response.environment.as_deref(),
                Some(upgrade.reference().to_string().as_str())
            );
            let row = created.fixture.row(&response.sandbox_id).unwrap();
            assert_eq!(row.execution_metadata, Some(execution.clone()));
            assert_eq!(
                created.fixture.row(&donor).unwrap().state,
                SandboxState::Paused
            );
        } else {
            assert_eq!(response.sandbox_id, donor);
            assert_eq!(response.boot_mode_used, BootMode::Pool as i32);
            let adopted = created.fixture.row(&donor).unwrap();
            assert_eq!(adopted.user.as_str(), OWNER);
            assert_eq!(adopted.execution_metadata, Some(execution.clone()));
        }
        created.shutdown().await;
    }
}

/// The producer a template's base snapshot records, if any.
#[derive(Clone, Copy, Debug)]
enum BaseSnapshot {
    CapturedUnder(&'static str),
    WithoutProvenance,
}

impl BaseSnapshot {
    fn write(self, dir: &std::path::Path) {
        std::fs::write(dir.join("vmstate.base"), b"synthetic vmstate").unwrap();
        std::fs::write(dir.join("snapshot.base"), vec![0u8; 1024]).unwrap();
        if let Self::CapturedUnder(name) = self {
            let producer = SnapshotExecutionMetadata::new(bound(&definition(name))).unwrap();
            std::fs::write(
                dir.join("metadata.json"),
                serde_json::to_vec(&producer).unwrap(),
            )
            .unwrap();
        }
    }
}

#[tokio::test]
async fn an_upgrade_template_boots_warm_only_from_a_base_snapshot_captured_under_it() {
    if !enter_isolated_test_process() {
        return;
    }
    let upgrade = definition(UPGRADE);
    let execution = bound(&upgrade);
    for (snapshot, selected, warm) in [
        (BaseSnapshot::CapturedUnder(UPGRADE), false, true),
        // Explicit selection never falls back, so it never restores through a
        // loader the node picks for the snapshot.
        (BaseSnapshot::CapturedUnder(UPGRADE), true, false),
        (BaseSnapshot::CapturedUnder("e0"), false, false),
        (
            BaseSnapshot::CapturedUnder(UPGRADE_WITH_NEW_KERNEL),
            false,
            false,
        ),
        (BaseSnapshot::WithoutProvenance, false, false),
    ] {
        let build = bound_template(&execution);
        let snapshot_dir = tempfile::tempdir().unwrap();
        snapshot.write(snapshot_dir.path());
        let mut response = build.response(BUILD);
        response.snapshot_dir = Some(snapshot_dir.path().to_str().unwrap().into());
        let created = Created::new(
            crate::db::test_conn(),
            single_build(response),
            node_manager(&upgrade, Node::Executes),
        );
        let id = Uuid::now_v7().to_string();
        let mut request = CreateRequest {
            boot_mode: BootMode::BaseSnapshot as i32,
            sandbox_id: Some(id.clone()),
            ..create_request()
        };
        if selected {
            pplx_space_sbx_mgr_client::environment::bind(&mut request, upgrade.reference());
        }
        let response = created
            .fixture
            .create(request)
            .await
            .unwrap_or_else(|error| panic!("{snapshot:?}: {error}"));
        let staged = created.cache.path().join(&id).join("snapshot");
        if warm {
            assert_eq!(response.boot_mode_used, BootMode::BaseSnapshot as i32);
            // Restore selects its consumer from this staged producer.
            let runtime: SnapshotExecutionMetadata =
                serde_json::from_slice(&std::fs::read(staged.join("runtime.json")).unwrap())
                    .unwrap();
            assert_eq!(runtime.execution().environment(), Some(upgrade.reference()));
            assert!(staged.join("vmstate").exists());
            assert!(staged.join("memory").exists());
        } else {
            assert_eq!(
                response.boot_mode_used,
                BootMode::Cold as i32,
                "{snapshot:?}"
            );
            assert!(!staged.exists(), "{snapshot:?}");
        }
        let row = created.fixture.row(&id).unwrap();
        assert_eq!(
            row.execution_metadata,
            Some(execution.clone()),
            "{snapshot:?}"
        );
        assert_eq!(created.resolutions(), 1, "{snapshot:?}");
        created.shutdown().await;
    }
}

/// A retried cluster create reuses its row only when an explicit selection
/// names the binding the row already saved.
#[tokio::test]
async fn a_requested_id_retry_reuses_only_a_row_with_the_selected_binding() {
    if !enter_isolated_test_process() {
        return;
    }
    let upgrade = definition(UPGRADE);
    for (saved, reused) in [(bound(&upgrade), true), (bound(&definition("e0")), false)] {
        let conn = crate::db::test_conn();
        let id = Uuid::now_v7().to_string();
        insert_created_row(
            &conn,
            &id,
            SandboxState::Created,
            BUILD,
            &SandboxOwner::parse(OWNER).unwrap(),
            None,
        );
        crate::db::upsert_sandbox_execution_metadata(&conn, &id, &saved).unwrap();
        let build = bound_template(&bound(&upgrade));
        let fixture = Fixture::new(
            conn,
            build.resolver(BUILD),
            node_manager(&upgrade, Node::Executes),
        );
        let mut request = cold_request(|_| {}, Some(&upgrade));
        request.sandbox_id = Some(id.clone());
        let result = fixture.create(request).await;
        if reused {
            let response = result.unwrap();
            assert!(response.reused_existing);
            assert_eq!(response.sandbox_id, id);
            // The selected client requires the reuse to acknowledge it too.
            assert_eq!(
                response.environment.as_deref(),
                Some(upgrade.reference().to_string().as_str())
            );
        } else {
            let error = result.unwrap_err();
            assert_eq!(error.code(), tonic::Code::AlreadyExists, "{error}");
            assert!(error.message().contains("environment"), "{error}");
        }
        assert_eq!(
            fixture.row(&id).unwrap().execution_metadata,
            Some(saved.clone())
        );
        assert_eq!(fixture.row_count(), 1);
    }
}

/// A pool refill clones a paused donor. The clone keeps the donor's exact
/// binding, and a non-default one must still be executed by the node.
#[tokio::test]
async fn a_pool_clone_saves_and_admits_its_donors_binding() {
    use pplx_space_node_mgr_client::proto::{CloneCapabilitiesRequest, CloneCapabilitiesResponse};
    const CLONE_EXECUTION: &str = "clone execution: ";
    if !enter_isolated_test_process() {
        return;
    }
    for (name, node) in [
        (UPGRADE, Node::Executes),
        (UPGRADE, Node::Refuses),
        ("e0", Node::Refuses),
    ] {
        let environment = definition(name);
        let execution = bound(&environment);
        let conn = crate::db::test_conn();
        let donor = insert_pool_donor(&conn, BUILD);
        crate::db::upsert_sandbox_execution_metadata(&conn, &donor, &execution).unwrap();
        // The template names another binding, so only the donor's can explain
        // what the clone saves and what the node is asked to admit.
        let other = if name == UPGRADE { "e0" } else { UPGRADE };
        let build = bound_template(&bound(&definition(other)));
        let fixture = Fixture::new(
            conn,
            build.resolver(BUILD),
            node_manager(&environment, node).unary(
                "GetCloneCapabilities",
                |_: CloneCapabilitiesRequest| {
                    Ok(CloneCapabilitiesResponse {
                        prepared_clone_disposal: true,
                    })
                },
            ),
        );
        // Stop at the clone row's binding write and report what it saved.
        fixture
            .service
            .db_conn()
            .unwrap()
            .execute_batch(&format!(
                "DROP TRIGGER create_after_lookup_insert_fence;
                 CREATE TEMP TRIGGER clone_execution_fence
                 BEFORE UPDATE OF execution_metadata_json ON main.sandboxes
                 WHEN NEW.id != '{donor}'
                 BEGIN
                     SELECT RAISE(ABORT, '{CLONE_EXECUTION}' || NEW.execution_metadata_json);
                 END;"
            ))
            .unwrap();
        let error = tokio::time::timeout(
            CREATE_WAIT,
            fixture
                .service
                .clone_from_donor(internal_request(CloneFromDonorRequest {
                    donor_sandbox_id: donor.clone(),
                    template: TEMPLATE.into(),
                    owner: OWNER.into(),
                    pool_template: Some(TEMPLATE.into()),
                })),
        )
        .await
        .unwrap()
        .unwrap_err();
        let context = format!("{name} {node:?}");
        let non_default = name == UPGRADE;
        if non_default && node == Node::Refuses {
            assert_eq!(
                error.code(),
                tonic::Code::FailedPrecondition,
                "{context}: {error}"
            );
            assert!(
                error.message().contains("not allowed"),
                "{context}: {error}"
            );
        } else {
            let (_, saved) = error
                .message()
                .split_once(CLONE_EXECUTION)
                .unwrap_or_else(|| panic!("{context}: clone saved no binding: {error}"));
            assert_eq!(
                serde_json::from_str::<ExecutionMetadata>(saved).unwrap(),
                execution,
                "{context}"
            );
        }
        assert_eq!(resolutions(&fixture), usize::from(non_default), "{context}");
        assert_eq!(fixture.row_count(), 1, "{context}: only the donor remains");
    }
}

/// Adoption admits the donor it claims, not the template: a warm pool fill can
/// leave a donor on a loader other than its template's binding.
#[tokio::test]
async fn pool_adoption_admits_the_donors_own_binding() {
    if !enter_isolated_test_process() {
        return;
    }
    let upgrade = definition(UPGRADE);
    let donor_execution = bound(&upgrade);
    for node in [Node::Refuses, Node::Executes] {
        let conn = crate::db::test_conn();
        let donor = insert_pool_donor(&conn, BUILD);
        crate::db::upsert_sandbox_execution_metadata(&conn, &donor, &donor_execution).unwrap();
        // The template names E0, which needs no admission at all.
        let build = bound_template(&bound(&definition("e0")));
        let created = Created::new(conn, build.resolver(BUILD), node_manager(&upgrade, node));
        let before = created.fixture.row(&donor).unwrap();
        let result = created
            .fixture
            .create(CreateRequest {
                boot_mode: BootMode::Pool as i32,
                ..create_request()
            })
            .await;
        assert_eq!(created.resolutions(), 1, "{node:?}");
        match node {
            Node::Refuses => {
                // Pool is an optimization: the create boots its own E0 sandbox
                // and leaves the refused donor in the pool.
                let response = result.unwrap();
                assert_ne!(response.sandbox_id, donor);
                assert_eq!(response.boot_mode_used, BootMode::Cold as i32);
                assert_donor_untouched(&created.fixture, &before);
            }
            Node::Executes => {
                assert_eq!(result.unwrap().sandbox_id, donor);
                let after = created.fixture.row(&donor).unwrap();
                assert_eq!(after.user.as_str(), OWNER);
                assert_eq!(after.execution_metadata, Some(donor_execution.clone()));
            }
            Node::Unavailable => unreachable!("covered by its own test"),
        }
        created.shutdown().await;
    }
}

/// Warm Start restores through the base snapshot's producer too, so it refuses
/// one captured under another binding rather than moving the sandbox.
#[tokio::test]
async fn warm_start_refuses_a_base_snapshot_captured_under_another_environment() {
    if !enter_isolated_test_process() {
        return;
    }
    let upgrade = definition(UPGRADE);
    for (snapshot, saved, refused) in [
        (BaseSnapshot::CapturedUnder("e0"), true, true),
        (BaseSnapshot::WithoutProvenance, true, true),
        (BaseSnapshot::CapturedUnder(UPGRADE), true, false),
        // A row that predates saved bindings ran the initial environment.
        (BaseSnapshot::CapturedUnder(UPGRADE), false, true),
    ] {
        let conn = crate::db::test_conn();
        let id = Uuid::now_v7().to_string();
        insert_created_row(
            &conn,
            &id,
            SandboxState::Stopped,
            BUILD,
            &SandboxOwner::parse(OWNER).unwrap(),
            None,
        );
        if saved {
            crate::db::upsert_sandbox_execution_metadata(&conn, &id, &bound(&upgrade)).unwrap();
        }
        let build = bound_template(&bound(&upgrade));
        let snapshot_dir = tempfile::tempdir().unwrap();
        snapshot.write(snapshot_dir.path());
        let mut response = build.response(BUILD);
        response.snapshot_dir = Some(snapshot_dir.path().to_str().unwrap().into());
        let created = Created::new(
            conn,
            single_build(response),
            node_manager(&upgrade, Node::Executes),
        );
        let fixture = &created.fixture;
        let error = tokio::time::timeout(
            CREATE_WAIT,
            fixture.service.start_sandbox(
                StartRequest {
                    sandbox_id: id.clone(),
                    from_warm: true,
                    ..Default::default()
                },
                false,
            ),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert_eq!(
            error
                .message()
                .contains("captured under another execution environment"),
            refused,
            "{snapshot:?}: {error}"
        );
        // A matching snapshot is staged under the test's cache root, and the
        // restore that follows fails later at the node; a refusal stages nothing.
        let staged = created.cache.path().join(&id).join("snapshot");
        assert_eq!(
            staged.join("runtime.json").exists(),
            !refused,
            "{snapshot:?}"
        );
        if refused {
            assert_eq!(
                fixture.row(&id).unwrap().state,
                SandboxState::Stopped,
                "{snapshot:?}"
            );
        }
        created.shutdown().await;
    }
}

/// Adoption passes over an older donor whose binding the node refuses and
/// takes the next one it admits; the refused donor is left untouched.
#[tokio::test]
async fn pool_adoption_skips_a_refused_donor_for_the_next_admitted_one() {
    if !enter_isolated_test_process() {
        return;
    }
    let upgrade = definition(UPGRADE);
    let conn = crate::db::test_conn();
    let refused = insert_pool_donor(&conn, BUILD);
    crate::db::upsert_sandbox_execution_metadata(&conn, &refused, &bound(&upgrade)).unwrap();
    // A second donor with the same evidence is skipped without asking again.
    let refused_twin = insert_pool_donor(&conn, BUILD);
    crate::db::upsert_sandbox_execution_metadata(&conn, &refused_twin, &bound(&upgrade)).unwrap();
    let admitted = insert_pool_donor(&conn, BUILD);
    let e0 = bound(&definition("e0"));
    crate::db::upsert_sandbox_execution_metadata(&conn, &admitted, &e0).unwrap();
    let build = bound_template(&e0);
    let created = Created::new(
        conn,
        build.resolver(BUILD),
        node_manager(&upgrade, Node::Refuses),
    );
    let before = created.fixture.row(&refused).unwrap();
    let response = created
        .fixture
        .create(CreateRequest {
            boot_mode: BootMode::Pool as i32,
            ..create_request()
        })
        .await
        .unwrap();
    assert_eq!(response.sandbox_id, admitted);
    assert_eq!(response.boot_mode_used, BootMode::Pool as i32);
    assert_eq!(
        created.resolutions(),
        1,
        "the refused binding is asked once"
    );
    assert_eq!(created.fixture.row(&admitted).unwrap().user.as_str(), OWNER);
    assert_donor_untouched(&created.fixture, &before);
    created.shutdown().await;
}

/// The AdoptPoolSandbox RPC reports a pool whose donors the node all refuses
/// the way it reports an empty one, which its callers boot past.
#[tokio::test]
async fn the_adoption_rpc_reports_an_all_refused_pool_as_exhausted() {
    if !enter_isolated_test_process() {
        return;
    }
    let upgrade = definition(UPGRADE);
    let conn = crate::db::test_conn();
    let donor = insert_pool_donor(&conn, BUILD);
    crate::db::upsert_sandbox_execution_metadata(&conn, &donor, &bound(&upgrade)).unwrap();
    let build = bound_template(&bound(&upgrade));
    let fixture = Fixture::new(
        conn,
        build.resolver(BUILD),
        node_manager(&upgrade, Node::Refuses),
    );
    let before = fixture.row(&donor).unwrap();
    let request = create_request();
    let error = tokio::time::timeout(
        CREATE_WAIT,
        fixture
            .service
            .adopt_pool_sandbox(internal_request(super::pool_adoption_request(
                &request,
                TEMPLATE.into(),
                OWNER.into(),
                (&AutoLifecyclePolicy::disabled()).into(),
                crate::db::PolicyAuthority::Cluster.as_proto_i32(),
                Some(BUILD.into()),
            ))),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert_eq!(error.code(), tonic::Code::ResourceExhausted, "{error}");
    assert_donor_untouched(&fixture, &before);
}

/// Only a node refusal skips a donor. A failure to ask is the caller's to retry,
/// not a reason to boot without the pool, and it leaves the donor untouched.
#[tokio::test]
async fn pool_adoption_returns_a_failure_to_ask_instead_of_skipping_the_donor() {
    if !enter_isolated_test_process() {
        return;
    }
    let upgrade = definition(UPGRADE);
    let conn = crate::db::test_conn();
    let donor = insert_pool_donor(&conn, BUILD);
    crate::db::upsert_sandbox_execution_metadata(&conn, &donor, &bound(&upgrade)).unwrap();
    let build = bound_template(&bound(&definition("e0")));
    let fixture = Fixture::new(
        conn,
        build.resolver(BUILD),
        node_manager(&upgrade, Node::Unavailable),
    );
    let before = fixture.row(&donor).unwrap();
    let error = fixture
        .create(CreateRequest {
            boot_mode: BootMode::Pool as i32,
            ..create_request()
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::Unavailable, "{error}");
    assert_eq!(fixture.row_count(), 1, "no sandbox booted past the pool");
    assert_donor_untouched(&fixture, &before);
}

/// Refusals are remembered by the evidence the node judged: a donor with
/// forged evidence doesn't hide a later one that names the same environment
/// with the evidence the node admits.
#[tokio::test]
async fn pool_adoption_judges_each_donors_own_evidence() {
    if !enter_isolated_test_process() {
        return;
    }
    let upgrade = definition(UPGRADE);
    let mut wire = pplx_space_execution_common::proto::ExecutionMetadata::from(&bound(&upgrade));
    wire.components[0].digest = Some(format!("sha256:{}", "00".repeat(32)));
    let forged = ExecutionMetadata::try_from(&wire).unwrap();
    let conn = crate::db::test_conn();
    let forged_donor = insert_pool_donor(&conn, BUILD);
    crate::db::upsert_sandbox_execution_metadata(&conn, &forged_donor, &forged).unwrap();
    let valid_donor = insert_pool_donor(&conn, BUILD);
    crate::db::upsert_sandbox_execution_metadata(&conn, &valid_donor, &bound(&upgrade)).unwrap();
    let build = bound_template(&bound(&upgrade));
    let created = Created::new(
        conn,
        build.resolver(BUILD),
        node_manager(&upgrade, Node::Executes),
    );
    let before = created.fixture.row(&forged_donor).unwrap();
    let response = created
        .fixture
        .create(CreateRequest {
            boot_mode: BootMode::Pool as i32,
            ..create_request()
        })
        .await
        .unwrap();
    assert_eq!(response.sandbox_id, valid_donor);
    assert_eq!(created.resolutions(), 2);
    assert_eq!(
        created.fixture.row(&forged_donor).unwrap().user,
        before.user
    );
    created.shutdown().await;
}

/// A concurrent adoption can win the oldest donor while admission is awaited:
/// adoption then claims the next one without asking the node again, and boots
/// without the pool once every donor is taken.
#[tokio::test]
async fn pool_adoption_moves_past_a_donor_taken_concurrently() {
    if !enter_isolated_test_process() {
        return;
    }
    let upgrade = definition(UPGRADE);
    for taken in [1, 2] {
        let conn = crate::db::test_conn();
        let donors: Vec<String> = (0..2)
            .map(|_| {
                let donor = insert_pool_donor(&conn, BUILD);
                crate::db::upsert_sandbox_execution_metadata(&conn, &donor, &bound(&upgrade))
                    .unwrap();
                donor
            })
            .collect();
        let build = bound_template(&bound(&upgrade));
        let created = Created::new(
            conn,
            build.resolver(BUILD),
            node_manager(&upgrade, Node::Executes),
        );
        // A held wake lock is how a concurrent adoption's claim looks here.
        let locks: Vec<_> = donors[..taken]
            .iter()
            .map(|donor| created.fixture.service.auto_lifecycle.wake_lock(donor))
            .collect();
        let held: Vec<_> = locks.iter().map(|lock| lock.try_lock().unwrap()).collect();
        let response = created
            .fixture
            .create(CreateRequest {
                boot_mode: BootMode::Pool as i32,
                ..create_request()
            })
            .await
            .unwrap();
        if taken == 1 {
            assert_eq!(response.sandbox_id, donors[1]);
            assert_eq!(response.boot_mode_used, BootMode::Pool as i32);
            assert_eq!(created.resolutions(), 1, "the admission is reused");
        } else {
            assert!(!donors.contains(&response.sandbox_id));
            assert_eq!(response.boot_mode_used, BootMode::Cold as i32);
            // Once for the donors' binding, once for the template's.
            assert_eq!(created.resolutions(), 2);
        }
        drop(held);
        created.shutdown().await;
    }
}
