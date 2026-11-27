//! Create with `required_environment`, as the shim sends it for a bound
//! Kubernetes pod while the environment placement gate is on, against a
//! template-manager that holds builds of one digest under two environments
//! and answers a lookup's environment preference as `find_ready_build` does.

use super::create_after_lookup_tests::{
    BUILD, OWNER, TEMPLATE, TemplateBuild, create_request, enter_isolated_test_process,
    insert_created_row, insert_pool_donor,
};
use super::create_environment_tests::{
    Created, Node, UPGRADE, bound, bound_template, definition, node_manager,
};
use super::*;
use crate::template_resolver::fake::FakeTemplateResolver;
use pplx_space_execution_common::environment::EnvironmentDefinition;
use pplx_space_template_mgr_client::proto::TemplateRef;

/// The environment a pod requires that isn't the initial one: the catalog's
/// Firecracker 1.17 upgrade of E0's kernel, which caches its builds in its
/// own partition.
fn e1() -> EnvironmentDefinition {
    definition(UPGRADE)
}

/// The initial environment, whose builds share the legacy partition with
/// unbound ones.
fn e0() -> EnvironmentDefinition {
    definition("e0")
}

/// What one node's template-manager holds of [`BUILD`].
struct Holdings {
    /// The legacy-partition build: unbound, as every build predating
    /// environment bindings is.
    legacy: TemplateBuild,
    /// The build bound to [`e1`], if the node holds one.
    e1: Option<TemplateBuild>,
}

impl Holdings {
    fn both() -> Self {
        Self {
            legacy: TemplateBuild::new(),
            e1: Some(bound_template(&bound(&e1()))),
        }
    }

    fn legacy_only() -> Self {
        Self {
            legacy: TemplateBuild::new(),
            e1: None,
        }
    }

    /// A template-manager answering a lookup that prefers E1 with the E1
    /// build when it holds one, and everything else with the legacy build:
    /// `find_ready_build`'s order, preferred partition first, then legacy.
    fn resolver(&self) -> Arc<FakeTemplateResolver> {
        let e1_reference = e1().reference().to_string();
        let legacy = self.legacy.response(BUILD);
        let bound = self.e1.as_ref().map(|build| {
            let mut response = build.response(BUILD);
            if let Some(template) = response.template.as_mut() {
                template.environment = Some(e1_reference.clone());
            }
            response
        });
        Arc::new(FakeTemplateResolver::answering(move |request| {
            let preferred = request
                .template
                .as_ref()
                .and_then(|template| template.environment.as_deref());
            let response = match (&bound, preferred) {
                (Some(bound), Some(preferred)) if preferred == e1_reference => bound.clone(),
                _ => legacy.clone(),
            };
            let mut response = tonic::Response::new(response);
            pplx_space_template_mgr_client::lookup_scope::TemplateLookupScope::LocalExecution
                .receipt(&mut response);
            Ok(response)
        }))
    }
}

/// A cold create the way the shim sends one for a pod: an unspecified boot
/// mode, the control plane's sandbox id and route identity, the pod's
/// required environment, and no runtime, which the required build's binding
/// names.
fn pod_create(required: &EnvironmentDefinition) -> CreateRequest {
    CreateRequest {
        runtime: String::new(),
        boot_mode: BootMode::Unspecified as i32,
        sandbox_id: Some(Uuid::now_v7().to_string()),
        route_identity: Some(Default::default()),
        content_hash: Some(BUILD.into()),
        required_environment: Some(required.reference().to_string()),
        ..create_request()
    }
}

fn lookup_preferences(created: &Created) -> Vec<Option<String>> {
    created
        .fixture
        .templates
        .requests()
        .into_iter()
        .map(|request| {
            request
                .template
                .and_then(|template: TemplateRef| template.environment)
        })
        .collect()
}

/// The issue's acceptance: a node holding an E0 legacy build and an E1 build
/// of the same digest boots the E1 build for a pod that requires E1.
#[tokio::test]
async fn a_pod_requiring_e1_boots_the_e1_build_of_a_digest_the_node_also_holds_under_e0() {
    if !enter_isolated_test_process() {
        return;
    }
    let holdings = Holdings::both();
    let created = Created::new(
        crate::db::test_conn(),
        holdings.resolver(),
        node_manager(&e1(), Node::Executes),
    );
    let request = pod_create(&e1());
    let response = created.fixture.create(request.clone()).await.unwrap();
    assert_eq!(Some(&response.sandbox_id), request.sandbox_id.as_ref());
    let row = created.fixture.row(&response.sandbox_id).unwrap();
    assert_eq!(
        row.execution_metadata,
        Some(bound(&e1())),
        "the sandbox must run the E1 build, not the legacy one"
    );
    assert_eq!(
        row.runtime,
        pplx_space_execution_spec::RUNTIME_FIRECRACKER_ALIAS,
        "the runtime comes from the build's binding"
    );
    assert_eq!(
        lookup_preferences(&created),
        [Some(e1().reference().to_string())],
        "the lookup must prefer the required environment's build"
    );
    assert!(
        created
            .fixture
            .templates
            .requests()
            .iter()
            .all(|request| request.environment_required),
        "template-manager must answer only with the required environment's build"
    );
    created.shutdown().await;
}

/// A required environment is a constraint, not only a preference: a node
/// whose template-manager answers with the digest's build in another
/// partition, as one predating `environment_required` does, refuses the
/// create rather than boot that build in the required one's place. A current
/// template-manager pulls instead, or answers NOT_FOUND.
#[tokio::test]
async fn a_pod_requiring_e1_is_refused_on_a_node_holding_the_digest_only_under_e0() {
    if !enter_isolated_test_process() {
        return;
    }
    let holdings = Holdings::legacy_only();
    let created = Created::new(
        crate::db::test_conn(),
        holdings.resolver(),
        node_manager(&e1(), Node::Executes),
    );
    let error = created.fixture.create(pod_create(&e1())).await.unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{error}");
    assert!(
        error.message().contains(&e1().reference().to_string()),
        "the refusal names the required environment: {error}"
    );
    assert_eq!(created.fixture.row_count(), 0);
    created.shutdown().await;
}

/// E0 caches its builds in the legacy partition, which unbound builds share,
/// so a pod requiring E0 boots the node's unbound build of the digest.
#[tokio::test]
async fn a_pod_requiring_e0_boots_an_unbound_legacy_build() {
    if !enter_isolated_test_process() {
        return;
    }
    let holdings = Holdings::both();
    let created = Created::new(
        crate::db::test_conn(),
        holdings.resolver(),
        node_manager(&e1(), Node::Executes),
    );
    let response = created.fixture.create(pod_create(&e0())).await.unwrap();
    let row = created.fixture.row(&response.sandbox_id).unwrap();
    assert!(
        row.execution_metadata
            .as_ref()
            .and_then(|execution| execution.environment())
            .is_none(),
        "the unbound legacy build boots: {:?}",
        row.execution_metadata
    );
    assert_eq!(
        lookup_preferences(&created),
        [Some(e0().reference().to_string())]
    );
    created.shutdown().await;
}

/// A node-local create that requires an environment keeps the build's exact
/// binding: compatible selection could move it to another environment. The
/// node-manager fake fails the test if it's asked to select one.
#[tokio::test]
async fn a_node_local_create_requiring_e1_keeps_the_builds_binding() {
    if !enter_isolated_test_process() {
        return;
    }
    let holdings = Holdings::both();
    let created = Created::new(
        crate::db::test_conn(),
        holdings.resolver(),
        node_manager(&e1(), Node::Executes),
    );
    let request = CreateRequest {
        sandbox_id: None,
        route_identity: None,
        ..pod_create(&e1())
    };
    let response = created.fixture.create(request).await.unwrap();
    let row = created.fixture.row(&response.sandbox_id).unwrap();
    assert_eq!(row.execution_metadata, Some(bound(&e1())));
    created.shutdown().await;
}

/// Pool donors are matched by template, not by the partition of the build
/// they were created from, so a create that requires an environment never
/// adopts one: here a legacy donor would boot E0 in E1's place.
#[tokio::test]
async fn a_pool_create_requiring_e1_boots_its_own_sandbox_instead_of_a_legacy_donor() {
    if !enter_isolated_test_process() {
        return;
    }
    let holdings = Holdings::both();
    let conn = crate::db::test_conn();
    let donor = insert_pool_donor(&conn, BUILD);
    let created = Created::new(
        conn,
        holdings.resolver(),
        node_manager(&e1(), Node::Executes),
    );
    let request = CreateRequest {
        boot_mode: BootMode::Pool as i32,
        sandbox_id: None,
        route_identity: None,
        ..pod_create(&e1())
    };
    let response = created.fixture.create(request).await.unwrap();
    assert_ne!(response.sandbox_id, donor);
    assert_eq!(response.boot_mode_used, BootMode::Cold as i32);
    let row = created.fixture.row(&response.sandbox_id).unwrap();
    assert_eq!(row.execution_metadata, Some(bound(&e1())));
    assert_eq!(
        created.fixture.row(&donor).unwrap().state,
        SandboxStatus::from(SandboxState::Paused),
        "the donor stays in the pool"
    );
    created.shutdown().await;
}

/// A retried create for a sandbox the node already holds under another
/// partition's binding is a different create, not a reuse.
#[tokio::test]
async fn a_retry_requiring_e1_refuses_a_row_created_from_the_legacy_build() {
    if !enter_isolated_test_process() {
        return;
    }
    let holdings = Holdings::both();
    let created = Created::new(
        crate::db::test_conn(),
        holdings.resolver(),
        node_manager(&e1(), Node::Executes),
    );
    let request = pod_create(&e1());
    let id = request.sandbox_id.clone().unwrap();
    insert_created_row(
        &created.fixture.service.db_conn().unwrap(),
        &id,
        SandboxState::Created,
        BUILD,
        &SandboxOwner::parse(OWNER).unwrap(),
        None,
    );
    let error = created.fixture.create(request).await.unwrap_err();
    assert_eq!(error.code(), tonic::Code::AlreadyExists, "{error}");
    assert!(error.message().contains("required_environment"), "{error}");
    created.shutdown().await;
}

#[tokio::test]
async fn a_malformed_conflicting_or_digestless_required_environment_is_refused_before_lookup() {
    if !enter_isolated_test_process() {
        return;
    }
    let malformed = CreateRequest {
        required_environment: Some("not-a-reference".into()),
        ..pod_create(&e1())
    };
    let without_digest = CreateRequest {
        content_hash: None,
        ..pod_create(&e1())
    };
    let with_runtime = CreateRequest {
        runtime: pplx_space_execution_spec::RUNTIME_FIRECRACKER_ALIAS.into(),
        ..pod_create(&e1())
    };
    let pool_member = CreateRequest {
        pool_template: Some(TEMPLATE.into()),
        sandbox_id: None,
        route_identity: None,
        ..pod_create(&e1())
    };
    let mut selected = pod_create(&e1());
    pplx_space_sbx_mgr_client::environment::bind(&mut selected, e1().reference());
    for (name, request) in [
        ("malformed", malformed),
        ("without a digest", without_digest),
        ("with a runtime", with_runtime),
        ("as a pool member", pool_member),
        ("with selection", selected),
    ] {
        let holdings = Holdings::both();
        let created = Created::new(
            crate::db::test_conn(),
            holdings.resolver(),
            node_manager(&e1(), Node::Executes),
        );
        let error = created.fixture.create(request).await.unwrap_err();
        assert_eq!(
            error.code(),
            tonic::Code::InvalidArgument,
            "{name}: {error}"
        );
        assert!(created.fixture.templates.requests().is_empty(), "{name}");
        created.shutdown().await;
    }
}

/// A sandbox-manager that predates `required_environment` drops it, and then
/// sees a create that names no runtime. It refuses that before any template
/// lookup, so a rollback between the shim's capability probe and its create
/// can't boot the manager's preferred build in the required one's place.
/// This is the request the shim sends, as such a manager decodes it.
#[tokio::test]
async fn a_required_create_without_its_field_is_refused_for_want_of_a_runtime() {
    if !enter_isolated_test_process() {
        return;
    }
    let holdings = Holdings::both();
    let created = Created::new(
        crate::db::test_conn(),
        holdings.resolver(),
        node_manager(&e1(), Node::Executes),
    );
    let as_an_older_manager_reads_it = CreateRequest {
        required_environment: None,
        ..pod_create(&e1())
    };
    let error = created
        .fixture
        .create(as_an_older_manager_reads_it)
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument, "{error}");
    assert!(error.message().contains("runtime is required"), "{error}");
    assert!(created.fixture.templates.requests().is_empty());
    assert_eq!(created.fixture.row_count(), 0);
    created.shutdown().await;
}
