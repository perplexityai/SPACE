use pplx_space_common_core::types::UserId;
use pplx_space_common_core::types::{OrgId, SandboxId};
use uuid::Uuid;

use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;

use k8s_openapi::api::core::v1::{NodeSelectorRequirement, PodCondition, PodSpec, PodStatus};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use pplx_space_cluster_pool::{PoolSlot, PoolSlotState};
use pplx_space_common_core::kubernetes::{
    DEFAULT_SANDBOX_INTERNAL_CONTROL_TOKEN_KEY, DEFAULT_SANDBOX_INTERNAL_SECRET_NAME,
    DEFAULT_SANDBOX_INTERNAL_TOKEN_KEY, DEFAULT_SANDBOX_POD_IMAGE, DEFAULT_SANDBOX_RUNTIME_CLASS,
    PodToleration, SANDBOX_ACTIVATION_ATTEMPT_ID_ANNOTATION, SANDBOX_AUTO_LIFECYCLE_ANNOTATION,
    SANDBOX_AUTO_LIFECYCLE_AUTHORITY_ANNOTATION, SANDBOX_CONTENT_HASH_ANNOTATION,
    SANDBOX_DISK_MIB_ANNOTATION, SANDBOX_MANAGED_BY_LABEL, SANDBOX_MEMORY_MIB_ANNOTATION,
    SANDBOX_NETWORK_SLOTS_RESOURCE, SANDBOX_NODE_GROUP_LABEL, SANDBOX_POOL_CONFIG_ID_KEY,
    SANDBOX_POOL_ROLE_KEY, SANDBOX_POOL_ROLE_WARM, SANDBOX_POOL_SLOT_ID_KEY,
    SANDBOX_TEMPLATE_ANNOTATION, SANDBOX_VCPU_ANNOTATION, SandboxRuntimePodMetadata,
};
use pplx_space_common_core::types::{
    ContentHash, Digest, MemorySize, NodeGroup, NodeGroupId, NodeGroupName, NodeGroupPolicy,
    PolicyAuthority, SandboxOwner, TemplateCompatSpec, TemplateName, TemplateVisibility, VcpuCount,
};
use time::OffsetDateTime;

use super::*;

#[test]
fn build_pool_pod_sets_sandbox_and_pool_metadata() {
    let slot = pool_slot();
    let config = pool_pod_config();

    let pod = build_pool_pod(&config, &slot, None);

    assert_eq!(pod.metadata.name.as_deref(), Some("space-sbx-test-g1"));
    assert_eq!(pod.metadata.namespace.as_deref(), Some("space"));
    let labels = pod.metadata.labels.as_ref().unwrap();
    assert_eq!(labels.get(SANDBOX_MANAGED_BY_LABEL).unwrap(), "space-api");
    assert_eq!(
        labels.get(SANDBOX_POOL_ROLE_KEY).unwrap(),
        SANDBOX_POOL_ROLE_WARM
    );
    assert_eq!(
        labels.get(SANDBOX_POOL_CONFIG_ID_KEY).unwrap(),
        &slot.config_id.to_string()
    );
    assert_eq!(labels.get(SANDBOX_POOL_SLOT_ID_KEY), None);
    let annotations = pod.metadata.annotations.as_ref().unwrap();
    assert_eq!(
        annotations.get(SANDBOX_POOL_SLOT_ID_KEY).unwrap(),
        &slot.id.to_string()
    );
    assert_eq!(
        annotations
            .get(SANDBOX_ACTIVATION_ATTEMPT_ID_ANNOTATION)
            .unwrap(),
        &slot.id.to_string()
    );
    assert_eq!(
        annotations.get(SANDBOX_TEMPLATE_ANNOTATION).unwrap(),
        "seed"
    );
    assert_eq!(
        annotations.get(SANDBOX_CONTENT_HASH_ANNOTATION).unwrap(),
        &content_hash().to_string()
    );
    assert_eq!(annotations.get(SANDBOX_VCPU_ANNOTATION).unwrap(), "2");
    assert_eq!(
        annotations.get(SANDBOX_MEMORY_MIB_ANNOTATION).unwrap(),
        "8192"
    );
    assert_eq!(
        annotations.get(SANDBOX_AUTO_LIFECYCLE_ANNOTATION).unwrap(),
        "{}"
    );
    assert_eq!(
        annotations
            .get(SANDBOX_AUTO_LIFECYCLE_AUTHORITY_ANNOTATION)
            .unwrap(),
        "cluster"
    );

    let spec = pod.spec.as_ref().unwrap();
    assert_eq!(spec.runtime_class_name.as_deref(), Some("space"));
    // Warm-pool pods occupy a real node-manager network slot, so they
    // request the extended resource just like on-demand sandbox pods.
    let resources = spec.containers[0].resources.as_ref().unwrap();
    assert_eq!(
        resources
            .requests
            .as_ref()
            .and_then(|requests| requests.get(SANDBOX_NETWORK_SLOTS_RESOURCE))
            .map(|quantity| quantity.0.as_str()),
        Some("1")
    );
    assert_eq!(
        resources
            .limits
            .as_ref()
            .and_then(|limits| limits.get(SANDBOX_NETWORK_SLOTS_RESOURCE))
            .map(|quantity| quantity.0.as_str()),
        Some("1")
    );
    assert_eq!(
        spec.node_selector
            .as_ref()
            .unwrap()
            .get("space.pplx.net/node-pool")
            .unwrap(),
        "workers"
    );
}

/// A warm pod's node-group boundary has to reach the PodSpec. `None` (the gate
/// off, or a cluster with no generic group) must render nothing at all — a
/// required term over a label the workers do not carry would strand every warm
/// slot `Pending` — and a resolved set must render the required term.
///
/// A pooled pod carries no org, and the builder answers an org-less pod that
/// claims *dedicated* capacity with an unsatisfiable boundary, so asserting the
/// single `In` term also asserts the caller resolved a shared-only set.
#[test]
fn build_pool_pod_renders_the_node_group_boundary_only_when_resolved() {
    let slot = pool_slot();
    let config = pool_pod_config();

    let ungated = build_pool_pod(&config, &slot, None);
    assert!(
        node_group_expressions(&ungated).is_empty(),
        "a pool pod with no resolved set must carry no node-group constraint"
    );

    let group = NodeGroup {
        id: NodeGroupId::new(),
        name: NodeGroupName::parse("workers").expect("valid group name"),
        policy: NodeGroupPolicy::Generic { is_default: true },
        template_allowlist: BTreeSet::new(),
    };
    let template = TemplateCompatSpec {
        name: slot.template_name.clone(),
        org_id: None,
        visibility: TemplateVisibility::Generic,
    };
    // A warm-pool candidate set is resolved against no org at all.
    let placement = NodeGroupPlacement::for_caller(None, &template, &[group])
        .expect("a generic group admits a generic template");
    assert!(!placement.targets_dedicated_capacity());

    let placed = build_pool_pod(&config, &slot, Some(&placement));
    let expressions = node_group_expressions(&placed);
    assert_eq!(expressions.len(), 1, "expected one node-group term");
    let expression = &expressions[0];
    assert_eq!(expression.operator, "In");
    assert_eq!(
        expression.values.as_deref(),
        Some(["workers".to_string()].as_slice())
    );
}

/// Required nodeAffinity match-expressions on the node-group label, flattened
/// across every ORed selector term.
fn node_group_expressions(pod: &Pod) -> Vec<NodeSelectorRequirement> {
    pod.spec
        .as_ref()
        .and_then(|spec| spec.affinity.as_ref())
        .and_then(|affinity| affinity.node_affinity.as_ref())
        .and_then(|node_affinity| {
            node_affinity
                .required_during_scheduling_ignored_during_execution
                .as_ref()
        })
        .map(|selector| selector.node_selector_terms.clone())
        .unwrap_or_default()
        .iter()
        .flat_map(|term| term.match_expressions.clone().unwrap_or_default())
        .filter(|expression| expression.key == SANDBOX_NODE_GROUP_LABEL)
        .collect()
}

#[test]
fn build_pool_pod_sets_explicit_disk_capacity() {
    let mut slot = pool_slot();
    slot.disk_mib = Some(32768);

    let pod = build_pool_pod(&pool_pod_config(), &slot, None);

    assert_eq!(
        pod.metadata
            .annotations
            .as_ref()
            .and_then(|annotations| annotations.get(SANDBOX_DISK_MIB_ANNOTATION))
            .map(String::as_str),
        Some("32768")
    );
}

#[test]
fn build_pool_pod_requires_all_worker_labels_from_config() {
    // Regression: a warm pod that carries only the base `node-pool=workers`
    // label can land on a node missing the other three worker labels — one
    // where the worker DaemonSets aren't running — and fail immediately
    // with `sandbox-manager.sock: connect: no such file or directory`.
    // The pool pod config carries the extra worker labels, so the rendered
    // nodeSelector must require the full set the worker DaemonSets select
    // on.
    let slot = pool_slot();
    let config = pool_pod_config();

    let pod = build_pool_pod(&config, &slot, None);

    let selector = pod.spec.as_ref().unwrap().node_selector.as_ref().unwrap();
    assert_eq!(
        selector,
        &BTreeMap::from([
            (
                "space.pplx.net/node-pool".to_string(),
                "workers".to_string()
            ),
            (
                "node.kubernetes.io/space-enabled".to_string(),
                "true".to_string()
            ),
            (
                "space.pplx.net/subnet-tier".to_string(),
                "secondary".to_string()
            ),
            ("workload".to_string(), "workers".to_string()),
        ])
    );
}

#[test]
fn cluster_controller_built_restore_drops_source_node_pin_and_omits_tolerations() {
    // Models the cluster-controller's evacuation Restore call: no
    // source-node affinity (kube-scheduler picks any uncordoned node)
    // and an empty toleration list (rollout pattern is opt-in, so the
    // default produces no tolerations).
    let env = BTreeMap::new();
    let sandbox_owner = test_sandbox_owner();
    let runtime = evacuation_runtime(&sandbox_owner);
    let pod = build_sandbox_pod(
        &pool_pod_config().for_namespace("space"),
        SandboxPodRequest::Restore {
            artifact_kind: pplx_space_common_core::types::ArtifactKind::Full,
            common: evacuation_common("space-sbx-evac-1", &env),
            runtime,
            fork_disk_restore: None,
            suspend_restore_storage_uri: None,
            suspend_restore_source_node: None,
            direct_node_name: None,
            preferred_node_name: None,
            restore_node_shortlist: Vec::new(),
            extra_tolerations: &[],
            extra_required_affinity: None,
            node_group_placement: None,
        },
    );

    let spec = pod.spec.as_ref().unwrap();
    let node_affinity = spec
        .affinity
        .as_ref()
        .and_then(|affinity| affinity.node_affinity.as_ref())
        .expect("SSD-headroom tier bias should render preferred node affinity");
    // Controller-specific guarantee: an unconfigured evacuation restore
    // pins to no node. (The placement-block required term is covered by the
    // golden snapshots, so it is not re-asserted here.)
    if let Some(required) = node_affinity
        .required_during_scheduling_ignored_during_execution
        .as_ref()
    {
        assert!(
            required
                .node_selector_terms
                .iter()
                .all(|term| term.match_fields.is_none()),
            "unconfigured evacuation restore must not require any node"
        );
    }
    let preferred = node_affinity
        .preferred_during_scheduling_ignored_during_execution
        .as_ref()
        .expect("SSD-headroom tier bias should render preferred terms");
    assert!(
        preferred
            .iter()
            .all(|term| term.preference.match_fields.is_none()),
        "evacuation restore must not carry a source-node preference"
    );
    assert!(spec.tolerations.is_none());
}

#[test]
fn cluster_controller_built_restore_renders_configured_placement() {
    let env = BTreeMap::new();
    let sandbox_owner = test_sandbox_owner();
    let runtime = evacuation_runtime(&sandbox_owner);
    let toleration = PodToleration {
        key: Some("space.pplx.net/rollout-cohort".to_string()),
        operator: Some("Equal".to_string()),
        value: Some("test-2".to_string()),
        effect: Some("NoSchedule".to_string()),
        toleration_seconds: None,
    };
    let pod = build_sandbox_pod(
        &pool_pod_config().for_namespace("space"),
        SandboxPodRequest::Restore {
            artifact_kind: pplx_space_common_core::types::ArtifactKind::Full,
            common: evacuation_common("space-sbx-evac-2", &env),
            runtime,
            fork_disk_restore: None,
            suspend_restore_storage_uri: None,
            suspend_restore_source_node: None,
            direct_node_name: None,
            preferred_node_name: None,
            restore_node_shortlist: Vec::new(),
            extra_tolerations: std::slice::from_ref(&toleration),
            extra_required_affinity: Some(("space.pplx.net/rollout-cohort", "test-2")),
            node_group_placement: None,
        },
    );

    let spec = pod.spec.as_ref().unwrap();

    // Toleration: Equal-operator on the cohort taint.
    let rendered = spec
        .tolerations
        .as_ref()
        .expect("cohort toleration should be rendered");
    assert_eq!(rendered.len(), 1);
    assert_eq!(
        rendered[0].key.as_deref(),
        Some("space.pplx.net/rollout-cohort")
    );
    assert_eq!(rendered[0].operator.as_deref(), Some("Equal"));
    assert_eq!(rendered[0].value.as_deref(), Some("test-2"));
    assert_eq!(rendered[0].effect.as_deref(), Some("NoSchedule"));

    // Required nodeAffinity: must pin to the target cohort label.
    let node_affinity = spec
        .affinity
        .as_ref()
        .and_then(|a| a.node_affinity.as_ref())
        .expect("node_affinity must be present");
    let required = node_affinity
        .required_during_scheduling_ignored_during_execution
        .as_ref()
        .expect("required affinity must be set for cohort placement");
    assert_eq!(required.node_selector_terms.len(), 1);
    let term = &required.node_selector_terms[0];
    let expressions = term
        .match_expressions
        .as_ref()
        .expect("match_expressions must be present for label-based affinity");
    // Controller-specific guarantee: the configured cohort label is required.
    // (The block-exclusion term ANDed alongside it is covered by the golden
    // snapshots, so it is not re-asserted here.)
    let cohort = expressions
        .iter()
        .find(|expr| expr.key == "space.pplx.net/rollout-cohort")
        .expect("cohort placement must render its required label");
    assert_eq!(cohort.operator, "In");
    assert_eq!(
        cohort.values.as_deref(),
        Some(["test-2".to_string()].as_slice())
    );
}

#[test]
fn ready_pod_metadata_requires_running_pod_with_network() {
    let mut pod = Pod {
        metadata: ObjectMeta {
            uid: Some("pod-uid".to_string()),
            ..Default::default()
        },
        spec: Some(PodSpec {
            node_name: Some("worker-1".to_string()),
            ..Default::default()
        }),
        status: Some(PodStatus {
            phase: Some("Running".to_string()),
            pod_ip: Some("10.90.0.9".to_string()),
            conditions: Some(vec![ready_condition("True")]),
            ..Default::default()
        }),
    };

    let metadata = ready_pod_metadata(&SandboxPodView::from(pod.clone())).unwrap();

    assert_eq!(
        metadata,
        ReadyPodMetadata {
            uid: "pod-uid".to_string(),
            pod_ip: "10.90.0.9".to_string(),
            node_name: "worker-1".to_string(),
        }
    );

    pod.status.as_mut().unwrap().phase = Some("Pending".to_string());
    assert_eq!(ready_pod_metadata(&SandboxPodView::from(pod.clone())), None);

    pod.status.as_mut().unwrap().phase = Some("Running".to_string());
    pod.status.as_mut().unwrap().conditions = Some(vec![ready_condition("False")]);
    assert_eq!(ready_pod_metadata(&SandboxPodView::from(pod.clone())), None);

    pod.status.as_mut().unwrap().conditions = Some(vec![ready_condition("True")]);
    pod.metadata.deletion_timestamp =
        Some(serde_json::from_str("\"2026-05-28T00:00:00Z\"").unwrap());
    assert_eq!(ready_pod_metadata(&SandboxPodView::from(pod.clone())), None);
}

fn ready_condition(status: &str) -> PodCondition {
    PodCondition {
        type_: "Ready".to_string(),
        status: status.to_string(),
        ..Default::default()
    }
}

fn pool_pod_config() -> SandboxPodConfigValues {
    SandboxPodConfigValues {
        sandbox_image: DEFAULT_SANDBOX_POD_IMAGE.to_string(),
        runtime_class_name: DEFAULT_SANDBOX_RUNTIME_CLASS.to_string(),
        sandbox_node_selector_key: "space.pplx.net/node-pool".to_string(),
        sandbox_node_selector_value: "workers".to_string(),
        extra_node_selector_labels: BTreeMap::from([
            (
                "node.kubernetes.io/space-enabled".to_string(),
                "true".to_string(),
            ),
            (
                "space.pplx.net/subnet-tier".to_string(),
                "secondary".to_string(),
            ),
            ("workload".to_string(), "workers".to_string()),
        ]),
        internal_secret_name: DEFAULT_SANDBOX_INTERNAL_SECRET_NAME.to_string(),
        internal_token_key: DEFAULT_SANDBOX_INTERNAL_TOKEN_KEY.to_string(),
        internal_control_token_key: DEFAULT_SANDBOX_INTERNAL_CONTROL_TOKEN_KEY.to_string(),
        ..SandboxPodConfigValues::default()
    }
}

fn pool_slot() -> PoolSlot {
    let now = OffsetDateTime::now_utc();
    PoolSlot {
        id: Uuid::from_str("019e5f38-c338-7c13-a8d4-587fd9fd8dd5").unwrap(),
        config_id: Uuid::from_str("019e5f38-c76e-7e22-9b2f-e389cc9dc6b3").unwrap(),
        template_release_id: Uuid::from_str("019e5f38-ca6c-72c2-a89d-86d4488c6fe0").unwrap(),
        reserved_sandbox_id: SandboxId::from_uuid(
            Uuid::from_str("019e5f38-cd2e-7400-b3c8-e33ae36692d7").unwrap(),
        ),
        template_name: TemplateName::parse("seed").unwrap(),
        content_hash: content_hash(),
        vcpu: VcpuCount::try_from_u32(2).unwrap(),
        memory: MemorySize::mib(8192).unwrap(),
        disk_mib: None,
        disk_io_limits: Default::default(),
        pod_name: "space-sbx-test-g1".to_string(),
        pod_namespace: "space".to_string(),
        pod_uid: None,
        pod_ip: None,
        k8s_node_name: None,
        state: PoolSlotState::Warming,
        claim_id: None,
        ready_at: None,
        claimed_at: None,
        created_at: now,
        updated_at: now,
    }
}

fn content_hash() -> ContentHash {
    ContentHash::new(Digest::sha256([0x11; 32]))
}

const EVAC_TEST_TEMPLATE: &str = "seed";
const EVAC_TEST_AUTO_LIFECYCLE: &str = "{}";
const EVAC_TEST_NETWORK_POLICY: &str = "{}";
const EVAC_TEST_CONTENT_HASH: &str =
    "sha256:969f3d03c5e44174169acb4fc5a8019590338e04aa48e5c11f0fd2e10716e254";
static EVAC_TEST_SANDBOX_OWNER: SandboxOwner = SandboxOwner::new_static("evacuation@local");

fn test_sandbox_owner() -> SandboxOwner {
    SandboxOwner::platform_user(UserId::from_uuid(Uuid::from_u128(0x20)))
}

fn evacuation_runtime(_sandbox_owner: &SandboxOwner) -> SandboxRuntimePodMetadata<'static> {
    SandboxRuntimePodMetadata {
        sandbox_owner: &EVAC_TEST_SANDBOX_OWNER,
        user_id: UserId::from_uuid(Uuid::from_u128(0x20)),
        org_id: OrgId::from_uuid(Uuid::from_u128(0x02)),
        auto_lifecycle_json: EVAC_TEST_AUTO_LIFECYCLE,
        auto_lifecycle_authority: PolicyAuthority::Cluster,
        network_policy_json: EVAC_TEST_NETWORK_POLICY,
        network_policy_generation: 1,
        network_policy_authority: PolicyAuthority::Cluster,
        volume_encryption: false,
        vcpu: None,
        memory_mib: None,
        disk_mib: None,
        disk_io_limits: Default::default(),
    }
}

fn evacuation_common<'a>(
    pod_name: &'a str,
    env: &'a BTreeMap<String, String>,
) -> SandboxPodCommon<'a> {
    SandboxPodCommon {
        pod_name,
        sandbox_id: SandboxId::from_uuid(Uuid::from_u128(0x019de67463d77ef0a5888e106a878093)),
        execution_generation: 2,
        activation_attempt_id: None,
        materialization_id: None,
        template: EVAC_TEST_TEMPLATE,
        content_hash: Some(EVAC_TEST_CONTENT_HASH),
        env,
        traceparent: None,
    }
}

/// The node-group labeler labels exactly the pool sandbox pods schedule onto,
/// so the selector it composes must equal the `nodeSelector` this module
/// actually renders. Both sides now go through `base_node_selector`, so the
/// composition itself cannot drift — that half is covered by
/// `common/core/src/k8s/node_selector/tests.rs`.
///
/// This is not therefore tautological. `build_sandbox_pod` seeds its map from
/// `base_node_selector().into_labels()` and then keeps writing to it, so the
/// renderer can still *add* an entry the shared composition never had. On the
/// unpinned path asserted here it must not.
///
/// Drift here is a security problem, not a cosmetic one: the labeler writes a
/// tenant-isolation boundary label onto whatever set it computes. If this
/// module gained a selector entry the shared composition didn't, the labeler
/// would label nodes no sandbox can schedule onto.
#[cfg(feature = "kubernetes")]
#[test]
fn node_group_labeler_selector_matches_the_rendered_pod_node_selector() {
    let values = pool_pod_config();
    let env = BTreeMap::new();
    let sandbox_owner = test_sandbox_owner();
    let runtime = evacuation_runtime(&sandbox_owner);
    // Restore rather than OnDemand: the create-time isolation-pool pin is an
    // OnDemand-only addition, and the labeler deliberately tracks the
    // unpinned selector.
    let pod = build_sandbox_pod(
        &values.for_namespace("space"),
        SandboxPodRequest::Restore {
            artifact_kind: pplx_space_common_core::types::ArtifactKind::Full,
            common: evacuation_common("space-sbx-selector-diff", &env),
            runtime,
            fork_disk_restore: None,
            suspend_restore_storage_uri: None,
            suspend_restore_source_node: None,
            direct_node_name: None,
            preferred_node_name: None,
            restore_node_shortlist: Vec::new(),
            extra_tolerations: &[],
            extra_required_affinity: None,
            // No placement: the labeler tracks the *unpinned* selector, and a
            // placement adds required nodeAffinity rather than nodeSelector
            // entries, so it would not change what this compares.
            node_group_placement: None,
        },
    );

    let rendered = pod
        .spec
        .as_ref()
        .and_then(|spec| spec.node_selector.clone())
        .expect("sandbox pods render a nodeSelector");
    let labeler = values.base_node_selector().into_labels();

    assert_eq!(
        labeler, rendered,
        "the node-group labeler's worker selector has drifted from the sandbox pod's \
         rendered nodeSelector"
    );
}
