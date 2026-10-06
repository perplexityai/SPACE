fn placement_template() -> crate::types::TemplateCompatSpec {
    crate::types::TemplateCompatSpec {
        name: crate::types::TemplateName::parse("seed").unwrap(),
        org_id: None,
        visibility: crate::types::TemplateVisibility::Generic,
    }
}

fn node_group(name: &str) -> NodeGroupName {
    NodeGroupName::parse(name).unwrap()
}

fn shared_group(name: &str) -> crate::types::NodeGroup {
    crate::types::NodeGroup {
        id: crate::types::NodeGroupId::new(),
        name: node_group(name),
        policy: crate::types::NodeGroupPolicy::Generic { is_default: false },
        template_allowlist: std::collections::BTreeSet::new(),
    }
}

fn dedicated_group(name: &str, org_id: OrgId) -> crate::types::NodeGroup {
    crate::types::NodeGroup {
        id: crate::types::NodeGroupId::new(),
        name: node_group(name),
        policy: crate::types::NodeGroupPolicy::Dedicated {
            org_id,
            template_policy: crate::types::NodeGroupTemplatePolicy::Default,
        },
        template_allowlist: std::collections::BTreeSet::new(),
    }
}

/// A placement for an org-specific template — the shape whose whole point is
/// that shared capacity is never offered. Distinct from a *generic* template
/// resolved to a dedicated-only candidate set, which renders the same boundary
/// for a different reason and is what `dedicated_node_group_placement(false)`
/// builds.
fn org_specific_node_group_placement() -> NodeGroupPlacement {
    let org = TEST_ORG_ID;
    let template = crate::types::TemplateCompatSpec {
        name: crate::types::TemplateName::parse(ORG_SPECIFIC_TEMPLATE).unwrap(),
        org_id: Some(org),
        visibility: crate::types::TemplateVisibility::OrgSpecific,
    };
    NodeGroupPlacement::for_caller(
        Some(org),
        &template,
        &[dedicated_group("acme-dedicated", org)],
    )
    .unwrap()
}

fn shared_node_group_placement() -> NodeGroupPlacement {
    NodeGroupPlacement::for_caller(
        Some(TEST_ORG_ID),
        &placement_template(),
        &[shared_group("workers")],
    )
    .unwrap()
}

/// Placement whose candidate set is the caller's own dedicated group plus,
/// optionally, shared capacity. `include_shared` mirrors the generic-template
/// (may spill) versus org-specific-template (dedicated only) split.
fn dedicated_node_group_placement(include_shared: bool) -> NodeGroupPlacement {
    let org = TEST_ORG_ID;
    let mut candidates = vec![dedicated_group("acme-dedicated", org)];
    if include_shared {
        candidates.push(shared_group("workers"));
    }
    NodeGroupPlacement::for_caller(Some(org), &placement_template(), &candidates).unwrap()
}

use std::collections::BTreeMap;

use super::*;
use crate::types::{
    AutoLifecycleConditionSet, AutoLifecycleConditionSetId, AutoLifecycleIdleAction,
    AutoLifecyclePolicy, AutoLifecycleStage, NodeGroupName, OrgId,
};

const TEST_SANDBOX_ID: crate::types::SandboxId =
    crate::types::SandboxId::from_uuid(Uuid::from_u128(0x019de67463d77ef0a5888e106a878093));
const TEST_POOL_SANDBOX_ID: crate::types::SandboxId =
    crate::types::SandboxId::from_uuid(Uuid::from_u128(0x019e5f38cd2e7400b3c8e33ae36692d7));
const TEST_USER_ID: crate::types::UserId =
    crate::types::UserId::from_uuid(Uuid::from_u128(0x00000000000000000000000000000020));
const TEST_ORG_ID: crate::types::OrgId =
    crate::types::OrgId::from_uuid(Uuid::from_u128(0x00000000000000000000000000000002));
const TEST_POOL_SLOT_ID: Uuid = Uuid::from_u128(0x019e5f38c3387c13a8d4587fd9fd8dd5);
const TEST_POOL_CONFIG_ID: Uuid = Uuid::from_u128(0x019e5f38c76e7e229b2fe389cc9dc6b3);
// This stable fixture is independent of deployed template releases.
const ORG_SPECIFIC_TEMPLATE: &str = "acme-secret";
// This stable fixture is independent of deployed template releases.
const TEST_CONTENT_HASH: &str =
    "sha256:969f3d03c5e44174169acb4fc5a8019590338e04aa48e5c11f0fd2e10716e254";

static TEST_EXTRA_NODE_SELECTOR_LABELS: std::sync::LazyLock<BTreeMap<String, String>> =
    std::sync::LazyLock::new(|| {
        BTreeMap::from([
            (
                "node.kubernetes.io/space-enabled".to_string(),
                "true".to_string(),
            ),
            (
                "space.pplx.net/subnet-tier".to_string(),
                "secondary".to_string(),
            ),
            ("workload".to_string(), "workers".to_string()),
        ])
    });
fn test_sandbox_owner() -> SandboxOwner {
    SandboxOwner::platform_user(TEST_USER_ID)
}

fn test_pod_config() -> SandboxPodConfig<'static> {
    SandboxPodConfig {
        namespace: "space",
        sandbox_image: DEFAULT_SANDBOX_POD_IMAGE,
        runtime_class_name: DEFAULT_SANDBOX_RUNTIME_CLASS,
        sandbox_node_selector_key: "space.pplx.net/node-pool",
        sandbox_node_selector_value: "workers",
        extra_node_selector_labels: &TEST_EXTRA_NODE_SELECTOR_LABELS,
        internal_secret_name: DEFAULT_SANDBOX_INTERNAL_SECRET_NAME,
        internal_token_key: DEFAULT_SANDBOX_INTERNAL_TOKEN_KEY,
        internal_control_token_key: DEFAULT_SANDBOX_INTERNAL_CONTROL_TOKEN_KEY,
        ssd_headroom_tier_affinity: DEFAULT_SANDBOX_SSD_HEADROOM_TIER_AFFINITY,
        restore_source_node_hard_pin: DEFAULT_SANDBOX_RESTORE_SOURCE_NODE_HARD_PIN,
        node_services_ready_affinity: DEFAULT_SANDBOX_NODE_SERVICES_READY_AFFINITY,
    }
}

#[test]
fn parse_node_selector_labels_parses_trimmed_pairs() {
    let labels =
        parse_node_selector_labels(" node.kubernetes.io/space-enabled = true , workload=workers ,")
            .unwrap();

    assert_eq!(
        labels,
        BTreeMap::from([
            (
                "node.kubernetes.io/space-enabled".to_string(),
                "true".to_string()
            ),
            ("workload".to_string(), "workers".to_string()),
        ])
    );
}

#[test]
fn parse_node_selector_labels_empty_is_empty_map() {
    assert!(parse_node_selector_labels("").unwrap().is_empty());
    assert!(parse_node_selector_labels("  , ").unwrap().is_empty());
}

#[test]
fn parse_node_selector_labels_rejects_entry_without_equals() {
    assert_eq!(
        parse_node_selector_labels("workload"),
        Err(NodeSelectorLabelsParseError::MissingEquals {
            entry: "workload".to_string(),
        })
    );
}

#[test]
fn parse_node_selector_labels_rejects_empty_key() {
    assert_eq!(
        parse_node_selector_labels("=workers"),
        Err(NodeSelectorLabelsParseError::EmptyKey {
            entry: "=workers".to_string(),
        })
    );
}

#[test]
fn build_sandbox_pod_merges_extra_node_selector_labels() {
    let env = BTreeMap::new();
    let config = test_pod_config();
    let pod = build_sandbox_pod(
        &config,
        SandboxPodRequest::Pooled {
            common: test_common("space-sbx-pool-g1", TEST_POOL_SANDBOX_ID, 1, &env),
            pool: SandboxPoolPodMetadata {
                slot_id: TEST_POOL_SLOT_ID,
                config_id: TEST_POOL_CONFIG_ID,
                vcpu: VcpuCount::try_from_u32(2).unwrap(),
                memory: MemorySize::mib(8192).unwrap(),
                disk_mib: None,
                disk_io_limits: Default::default(),
            },
            node_group_placement: None,
        },
    );

    let selector = pod.spec.unwrap().node_selector.unwrap();
    assert_eq!(
        selector,
        BTreeMap::from([
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

/// The layering contract every node filter depends on: `build_sandbox_pod`
/// renders the shared base selector and adds *only* the create-time
/// isolation-pool pin, when the request carries one.
///
/// Scoped to the layering on purpose. Both sides of the comparison call
/// `base_node_selector()`, so this says nothing about what the base contains
/// — that is pinned against literal maps by
/// `build_sandbox_pod_merges_extra_node_selector_labels` above and by
/// `node_selector::tests`. What only this test can catch is a third
/// contributor sneaking into the rendered map, which would make every filter
/// that composes the base disagree with the pod the scheduler actually sees.
#[test]
fn rendered_pod_node_selector_is_the_base_selector_plus_the_pool_pin() {
    let env = BTreeMap::new();
    let config = test_pod_config();
    let base = config.base_node_selector().into_labels();
    let auto_lifecycle_json = test_auto_lifecycle_json();
    let network_policy_json = test_network_policy_json();
    let sandbox_owner = test_sandbox_owner();
    let runtime = test_runtime(&sandbox_owner, &auto_lifecycle_json, &network_policy_json);

    let unpinned = build_sandbox_pod(
        &config,
        SandboxPodRequest::OnDemand {
            common: test_common("space-sbx-unpinned-g1", TEST_SANDBOX_ID, 1, &env),
            runtime,
            node_pool: None,
            node_group_placement: None,
        },
    );
    let unpinned_selector = unpinned.spec.unwrap().node_selector.unwrap();
    assert_eq!(
        unpinned_selector, base,
        "an unpinned create must render the base selector unchanged"
    );
    // Literal anchor for the OnDemand shape: the comparison above holds even
    // if `base_node_selector` is wrong, since both sides call it. The
    // pre-existing literal assertion covers Pooled only.
    assert_eq!(
        unpinned_selector,
        BTreeMap::from([
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

    let pool = NodePoolName::parse("isolated-test-pool").unwrap();
    let pinned = build_sandbox_pod(
        &config,
        SandboxPodRequest::OnDemand {
            common: test_common("space-sbx-placed-g1", TEST_SANDBOX_ID, 1, &env),
            runtime,
            node_pool: Some(&pool),
            node_group_placement: None,
        },
    );
    let mut base_plus_pin = base.clone();
    base_plus_pin.insert(
        SANDBOX_PLACEMENT_POOL_LABEL_KEY.to_string(),
        "isolated-test-pool".to_string(),
    );
    assert_eq!(
        pinned.spec.unwrap().node_selector.unwrap(),
        base_plus_pin,
        "a pool-pinned create must add exactly the pool entry to the base selector"
    );
}

/// The pool pin is layered *after* the base selector, so it wins the pool key.
///
/// Only reachable by misconfiguration — an operator putting the pool
/// discriminator into `SPACE_SANDBOX_NODE_SELECTOR_LABELS` — but the
/// precedence decides where a pinned sandbox lands, so it should be the
/// documented one rather than whichever the composition order happens to give.
#[test]
fn the_pool_pin_overrides_an_extra_label_on_the_pool_key() {
    let env = BTreeMap::new();
    let extra = BTreeMap::from([(
        SANDBOX_PLACEMENT_POOL_LABEL_KEY.to_string(),
        "some-other-pool".to_string(),
    )]);
    let config = SandboxPodConfig {
        extra_node_selector_labels: &extra,
        ..test_pod_config()
    };
    let auto_lifecycle_json = test_auto_lifecycle_json();
    let network_policy_json = test_network_policy_json();
    let sandbox_owner = test_sandbox_owner();
    let runtime = test_runtime(&sandbox_owner, &auto_lifecycle_json, &network_policy_json);
    let pool = NodePoolName::parse("isolated-test-pool").unwrap();

    let pod = build_sandbox_pod(
        &config,
        SandboxPodRequest::OnDemand {
            common: test_common("space-sbx-placed-g1", TEST_SANDBOX_ID, 1, &env),
            runtime,
            node_pool: Some(&pool),
            node_group_placement: None,
        },
    );

    assert_eq!(
        pod.spec
            .unwrap()
            .node_selector
            .unwrap()
            .get(SANDBOX_PLACEMENT_POOL_LABEL_KEY)
            .map(String::as_str),
        Some("isolated-test-pool"),
        "the create-time pin must decide the pool, not a configured extra label"
    );
}

#[test]
fn ssd_headroom_tier_buckets_free_fraction_by_usage_thresholds() {
    // (free_fraction, expected tier). Thresholds are easier to read as
    // disk usage (= 1 - free): < 80% -> 3, 80–90% -> 2, 90–95% -> 1,
    // >= 95% -> 0.
    for (free, tier) in [
        (1.00, 3), // empty fs
        (0.50, 3), // 50% used
        (0.21, 3), // 79% used
        (0.20, 2), // exactly 80% used
        (0.15, 2), // 85% used
        (0.10, 1), // exactly 90% used
        (0.06, 1), // 94% used
        (0.05, 0), // exactly 95% used
        (0.00, 0), // full
    ] {
        assert_eq!(
            SsdHeadroomTier::from_free_fraction(free).as_u8(),
            tier,
            "free fraction {free} must map to tier {tier}"
        );
    }
    // Nonsense measurements clamp to the conservative (near-full) end.
    assert_eq!(SsdHeadroomTier::from_free_fraction(f64::NAN).as_u8(), 0);
    assert_eq!(
        SsdHeadroomTier::from_free_fraction(f64::INFINITY).as_u8(),
        0
    );
    assert_eq!(SsdHeadroomTier::from_free_fraction(-0.3).as_u8(), 0);
    assert_eq!(SsdHeadroomTier::from_free_fraction(1.7).as_u8(), 3);
    assert_eq!(SsdHeadroomTier::MAX.to_string(), "3");
}

#[test]
fn ssd_headroom_terms_flatten_healthy_and_steepen_toward_disk_gate() {
    let terms: Vec<_> = ssd_headroom_tier_preferred_terms().collect();
    // One term per non-zero tier (`1..=MAX`); the near-full tier 0 emits
    // none.
    assert_eq!(terms.len(), usize::from(SsdHeadroomTier::MAX.as_u8()));
    for term in &terms {
        let expressions = term.preference.match_expressions.as_ref().unwrap();
        assert_eq!(expressions.len(), 1);
        let expression = expressions.first().expect("one SSD tier expression");
        assert_eq!(expression.key, SSD_HEADROOM_TIER_LABEL);
        assert_eq!(expression.operator, "In");
    }

    let weight_for = |tier: u8| -> Option<i32> {
        let want = tier.to_string();
        terms.iter().find_map(|term| {
            let expression = term.preference.match_expressions.as_ref()?.first()?;
            (expression.key == SSD_HEADROOM_TIER_LABEL
                && expression
                    .values
                    .as_ref()
                    .is_some_and(|vs| vs.contains(&want)))
            .then_some(term.weight)
        })
    };

    // The healthy band (the single top tier) scores the flat healthy
    // weight: a nearly-empty node never out-attracts the working bulk.
    assert_eq!(
        weight_for(SsdHeadroomTier::MAX.as_u8()),
        Some(SSD_HEADROOM_HEALTHY_WEIGHT)
    );
    // The sub-healthy tiers fall steeply and monotonically toward the disk
    // gate (the convex `healthy × (tier / max)²` ramp); the near-full tier
    // carries no preference at all. Tier 2 stays out of the healthy term on
    // purpose: the value `"2"` collides with a legacy *healthy* decile, but
    // a new-scheme `"2"` is a filling 80–90% node and must not score
    // healthy (see `SSD_HEADROOM_LEGACY_DECILE_MAX`).
    assert_eq!(weight_for(2), Some(22));
    assert_eq!(weight_for(1), Some(5));
    assert_eq!(weight_for(0), None);
    assert!(weight_for(1).unwrap() < weight_for(2).unwrap());
    assert!(weight_for(2).unwrap() < SSD_HEADROOM_HEALTHY_WEIGHT);

    // Rollout safety: a worker still carrying an old `0..=10` free-space
    // label (`4..=10`) keeps the healthy weight rather than matching no term
    // and dropping to zero — otherwise a mixed-version fleet would invert
    // the bias toward fuller nodes.
    for legacy in SsdHeadroomTier::MAX.as_u8()..=SSD_HEADROOM_LEGACY_DECILE_MAX {
        assert_eq!(
            weight_for(legacy),
            Some(SSD_HEADROOM_HEALTHY_WEIGHT),
            "legacy decile label {legacy} must still score the healthy weight"
        );
    }

    // Even the healthy weight stays below the weight-100 source-node and
    // default template-cache preferences.
    assert!(
        terms
            .iter()
            .all(|term| term.weight < RESTORE_SOURCE_NODE_PREFERENCE_WEIGHT)
    );
}

fn preferred_scheduling_terms(pod: &Pod) -> Vec<PreferredSchedulingTerm> {
    pod.spec
        .as_ref()
        .and_then(|spec| spec.affinity.as_ref())
        .and_then(|affinity| affinity.node_affinity.as_ref())
        .and_then(|node_affinity| {
            node_affinity
                .preferred_during_scheduling_ignored_during_execution
                .clone()
        })
        .unwrap_or_default()
}

fn required_node_selector_terms(pod: &Pod) -> Vec<NodeSelectorTerm> {
    pod.spec
        .as_ref()
        .and_then(|spec| spec.affinity.as_ref())
        .and_then(|affinity| affinity.node_affinity.as_ref())
        .and_then(|node_affinity| {
            node_affinity
                .required_during_scheduling_ignored_during_execution
                .clone()
        })
        .map(|selector| selector.node_selector_terms)
        .unwrap_or_default()
}

fn on_demand_request<'a>(
    common: SandboxPodCommon<'a>,
    runtime: SandboxRuntimePodMetadata<'a>,
) -> SandboxPodRequest<'a> {
    SandboxPodRequest::OnDemand {
        common,
        runtime,
        node_pool: None,
        node_group_placement: None,
    }
}

/// Exhaustive `SandboxRuntimePodMetadata` literal for tests to mutate or
/// struct-update from. Neither pod-render type derives `Default` (every field
/// is meant to be considered at the call site), so this is the one place the
/// common test values (cluster-authority policies, no vcpu/memory/disk pin, no
/// volume encryption) are written out; callers that need a different value
/// override it via `..test_runtime(...)`.
fn test_runtime<'a>(
    sandbox_owner: &'a SandboxOwner,
    auto_lifecycle_json: &'a str,
    network_policy_json: &'a str,
) -> SandboxRuntimePodMetadata<'a> {
    SandboxRuntimePodMetadata {
        sandbox_owner,
        user_id: TEST_USER_ID,
        org_id: TEST_ORG_ID,
        auto_lifecycle_json,
        auto_lifecycle_authority: PolicyAuthority::Cluster,
        network_policy_json,
        network_policy_generation: 1,
        network_policy_authority: PolicyAuthority::Cluster,
        vcpu: None,
        memory_mib: None,
        disk_mib: None,
        disk_io_limits: Default::default(),
        volume_encryption: false,
    }
}

/// Exhaustive `SandboxPodCommon` literal for tests to mutate or struct-update
/// from. See [`test_runtime`] for why this isn't a `Default` impl.
fn test_common<'a>(
    pod_name: &'a str,
    sandbox_id: crate::types::SandboxId,
    execution_generation: i64,
    env: &'a BTreeMap<String, String>,
) -> SandboxPodCommon<'a> {
    SandboxPodCommon {
        pod_name,
        sandbox_id,
        execution_generation,
        activation_attempt_id: None,
        template: "seed",
        content_hash: Some(TEST_CONTENT_HASH),
        env,
        traceparent: None,
    }
}

#[test]
fn build_sandbox_pod_biases_scheduling_toward_free_ssd_tiers() {
    let env = BTreeMap::new();
    let auto_lifecycle_json = test_auto_lifecycle_json();
    let network_policy_json = test_network_policy_json();
    let sandbox_owner = test_sandbox_owner();
    let runtime = test_runtime(&sandbox_owner, &auto_lifecycle_json, &network_policy_json);
    let common = test_common("space-sbx-tier-g1", TEST_SANDBOX_ID, 1, &env);

    let pod = build_sandbox_pod(&test_pod_config(), on_demand_request(common, runtime));
    let terms = preferred_scheduling_terms(&pod);
    let tier_terms: Vec<_> = terms
        .iter()
        .filter(|term| {
            term.preference
                .match_expressions
                .as_ref()
                .is_some_and(|exprs| exprs.iter().any(|expr| expr.key == SSD_HEADROOM_TIER_LABEL))
        })
        .collect();
    assert_eq!(tier_terms.len(), usize::from(SsdHeadroomTier::MAX.as_u8()));
    // The template-cache preference is still rendered alongside the bias.
    assert_eq!(terms.len(), tier_terms.len() + 1);
}

#[test]
fn build_sandbox_pod_kill_switch_disables_ssd_tier_bias() {
    let env = BTreeMap::new();
    let auto_lifecycle_json = test_auto_lifecycle_json();
    let network_policy_json = test_network_policy_json();
    let sandbox_owner = test_sandbox_owner();
    let runtime = test_runtime(&sandbox_owner, &auto_lifecycle_json, &network_policy_json);
    let common = test_common("space-sbx-tier-g1", TEST_SANDBOX_ID, 1, &env);
    let config = SandboxPodConfig {
        ssd_headroom_tier_affinity: false,
        ..test_pod_config()
    };

    let pod = build_sandbox_pod(&config, on_demand_request(common, runtime));
    let terms = preferred_scheduling_terms(&pod);
    assert!(
        terms.iter().all(|term| {
            term.preference
                .match_expressions
                .as_ref()
                .is_none_or(|exprs| exprs.iter().all(|expr| expr.key != SSD_HEADROOM_TIER_LABEL))
        }),
        "kill switch must drop every ssd-headroom-tier term"
    );
    // The template-cache preference must survive the kill switch.
    assert_eq!(terms.len(), 1);
}

#[test]
fn build_sandbox_pod_direct_restore_keeps_artifact_kind_and_node_assignment() {
    for (artifact_kind, checkpoint) in [
        (crate::types::ArtifactKind::Full, "suspend"),
        (crate::types::ArtifactKind::Disk, "stopped"),
    ] {
        let env = BTreeMap::new();
        let auto_lifecycle_json = test_auto_lifecycle_json();
        let network_policy_json = test_network_policy_json();
        let sandbox_owner = test_sandbox_owner();
        let runtime = test_runtime(&sandbox_owner, &auto_lifecycle_json, &network_policy_json);
        let common = test_common("space-sbx-tier-g2", TEST_SANDBOX_ID, 2, &env);

        let pod = build_sandbox_pod(
            &test_pod_config(),
            SandboxPodRequest::Restore {
                artifact_kind,
                common,
                runtime,
                suspend_restore_storage_uri: None,
                suspend_restore_source_node: Some("ip-10-0-0-1.ec2.internal"),
                fork_disk_restore: None,
                direct_node_name: Some("ip-10-0-0-1.ec2.internal"),
                preferred_node_name: None,
                restore_node_shortlist: Vec::new(),
                extra_tolerations: &[],
                extra_required_affinity: None,
                node_group_placement: None,
            },
        );

        let spec = pod.spec.as_ref().unwrap();
        assert_eq!(spec.node_name.as_deref(), Some("ip-10-0-0-1.ec2.internal"));
        assert!(
            spec.affinity.is_none(),
            "a scheduler-bypassing direct assignment must not render scoring terms"
        );
        assert_eq!(
            pod.metadata
                .annotations
                .as_ref()
                .unwrap()
                .get(SANDBOX_CHECKPOINT_ANNOTATION)
                .map(String::as_str),
            Some(checkpoint),
        );
    }
}

/// The *standalone* `node-services-ready` preference — the term whose only
/// requirement is readiness. Deliberately excludes the template-cache term,
/// which also carries the readiness requirement (ANDed) so that cache
/// locality cannot outrank readiness.
fn node_services_ready_terms(pod: &Pod) -> Vec<PreferredSchedulingTerm> {
    preferred_scheduling_terms(pod)
        .into_iter()
        .filter(|term| {
            term.preference
                .match_expressions
                .as_deref()
                .is_some_and(|exprs| exprs.len() == 1 && exprs[0].key == NODE_SERVICES_READY_LABEL)
        })
        .collect()
}

fn node_services_bias_test_pod(config: &SandboxPodConfig<'_>) -> Pod {
    let env = BTreeMap::new();
    let auto_lifecycle_json = test_auto_lifecycle_json();
    let network_policy_json = test_network_policy_json();
    let sandbox_owner = test_sandbox_owner();
    let runtime = test_runtime(&sandbox_owner, &auto_lifecycle_json, &network_policy_json);
    let common = test_common("space-sbx-services-g1", TEST_SANDBOX_ID, 1, &env);
    build_sandbox_pod(config, on_demand_request(common, runtime))
}

/// kube-scheduler's NodeAffinity score for a node with `labels`, before
/// normalization: the sum of the weights of every preferred term the node
/// matches. `matchExpressions` within one term are ANDed; `NotIn` is
/// satisfied by a node that lacks the key.
fn node_affinity_score(pod: &Pod, labels: &BTreeMap<&str, String>) -> i32 {
    preferred_scheduling_terms(pod)
        .iter()
        .filter(|term| {
            term.preference
                .match_expressions
                .as_deref()
                .unwrap_or_default()
                .iter()
                .all(|expr| {
                    let value = labels.get(expr.key.as_str()).map(String::as_str);
                    let listed = expr
                        .values
                        .as_deref()
                        .unwrap_or_default()
                        .iter()
                        .any(|candidate| Some(candidate.as_str()) == value);
                    match expr.operator.as_str() {
                        "In" => listed,
                        "NotIn" => !listed,
                        other => panic!("unhandled node selector operator {other}"),
                    }
                })
        })
        .map(|term| term.weight)
        .sum()
}

/// The readiness bias has to *strictly dominate* the other preferences, not
/// merely add to them. Both the template-cache term and this one cap at
/// weight 100, so a cached-but-rolling node scoring the cache term while a
/// healthy uncached node scores the readiness term would tie — and the
/// scheduler's remaining plugins would be free to pick the rolling node,
/// which is exactly the daemon wait this feature exists to avoid.
#[test]
fn node_services_readiness_outranks_every_other_placement_preference() {
    let pod = node_services_bias_test_pod(&SandboxPodConfig {
        node_services_ready_affinity: true,
        ..test_pod_config()
    });
    let template_label = preferred_scheduling_terms(&pod)
        .iter()
        .flat_map(|term| {
            term.preference
                .match_expressions
                .clone()
                .unwrap_or_default()
        })
        .find(|expr| expr.key.starts_with(TEMPLATE_BUILD_LABEL_KEY_PREFIX))
        .map(|expr| expr.key)
        .expect("the on-demand shape renders a template-cache preference");

    // Every combination of (services ready?, template cached?) at the
    // healthiest disk tier — the tier a rolling node most plausibly has.
    let mut worst_ready = i32::MAX;
    let mut best_not_ready = i32::MIN;
    for services_ready in [true, false] {
        for cached in [true, false] {
            let mut labels =
                BTreeMap::from([(SSD_HEADROOM_TIER_LABEL, SsdHeadroomTier::MAX.to_string())]);
            labels.insert(
                NODE_SERVICES_READY_LABEL,
                if services_ready {
                    NODE_SERVICES_READY_VALUE.to_string()
                } else {
                    NODE_SERVICES_NOT_READY_VALUE.to_string()
                },
            );
            if cached {
                labels.insert(
                    template_label.as_str(),
                    TEMPLATE_PRESENT_LABEL_VALUE.to_string(),
                );
            }
            let score = node_affinity_score(&pod, &labels);
            if services_ready {
                worst_ready = worst_ready.min(score);
            } else {
                best_not_ready = best_not_ready.max(score);
            }
        }
    }

    assert!(
        worst_ready > best_not_ready,
        "the worst serviced node ({worst_ready}) must outscore the best \
             not-ready node ({best_not_ready}); a tie lets the scheduler's other \
             plugins pick a node whose daemons are restarting"
    );
}

#[test]
fn build_sandbox_pod_deprioritizes_nodes_with_not_ready_node_services() {
    let pod = node_services_bias_test_pod(&SandboxPodConfig {
        node_services_ready_affinity: true,
        ..test_pod_config()
    });

    let terms = node_services_ready_terms(&pod);
    assert_eq!(terms.len(), 1, "exactly one readiness preference");
    let term = &terms[0];
    assert_eq!(term.weight, NODE_SERVICES_READY_PREFERENCE_WEIGHT);
    let expr = &term.preference.match_expressions.as_ref().unwrap()[0];
    // `NotIn` (rather than `In` the ready value) is load-bearing: a node
    // that has never been labelled — writer disabled, node just joined —
    // must keep the full weight instead of being deprioritized by default.
    assert_eq!(expr.operator, "NotIn");
    assert_eq!(
        expr.values.as_deref(),
        Some([NODE_SERVICES_NOT_READY_VALUE.to_string()].as_slice())
    );
    // The bias is additive: it must not displace the disk-headroom scoring
    // or the required placement-block exclusion.
    assert!(
        preferred_scheduling_terms(&pod).iter().any(|term| {
            term.preference
                .match_expressions
                .as_ref()
                .is_some_and(|exprs| exprs.iter().any(|expr| expr.key == SSD_HEADROOM_TIER_LABEL))
        }),
        "ssd-headroom-tier scoring must survive the readiness bias"
    );
}

#[test]
fn build_sandbox_pod_omits_node_services_bias_by_default() {
    let pod = node_services_bias_test_pod(&test_pod_config());
    assert!(
        node_services_ready_terms(&pod).is_empty(),
        "the readiness bias must stay off until it is explicitly enabled"
    );
}

#[test]
fn build_sandbox_pod_source_pinned_restore_skips_node_services_bias() {
    let env = BTreeMap::new();
    let auto_lifecycle_json = test_auto_lifecycle_json();
    let network_policy_json = test_network_policy_json();
    let sandbox_owner = test_sandbox_owner();
    let runtime = test_runtime(&sandbox_owner, &auto_lifecycle_json, &network_policy_json);
    let common = test_common("space-sbx-services-g2", TEST_SANDBOX_ID, 2, &env);

    let pod = build_sandbox_pod(
        &SandboxPodConfig {
            node_services_ready_affinity: true,
            restore_source_node_hard_pin: true,
            ..test_pod_config()
        },
        SandboxPodRequest::Restore {
            artifact_kind: crate::types::ArtifactKind::Full,
            common,
            runtime,
            suspend_restore_storage_uri: Some(
                "s3://space-test/suspend-snapshots/current?region=us-east-1",
            ),
            suspend_restore_source_node: Some("ip-10-0-0-1.ec2.internal"),
            direct_node_name: None,
            preferred_node_name: Some("ip-10-0-0-1.ec2.internal"),
            restore_node_shortlist: vec![
                "ip-10-0-0-1.ec2.internal".to_string(),
                "ip-10-0-0-2.ec2.internal".to_string(),
            ],
            extra_tolerations: &[],
            extra_required_affinity: None,
            fork_disk_restore: None,
            node_group_placement: None,
        },
    );

    // A shortlisted restore keeps the source preference as the only
    // NodeAffinity score; readiness is applied by the API when it builds
    // the shortlist, not as a competing weight here.
    assert!(node_services_ready_terms(&pod).is_empty());
}

fn assert_pod_yaml_snapshot(name: &'static str, pod: &Pod) {
    insta::with_settings!({ snapshot_path => "../fixtures", prepend_module_to_snapshot => false }, {
        insta::assert_yaml_snapshot!(name, pod);
    });
}

fn test_auto_lifecycle_json() -> String {
    let policy = AutoLifecyclePolicy {
        condition_sets: BTreeMap::from([(
            AutoLifecycleConditionSetId::AccessIdle,
            AutoLifecycleConditionSet::access_idle(),
        )]),
        stages: vec![AutoLifecycleStage {
            condition_set: AutoLifecycleConditionSetId::AccessIdle,
            at: 60,
            action: AutoLifecycleIdleAction::Delete,
        }],
        ..Default::default()
    };

    serde_json::to_string(&policy).expect("auto lifecycle policy should serialize")
}

fn test_network_policy_json() -> String {
    let policy = pplx_space_net_common::NetworkPolicyV1::default();

    serde_json::to_string(&policy).expect("network policy should serialize")
}

#[test]
fn sandbox_pod_shapes_render_expected_yaml() {
    let env = BTreeMap::new();
    let config = test_pod_config();
    let auto_lifecycle_json = test_auto_lifecycle_json();
    let network_policy_json = test_network_policy_json();
    let sandbox_owner = test_sandbox_owner();
    let runtime = test_runtime(&sandbox_owner, &auto_lifecycle_json, &network_policy_json);
    let shaped_runtime = SandboxRuntimePodMetadata {
        vcpu: Some(2),
        memory_mib: Some(8192),
        ..runtime
    };
    let activation_attempt_id = crate::types::SandboxActivationAttemptId::from_uuid(
        Uuid::from_u128(0x019f80000000700080000000000000aa),
    );
    let common = |pod_name, sandbox_id, execution_generation| SandboxPodCommon {
        activation_attempt_id: Some(activation_attempt_id),
        ..test_common(pod_name, sandbox_id, execution_generation, &env)
    };

    let on_demand_common = common(
        "space-sbx-019de67463d77ef0a5888e106a878093-g1",
        TEST_SANDBOX_ID,
        1,
    );
    let on_demand = build_sandbox_pod(
        &config,
        SandboxPodRequest::OnDemand {
            common: on_demand_common,
            runtime: shaped_runtime,
            node_pool: None,
            node_group_placement: None,
        },
    );
    // Placed create: the same cold create pinned to an isolation node pool
    // renders a `nodeSelector` entry on the pool label plus a toleration for
    // the pool taint, on top of the usual cluster-wide selectors.
    let placement_pool = NodePoolName::parse("isolated-test-pool").unwrap();
    let on_demand_placed = build_sandbox_pod(
        &config,
        SandboxPodRequest::OnDemand {
            common: common(
                "space-sbx-019de67463d77ef0a5888e106a878093-g8",
                TEST_SANDBOX_ID,
                8,
            ),
            runtime: shaped_runtime,
            node_pool: Some(&placement_pool),
            node_group_placement: None,
        },
    );
    let restore = build_sandbox_pod(
        &config,
        SandboxPodRequest::Restore {
            artifact_kind: crate::types::ArtifactKind::Full,
            common: common(
                "space-sbx-019de67463d77ef0a5888e106a878093-g2",
                TEST_SANDBOX_ID,
                2,
            ),
            runtime,
            suspend_restore_storage_uri: None,
            suspend_restore_source_node: Some("space-worker-a"),
            fork_disk_restore: None,
            direct_node_name: Some("space-worker-a"),
            preferred_node_name: None,
            restore_node_shortlist: Vec::new(),
            extra_tolerations: &[],
            extra_required_affinity: None,
            node_group_placement: None,
        },
    );
    let restore_portable = build_sandbox_pod(
        &config,
        SandboxPodRequest::Restore {
            artifact_kind: crate::types::ArtifactKind::Full,
            common: common(
                "space-sbx-019de67463d77ef0a5888e106a878093-g3",
                TEST_SANDBOX_ID,
                3,
            ),
            runtime,
            suspend_restore_storage_uri: Some(
                "s3://space-test/suspend-snapshots/current?region=us-east-1",
            ),
            suspend_restore_source_node: Some("space-worker-a"),
            fork_disk_restore: None,
            direct_node_name: None,
            preferred_node_name: Some("space-worker-a"),
            restore_node_shortlist: Vec::new(),
            extra_tolerations: &[],
            extra_required_affinity: None,
            node_group_placement: None,
        },
    );
    // Source-aware hard pin on: the same portable restore renders a required
    // `matchFields: metadata.name` term instead of the weight-100 preferred
    // term, keeps the hard ssd-placement-block exclusion, and drops the soft
    // scoring terms (single-target node).
    let restore_source_pinned = build_sandbox_pod(
        &SandboxPodConfig {
            restore_source_node_hard_pin: true,
            ..test_pod_config()
        },
        SandboxPodRequest::Restore {
            artifact_kind: crate::types::ArtifactKind::Full,
            common: common(
                "space-sbx-019de67463d77ef0a5888e106a878093-g4",
                TEST_SANDBOX_ID,
                4,
            ),
            runtime,
            suspend_restore_storage_uri: Some(
                "s3://space-test/suspend-snapshots/current?region=us-east-1",
            ),
            suspend_restore_source_node: Some("space-worker-a"),
            fork_disk_restore: None,
            direct_node_name: None,
            preferred_node_name: Some("space-worker-a"),
            restore_node_shortlist: Vec::new(),
            extra_tolerations: &[],
            extra_required_affinity: None,
            node_group_placement: None,
        },
    );
    // Shortlist mode: ORed required terms for source + fallbacks, the
    // weight-100 preferred source term kept, scoring terms kept.
    let restore_source_shortlist = build_sandbox_pod(
        &SandboxPodConfig {
            restore_source_node_hard_pin: true,
            ..test_pod_config()
        },
        SandboxPodRequest::Restore {
            artifact_kind: crate::types::ArtifactKind::Full,
            common: common(
                "space-sbx-019de67463d77ef0a5888e106a878093-g6",
                TEST_SANDBOX_ID,
                6,
            ),
            runtime,
            suspend_restore_storage_uri: Some(
                "s3://space-test/suspend-snapshots/current?region=us-east-1",
            ),
            suspend_restore_source_node: Some("space-worker-a"),
            fork_disk_restore: None,
            direct_node_name: None,
            preferred_node_name: Some("space-worker-a"),
            restore_node_shortlist: vec![
                "space-worker-a".to_string(),
                "space-worker-b".to_string(),
                "space-worker-c".to_string(),
            ],
            extra_tolerations: &[],
            extra_required_affinity: None,
            node_group_placement: None,
        },
    );
    let restore_storage_only = build_sandbox_pod(
        &config,
        SandboxPodRequest::Restore {
            artifact_kind: crate::types::ArtifactKind::Full,
            common: common(
                "space-sbx-019de67463d77ef0a5888e106a878093-g5",
                TEST_SANDBOX_ID,
                5,
            ),
            runtime,
            suspend_restore_storage_uri: Some(
                "s3://space-test/suspend-snapshots/current?region=us-east-1",
            ),
            suspend_restore_source_node: None,
            fork_disk_restore: None,
            direct_node_name: None,
            preferred_node_name: None,
            restore_node_shortlist: Vec::new(),
            extra_tolerations: &[],
            extra_required_affinity: None,
            node_group_placement: None,
        },
    );
    // Evacuation pod with cohort placement: one Equal-operator toleration
    // and a matching required nodeAffinity term derived from the same
    // cohort key+value pair (the cluster-controller's
    // `drain_evacuation_placement` → `EvacuationPlacement::to_toleration()`
    // + `EvacuationPlacement::to_required_affinity()` path).
    let evacuation_toleration = PodToleration {
        key: Some("space.pplx.net/rollout-cohort".to_string()),
        operator: Some("Equal".to_string()),
        value: Some("test-2".to_string()),
        effect: Some("NoSchedule".to_string()),
        toleration_seconds: None,
    };
    let restore_evacuation = build_sandbox_pod(
        &config,
        SandboxPodRequest::Restore {
            artifact_kind: crate::types::ArtifactKind::Full,
            common: common(
                "space-sbx-019de67463d77ef0a5888e106a878093-g4",
                TEST_SANDBOX_ID,
                4,
            ),
            runtime,
            suspend_restore_storage_uri: Some(
                "s3://space-test/suspend-snapshots/current?region=us-east-1",
            ),
            suspend_restore_source_node: None,
            fork_disk_restore: None,
            direct_node_name: None,
            preferred_node_name: None,
            restore_node_shortlist: Vec::new(),
            extra_tolerations: std::slice::from_ref(&evacuation_toleration),
            extra_required_affinity: Some(("space.pplx.net/rollout-cohort", "test-2")),
            node_group_placement: None,
        },
    );
    let restore_fork_disk = build_sandbox_pod(
        &config,
        SandboxPodRequest::Restore {
            artifact_kind: crate::types::ArtifactKind::Disk,
            common: common(
                "space-sbx-019de67463d77ef0a5888e106a878093-g5",
                TEST_SANDBOX_ID,
                5,
            ),
            runtime,
            suspend_restore_storage_uri: Some(
                "s3://space-test/suspend-snapshots/00/source/019f8000-0000-7000-8000-000000000003?region=us-east-1",
            ),
            suspend_restore_source_node: None,
            fork_disk_restore: Some(Box::new(ForkDiskRestorePodContext {
                source_sandbox_id: "019f8000-0000-7000-8000-000000000002",
                artifact_snapshot_id: "019f8000-0000-7000-8000-000000000003",
                target_owner: "user:019f8000-0000-7000-8000-000000000004",
            })),
            direct_node_name: None,
            preferred_node_name: None,
            restore_node_shortlist: Vec::new(),
            extra_tolerations: &[],
            extra_required_affinity: None,
            node_group_placement: None,
        },
    );
    // BYOK: an encrypted cold create renders the volume-encryption
    // annotation the shim forwards into CreateRequest; every other shape
    // omits it entirely (absent = plaintext).
    let on_demand_encrypted = build_sandbox_pod(
        &config,
        SandboxPodRequest::OnDemand {
            common: common(
                "space-sbx-019de67463d77ef0a5888e106a878093-g6",
                TEST_SANDBOX_ID,
                6,
            ),
            runtime: SandboxRuntimePodMetadata {
                volume_encryption: true,
                ..shaped_runtime
            },
            node_pool: None,
            node_group_placement: None,
        },
    );
    // Node-local-service readiness bias on: the cold create additionally
    // carries the weight-100 `node-services-ready NotIn ["false"]`
    // preference, so a node whose node-ctl/volume-manager pod is mid-restart
    // scores below the serviced fleet while staying schedulable.
    let on_demand_node_services_bias = build_sandbox_pod(
        &SandboxPodConfig {
            node_services_ready_affinity: true,
            ..test_pod_config()
        },
        SandboxPodRequest::OnDemand {
            common: common(
                "space-sbx-019de67463d77ef0a5888e106a878093-g7",
                TEST_SANDBOX_ID,
                7,
            ),
            runtime: shaped_runtime,
            node_pool: None,
            node_group_placement: None,
        },
    );
    let mut pooled_common = common(
        "space-sbx-019e5f38cd2e7400b3c8e33ae36692d7-g1",
        TEST_POOL_SANDBOX_ID,
        1,
    );
    pooled_common.activation_attempt_id = None;
    let pooled = build_sandbox_pod(
        &config,
        SandboxPodRequest::Pooled {
            common: pooled_common,
            pool: SandboxPoolPodMetadata {
                slot_id: TEST_POOL_SLOT_ID,
                config_id: TEST_POOL_CONFIG_ID,
                vcpu: VcpuCount::try_from_u32(2).unwrap(),
                memory: MemorySize::mib(8192).unwrap(),
                disk_mib: None,
                disk_io_limits: Default::default(),
            },
            node_group_placement: None,
        },
    );

    // The node-group placement shapes: a generic template on the shared pool, a
    // generic template from an org with dedicated capacity (spill to shared
    // still legal), the same generic template with no shared fallback offered,
    // and an org-specific template whose candidate set is the dedicated group.
    let shared_placement = shared_node_group_placement();
    let generic_on_shared = build_sandbox_pod(
        &config,
        SandboxPodRequest::OnDemand {
            common: common(
                "space-sbx-019de67463d77ef0a5888e106a878093-g5",
                TEST_SANDBOX_ID,
                5,
            ),
            runtime: shaped_runtime,
            node_group_placement: Some(&shared_placement),
            node_pool: None,
        },
    );
    let generic_dedicated_placement = dedicated_node_group_placement(true);
    let generic_on_dedicated = build_sandbox_pod(
        &config,
        SandboxPodRequest::OnDemand {
            common: common(
                "space-sbx-019de67463d77ef0a5888e106a878093-g6",
                TEST_SANDBOX_ID,
                6,
            ),
            runtime: shaped_runtime,
            node_group_placement: Some(&generic_dedicated_placement),
            node_pool: None,
        },
    );
    // A *generic* template whose org has dedicated capacity and no shared
    // fallback on offer: same boundary shape, reached for a different reason
    // than the org-specific case below.
    let dedicated_only_placement = dedicated_node_group_placement(false);
    let generic_on_dedicated_only = build_sandbox_pod(
        &config,
        SandboxPodRequest::OnDemand {
            common: common(
                "space-sbx-019de67463d77ef0a5888e106a878093-g7",
                TEST_SANDBOX_ID,
                7,
            ),
            runtime: shaped_runtime,
            node_group_placement: Some(&dedicated_only_placement),
            node_pool: None,
        },
    );
    // The org-specific shape: the placement and the pod both name the
    // org-scoped template, so the cross-check passes and the render succeeds
    // — the success counterpart to `sandbox_pod_on_demand_org_mismatch`.
    let org_specific_placement = org_specific_node_group_placement();
    let org_specific_on_dedicated = build_sandbox_pod(
        &config,
        SandboxPodRequest::OnDemand {
            common: SandboxPodCommon {
                template: ORG_SPECIFIC_TEMPLATE,
                ..common(
                    "space-sbx-019de67463d77ef0a5888e106a878093-g20",
                    TEST_SANDBOX_ID,
                    20,
                )
            },
            runtime: shaped_runtime,
            node_group_placement: Some(&org_specific_placement),
            node_pool: None,
        },
    );

    // Placement on the source-aware restore shapes: the boundary must appear in
    // every ORed shortlist term, and the pooled pod must render the boundary
    // with no dedicated toleration.
    let restore_shortlist_placed = build_sandbox_pod(
        &SandboxPodConfig {
            restore_source_node_hard_pin: true,
            ..test_pod_config()
        },
        SandboxPodRequest::Restore {
            artifact_kind: crate::types::ArtifactKind::Full,
            common: common(
                "space-sbx-019de67463d77ef0a5888e106a878093-g11",
                TEST_SANDBOX_ID,
                11,
            ),
            runtime,
            node_group_placement: Some(&generic_dedicated_placement),
            suspend_restore_storage_uri: Some(
                "s3://space-test/suspend-snapshots/current?region=us-east-1",
            ),
            suspend_restore_source_node: Some("space-worker-a"),
            fork_disk_restore: None,
            restore_node_shortlist: vec![
                "space-worker-a".to_string(),
                "space-worker-b".to_string(),
            ],
            direct_node_name: None,
            preferred_node_name: Some("space-worker-a"),
            extra_tolerations: &[],
            extra_required_affinity: None,
        },
    );
    let pooled_placed = build_sandbox_pod(
        &config,
        SandboxPodRequest::Pooled {
            common: common(
                "space-sbx-019e5f38cd2e7400b3c8e33ae36692d7-g2",
                TEST_POOL_SANDBOX_ID,
                2,
            ),
            pool: SandboxPoolPodMetadata {
                slot_id: TEST_POOL_SLOT_ID,
                config_id: TEST_POOL_CONFIG_ID,
                vcpu: VcpuCount::try_from_u32(2).unwrap(),
                memory: MemorySize::mib(8192).unwrap(),
                disk_mib: None,
                disk_io_limits: Default::default(),
            },
            node_group_placement: Some(&shared_placement),
        },
    );

    // The pre-bound and org-mismatch shapes, so both wire forms are visible in
    // the diff rather than only asserted in a unit test.
    let restore_direct_placed = build_sandbox_pod(
        &config,
        SandboxPodRequest::Restore {
            artifact_kind: crate::types::ArtifactKind::Full,
            common: common(
                "space-sbx-019de67463d77ef0a5888e106a878093-g15",
                TEST_SANDBOX_ID,
                15,
            ),
            runtime,
            node_group_placement: Some(&dedicated_only_placement),
            suspend_restore_storage_uri: None,
            suspend_restore_source_node: Some("space-worker-a"),
            fork_disk_restore: None,
            restore_node_shortlist: Vec::new(),
            direct_node_name: Some("space-worker-a"),
            preferred_node_name: None,
            extra_tolerations: &[],
            extra_required_affinity: None,
        },
    );
    let mismatched_placement = NodeGroupPlacement::for_caller(
        Some(OrgId::from_uuid(Uuid::from_u128(0x999))),
        &placement_template(),
        &[
            shared_group("workers"),
            dedicated_group("acme-dedicated", OrgId::from_uuid(Uuid::from_u128(0x999))),
        ],
    )
    .unwrap();
    let on_demand_org_mismatch = build_sandbox_pod(
        &config,
        SandboxPodRequest::OnDemand {
            common: common(
                "space-sbx-019de67463d77ef0a5888e106a878093-g16",
                TEST_SANDBOX_ID,
                16,
            ),
            runtime: shaped_runtime,
            node_group_placement: Some(&mismatched_placement),
            node_pool: None,
        },
    );

    for (name, pod) in [
        ("sandbox_pod_on_demand_create", on_demand),
        ("sandbox_pod_on_demand_create_placed", on_demand_placed),
        ("sandbox_pod_restore", restore),
        ("sandbox_pod_restore_portable", restore_portable),
        ("sandbox_pod_restore_source_pinned", restore_source_pinned),
        (
            "sandbox_pod_restore_source_shortlist",
            restore_source_shortlist,
        ),
        ("sandbox_pod_restore_storage_only", restore_storage_only),
        ("sandbox_pod_restore_evacuation", restore_evacuation),
        ("sandbox_pod_restore_fork_disk", restore_fork_disk),
        (
            "sandbox_pod_on_demand_create_encrypted",
            on_demand_encrypted,
        ),
        (
            "sandbox_pod_on_demand_create_node_services_bias",
            on_demand_node_services_bias,
        ),
        ("sandbox_pod_pooled", pooled),
        ("sandbox_pod_generic_on_shared", generic_on_shared),
        (
            "sandbox_pod_restore_shortlist_placed",
            restore_shortlist_placed,
        ),
        ("sandbox_pod_pooled_placed", pooled_placed),
        ("sandbox_pod_restore_direct_placed", restore_direct_placed),
        ("sandbox_pod_on_demand_org_mismatch", on_demand_org_mismatch),
        ("sandbox_pod_generic_on_dedicated", generic_on_dedicated),
        (
            "sandbox_pod_generic_on_dedicated_only",
            generic_on_dedicated_only,
        ),
        (
            "sandbox_pod_org_specific_on_dedicated",
            org_specific_on_dedicated,
        ),
    ] {
        assert_pod_yaml_snapshot(name, &pod);
    }
}

#[test]
fn on_demand_placed_pod_gets_pool_node_selector_and_toleration() {
    let env = BTreeMap::new();
    let auto_lifecycle_json = test_auto_lifecycle_json();
    let network_policy_json = test_network_policy_json();
    let sandbox_owner = test_sandbox_owner();
    let runtime = test_runtime(&sandbox_owner, &auto_lifecycle_json, &network_policy_json);
    let common = test_common("space-sbx-placed-g1", TEST_SANDBOX_ID, 1, &env);
    let unpinned_common = test_common("space-sbx-unpinned-g1", TEST_SANDBOX_ID, 1, &env);
    let pool = NodePoolName::parse("isolated-test-pool").unwrap();
    let pod = build_sandbox_pod(
        &test_pod_config(),
        SandboxPodRequest::OnDemand {
            common,
            runtime,
            node_pool: Some(&pool),
            node_group_placement: None,
        },
    );
    let spec = pod.spec.as_ref().unwrap();

    // Plain nodeSelector entry on the pool label (NOT a nodeAffinity term).
    assert_eq!(
        spec.node_selector
            .as_ref()
            .and_then(|selector| selector.get(SANDBOX_PLACEMENT_POOL_LABEL_KEY))
            .map(String::as_str),
        Some("isolated-test-pool")
    );
    // Matching toleration for the pool taint.
    assert!(
        spec.tolerations.as_ref().is_some_and(|tolerations| {
            tolerations.iter().any(|tol| {
                tol.key.as_deref() == Some(SANDBOX_PLACEMENT_POOL_LABEL_KEY)
                    && tol.operator.as_deref() == Some("Exists")
                    && tol.effect.as_deref() == Some("NoSchedule")
            })
        }),
        "placed pod must tolerate the pool taint: {:?}",
        spec.tolerations
    );

    // Unpinned create carries neither.
    let unpinned = build_sandbox_pod(
        &test_pod_config(),
        SandboxPodRequest::OnDemand {
            common: unpinned_common,
            runtime,
            node_pool: None,
            node_group_placement: None,
        },
    );
    let unpinned_spec = unpinned.spec.as_ref().unwrap();
    assert!(
        unpinned_spec
            .node_selector
            .as_ref()
            .is_none_or(|selector| !selector.contains_key(SANDBOX_PLACEMENT_POOL_LABEL_KEY))
    );
    assert!(unpinned_spec.tolerations.is_none());
}

#[test]
fn every_sandbox_pod_shape_requests_one_network_slot() {
    let env = BTreeMap::new();
    let config = test_pod_config();
    let auto_lifecycle_json = test_auto_lifecycle_json();
    let network_policy_json = test_network_policy_json();
    let sandbox_owner = SandboxOwner::parse("user@example.com").unwrap();
    let runtime = test_runtime(&sandbox_owner, &auto_lifecycle_json, &network_policy_json);
    let common = |pod_name, sandbox_id, execution_generation| {
        test_common(pod_name, sandbox_id, execution_generation, &env)
    };
    let pods = [
        build_sandbox_pod(
            &config,
            SandboxPodRequest::OnDemand {
                common: common("space-sbx-on-demand-g1", TEST_SANDBOX_ID, 1),
                runtime,
                node_pool: None,
                node_group_placement: None,
            },
        ),
        build_sandbox_pod(
            &config,
            SandboxPodRequest::Restore {
                artifact_kind: crate::types::ArtifactKind::Full,
                common: common("space-sbx-restore-g2", TEST_SANDBOX_ID, 2),
                runtime,
                suspend_restore_storage_uri: None,
                suspend_restore_source_node: None,
                fork_disk_restore: None,
                direct_node_name: None,
                preferred_node_name: None,
                restore_node_shortlist: Vec::new(),
                extra_tolerations: &[],
                extra_required_affinity: None,
                node_group_placement: None,
            },
        ),
        build_sandbox_pod(
            &config,
            SandboxPodRequest::Pooled {
                common: common("space-sbx-pooled-g1", TEST_POOL_SANDBOX_ID, 1),
                pool: SandboxPoolPodMetadata {
                    slot_id: TEST_POOL_SLOT_ID,
                    config_id: TEST_POOL_CONFIG_ID,
                    vcpu: VcpuCount::try_from_u32(2).unwrap(),
                    memory: MemorySize::mib(8192).unwrap(),
                    disk_mib: None,
                    disk_io_limits: Default::default(),
                },
                node_group_placement: None,
            },
        ),
    ];

    let one = Quantity("1".to_string());
    for pod in &pods {
        let resources = pod
            .spec
            .as_ref()
            .unwrap()
            .containers
            .first()
            .expect("sandbox pod must have a container")
            .resources
            .as_ref()
            .expect("every sandbox pod must request the network-slot resource");
        // Request and limit are both exactly 1 so the extended resource is
        // never overcommitted: one sandbox pod == one node-manager slot.
        assert_eq!(
            resources
                .requests
                .as_ref()
                .and_then(|requests| requests.get(SANDBOX_NETWORK_SLOTS_RESOURCE)),
            Some(&one)
        );
        assert_eq!(
            resources
                .limits
                .as_ref()
                .and_then(|limits| limits.get(SANDBOX_NETWORK_SLOTS_RESOURCE)),
            Some(&one)
        );
    }
}

#[test]
fn sandbox_pod_renders_local_policy_authorities_and_disk_io_limits() {
    let env = BTreeMap::new();
    let config = test_pod_config();
    let auto_lifecycle_json = test_auto_lifecycle_json();
    let network_policy_json = test_network_policy_json();
    let sandbox_owner = test_sandbox_owner();
    let pod = build_sandbox_pod(
        &config,
        SandboxPodRequest::OnDemand {
            common: test_common(
                "space-sbx-019de67463d77ef0a5888e106a878093-g4",
                TEST_SANDBOX_ID,
                4,
                &env,
            ),
            runtime: SandboxRuntimePodMetadata {
                auto_lifecycle_authority: PolicyAuthority::Local,
                network_policy_authority: PolicyAuthority::Local,
                disk_io_limits: crate::types::DiskIoLimits::try_new(5_000, 200).unwrap(),
                ..test_runtime(&sandbox_owner, &auto_lifecycle_json, &network_policy_json)
            },
            node_pool: None,
            node_group_placement: None,
        },
    );
    let annotations = pod
        .metadata
        .annotations
        .as_ref()
        .expect("sandbox pod should include annotations");

    assert_eq!(
        annotations.get(SANDBOX_AUTO_LIFECYCLE_AUTHORITY_ANNOTATION),
        Some(&PolicyAuthority::Local.to_string())
    );
    assert_eq!(
        annotations.get(SANDBOX_NETWORK_POLICY_AUTHORITY_ANNOTATION),
        Some(&PolicyAuthority::Local.to_string())
    );
    assert_eq!(
        annotations.get(SANDBOX_DISK_IO_MAX_OPERATIONS_PER_SEC_ANNOTATION),
        Some(&"5000".to_string())
    );
    assert_eq!(
        annotations.get(SANDBOX_DISK_IO_MAX_BANDWIDTH_MIB_PER_SEC_ANNOTATION),
        Some(&"200".to_string())
    );
}

#[test]
fn portable_restore_prefers_source_node_and_carries_remote_fallback() {
    let env = BTreeMap::new();
    let config = test_pod_config();
    let auto_lifecycle_json = test_auto_lifecycle_json();
    let network_policy_json = test_network_policy_json();
    let sandbox_owner = test_sandbox_owner();
    let runtime = test_runtime(&sandbox_owner, &auto_lifecycle_json, &network_policy_json);
    let pod = build_sandbox_pod(
        &config,
        SandboxPodRequest::Restore {
            artifact_kind: crate::types::ArtifactKind::Full,
            common: test_common(
                "space-sbx-019de67463d77ef0a5888e106a878093-g3",
                TEST_SANDBOX_ID,
                3,
                &env,
            ),
            runtime,
            suspend_restore_storage_uri: Some(
                "s3://space-test/suspend-snapshots/current?region=us-east-1",
            ),
            suspend_restore_source_node: Some("space-worker-a"),
            fork_disk_restore: None,
            direct_node_name: None,
            preferred_node_name: Some("space-worker-a"),
            restore_node_shortlist: Vec::new(),
            extra_tolerations: &[],
            extra_required_affinity: None,
            node_group_placement: None,
        },
    );

    let spec = pod.spec.as_ref().unwrap();
    let annotations = pod
        .metadata
        .annotations
        .as_ref()
        .expect("sandbox pod should include annotations");
    assert_eq!(
        annotations.get(SANDBOX_CHECKPOINT_ANNOTATION),
        Some(&"suspend".to_string()),
        "restore pods must carry checkpoint intent to the shim"
    );
    assert_eq!(
        annotations.get(SANDBOX_SUSPEND_RESTORE_STORAGE_URI_ANNOTATION),
        Some(&"s3://space-test/suspend-snapshots/current?region=us-east-1".to_string()),
        "restore pods must bind the node to the API-selected remote snapshot"
    );
    assert_eq!(
        annotations.get(SANDBOX_SUSPEND_RESTORE_SOURCE_NODE_ANNOTATION),
        Some(&"space-worker-a".to_string()),
        "restore pods must identify the only node trusted for local restore"
    );
    let node_affinity = spec
        .affinity
        .as_ref()
        .and_then(|a| a.node_affinity.as_ref())
        .expect("source-node preference should produce node affinity");
    // Portable restore must not *pin* to the previous node: the only required
    // term is the hard placement-block exclusion, never a metadata.name match.
    let required_term = node_affinity
        .required_during_scheduling_ignored_during_execution
        .as_ref()
        .expect("the placement-block exclusion should render a required term")
        .node_selector_terms
        .first()
        .expect("one required node selector term");
    assert!(
        required_term.match_fields.is_none(),
        "portable restore must not require the previous node"
    );
    let block = required_term
        .match_expressions
        .as_ref()
        .expect("required term must carry the placement-block exclusion");
    assert_eq!(block.len(), 1);
    assert_eq!(block[0].key, SSD_PLACEMENT_BLOCKED_LABEL);
    assert_eq!(block[0].operator, "NotIn");
    assert_eq!(
        block[0].values,
        Some(vec![SSD_PLACEMENT_BLOCKED_VALUE.to_string()])
    );
    let preferred = node_affinity
        .preferred_during_scheduling_ignored_during_execution
        .as_ref()
        .expect("portable restore should prefer the previous node");
    // Source-node preference first, then template-cache preference, then
    // the SSD-headroom tier bias terms.
    assert_eq!(
        preferred.len(),
        2 + usize::from(SsdHeadroomTier::MAX.as_u8())
    );
    assert_eq!(
        preferred
            .first()
            .expect("source-node preference must be first")
            .preference
            .match_fields
            .as_ref()
            .unwrap()
            .first()
            .expect("source-node preference must match metadata.name")
            .values,
        Some(vec!["space-worker-a".to_string()])
    );
    assert_eq!(
        preferred
            .first()
            .expect("source-node preference must be first")
            .weight,
        RESTORE_SOURCE_NODE_PREFERENCE_WEIGHT
    );
    let template_cache_preference = preferred
        .get(1)
        .expect("template-cache preference must follow source-node preference");
    let template_cache_match = template_cache_preference
        .preference
        .match_expressions
        .as_ref()
        .and_then(|expressions| expressions.first())
        .expect("template-cache preference must match the template-build label");
    assert_eq!(
        template_cache_match.key,
        template_build_presence_label_key("seed", TEST_CONTENT_HASH)
    );
    assert_eq!(template_cache_match.operator, "In");
    assert_eq!(
        template_cache_match.values,
        Some(vec![TEMPLATE_PRESENT_LABEL_VALUE.to_string()])
    );
    assert_eq!(
        template_cache_preference.weight,
        RESTORE_TEMPLATE_CACHE_PREFERENCE_WEIGHT
    );
    assert!(
        spec.tolerations.is_none(),
        "user-API Restore must not emit pod tolerations"
    );
}

/// Flag on: a portable restore that knows its source node renders a
/// **required** `matchFields: metadata.name` pin (not `spec.nodeName`),
/// keeps the hard ssd-placement-block exclusion in that same required term,
/// and drops every scoring term (single candidate node).
#[test]
fn build_sandbox_pod_hard_pins_portable_restore_to_source_node() {
    let env = BTreeMap::new();
    let config = SandboxPodConfig {
        restore_source_node_hard_pin: true,
        ..test_pod_config()
    };
    let auto_lifecycle_json = test_auto_lifecycle_json();
    let network_policy_json = test_network_policy_json();
    let sandbox_owner = test_sandbox_owner();
    let runtime = test_runtime(&sandbox_owner, &auto_lifecycle_json, &network_policy_json);
    let pod = build_sandbox_pod(
        &config,
        SandboxPodRequest::Restore {
            artifact_kind: crate::types::ArtifactKind::Full,
            common: test_common("space-sbx-pin-g2", TEST_SANDBOX_ID, 2, &env),
            runtime,
            suspend_restore_storage_uri: Some("s3://space-test/suspend-snapshots/current"),
            suspend_restore_source_node: Some("ip-10-0-0-1.ec2.internal"),
            fork_disk_restore: None,
            direct_node_name: None,
            preferred_node_name: Some("ip-10-0-0-1.ec2.internal"),
            restore_node_shortlist: Vec::new(),
            extra_tolerations: &[],
            extra_required_affinity: None,
            node_group_placement: None,
        },
    );

    let spec = pod.spec.as_ref().unwrap();
    assert_eq!(
        spec.node_name, None,
        "a required pin must schedule through kube-scheduler, not bypass it"
    );
    let terms = required_node_selector_terms(&pod);
    assert_eq!(terms.len(), 1, "exactly one required nodeAffinity term");
    let match_fields = terms[0]
        .match_fields
        .as_ref()
        .expect("required term pins via matchFields");
    assert_eq!(match_fields.len(), 1);
    assert_eq!(match_fields[0].key, "metadata.name");
    assert_eq!(match_fields[0].operator, "In");
    assert_eq!(
        match_fields[0].values.as_deref(),
        Some(["ip-10-0-0-1.ec2.internal".to_string()].as_slice())
    );
    // The hard disk-safety exclusion stays in the same required term even
    // for a single-target pin: a placement-blocked source must leave the pod
    // Unschedulable for the in-request cross-node re-drive rather than
    // overload that node.
    let exprs = terms[0]
        .match_expressions
        .as_ref()
        .expect("hard pin must keep the ssd-placement-blocked exclusion");
    assert_eq!(exprs.len(), 1);
    assert_eq!(exprs[0].key, SSD_PLACEMENT_BLOCKED_LABEL);
    assert_eq!(exprs[0].operator, "NotIn");
    assert_eq!(
        exprs[0].values.as_deref(),
        Some([SSD_PLACEMENT_BLOCKED_VALUE.to_string()].as_slice())
    );
    assert!(
        preferred_scheduling_terms(&pod).is_empty(),
        "a required pin targets one node, so no preferred scoring terms are rendered"
    );
}

/// The cross-node re-drive successor's shape: source node known (carried as
/// the trusted-local-bytes annotation) but **no** placement pin of any kind
/// — `preferred_node_name`/`direct_node_name` are both `None`, so even with
/// the hard-pin flag on, the pod renders the source annotation while
/// scheduling anywhere.
#[test]
fn build_sandbox_pod_source_annotation_without_pin_renders_no_source_affinity() {
    let env = BTreeMap::new();
    let config = SandboxPodConfig {
        restore_source_node_hard_pin: true,
        ..test_pod_config()
    };
    let auto_lifecycle_json = test_auto_lifecycle_json();
    let network_policy_json = test_network_policy_json();
    let sandbox_owner = test_sandbox_owner();
    let runtime = test_runtime(&sandbox_owner, &auto_lifecycle_json, &network_policy_json);
    let pod = build_sandbox_pod(
        &config,
        SandboxPodRequest::Restore {
            artifact_kind: crate::types::ArtifactKind::Full,
            common: test_common("space-sbx-redrive-g3", TEST_SANDBOX_ID, 3, &env),
            runtime,
            suspend_restore_storage_uri: Some("s3://space-test/suspend-snapshots/current"),
            suspend_restore_source_node: Some("ip-10-0-0-1.ec2.internal"),
            fork_disk_restore: None,
            direct_node_name: None,
            preferred_node_name: None,
            restore_node_shortlist: Vec::new(),
            extra_tolerations: &[],
            extra_required_affinity: None,
            node_group_placement: None,
        },
    );

    let annotations = pod
        .metadata
        .annotations
        .as_ref()
        .expect("re-drive successor should carry annotations");
    assert_eq!(
        annotations.get(SANDBOX_SUSPEND_RESTORE_SOURCE_NODE_ANNOTATION),
        Some(&"ip-10-0-0-1.ec2.internal".to_string()),
        "source provenance must survive the re-drive"
    );
    assert_eq!(pod.spec.as_ref().unwrap().node_name, None);
    for term in required_node_selector_terms(&pod) {
        assert!(
            term.match_fields.is_none(),
            "re-drive successor must not carry any metadata.name pin"
        );
    }
    assert!(
        preferred_scheduling_terms(&pod)
            .iter()
            .all(|term| term.preference.match_fields.is_none()),
        "re-drive successor must not carry a preferred source term either"
    );
}

/// Shortlist mode: source + fallbacks render as ORed required
/// `matchFields: metadata.name` terms (one node per term, each repeating
/// the hard ssd-placement-block exclusion), the source keeps its
/// weight-100 **preferred** term so it wins scoring whenever feasible,
/// and the scoring terms (template-cache, ssd tiers) stay on to rank the
/// fallbacks.
#[test]
fn build_sandbox_pod_shortlist_renders_or_terms_with_preferred_source() {
    let env = BTreeMap::new();
    let config = SandboxPodConfig {
        restore_source_node_hard_pin: true,
        ..test_pod_config()
    };
    let auto_lifecycle_json = test_auto_lifecycle_json();
    let network_policy_json = test_network_policy_json();
    let sandbox_owner = test_sandbox_owner();
    let runtime = test_runtime(&sandbox_owner, &auto_lifecycle_json, &network_policy_json);
    let pod = build_sandbox_pod(
        &config,
        SandboxPodRequest::Restore {
            artifact_kind: crate::types::ArtifactKind::Full,
            common: test_common("space-sbx-shortlist-g2", TEST_SANDBOX_ID, 2, &env),
            runtime,
            suspend_restore_storage_uri: Some("s3://space-test/suspend-snapshots/current"),
            suspend_restore_source_node: Some("source-node"),
            fork_disk_restore: None,
            direct_node_name: None,
            preferred_node_name: Some("source-node"),
            restore_node_shortlist: vec![
                "source-node".to_string(),
                "fallback-a".to_string(),
                "fallback-b".to_string(),
            ],
            extra_tolerations: &[],
            extra_required_affinity: None,
            node_group_placement: None,
        },
    );

    let spec = pod.spec.as_ref().unwrap();
    assert_eq!(spec.node_name, None);
    let terms = required_node_selector_terms(&pod);
    assert_eq!(terms.len(), 3, "one ORed required term per shortlist node");
    let pinned_names: Vec<&str> = terms
        .iter()
        .map(|term| {
            let fields = term
                .match_fields
                .as_ref()
                .expect("every shortlist term pins one node via matchFields");
            assert_eq!(fields.len(), 1, "metadata.name allows a single value");
            assert_eq!(fields[0].key, "metadata.name");
            fields[0].values.as_ref().unwrap()[0].as_str()
        })
        .collect();
    assert_eq!(pinned_names, ["source-node", "fallback-a", "fallback-b"]);
    for term in &terms {
        let exprs = term
            .match_expressions
            .as_ref()
            .expect("every ORed term repeats the hard exclusions");
        assert_eq!(exprs.len(), 1);
        assert_eq!(exprs[0].key, SSD_PLACEMENT_BLOCKED_LABEL);
    }
    let preferred = preferred_scheduling_terms(&pod);
    let source_pref = preferred
        .first()
        .expect("shortlist keeps the weight-100 preferred source term");
    assert_eq!(source_pref.weight, RESTORE_SOURCE_NODE_PREFERENCE_WEIGHT);
    assert_eq!(
        source_pref.preference.match_fields.as_ref().unwrap()[0]
            .values
            .as_deref(),
        Some(["source-node".to_string()].as_slice())
    );
    // The source term must be the ONLY NodeAffinity preference: preferred
    // weights cap at 100, so a fallback matching template-cache + a top
    // ssd tier would otherwise tie the source and hand the decision to
    // the resource plugins even when the source fits.
    assert_eq!(
        preferred.len(),
        1,
        "no competing scoring terms in shortlist mode"
    );
}

/// A hard-pinned restore built with `ssd_headroom_tier_affinity` **off**
/// carries only the bare `metadata.name` pin — no disk-safety exclusion to
/// keep, no scoring terms — so the required term is a pure NodeNames match.
#[test]
fn build_sandbox_pod_hard_pin_without_ssd_affinity_omits_placement_block() {
    let env = BTreeMap::new();
    let config = SandboxPodConfig {
        restore_source_node_hard_pin: true,
        ssd_headroom_tier_affinity: false,
        ..test_pod_config()
    };
    let auto_lifecycle_json = test_auto_lifecycle_json();
    let network_policy_json = test_network_policy_json();
    let sandbox_owner = test_sandbox_owner();
    let runtime = test_runtime(&sandbox_owner, &auto_lifecycle_json, &network_policy_json);
    let pod = build_sandbox_pod(
        &config,
        SandboxPodRequest::Restore {
            artifact_kind: crate::types::ArtifactKind::Full,
            common: test_common("space-sbx-pin-noheadroom-g2", TEST_SANDBOX_ID, 2, &env),
            runtime,
            suspend_restore_storage_uri: Some("s3://space-test/suspend-snapshots/current"),
            suspend_restore_source_node: Some("ip-10-0-0-1.ec2.internal"),
            fork_disk_restore: None,
            direct_node_name: None,
            preferred_node_name: Some("ip-10-0-0-1.ec2.internal"),
            restore_node_shortlist: Vec::new(),
            extra_tolerations: &[],
            extra_required_affinity: None,
            node_group_placement: None,
        },
    );

    let terms = required_node_selector_terms(&pod);
    assert_eq!(terms.len(), 1);
    assert!(
        terms[0]
            .match_fields
            .as_ref()
            .is_some_and(|fields| fields.iter().any(|field| field.key == "metadata.name")),
        "pin still renders the metadata.name matchField"
    );
    assert!(
        terms[0].match_expressions.is_none(),
        "no ssd affinity means no placement-block exclusion in the required term"
    );
}

#[test]
fn restore_with_extra_tolerations_renders_pod_tolerations() {
    let env = BTreeMap::new();
    let config = test_pod_config();
    let auto_lifecycle_json = test_auto_lifecycle_json();
    let network_policy_json = test_network_policy_json();
    let sandbox_owner = test_sandbox_owner();
    let runtime = test_runtime(&sandbox_owner, &auto_lifecycle_json, &network_policy_json);
    let tolerations = vec![PodToleration {
        key: Some("space.pplx.net/cohort".to_string()),
        operator: Some("Equal".to_string()),
        value: Some("green".to_string()),
        effect: Some("NoSchedule".to_string()),
        toleration_seconds: None,
    }];
    let pod = build_sandbox_pod(
        &config,
        SandboxPodRequest::Restore {
            artifact_kind: crate::types::ArtifactKind::Full,
            common: test_common(
                "space-sbx-019de67463d77ef0a5888e106a878093-g4",
                TEST_SANDBOX_ID,
                4,
                &env,
            ),
            runtime,
            suspend_restore_storage_uri: Some(
                "s3://space-test/suspend-snapshots/current?region=us-east-1",
            ),
            suspend_restore_source_node: None,
            fork_disk_restore: None,
            direct_node_name: None,
            preferred_node_name: None,
            restore_node_shortlist: Vec::new(),
            extra_tolerations: &tolerations,
            extra_required_affinity: None,
            node_group_placement: None,
        },
    );

    let rendered = pod
        .spec
        .as_ref()
        .unwrap()
        .tolerations
        .as_ref()
        .expect("tolerations should render when extra_tolerations is non-empty");
    assert_eq!(rendered.len(), 1);
    let rendered = rendered.first().expect("one rendered toleration");
    assert_eq!(rendered.key.as_deref(), Some("space.pplx.net/cohort"));
    assert_eq!(rendered.operator.as_deref(), Some("Equal"));
    assert_eq!(rendered.value.as_deref(), Some("green"));
    assert_eq!(rendered.effect.as_deref(), Some("NoSchedule"));
    assert_eq!(rendered.toleration_seconds, None);
}

#[test]
fn pod_toleration_deserializes_kubernetes_camelcase_wire_format() {
    let json = r#"{
            "key": "space.pplx.net/cohort",
            "operator": "Equal",
            "value": "green",
            "effect": "NoExecute",
            "tolerationSeconds": 120
        }"#;
    let parsed: PodToleration = serde_json::from_str(json).expect("must parse");
    assert_eq!(parsed.key.as_deref(), Some("space.pplx.net/cohort"));
    assert_eq!(parsed.operator.as_deref(), Some("Equal"));
    assert_eq!(parsed.value.as_deref(), Some("green"));
    assert_eq!(parsed.effect.as_deref(), Some("NoExecute"));
    assert_eq!(parsed.toleration_seconds, Some(120));
}

#[test]
fn pod_toleration_to_k8s_toleration_preserves_all_fields() {
    let toleration = PodToleration {
        key: Some("k".to_string()),
        operator: Some("Exists".to_string()),
        value: None,
        effect: Some("NoSchedule".to_string()),
        toleration_seconds: Some(42),
    };
    let k8s = toleration.to_k8s_toleration();
    assert_eq!(k8s.key.as_deref(), Some("k"));
    assert_eq!(k8s.operator.as_deref(), Some("Exists"));
    assert_eq!(k8s.value, None);
    assert_eq!(k8s.effect.as_deref(), Some("NoSchedule"));
    assert_eq!(k8s.toleration_seconds, Some(42));
}

#[test]
fn evacuation_placement_from_parts_rejects_empty_fields() {
    assert!(EvacuationPlacement::from_parts("", "test-2").is_none());
    assert!(EvacuationPlacement::from_parts("space.pplx.net/rollout-cohort", "").is_none());
    assert!(EvacuationPlacement::from_parts("", "").is_none());
    assert!(EvacuationPlacement::from_parts("  ", "test-2").is_none());
}

#[test]
fn evacuation_placement_derives_toleration_and_affinity() {
    let p = EvacuationPlacement::from_parts("space.pplx.net/rollout-cohort", "test-2")
        .expect("non-empty fields must succeed");
    let tol = p.to_toleration();
    assert_eq!(tol.key.as_deref(), Some("space.pplx.net/rollout-cohort"));
    assert_eq!(tol.operator.as_deref(), Some("Equal"));
    assert_eq!(tol.value.as_deref(), Some("test-2"));
    assert_eq!(tol.effect.as_deref(), Some("NoSchedule"));
    assert_eq!(tol.toleration_seconds, None);
    let (key, value) = p.to_required_affinity();
    assert_eq!(key, "space.pplx.net/rollout-cohort");
    assert_eq!(value, "test-2");
}

#[test]
fn extra_required_affinity_renders_into_required_node_selector_term() {
    // When extra_required_affinity is Some, the rendered pod must carry
    // a required nodeAffinity term matching the label.
    let env = BTreeMap::new();
    let config = test_pod_config();
    let auto_lifecycle_json = test_auto_lifecycle_json();
    let network_policy_json = test_network_policy_json();
    let sandbox_owner = test_sandbox_owner();
    let toleration = PodToleration {
        key: Some("space.pplx.net/rollout-cohort".to_string()),
        operator: Some("Equal".to_string()),
        value: Some("test-2".to_string()),
        effect: Some("NoSchedule".to_string()),
        toleration_seconds: None,
    };
    let runtime = test_runtime(&sandbox_owner, &auto_lifecycle_json, &network_policy_json);
    let pod = build_sandbox_pod(
        &config,
        SandboxPodRequest::Restore {
            artifact_kind: crate::types::ArtifactKind::Full,
            common: test_common(
                "space-sbx-019de67463d77ef0a5888e106a878093-g5",
                TEST_SANDBOX_ID,
                5,
                &env,
            ),
            runtime,
            suspend_restore_storage_uri: Some(
                "s3://space-test/suspend-snapshots/current?region=us-east-1",
            ),
            suspend_restore_source_node: None,
            fork_disk_restore: None,
            direct_node_name: None,
            preferred_node_name: None,
            restore_node_shortlist: Vec::new(),
            extra_tolerations: std::slice::from_ref(&toleration),
            extra_required_affinity: Some(("space.pplx.net/rollout-cohort", "test-2")),
            node_group_placement: None,
        },
    );
    let spec = pod.spec.as_ref().unwrap();
    let node_affinity = spec
        .affinity
        .as_ref()
        .and_then(|a| a.node_affinity.as_ref())
        .expect("affinity must be present");
    let required = node_affinity
        .required_during_scheduling_ignored_during_execution
        .as_ref()
        .expect("required affinity must be rendered for cohort placement");
    assert_eq!(required.node_selector_terms.len(), 1);
    let term = required
        .node_selector_terms
        .first()
        .expect("one required affinity term");
    // Cohort requirement first, then the hard placement-block exclusion,
    // ANDed in the single required term.
    let exprs = term
        .match_expressions
        .as_ref()
        .expect("required affinity expressions");
    assert_eq!(exprs.len(), 2);
    assert_eq!(exprs[0].key, "space.pplx.net/rollout-cohort");
    assert_eq!(exprs[0].operator, "In");
    assert_eq!(
        exprs[0].values.as_deref(),
        Some(["test-2".to_string()].as_slice())
    );
    assert_eq!(exprs[1].key, SSD_PLACEMENT_BLOCKED_LABEL);
    assert_eq!(exprs[1].operator, "NotIn");
    assert_eq!(
        exprs[1].values.as_deref(),
        Some([SSD_PLACEMENT_BLOCKED_VALUE.to_string()].as_slice())
    );
    // Source-node pin must NOT be added for evacuation restores.
    assert!(term.match_fields.is_none());
}

#[test]
fn template_build_label_key_is_kubernetes_safe() {
    let key = template_build_presence_label_key("app-runtime:some/release", "sha256:abc");
    let (_, name) = key.split_once('/').unwrap();
    assert!(key.starts_with("space.pplx.net/template-build-app-runtime-some-"));
    assert!(name.len() <= 63);
    assert!(is_template_presence_metadata_key(&key));
}

#[test]
fn template_build_label_key_includes_hash_dimension() {
    let first = template_build_presence_label_key("seed", "sha256:abc");
    let second = template_build_presence_label_key("seed", "sha256:def");
    assert_ne!(first, second);
}

#[test]
fn template_write_annotation_key_is_stable() {
    let key = template_write_annotation_key();

    assert_eq!(key, "space.pplx.net/template-write");
    assert!(is_template_write_annotation_key(key));
}

#[test]
fn template_write_annotation_key_for_reference_is_stable() {
    let reference = TemplateReference::from_str(
        "seed@sha256:862261dbcc1df1b313ac950b8b994ce4a1f800e7914ba63b358d52c38b441fde",
    )
    .unwrap();
    let key = template_write_annotation_key_for(&reference);

    assert!(key.starts_with("space.pplx.net/template-write-"));
    assert!(is_template_write_annotation_key(&key));
    assert_ne!(key, template_write_annotation_key());
}

#[test]
fn template_write_annotation_value_can_request_latest() {
    assert_eq!(
        parse_template_write_annotation(template_write_annotation_key(), "app-runtime:latest"),
        Some(TemplateReference::from_str("app-runtime:latest").unwrap())
    );
}

#[test]
fn template_write_annotation_value_can_request_a_specific_tag() {
    assert_eq!(
        parse_template_write_annotation(
            template_write_annotation_key(),
            "code-runtime:release-2026-05-03",
        ),
        Some(TemplateReference::from_str("code-runtime:release-2026-05-03").unwrap())
    );
}

#[test]
fn template_write_annotation_can_request_exact_artifact_digest() {
    assert_eq!(
        parse_template_write_annotation(
            template_write_annotation_key(),
            "seed@sha256:862261dbcc1df1b313ac950b8b994ce4a1f800e7914ba63b358d52c38b441fde",
        ),
        Some(
            TemplateReference::from_str(
                "seed@sha256:862261dbcc1df1b313ac950b8b994ce4a1f800e7914ba63b358d52c38b441fde",
            )
            .unwrap(),
        )
    );
}

#[test]
fn template_write_annotation_rejects_wrong_key_or_selector_only_value() {
    assert!(
        parse_template_write_annotation("space.pplx.net/template-writes", "seed:latest").is_none()
    );
    assert!(parse_template_write_annotation(template_write_annotation_key(), "latest").is_none());
}
#[test]
fn node_group_placement_renders_boundary_affinity_and_dedicated_toleration() {
    let env = BTreeMap::new();
    let auto_lifecycle_json = test_auto_lifecycle_json();
    let network_policy_json = test_network_policy_json();
    let sandbox_owner = test_sandbox_owner();
    let placement = dedicated_node_group_placement(true);
    let pod = build_sandbox_pod(
        &test_pod_config(),
        SandboxPodRequest::OnDemand {
            common: SandboxPodCommon {
                pod_name: "space-sbx-node-group-g1",
                sandbox_id: TEST_SANDBOX_ID,
                execution_generation: 1,
                template: "seed",
                content_hash: Some(TEST_CONTENT_HASH),
                env: &env,
                traceparent: None,
                activation_attempt_id: None,
            },
            runtime: placement_test_runtime(
                &sandbox_owner,
                &auto_lifecycle_json,
                &network_policy_json,
            ),
            node_group_placement: Some(&placement),
            node_pool: None,
        },
    );

    // Required boundary: one term, one In-expression over the whole
    // (sorted) candidate set.
    let terms = required_node_selector_terms(&pod);
    assert_eq!(terms.len(), 1);
    // The boundary is AND-ed with the standing SSD placement-block exclusion.
    let expressions = terms[0].match_expressions.as_ref().unwrap();
    assert_eq!(expressions.len(), 2);
    assert_eq!(expressions[0].key, SANDBOX_NODE_GROUP_LABEL);
    assert_eq!(expressions[0].operator, "In");
    assert_eq!(
        expressions[0].values,
        Some(vec!["acme-dedicated".to_string(), "workers".to_string()])
    );
    assert_eq!(expressions[1].key, SSD_PLACEMENT_BLOCKED_LABEL);
    assert_eq!(expressions[1].operator, "NotIn");

    // Preferred bias toward the dedicated subset.
    let preferred = preferred_scheduling_terms(&pod);
    let dedicated_terms: Vec<_> = preferred
        .iter()
        .filter(|term| {
            term.preference
                .match_expressions
                .as_ref()
                .is_some_and(|exprs| {
                    exprs
                        .iter()
                        .any(|expr| expr.key == SANDBOX_NODE_GROUP_LABEL)
                })
        })
        .collect();
    assert_eq!(dedicated_terms.len(), 1);
    assert_eq!(
        dedicated_terms[0].weight,
        DEDICATED_NODE_GROUP_PREFERENCE_WEIGHT
    );
    assert_eq!(
        dedicated_terms[0]
            .preference
            .match_expressions
            .as_ref()
            .unwrap()[0]
            .values,
        Some(vec!["acme-dedicated".to_string()])
    );

    // Toleration for the owning org's dedicated taint.
    let tolerations = pod.spec.as_ref().unwrap().tolerations.as_ref().unwrap();
    assert_eq!(tolerations.len(), 1);
    assert_eq!(
        tolerations[0].key.as_deref(),
        Some(DEDICATED_NODE_TAINT_KEY)
    );
    assert_eq!(tolerations[0].operator.as_deref(), Some("Equal"));
    assert_eq!(tolerations[0].value, Some(TEST_ORG_ID.to_string()));
    assert_eq!(tolerations[0].effect.as_deref(), Some("NoSchedule"));
}

#[test]
fn shared_only_node_group_placement_renders_no_dedicated_toleration() {
    let env = BTreeMap::new();
    let auto_lifecycle_json = test_auto_lifecycle_json();
    let network_policy_json = test_network_policy_json();
    let sandbox_owner = test_sandbox_owner();
    let placement = shared_node_group_placement();
    let pod = build_sandbox_pod(
        &test_pod_config(),
        SandboxPodRequest::OnDemand {
            common: SandboxPodCommon {
                pod_name: "space-sbx-node-group-g2",
                sandbox_id: TEST_SANDBOX_ID,
                execution_generation: 2,
                template: "seed",
                content_hash: Some(TEST_CONTENT_HASH),
                env: &env,
                traceparent: None,
                activation_attempt_id: None,
            },
            runtime: placement_test_runtime(
                &sandbox_owner,
                &auto_lifecycle_json,
                &network_policy_json,
            ),
            node_group_placement: Some(&placement),
            node_pool: None,
        },
    );

    let terms = required_node_selector_terms(&pod);
    assert_eq!(terms.len(), 1);
    let expressions = terms[0].match_expressions.as_ref().unwrap();
    assert_eq!(expressions[0].key, SANDBOX_NODE_GROUP_LABEL);
    assert_eq!(expressions[0].values, Some(vec!["workers".to_string()]));
    // No dedicated target: the pod must not tolerate any dedicated taint
    // and must not carry a dedicated-group preference.
    assert!(pod.spec.as_ref().unwrap().tolerations.is_none());
    assert!(preferred_scheduling_terms(&pod).iter().all(|term| {
        term.preference
            .match_expressions
            .as_ref()
            .is_none_or(|exprs| {
                exprs
                    .iter()
                    .all(|expr| expr.key != SANDBOX_NODE_GROUP_LABEL)
            })
    }));
}

#[test]
fn node_group_placement_and_drain_cohort_are_anded_in_one_term() {
    let env = BTreeMap::new();
    let auto_lifecycle_json = test_auto_lifecycle_json();
    let network_policy_json = test_network_policy_json();
    let sandbox_owner = test_sandbox_owner();
    let cohort_toleration = PodToleration {
        key: Some("space.pplx.net/rollout-cohort".to_string()),
        operator: Some("Equal".to_string()),
        value: Some("test-2".to_string()),
        effect: Some("NoSchedule".to_string()),
        toleration_seconds: None,
    };
    let placement = dedicated_node_group_placement(true);
    let pod = build_sandbox_pod(
        &test_pod_config(),
        SandboxPodRequest::Restore {
            artifact_kind: crate::types::ArtifactKind::Full,
            common: SandboxPodCommon {
                pod_name: "space-sbx-node-group-g3",
                sandbox_id: TEST_SANDBOX_ID,
                execution_generation: 3,
                template: "seed",
                content_hash: Some(TEST_CONTENT_HASH),
                env: &env,
                traceparent: None,
                activation_attempt_id: None,
            },
            runtime: placement_test_runtime(
                &sandbox_owner,
                &auto_lifecycle_json,
                &network_policy_json,
            ),
            node_group_placement: Some(&placement),
            suspend_restore_storage_uri: Some(
                "s3://space-test/suspend-snapshots/current?region=us-east-1",
            ),
            suspend_restore_source_node: None,
            direct_node_name: None,
            preferred_node_name: None,
            extra_tolerations: std::slice::from_ref(&cohort_toleration),
            extra_required_affinity: Some(("space.pplx.net/rollout-cohort", "test-2")),
            fork_disk_restore: None,
            restore_node_shortlist: Vec::new(),
        },
    );

    // Both constraints must land in a single node-selector term: terms
    // are OR-ed by kube-scheduler, so two separate terms would let the
    // pod escape the node-group boundary via the cohort term (or vice
    // versa).
    let terms = required_node_selector_terms(&pod);
    assert_eq!(terms.len(), 1);
    let expressions = terms[0].match_expressions.as_ref().unwrap();
    assert_eq!(expressions.len(), 3);
    assert_eq!(expressions[0].key, "space.pplx.net/rollout-cohort");
    assert_eq!(expressions[1].key, SANDBOX_NODE_GROUP_LABEL);
    assert_eq!(expressions[2].key, SSD_PLACEMENT_BLOCKED_LABEL);

    // Cohort toleration and dedicated toleration both render.
    let tolerations = pod.spec.as_ref().unwrap().tolerations.as_ref().unwrap();
    assert_eq!(tolerations.len(), 2);
    assert_eq!(
        tolerations[0].key.as_deref(),
        Some("space.pplx.net/rollout-cohort")
    );
    assert_eq!(
        tolerations[1].key.as_deref(),
        Some(DEDICATED_NODE_TAINT_KEY)
    );
}

#[test]
fn pooled_pod_renders_node_group_placement() {
    let env = BTreeMap::new();
    let placement = shared_node_group_placement();
    let pod = build_sandbox_pod(
        &test_pod_config(),
        SandboxPodRequest::Pooled {
            common: SandboxPodCommon {
                pod_name: "space-sbx-pool-node-group-g1",
                sandbox_id: TEST_POOL_SANDBOX_ID,
                execution_generation: 1,
                template: "seed",
                content_hash: Some(TEST_CONTENT_HASH),
                env: &env,
                traceparent: None,
                activation_attempt_id: None,
            },
            pool: SandboxPoolPodMetadata {
                slot_id: TEST_POOL_SLOT_ID,
                config_id: TEST_POOL_CONFIG_ID,
                vcpu: VcpuCount::try_from_u32(2).unwrap(),
                memory: MemorySize::mib(8192).unwrap(),
                disk_mib: None,
                disk_io_limits: Default::default(),
            },
            node_group_placement: Some(&placement),
        },
    );

    let terms = required_node_selector_terms(&pod);
    assert_eq!(terms.len(), 1);
    let expressions = terms[0].match_expressions.as_ref().unwrap();
    assert_eq!(expressions[0].key, SANDBOX_NODE_GROUP_LABEL);
    assert_eq!(expressions[0].values, Some(vec!["workers".to_string()]));
    // Warm pods are generic-only, so no dedicated taint toleration.
    assert!(pod.spec.as_ref().unwrap().tolerations.is_none());
}

fn placement_test_runtime<'a>(
    sandbox_owner: &'a SandboxOwner,
    auto_lifecycle_json: &'a str,
    network_policy_json: &'a str,
) -> SandboxRuntimePodMetadata<'a> {
    SandboxRuntimePodMetadata {
        sandbox_owner,
        user_id: TEST_USER_ID,
        org_id: TEST_ORG_ID,
        auto_lifecycle_json,
        auto_lifecycle_authority: PolicyAuthority::Cluster,
        network_policy_json,
        network_policy_generation: 1,
        network_policy_authority: PolicyAuthority::Cluster,
        vcpu: None,
        memory_mib: None,
        disk_mib: None,
        disk_io_limits: Default::default(),
        volume_encryption: false,
    }
}

#[test]
fn local_only_restore_keeps_direct_node_assignment_under_placement() {
    // A local-only restore has no portable snapshot: its memory and disk live
    // on the source node only. Converting the `spec.nodeName` assignment into
    // a scheduler-filtered pin would subject the resume to cordons and
    // unrelated `NoSchedule` taints and strand a sandbox that has nowhere else
    // to go. So the assignment is preserved. The boundary is still rendered and
    // is *not* dormant: kubelet evaluates required nodeAffinity on admission,
    // so a source node outside the candidate set rejects this pod rather than
    // resuming it — and with no scheduler in the loop there is no other node to
    // fall to, making that terminal. Moving such a sandbox therefore takes a
    // portable snapshot and a re-place, which is a migration, not something the
    // renderer can paper over by relaxing the boundary.
    let env = BTreeMap::new();
    let auto_lifecycle_json = test_auto_lifecycle_json();
    let network_policy_json = test_network_policy_json();
    let sandbox_owner = test_sandbox_owner();
    let placement = dedicated_node_group_placement(false);
    let pod = build_sandbox_pod(
        &test_pod_config(),
        SandboxPodRequest::Restore {
            artifact_kind: crate::types::ArtifactKind::Full,
            common: SandboxPodCommon {
                pod_name: "space-sbx-node-group-g4",
                sandbox_id: TEST_SANDBOX_ID,
                execution_generation: 4,
                activation_attempt_id: None,
                template: "seed",
                content_hash: Some(TEST_CONTENT_HASH),
                env: &env,
                traceparent: None,
            },
            runtime: placement_test_runtime(
                &sandbox_owner,
                &auto_lifecycle_json,
                &network_policy_json,
            ),
            node_group_placement: Some(&placement),
            suspend_restore_storage_uri: None,
            suspend_restore_source_node: Some("ip-10-0-0-1.ec2.internal"),
            fork_disk_restore: None,
            restore_node_shortlist: Vec::new(),
            direct_node_name: Some("ip-10-0-0-1.ec2.internal"),
            preferred_node_name: None,
            extra_tolerations: &[],
            extra_required_affinity: None,
        },
    );

    let spec = pod.spec.as_ref().unwrap();
    assert_eq!(
        spec.node_name.as_deref(),
        Some("ip-10-0-0-1.ec2.internal"),
        "a local-only restore must keep its direct source-node assignment"
    );
    // No required source-node matchFields term is synthesized from the direct
    // assignment: the pod is pre-bound, so the scheduler never evaluates it.
    let terms = required_node_selector_terms(&pod);
    assert!(
        terms.iter().all(|term| term.match_fields.is_none()),
        "a pre-bound pod must not also carry a scheduler source pin"
    );
    // The boundary IS rendered. kube-scheduler is skipped, but kubelet's
    // admission re-evaluates required node affinity against the node it is
    // bound to and rejects on a mismatch — the same path this pod's worker
    // `nodeSelector` relies on. Dropping the term here would discard the only
    // enforcement a pre-bound pod still has.
    assert!(
        terms.iter().any(|term| term
            .match_expressions
            .as_ref()
            .is_some_and(|exprs| exprs.iter().any(|e| e.key == SANDBOX_NODE_GROUP_LABEL))),
        "a pre-bound pod must still carry the boundary kubelet admission checks"
    );
}

#[test]
fn dedicated_org_mismatch_renders_an_unsatisfiable_boundary() {
    // Withholding the toleration is not enough: the candidate set still names
    // the other org's groups, and the label and the taint are independent
    // control-plane writes, so a labeled-but-not-yet-tainted node would admit
    // this pod. `In (C)` AND `NotIn (C)` in one term is satisfiable by no node,
    // so the pod stays Unschedulable instead.
    let env = BTreeMap::new();
    let auto_lifecycle_json = test_auto_lifecycle_json();
    let network_policy_json = test_network_policy_json();
    let sandbox_owner = test_sandbox_owner();
    let placement = NodeGroupPlacement::for_caller(
        Some(OrgId::from_uuid(Uuid::from_u128(0x999))),
        &placement_template(),
        &[
            shared_group("workers"),
            dedicated_group("acme-dedicated", OrgId::from_uuid(Uuid::from_u128(0x999))),
        ],
    )
    .unwrap();
    let pod = build_sandbox_pod(
        &test_pod_config(),
        SandboxPodRequest::OnDemand {
            common: SandboxPodCommon {
                pod_name: "space-sbx-node-group-g12",
                sandbox_id: TEST_SANDBOX_ID,
                execution_generation: 12,
                activation_attempt_id: None,
                template: "seed",
                content_hash: Some(TEST_CONTENT_HASH),
                env: &env,
                traceparent: None,
            },
            runtime: placement_test_runtime(
                &sandbox_owner,
                &auto_lifecycle_json,
                &network_policy_json,
            ),
            node_pool: None,
            node_group_placement: Some(&placement),
        },
    );

    let terms = required_node_selector_terms(&pod);
    assert_eq!(terms.len(), 1);
    let boundary: Vec<_> = terms[0]
        .match_expressions
        .as_ref()
        .unwrap()
        .iter()
        .filter(|expr| expr.key == SANDBOX_NODE_GROUP_LABEL)
        .collect();
    assert_eq!(boundary.len(), 2, "the boundary must contradict itself");
    let operators: Vec<&str> = boundary.iter().map(|e| e.operator.as_str()).collect();
    assert!(operators.contains(&"In") && operators.contains(&"NotIn"));
    assert_eq!(boundary[0].values, boundary[1].values);
    assert!(
        pod.spec.as_ref().unwrap().tolerations.is_none(),
        "a mismatched org must not be tolerated"
    );
}

#[test]
fn placement_boundary_survives_with_every_soft_bias_disabled() {
    // The boundary must not depend on the ssd or readiness biases: with both
    // off, the early return in `sandbox_node_affinity` is the only thing that
    // could drop it.
    let env = BTreeMap::new();
    let auto_lifecycle_json = test_auto_lifecycle_json();
    let network_policy_json = test_network_policy_json();
    let sandbox_owner = test_sandbox_owner();
    let placement = shared_node_group_placement();
    let config = SandboxPodConfig {
        ssd_headroom_tier_affinity: false,
        node_services_ready_affinity: false,
        ..test_pod_config()
    };
    let pod = build_sandbox_pod(
        &config,
        SandboxPodRequest::OnDemand {
            common: SandboxPodCommon {
                pod_name: "space-sbx-node-group-g13",
                sandbox_id: TEST_SANDBOX_ID,
                execution_generation: 13,
                activation_attempt_id: None,
                template: "seed",
                content_hash: None,
                env: &env,
                traceparent: None,
            },
            runtime: placement_test_runtime(
                &sandbox_owner,
                &auto_lifecycle_json,
                &network_policy_json,
            ),
            node_pool: None,
            node_group_placement: Some(&placement),
        },
    );

    let terms = required_node_selector_terms(&pod);
    assert_eq!(terms.len(), 1);
    let expressions = terms[0].match_expressions.as_ref().unwrap();
    assert_eq!(
        expressions.len(),
        1,
        "only the boundary should remain with both biases off"
    );
    assert_eq!(expressions[0].key, SANDBOX_NODE_GROUP_LABEL);
}

#[test]
fn soft_pinned_restore_suppresses_the_dedicated_preference() {
    // The default restore path (`restore_source_node_hard_pin` off) renders a
    // weight-100 preferred source term and no required pin. An unconditional
    // weight-100 dedicated term would tie it and hand the choice to the
    // resource plugins — an off-source restore that pulls from S3 for nothing.
    let env = BTreeMap::new();
    let auto_lifecycle_json = test_auto_lifecycle_json();
    let network_policy_json = test_network_policy_json();
    let sandbox_owner = test_sandbox_owner();
    let placement = dedicated_node_group_placement(true);
    let pod = build_sandbox_pod(
        &test_pod_config(),
        SandboxPodRequest::Restore {
            artifact_kind: crate::types::ArtifactKind::Full,
            common: SandboxPodCommon {
                pod_name: "space-sbx-node-group-g14",
                sandbox_id: TEST_SANDBOX_ID,
                execution_generation: 14,
                activation_attempt_id: None,
                template: "seed",
                content_hash: Some(TEST_CONTENT_HASH),
                env: &env,
                traceparent: None,
            },
            runtime: placement_test_runtime(
                &sandbox_owner,
                &auto_lifecycle_json,
                &network_policy_json,
            ),
            node_group_placement: Some(&placement),
            suspend_restore_storage_uri: Some(
                "s3://space-test/suspend-snapshots/current?region=us-east-1",
            ),
            suspend_restore_source_node: Some("space-worker-a"),
            fork_disk_restore: None,
            restore_node_shortlist: Vec::new(),
            direct_node_name: None,
            preferred_node_name: Some("space-worker-a"),
            extra_tolerations: &[],
            extra_required_affinity: None,
        },
    );

    assert!(
        dedicated_preferred_terms(&pod).is_empty(),
        "a soft source preference must also suppress the dedicated preference"
    );
    let tolerations = pod.spec.as_ref().unwrap().tolerations.as_ref().unwrap();
    assert_eq!(
        tolerations[0].key.as_deref(),
        Some(DEDICATED_NODE_TAINT_KEY)
    );
}

fn dedicated_preferred_terms(pod: &Pod) -> Vec<PreferredSchedulingTerm> {
    preferred_scheduling_terms(pod)
        .into_iter()
        .filter(|term| {
            term.preference
                .match_expressions
                .as_ref()
                .is_some_and(|exprs| {
                    exprs
                        .iter()
                        .any(|expr| expr.key == SANDBOX_NODE_GROUP_LABEL)
                })
        })
        .collect()
}

#[test]
fn pooled_pod_never_renders_dedicated_toleration_or_preference() {
    // A pooled pod carries no org identity, so a placement that names a
    // dedicated target cannot belong to it: a generic warm pod that landed on
    // dedicated hardware could later be claimed by an unrelated org. That is
    // an org mismatch like any other, so it takes the same fail-closed path —
    // the boundary contradicts itself and the slot pod schedules **nowhere**,
    // not onto the shared candidates. A warm-pool reconciler handed a
    // dedicated placement therefore mints pods that never bind, which is the
    // loud outcome; the quiet one would be a warm pod sitting on reserved
    // hardware.
    let env = BTreeMap::new();
    let placement = dedicated_node_group_placement(true);
    let pod = build_sandbox_pod(
        &test_pod_config(),
        SandboxPodRequest::Pooled {
            common: SandboxPodCommon {
                pod_name: "space-sbx-pool-node-group-g2",
                sandbox_id: TEST_POOL_SANDBOX_ID,
                execution_generation: 1,
                template: "seed",
                content_hash: Some(TEST_CONTENT_HASH),
                env: &env,
                traceparent: None,
                activation_attempt_id: None,
            },
            pool: SandboxPoolPodMetadata {
                slot_id: TEST_POOL_SLOT_ID,
                config_id: TEST_POOL_CONFIG_ID,
                vcpu: VcpuCount::try_from_u32(2).unwrap(),
                memory: MemorySize::mib(8192).unwrap(),
                disk_mib: None,
                disk_io_limits: Default::default(),
            },
            node_group_placement: Some(&placement),
        },
    );

    let terms = required_node_selector_terms(&pod);
    assert_eq!(terms.len(), 1);
    let boundary: Vec<_> = terms[0]
        .match_expressions
        .as_ref()
        .unwrap()
        .iter()
        .filter(|expr| expr.key == SANDBOX_NODE_GROUP_LABEL)
        .collect();
    assert_eq!(
        boundary.len(),
        2,
        "an org-less pod must get the self-contradicting boundary, not a \
         satisfiable one over the shared candidates"
    );
    let operators: Vec<&str> = boundary.iter().map(|e| e.operator.as_str()).collect();
    assert!(operators.contains(&"In") && operators.contains(&"NotIn"));
    assert!(pod.spec.as_ref().unwrap().tolerations.is_none());
    assert!(dedicated_preferred_terms(&pod).is_empty());
}

#[test]
fn dedicated_rendering_requires_matching_runtime_org() {
    // A placement resolved for the wrong actor must not mint a toleration for
    // that actor's dedicated taint: the pod's trusted org identity is
    // `SandboxRuntimePodMetadata::org_id`. What keeps the pod off the other
    // org's nodes is not the taint — a `NoSchedule` taint is scheduler-only and
    // an untolerated one still lets a pre-bound pod through — but the
    // deliberately unsatisfiable boundary the mismatch renders.
    let env = BTreeMap::new();
    let auto_lifecycle_json = test_auto_lifecycle_json();
    let network_policy_json = test_network_policy_json();
    let sandbox_owner = test_sandbox_owner();
    let placement = NodeGroupPlacement::for_caller(
        Some(OrgId::from_uuid(Uuid::from_u128(0x999))),
        &placement_template(),
        &[
            shared_group("workers"),
            dedicated_group("acme-dedicated", OrgId::from_uuid(Uuid::from_u128(0x999))),
        ],
    )
    .unwrap();
    let pod = build_sandbox_pod(
        &test_pod_config(),
        SandboxPodRequest::OnDemand {
            common: SandboxPodCommon {
                pod_name: "space-sbx-node-group-g5",
                sandbox_id: TEST_SANDBOX_ID,
                execution_generation: 5,
                template: "seed",
                content_hash: Some(TEST_CONTENT_HASH),
                env: &env,
                traceparent: None,
                activation_attempt_id: None,
            },
            runtime: placement_test_runtime(
                &sandbox_owner,
                &auto_lifecycle_json,
                &network_policy_json,
            ),
            node_group_placement: Some(&placement),
            node_pool: None,
        },
    );

    let terms = required_node_selector_terms(&pod);
    let boundary: Vec<_> = terms[0]
        .match_expressions
        .as_ref()
        .unwrap()
        .iter()
        .filter(|expr| expr.key == SANDBOX_NODE_GROUP_LABEL)
        .collect();
    assert_eq!(
        boundary.len(),
        2,
        "a mismatched org must render the self-contradicting boundary"
    );
    let operators: Vec<&str> = boundary.iter().map(|e| e.operator.as_str()).collect();
    assert!(operators.contains(&"In") && operators.contains(&"NotIn"));
    assert!(
        pod.spec.as_ref().unwrap().tolerations.is_none(),
        "a dedicated org that differs from the pod's runtime org must not be tolerated"
    );
    assert!(dedicated_preferred_terms(&pod).is_empty());
}

#[test]
fn restore_shortlist_suppresses_dedicated_preference_but_keeps_toleration() {
    // On a source-aware shortlist the source's weight-100 preference must
    // be the only NodeAffinity score: an unconditional weight-100
    // dedicated term would let a dedicated fallback tie a feasible
    // source and force an off-source restore. The toleration stays — the
    // restore must still be able to land on a dedicated source.
    let env = BTreeMap::new();
    let auto_lifecycle_json = test_auto_lifecycle_json();
    let network_policy_json = test_network_policy_json();
    let sandbox_owner = test_sandbox_owner();
    let placement = dedicated_node_group_placement(true);
    let config = SandboxPodConfig {
        restore_source_node_hard_pin: true,
        ..test_pod_config()
    };
    let pod = build_sandbox_pod(
        &config,
        SandboxPodRequest::Restore {
            artifact_kind: crate::types::ArtifactKind::Full,
            common: SandboxPodCommon {
                pod_name: "space-sbx-node-group-g6",
                sandbox_id: TEST_SANDBOX_ID,
                execution_generation: 6,
                template: "seed",
                content_hash: Some(TEST_CONTENT_HASH),
                env: &env,
                traceparent: None,
                activation_attempt_id: None,
            },
            runtime: placement_test_runtime(
                &sandbox_owner,
                &auto_lifecycle_json,
                &network_policy_json,
            ),
            node_group_placement: Some(&placement),
            suspend_restore_storage_uri: Some(
                "s3://space-test/suspend-snapshots/current?region=us-east-1",
            ),
            suspend_restore_source_node: Some("node-a"),
            direct_node_name: None,
            preferred_node_name: Some("node-a"),
            restore_node_shortlist: vec!["node-a".to_string(), "node-b".to_string()],
            extra_tolerations: &[],
            extra_required_affinity: None,
            fork_disk_restore: None,
        },
    );

    let tolerations = pod.spec.as_ref().unwrap().tolerations.as_ref().unwrap();
    assert_eq!(
        tolerations[0].key.as_deref(),
        Some(DEDICATED_NODE_TAINT_KEY),
        "the dedicated toleration must survive a shortlist restore"
    );
    assert!(
        dedicated_preferred_terms(&pod).is_empty(),
        "the dedicated preference must not compete with the source preference"
    );

    // The load-bearing invariant of the whole change: node-selector terms are
    // ORed, so a shortlist that repeats the source pin per term must repeat the
    // node-group boundary in *every* one of them. A term that carries only
    // `matchFields: metadata.name` is a hole — the scheduler would satisfy it on
    // a shared fallback while the pod carries the dedicated toleration — so the
    // boundary would stop constraining placement at all. (This fixture uses a
    // generic template, whose set legally includes `workers`; the same hole on
    // an org-specific template's set is what puts a private rootfs on shared
    // hardware.)
    let terms = required_node_selector_terms(&pod);
    assert_eq!(terms.len(), 2, "one ORed term per shortlisted node");
    for (index, term) in terms.iter().enumerate() {
        let expressions = term
            .match_expressions
            .as_ref()
            .unwrap_or_else(|| panic!("term {index} carries no match expressions"));
        let boundary = expressions
            .iter()
            .find(|expr| expr.key == SANDBOX_NODE_GROUP_LABEL)
            .unwrap_or_else(|| panic!("term {index} is missing the node-group boundary"));
        assert_eq!(boundary.operator, "In");
        assert_eq!(
            boundary.values,
            Some(vec!["acme-dedicated".to_string(), "workers".to_string()]),
            "term {index} must constrain the whole candidate set"
        );
        assert!(
            term.match_fields.is_some(),
            "term {index} must still pin one shortlisted node"
        );
    }
}

#[test]
fn dedicated_preference_is_gated_on_node_services_readiness() {
    // The dedicated preference is weight-100, the same cap as the readiness
    // preference, so leaving it ungated lets a not-ready dedicated node tie a
    // ready one and hand the choice to the remaining scoring plugins — the
    // daemon wait the readiness bias exists to avoid. Gate it the way the
    // template-cache preference is gated: AND the readiness requirement into
    // the term so a rolling dedicated node scores nothing here.
    let env = BTreeMap::new();
    let auto_lifecycle_json = test_auto_lifecycle_json();
    let network_policy_json = test_network_policy_json();
    let sandbox_owner = test_sandbox_owner();
    let placement = dedicated_node_group_placement(true);
    let config = SandboxPodConfig {
        node_services_ready_affinity: true,
        ..test_pod_config()
    };
    let pod = build_sandbox_pod(
        &config,
        SandboxPodRequest::OnDemand {
            common: SandboxPodCommon {
                pod_name: "space-sbx-node-group-g9",
                sandbox_id: TEST_SANDBOX_ID,
                execution_generation: 9,
                activation_attempt_id: None,
                template: "seed",
                content_hash: Some(TEST_CONTENT_HASH),
                env: &env,
                traceparent: None,
            },
            runtime: placement_test_runtime(
                &sandbox_owner,
                &auto_lifecycle_json,
                &network_policy_json,
            ),
            node_pool: None,
            node_group_placement: Some(&placement),
        },
    );

    let dedicated = dedicated_preferred_terms(&pod);
    assert_eq!(dedicated.len(), 1);
    let expressions = dedicated[0].preference.match_expressions.as_ref().unwrap();
    assert_eq!(
        expressions.len(),
        2,
        "the dedicated preference must AND the readiness requirement when the bias is on"
    );
    assert_eq!(expressions[0].key, SANDBOX_NODE_GROUP_LABEL);
    // Spelled out rather than compared against `node_services_ready_requirement()`:
    // asserting a rendered value against the helper that rendered it passes even
    // if the helper inverts the sense, which is the one mistake worth catching
    // here — `NotIn ["false"]` and `In ["false"]` differ by every node.
    assert_eq!(expressions[1].key, NODE_SERVICES_READY_LABEL);
    assert_eq!(expressions[1].operator, "NotIn");
    assert_eq!(
        expressions[1].values.as_deref(),
        Some([NODE_SERVICES_NOT_READY_VALUE.to_string()].as_slice())
    );
}

#[test]
fn dedicated_preference_is_ungated_when_readiness_bias_is_off() {
    // With the bias off there is no readiness preference to tie, so the
    // dedicated term stays a plain node-group match.
    let env = BTreeMap::new();
    let auto_lifecycle_json = test_auto_lifecycle_json();
    let network_policy_json = test_network_policy_json();
    let sandbox_owner = test_sandbox_owner();
    let placement = dedicated_node_group_placement(true);
    let pod = build_sandbox_pod(
        &test_pod_config(),
        SandboxPodRequest::OnDemand {
            common: SandboxPodCommon {
                pod_name: "space-sbx-node-group-g10",
                sandbox_id: TEST_SANDBOX_ID,
                execution_generation: 10,
                activation_attempt_id: None,
                template: "seed",
                content_hash: Some(TEST_CONTENT_HASH),
                env: &env,
                traceparent: None,
            },
            runtime: placement_test_runtime(
                &sandbox_owner,
                &auto_lifecycle_json,
                &network_policy_json,
            ),
            node_pool: None,
            node_group_placement: Some(&placement),
        },
    );

    let dedicated = dedicated_preferred_terms(&pod);
    assert_eq!(dedicated.len(), 1);
    let expressions = dedicated[0].preference.match_expressions.as_ref().unwrap();
    assert_eq!(expressions.len(), 1);
    assert_eq!(expressions[0].key, SANDBOX_NODE_GROUP_LABEL);
}

#[test]
fn all_shared_placement_for_another_org_fails_closed() {
    // The case a dedicated-only org check misses entirely. "Shared" means
    // shared among the orgs a caller was cleared to include, so a candidate set
    // computed for the triggering actor's org rather than the sandbox's owning
    // org — the confusion `NodeGroup::compatible` warns about on restore and
    // drain-migrate paths — must not render a satisfiable boundary just because
    // it happens to contain no dedicated groups.
    let env = BTreeMap::new();
    let auto_lifecycle_json = test_auto_lifecycle_json();
    let network_policy_json = test_network_policy_json();
    let sandbox_owner = test_sandbox_owner();
    let other_org = OrgId::from_uuid(Uuid::from_u128(0x999));
    let placement = NodeGroupPlacement::for_caller(
        Some(other_org),
        &placement_template(),
        &[shared_group("other-orgs-shared")],
    )
    .unwrap();
    let pod = build_sandbox_pod(
        &test_pod_config(),
        SandboxPodRequest::OnDemand {
            common: SandboxPodCommon {
                pod_name: "space-sbx-node-group-g17",
                sandbox_id: TEST_SANDBOX_ID,
                execution_generation: 17,
                activation_attempt_id: None,
                template: "seed",
                content_hash: Some(TEST_CONTENT_HASH),
                env: &env,
                traceparent: None,
            },
            runtime: placement_test_runtime(
                &sandbox_owner,
                &auto_lifecycle_json,
                &network_policy_json,
            ),
            node_pool: None,
            node_group_placement: Some(&placement),
        },
    );

    let terms = required_node_selector_terms(&pod);
    let boundary: Vec<_> = terms[0]
        .match_expressions
        .as_ref()
        .unwrap()
        .iter()
        .filter(|expr| expr.key == SANDBOX_NODE_GROUP_LABEL)
        .collect();
    assert_eq!(
        boundary.len(),
        2,
        "an all-shared placement for another org must still fail closed"
    );
    let operators: Vec<&str> = boundary.iter().map(|e| e.operator.as_str()).collect();
    assert!(operators.contains(&"In") && operators.contains(&"NotIn"));
    assert!(pod.spec.as_ref().unwrap().tolerations.is_none());
}

#[test]
fn org_less_placement_on_an_org_owned_pod_fails_closed() {
    // The other direction of the org cross-check, and the one no production
    // path produces today: an org-less candidate set — the shape the warm pool
    // resolves, generic groups only — rendered onto a pod that *does* carry an
    // org.
    //
    // The property: a set resolved without an org identity was not resolved for
    // this sandbox, so it must not render a satisfiable boundary even though it
    // is narrower than what that org would have got. Narrower is not the same
    // as "computed for the right question", and the boundary is not a place to
    // render an answer to a different one.
    let env = BTreeMap::new();
    let auto_lifecycle_json = test_auto_lifecycle_json();
    let network_policy_json = test_network_policy_json();
    let sandbox_owner = test_sandbox_owner();
    let placement =
        NodeGroupPlacement::for_caller(None, &placement_template(), &[shared_group("workers")])
            .expect("a generic group admits a generic template for an org-less caller");
    assert_eq!(placement.caller_org(), None);
    // No dedicated subset is even representable for an org-less caller, so the
    // toleration assertion at the end is about the *boundary* failing closed —
    // not about the toleration, which could not have been emitted either way.
    assert!(!placement.targets_dedicated_capacity());
    let pod = build_sandbox_pod(
        &test_pod_config(),
        SandboxPodRequest::OnDemand {
            common: SandboxPodCommon {
                pod_name: "space-sbx-node-group-g18",
                sandbox_id: TEST_SANDBOX_ID,
                execution_generation: 18,
                activation_attempt_id: None,
                template: "seed",
                content_hash: Some(TEST_CONTENT_HASH),
                env: &env,
                traceparent: None,
            },
            // This runtime carries an org, which is what makes the pair a
            // mismatch — a pooled pod (the only org-less pod there is) renders
            // no runtime block at all.
            runtime: placement_test_runtime(
                &sandbox_owner,
                &auto_lifecycle_json,
                &network_policy_json,
            ),
            node_pool: None,
            node_group_placement: Some(&placement),
        },
    );

    let terms = required_node_selector_terms(&pod);
    let boundary: Vec<_> = terms[0]
        .match_expressions
        .as_ref()
        .unwrap()
        .iter()
        .filter(|expr| expr.key == SANDBOX_NODE_GROUP_LABEL)
        .collect();
    assert_eq!(
        boundary.len(),
        2,
        "an org-less placement on an org-owned pod must fail closed"
    );
    let operators: Vec<&str> = boundary.iter().map(|e| e.operator.as_str()).collect();
    assert!(operators.contains(&"In") && operators.contains(&"NotIn"));
    assert!(pod.spec.as_ref().unwrap().tolerations.is_none());
}

#[test]
fn placement_for_a_different_template_fails_closed() {
    // A candidate set is derived from an (org, template) pair. A generic
    // template's set legally includes shared groups; reusing it for a pod whose
    // template is org-specific would put that rootfs on shared hardware, so the
    // template is cross-checked exactly like the org.
    let env = BTreeMap::new();
    let auto_lifecycle_json = test_auto_lifecycle_json();
    let network_policy_json = test_network_policy_json();
    let sandbox_owner = test_sandbox_owner();
    let placement = shared_node_group_placement();
    assert_eq!(placement.template().as_str(), "seed");
    let pod = build_sandbox_pod(
        &test_pod_config(),
        SandboxPodRequest::OnDemand {
            common: SandboxPodCommon {
                pod_name: "space-sbx-node-group-g18",
                sandbox_id: TEST_SANDBOX_ID,
                execution_generation: 18,
                activation_attempt_id: None,
                // Not the template the placement was resolved for.
                template: "acme-secret",
                content_hash: Some(TEST_CONTENT_HASH),
                env: &env,
                traceparent: None,
            },
            runtime: placement_test_runtime(
                &sandbox_owner,
                &auto_lifecycle_json,
                &network_policy_json,
            ),
            node_pool: None,
            node_group_placement: Some(&placement),
        },
    );

    let terms = required_node_selector_terms(&pod);
    let boundary: Vec<_> = terms[0]
        .match_expressions
        .as_ref()
        .unwrap()
        .iter()
        .filter(|expr| expr.key == SANDBOX_NODE_GROUP_LABEL)
        .collect();
    assert_eq!(
        boundary.len(),
        2,
        "a placement resolved for another template must fail closed"
    );
    let operators: Vec<&str> = boundary.iter().map(|e| e.operator.as_str()).collect();
    assert!(operators.contains(&"In") && operators.contains(&"NotIn"));
}

#[test]
fn an_empty_preferred_node_name_does_not_drop_the_required_shortlist() {
    // The dedicated-preference gate needs to know whether a *usable* source
    // preference exists, but that emptiness check must not be hoisted onto
    // `preferred_node_name` upstream: the hard-pin match reads it to decide
    // whether to emit the required shortlist at all, so filtering it there
    // would silently widen a required constraint to the whole fleet.
    //
    // The empty name is not a synthetic value with no path here: the caller
    // derives it from the source node's stored address via `Option::flatten`,
    // which drops `None` and passes `Some("")` straight through, and the field
    // is a plain `&str` rather than a validated node-name type.
    let env = BTreeMap::new();
    let auto_lifecycle_json = test_auto_lifecycle_json();
    let network_policy_json = test_network_policy_json();
    let sandbox_owner = test_sandbox_owner();
    let placement = dedicated_node_group_placement(true);
    let config = SandboxPodConfig {
        restore_source_node_hard_pin: true,
        ..test_pod_config()
    };
    let pod = build_sandbox_pod(
        &config,
        SandboxPodRequest::Restore {
            artifact_kind: crate::types::ArtifactKind::Full,
            common: SandboxPodCommon {
                pod_name: "space-sbx-node-group-g19",
                sandbox_id: TEST_SANDBOX_ID,
                execution_generation: 19,
                activation_attempt_id: None,
                template: "seed",
                content_hash: Some(TEST_CONTENT_HASH),
                env: &env,
                traceparent: None,
            },
            runtime: placement_test_runtime(
                &sandbox_owner,
                &auto_lifecycle_json,
                &network_policy_json,
            ),
            node_group_placement: Some(&placement),
            suspend_restore_storage_uri: Some(
                "s3://space-test/suspend-snapshots/current?region=us-east-1",
            ),
            suspend_restore_source_node: Some(""),
            fork_disk_restore: None,
            restore_node_shortlist: vec![
                "space-worker-a".to_string(),
                "space-worker-b".to_string(),
            ],
            direct_node_name: None,
            preferred_node_name: Some(""),
            extra_tolerations: &[],
            extra_required_affinity: None,
        },
    );

    let terms = required_node_selector_terms(&pod);
    assert_eq!(
        terms.len(),
        2,
        "the required shortlist must survive an empty source-node name"
    );
    for term in &terms {
        assert!(
            term.match_fields.is_some(),
            "every ORed term must still pin one shortlisted node"
        );
    }
    // The dedicated preference stays suppressed here — not because the name is
    // empty, but because a required shortlist exists at all, and within one the
    // source preference owns the scoring. In this fixture the source name is
    // empty, so no source preference is rendered either and no affinity
    // preference orders the two shortlisted nodes; the required terms still
    // bound the choice to them, which is the property under test.
    assert!(dedicated_preferred_terms(&pod).is_empty());
    // The toleration survives, so a dedicated shortlisted node is reachable.
    let tolerations = pod.spec.as_ref().unwrap().tolerations.as_ref().unwrap();
    assert_eq!(
        tolerations[0].key.as_deref(),
        Some(DEDICATED_NODE_TAINT_KEY)
    );
}

#[test]
fn cold_create_env_reaches_infra_pod_contract() {
    let contract: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../../shim/testdata/cold-create-env.json"
    ))
    .unwrap();
    let mut env = BTreeMap::from([
        ("EMPTY".to_string(), "".to_string()),
        (
            "FROZEN".to_string(),
            "original café 雪 / line\nbreak".to_string(),
        ),
        ("EQUALS".to_string(), "a=b=c".to_string()),
        ("MULTILINE".to_string(), "first\nsecond".to_string()),
        ("UNICODE".to_string(), "雪 café 🚀".to_string()),
        (
            "SPACE_INTERNAL_TOKEN".to_string(),
            "must-not-leak".to_string(),
        ),
        (
            "SPACE_INTERNAL_CONTROL_TOKEN".to_string(),
            "must-not-leak".to_string(),
        ),
    ]);
    let auto = test_auto_lifecycle_json();
    let network = test_network_policy_json();
    let owner = test_sandbox_owner();
    for case in ["frozen", "maximum", "empty"] {
        if case == "empty" {
            env.clear();
        }
        let maximum_control_values = case == "maximum";
        if maximum_control_values {
            // Existing EnvMap limits, including worst-case JSON escaping.
            env = (0..256)
                .map(|i| (format!("K{i:0127}"), "\u{1}".repeat(128)))
                .collect();
            crate::types::EnvMap::try_from(env.clone()).unwrap();
        }
        let runtime = test_runtime(&owner, &auto, &network);
        let common = test_common("space-sbx-env-g1", TEST_SANDBOX_ID, 1, &env);
        let pod = build_sandbox_pod(&test_pod_config(), on_demand_request(common, runtime));
        let annotations = pod.metadata.annotations.as_ref().unwrap();
        let wire = &annotations[SANDBOX_PROCESS_ENV_ANNOTATION];
        if maximum_control_values {
            use base64::Engine as _;
            let decoded = base64::engine::general_purpose::STANDARD
                .decode(wire)
                .unwrap();
            let expected: String = env.iter().map(|(k, v)| format!("{k}={v}\0")).collect();
            assert_eq!(decoded, expected.as_bytes());
            // Kubernetes's existing total annotation cap, including the maximum
            // supported network policy, still admits this valid environment.
            let other_bytes: usize = annotations
                .iter()
                .filter(|(k, _)| k.as_str() != SANDBOX_NETWORK_POLICY_ANNOTATION)
                .map(|(k, v)| k.len() + v.len())
                .sum();
            assert!(
                other_bytes
                    + SANDBOX_NETWORK_POLICY_ANNOTATION.len()
                    + MAX_SANDBOX_NETWORK_POLICY_ANNOTATION_BYTES
                    < 256 * 1024
            );
        } else if case == "empty" {
            assert_eq!(wire, "");
        } else {
            assert_eq!(
                wire,
                contract["metadata"]["annotations"][SANDBOX_PROCESS_ENV_ANNOTATION]
                    .as_str()
                    .unwrap()
            );
            let rendered = serde_json::to_value(&pod).unwrap();
            assert_eq!(
                rendered["spec"]["containers"][0]["env"],
                contract["spec"]["containers"][0]["env"]
            );
        }
    }
}
