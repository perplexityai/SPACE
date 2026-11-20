use super::*;
use tonic::Code;

#[test]
fn ready_state_is_required_before_satisfying_pull_requests() {
    ensure_ready_state("ready", "seed@sha256:abc").unwrap();

    let err = ensure_ready_state("pulling", "seed@sha256:abc").unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert!(err.message().contains("expected 'ready'"));
}

fn listed_build(
    name: &str,
    content_hash: Option<&str>,
    cache_partition: Option<&str>,
) -> pplx_space_template_mgr_client::proto::TemplateFullInfo {
    pplx_space_template_mgr_client::proto::TemplateFullInfo {
        name: name.to_string(),
        content_hash: content_hash.map(ToOwned::to_owned),
        cache_partition: cache_partition.map(ToOwned::to_owned),
        ..Default::default()
    }
}

/// The partition-less label and its annotation are exactly what they were
/// before partitions, for every build; each build adds its partition's label.
#[test]
fn presence_keeps_the_partition_less_label_and_adds_the_partition_label() {
    let hash = format!("sha256:{}", "ab".repeat(32));
    let environment = format!("sha256:{}", "cd".repeat(32));
    let presence = template_presence(vec![
        listed_build("seed", Some(&hash), None),
        listed_build("legacy", Some(&hash), Some("")),
        listed_build("twin", Some(&hash), Some(&environment)),
        listed_build("unparsed", Some(&hash), Some("not-a-partition")),
        listed_build("no-hash", None, Some("")),
    ]);

    let legacy = TemplateCachePartition::Legacy;
    let bound = TemplateCachePartition::from_key(&environment).unwrap();
    let expected = BTreeMap::from([
        (
            template_build_presence_label_key("seed", &hash),
            hash.clone(),
        ),
        (
            template_partition_presence_label_key("seed", &hash, legacy),
            format!("{hash}@"),
        ),
        (
            template_build_presence_label_key("legacy", &hash),
            hash.clone(),
        ),
        (
            template_partition_presence_label_key("legacy", &hash, legacy),
            format!("{hash}@"),
        ),
        (
            template_build_presence_label_key("twin", &hash),
            hash.clone(),
        ),
        (
            template_partition_presence_label_key("twin", &hash, bound),
            format!("{hash}@{environment}"),
        ),
        (
            template_build_presence_label_key("unparsed", &hash),
            hash.clone(),
        ),
    ]);
    assert_eq!(presence, expected);
}

/// A build's directory is named by exactly one of its presence annotations,
/// in either partition: consumers match a node's annotations against the
/// build directories it holds.
#[test]
fn each_build_directory_is_named_by_one_annotation() {
    let hash = format!("sha256:{}", "ab".repeat(32));
    let environment = format!("sha256:{}", "cd".repeat(32));
    for (partition, dir) in [
        (Some(""), hash.clone()),
        (None, hash.clone()),
        (Some(environment.as_str()), format!("{hash}@{environment}")),
    ] {
        let presence = template_presence(vec![listed_build("seed", Some(&hash), partition)]);
        let naming = presence.values().filter(|value| **value == dir).count();
        assert_eq!(naming, 1, "{partition:?}: {presence:?}");
    }
}

/// A legacy-layout build bound to another environment, as startup recovery
/// adopts it in the legacy partition, carries that environment's partition
/// label instead of the legacy one: lookups requiring the environment take
/// it, and lookups requiring the initial environment, whose partition is the
/// legacy one, don't. Its annotation names the legacy directory it lives in.
/// An unbound or initial-environment legacy build carries the legacy one.
#[test]
fn an_adopted_build_carries_its_binding_s_partition_label_not_the_legacy_one() {
    let hash = format!("sha256:{}", "ab".repeat(32));
    let environment = format!("sha256:{}", "cd".repeat(32));
    let initial =
        pplx_space_node_runtime_core::catalog::initial_environment_reference().to_string();
    let bound =
        |name: &str, environment: &str| pplx_space_template_mgr_client::proto::TemplateFullInfo {
            execution: Some(pplx_space_execution_common::proto::ExecutionMetadata {
                environment: Some(environment.to_string()),
                ..Default::default()
            }),
            ..listed_build(name, Some(&hash), Some(""))
        };
    let presence = template_presence(vec![
        bound("adopted", &environment),
        bound("initial", &initial),
    ]);

    let twin = TemplateCachePartition::from_key(&environment).unwrap();
    assert_eq!(
        presence.get(&template_partition_presence_label_key(
            "adopted", &hash, twin
        )),
        Some(&format!("{hash}@"))
    );
    assert!(
        !presence.contains_key(&template_partition_presence_label_key(
            "adopted",
            &hash,
            TemplateCachePartition::Legacy
        )),
        "an adopted build doesn't attract pods requiring the initial environment"
    );
    assert!(
        presence.contains_key(&template_partition_presence_label_key(
            "initial",
            &hash,
            TemplateCachePartition::Legacy
        ))
    );
    assert_eq!(
        presence
            .keys()
            .filter(|key| key.starts_with("space.pplx.net/template-build-initial"))
            .count(),
        2,
        "an initial-environment build carries the partition-less and legacy labels only"
    );
}

/// A node holding both an adopted legacy-layout build bound to an environment
/// and that environment's own build of the digest labels the environment's
/// partition with the latter's directory, whichever row lists last.
#[test]
fn an_environment_s_own_build_keeps_its_label_beside_an_adopted_one() {
    let hash = format!("sha256:{}", "ab".repeat(32));
    let environment = format!("sha256:{}", "cd".repeat(32));
    let adopted = pplx_space_template_mgr_client::proto::TemplateFullInfo {
        execution: Some(pplx_space_execution_common::proto::ExecutionMetadata {
            environment: Some(environment.clone()),
            ..Default::default()
        }),
        ..listed_build("seed", Some(&hash), Some(""))
    };
    let own = pplx_space_template_mgr_client::proto::TemplateFullInfo {
        execution: adopted.execution.clone(),
        ..listed_build("seed", Some(&hash), Some(&environment))
    };
    let key = template_partition_presence_label_key(
        "seed",
        &hash,
        TemplateCachePartition::from_key(&environment).unwrap(),
    );
    for templates in [
        vec![adopted.clone(), own.clone()],
        vec![own.clone(), adopted.clone()],
    ] {
        assert_eq!(
            template_presence(templates).get(&key),
            Some(&format!("{hash}@{environment}"))
        );
    }
}

/// An environment write is done only when the ready build the answer names is
/// bound to that environment's partition: a template-manager that ignores
/// `environment_required` and answers with the original, or one whose answer
/// carries no binding, leaves it pending. The initial environment takes an
/// unbound build, and a write naming no environment takes any binding.
#[test]
fn an_environment_write_waits_for_a_build_bound_to_that_environment() {
    use pplx_space_execution_spec::EnvironmentReference;
    let hash = format!("sha256:{}", "ab".repeat(32));
    let twin: EnvironmentReference = format!("sha256:{}", "cd".repeat(32)).parse().unwrap();
    let initial = pplx_space_node_runtime_core::catalog::initial_environment_reference();
    let answer = |bound: Option<EnvironmentReference>| {
        pplx_space_template_mgr_client::proto::GetTemplateResponse {
            template: Some(TemplateRef {
                name: "seed".to_string(),
                content_hash: Some(hash.clone()),
                tag: None,
                environment: bound.map(|bound| bound.to_string()),
            }),
            state: TEMPLATE_READY_STATE.to_string(),
            ..Default::default()
        }
    };

    let ensured = ensured_template(answer(Some(twin)), "seed", Some(twin)).unwrap();
    assert_eq!(ensured.content_hash.as_deref(), Some(hash.as_str()));
    for (bound, requested) in [
        (Some(initial), Some(twin)),
        (None, Some(twin)),
        (Some(twin), Some(initial)),
    ] {
        let error = ensured_template(answer(bound), "seed", requested).unwrap_err();
        assert_eq!(error.code(), Code::FailedPrecondition, "{bound:?}");
    }
    let mut unparsable = answer(None);
    unparsable.template.as_mut().unwrap().environment = Some("not-a-reference".to_string());
    assert_eq!(
        ensured_template(unparsable, "seed", Some(initial))
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    for (bound, requested) in [
        (None, Some(initial)),
        (Some(initial), Some(initial)),
        (Some(twin), None),
        (None, None),
    ] {
        ensured_template(answer(bound), "seed", requested)
            .unwrap_or_else(|error| panic!("{bound:?} for {requested:?}: {error}"));
    }
}
