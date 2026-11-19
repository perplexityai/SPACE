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
