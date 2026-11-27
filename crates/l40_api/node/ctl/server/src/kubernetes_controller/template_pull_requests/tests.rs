use std::str::FromStr;

use pplx_space_sandbox_pod::template_write_annotation_key;

use super::*;

fn test_reconciler() -> TemplatePullRequestReconciler {
    TemplatePullRequestReconciler::new(std::time::Duration::from_secs(1), CancellationToken::new())
}

#[test]
fn clear_template_write_annotation_patch_removes_satisfied_request_metadata() {
    let key = template_write_annotation_key().to_string();
    let patch = clear_template_write_annotation_patch("123", std::slice::from_ref(&key));

    assert_eq!(patch["metadata"]["resourceVersion"], "123");
    assert_eq!(
        patch["metadata"]["annotations"][&key],
        serde_json::Value::Null
    );
    assert!(patch["metadata"]["labels"].is_null());
}

#[test]
fn template_write_requests_parse_desired_template_references() {
    let current = node_watch::NodeMetadata {
        resource_version: "123".to_string(),
        labels: BTreeMap::from([(
            "node.kubernetes.io/instance-type".to_string(),
            "metal".to_string(),
        )]),
        annotations: BTreeMap::from([(
            template_write_annotation_key().to_string(),
            "code-runtime:release-2026-05-03".to_string(),
        )]),
        network_slots_capacity: None,
    };

    let mut reconciler = test_reconciler();

    assert_eq!(
        reconciler
            .template_write_requests_by_key(&current)
            .into_values()
            .collect::<Vec<_>>(),
        vec![TemplateWriteTaskRequest {
            reference: TemplateReference::from_str("code-runtime:release-2026-05-03").unwrap(),
            environment: None,
        }]
    );
}

#[test]
fn template_write_requests_parse_exact_artifact_digest_annotation() {
    let current = node_watch::NodeMetadata {
        resource_version: "123".to_string(),
        labels: BTreeMap::new(),
        annotations: BTreeMap::from([(
            template_write_annotation_key().to_string(),
            "seed@sha256:862261dbcc1df1b313ac950b8b994ce4a1f800e7914ba63b358d52c38b441fde"
                .to_string(),
        )]),
        network_slots_capacity: None,
    };

    let mut reconciler = test_reconciler();

    assert_eq!(
        reconciler
            .template_write_requests_by_key(&current)
            .into_values()
            .collect::<Vec<_>>(),
        vec![TemplateWriteTaskRequest {
            reference: TemplateReference::from_str(
                "seed@sha256:862261dbcc1df1b313ac950b8b994ce4a1f800e7914ba63b358d52c38b441fde",
            )
            .unwrap(),
            environment: None,
        }]
    );
}

#[test]
fn malformed_template_write_warning_state_tracks_value_changes() {
    let key = template_write_annotation_key().to_string();
    let mut reconciler = test_reconciler();
    let current = node_watch::NodeMetadata {
        resource_version: "123".to_string(),
        labels: BTreeMap::new(),
        annotations: BTreeMap::from([(key.clone(), "seed".to_string())]),
        network_slots_capacity: None,
    };

    assert!(
        reconciler
            .template_write_requests_by_key(&current)
            .is_empty()
    );
    assert_eq!(
        reconciler
            .last_malformed_warnings
            .get(&key)
            .map(String::as_str),
        Some("seed")
    );

    assert!(
        reconciler
            .template_write_requests_by_key(&current)
            .is_empty()
    );
    assert_eq!(reconciler.last_malformed_warnings.len(), 1);

    let fixed = node_watch::NodeMetadata {
        resource_version: "124".to_string(),
        labels: BTreeMap::new(),
        annotations: BTreeMap::from([(key.clone(), "seed:latest".to_string())]),
        network_slots_capacity: None,
    };
    assert_eq!(reconciler.template_write_requests_by_key(&fixed).len(), 1);
    assert!(!reconciler.last_malformed_warnings.contains_key(&key));
}

#[test]
fn template_reference_lookup_parts_maps_latest_to_default_lookup() {
    let reference = TemplateReference::from_str("seed:latest").unwrap();

    assert_eq!(
        template_reference_lookup_parts(&reference),
        ("seed".to_string(), None, None)
    );
}

#[test]
fn template_reference_lookup_parts_maps_tag_to_tag_lookup() {
    let reference = TemplateReference::from_str("seed:stable").unwrap();

    assert_eq!(
        template_reference_lookup_parts(&reference),
        ("seed".to_string(), None, Some("stable".to_string()))
    );
}

#[test]
fn template_reference_lookup_parts_maps_artifact_digest_to_hash_lookup() {
    let reference = TemplateReference::from_str(
        "seed@sha256:862261dbcc1df1b313ac950b8b994ce4a1f800e7914ba63b358d52c38b441fde",
    )
    .unwrap();

    assert_eq!(
        template_reference_lookup_parts(&reference),
        (
            "seed".to_string(),
            Some(
                "sha256:862261dbcc1df1b313ac950b8b994ce4a1f800e7914ba63b358d52c38b441fde"
                    .to_string(),
            ),
            None,
        )
    );
}

#[test]
fn an_environment_write_requests_that_environment_s_build_of_the_digest() {
    let reference = TemplateReference::from_str(
        "seed@sha256:862261dbcc1df1b313ac950b8b994ce4a1f800e7914ba63b358d52c38b441fde",
    )
    .unwrap();
    let environment: pplx_space_execution_spec::EnvironmentReference =
        format!("sha256:{}", "e".repeat(64)).parse().unwrap();
    let write =
        pplx_space_sandbox_pod::TemplateEnvironmentWrite::new(reference.clone(), environment)
            .unwrap();
    let current = node_watch::NodeMetadata {
        resource_version: "123".to_string(),
        labels: BTreeMap::new(),
        annotations: BTreeMap::from([
            (write.annotation_key(), write.annotation_value()),
            // A malformed one is skipped, not taken as an environment-less write.
            (
                format!(
                    "{}-x",
                    pplx_space_sandbox_pod::TEMPLATE_ENVIRONMENT_WRITE_ANNOTATION_KEY
                ),
                "seed:latest".to_string(),
            ),
        ]),
        network_slots_capacity: None,
    };

    let requests = test_reconciler().template_write_requests_by_key(&current);
    assert_eq!(
        requests.into_iter().collect::<Vec<_>>(),
        vec![(
            write.annotation_key(),
            TemplateWriteTaskRequest {
                reference,
                environment: Some(environment),
            }
        )]
    );

    // It asks template-manager for that environment's build only.
    let request = super::template_manager::template_request(
        "seed",
        Some("sha256:862261dbcc1df1b313ac950b8b994ce4a1f800e7914ba63b358d52c38b441fde"),
        None,
        Some(environment),
    );
    assert!(request.environment_required);
    assert_eq!(
        request.template.unwrap().environment,
        Some(environment.to_string())
    );
    let unbound = super::template_manager::template_request("seed", None, None, None);
    assert!(!unbound.environment_required);
    assert_eq!(unbound.template.unwrap().environment, None);
}
