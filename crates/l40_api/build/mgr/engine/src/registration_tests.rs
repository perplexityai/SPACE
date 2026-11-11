use super::*;
use pplx_space_common_core::types::{ContentHash, Digest, EnvironmentReference, TemplateName};
use pplx_space_template_mgr_client::proto::RegisterTemplateResponse;

fn hash() -> String {
    format!("sha256:{}", "a".repeat(64))
}

fn response(content_hash: Option<String>, environment: Option<String>) -> RegisterTemplateResponse {
    RegisterTemplateResponse {
        template: Some(TemplateRef {
            name: "seed".into(),
            content_hash,
            tag: None,
            environment,
        }),
        ..Default::default()
    }
}

/// The environment template-manager echoes for a registered build is the one
/// build-manager publishes under; an absent one is the legacy layout.
#[test]
fn a_registration_reports_the_environment_template_manager_echoes() {
    let e0 = pplx_space_node_runtime_core::catalog::initial_environment_reference();
    let other = EnvironmentReference::new(Digest::sha256([2; 32]));
    for environment in [None, Some(e0), Some(other)] {
        let registered = registered_build(
            "seed",
            &response(
                Some(hash()),
                environment.map(|reference| reference.to_string()),
            ),
        );
        assert!(
            matches!(
                &registered,
                Ok(build) if *build == RegisteredBuild { content_hash: hash(), environment }
            ),
            "{environment:?}"
        );
    }
}

/// An environment that doesn't parse fails the registration at once: the
/// build is already registered, so retrying can't change it. A missing
/// content hash is retried, as before.
#[test]
fn an_unusable_registration_response_is_final_only_for_its_environment() {
    assert!(matches!(
        registered_build("seed", &response(Some(hash()), Some("not-a-digest".into()))),
        Err(RegistrationFailure::Final(_))
    ));
    assert!(matches!(
        registered_build("seed", &response(None, None)),
        Err(RegistrationFailure::Retryable(_))
    ));
}

/// A build bound to the initial environment, or to none, publishes from the
/// legacy directory; one bound to another environment from its partition's.
#[test]
fn a_registered_build_publishes_from_its_partition_s_directory() {
    let template = TemplateName::parse("seed".to_string()).unwrap();
    let content_hash = hash().parse::<ContentHash>().unwrap();
    let e0 = pplx_space_node_runtime_core::catalog::initial_environment_reference();
    let other = EnvironmentReference::new(Digest::sha256([2; 32]));
    let legacy = std::path::PathBuf::from(format!(
        "{}/seed/{}",
        pplx_space_common_core::host::linux::paths::TEMPLATE_DIR,
        hash()
    ));
    for environment in [None, Some(e0)] {
        assert_eq!(
            publish::published_build_dir(&template, &content_hash, environment),
            legacy,
            "{environment:?}"
        );
    }
    assert_eq!(
        publish::published_build_dir(&template, &content_hash, Some(other)),
        std::path::PathBuf::from(format!("{}@{other}", legacy.display()))
    );
}
