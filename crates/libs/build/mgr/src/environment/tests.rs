use super::*;
use crate::recipe::{BuildConfig, NamedStep, Origin, RecipeMetadata};
use pplx_space_common_core::types::MAX_ENV_VALUE_BYTES;

fn recipe(refs: Vec<Vec<&str>>) -> Recipe {
    Recipe {
        metadata: RecipeMetadata { description: None },
        build_config: BuildConfig::default(),
        origin: Origin::FromTemplate {
            base: "seed".parse().unwrap(),
            base_raw: "seed".into(),
        },
        steps: refs
            .into_iter()
            .map(|refs| NamedStep {
                display_name: None,
                env_refs: refs
                    .into_iter()
                    .map(|name| EnvKey::parse(name).unwrap())
                    .collect(),
                step: Step::Run {
                    command: "true".into(),
                    user: None,
                },
            })
            .collect(),
    }
}

fn literal(vars: Vec<(String, String)>) -> NamedStep {
    NamedStep {
        display_name: None,
        env_refs: vec![],
        step: Step::Env {
            vars: EnvMap::try_from(vars.into_iter().collect::<BTreeMap<_, _>>()).unwrap(),
        },
    }
}

#[test]
fn references_resolve_only_operator_exports_under_guest_names() {
    let host = BTreeMap::from([
        ("SPACE_INTERNAL_TOKEN", "dummy-daemon-only"),
        ("PACKAGE_TOKEN", "dummy-unexported-value"),
        (
            "SPACE_BUILD_EXPORT_PACKAGE_TOKEN",
            "exported ' \" $()\nvalue",
        ),
        ("SPACE_BUILD_EXPORT_EMPTY", ""),
    ]);
    let lookup = |name: &str| {
        host.get(name)
            .map(|v| (*v).into())
            .ok_or(std::env::VarError::NotPresent)
    };

    let prepared =
        PreparedRecipe::resolve_with(recipe(vec![vec!["PACKAGE_TOKEN", "EMPTY"]]), lookup).unwrap();
    let env = prepared.environments().next().unwrap();

    assert_eq!(
        env.get("PACKAGE_TOKEN").unwrap().expose(),
        "exported ' \" $()\nvalue"
    );
    assert_eq!(env.get("EMPTY").unwrap().expose(), "");
    for name in [
        "SPACE_INTERNAL_TOKEN",
        "MISSING",
        "SPACE_BUILD_EXPORT_PACKAGE_TOKEN",
    ] {
        let error =
            PreparedRecipe::resolve_with(recipe(vec![vec![], vec![name]]), lookup).unwrap_err();
        assert!(matches!(
            error,
            BuildEnvironmentError::NotExported { index: 2, .. }
        ));
        assert!(!format!("{error:?}").contains("dummy-daemon-only"));
    }
}

#[test]
fn repeated_exports_are_resolved_and_retained_once_across_steps() {
    let mut calls = 0;
    let prepared = PreparedRecipe::resolve_with(recipe(vec![vec!["TOKEN"; 10]; 1000]), |_| {
        calls += 1;
        Ok("x".repeat(MAX_ENV_VALUE_BYTES))
    })
    .unwrap();

    assert_eq!(calls, 1);
    assert_eq!(prepared.exports.as_map().len(), 1);
    let retained = prepared.exports.as_map().values().next().unwrap();
    assert!(!format!("{:?}", prepared.exports).contains(&"x".repeat(100)));
    for env in prepared.environments() {
        assert!(std::ptr::eq(env.get("TOKEN").unwrap(), retained));
    }
}

#[test]
fn unique_exports_stop_at_count_and_byte_limits() {
    for (count, size, expected_lookups, expected_reason) in [
        (
            MAX_ENV_ENTRIES + 1,
            0,
            MAX_ENV_ENTRIES,
            "env has too many entries",
        ),
        (10, MAX_ENV_VALUE_BYTES, 8, "env has too many bytes"),
        (1, MAX_ENV_VALUE_BYTES + 1, 1, "env value is too long"),
    ] {
        let names: Vec<_> = (0..count).map(|i| format!("TOKEN_{i}")).collect();
        let mut calls = 0;

        let result = PreparedRecipe::resolve_with(
            recipe(vec![names.iter().map(String::as_str).collect()]),
            |_| {
                calls += 1;
                Ok("x".repeat(size))
            },
        );

        let error = result.unwrap_err();
        assert!(error.to_string().contains(expected_reason), "{error}");
        assert_eq!(calls, expected_lookups);
        assert!(!format!("{error:?}").contains(&"x".repeat(100)));
    }
}

#[test]
fn export_overrides_are_scoped_and_combined_with_accumulated_literals() {
    let mut scoped_recipe = recipe(vec![vec![], vec!["TOKEN"], vec![]]);
    scoped_recipe.steps[0] = literal(vec![("TOKEN".into(), "literal".into())]);
    let prepared = PreparedRecipe::resolve_with(scoped_recipe, |_| Ok("exported".into())).unwrap();

    let values: Vec<_> = prepared
        .environments()
        .map(|env| env.get("TOKEN").unwrap().expose())
        .collect();

    assert_eq!(values, ["literal", "exported", "literal"]);

    for exported in [true, false] {
        let mut recipe = recipe(vec![vec![], vec![], vec![]]);
        recipe.steps[0] = literal(
            (0..7)
                .map(|i| (format!("L{i}"), "x".repeat(MAX_ENV_VALUE_BYTES)))
                .collect(),
        );
        if exported {
            recipe.steps[1].env_refs = vec![EnvKey::parse("TOKEN").unwrap()];
        } else {
            recipe.steps[1] = literal(vec![("MORE".into(), "x".repeat(MAX_ENV_VALUE_BYTES))]);
        }

        let error = PreparedRecipe::resolve_with(recipe, |_| Ok("x".repeat(MAX_ENV_VALUE_BYTES)))
            .unwrap_err();

        assert!(matches!(
            error,
            BuildEnvironmentError::Limit {
                index: 2,
                field: "effective env",
                ..
            }
        ));
    }
}

#[test]
fn invalid_export_values_are_redacted_and_contextual() {
    for value in ["secret\0value".into(), "secret".repeat(MAX_ENV_VALUE_BYTES)] {
        let error =
            PreparedRecipe::resolve_with(
                recipe(vec![vec![], vec!["TOKEN"]]),
                |_| Ok(value.clone()),
            )
            .unwrap_err();

        assert!(matches!(
            error,
            BuildEnvironmentError::InvalidValue { index: 2, .. }
        ));
        assert!(error.to_string().contains("env_refs"));
        assert!(!format!("{error:?}").contains("secret"));
    }
}

#[cfg(unix)]
#[test]
fn non_unicode_exports_are_rejected_without_their_value() {
    use std::os::unix::ffi::OsStringExt;
    let error = PreparedRecipe::resolve_with(recipe(vec![vec!["TOKEN"]]), |_| {
        Err(std::env::VarError::NotUnicode(
            std::ffi::OsString::from_vec(b"secret\xff".to_vec()),
        ))
    })
    .unwrap_err();

    assert!(matches!(
        error,
        BuildEnvironmentError::NotUnicode { index: 1, .. }
    ));
    assert!(!format!("{error:?}").contains("secret"));
}

#[tokio::test]
async fn programmatic_builder_preflights_before_connecting_to_services() {
    if !pplx_space_test_env::enter_test_process(&[("SPACE_BUILD_EXPORT_MISSING_BUILD_TEST", None)])
    {
        return;
    }
    let error = crate::TemplateBuilder::new()
        .build(
            "synthetic",
            recipe(vec![vec!["MISSING_BUILD_TEST"]]),
            "example",
            &pplx_space_common_core::types::JobId::new(),
        )
        .await
        .err()
        .expect("missing export must fail");

    assert!(matches!(
        error,
        crate::BuildError::Environment(BuildEnvironmentError::NotExported { .. })
    ));
}
