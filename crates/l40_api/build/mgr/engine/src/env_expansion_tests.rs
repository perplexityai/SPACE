use super::resolve_env_var_in_string;
use std::collections::HashMap;

#[test]
fn package_index_arguments_use_only_explicit_recipe_environment() {
    let env = pplx_space_common_core::types::EnvMap::try_from(HashMap::from([
        ("PACKAGE_TOKEN".into(), "token$NOT_EXPANDED".into()),
        (
            "PACKAGE_INDEX_URL".into(),
            "https://packages.example/simple".into(),
        ),
        (
            "CODEARTIFACT_INDEX_URL".into(),
            "https://registry.example/simple".into(),
        ),
    ]))
    .unwrap();
    let env = super::StepEnvironment::from_env(&env);

    for (input, expected) in [
        (
            "https://user:$PACKAGE_TOKEN@packages.example/simple",
            "https://user:token$NOT_EXPANDED@packages.example/simple",
        ),
        ("$PACKAGE_INDEX_URL", "https://packages.example/simple"),
        ("$CODEARTIFACT_INDEX_URL", "https://registry.example/simple"),
        ("$UNSET", ""),
        ("literal-$-value", "literal-$-value"),
    ] {
        assert_eq!(resolve_env_var_in_string(input, &env), expected);
    }
}
