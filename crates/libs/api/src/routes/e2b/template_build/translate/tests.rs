use super::*;
use rstest::{fixture, rstest};

const TEST_ORG: OrgId = OrgId::from_uuid(uuid::Uuid::from_u128(1));

#[test]
fn base_alias_remaps_code_interpreter_v1() {
    assert_eq!(apply_base_alias("code-interpreter-v1"), "code-runtime");
}

#[test]
fn base_alias_passes_unknown_names_through() {
    assert_eq!(apply_base_alias("seed"), "seed");
    assert_eq!(apply_base_alias("custom-template"), "custom-template");
}

#[test]
fn resolve_base_template_rejects_alias_exact_digest() {
    let hash = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    let error = resolve_base_template(&format!("code-interpreter-v1@{hash}")).unwrap_err();

    assert_eq!(error.status.as_u16(), 400);
    assert!(error.message.contains("cannot combine alias"));
    assert!(error.message.contains("code-runtime"));
}

#[test]
fn resolve_base_template_rejects_alias_digest_prefix() {
    let error = resolve_base_template("code-interpreter-v1@sha256:0123456789abcdef")
        .expect_err("alias digest prefixes cannot be resolved in another template namespace");

    assert_eq!(error.status.as_u16(), 400);
    assert!(error.message.contains("artifact digest selector"));
    assert!(error.message.contains("code-runtime"));
}

#[test]
fn resolve_base_template_keeps_custom_exact_digest() {
    let hash = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    let resolved = resolve_base_template(&format!("custom-template@{hash}")).unwrap();

    assert_eq!(resolved.to_string(), format!("custom-template@{hash}"));
}

#[test]
fn resolve_base_template_defaults_alias_to_latest() {
    let resolved = resolve_base_template("code-interpreter-v1").unwrap();

    assert_eq!(resolved.to_string(), "code-runtime:latest");
}

#[test]
fn base_template_defaults_code_runtime_uses_user_and_home() {
    let (user, workdir) = base_template_defaults("code-runtime");
    assert_eq!(user, "user");
    assert_eq!(workdir.as_deref(), Some("/home/user"));
}

#[test]
fn base_template_defaults_code_interpreter_v1_uses_user_and_home() {
    // Callers may pass the pre-alias name; defaults must match
    // `code-runtime` since the translator aliases it to that.
    let (user, workdir) = base_template_defaults("code-interpreter-v1");
    assert_eq!(user, "user");
    assert_eq!(workdir.as_deref(), Some("/home/user"));
}

#[test]
fn base_template_defaults_unknown_base_falls_back_to_root() {
    let (user, workdir) = base_template_defaults("seed");
    assert_eq!(user, "root");
    assert_eq!(workdir, None);

    let (user, workdir) = base_template_defaults("custom-template");
    assert_eq!(user, "root");
    assert_eq!(workdir, None);
}

#[rstest]
#[case("custom-template", Some(20480))]
#[case("batch-small", Some(4096))]
#[case("batch-unset", None)]
#[case("batch-large-task", Some(32768))]
#[case("batch-task", Some(16384))]
#[case("batch", None)]
#[case("other-batch-task", None)]
#[case("unknown", None)]
fn configured_disk_defaults(#[case] template_id: &str, #[case] expected: Option<u64>) {
    let rules = r#"{"custom-template":20480,"batch-*":16384,"batch-large-*":32768,"batch-small":4096,"batch-unset":null}"#;

    let actual = disk_mib_with_override(template_id, None, Some(rules)).unwrap();

    assert_eq!(actual, expected);
    assert_eq!(
        disk_mib_with_override(template_id, Some("40960"), Some(rules)).unwrap(),
        Some(40960)
    );
    for value in ["0", "invalid"] {
        assert_eq!(
            disk_mib_with_override(template_id, Some(value), Some(rules)).unwrap(),
            expected
        );
    }
}

#[test]
fn unconfigured_templates_defer_disk_sizing_to_build_manager() {
    assert_eq!(
        disk_mib_with_override("custom-template", None, None).unwrap(),
        None
    );
}

#[rstest]
#[case("invalid")]
#[case(r#"{"custom-template":0}"#)]
#[case(r#"{"custom-template":-1}"#)]
#[case(r#"{"custom-template":18446744073709551615}"#)]
#[case(r#"{"custom-template":"4096"}"#)]
#[case(r#"{"bad*pattern":4096}"#)]
#[case(r#"{"*":4096}"#)]
fn invalid_disk_configuration_rejects_build(#[case] rules: &str) {
    let error = disk_mib_with_override("custom-template", None, Some(rules)).unwrap_err();

    assert_eq!(error.status.as_u16(), 500);
}

#[test]
fn disk_override_rejects_byte_overflow() {
    let error =
        disk_mib_with_override("custom-template", Some("18446744073709551615"), None).unwrap_err();

    assert_eq!(error.status.as_u16(), 500);
}

#[fixture]
fn template_build_state() -> std::sync::Arc<TemplateBuildState> {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgres://space:space@localhost/space")
        .unwrap();
    TemplateBuildState::new(pool)
}

#[fixture]
fn public_oci_build_request(
    template_build_state: std::sync::Arc<TemplateBuildState>,
) -> (std::sync::Arc<TemplateBuildState>, TemplateBuildStartV2) {
    let req = TemplateBuildStartV2 {
        from_image: Some("docker.io/library/python:3.12-slim-bookworm".to_string()),
        from_template: Some(String::new()),
        steps: [
            ("USER", vec!["root"]),
            ("WORKDIR", vec!["/"]),
            ("RUN", vec!["true", ""]),
            ("USER", vec!["user"]),
            ("WORKDIR", vec!["/home/user"]),
            ("RUN", vec!["pwd", ""]),
        ]
        .into_iter()
        .map(|(kind, args)| TemplateStep {
            kind: kind.to_string(),
            args: args.into_iter().map(str::to_string).collect(),
            files_hash: None,
            ..Default::default()
        })
        .collect(),
        ..Default::default()
    };
    (template_build_state, req)
}

#[tokio::test]
async fn baseline_python_sdk_template_payload_translates() {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgres://space:space@localhost/space")
        .unwrap();
    let state = TemplateBuildState::new(pool);
    // e2b==2.21.1 serializes `force` on every step and requires readyCmd
    // whenever callers use set_start_cmd.
    let request: TemplateBuildStartV2 = serde_json::from_value(serde_json::json!({
        "fromImage": "python:3.12-slim",
        "force": false,
        "steps": [{
            "type": "RUN",
            "args": ["echo build", "root"],
            "force": false,
            "forceUpload": false
        }],
        "startCmd": "python /app/server.py",
        "readyCmd": "curl --fail http://127.0.0.1:8080/health"
    }))
    .unwrap();

    let recipe = build_recipe(
        TEST_ORG,
        &TemplateName::parse("sdk-template").unwrap(),
        &request,
        state.as_ref(),
        None,
        None,
    )
    .await
    .expect("the pinned Python SDK payload must remain compatible");

    assert_eq!(recipe.steps.len(), 2, "RUN plus installed start command");
}

#[tokio::test]
async fn per_step_force_from_skip_cache_translates() {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgres://space:space@localhost/space")
        .unwrap();
    let state = TemplateBuildState::new(pool);
    // e2b==2.21.1 emits this shape after `skip_cache()` marks the next step.
    let request: TemplateBuildStartV2 = serde_json::from_value(serde_json::json!({
        "force": false,
        "steps": [{
            "type": "RUN",
            "args": ["true", "root"],
            "force": true
        }]
    }))
    .unwrap();
    assert!(request.force_build());

    let recipe = build_recipe(
        TEST_ORG,
        &TemplateName::parse("forced-step").unwrap(),
        &request,
        state.as_ref(),
        None,
        None,
    )
    .await
    .expect("skip_cache payloads from the pinned SDK must remain compatible");
    assert_eq!(recipe.steps.len(), 1);
}

#[rstest]
#[tokio::test]
async fn public_oci_build_preserves_the_image_and_dockerfile_execution_context(
    public_oci_build_request: (std::sync::Arc<TemplateBuildState>, TemplateBuildStartV2),
) {
    let (state, req) = public_oci_build_request;
    let recipe = build_recipe(
        TEST_ORG,
        &TemplateName::parse("oci-smoke").unwrap(),
        &req,
        state.as_ref(),
        Some(2),
        Some(1024),
    )
    .await
    .expect("public OCI build should translate");

    let image = match recipe.origin.and_then(|origin| origin.kind) {
        Some(build_proto::recipe::origin::Kind::FromOciImage(image)) => image,
        other => panic!("expected OCI image origin, got {other:?}"),
    };
    assert_eq!(image.base, "seed");
    assert_eq!(
        image.reference,
        "docker.io/library/python:3.12-slim-bookworm"
    );
    assert_eq!(recipe.build_config.unwrap().vcpu, Some(2));

    let runs: Vec<_> = recipe
        .steps
        .iter()
        .map(|step| match step.body.as_ref() {
            Some(build_proto::recipe::step::Body::Run(run)) => {
                (run.command.as_str(), run.user.as_deref())
            }
            other => panic!("expected RUN step, got {other:?}"),
        })
        .collect();
    assert_eq!(
        runs,
        [
            ("mkdir -p '/'", Some("root")),
            ("cd '/' && {\ntrue\n}", Some("root")),
            ("mkdir -p '/home/user'", Some("root")),
            ("cd '/home/user' && {\npwd\n}", Some("user")),
        ]
    );
}

#[rstest]
#[tokio::test]
async fn ecr_build_preserves_the_private_image_reference(
    template_build_state: std::sync::Arc<TemplateBuildState>,
) {
    let reference = "111111111111.dkr.ecr.us-east-1.amazonaws.com/example/browser-runtime:test";
    let req = TemplateBuildStartV2 {
        from_image: Some(reference.to_string()),
        steps: Vec::new(),
        ..Default::default()
    };

    let recipe = build_recipe(
        TEST_ORG,
        &TemplateName::parse("private-ecr-template").unwrap(),
        &req,
        template_build_state.as_ref(),
        None,
        None,
    )
    .await
    .expect("valid ECR image should reach build-manager");

    let image = match recipe.origin.and_then(|origin| origin.kind) {
        Some(build_proto::recipe::origin::Kind::FromOciImage(image)) => image,
        other => panic!("expected OCI image origin, got {other:?}"),
    };
    assert_eq!(image.reference, reference);
}

#[rstest]
#[case(None, "seed:latest")]
#[case(Some("code-runtime"), "code-runtime:latest")]
#[tokio::test]
async fn empty_from_image_uses_the_template_base(
    template_build_state: std::sync::Arc<TemplateBuildState>,
    #[case] from_template: Option<&str>,
    #[case] expected_base: &str,
) {
    let req = TemplateBuildStartV2 {
        from_image: Some(String::new()),
        from_template: from_template.map(str::to_string),
        ..Default::default()
    };

    let recipe = build_recipe(
        TEST_ORG,
        &TemplateName::parse("empty-image").unwrap(),
        &req,
        template_build_state.as_ref(),
        None,
        None,
    )
    .await
    .expect("an empty image should be treated as unset");

    let base = match recipe.origin.and_then(|origin| origin.kind) {
        Some(build_proto::recipe::origin::Kind::FromTemplate(from_template)) => from_template.base,
        other => panic!("expected template origin, got {other:?}"),
    };
    assert_eq!(base, expected_base);
}

#[rstest]
#[tokio::test]
async fn oci_build_rejects_caller_supplied_registry_credentials(
    template_build_state: std::sync::Arc<TemplateBuildState>,
) {
    let credentialed_registry: TemplateBuildStartV2 = serde_json::from_value(serde_json::json!({
        "fromImage": "docker.io/library/debian:bookworm-slim",
        "fromImageRegistry": {
            "username": "builder-user",
            "password": "builder-password"
        },
        "steps": [],
    }))
    .expect("deserialize credentialed registry request");

    let credential_error = build_recipe(
        TEST_ORG,
        &TemplateName::parse("oci-smoke").unwrap(),
        &credentialed_registry,
        template_build_state.as_ref(),
        None,
        None,
    )
    .await
    .expect_err("registry credentials must not be discarded");

    assert_eq!(credential_error.status.as_u16(), 400);
    assert!(credential_error.message.contains("fromImageRegistry"));
}

#[rstest]
#[case("not a reference leak-marker", "leak-marker")]
fn rejected_oci_references_do_not_reach_error_descriptions(
    #[case] raw: &str,
    #[case] marker: &str,
) {
    let error = resolve_oci_image(raw).expect_err("reference must be rejected");

    assert_eq!(error.status.as_u16(), 400);
    assert!(!error.message.contains(marker));
}

#[tokio::test]
async fn code_runtime_base_runs_implicit_steps_as_user_in_home() {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgres://space:space@localhost/space")
        .unwrap();
    let state = TemplateBuildState::new(pool);
    let req = TemplateBuildStartV2 {
        from_template: Some("code-runtime".to_string()),
        steps: vec![TemplateStep {
            kind: "RUN".to_string(),
            args: vec!["stat -c '%U:%G' /home/user".to_string(), String::new()],
            files_hash: None,
            ..Default::default()
        }],
        ..Default::default()
    };

    let recipe = build_recipe(
        TEST_ORG,
        &TemplateName::parse("custom-template").unwrap(),
        &req,
        state.as_ref(),
        None,
        None,
    )
    .await
    .unwrap();

    assert_eq!(
        recipe
            .origin
            .as_ref()
            .and_then(|origin| origin.kind.as_ref())
            .and_then(|kind| match kind {
                build_proto::recipe::origin::Kind::FromTemplate(from_template) => {
                    Some(from_template.base.as_str())
                }
                _ => None,
            }),
        Some("code-runtime:latest")
    );
    let run = recipe
        .steps
        .first()
        .and_then(|step| match step.body.as_ref() {
            Some(build_proto::recipe::step::Body::Run(run)) => Some(run),
            _ => None,
        });
    assert_eq!(run.and_then(|run| run.user.as_deref()), Some("user"));
    assert_eq!(
        run.map(|run| run.command.as_str()),
        Some("cd '/home/user' && {\nstat -c '%U:%G' /home/user\n}")
    );
}

#[tokio::test]
async fn code_runtime_base_preserves_heredoc_terminator_lines() {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgres://space:space@localhost/space")
        .unwrap();
    let state = TemplateBuildState::new(pool);
    let req = TemplateBuildStartV2 {
        from_template: Some("code-runtime".to_string()),
        steps: vec![TemplateStep {
            kind: "RUN".to_string(),
            args: vec![
                "cat > /usr/local/bin/tool << 'SCRIPT_EOF'\necho ok\nSCRIPT_EOF".to_string(),
                String::new(),
            ],
            files_hash: None,
            ..Default::default()
        }],
        ..Default::default()
    };

    let recipe = build_recipe(
        TEST_ORG,
        &TemplateName::parse("custom-template").unwrap(),
        &req,
        state.as_ref(),
        None,
        None,
    )
    .await
    .unwrap();
    let command = recipe
        .steps
        .first()
        .and_then(|step| match step.body.as_ref() {
            Some(build_proto::recipe::step::Body::Run(run)) => Some(run.command.as_str()),
            _ => None,
        })
        .unwrap();

    assert!(command.contains("\nSCRIPT_EOF\n"));
    assert!(!command.contains("SCRIPT_EOF)"));
}

#[test]
fn parse_env_accepts_flat_pairs() {
    let pairs = parse_env_args(&["FOO".into(), "1".into(), "BAR".into(), "2".into()]).unwrap();
    assert_eq!(
        pairs,
        vec![("FOO".into(), "1".into()), ("BAR".into(), "2".into())]
    );
}

#[test]
fn parse_env_accepts_key_equals_value() {
    let pairs = parse_env_args(&["FOO=bar".into(), "BAZ=qux".into()]).unwrap();
    assert_eq!(
        pairs,
        vec![("FOO".into(), "bar".into()), ("BAZ".into(), "qux".into())]
    );
}

#[test]
fn expand_copy_owner_bare_user_becomes_user_user() {
    assert_eq!(expand_copy_owner("user"), "user:user");
    assert_eq!(expand_copy_owner("root"), "root:root");
}

#[test]
fn expand_copy_owner_user_group_passes_through() {
    assert_eq!(expand_copy_owner("user:group"), "user:group");
    assert_eq!(expand_copy_owner("nobody:nogroup"), "nobody:nogroup");
    // numeric is fine — vendored e2b doesn't validate, daemon-side
    // chown surfaces any error.
    assert_eq!(expand_copy_owner("1000:1000"), "1000:1000");
}

#[test]
fn home_dir_for_user_defaults_to_root() {
    assert_eq!(home_dir_for_user(""), "/root");
    assert_eq!(home_dir_for_user("root"), "/root");
}

#[test]
fn home_dir_for_user_lands_in_home_for_non_root() {
    assert_eq!(home_dir_for_user("user"), "/home/user");
    assert_eq!(home_dir_for_user("testuser"), "/home/testuser");
}

#[test]
fn shell_quote_escapes_single_quotes() {
    assert_eq!(shell_quote("it's"), "'it'\\''s'");
}

#[test]
fn copy_blob_step_preserves_e2b_source_path() {
    let step = copy_blob_step(
        "4b576166deadbeef",
        "76c9941630f318665952a3cd225fcedf51f21908b3eca8d5e38a25e591709d37".to_string(),
        "local_wheels/example_sdk.whl".to_string(),
        "/tmp/example_sdk_local/example_sdk.whl".to_string(),
    );

    assert_eq!(
        step.display_name.as_deref(),
        Some("copy from blob 4b576166 to /tmp/example_sdk_local/example_sdk.whl")
    );
    match step.body.expect("copy body") {
        build_proto::recipe::step::Body::Copy(copy) => {
            assert_eq!(
                copy.source_path.as_deref(),
                Some("local_wheels/example_sdk.whl")
            );
            assert_eq!(copy.dest, "/tmp/example_sdk_local/example_sdk.whl");
        }
        other => panic!("expected Copy step, got {other:?}"),
    }
}

// The seed, not the API, places the start command and lifts its task cap: a
// unit that set Slice= and TasksMax=infinity itself would run uncapped on a
// seed without the envelope's limits.
#[test]
fn start_cmd_unit_leaves_placement_and_task_cap_to_the_seed() {
    let step = install_start_cmd_unit("python -m example_daemon");
    let Some(build_proto::recipe::step::Body::Run(run)) = step.body else {
        panic!("expected Run step");
    };
    let encoded = run
        .command
        .split_once("printf %s ")
        .and_then(|(_, rest)| rest.split_once(" | base64 -d"))
        .map(|(quoted, _)| quoted.trim_matches('\''))
        .expect("unit payload");
    let unit = String::from_utf8(Base64.decode(encoded).expect("base64")).expect("utf-8");
    assert!(unit.contains("ExecStart=/bin/sh -lc"));
    assert!(!unit.contains("Slice="), "{unit}");
    assert!(!unit.contains("TasksMax="), "{unit}");
}

#[test]
fn install_start_cmd_unit_emits_run_with_unit_text() {
    let step = install_start_cmd_unit("python -m example_daemon");
    let body = step.body.expect("step body");
    match body {
        build_proto::recipe::step::Body::Run(run) => {
            assert!(run.command.contains("base64 -d"));
            assert!(run.command.contains(START_UNIT_PATH));
            assert!(run.command.contains(START_UNIT_NAME));
            // The final step must flush so the unit + enable symlink are
            // durable before the build VM is torn down (else the finalize
            // e2fsck journal recovery can drop this last write).
            assert!(run.command.trim_end().ends_with("sync"));
            assert_eq!(run.user.as_deref(), Some("root"));
        }
        other => panic!("expected Run step, got {other:?}"),
    }
}
