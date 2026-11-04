use super::*;

#[test]
fn gateway_environment_omits_retired_tls_settings() {
    let env = net_gateway_env();
    for key in ["SPACE_TLS_INTERCEPTION_MODE", "SPACE_TLS_INTERCEPTION_CA"] {
        assert!(!env.contains_key(key));
    }
}

#[test]
fn inherited_tls_settings_are_not_forwarded_to_gateway_processes() {
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "sidecar_env::tests::gateway_environment_omits_retired_tls_settings",
        ])
        .env("SPACE_TLS_INTERCEPTION_MODE", "enforce")
        .env("SPACE_TLS_INTERCEPTION_CA", "/does-not-exist/root.pem")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stdout)
    );
}

#[test]
fn cluster_id_is_forwarded_to_sidecars() {
    // Sidecars emit sandbox events + metrics, so the cluster identity must be
    // in the forwarded set or their telemetry loses the cluster_id tag/field.
    assert!(TELEMETRY_ENV_KEYS.contains(&CLUSTER_ID_ENV));
}

#[test]
fn canary_parent_uses_canary_sidecar_service() {
    let service = child_service_name("space-net-gateway", Some("space-sandbox-manager-canary"));

    assert_eq!(service, "space-net-gateway-canary");
}

#[test]
fn stable_parent_uses_stable_sidecar_service() {
    let service = child_service_name("space-net-gateway", Some("space-sandbox-manager"));

    assert_eq!(service, "space-net-gateway");
}

#[test]
fn sidecars_inherit_metrics_transport_without_inheriting_unrelated_environment() {
    const CHILD: &str = "SPACE_SIDECAR_METRICS_TEST_CHILD";
    const SETTINGS: &[(&str, &str)] = &[
        (
            "OTEL_RESOURCE_ATTRIBUTES",
            "team=infra,deployment.environment.name=testing",
        ),
        ("OTEL_SERVICE_NAME", "manager-canary"),
        ("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.2:4317"),
        (
            "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
            "http://127.0.0.3:4317",
        ),
        ("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc"),
        (
            "OTEL_EXPORTER_OTLP_METRICS_PROTOCOL",
            "unsupported-protocol",
        ),
        // The child owns its reader, so the cadence must survive the launch
        // boundary alongside the endpoint it applies to.
        ("OTEL_METRIC_EXPORT_INTERVAL", "5000"),
        // Invalid settings must reach the child's validator too: dropping
        // headers would silently bypass the plaintext profile's rejection.
        // Trace verbosity is selected separately from RUST_LOG, so a child
        // without it would export a different span set than its parent.
        ("SPACE_TRACE_FILTER", "warn"),
        ("OTEL_EXPORTER_OTLP_HEADERS", "x-test=common"),
        ("OTEL_EXPORTER_OTLP_METRICS_HEADERS", "x-test=metrics"),
    ];
    if std::env::var_os(CHILD).is_some() {
        for env in [
            net_gateway_env(),
            telemetry_env_for_service("space-spaced-host"),
        ] {
            let configured = std::env::var("SPACE_METRICS_SINK").ok();
            assert_eq!(
                env.get("SPACE_METRICS_SINK"),
                configured.as_ref().filter(|value| !value.trim().is_empty())
            );
            for &(key, value) in SETTINGS {
                assert_eq!(env.get(key).map(String::as_str), Some(value), "{key}");
            }
            assert!(!env.contains_key("DATABASE_URL"));
            assert!(!env.contains_key(CHILD));
        }
        return;
    }

    // Isolate environment changes from other tests in this process. The real
    // launch boundary must preserve each explicit, mutually exclusive sink
    // while leaving absent/blank selectors to the child's normal default.
    for selector in [
        None,
        Some(" "),
        Some("auto"),
        Some("datadog"),
        Some("otlp"),
        Some("off"),
    ] {
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command
            .args(["--exact", "sidecar_env::tests::sidecars_inherit_metrics_transport_without_inheriting_unrelated_environment"])
            .env_clear()
            .env(CHILD, "1")
            .env("DATABASE_URL", "must-not-reach-sidecars")
            .envs(SETTINGS.iter().copied());
        if let Some(selector) = selector {
            command.env("SPACE_METRICS_SINK", selector);
        }
        let output = command.output().unwrap();
        assert!(output.status.success(), "selector={selector:?}: {output:?}");
    }
}

#[test]
fn sidecar_names_respect_explicit_identity_and_legacy_suffix_rules() {
    const CHILD: &str = "SPACE_SIDECAR_LOG_TEST_CHILD";
    const TRANSPORT: &[(&str, &str)] = &[
        ("DD_AGENT_HOST", "127.0.0.2"),
        ("DD_DOGSTATSD_PORT", "18125"),
        ("DD_TRACE_AGENT_PORT", "18126"),
        ("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.2:14317"),
    ];
    if std::env::var_os(CHILD).is_some() {
        for service in ["space-net-gateway", "space-spaced-host"] {
            let env = telemetry_env_for_service(service);
            let log_suffix = std::env::var("EXPECTED_LOG_SUFFIX").unwrap();
            let dd_suffix = std::env::var("EXPECTED_DD_SUFFIX").unwrap();
            assert_eq!(
                env[logging::SPACE_SERVICE_NAME],
                format!("{service}{log_suffix}")
            );
            assert_eq!(env["DD_SERVICE"], format!("{service}{dd_suffix}"));
            assert_eq!(env[logging::SPACE_ENV], "neutral-environment");
            assert_eq!(env[logging::SPACE_VERSION], "neutral-revision");
            assert_eq!(env["DD_ENV"], "legacy-environment");
            assert_eq!(env["DD_VERSION"], "legacy-revision");
            assert_eq!(env["SPACE_LOG_FORMAT"], "json");
            for &(key, value) in TRANSPORT {
                assert_eq!(env[key], value);
            }
        }
        return;
    }

    for (log_parent, dd_parent, log_suffix, dd_suffix) in [
        ("parent-canary", "parent", "-canary", ""),
        ("parent", "parent-canary", "", "-canary"),
        (" ", "parent-canary", "-canary", "-canary"),
        // A DD-only parent retains the raw suffix rule for metrics/traces;
        // its generated SPACE name must agree rather than trimming into canary.
        (" ", "parent-canary ", "", ""),
        ("parent", "parent", "", ""),
    ] {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "sidecar_env::tests::sidecar_names_respect_explicit_identity_and_legacy_suffix_rules",
            ])
            .env_clear()
            .env(CHILD, "1")
            .env(logging::SPACE_SERVICE_NAME, log_parent)
            .env("DD_SERVICE", dd_parent)
            .env(logging::SPACE_ENV, "neutral-environment")
            .env(logging::SPACE_VERSION, "neutral-revision")
            .env("DD_ENV", "legacy-environment")
            .env("DD_VERSION", "legacy-revision")
            .envs(TRANSPORT.iter().copied())
            .env("EXPECTED_LOG_SUFFIX", log_suffix)
            .env("EXPECTED_DD_SUFFIX", dd_suffix)
            .output()
            .unwrap();
        assert!(output.status.success(), "{output:?}");
    }
}
