use super::Identity;

fn identity(values: &[(&str, &str)]) -> Identity {
    Identity::from_lookup("default-service", |key| {
        values
            .iter()
            .find(|(name, _)| *name == key)
            .map(|(_, value)| (*value).into())
    })
}

#[test]
fn identity_precedence_table() {
    type Case = (&'static [(&'static str, &'static str)], [&'static str; 3]);
    let cases: &[Case] = &[
        (&[], ["default-service", "", ""]),
        (
            &[
                ("SPACE_SERVICE_NAME", "space"),
                ("SPACE_ENV", "prod"),
                ("SPACE_VERSION", "v1"),
            ],
            ["space", "prod", "v1"],
        ),
        (
            &[(
                "OTEL_RESOURCE_ATTRIBUTES",
                "service.name=resource,deployment.environment.name=staging,service.version=v2",
            )],
            ["resource", "staging", "v2"],
        ),
        (
            &[
                ("OTEL_SERVICE_NAME", "otel"),
                ("OTEL_RESOURCE_ATTRIBUTES", "service.name=resource"),
            ],
            ["otel", "", ""],
        ),
        (
            &[
                ("DD_SERVICE", "dd"),
                ("DD_ENV", "dev"),
                ("DD_VERSION", "v3"),
            ],
            ["dd", "dev", "v3"],
        ),
        (
            &[
                ("SPACE_SERVICE_NAME", "space"),
                ("SPACE_ENV", "prod"),
                ("SPACE_VERSION", "v1"),
                ("OTEL_SERVICE_NAME", "otel"),
                (
                    "OTEL_RESOURCE_ATTRIBUTES",
                    "service.name=resource,deployment.environment.name=staging,service.version=v2",
                ),
                ("DD_SERVICE", "dd"),
                ("DD_ENV", "dev"),
                ("DD_VERSION", "v3"),
            ],
            ["space", "prod", "v1"],
        ),
        (
            &[
                ("SPACE_SERVICE_NAME", "  "),
                ("SPACE_ENV", ""),
                ("SPACE_VERSION", "\t"),
                ("OTEL_SERVICE_NAME", " otel "),
                (
                    "OTEL_RESOURCE_ATTRIBUTES",
                    "deployment.environment.name= staging ,service.version= v2 ",
                ),
                ("DD_SERVICE", "dd"),
                ("DD_ENV", "dev"),
                ("DD_VERSION", "v3"),
            ],
            ["otel", "staging", "v2"],
        ),
        (
            &[
                ("SPACE_SERVICE_NAME", ""),
                ("OTEL_SERVICE_NAME", "\t"),
                (
                    "OTEL_RESOURCE_ATTRIBUTES",
                    "service.name= ,deployment.environment.name= ,service.version=",
                ),
                ("DD_SERVICE", " dd "),
                ("DD_ENV", " dev "),
                ("DD_VERSION", " v3 "),
            ],
            ["dd", "dev", "v3"],
        ),
        (
            &[("DD_SERVICE", ""), ("DD_ENV", " "), ("DD_VERSION", "")],
            ["default-service", "", ""],
        ),
    ];
    for (values, expected) in cases {
        let resolved = identity(values);
        assert_eq!(
            [
                resolved.log_service_name(),
                resolved.log_environment().unwrap_or(""),
                resolved.log_version().unwrap_or("")
            ],
            *expected,
            "{values:?}"
        );
    }
}

#[test]
fn legacy_service_views_preserve_datadog_bytes() {
    for (raw, telemetry, log) in [
        (None, "default-service", "default-service"),
        (Some(""), "default-service", "default-service"),
        (Some("  "), "  ", "default-service"),
        (Some(" dd "), " dd ", "dd"),
    ] {
        let resolved = Identity::from_lookup("default-service", |key| {
            (key == "DD_SERVICE")
                .then_some(raw)
                .flatten()
                .map(str::to_string)
        });
        assert_eq!(resolved.service_name(), telemetry);
        assert_eq!(resolved.log_service_name(), log);
        assert_eq!(resolved.metric_identity()[1], raw);
    }
}

#[cfg(feature = "otlp-metrics")]
#[test]
fn resources_share_identity_but_local_process_fields_win() {
    let resolved = identity(&[
        ("SPACE_SERVICE_NAME", "space"),
        ("SPACE_ENV", "prod"),
        ("SPACE_VERSION", "v1"),
        ("OTEL_SERVICE_NAME", "otel"),
        (
            "OTEL_RESOURCE_ATTRIBUTES",
            "service.name=spoof,service.instance.id=spoof,host.name=spoof,k8s.node.name=spoof,k8s.pod.name=spoof,deployment.environment.name=spoof,service.version=spoof,k8s.cluster.name=kube-cluster,team=first,malformed,team=infra=ops",
        ),
        ("SPACE_K8S_NODE_NAME", "node"),
        ("SPACE_K8S_POD_NAME", "pod"),
        ("HOSTNAME", "host"),
    ]);
    #[cfg(feature = "otel")]
    let resources = [
        resolved.metric_resource(Some("cluster")),
        resolved.trace_resource(),
    ];
    #[cfg(not(feature = "otel"))]
    let resources = [resolved.metric_resource(Some("cluster"))];
    for resource in resources {
        for (key, expected) in [
            ("service.name", "space".into()),
            ("deployment.environment.name", "prod".into()),
            ("service.version", "v1".into()),
            ("host.name", "node".into()),
            ("k8s.node.name", "node".into()),
            ("k8s.pod.name", "pod".into()),
            ("k8s.cluster.name", "kube-cluster".into()),
            (
                "service.instance.id",
                format!("host:{}", std::process::id()),
            ),
            ("team", "infra=ops".into()),
        ] {
            assert_eq!(
                resource
                    .get(&opentelemetry::Key::from_static_str(key))
                    .map(|v| v.to_string()),
                Some(expected),
                "{key}"
            );
        }
    }
    assert_eq!(
        resolved
            .metric_resource(Some("cluster"))
            .get(&opentelemetry::Key::from_static_str("space.cluster.id"))
            .map(|v| v.to_string()),
        Some("cluster".into())
    );
}

#[cfg(feature = "datadog")]
#[test]
fn dd_only_keeps_native_sdk_resource_defaults() {
    let dd = identity(&[
        ("DD_SERVICE", "dd"),
        ("DD_ENV", "prod"),
        ("DD_VERSION", "v1"),
    ]);
    assert!(!dd.has_otel_resource_settings());
    for key in ["SPACE_SERVICE_NAME", "SPACE_ENV", "SPACE_VERSION"] {
        assert!(!identity(&[(key, "sidecar")]).has_otel_resource_settings());
    }
    for key in ["OTEL_SERVICE_NAME", "OTEL_RESOURCE_ATTRIBUTES"] {
        assert!(identity(&[(key, "")]).has_otel_resource_settings());
    }
}

/// Resource attribute values are percent-decoded like the OTel SDK, so one
/// shared OTEL_RESOURCE_ATTRIBUTES means the same thing to every producer.
#[test]
fn resource_attribute_values_are_percent_decoded() {
    for (raw, service, environment) in [
        ("service.name=service%2Fapi", "service/api", ""),
        ("service.name=a%20b", "a b", ""),
        ("service.name=100%25", "100%", ""),
        (
            "service.name=svc,deployment.environment.name=eu%2Dwest",
            "svc",
            "eu-west",
        ),
        // A malformed escape is kept verbatim rather than dropped or repaired.
        ("service.name=50%", "50%", ""),
        ("service.name=%zz", "%zz", ""),
        ("service.name=%2", "%2", ""),
        ("service.name=a%2", "a%2", ""),
        // A decode that is not valid UTF-8 cannot be honored, so the raw value
        // survives and every producer still agrees on it.
        ("service.name=%ff", "%ff", ""),
    ] {
        let resolved = identity(&[("OTEL_RESOURCE_ATTRIBUTES", raw)]);
        assert_eq!(resolved.service_name(), service, "{raw}");
        assert_eq!(
            resolved.log_environment().unwrap_or_default(),
            environment,
            "{raw}"
        );
    }
}
