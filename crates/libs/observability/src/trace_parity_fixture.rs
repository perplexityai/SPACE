//! Process fixture for scripts/trace_parity.py. The external Agent and backend
//! own the assertions; routine cargo tests never send this workload.

#[test]
fn emit() {
    if pplx_space_common_core::env::test_hook_var("SPACE_TRACE_PARITY_MODE").is_none() {
        return;
    }
    let runtime = tokio::runtime::Runtime::new().expect("fixture runtime");
    runtime.block_on(async {
        let guard = crate::init("space-trace-parity-rust").expect("tracing initialization");
        if pplx_space_common_core::env::test_hook_var("SPACE_TRACE_PARITY_MODE").as_deref()
            == Some("operation-names")
        {
            operation_names_workload();
        } else {
            for failed in [false, true] {
                if let Some(endpoint) =
                    pplx_space_common_core::env::test_hook_var("SPACE_TRACE_MIXED_ENDPOINT")
                {
                    mixed_workload(failed, endpoint).await;
                } else {
                    workload(failed);
                }
            }
        }
        tracing::info!("trace parity workload completed");
        // The tonic exporter needs the runtime alive while its worker drains.
        guard
            .shutdown_async()
            .await
            .expect("flush and shut down exporter");
    });
}

fn operation_names_workload() {
    use opentelemetry::{
        KeyValue,
        trace::{Span as _, SpanKind, Tracer as _},
    };
    use tracing_opentelemetry::OpenTelemetrySpanExt as _;

    let root = tracing::info_span!("naming.root", "operation.name" = "Naming.Root");
    let parent = root.context();
    // An ordinary unannotated tracing span must get the same compatibility
    // treatment as spans created directly through the SDK by instrumentation.
    let ordinary = tracing::info_span!("naming.default");
    ordinary.set_parent(parent.clone()).expect("fixture parent");
    drop(ordinary);

    let tracer = opentelemetry::global::tracer("space-operation-name-fixture");
    let kv = KeyValue::new;
    for (name, kind, attributes) in [
        ("internal", SpanKind::Internal, vec![]),
        ("producer", SpanKind::Producer, vec![]),
        ("consumer", SpanKind::Consumer, vec![]),
        ("client", SpanKind::Client, vec![]),
        ("server", SpanKind::Server, vec![]),
        (
            "explicit",
            SpanKind::Internal,
            vec![kv("operation.name", "MiXeD.Custom")],
        ),
        ("empty", SpanKind::Internal, vec![kv("operation.name", "")]),
        (
            "messaging-producer",
            SpanKind::Producer,
            vec![
                kv("messaging.system", "kafka"),
                kv("messaging.operation", "publish"),
            ],
        ),
        (
            "messaging-consumer",
            SpanKind::Consumer,
            vec![
                kv("messaging.system", "kafka"),
                kv("messaging.operation.type", "process"),
            ],
        ),
        (
            "incomplete-messaging",
            SpanKind::Producer,
            vec![kv("messaging.system", "kafka")],
        ),
        (
            "graphql-internal",
            SpanKind::Internal,
            vec![kv("graphql.operation.type", "query")],
        ),
        (
            "graphql-producer",
            SpanKind::Producer,
            vec![kv("graphql.operation.type", "query")],
        ),
        (
            "graphql-consumer",
            SpanKind::Consumer,
            vec![kv("graphql.operation.type", "query")],
        ),
        (
            "empty-graphql",
            SpanKind::Internal,
            vec![kv("graphql.operation.type", "")],
        ),
        (
            "http-client",
            SpanKind::Client,
            vec![kv("http.request.method", "GET")],
        ),
        (
            "http-server",
            SpanKind::Server,
            vec![kv("http.request.method", "GET")],
        ),
        (
            "database",
            SpanKind::Client,
            vec![kv("db.system", "postgresql")],
        ),
        ("rpc", SpanKind::Client, vec![kv("rpc.system", "grpc")]),
        (
            "faas",
            SpanKind::Client,
            vec![
                kv("faas.invoked_provider", "aws"),
                kv("faas.invoked_name", "fixture"),
            ],
        ),
        (
            "network",
            SpanKind::Server,
            vec![kv("network.protocol.name", "tcp")],
        ),
        (
            "explicit-messaging",
            SpanKind::Producer,
            vec![
                kv("messaging.system", "kafka"),
                kv("messaging.operation", "publish"),
                kv("operation.name", "MiXeD.Messaging"),
            ],
        ),
    ] {
        let name = format!("naming.{name}");
        let mut span =
            tracer.build_with_context(tracer.span_builder(name.clone()).with_kind(kind), &parent);
        // Instrumentation often records semantic attributes after span start.
        // Naming must inspect the completed span, not freeze its initial shape.
        span.set_attributes(attributes);
        span.set_attribute(KeyValue::new("resource.name", name));
        span.end();
    }
    drop(root);
}

fn workload(failed: bool) {
    let case = if failed { "error" } else { "success" };
    let root = tracing::info_span!(
        "http.server",
        otel.name = "POST /trace-parity",
        otel.kind = "server",
        otel.status_code = if failed { "ERROR" } else { "OK" },
        otel.status_description = tracing::field::Empty,
        "operation.name" = "http.server",
        http.request.method = "POST",
        http.method = "POST",
        http.route = "/trace-parity",
        http.response.status_code = if failed { 500_i64 } else { 200_i64 },
        http.status_code = if failed { 500_i64 } else { 200_i64 },
        parity.case = case,
        sandbox_id = "parity-sandbox",
    );
    if failed {
        root.record("otel.status_description", "fixture failure");
    }
    let parent = crate::trace_propagation::traceparent_for_span(&root);
    let saved_parent = if std::env::var("SPACE_TRACE_EXPORTER").as_deref() == Ok("off") {
        assert!(parent.is_none(), "off must not create trace context");
        String::new()
    } else {
        parent.expect("fixture root has W3C context")
    };
    {
        let _root = root.enter();
        {
            let client = tracing::info_span!(
                "grpc.client",
                otel.name = "space.parity.v1.Worker/Execute",
                otel.kind = "client",
                "operation.name" = "grpc.client",
                rpc.system = "grpc",
                rpc.service = "space.parity.v1.Worker",
                rpc.method = "Execute",
                rpc.grpc.status_code = if failed { 13_i64 } else { 0_i64 },
                otel.status_code = if failed { "ERROR" } else { "OK" },
                otel.status_description = tracing::field::Empty,
            );
            if failed {
                client.record("otel.status_description", "fixture failure");
            }
            let _client = client.enter();
            let mut metadata = tonic::metadata::MetadataMap::new();
            crate::trace_propagation::inject_current_context(&mut metadata);
            let server = tracing::info_span!(
                parent: None,
                "grpc.server",
                otel.name = "space.parity.v1.Worker/Execute",
                otel.kind = "server",
                "operation.name" = "grpc.server",
                rpc.system = "grpc",
                rpc.service = "space.parity.v1.Worker",
                rpc.method = "Execute",
                rpc.grpc.status_code = if failed { 13_i64 } else { 0_i64 },
                otel.status_code = if failed { "ERROR" } else { "OK" },
                otel.status_description = tracing::field::Empty,
            );
            if failed {
                server.record("otel.status_description", "fixture failure");
            }
            crate::trace_propagation::set_span_parent_context_from(&server, &metadata);
            let _server = server.enter();
            tracing::info!(
                target: "sqlx::query",
                {
                db.system = "postgresql",
                db.statement = "SELECT 1",
                elapsed_secs = 0.000001_f64,
                rows_returned = 1_u64,
                rows_affected = 0_u64,
                },
                "fixture query completed"
            );
            tracing::info!(target: "sqlx::pool::acquire", acquired_after_secs = 0.000001_f64);
        }
        let redis = tracing::info_span!(
            "redis.client",
            otel.name = "GET parity",
            otel.kind = "client",
            "operation.name" = "redis.client",
            db.system = "redis",
            db.operation.name = "GET",
            otel.status_code = "OK",
        );
        drop(redis);
    }
    // Saved work runs as a fresh root linked to the request that saved it, as
    // the jobs worker does.
    let job = tracing::info_span!(
        parent: None,
        "job.execute",
        otel.name = "job.parity",
        otel.kind = "internal",
        "operation.name" = "job.parity",
        "resource.name" = "parity",
        job.kind = "parity",
        otel.status_code = "OK",
    );
    crate::trace_propagation::link_span_from_traceparent(&job, &saved_parent);
    drop(job);
    drop(root);
}

async fn mixed_workload(failed: bool, endpoint: String) {
    use tracing::Instrument as _;
    let case = if failed { "error" } else { "success" };
    let root = tracing::info_span!(
        "http.server",
        otel.kind = "server",
        "operation.name" = "http.server",
        "resource.name" = "POST /trace-mixed",
        parity.case = case,
        sandbox_id = "parity-sandbox",
        otel.status_code = if failed { "ERROR" } else { "OK" },
        otel.status_description = tracing::field::Empty,
    );
    if failed {
        root.record("otel.status_description", "fixture failure");
    }
    let serialized_parent =
        crate::trace_propagation::traceparent_for_span(&root).expect("W3C root context");
    async {
        let span = tracing::info_span!(
            "grpc.client",
            otel.kind = "client",
            "operation.name" = "grpc.client",
            "resource.name" = "grpc.health.v1.Health/Check",
            otel.status_code = if failed { "ERROR" } else { "OK" },
            otel.status_description = tracing::field::Empty
        );
        if failed {
            span.record("otel.status_description", "fixture failure");
        }
        async {
            let channel = tonic::transport::Endpoint::from_shared(endpoint)
                .expect("fixture URL")
                .connect_timeout(std::time::Duration::from_secs(5))
                .connect()
                .await
                .expect("connect to Go fixture");
            let mut client = tonic_health::pb::health_client::HealthClient::new(channel);
            let mut request = tonic::Request::new(tonic_health::pb::HealthCheckRequest {
                service: format!("{serialized_parent}|{case}"),
            });
            crate::trace_propagation::inject_current_context(request.metadata_mut());
            let result = client.check(request).await;
            if failed {
                assert_eq!(
                    result.expect_err("failed fixture").code(),
                    tonic::Code::Internal
                );
            } else {
                result.expect("successful fixture");
            }
        }
        .instrument(span)
        .await;
    }
    .instrument(root)
    .await;
}
