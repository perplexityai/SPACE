use std::sync::{Arc, Mutex};

use http::{Method, Request};
use tower_http::trace::MakeSpan;
use tracing_subscriber::{Layer, layer::Context, prelude::*};

use tokio_util::sync::CancellationToken;

use super::{
    FailedCallSpan, K8sPathParts, KubeMakeSpan, PendingFailure, k8s_otel_name, k8s_path_parts,
    kube_operation,
};

#[test]
fn k8s_path_parts_parses_api_paths() {
    struct Case<'a> {
        name: &'a str,
        path: &'a str,
        want: K8sPathParts<'a>,
    }

    let cases = [
        Case {
            name: "namespaced core resource",
            path: "/api/v1/namespaces/space/pods/space-sbx-1",
            want: K8sPathParts {
                namespace: Some("space"),
                resource: Some("pods"),
                name: Some("space-sbx-1"),
            },
        },
        Case {
            name: "cluster scoped core resource",
            path: "/api/v1/nodes/ip-10-0-0-1",
            want: K8sPathParts {
                namespace: None,
                resource: Some("nodes"),
                name: Some("ip-10-0-0-1"),
            },
        },
        Case {
            name: "namespaced group resource",
            path: "/apis/apps/v1/namespaces/space/deployments/space-api",
            want: K8sPathParts {
                namespace: Some("space"),
                resource: Some("deployments"),
                name: Some("space-api"),
            },
        },
        Case {
            name: "resource collection",
            path: "/api/v1/namespaces/space/pods",
            want: K8sPathParts {
                namespace: Some("space"),
                resource: Some("pods"),
                name: None,
            },
        },
    ];

    for case in cases {
        let parts = k8s_path_parts(case.path);

        assert_eq!(parts, case.want, "{}", case.name);
    }
}

#[test]
fn kube_operation_uses_extension_or_method_fallback() {
    struct Case {
        name: &'static str,
        method: Method,
        operation: Option<&'static str>,
        want: &'static str,
    }

    let cases = [
        Case {
            name: "kube operation extension",
            method: Method::POST,
            operation: Some("create"),
            want: "create",
        },
        Case {
            name: "http method fallback",
            method: Method::DELETE,
            operation: None,
            want: "delete",
        },
    ];

    for case in cases {
        let mut request = Request::builder()
            .method(case.method)
            .uri("/api/v1/namespaces/space/pods")
            .body(kube::client::Body::empty())
            .unwrap();
        if let Some(operation) = case.operation {
            request.extensions_mut().insert(operation);
        }

        assert_eq!(kube_operation(&request), case.want, "{}", case.name);
    }
}

#[test]
fn k8s_otel_name_formats_operation_and_resource() {
    let cases = [
        (
            "with resource",
            "create",
            Some("pods"),
            "kubernetes.create pods",
        ),
        ("without resource", "get", None, "kubernetes.get"),
    ];

    for (name, operation, resource, want) in cases {
        assert_eq!(k8s_otel_name(operation, resource), want, "{name}");
    }
}

#[test]
fn a_call_dropped_early_fails_unless_abandoned_at_shutdown() {
    struct Case {
        name: &'static str,
        operation: &'static str,
        shutdown: Option<bool>,
        want_failure: bool,
    }

    let cases = [
        Case {
            name: "lease renewal cut off by a caller timeout",
            operation: "replace",
            shutdown: Some(false),
            want_failure: true,
        },
        Case {
            name: "lease renewal abandoned at shutdown",
            operation: "replace",
            shutdown: Some(true),
            want_failure: false,
        },
        Case {
            name: "background read cut off by a caller timeout",
            operation: "get",
            shutdown: None,
            want_failure: true,
        },
        Case {
            name: "watch ended by its consumer",
            operation: "watch",
            shutdown: None,
            want_failure: false,
        },
    ];

    for case in cases {
        let request = Request::builder()
            .uri("/apis/coordination.k8s.io/v1/namespaces/space/leases/space-cluster-controller")
            .body(kube::client::Body::empty())
            .unwrap();
        let shutdown = case.shutdown.map(|cancelled| {
            let token = CancellationToken::new();
            if cancelled {
                token.cancel();
            }
            token
        });
        let mut pending = PendingFailure {
            call: Some(FailedCallSpan::new(&request, case.operation.to_string())),
            shutdown,
        };

        assert_eq!(
            pending.drop_is_failure(),
            case.want_failure,
            "{}",
            case.name
        );
        pending.complete();
    }
}

#[test]
fn kube_make_span_records_kubernetes_fields() {
    struct Case<'a> {
        name: &'a str,
        method: Method,
        uri: &'a str,
        operation: Option<&'static str>,
        want_fields: &'a [(&'a str, &'a str)],
    }

    let cases = [
        Case {
            name: "namespaced pod create",
            method: Method::POST,
            uri: "/api/v1/namespaces/space/pods?fieldManager=space-api",
            operation: Some("create"),
            want_fields: &[
                ("otel.kind", "client"),
                ("otel.name", "kubernetes.create pods"),
                ("operation.name", "kubernetes.client"),
                ("http.method", "POST"),
                (
                    "http.target",
                    "/api/v1/namespaces/space/pods?fieldManager=space-api",
                ),
                ("url.path", "/api/v1/namespaces/space/pods"),
                ("k8s.operation", "create"),
                ("k8s.namespace", "space"),
                ("k8s.resource", "pods"),
                ("k8s.name", ""),
            ],
        },
        Case {
            name: "method fallback for cluster resource",
            method: Method::GET,
            uri: "/api/v1/nodes/ip-10-0-0-1",
            operation: None,
            want_fields: &[
                ("otel.name", "kubernetes.get nodes"),
                ("http.method", "GET"),
                ("k8s.operation", "get"),
                ("k8s.namespace", ""),
                ("k8s.resource", "nodes"),
                ("k8s.name", "ip-10-0-0-1"),
            ],
        },
    ];

    for case in cases {
        let mut request = Request::builder()
            .method(case.method)
            .uri(case.uri)
            .body(kube::client::Body::empty())
            .unwrap();
        if let Some(operation) = case.operation {
            request.extensions_mut().insert(operation);
        }

        let fields = Arc::new(Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::registry().with(SpanFieldCapture {
            fields: Arc::clone(&fields),
        });
        tracing::subscriber::with_default(subscriber, || {
            let mut make_span = KubeMakeSpan;
            let _span = make_span.make_span(&request);
        });

        let fields = fields.lock().unwrap().clone();
        for (field, want) in case.want_fields {
            assert_eq!(
                field_value(&fields, field).as_deref(),
                Some(*want),
                "{} field {field}; fields={fields:?}",
                case.name,
            );
        }
    }
}

#[derive(Clone)]
struct SpanFieldCapture {
    fields: Arc<Mutex<Vec<(String, String)>>>,
}

impl<S: tracing::Subscriber> Layer<S> for SpanFieldCapture {
    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        _id: &tracing::span::Id,
        _ctx: Context<'_, S>,
    ) {
        let mut visitor = pplx_space_observability::test_capture::RecordingVisitor::default();
        attrs.record(&mut visitor);
        self.fields.lock().unwrap().extend(visitor.into_fields());
    }
}

fn field_value(fields: &[(String, String)], name: &str) -> Option<String> {
    fields
        .iter()
        .find(|(field_name, _)| field_name == name)
        .map(|(_, value)| value.clone())
}
