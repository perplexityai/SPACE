mod reflector;
pub use reflector::spawn_reflector;

use std::time::Duration;

use http::{Request, Response};
use kube::{
    Client, Config, Error,
    client::{Body, ClientBuilder},
};
use tower_http::{
    classify::ServerErrorsFailureClass,
    trace::{MakeSpan, TraceLayer},
};
use tracing::Span;

pub async fn traced_kube_client_from_env() -> Result<Client, Error> {
    let config = Config::infer().await.map_err(Error::InferConfig)?;
    traced_kube_client(config)
}

pub fn traced_kube_client(config: Config) -> Result<Client, Error> {
    let trace_layer = TraceLayer::new_for_http()
        .make_span_with(KubeMakeSpan)
        .on_response(
            |response: &Response<Box<kube::client::DynBody>>, _latency: Duration, span: &Span| {
                let status = response.status();
                span.record("http.status_code", status.as_u16());
                if status.is_client_error() || status.is_server_error() {
                    span.record("otel.status_code", "ERROR");
                    span.record("otel.status_description", status.as_str());
                } else {
                    span.record("otel.status_code", "OK");
                }
            },
        )
        .on_failure(
            |failure: ServerErrorsFailureClass, _latency: Duration, span: &Span| {
                span.record("otel.status_code", "ERROR");
                match failure {
                    ServerErrorsFailureClass::StatusCode(status) => {
                        span.record("http.status_code", status.as_u16());
                        span.record("otel.status_description", status.as_str());
                    }
                    ServerErrorsFailureClass::Error(error) => {
                        span.record("otel.status_description", error.as_str());
                    }
                }
            },
        );
    Ok(ClientBuilder::try_from(config)?
        .with_layer(&trace_layer)
        .build())
}

#[derive(Clone, Copy, Debug)]
struct KubeMakeSpan;

impl MakeSpan<Body> for KubeMakeSpan {
    fn make_span(&mut self, request: &Request<Body>) -> Span {
        let operation = kube_operation(request);
        let path = request.uri().path();
        let path_parts = k8s_path_parts(path);
        let resource = path_parts.resource.unwrap_or("unknown");
        let otel_name = k8s_otel_name(&operation, path_parts.resource);

        tracing::info_span!(
            "kubernetes.client",
            otel.kind = "client",
            otel.name = %otel_name,
            otel.status_code = tracing::field::Empty,
            otel.status_description = tracing::field::Empty,
            "operation.name" = "kubernetes.client",
            http.method = %request.method(),
            http.target = %request.uri(),
            http.status_code = tracing::field::Empty,
            url.path = %path,
            k8s.operation = %operation,
            k8s.resource = %resource,
            k8s.namespace = path_parts.namespace.unwrap_or(""),
            k8s.name = path_parts.name.unwrap_or(""),
        )
    }
}

fn kube_operation(request: &Request<Body>) -> String {
    request
        .extensions()
        .get::<&'static str>()
        .map(|operation| (*operation).to_string())
        .unwrap_or_else(|| request.method().as_str().to_ascii_lowercase())
}

fn k8s_otel_name(operation: &str, resource: Option<&str>) -> String {
    match resource {
        Some(resource) => format!("kubernetes.{operation} {resource}"),
        None => format!("kubernetes.{operation}"),
    }
}

#[derive(Debug, PartialEq, Eq)]
struct K8sPathParts<'a> {
    namespace: Option<&'a str>,
    resource: Option<&'a str>,
    name: Option<&'a str>,
}

fn k8s_path_parts(path: &str) -> K8sPathParts<'_> {
    let segments: Vec<_> = path
        .trim_matches('/')
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect();
    let mut index = match segments.as_slice() {
        ["api", _version, ..] => 2,
        ["apis", _group, _version, ..] => 3,
        _ => 0,
    };
    let namespace = if segments.get(index) == Some(&"namespaces") {
        let namespace = segments.get(index + 1).copied();
        index += 2;
        namespace
    } else {
        None
    };

    K8sPathParts {
        namespace,
        resource: segments.get(index).copied(),
        name: segments.get(index + 1).copied(),
    }
}

#[cfg(test)]
mod tests;
