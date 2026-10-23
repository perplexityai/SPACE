//! HTTP server plumbing shared by the services' axum servers.
//!
//! [`http_trace_layer`] creates each request's `http.server` span (route
//! naming and path redaction through [`HttpSpanMetadata`], no span for
//! probes, the request id and parent trace context from the headers) and
//! returns the span's `traceparent` on the response. [`http_listener`]
//! accepts connections with `TCP_NODELAY` set.
//!
//! The parent trace context and the `traceparent` response header need
//! `pplx-space-observability`'s `otel` feature, which this crate does not
//! forward: services enable it through their own features.
//!
//! It is its own crate so that an HTTP server can be traced and served
//! without depending on `pplx-space-common-svc`, which re-exports these items
//! at their old paths (`bootstrap::…`, the crate root and `http_listener`).

pub mod http_listener;
mod http_trace;

pub use http_trace::*;
