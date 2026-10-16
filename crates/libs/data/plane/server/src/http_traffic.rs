//! Data-plane HTTP traffic accounting: body bytes by route class, every 429 by
//! reason, and in-flight ECW uploads.
//!
//! Measurement only. Nothing here admits, delays, or rejects a request. Upload
//! attribution uses the keys the native attached-stream limiter uses (`OrgId`,
//! `SandboxId`), so upload admission limits can be sized from these records and
//! later compared against their own decisions.

use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};
use std::time::Instant;

use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{HeaderMap, Request, StatusCode};
use axum::middleware::Next;
use axum::response::Response;
use http_body::{Frame, SizeHint};
use pplx_space_common_core::types::{OrgId, SandboxId};
use pplx_space_metrics::data_plane::{HttpBodyDirection, HttpRouteClass, TooManyRequestsReason};

use super::{EcwHostConfig, should_proxy_ecw_service_port_request};

/// Bytes a body accumulates before they are flushed to the counter, so a
/// large transfer reports every MiB while it moves without one metric call per
/// frame. A body that moves less than this reports only when it ends.
const BYTE_FLUSH_THRESHOLD: u64 = 1024 * 1024;

/// Classifies a request by the surface it reaches. Service-port traffic is
/// decided by host before path, matching `route_ecw_service_port_requests`,
/// which forwards it before path routing runs.
pub(super) fn classify_route(
    ecw_hosts: &EcwHostConfig,
    headers: &HeaderMap,
    path: &str,
) -> HttpRouteClass {
    if should_proxy_ecw_service_port_request(ecw_hosts, headers) {
        return HttpRouteClass::ServicePort;
    }
    match path {
        "/files" => HttpRouteClass::Files,
        "/healthz" | "/readyz" | "/health" => HttpRouteClass::Health,
        _ if path.starts_with("/filesystem.Filesystem/") => HttpRouteClass::Filesystem,
        _ if path.starts_with("/process.Process/") => HttpRouteClass::Process,
        _ if path.starts_with("/v1/sessions/") => HttpRouteClass::NativeSession,
        _ => HttpRouteClass::Other,
    }
}

/// Request body bytes read so far, shared with the handler of a `files` request
/// so an upload's completion record carries what was actually received.
#[derive(Clone, Default)]
pub(super) struct RequestBodyBytes(Arc<AtomicU64>);

impl RequestBodyBytes {
    fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// Counts request and response body bytes and every 429 for one request.
pub(super) async fn observe_http_traffic(
    State(ecw_hosts): State<Arc<EcwHostConfig>>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let route_class = classify_route(&ecw_hosts, request.headers(), request.uri().path());
    let (mut parts, body) = request.into_parts();
    let request_bytes = (route_class == HttpRouteClass::Files).then(RequestBodyBytes::default);
    if let Some(request_bytes) = &request_bytes {
        parts.extensions.insert(request_bytes.clone());
    }
    let body = CountedBody::wrap(
        body,
        ByteTally::new(HttpBodyDirection::In, route_class, request_bytes),
    );

    let response = next.run(Request::from_parts(parts, body)).await;
    if response.status() == StatusCode::TOO_MANY_REQUESTS {
        let reason = response
            .extensions()
            .get::<TooManyRequestsReason>()
            .copied()
            .unwrap_or(TooManyRequestsReason::Unclassified);
        pplx_space_metrics::data_plane::http_too_many_requests(route_class, reason);
    }
    let (parts, body) = response.into_parts();
    Response::from_parts(
        parts,
        CountedBody::wrap(
            body,
            ByteTally::new(HttpBodyDirection::Out, route_class, None),
        ),
    )
}

/// Batches body bytes into `data_plane.http.bytes`. Dropping it flushes the
/// remainder, so a body abandoned mid-transfer still reports what moved.
struct ByteTally {
    direction: HttpBodyDirection,
    route_class: HttpRouteClass,
    pending: u64,
    shared: Option<RequestBodyBytes>,
}

impl ByteTally {
    fn new(
        direction: HttpBodyDirection,
        route_class: HttpRouteClass,
        shared: Option<RequestBodyBytes>,
    ) -> Self {
        Self {
            direction,
            route_class,
            pending: 0,
            shared,
        }
    }

    fn add(&mut self, bytes: usize) {
        let bytes = bytes as u64;
        if let Some(shared) = &self.shared {
            shared.0.fetch_add(bytes, Ordering::Relaxed);
        }
        self.pending += bytes;
        if self.pending >= BYTE_FLUSH_THRESHOLD {
            self.flush();
        }
    }

    fn flush(&mut self) {
        pplx_space_metrics::data_plane::http_bytes(
            self.direction,
            self.route_class,
            std::mem::take(&mut self.pending),
        );
    }
}

impl Drop for ByteTally {
    fn drop(&mut self) {
        self.flush();
    }
}

/// An HTTP body that counts its data frames. Hand-written rather than
/// `BodyExt::map_frame` because that combinator does not forward `size_hint`,
/// which would turn every `Content-Length` response into a chunked one.
struct CountedBody {
    inner: Body,
    tally: ByteTally,
}

impl CountedBody {
    fn wrap(inner: Body, tally: ByteTally) -> Body {
        Body::new(Self { inner, tally })
    }
}

impl http_body::Body for CountedBody {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let frame = Pin::new(&mut self.inner).poll_frame(cx);
        if let Poll::Ready(Some(Ok(frame))) = &frame
            && let Some(data) = frame.data_ref()
        {
            self.tally.add(data.len());
        }
        frame
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

/// Per-pod count of in-flight ECW uploads and the bytes they reserve.
///
/// Each change and its gauge emit happen under one lock. Emitting after
/// releasing it would let two concurrent releases report out of order and
/// leave an idle pod showing a stale non-zero gauge until the next upload.
#[derive(Default)]
pub(super) struct UploadTracker {
    totals: Mutex<UploadTotals>,
}

#[derive(Clone, Copy, Default)]
struct UploadTotals {
    count: usize,
    reserved_bytes: u64,
}

impl UploadTracker {
    /// Starts tracking one upload whose route has resolved, so its sandbox
    /// and org are known. `reserved_bytes` is the budget upload admission
    /// holds for it, or, with admission off, its declared `Content-Length` or
    /// its write path's body limit.
    pub(super) fn begin(
        self: &Arc<Self>,
        reserved_bytes: u64,
        sandbox_id: SandboxId,
        org_id: Option<OrgId>,
        request_bytes: Option<RequestBodyBytes>,
    ) -> InFlightUpload {
        self.update(|totals| {
            totals.count += 1;
            totals.reserved_bytes += reserved_bytes;
        });
        InFlightUpload {
            tracker: Arc::clone(self),
            reserved_bytes,
            request_bytes,
            started: Instant::now(),
            sandbox_id,
            org_id,
            status: None,
        }
    }

    fn update(&self, change: impl FnOnce(&mut UploadTotals)) {
        let mut totals = self.totals.lock().unwrap_or_else(PoisonError::into_inner);
        change(&mut totals);
        pplx_space_metrics::data_plane::upload_in_flight(totals.count, totals.reserved_bytes);
    }

    #[cfg(test)]
    pub(super) fn snapshot(&self) -> (usize, u64) {
        let totals = *self.totals.lock().unwrap_or_else(PoisonError::into_inner);
        (totals.count, totals.reserved_bytes)
    }
}

/// One tracked upload. Dropping it releases its share of the gauges and logs a
/// completion record. `completed` means the handler returned, whatever its
/// status; a client that disconnects mid-body usually surfaces there as a 400
/// from the failed body read. `cancelled` means the handler future was dropped
/// before it returned.
pub(super) struct InFlightUpload {
    tracker: Arc<UploadTracker>,
    reserved_bytes: u64,
    request_bytes: Option<RequestBodyBytes>,
    started: Instant,
    sandbox_id: SandboxId,
    org_id: Option<OrgId>,
    status: Option<StatusCode>,
}

impl InFlightUpload {
    pub(super) fn finish(mut self, status: StatusCode) {
        self.status = Some(status);
    }
}

impl Drop for InFlightUpload {
    fn drop(&mut self) {
        let reserved_bytes = self.reserved_bytes;
        self.tracker.update(|totals| {
            totals.count = totals.count.saturating_sub(1);
            totals.reserved_bytes = totals.reserved_bytes.saturating_sub(reserved_bytes);
        });

        // The per-org record behind upload sizing. A log rather than a metric
        // tag because org IDs are unbounded; log-based metrics can group by
        // `org_id` without adding a custom-metric series per org.
        tracing::info!(
            target: "space.data_plane.upload",
            org_id = self.org_id.as_ref().map(tracing::field::display),
            sandbox_id = %self.sandbox_id,
            outcome = if self.status.is_some() { "completed" } else { "cancelled" },
            http_status = self.status.map(|status| status.as_u16()),
            request_body_bytes = self.request_bytes.as_ref().map(RequestBodyBytes::get),
            reserved_bytes = self.reserved_bytes,
            duration_ms = self.started.elapsed().as_millis() as u64,
            "ECW upload finished",
        );
    }
}

#[cfg(test)]
mod tests;
