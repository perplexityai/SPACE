//! One outcome per parsed request, owned through the response body's lifetime.
//! Completion means the response body was produced for Hyper, not acknowledged
//! by the peer. An upgrade completes the HTTP handshake; its tunnel is observed
//! separately as a connection relay.

use super::{
    super::observation::{Connection, Facts, NativeError, Phase, Side},
    HttpProtocol,
};
use http::Response;
use hyper::body::{Body, Frame, SizeHint};
use std::{
    pin::Pin,
    sync::{Arc, OnceLock},
    task::{Context, Poll},
};

#[derive(Clone, Copy, Debug)]
pub(super) enum StatusSource {
    Local,
    Upstream,
}
impl StatusSource {
    fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Upstream => "upstream",
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) enum Injection {
    Injected,
    Failed,
}
impl Injection {
    fn as_str(self) -> &'static str {
        match self {
            Self::Injected => "injected",
            Self::Failed => "failed",
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Outcome {
    Completed,
    Aborted,
}
impl Outcome {
    fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Aborted => "aborted",
        }
    }
}

/// The upload body may still be running while response headers arrive. Its
/// observer shares facts without owning another terminal request event.
#[derive(Clone)]
pub(super) struct Observer {
    connection: Connection,
    failure: Arc<OnceLock<Facts>>,
}
impl Observer {
    pub(super) fn failure(
        &self,
        phase: Phase,
        side: Side,
        error: &(dyn std::error::Error + 'static),
    ) {
        let facts = self.failure.get_or_init(|| {
            let error = NativeError::from_error(error);
            Facts {
                phase,
                side: Some(side),
                close: error.close,
                error,
            }
        });
        self.connection.note_failure(facts.clone());
    }
}

pub(super) struct Request {
    observer: Observer,
    protocol: HttpProtocol,
    phase: Phase,
    injection: Option<Injection>,
    response: Option<(http::StatusCode, StatusSource)>,
    finished: bool,
}
impl Request {
    pub(super) fn new(protocol: HttpProtocol, connection: Connection) -> Self {
        Self {
            observer: Observer {
                connection,
                failure: Arc::new(OnceLock::new()),
            },
            protocol,
            phase: Phase::RequestHeaders,
            injection: None,
            response: None,
            finished: false,
        }
    }
    pub(super) fn observer(&self) -> Observer {
        self.observer.clone()
    }
    pub(super) fn phase(&mut self, phase: Phase) {
        self.phase = phase;
    }
    pub(super) fn injection(&mut self, injection: Injection) {
        self.injection = Some(injection);
        if matches!(injection, Injection::Failed) {
            self.observer.connection.injection_failed();
        }
    }
    pub(super) fn failure(
        &self,
        phase: Phase,
        side: Side,
        error: &(dyn std::error::Error + 'static),
    ) {
        self.observer.failure(phase, side, error);
    }
    pub(super) fn response(mut self, reply: super::Reply) -> Response<ResponseBody> {
        let super::Reply { response, source } = reply;
        self.response = Some((response.status(), source));
        self.phase(Phase::ResponseBody);
        response.map(|inner| {
            if inner.is_end_stream() {
                self.finish(Outcome::Completed);
            }
            ResponseBody {
                inner,
                request: self,
            }
        })
    }
    fn finish(&mut self, outcome: Outcome) {
        if self.finished {
            return;
        }
        self.finished = true;
        let facts = self.observer.failure.get().cloned().unwrap_or(Facts {
            phase: self.phase,
            ..Facts::default()
        });
        if matches!(outcome, Outcome::Aborted) {
            self.observer.connection.request_aborted(facts.phase);
        }
        pplx_space_metrics::gateway::http_request(
            self.protocol.as_str(),
            outcome.as_str(),
            self.response.map(|(status, _)| status.as_u16()),
            self.response.map(|(_, source)| source.as_str()),
            self.injection.map(Injection::as_str),
            &facts.tags(),
        );
    }
}
impl Drop for Request {
    fn drop(&mut self) {
        self.finish(Outcome::Aborted);
    }
}

pub(super) struct ResponseBody {
    inner: super::Body,
    request: Request,
}
impl Body for ResponseBody {
    type Data = hyper::body::Bytes;
    type Error = <super::Body as Body>::Error;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let result = Pin::new(&mut self.inner).poll_frame(cx);
        match &result {
            Poll::Ready(Some(Err(error))) => {
                self.request
                    .failure(Phase::ResponseBody, Side::Upstream, error.as_ref());
                self.request.finish(Outcome::Aborted);
            }
            Poll::Ready(None) => self.request.finish(Outcome::Completed),
            Poll::Ready(Some(Ok(_))) if self.inner.is_end_stream() => {
                self.request.finish(Outcome::Completed)
            }
            _ => {}
        }
        result
    }
    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }
    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}
