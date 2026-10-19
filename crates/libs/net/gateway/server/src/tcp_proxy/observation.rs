//! Facts about a connection, without judging whether its owner was degraded.
//! Error fields are taken from typed library values, never formatted messages.

use pplx_space_metrics::gateway;
use std::{
    error::Error,
    io,
    sync::{Arc, Mutex},
};

#[derive(Clone, Copy, Debug)]
pub(super) enum Side {
    Downstream,
    Upstream,
}
impl Side {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Downstream => "downstream",
            Self::Upstream => "upstream",
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) enum Close {
    Local,
    Remote,
}
impl Close {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Remote => "remote",
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) enum Phase {
    #[default]
    Policy,
    ClientHello,
    Admission,
    Certificate,
    DownstreamTls,
    HttpHeaders,
    Http,
    UpstreamConnect,
    UpstreamTls,
    RequestHeaders,
    RequestBody,
    ResponseHeaders,
    ResponseBody,
    Upgrade,
    Relay,
    ReservedDns,
}
impl Phase {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Policy => "policy",
            Self::ClientHello => "client_hello",
            Self::Admission => "admission",
            Self::Certificate => "certificate",
            Self::DownstreamTls => "downstream_tls",
            Self::HttpHeaders => "http_headers",
            Self::Http => "http",
            Self::UpstreamConnect => "upstream_connect",
            Self::UpstreamTls => "upstream_tls",
            Self::RequestHeaders => "request_headers",
            Self::RequestBody => "request_body",
            Self::ResponseHeaders => "response_headers",
            Self::ResponseBody => "response_body",
            Self::Upgrade => "upgrade",
            Self::Relay => "relay",
            Self::ReservedDns => "reserved_dns",
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(super) struct NativeError {
    pub error_type: Option<&'static str>,
    pub io_error: Option<String>,
    pub errno: Option<i32>,
    pub tls_alert: Option<String>,
    pub rustls_error: Option<&'static str>,
    pub hyper_error: Option<&'static str>,
    pub http2_error_code: Option<u32>,
    pub close: Option<Close>,
}

impl NativeError {
    pub(super) fn from_error(error: &(dyn Error + 'static)) -> Self {
        let mut facts = Self {
            error_type: Some("_OTHER"),
            ..Self::default()
        };
        let mut cause = Some(error);
        // Source chains are diagnostic, not a reason to trust arbitrary Display
        // implementations or allow a cyclic custom error to stall a connection.
        for _ in 0..16 {
            let Some(error) = cause else { break };
            cause = error.source();
            if let Some(error) = error.downcast_ref::<io::Error>() {
                facts.error_type = Some("std::io::Error");
                facts.io_error = Some(format!("{:?}", error.kind()));
                facts.errno = error.raw_os_error();
                if let Some(inner) = error.get_ref() {
                    cause = Some(inner);
                }
            } else if let Some(error) = error.downcast_ref::<rustls::Error>() {
                facts.error_type = Some("rustls::Error");
                facts.rustls_error = rustls_category(error);
                if let rustls::Error::AlertReceived(alert) = error {
                    facts.tls_alert = Some(format!("{alert:?}"));
                    facts.close = Some(Close::Remote);
                }
            } else if let Some(error) = error.downcast_ref::<hyper::Error>() {
                facts.error_type = Some("hyper::Error");
                facts.hyper_error = Some(if error.is_canceled() {
                    "canceled"
                } else {
                    super::tls_http::http_reason(error)
                });
            } else if let Some(error) = error.downcast_ref::<h2::Error>() {
                facts.error_type = Some("h2::Error");
                facts.http2_error_code = error.reason().map(u32::from);
                // A stream reset does not close its HTTP/2 connection.
                if error.is_go_away() {
                    if error.is_remote() {
                        facts.close = Some(Close::Remote);
                    } else if error.is_library() {
                        facts.close = Some(Close::Local);
                    }
                }
            }
        }
        facts
    }

    pub(super) fn tags(&self) -> gateway::NativeError<'_> {
        gateway::NativeError {
            error_type: self.error_type,
            io_error: self.io_error.as_deref(),
            errno: self.errno,
            tls_alert: self.tls_alert.as_deref(),
            rustls_error: self.rustls_error,
            hyper_error: self.hyper_error,
            http2_error_code: self.http2_error_code,
        }
    }
}

fn rustls_category(error: &rustls::Error) -> Option<&'static str> {
    use rustls::Error::*;
    Some(match error {
        InappropriateMessage { .. } => "InappropriateMessage",
        InappropriateHandshakeMessage { .. } => "InappropriateHandshakeMessage",
        InvalidEncryptedClientHello(_) => "InvalidEncryptedClientHello",
        InvalidMessage(_) => "InvalidMessage",
        NoCertificatesPresented => "NoCertificatesPresented",
        UnsupportedNameType => "UnsupportedNameType",
        DecryptError => "DecryptError",
        EncryptError => "EncryptError",
        PeerIncompatible(_) => "PeerIncompatible",
        PeerMisbehaved(_) => "PeerMisbehaved",
        AlertReceived(_) => "AlertReceived",
        InvalidCertificate(_) => "InvalidCertificate",
        InvalidCertRevocationList(_) => "InvalidCertRevocationList",
        General(_) => "General",
        FailedToGetCurrentTime => "FailedToGetCurrentTime",
        FailedToGetRandomBytes => "FailedToGetRandomBytes",
        HandshakeNotComplete => "HandshakeNotComplete",
        PeerSentOversizedRecord => "PeerSentOversizedRecord",
        NoApplicationProtocol => "NoApplicationProtocol",
        BadMaxFragmentSize => "BadMaxFragmentSize",
        InconsistentKeys(_) => "InconsistentKeys",
        Other(_) => "Other",
        _ => return None,
    })
}

#[derive(Clone, Debug, Default)]
pub(super) struct Facts {
    pub phase: Phase,
    pub side: Option<Side>,
    pub close: Option<Close>,
    pub error: NativeError,
}
impl Facts {
    pub(super) fn tags(&self) -> gateway::ConnectionObservation<'_> {
        gateway::ConnectionObservation {
            phase: self.phase.as_str(),
            side: self.side.map(Side::as_str),
            close: self.close.map(Close::as_str),
            error: self.error.tags(),
        }
    }
}

#[derive(Default)]
struct State {
    phase: Phase,
    first_failure: Option<Facts>,
    first_abort: Option<Phase>,
    aborted_requests: u64,
    failed_injections: u64,
    first_close: Option<(Phase, Side, Close)>,
}

#[derive(Clone, Default)]
pub(super) struct Connection(Arc<Mutex<State>>);
impl Connection {
    pub(super) fn phase(&self, phase: Phase) {
        self.0
            .lock()
            .expect("connection observation lock poisoned")
            .phase = phase;
    }
    pub(super) fn http_request(&self) {
        let mut state = self.0.lock().expect("connection observation lock poisoned");
        // Another HTTP/2 request must not overwrite an in-flight origin setup.
        if matches!(state.phase, Phase::HttpHeaders) {
            state.phase = Phase::Http;
        }
    }
    pub(super) fn current_phase(&self) -> Phase {
        self.0
            .lock()
            .expect("connection observation lock poisoned")
            .phase
    }
    pub(super) fn failure(&self, phase: Phase, side: Option<Side>, error: &(dyn Error + 'static)) {
        let error = NativeError::from_error(error);
        self.note_failure(Facts {
            phase,
            side,
            close: error.close,
            error,
        });
    }
    pub(super) fn note_failure(&self, facts: Facts) {
        self.0
            .lock()
            .expect("connection observation lock poisoned")
            .first_failure
            .get_or_insert(facts);
    }
    pub(super) fn closed(&self, side: Side, close: Close) {
        let mut state = self.0.lock().expect("connection observation lock poisoned");
        let phase = state.phase;
        state.first_close.get_or_insert((phase, side, close));
    }
    pub(super) fn injection_failed(&self) {
        self.0
            .lock()
            .expect("connection observation lock poisoned")
            .failed_injections += 1;
    }
    pub(super) fn request_aborted(&self, phase: Phase) {
        let mut state = self.0.lock().expect("connection observation lock poisoned");
        state.aborted_requests += 1;
        state.first_abort.get_or_insert(phase);
    }
    pub(super) fn record(&self, sandbox_id: &str, status: &str, detail: &str, failed: bool) {
        // Snapshot once so the counter and log describe the same observations.
        let (facts, aborted_requests, failed_injections) = {
            let state = self.0.lock().expect("connection observation lock poisoned");
            // A native failure retains its own leg and phase; an earlier EOF
            // does not establish the cause or initiator of that failure.
            let facts = match (&state.first_failure, state.first_abort, &state.first_close) {
                (Some(failure), _, _) => failure.clone(),
                (None, Some(phase), _) => Facts {
                    phase,
                    ..Facts::default()
                },
                (None, None, Some((phase, side, close))) => Facts {
                    phase: *phase,
                    side: Some(*side),
                    close: Some(*close),
                    ..Facts::default()
                },
                (None, None, None) => Facts {
                    phase: state.phase,
                    ..Facts::default()
                },
            };
            (facts, state.aborted_requests, state.failed_injections)
        };
        gateway::tcp_connection(status, detail, &facts.tags());
        if failed
            || facts.error.error_type.is_some()
            || aborted_requests != 0
            || failed_injections != 0
        {
            // One unsampled event from the same terminal path as the counter.
            // Other component logs are diagnostics, not connection counts.
            tracing::info!(
                telemetry_type = "gateway_connection_failure", sandbox_id,
                status, detail, aborted_requests, failed_injections, phase = facts.phase.as_str(), side = facts.side.map(Side::as_str),
                close = facts.close.map(Close::as_str), error.type = facts.error.error_type,
                io_error = facts.error.io_error.as_deref(), errno = facts.error.errno,
                tls_alert = facts.error.tls_alert.as_deref(), rustls_error = facts.error.rustls_error,
                hyper_error = facts.error.hyper_error, http2_error_code = facts.error.http2_error_code,
                "gateway connection ended with a failure"
            );
        }
    }
}
