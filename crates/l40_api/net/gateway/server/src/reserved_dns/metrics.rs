//! Reserved-DNS outcome metrics and failure diagnostics.

use std::time::Duration;

use hickory_resolver::proto::op::ResponseCode;
use hickory_resolver::proto::rr::RecordType;

use super::ResolutionFailure::{InvalidRequest, Lookup, Refused, UdpCapacity};
use super::ResolverFailure::{
    Busy, Cancelled, Capacity, LookupDeadline, Malformed, Sanitization, Timeout, Transport,
    Upstream,
};
use super::{
    ATTEMPT_TIMEOUT, GuestRequest, RequestFailure, ReservedResolver, ResolutionAnswer,
    ResolutionFailure, SanitizationFailure, TOTAL_LOOKUP_TIMEOUT,
};

pub(super) fn record_request_outcome(
    sandbox_id: &str,
    resolver: ReservedResolver,
    request: &Result<GuestRequest, RequestFailure>,
    result: &Result<ResolutionAnswer, ResolutionFailure>,
    duration: Duration,
) {
    let qtype = match request {
        Ok(request) => Some(request.question.query_type),
        Err(failure) => failure.qtype,
    };
    let (outcome, reason, upstream_rcode) = match result {
        Ok(ResolutionAnswer::LocalNoData) => ("nodata", "local_nodata", "none"),
        Ok(ResolutionAnswer::Upstream(answer)) if answer.code == ResponseCode::NoError => (
            if answer
                .answers
                .iter()
                .any(|record| Some(record.record_type()) == qtype)
            {
                "positive"
            } else {
                "nodata"
            },
            "upstream",
            "noerror",
        ),
        Ok(ResolutionAnswer::Upstream(answer)) => (
            rcode_label(answer.code),
            "upstream",
            rcode_label(answer.code),
        ),
        Err(error) => (
            if matches!(error, Lookup(Cancelled)) {
                "cancelled"
            } else {
                rcode_label(error.response_code())
            },
            error.as_str(),
            match error {
                Lookup(Upstream(code) | Sanitization(SanitizationFailure::ResponseCode(code))) => {
                    rcode_label(*code)
                }
                _ => "none",
            },
        ),
    };
    let resolver = resolver.as_str();
    let qtype = match qtype {
        Some(RecordType::A) => "a",
        Some(RecordType::AAAA) => "aaaa",
        Some(_) => "other",
        None => "unknown",
    };
    pplx_space_metrics::gateway::reserved_dns_request_outcome(
        resolver,
        qtype,
        outcome,
        reason,
        upstream_rcode,
        duration.as_secs_f64() * 1000.0,
    );

    let expected_failure = match result {
        Ok(ResolutionAnswer::LocalNoData) => return,
        Ok(ResolutionAnswer::Upstream(answer)) if answer.code == ResponseCode::NoError => return,
        Ok(ResolutionAnswer::Upstream(answer)) => answer.code == ResponseCode::NXDomain,
        Err(InvalidRequest(_) | Refused | Lookup(Cancelled)) => true,
        Err(_) => false,
    };
    let (query_id, query_name) = match request {
        Ok(request) => (request.id, Some(request.domain.as_str())),
        Err(failure) => (failure.id, None),
    };
    let timeout_source = match result {
        Err(Lookup(LookupDeadline)) => Some("total_lookup"),
        Err(Lookup(Timeout)) => Some("hickory"),
        _ => None,
    };

    // These outcomes precede reply delivery. A generated SERVFAIL does not
    // establish guest receipt, and Hickory's Timeout has erased the I/O phase.
    macro_rules! log_failure {
        ($level:expr) => {
            tracing::event!(
                $level,
                sandbox_id,
                gateway_pid = std::process::id(),
                git_sha = pplx_space_observability::git_sha(),
                query_id,
                query_name,
                qtype,
                resolver,
                outcome,
                reason,
                upstream_rcode,
                timeout_source,
                elapsed_ms = duration.as_secs_f64() * 1000.0,
                attempt_timeout_ms = ATTEMPT_TIMEOUT.as_secs_f64() * 1000.0,
                total_lookup_timeout_ms = TOTAL_LOOKUP_TIMEOUT.as_secs_f64() * 1000.0,
                stage = "resolution",
                error = result.as_ref().err().map(tracing::field::display),
                error_detail = result.as_ref().err().map(tracing::field::debug),
                "reserved DNS request failed"
            );
        };
    }
    if expected_failure {
        log_failure!(tracing::Level::DEBUG);
    } else {
        log_failure!(tracing::Level::WARN);
    }
}

impl ResolutionFailure {
    fn as_str(&self) -> &'static str {
        match self {
            InvalidRequest(_) => "invalid_request",
            Refused => "policy_denied",
            UdpCapacity => "udp_capacity",
            Lookup(Capacity) => "lookup_capacity",
            Lookup(Busy) => "resolver_busy",
            Lookup(Timeout | LookupDeadline) => "timeout",
            Lookup(Transport(_)) => "transport",
            Lookup(Malformed) => "malformed_answer",
            Lookup(Sanitization(error)) => error.as_str(),
            Lookup(Cancelled) => "cancelled",
            Lookup(Upstream(ResponseCode::ServFail)) => "upstream",
            Lookup(Upstream(_)) => "upstream_rcode_translation",
        }
    }
}

impl SanitizationFailure {
    fn as_str(self) -> &'static str {
        match self {
            Self::QuestionMismatch => "sanitization_question_mismatch",
            Self::ResponseCode(_) => "sanitization_response_code",
            Self::ConflictingCname => "sanitization_cname_conflict",
            Self::CnameCycle => "sanitization_cname_cycle",
            Self::CnameHopLimit => "sanitization_cname_hop_limit",
            Self::AddressDenied => "sanitization_address_denied",
        }
    }
}

fn rcode_label(code: ResponseCode) -> &'static str {
    match code {
        ResponseCode::NoError => "noerror",
        ResponseCode::NXDomain => "nxdomain",
        ResponseCode::ServFail => "servfail",
        ResponseCode::Refused => "refused",
        ResponseCode::NotImp => "notimp",
        ResponseCode::FormErr => "formerr",
        _ => "other",
    }
}
