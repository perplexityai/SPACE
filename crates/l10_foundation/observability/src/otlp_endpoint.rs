//! The shared plaintext OTLP endpoint rule.
//!
//! Rust metrics, Rust traces, and the Go shim accept the same endpoint shape,
//! so one Collector address works for every producer and a typo is rejected
//! the same way everywhere instead of failing later inside one exporter.
//! `docs/observability.md` states the rule for operators; this is where the
//! Rust half of it lives.

/// Why an endpoint cannot be used.
///
/// Each message describes the requirement rather than quoting the value: an
/// endpoint can carry credentials in its userinfo, and the caller's log line
/// names the setting instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum EndpointRejection {
    #[error("must be a URL")]
    Unparsable,
    #[error("must use the plaintext http:// scheme")]
    Scheme,
    #[error("must include a host")]
    MissingHost,
    #[error("must not embed credentials")]
    Userinfo,
    #[error("must use a port between 1 and 65535")]
    Port,
    #[error("must not include a path")]
    Path,
    #[error("must not include a query or fragment")]
    QueryOrFragment,
}

/// Accept `http://host[:port]`, optionally with a bare `/` path.
///
/// An omitted port is left to the exporter's scheme default, but a port that
/// is present must be usable. Reachability is not checked here: an endpoint
/// that parses but has nothing listening is a runtime condition.
pub(crate) fn validate(endpoint: &str) -> Result<(), EndpointRejection> {
    // `http::Uri` has no fragment concept, so reject the marker on the raw
    // string rather than depending on how the parser treats it.
    if endpoint.contains('#') {
        return Err(EndpointRejection::QueryOrFragment);
    }
    let uri = endpoint
        .parse::<http::Uri>()
        .map_err(|_| EndpointRejection::Unparsable)?;
    if uri.scheme_str() != Some("http") {
        return Err(EndpointRejection::Scheme);
    }
    let Some(authority) = uri.authority() else {
        return Err(EndpointRejection::MissingHost);
    };
    // `Authority::host` hides userinfo, so look for the delimiter directly.
    if authority.as_str().contains('@') {
        return Err(EndpointRejection::Userinfo);
    }
    if authority.host().is_empty() {
        return Err(EndpointRejection::MissingHost);
    }
    if let Some(port) = port_text(authority.as_str()) {
        // `u16::from_str` accepts a leading `+`, so `:+1` parsed as port 1.
        // The Go and Python producers of this same rule reject it, and no
        // resolver reads it, so require plain digits: one address has to read
        // the same everywhere.
        let digits = port.bytes().all(|byte| byte.is_ascii_digit());
        if !digits || !matches!(port.parse::<u16>(), Ok(1..=u16::MAX)) {
            return Err(EndpointRejection::Port);
        }
    }
    if !matches!(uri.path(), "" | "/") {
        return Err(EndpointRejection::Path);
    }
    if uri.query().is_some() {
        return Err(EndpointRejection::QueryOrFragment);
    }
    Ok(())
}

/// The port text an authority carries, if it carries the delimiter at all.
///
/// `Uri::port_u16` answers `None` for an absent port, for text it could not
/// read as a port, and for an empty one, so `http://collector:abc` and
/// `http://collector:` look exactly like `http://collector`. Reading the
/// authority directly tells them apart. An IPv6 literal keeps its own colons
/// inside brackets, so the delimiter is the colon after the closing bracket.
fn port_text(authority: &str) -> Option<&str> {
    match authority.rsplit_once(']') {
        Some((_, after_bracket)) => after_bracket.strip_prefix(':'),
        None => authority.rsplit_once(':').map(|(_, port)| port),
    }
}

#[cfg(test)]
mod tests {
    use super::{EndpointRejection, validate};

    #[test]
    fn accepts_plaintext_collector_addresses() {
        for endpoint in [
            "http://localhost:4317",
            "http://collector.observability.svc.cluster.local:4317",
            // A bare root path is the same address as none.
            "http://collector:4317/",
            "http://collector",
            "http://127.0.0.1:4317",
            // An IPv6 literal's own colons must not read as a port delimiter.
            "http://[::1]:4317",
            "http://[::1]",
            "http://collector:65535",
            "http://collector:1",
        ] {
            assert_eq!(validate(endpoint), Ok(()), "{endpoint}");
        }
    }

    #[test]
    fn rejects_unusable_endpoints() {
        for (endpoint, expected) in [
            ("", EndpointRejection::Unparsable),
            ("collector:4317", EndpointRejection::Scheme),
            ("https://collector:4317", EndpointRejection::Scheme),
            ("http://:", EndpointRejection::MissingHost),
            ("http://user:pw@collector:4317", EndpointRejection::Userinfo),
            ("http://collector:0", EndpointRejection::Port),
            // `Uri::port_u16` reports None for all three, so each used to be
            // indistinguishable from an address carrying no port at all.
            ("http://collector:65536", EndpointRejection::Port),
            ("http://collector:abc", EndpointRejection::Port),
            ("http://collector:", EndpointRejection::Port),
            // `u16::from_str` accepts a leading sign; the Go and Python
            // producers of this rule do not, so neither does this one.
            ("http://collector:+1", EndpointRejection::Port),
            ("http://collector:+65535", EndpointRejection::Port),
            ("http://collector:4317/v1/metrics", EndpointRejection::Path),
            // An empty query or fragment is still a query or fragment.
            ("http://collector:4317?", EndpointRejection::QueryOrFragment),
            ("http://collector:4317#", EndpointRejection::QueryOrFragment),
            (
                "http://collector:4317/?q",
                EndpointRejection::QueryOrFragment,
            ),
            (
                "http://collector:4317/path?q#f",
                EndpointRejection::QueryOrFragment,
            ),
        ] {
            assert_eq!(validate(endpoint), Err(expected), "{endpoint}");
        }
    }

    /// The messages reach operator logs, so they must describe the requirement
    /// without repeating an endpoint that may embed credentials.
    #[test]
    fn rejection_messages_omit_the_value() {
        let Err(rejection) = validate("http://user:pw@collector:4317") else {
            panic!("userinfo must be rejected");
        };
        let message = rejection.to_string();
        assert!(!message.contains("pw"), "{message}");
        assert!(!message.contains("collector"), "{message}");
    }
}
