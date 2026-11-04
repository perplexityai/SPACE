//! End-to-end reads through the **real** flagd provider against the
//! checked-in reference configuration.
//!
//! The unit tests drive a hand-written OpenFeature provider, which answers
//! with whatever `StructValue` the test hands it. That proves the adapter's
//! own conversion and nothing about the backend's, so a backend that mangles a
//! value on the way out is invisible to every one of them. These tests close
//! that gap: flagd's File resolver needs no daemon and no network, so the
//! whole path — reference JSON on disk, flagd's parser, its resolver, the
//! adapter — runs in-process.

#![cfg(feature = "flagd")]

use pplx_space_feature_flags::openfeature::flagd;
use pplx_space_feature_flags::{
    Attributes, FeatureFlagClient, JsonFlag, SPACE_API_CLIENT_CONCURRENCY_LIMITER_FLAG,
    SPACE_API_CLIENT_CONCURRENCY_QUOTAS_FLAG, SPACE_CLUSTER_RATE_LIMITER_FLAG,
    SPACE_NET_GATEWAY_KNOWN_EGRESS_DOMAINS_FLAG,
};

const REFERENCE_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/reference/flagd-space-flags.json"
);

/// Build a client backed by real flagd reading `path`.
fn flagd_client<R>(path: &str, body: impl FnOnce(&FeatureFlagClient) -> R) -> R {
    let options = open_feature_flagd::FlagdOptions {
        resolver_type: open_feature_flagd::ResolverType::File,
        source_configuration: Some(path.to_owned()),
        selector: None,
        ..Default::default()
    };
    let provider =
        flagd::in_process(options).expect("the file resolver should start from a flag file");
    body(&FeatureFlagClient::from_provider(provider))
}

/// The reference file is a configuration flagd actually accepts, and its
/// scalar flags resolve to the values it declares.
///
/// The guards in `flagd_reference.rs` read the file as JSON; they cannot tell
/// whether flagd's own parser accepts it, which is a different and stricter
/// question — a `targeting` expression it rejects, or a variant type it
/// refuses, fails only here.
#[test]
fn scalar_flags_resolve_through_real_flagd() {
    use std::ffi::OsStr;
    if !pplx_space_test_env::enter_test_process(&[
        ("FLAGD_RESOLVER", Some(OsStr::new("file"))),
        (
            "FLAGD_OFFLINE_FLAG_SOURCE_PATH",
            Some(OsStr::new(REFERENCE_PATH)),
        ),
        ("FLAGD_HOST", None),
        ("FLAGD_PORT", None),
        ("FLAGD_SOURCE_SELECTOR", None),
    ]) {
        return;
    }
    let provider =
        flagd::in_process_from_env().expect("environment-configured file resolver starts");
    let client = FeatureFlagClient::from_provider(provider);
    // The reference declares each flag at the same value its declaration
    // falls back to, so a read that returned the value would prove
    // nothing — the fallback path returns it too. The reason-carrying
    // reads are what separate them: `Ok` can only come from an assignment
    // flagd served out of the file.
    assert_eq!(
        client.resolve_bool(
            SPACE_API_CLIENT_CONCURRENCY_LIMITER_FLAG,
            "space-cluster",
            Attributes::default()
        ),
        Ok(true),
        "the client concurrency limiter should resolve on from the reference"
    );
    assert_eq!(
        client.resolve_bool(
            SPACE_CLUSTER_RATE_LIMITER_FLAG,
            "space-cluster",
            Attributes::default()
        ),
        Ok(true),
        "the cluster rate limiter should resolve on from the reference"
    );
}

/// flagd 0.2.2's in-process resolvers flatten any **nested** array or object
/// inside a JSON flag's variant into a JSON *string*.
///
/// Both in-process resolvers do it in the same place — the catch-all arm of
/// their struct conversion is `_ => Value::String(v.to_string())` — so it
/// applies to the file resolver here and to the gRPC one a cluster would use.
/// Scalar members survive; anything one level deeper does not.
///
/// The fixture is written here rather than taken from the reference file
/// because the reference's JSON flags deliberately default to inert, empty
/// objects; resolving one of those would say nothing about nesting either way.
/// A scalar sibling sits next to each nested value so the assertion
/// distinguishes "flagd mangled the nesting" from "flagd failed to resolve the
/// flag at all".
///
/// The adapter deliberately does not undo this. By the time a value reaches
/// it, a stringified member is indistinguishable from a flag whose author
/// meant a string, so re-parsing would corrupt the latter to rescue the
/// former. Each *reader* decodes its own field instead, where the field's type
/// is known — see `pplx-space-sbx-mgr`'s known-egress reader and
/// `pplx-space-api`'s quota overrides.
///
/// This test pins the upstream behaviour those workarounds exist for, so the
/// limitation is a demonstrated fact with a named cause rather than a claim in
/// a comment — and so an upstream fix announces itself by failing here, which
/// is the signal to delete them.
#[test]
fn nested_json_is_stringified_by_flagd_in_process_resolvers() {
    // Declared here rather than in the crate root: it exists to probe flagd's
    // conversion, and nothing in SPACE reads it.
    const NESTED_PROBE: JsonFlag = JsonFlag::new("space-nested-probe");

    let directory = tempfile::tempdir().expect("a temp dir for the flag file");
    let path = directory.path().join("nested-flags.json");
    std::fs::write(
        &path,
        r#"{
          "flags": {
            "space-nested-probe": {
              "state": "ENABLED",
              "defaultVariant": "nested",
              "variants": {
                "nested": {
                  "scalar": 8,
                  "an_object": { "inner": 1 },
                  "an_array": ["a", "b"]
                }
              },
              "targeting": {}
            }
          }
        }"#,
    )
    .expect("write the flag file");

    flagd_client(path.to_str().expect("utf-8 temp path"), |client| {
        let value = client
            .resolve_json(NESTED_PROBE, "subject", Attributes::default())
            .expect("the probe flag should resolve as JSON");

        assert_eq!(
            value.get("scalar").and_then(serde_json::Value::as_i64),
            Some(8),
            "a scalar member should survive intact; got {value}"
        );
        assert_eq!(
            value.get("an_object").and_then(serde_json::Value::as_str),
            Some(r#"{"inner":1}"#),
            "a nested object should arrive stringified; got {value}"
        );
        assert_eq!(
            value.get("an_array").and_then(serde_json::Value::as_str),
            Some(r#"["a","b"]"#),
            "a nested array should arrive stringified; got {value}"
        );
    });
}

/// The two SPACE flags whose shape needs nesting, in the exact encoding their
/// readers receive from flagd.
///
/// `space-net-gateway-known-egress-domains` carries an array of domain names
/// and `space-api-client-concurrency-quotas` a map of per-client caps; both
/// arrive stringified. This pins the wire form each reader's decoding is
/// written against, so a change in what flagd emits surfaces here rather than
/// as a flag that quietly stops applying in a cluster.
#[test]
fn the_two_nesting_flags_arrive_stringified_from_flagd() {
    let directory = tempfile::tempdir().expect("a temp dir for the flag file");
    let path = directory.path().join("populated-flags.json");
    std::fs::write(
        &path,
        format!(
            r#"{{
              "flags": {{
                "{domains_flag}": {{
                  "state": "ENABLED",
                  "defaultVariant": "populated",
                  "variants": {{ "populated": {{ "domains": ["egress-proxy.example.test"] }} }},
                  "targeting": {{}}
                }},
                "{quotas_flag}": {{
                  "state": "ENABLED",
                  "defaultVariant": "populated",
                  "variants": {{ "populated": {{ "unspecified": 8, "overrides": {{ "acme": 32 }} }} }},
                  "targeting": {{}}
                }}
              }}
            }}"#,
            domains_flag = SPACE_NET_GATEWAY_KNOWN_EGRESS_DOMAINS_FLAG,
            quotas_flag = SPACE_API_CLIENT_CONCURRENCY_QUOTAS_FLAG,
        ),
    )
    .expect("write the flag file");

    flagd_client(path.to_str().expect("utf-8 temp path"), |client| {
        let domains = client
            .resolve_json(
                SPACE_NET_GATEWAY_KNOWN_EGRESS_DOMAINS_FLAG,
                "sbx-1",
                Attributes::default(),
            )
            .expect("the known-egress flag should resolve as JSON");
        assert!(
            domains
                .get("domains")
                .is_some_and(serde_json::Value::is_string),
            "the domain list should arrive stringified; got {domains}"
        );

        let quotas = client
            .resolve_json(
                SPACE_API_CLIENT_CONCURRENCY_QUOTAS_FLAG,
                "space-cluster",
                Attributes::default(),
            )
            .expect("the quota flag should resolve as JSON");
        assert_eq!(
            quotas
                .get("unspecified")
                .and_then(serde_json::Value::as_i64),
            Some(8),
            "the scalar cap should survive; got {quotas}"
        );
        assert!(
            quotas
                .get("overrides")
                .is_some_and(serde_json::Value::is_string),
            "the per-client override map should arrive stringified; got {quotas}"
        );
    });
}
