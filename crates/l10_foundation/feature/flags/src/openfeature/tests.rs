use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use open_feature::provider::{FeatureProvider, ProviderMetadata, ResolutionDetails};
use open_feature::{
    EvaluationContext, EvaluationContextFieldValue, EvaluationError, EvaluationErrorCode,
    EvaluationResult, StructValue, Value,
};

use super::*;
use crate::{
    AttributeValue, BoolFlag, FeatureFlagClient, FloatFlag, IntegerFlag, JsonFlag, StringFlag,
};

// The fixture flags, each declaring a fallback the fake does *not* answer
// with, so a read that returns the fake's value can only have come from the
// provider.
const BOOL: BoolFlag = BoolFlag::new("space-bool", false);
const INTEGER: IntegerFlag = IntegerFlag::new("space-int", 0);
const FLOAT: FloatFlag = FloatFlag::new("space-float", 1.0);
const STRING: StringFlag = StringFlag::new("space-string", "off");
const JSON: JsonFlag = JsonFlag::new("space-json");

/// What the fake holds for one flag key.
///
/// The shapes mirror flagd's in-process resolver rather than being invented:
/// a key that is absent — or defined but disabled — answers `FlagNotFound`, a
/// key read as the wrong type answers `TypeMismatch`, and anything else the
/// backend hits answers `General`. Testing against a fake that is kinder than
/// the real backend would prove nothing about the real one.
#[derive(Clone, Debug)]
enum FakeFlag {
    Bool(bool),
    Integer(i64),
    Float(f64),
    String(String),
    Json(StructValue),
    Fails(EvaluationErrorCode),
    /// Never answers at all, for the wedged-backend case.
    Hangs,
    /// Occupies the worker's single thread for [`BLOCKING_SPAN`], so requests
    /// behind it queue instead of being served. Blocking rather than sleeping
    /// on purpose: an `.await` would yield the thread and no backlog would
    /// form.
    BlocksThread,
}

/// Comfortably longer than [`super::worker::EVALUATION_TIMEOUT`] so anything
/// queued behind it is certainly past its deadline by the time the worker gets
/// to it.
const BLOCKING_SPAN: std::time::Duration = std::time::Duration::from_millis(1500);

#[derive(Default)]
struct FakeProvider {
    metadata: ProviderMetadata,
    flags: HashMap<String, FakeFlag>,
    /// What the fake reports for its own readiness. A constructor rather than
    /// a value because `ProviderStatus` is neither `Clone` nor `Copy` and
    /// `status` hands one back by value. `None` leaves the SPI's own default,
    /// which is `Ready`.
    status: Option<fn() -> ProviderStatus>,
    /// Every context the provider was handed, so a test can assert what the
    /// adapter converted rather than only what came back out.
    seen: Arc<Mutex<Vec<EvaluationContext>>>,
    /// Evaluations currently parked in [`FakeFlag::Hangs`], and the most that
    /// were ever parked at once. The pair is what makes "the worker cancelled
    /// it" checkable: the peak proves an evaluation really started, the live
    /// count proves it did not survive.
    hanging: Arc<AtomicUsize>,
    peak_hanging: Arc<AtomicUsize>,
}

/// Counts one parked evaluation for as long as the future is alive. Cancelling
/// that future drops this, which is precisely the event under test.
struct Hanging(Arc<AtomicUsize>);

impl Drop for Hanging {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl FakeProvider {
    fn with(flags: impl IntoIterator<Item = (&'static str, FakeFlag)>) -> Self {
        Self {
            metadata: ProviderMetadata::new("fake"),
            flags: flags
                .into_iter()
                .map(|(key, flag)| (key.to_owned(), flag))
                .collect(),
            ..Self::default()
        }
    }

    fn contexts(&self) -> Arc<Mutex<Vec<EvaluationContext>>> {
        Arc::clone(&self.seen)
    }

    fn hang_counters(&self) -> (Arc<AtomicUsize>, Arc<AtomicUsize>) {
        (Arc::clone(&self.hanging), Arc::clone(&self.peak_hanging))
    }

    /// Async so `Hangs` can be expressed here rather than in each of the five
    /// resolve methods: one place to add a behaviour, and one place to fix
    /// when the SDK grows a sixth resolve.
    async fn lookup(
        &self,
        flag_key: &str,
        context: &EvaluationContext,
    ) -> Result<FakeFlag, EvaluationError> {
        // Recover from a poisoned lock rather than panicking: this runs on the
        // worker thread, where a panic strands the caller on a disconnect and
        // reports as "the worker did not answer" instead of as the real
        // failure.
        self.seen
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(context.clone());
        match self.flags.get(flag_key) {
            Some(FakeFlag::BlocksThread) => {
                std::thread::sleep(BLOCKING_SPAN);
                Ok(FakeFlag::Bool(true))
            }
            Some(FakeFlag::Hangs) => {
                let live = self.hanging.fetch_add(1, Ordering::SeqCst) + 1;
                self.peak_hanging.fetch_max(live, Ordering::SeqCst);
                let _parked = Hanging(Arc::clone(&self.hanging));
                std::future::pending().await
            }
            Some(FakeFlag::Fails(code)) => Err(EvaluationError::builder()
                .code(code.clone())
                .message("the fake backend was told to fail")
                .build()),
            Some(flag) => Ok(flag.clone()),
            None => Err(EvaluationError::builder()
                .code(EvaluationErrorCode::FlagNotFound)
                .message(format!("flag {flag_key} not found"))
                .build()),
        }
    }
}

fn type_mismatch(flag_key: &str, wanted: &str) -> EvaluationError {
    EvaluationError::builder()
        .code(EvaluationErrorCode::TypeMismatch)
        .message(format!("flag {flag_key} is not {wanted}"))
        .build()
}

#[async_trait::async_trait]
impl FeatureProvider for FakeProvider {
    fn metadata(&self) -> &ProviderMetadata {
        &self.metadata
    }

    fn status(&self) -> ProviderStatus {
        self.status.map_or(ProviderStatus::Ready, |status| status())
    }

    async fn resolve_bool_value(
        &self,
        flag_key: &str,
        context: &EvaluationContext,
    ) -> EvaluationResult<ResolutionDetails<bool>> {
        match self.lookup(flag_key, context).await? {
            FakeFlag::Bool(value) => Ok(ResolutionDetails::new(value)),
            _ => Err(type_mismatch(flag_key, "a boolean")),
        }
    }

    async fn resolve_int_value(
        &self,
        flag_key: &str,
        context: &EvaluationContext,
    ) -> EvaluationResult<ResolutionDetails<i64>> {
        match self.lookup(flag_key, context).await? {
            FakeFlag::Integer(value) => Ok(ResolutionDetails::new(value)),
            _ => Err(type_mismatch(flag_key, "an integer")),
        }
    }

    async fn resolve_float_value(
        &self,
        flag_key: &str,
        context: &EvaluationContext,
    ) -> EvaluationResult<ResolutionDetails<f64>> {
        match self.lookup(flag_key, context).await? {
            FakeFlag::Float(value) => Ok(ResolutionDetails::new(value)),
            _ => Err(type_mismatch(flag_key, "a float")),
        }
    }

    async fn resolve_string_value(
        &self,
        flag_key: &str,
        context: &EvaluationContext,
    ) -> EvaluationResult<ResolutionDetails<String>> {
        match self.lookup(flag_key, context).await? {
            FakeFlag::String(value) => Ok(ResolutionDetails::new(value)),
            _ => Err(type_mismatch(flag_key, "a string")),
        }
    }

    async fn resolve_struct_value(
        &self,
        flag_key: &str,
        context: &EvaluationContext,
    ) -> EvaluationResult<ResolutionDetails<StructValue>> {
        match self.lookup(flag_key, context).await? {
            FakeFlag::Json(value) => Ok(ResolutionDetails::new(value)),
            _ => Err(type_mismatch(flag_key, "a structure")),
        }
    }
}

fn provider(flags: impl IntoIterator<Item = (&'static str, FakeFlag)>) -> OpenFeatureProvider {
    OpenFeatureProvider::new(FakeProvider::with(flags)).expect("the worker should start")
}

/// A provider that answers [`BOOL`] and reports `status` as its readiness.
fn provider_reporting(status: fn() -> ProviderStatus) -> OpenFeatureProvider {
    OpenFeatureProvider::new(FakeProvider {
        status: Some(status),
        ..FakeProvider::with([("space-bool", FakeFlag::Bool(true))])
    })
    .expect("the worker should start")
}

/// A client plus the log of contexts its provider was handed, for the tests
/// that assert on what the adapter converted rather than on what came back.
fn provider_with_contexts(
    flags: impl IntoIterator<Item = (&'static str, FakeFlag)>,
) -> (FeatureFlagClient, Arc<Mutex<Vec<EvaluationContext>>>) {
    let fake = FakeProvider::with(flags);
    let contexts = fake.contexts();
    let client = FeatureFlagClient::from_provider(
        OpenFeatureProvider::new(fake).expect("the worker should start"),
    );
    (client, contexts)
}

fn json_object(fields: impl IntoIterator<Item = (&'static str, Value)>) -> StructValue {
    StructValue {
        fields: fields
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value))
            .collect(),
    }
}

/// All five typed reads reach the provider and come back with the value it
/// resolved, so a backend installed through the adapter serves every flag type
/// SPACE has rather than the subset the trait used to carry.
#[test]
fn every_typed_read_resolves_through_the_adapter() {
    let client = FeatureFlagClient::from_provider(provider([
        ("space-bool", FakeFlag::Bool(true)),
        ("space-int", FakeFlag::Integer(42)),
        ("space-float", FakeFlag::Float(0.25)),
        ("space-string", FakeFlag::String("enforce".to_owned())),
        (
            "space-json",
            FakeFlag::Json(json_object([("unspecified", Value::Int(8))])),
        ),
    ]));

    assert!(client.bool_value(BOOL, "sandbox-1"));
    assert_eq!(client.integer_value(INTEGER, "sandbox-1"), 42);
    // Bit-compared, not value-compared: `clippy::float_cmp` is on repo-wide.
    assert_eq!(
        client.float_value(FLOAT, "sandbox-1").to_bits(),
        0.25_f64.to_bits()
    );
    assert_eq!(
        client.string_value(STRING, "sandbox-1", Attributes::default()),
        "enforce"
    );
    assert_eq!(
        client.json_value(JSON, "sandbox-1", Attributes::default()),
        Some(serde_json::json!({"unspecified": 8}))
    );
}

/// A double-valued flag is the reason `float_value` exists rather than being
/// folded into the integer read. Reading the same flag as an integer must not
/// quietly round or truncate it: OpenFeature resolves int and float through
/// separate methods, and the provider rejects the mismatch.
#[test]
fn a_double_valued_flag_resolves_as_a_float_and_refuses_an_integer_read() {
    // One key, declared twice, so both reads ask the same flag.
    const AS_FLOAT: FloatFlag = FloatFlag::new("space-ratio", 1.0);
    const AS_INTEGER: IntegerFlag = IntegerFlag::new("space-ratio", 7);

    let client =
        FeatureFlagClient::from_provider(provider([("space-ratio", FakeFlag::Float(0.25))]));

    assert_eq!(
        client.float_value(AS_FLOAT, "subject").to_bits(),
        0.25_f64.to_bits()
    );
    // The integer read falls back to the declaration rather than to 0.
    assert_eq!(client.integer_value(AS_INTEGER, "subject"), 7);
}

/// Every subject shape SPACE evaluates on reaches the provider as the
/// targeting key, which is the field a percentage rollout hashes. A subject
/// that arrived empty or mangled would still resolve flags — it would just
/// bucket every caller together — so this asserts the value, not just that a
/// read succeeded.
#[test]
fn each_subject_shape_becomes_the_targeting_key() {
    let (client, contexts) = provider_with_contexts([("space-bool", FakeFlag::Bool(true))]);

    // The shapes in use: a sandbox id, the TLS flag's cluster-qualified node,
    // the data-plane flag's prefixed subjects, and the constant cluster subject.
    for subject in [
        "sbx-019e5f38cd2e7400b3c8e33ae36692d7",
        "example-cluster/worker-192-0-2-10.example.com",
        "session:0198c0de",
        "org:acme",
        "cluster",
    ] {
        assert!(client.bool_value(BOOL, subject));
    }

    let seen: Vec<Option<String>> = contexts
        .lock()
        .expect("context log poisoned")
        .iter()
        .map(|context| context.targeting_key.clone())
        .collect();
    assert_eq!(
        seen,
        [
            Some("sbx-019e5f38cd2e7400b3c8e33ae36692d7".to_owned()),
            Some("example-cluster/worker-192-0-2-10.example.com".to_owned()),
            Some("session:0198c0de".to_owned()),
            Some("org:acme".to_owned()),
            Some("cluster".to_owned()),
        ]
    );
}

/// Targeting attributes survive the conversion into OpenFeature's context.
/// The TLS interception flag is the live case: it targets on `node_name` and
/// `cluster_id`, so an attribute dropped here turns a targeted rollout into a
/// silent miss that still resolves a value.
#[test]
fn targeting_attributes_reach_the_provider() {
    let (client, contexts) =
        provider_with_contexts([("space-string", FakeFlag::String("enforce".to_owned()))]);

    let attributes: Attributes = [
        ("node_name".into(), "worker-192-0-2-10.example.com".into()),
        ("cluster_id".into(), "example-cluster".into()),
        ("premium".into(), true.into()),
        ("score".into(), 12.5.into()),
    ]
    .into();
    assert_eq!(
        client.string_value(STRING, "subject", attributes),
        "enforce"
    );

    let contexts = contexts.lock().expect("context log poisoned");
    let fields = &contexts.first().expect("one evaluation").custom_fields;
    assert_eq!(
        fields
            .get("node_name")
            .and_then(EvaluationContextFieldValue::as_str),
        Some("worker-192-0-2-10.example.com")
    );
    assert_eq!(
        fields
            .get("cluster_id")
            .and_then(EvaluationContextFieldValue::as_str),
        Some("example-cluster")
    );
    assert_eq!(
        fields
            .get("premium")
            .and_then(EvaluationContextFieldValue::as_bool),
        Some(true)
    );
    assert_eq!(
        fields
            .get("score")
            .and_then(EvaluationContextFieldValue::as_f64),
        Some(12.5)
    );
}

/// `AttributeValue::Null` has nowhere to go in OpenFeature's context, so the
/// key is dropped rather than arriving as a present-but-empty value.
///
/// The other three attributes are seeded deliberately: asserting that one key
/// is missing from an otherwise empty map would hold just as well if the whole
/// conversion silently produced nothing, which is the failure this is meant to
/// catch. With three neighbours present, the absence means what it says.
#[test]
fn a_null_attribute_is_dropped_while_its_neighbours_survive() {
    let (client, contexts) = provider_with_contexts([("space-bool", FakeFlag::Bool(true))]);

    let attributes: Attributes = [
        ("kept_string".into(), "value".into()),
        ("kept_bool".into(), false.into()),
        ("kept_number".into(), 1.0.into()),
        ("dropped".into(), AttributeValue::Null),
    ]
    .into();
    assert!(client.bool_value_with_attributes(BOOL, "subject", attributes));

    let contexts = contexts.lock().expect("context log poisoned");
    let fields = &contexts.first().expect("one evaluation").custom_fields;
    assert_eq!(fields.len(), 3);
    assert!(fields.contains_key("kept_string"));
    assert!(fields.contains_key("kept_bool"));
    assert!(fields.contains_key("kept_number"));
    assert!(!fields.contains_key("dropped"));
}

/// An attribute named `targetingKey` is dropped rather than sent, and the
/// subject stands.
///
/// flagd builds its targeting input by inserting the targeting key first and
/// then inserting every custom field over the top, so this name would replace
/// the subject outright. A percentage rollout would then bucket on the
/// attribute — selecting a different population than the operator asked for,
/// for that one flag, with nothing in the outcome or the value to show it.
///
/// The surviving neighbour is what makes the absence mean something: without
/// it, "no `targetingKey` in the custom fields" would also hold if the whole
/// conversion had produced nothing.
#[test]
fn an_attribute_that_would_shadow_the_subject_is_dropped() {
    let (client, contexts) = provider_with_contexts([("space-bool", FakeFlag::Bool(true))]);

    let attributes: Attributes = [
        ("targetingKey".into(), "hijacked".into()),
        ("cluster_id".into(), "example-cluster".into()),
    ]
    .into();
    assert!(client.bool_value_with_attributes(BOOL, "the-real-subject", attributes));

    let contexts = contexts.lock().expect("context log poisoned");
    let context = contexts.first().expect("one evaluation");
    assert_eq!(
        context.targeting_key.as_deref(),
        Some("the-real-subject"),
        "the subject must survive"
    );
    assert!(
        !context.custom_fields.contains_key("targetingKey"),
        "the shadowing attribute must not be forwarded"
    );
    assert!(
        context.custom_fields.contains_key("cluster_id"),
        "its neighbour must still be forwarded"
    );
}

/// How each thing a provider can say is classified, over every reason a read
/// can carry.
///
/// The classification is the adapter's core semantic contract, and it is what
/// separates situations a call site and an operator must treat differently: a
/// flag nobody has defined yet is the steady state of every SPACE deployment,
/// a backend that is still starting resolves itself, and a definition whose
/// type its caller disagrees with never does and needs someone to go change
/// it. Collapsing any pair of those makes the reason useless — an error rate
/// that is permanently non-zero cannot be alerted on.
///
/// Four of the five [`Unresolved`] reasons appear here — every one this
/// adapter can produce. The fifth, [`Unresolved::NoProvider`], is the client's
/// answer for an empty provider slot; the trait lets a provider return it too,
/// to say it has no backend of its own, and this one always has the provider
/// it wraps.
///
/// One table rather than a function per case: the cases differ only in what
/// the backend says, and a hand-picked subset is how a fifth one gets added
/// with no classification at all.
#[test]
fn each_provider_verdict_is_classified_over_every_unresolved_reason() {
    let defined = [
        ("space-assigned", FakeFlag::Bool(true)),
        ("space-mistyped", FakeFlag::Integer(42)),
        (
            "space-broken",
            FakeFlag::Fails(EvaluationErrorCode::ParseError),
        ),
        (
            "space-unready",
            FakeFlag::Fails(EvaluationErrorCode::ProviderNotReady),
        ),
    ];

    // `space-undefined` is deliberately absent from the fixture while four
    // neighbours are present: "resolves nothing" is only evidence of an
    // unassignment if resolving something was possible.
    let cases = [
        ("space-assigned", Ok(true)),
        ("space-undefined", Err(Unresolved::NoAssignment)),
        ("space-mistyped", Err(Unresolved::EvaluationFailed)),
        ("space-broken", Err(Unresolved::EvaluationFailed)),
        ("space-unready", Err(Unresolved::ProviderUnready)),
    ];

    for (flag_key, expected) in cases {
        let client = FeatureFlagClient::from_provider(provider(defined.clone()));

        assert_eq!(
            client.resolve_bool(
                BoolFlag::new(flag_key, false),
                "subject",
                Attributes::default()
            ),
            expected,
            "{flag_key} was classified wrongly"
        );
    }
}

/// The provider's own readiness reaches [`crate::FeatureFlagClient`] as one of
/// the three states SPACE's contract has — and no status silences a read.
///
/// The second half is the one with teeth. The client skips the provider
/// entirely when it reports [`ProviderState::Absent`], so narrowing a
/// not-yet-ready or errored status to `Absent` rather than
/// [`ProviderState::Unready`] would stop every read from being attempted at
/// all: the adapter would go permanently inert with nothing to notice it,
/// since a provider that never gets asked also never recovers. Each case
/// therefore reads a flag the fake answers while reporting that status.
#[test]
fn the_providers_readiness_narrows_to_the_contract_without_silencing_reads() {
    let cases: [(fn() -> ProviderStatus, ProviderState); 4] = [
        (|| ProviderStatus::Ready, ProviderState::Ready),
        // A cached state that may no longer match the source of truth:
        // there is still a configuration to evaluate against, so its reads
        // are answers rather than absences.
        (|| ProviderStatus::STALE, ProviderState::Ready),
        (|| ProviderStatus::NotReady, ProviderState::Unready),
        (|| ProviderStatus::Error, ProviderState::Unready),
    ];

    for (status, expected) in cases {
        let client = FeatureFlagClient::from_provider(provider_reporting(status));

        assert_eq!(
            client.provider_state(),
            expected,
            "{:?} narrowed wrongly",
            status()
        );
        assert!(
            client.bool_value(BOOL, "subject"),
            "{:?} should still be asked; BOOL declares false, so a true is the provider's",
            status()
        );
    }
}

/// JSON flags resolve as objects, including nested arrays and numbers, which
/// is the shape SPACE's three JSON flags actually use.
#[test]
fn a_json_flag_resolves_nested_structure() {
    const QUOTAS: JsonFlag = JsonFlag::new("space-quotas");

    let client = FeatureFlagClient::from_provider(provider([(
        "space-quotas",
        FakeFlag::Json(json_object([
            ("unspecified", Value::Int(8)),
            (
                "domains",
                Value::Array(vec![Value::String("egress-proxy.example.test".to_owned())]),
            ),
            (
                "overrides",
                Value::Struct(json_object([("acme", Value::Int(32))])),
            ),
            ("ratio", Value::Float(0.5)),
        ])),
    )]));

    assert_eq!(
        client.json_value(QUOTAS, "subject", Attributes::default()),
        Some(serde_json::json!({
            "unspecified": 8,
            "domains": ["egress-proxy.example.test"],
            "overrides": {"acme": 32},
            "ratio": 0.5,
        }))
    );
}

// --- The sync/async bridge -------------------------------------------------
//
// These four pin the property the whole module is built around: a flag read is
// safe from any calling context. They look repetitive, and the repetition is
// the point — the two obvious implementations each pass one of these and panic
// in another, so a single-context test would have accepted either.

/// A plain synchronous function, with no runtime anywhere.
#[test]
fn a_flag_reads_from_a_context_with_no_runtime_at_all() {
    let client = FeatureFlagClient::from_provider(provider([("space-bool", FakeFlag::Bool(true))]));
    assert!(client.bool_value(BOOL, "subject"));
}

/// The default `#[tokio::test]` flavour, which is current-thread. This is the
/// one `block_in_place` panics in, and it is what all but a handful of this
/// repository's async tests use.
#[tokio::test]
async fn a_flag_reads_from_inside_a_current_thread_runtime() {
    let client = FeatureFlagClient::from_provider(provider([("space-bool", FakeFlag::Bool(true))]));
    assert!(client.bool_value(BOOL, "subject"));
}

/// A multi-threaded runtime, which is what the servers actually run on.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_flag_reads_from_inside_a_multi_thread_runtime() {
    let client = FeatureFlagClient::from_provider(provider([("space-bool", FakeFlag::Bool(true))]));
    assert!(client.bool_value(BOOL, "subject"));
}

/// Dropping the client inside an async context must not panic.
///
/// This is not hypothetical tidiness: a `Runtime` held as a field panics here
/// with "Cannot drop a runtime in a context where blocking is not allowed",
/// and every one of the tests above would still have passed, because they
/// never reach the drop. A server dropping its flag client at shutdown, or any
/// `#[tokio::test]` that builds one, hits exactly this.
#[tokio::test]
async fn the_client_can_be_dropped_inside_an_async_context() {
    let client = FeatureFlagClient::from_provider(provider([("space-bool", FakeFlag::Bool(true))]));
    assert!(client.bool_value(BOOL, "subject"));
    drop(client);
}

// Client cloning and provider sharing are the client layer's contract, pinned
// by `client::tests::custom_provider_receives_targeting_inputs_and_is_shared_until_last_clone_drops`.
// Nothing about owning a worker thread changes it, so it is not re-asserted here.

/// Concurrent reads from several threads all get answered. One worker serving
/// every caller is a queue, and a queue is a place to deadlock; each request
/// carries its own reply channel and each resolution is spawned, so a slow
/// flag cannot hold up the ones behind it.
#[test]
fn concurrent_reads_from_many_threads_are_all_answered() {
    let client = FeatureFlagClient::from_provider(provider([("space-bool", FakeFlag::Bool(true))]));

    std::thread::scope(|scope| {
        let handles: Vec<_> = (0..16)
            .map(|index| {
                let client = client.clone();
                scope.spawn(move || client.bool_value(BOOL, &format!("subject-{index}")))
            })
            .collect();
        for handle in handles {
            assert!(handle.join().expect("reader thread should not panic"));
        }
    });
}

/// A provider that never answers must not hang its caller.
///
/// Stated as an operator would: a wedged flag backend degrades SPACE to its
/// declared fallbacks within a bounded wait, it does not stall sandbox
/// creation behind a flag read. Without the timeout this test does not fail —
/// it hangs, which is the production failure it exists to prevent.
///
/// The reason is asserted too, and it is `EvaluationFailed` rather than
/// `NoAssignment`: a wedged backend must land where a backend problem belongs
/// and not among the flags a deployment simply has not defined. Substituting
/// the fallback for that reason is the client's job, pinned in
/// `client::tests`, so only the reason is read here.
#[test]
fn a_provider_that_never_answers_fails_the_read_within_the_deadline() {
    const WEDGED: BoolFlag = BoolFlag::new("space-wedged", true);

    let fake = FakeProvider::with([("space-wedged", FakeFlag::Hangs)]);
    let (hanging, peak) = fake.hang_counters();
    let client = FeatureFlagClient::from_provider(
        OpenFeatureProvider::new(fake).expect("the worker should start"),
    );

    let started = std::time::Instant::now();
    let resolved = client.resolve_bool(WEDGED, "subject", Attributes::default());
    let waited = started.elapsed();

    assert_eq!(resolved, Err(Unresolved::EvaluationFailed));
    // Expressed as a multiple of the constant under test rather than as a
    // wall-clock number: a fixed 5s ceiling is 10x the deadline, so it would
    // sit quietly through an 8x regression. Three leaves room for a loaded
    // machine and still fails on any real loosening.
    let ceiling = super::worker::EVALUATION_TIMEOUT * 3;
    assert!(
        waited < ceiling,
        "the read waited {waited:?}, past the {ceiling:?} a bounded wait allows"
    );

    // The evaluation really started — otherwise "nothing is parked" below
    // would hold for a provider that was never called.
    assert_eq!(
        peak.load(Ordering::SeqCst),
        1,
        "the provider should have been asked once"
    );

    // And it did not outlive the caller. Without the worker-side deadline the
    // task stays parked forever, and every later read on a wedged backend adds
    // another — a bounded latency problem turned into unbounded memory growth.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while hanging.load(Ordering::SeqCst) != 0 && std::time::Instant::now() < deadline {
        std::thread::yield_now();
    }
    assert_eq!(
        hanging.load(Ordering::SeqCst),
        0,
        "the timed-out evaluation is still parked; it was never cancelled"
    );
}

/// A request whose caller has already given up is never evaluated.
///
/// Stated as an operator would: when flag reads back up, the worker must not
/// spend its one thread answering callers who left. Every such evaluation is
/// work whose result is discarded, so under sustained overload goodput would
/// fall to zero and stay there while load continued — the queue drains only
/// as fast as it can do useless work.
///
/// The fixture builds a real backlog rather than simulating one: the first
/// read occupies the worker thread outright, so the second is still sitting in
/// the queue when its caller's deadline passes. Asserting on the contexts the
/// provider actually saw is what distinguishes "skipped at the head" from
/// "evaluated and the reply thrown away", which look identical to the caller.
#[test]
fn a_request_whose_caller_has_given_up_is_not_evaluated() {
    const BLOCKER: BoolFlag = BoolFlag::new("space-blocker", false);
    const QUEUED: BoolFlag = BoolFlag::new("space-queued", false);

    let fake = FakeProvider::with([
        ("space-blocker", FakeFlag::BlocksThread),
        ("space-queued", FakeFlag::Bool(true)),
    ]);
    let contexts = fake.contexts();
    let client = FeatureFlagClient::from_provider(
        OpenFeatureProvider::new(fake).expect("the worker should start"),
    );

    std::thread::scope(|scope| {
        let blocker = {
            let client = client.clone();
            scope.spawn(move || client.bool_value(BLOCKER, "blocker"))
        };
        // Give the blocking read time to be dequeued and seize the worker
        // thread, so the queued read really does land behind it.
        std::thread::sleep(std::time::Duration::from_millis(100));
        let queued = {
            let client = client.clone();
            scope.spawn(move || client.bool_value(QUEUED, "queued"))
        };

        // Both callers fall back to the `false` their declarations name: the
        // blocker outlasts its own deadline, and the queued one never gets
        // served at all.
        assert!(!blocker.join().expect("blocker thread"));
        assert!(!queued.join().expect("queued thread"));
    });

    // The worker is still working through the blocking read; wait for it to
    // finish so a late evaluation of the queued request would be caught.
    std::thread::sleep(BLOCKING_SPAN);

    let seen: Vec<Option<String>> = contexts
        .lock()
        .expect("context log poisoned")
        .iter()
        .map(|context| context.targeting_key.clone())
        .collect();
    assert!(
        seen.contains(&Some("blocker".to_owned())),
        "the blocking read should have reached the provider; saw {seen:?}"
    );
    assert!(
        !seen.contains(&Some("queued".to_owned())),
        "the abandoned read should have been skipped, not evaluated; saw {seen:?}"
    );
}

/// A provider whose construction fails is a startup error, not a client that
/// silently resolves nothing. The flagd case this stands in for is an
/// unreachable sync source: better to fail the deployment than to serve every
/// flag's default while looking healthy.
#[test]
fn a_failing_builder_is_reported_rather_than_swallowed() {
    let result = OpenFeatureProvider::from_builder(|| async {
        Err::<FakeProvider, _>(Box::<dyn std::error::Error + Send + Sync>::from(
            "flag source unreachable",
        ))
    });

    let error = result.expect_err("construction should fail");
    assert!(matches!(error, OpenFeatureProviderError::BuildProvider(_)));
    assert!(error.to_string().contains("failed to construct"));
}

/// The flagd opt-in accepts the workspace's whole truthy vocabulary, not just
/// `true`.
///
/// A local `== "true"` passes every other check in this crate and leaves flagd
/// off for an operator who wrote `SPACE_FLAGD_ENABLED=1` — the form that works
/// on every other `SPACE_*` knob — with no log line saying why. The falsy and
/// unrecognized cases are here too: an unrecognized value must read as off
/// rather than as "someone meant to enable this".
#[cfg(feature = "flagd")]
#[test]
fn the_flagd_opt_in_accepts_the_shared_truthy_vocabulary() {
    use super::flagd::{SPACE_FLAGD_ENABLED_ENV, enabled_from_environment};

    let mut env = pplx_space_test_env::TestEnv::default();
    assert!(
        !enabled_from_environment(&env),
        "unset must leave flagd off"
    );

    for truthy in ["1", "true", "TRUE", " yes ", "on", "enabled"] {
        env.set(SPACE_FLAGD_ENABLED_ENV, truthy);
        assert!(
            enabled_from_environment(&env),
            "{truthy:?} should enable flagd"
        );
    }

    for falsy in ["0", "false", "no", "off", "disabled", "", "maybe"] {
        env.set(SPACE_FLAGD_ENABLED_ENV, falsy);
        assert!(
            !enabled_from_environment(&env),
            "{falsy:?} should leave flagd off"
        );
    }
}

/// A flagd that was asked for and could not be built leaves a provider
/// installed, not an empty slot.
///
/// The two states take the same branch in [`crate::policy`] and differ
/// entirely in what they report, which is the point: `absent` is the
/// deliberate state of a deployment that named no backend, and an operator
/// who set `SPACE_FLAGD_ENABLED` and got nothing is somewhere else. Reporting
/// `absent` for both makes a broken flag source indistinguishable from a
/// decision, in `provider_state` and in the `outcome` tag alike.
///
/// `EPPO_SDK_KEY` is cleared because the fall-through is real: with Eppo
/// compiled in and configured, it takes the slot and this assertion would be
/// about whatever the host exported rather than about flagd.
///
/// The failure is provoked with the file resolver pointed at a path that does
/// not exist, so the test needs no daemon and no network: flagd's file
/// connector reports the read failure into its store, and the store's initial
/// state is then not `Ok`, which fails construction.
#[cfg(feature = "flagd")]
#[test]
fn a_flagd_that_was_asked_for_and_failed_reports_unready_rather_than_absent() {
    use super::flagd::SPACE_FLAGD_ENABLED_ENV;

    let directory = tempfile::tempdir().expect("a temp dir for the flag file");
    let missing = directory.path().join("there-is-no-such-file.json");

    if !pplx_space_test_env::enter_test_process(&[
        (SPACE_FLAGD_ENABLED_ENV, Some(std::ffi::OsStr::new("true"))),
        ("EPPO_SDK_KEY", None),
        ("FLAGD_RESOLVER", Some(std::ffi::OsStr::new("file"))),
        ("FLAGD_OFFLINE_FLAG_SOURCE_PATH", Some(missing.as_os_str())),
    ]) {
        return;
    }
    let client = FeatureFlagClient::from_env();
    assert_eq!(client.provider_state(), ProviderState::Unready);
    // And the reads agree with the state, which is what the trait
    // asks of a provider: unready, not unassigned, not an error.
    assert_eq!(
        client.resolve_bool(BOOL, "subject", Attributes::default()),
        Err(Unresolved::ProviderUnready)
    );
}
