use pplx_space_metrics_emit::{counter, tag};

/// What one flag read produced, as the `outcome` tag on
/// `feature_flags.evaluation`.
///
/// `NoProvider` and `ProviderUnready` are what keep a deployment that installs
/// no flag backend on purpose distinguishable from one whose backend broke —
/// both leave every flag at its declared fallback, and before these two values
/// existed they were the same absence of a series.
///
/// `assigned`, `unassigned`, and `error` keep the tag values emitted before the
/// other two were added, so their series stay continuous.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EvaluationOutcome {
    /// The provider resolved a value of the requested type.
    Assigned,
    /// No provider was installed, so nothing was asked.
    NoProvider,
    /// A provider was asked before it had configuration to answer from.
    ProviderUnready,
    /// The provider has configuration and no assignment for this subject.
    Unassigned,
    /// The provider tried to evaluate and failed.
    Error,
}

impl EvaluationOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Assigned => "assigned",
            Self::NoProvider => "no_provider",
            Self::ProviderUnready => "provider_unready",
            Self::Unassigned => "unassigned",
            Self::Error => "error",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DiagnosticCode {
    Match,
    ConfigurationMissing,
    FlagUnrecognizedOrDisabled,
    DefaultAllocationNull,
    TypeMismatch,
    UnexpectedConfigurationError,
    NotEvaluated,
}

impl DiagnosticCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Match => "match",
            Self::ConfigurationMissing => "configuration_missing",
            Self::FlagUnrecognizedOrDisabled => "flag_unrecognized_or_disabled",
            Self::DefaultAllocationNull => "default_allocation_null",
            Self::TypeMismatch => "type_mismatch",
            Self::UnexpectedConfigurationError => "unexpected_configuration_error",
            Self::NotEvaluated => "not_evaluated",
        }
    }
}

// Flag keys arrive as `&str` rather than `&'static str`: the flag-evaluation
// trait in `pplx-space-feature-flags` accepts a borrowed key so a provider whose
// key space comes from runtime configuration can implement it, and the key is
// emitted as it was handed over.
//
// The key becomes a tag value, and the `'static` bound used to be the only thing
// keeping that set finite, so a caller evaluating runtime-derived keys owns
// bounding its own. Note the asymmetry when judging that risk:
// `unassigned_diagnostic` is called behind a per-flag dedupe set capped at 64
// distinct keys, while `evaluation` and `boolean_assignment` fire on every
// evaluation with no cap at all.
//
// `evaluation` and `boolean_assignment` are emitted by `FeatureFlagClient`, not
// by a provider, so a deployment with no provider installed is counted too —
// that is the whole point of the `no_provider` outcome. `unassigned_diagnostic`
// stays with the provider, because its `reason` is the backend's own evaluation
// code and no vendor-neutral layer can produce it.

pub fn evaluation(flag_key: &str, outcome: EvaluationOutcome) {
    counter(
        "feature_flags.evaluation",
        1,
        &[tag("flag_key", flag_key), tag("outcome", outcome.as_str())],
    );
}

pub fn boolean_assignment(flag_key: &str, value: bool) {
    counter(
        "feature_flags.boolean_assignment",
        1,
        &[tag("flag_key", flag_key), tag("value", value)],
    );
}

pub fn unassigned_diagnostic(flag_key: &str, code: DiagnosticCode) {
    counter(
        "feature_flags.unassigned_diagnostic",
        1,
        &[tag("flag_key", flag_key), tag("reason", code.as_str())],
    );
}
