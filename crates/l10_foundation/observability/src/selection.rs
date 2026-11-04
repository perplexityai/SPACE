//! Where a resolved telemetry destination came from.
//!
//! `auto` and an explicit selector are promises of different strength, so a
//! configuration neither can use has to be treated differently. `auto` means
//! "whatever this build can do": the operator never named the destination it
//! landed on, so an unusable one is a warning and no export. An explicit
//! selector names a destination the process is expected to reach, so the same
//! problem fails initialization and the process does not start pretending to
//! be observed.
//!
//! Without this distinction, configuring an endpoint for one signal could stop
//! a service from starting because `auto` then selected the same endpoint for
//! the other signal and disliked something about it.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Selection {
    /// The selector was unset or `auto`.
    Auto,
    /// The selector named this destination.
    Explicit,
}

impl Selection {
    /// Read the provenance of a selector value, before it is parsed.
    ///
    /// An unset selector and an explicit `auto` are the same promise, so they
    /// resolve the same way. A value this crate cannot parse is `Explicit`:
    /// the operator meant something, and a typo must not be softened into
    /// "whatever works".
    pub(crate) fn of(raw: Option<&str>) -> Self {
        match raw {
            None => Self::Auto,
            Some(raw) if raw.trim().eq_ignore_ascii_case("auto") => Self::Auto,
            Some(_) => Self::Explicit,
        }
    }

    pub(crate) const fn is_auto(self) -> bool {
        matches!(self, Self::Auto)
    }
}

#[cfg(test)]
mod tests {
    use super::Selection;

    /// An unset selector and an explicit `auto` are the same promise. Every
    /// other value is the operator's word — including a blank or misspelled
    /// one, which must keep failing rather than degrading to whatever works.
    #[test]
    fn auto_is_the_only_deferred_promise() {
        assert_eq!(Selection::of(None), Selection::Auto);
        for raw in ["auto", "AUTO", " auto ", "\tAuto\n"] {
            assert_eq!(Selection::of(Some(raw)), Selection::Auto, "{raw:?}");
        }
        for raw in [
            "datadog", "otlp", "off", "journald", "", " ", "atuo", "OTLP",
        ] {
            assert_eq!(Selection::of(Some(raw)), Selection::Explicit, "{raw:?}");
        }
    }
}
