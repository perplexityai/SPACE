//! Flag declarations: a key and the behavior chosen for it when nothing
//! resolves.
//!
//! A flag used to be a bare `&str`, and what it fell back to when the provider
//! resolved nothing lived at the call site as an argument, so two call sites
//! could pick different fallbacks for one flag and a reader of the flag list
//! could not tell a kill switch that must stay on from a rollout gate that must
//! stay off. The fallback now travels with the key: a call site names a
//! declaration rather than choosing a value.
//!
//! That is a guarantee about the declarations, not about the key. [`Flag::new`]
//! is public and takes the fallback, so two declarations of one key can
//! disagree — and this crate's own tests do exactly that, declaring
//! `space-test-flag` both ways to drive both fallback directions from a single
//! fixture. It stays public because a flag only one reconciler reads is
//! reasonably declared next to that reconciler, as
//! `space-snapshot-retention-config` is in the snapshot-retention sweeper. So:
//! read a flag through the constant in the crate root and the fallback is the
//! declared one; hand-roll a `Flag` for a key that already has a constant and
//! you have re-introduced the thing this type removed.
//!
//! Nothing in a declaration is about *when* a flag is consulted. An env master
//! that can hard-disable a feature is a separate mechanism; see
//! [`crate::policy`].

/// A flag key together with the value a read of it falls back to.
///
/// One generic rather than a type per flag type: the four scalar flag types
/// differ only in what `T` is, and a reader comparing two declarations should
/// see the same two fields in the same order. JSON is the exception and has its
/// own type — see [`JsonFlag`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Flag<T> {
    key: &'static str,
    when_unresolved: T,
}

/// A boolean flag. Declare `true` for a gate that must stay on when the
/// provider says nothing (an on-by-default guardrail whose flag exists only to
/// switch it off), `false` for a rollout gate that must stay off.
pub type BoolFlag = Flag<bool>;

/// An integer-valued flag, falling back to a compiled-in number.
pub type IntegerFlag = Flag<i64>;

/// A float-valued flag, falling back to a compiled-in number.
pub type FloatFlag = Flag<f64>;

/// A string-valued flag, falling back to a compiled-in string.
pub type StringFlag = Flag<&'static str>;

impl<T> Flag<T> {
    /// Declare a flag key and what a read of it falls back to.
    pub const fn new(key: &'static str, when_unresolved: T) -> Self {
        Self {
            key,
            when_unresolved,
        }
    }

    /// The key the provider is asked for, and the `flag_key` metric tag.
    #[must_use]
    pub const fn key(&self) -> &'static str {
        self.key
    }
}

impl<T: Copy> Flag<T> {
    /// The value a read falls back to when the provider resolves nothing,
    /// whatever the reason. Deliberately not per-reason, and neither is
    /// [`crate::policy`]: "no provider installed" and "provider installed and
    /// broken" take this same value, and that module documents why splitting
    /// them makes a `false`-declaring flag unsafe to gate. The reason is still
    /// reported — it just does not change the value.
    #[must_use]
    pub fn when_unresolved(&self) -> T {
        self.when_unresolved
    }
}

impl<T> std::fmt::Display for Flag<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.key)
    }
}

/// A JSON-typed flag, which declares no fallback.
///
/// Its value is structured configuration the caller validates, so there is no
/// single compiled-in JSON document to fall back to: a read that resolves
/// nothing and a read that resolves something the caller rejects both land on
/// that caller's own local configuration, field by field. Forcing a
/// `when_unresolved` here would invite reducing an override document to a
/// boolean to fit the shape.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct JsonFlag {
    key: &'static str,
}

impl JsonFlag {
    /// Declare a JSON flag key.
    pub const fn new(key: &'static str) -> Self {
        Self { key }
    }

    /// The key the provider is asked for, and the `flag_key` metric tag.
    #[must_use]
    pub const fn key(&self) -> &'static str {
        self.key
    }
}

impl std::fmt::Display for JsonFlag {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.key)
    }
}

#[cfg(test)]
mod tests;
