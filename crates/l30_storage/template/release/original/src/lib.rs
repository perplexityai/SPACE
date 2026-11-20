//! The rule that picks a template hash's original release, as SQL fragments.
//!
//! A template's releases are `template_releases` rows, keyed by
//! `(template_name, hash)` and, since release twins, the execution environment
//! a release is bound to. Two facts make "the release of this hash" ambiguous:
//!
//! * **Twins.** A digest can have one release per execution environment. The
//!   first registered is the original; the others are twins, registered after
//!   it, and a lookup that names no environment must never answer with one.
//! * **Two spellings.** Releases registered before the `sha256:` prefix was
//!   required store the bare hex digest. A digest can therefore be stored
//!   under both `<hex>` and `sha256:<hex>`, each spelling with its own
//!   original and twins.
//!
//! This crate is the one definition of the answer. Every caller that resolves
//! a digest to the release a lookup without an environment means (the
//! internal API's registration, tagging, hash and tag lookups and listing,
//! the API's create (by digest or by tag) and placement resolution, the admin
//! tag and delete guards, the runtime environment in-use count, the warm
//! pool's retention and demand matching, the release store's twin resolution
//! (`resolved_release_sql`), whose tie between spellings goes to the digest
//! original, and `scripts/bootstrap-templates.sh`)
//! interpolates
//! [`digest_original`] rather than spelling the rule itself. Callers that
//! disagree split a digest across releases: a pool warms slots for one
//! release while creates claim another's, or a tag moves to a release with
//! no storage location.
//!
//! # The two predicates
//!
//! [`spelling_original`] holds for a release no older release of the same
//! template shares exactly the same stored hash with, ordered by
//! `(created_at, id)`. It's the twin test: a release that fails it is a twin.
//!
//! [`digest_original`] holds for exactly one release of each
//! `(template_name, digest)` with any release, whichever spellings it's
//! stored under. Among the spelling originals of the digest (one per stored
//! spelling, so at most two), the one that owns a recorded storage location
//! wins, then the older by `(created_at, id)`. Ownership comes first because
//! a tag or a pool attached to an artifact-less release resolves to bytes
//! nothing can pull. The fragment is a pure function of the rows, so every
//! caller reading the same snapshot picks the same release.
//!
//! Attaching a storage location to the digest original never changes the
//! answer: it either already owns one, or neither spelling original does and
//! it's the older. Attaching one to the other spelling's original can.
//!
//! # Sharing with the bootstrap script
//!
//! The fragments live in `.sql` files with a `{release}` placeholder for the
//! candidate row's alias. Rust callers read them through the functions here;
//! `scripts/bootstrap-templates.sh` reads `digest_original.sql` from this
//! directory and passes it to `psql` as a variable, so the script runs the
//! same text. The tests in [`tests`] run the script against a migrated
//! database.
//!
//! # Interpolating
//!
//! A fragment is a whole boolean expression. Build the statement once (a
//! `LazyLock<String>`), bind every value, and keep the rest of the statement
//! whole at its query site, as the `space-data-store-development` skill
//! requires of interpolated fragments. The fragments reference
//! `template_releases` and `template_release_storage_locations` under aliases
//! that start with `spelling_` or `digest_`; a caller's own aliases must not.

/// The fragment text behind [`spelling_original`].
const SPELLING_ORIGINAL: &str = include_str!("spelling_original.sql");

/// The fragment text behind [`digest_original`].
const DIGEST_ORIGINAL: &str = include_str!("digest_original.sql");

/// The placeholder both fragment files use for the candidate row's alias.
const PLACEHOLDER: &str = "{release}";

/// True when the `template_releases` row aliased `release` is the original of
/// its exact stored hash: no release of the same template with the same
/// stored hash is older by `(created_at, id)`. A release that fails it is a
/// twin.
///
/// Panics when `release` isn't a plain lowercase SQL identifier or collides
/// with the fragment's own aliases.
pub fn spelling_original(release: &str) -> String {
    fill(SPELLING_ORIGINAL, release)
}

/// True when the `template_releases` row aliased `release` is the original of
/// its digest under either spelling, `<hex>` or `sha256:<hex>`: it's its
/// spelling's original, and no other spelling's original of the same digest
/// outranks it. One owning a storage location outranks one without; between
/// equals the older by `(created_at, id)` wins. Exactly one release of each
/// template and digest satisfies it.
///
/// Panics when `release` isn't a plain lowercase SQL identifier or collides
/// with the fragment's own aliases.
pub fn digest_original(release: &str) -> String {
    fill(DIGEST_ORIGINAL, release)
}

fn fill(fragment: &str, release: &str) -> String {
    assert!(
        is_candidate_alias(release),
        "template release alias {release:?} must be a lowercase SQL identifier \
         that doesn't start with `spelling_` or `digest_`"
    );
    fragment.trim_end().replace(PLACEHOLDER, release)
}

fn is_candidate_alias(alias: &str) -> bool {
    let mut chars = alias.chars();
    chars
        .next()
        .is_some_and(|first| first.is_ascii_lowercase() || first == '_')
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        && !alias.starts_with("spelling_")
        && !alias.starts_with("digest_")
}

#[cfg(test)]
mod tests;
