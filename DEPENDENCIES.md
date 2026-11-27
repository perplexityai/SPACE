# Dependency policy

SPACE uses third-party code in production binaries, build tools, tests, and
deployment artifacts. A dependency change deserves review even when it is a
transitive version bump or does not change application source code.

## Adding or replacing a dependency

- Prefer the standard library or an existing workspace dependency when it
  provides the needed behavior without a worse fit. Keep the new crate's
  features and scope as small as practical.
- In the pull request, explain the use case, why the existing options do not
  suffice, where the dependency is used (runtime, build, or test), and any
  change to the production dependency graph.
- Review the package's source and provenance, license, maintainers, release
  history, issue handling, and relevant advisories. A recent release or a
  popularity score alone is not evidence of safety. Look at the code and the
  specific version being added, especially build scripts, procedural macros,
  unsafe code, and newly enabled features.
- Give extra scrutiny to dependencies that handle credentials, cryptography,
  authentication, network input, serialization, or telemetry, or that run
  code during the build. In those areas, explain the trust boundary and ask a
  reviewer familiar with the affected subsystem to review the change.
- Use crates.io releases by default. Explain any Git, alternate-registry, or
  vendored source and pin it to an immutable revision or artifact. Do not add
  an unreviewed source override to make a build pass.

## Updating dependencies

- Commit `Cargo.lock` with any resolution change. Keep the lockfile diff
  focused: use a targeted `cargo update -p <crate>` when updating one crate,
  and separate unrelated refreshes from feature changes where practical.
- Cargo normally waits three days before selecting a newly published registry
  version. Versions already in `Cargo.lock` remain eligible. If an urgent
  security fix falls within that window, use
  `CARGO_RESOLVER_INCOMPATIBLE_PUBLISH_AGE=allow cargo update -p <crate> --precise <version>`
  and review the resulting lockfile diff, including transitive updates.
- Review *all* added and updated direct and transitive packages in the
  lockfile diff. For an unexpected package, use
  `cargo tree --locked -i <crate>` to find the parent and check whether a new
  feature pulled it in. A version bump can change behavior even when the
  manifest stays the same.
- Summarize the dependency graph change in the pull request: new packages,
  removed packages, significant version jumps, source changes, and any
  advisory or license concern. If there are none, say so. Reviewers should
  inspect the lockfile diff rather than relying only on this summary.
- Run the relevant build and tests with `--locked`. Treat a known
  vulnerability as a tracked security issue; coordinate a fix through
  [SECURITY.md](SECURITY.md) if disclosure is sensitive. Do not hide an
  advisory with an unexplained ignore or a broad version pin.

## Cargo vetting

CI runs `cargo vet --locked` against the committed `supply-chain/` store.
Assess evidence in this order: publisher trust for the named crates below,
imported peer audits, then exact-version exemptions for what remains.

### Trusted publishers

The following entries in `supply-chain/audits.toml` meet `safe-to-deploy` only
for the listed crates released by the specified crates.io user ID. Each entry
starts on the publication date of its earliest locked version and ends on
2027-09-24. The list does not trust other crates from these publishers.

| Publisher | User ID | Trusted crates |
| --- | ---: | --- |
| `aws-sdk-rust-ci` | 160111 | `aws-config`, `aws-credential-types`, `aws-runtime`, `aws-sdk-acmpca`, `aws-sdk-ecr`, `aws-sdk-s3`, `aws-sdk-secretsmanager`, `aws-sdk-sso`, `aws-sdk-ssooidc`, `aws-sdk-sts`, `aws-sigv4`, `aws-smithy-async`, `aws-smithy-checksums`, `aws-smithy-eventstream`, `aws-smithy-http`, `aws-smithy-http-client`, `aws-smithy-json`, `aws-smithy-observability`, `aws-smithy-query`, `aws-smithy-runtime`, `aws-smithy-runtime-api`, `aws-smithy-runtime-api-macros`, `aws-smithy-schema`, `aws-smithy-types`, `aws-smithy-xml`, `aws-types` |
| `BurntSushi` | 189 | `globset`, `jiff`, `jiff-core`, `jiff-static`, `jiff-tzdb`, `jiff-tzdb-platform`, `regex`, `regex-automata`, `regex-lite`, `regex-syntax` |
| `Darksonn` | 6741 | `axum`, `bytes`, `tokio`, `tokio-macros`, `tokio-stream`, `tokio-util` |
| `dtolnay` | 3618 | `anyhow`, `itoa`, `proc-macro2`, `quote`, `ryu`, `serde`, `serde_core`, `serde_derive`, `serde_json`, `syn`, `thiserror`, `thiserror-impl` |
| `epage` | 6743 | `clap`, `clap_builder`, `clap_derive`, `clap_lex` |
| `hds` | 172786 | `tracing`, `tracing-attributes`, `tracing-core`, `tracing-subscriber` |
| `seanmonstar` | 359 | `h2`, `http`, `http-body`, `http-body-util`, `httparse`, `hyper`, `hyper-util`, `reqwest`, `tower`, `tower-http` |
| `taiki-e` | 33035 | `futures`, `futures-channel`, `futures-core`, `futures-executor`, `futures-io`, `futures-macro`, `futures-sink`, `futures-task`, `futures-util` |

Review the publisher and release process before adding or renewing an entry.
The listed AWS and HTTP crates handle credentials or network traffic, so their
lockfile changes also need a reviewer familiar with that subsystem. A changed
publisher account will not match an existing entry. Publisher trust is not a
source audit; review lockfile changes and newly enabled features even for
trusted publishers.

### Imported audits

`supply-chain/config.toml` imports the public audit sets of Mozilla, Google,
the Bytecode Alliance, Zcash, ISRG/Divvi Up, and Embark Studios. Cargo vet
records the relevant audits and publisher metadata in
`supply-chain/imports.lock`; CI uses that committed
snapshot. Imported evidence can include version audits, delta audits, and
publisher-scoped wildcard audits. Review changes to the peer list and import
lock before accepting refreshed evidence. Run `cargo vet` without `--locked`
to refresh the snapshot, then commit the resulting store changes together.
Imports are not transitive: adding one peer does not adopt its other peers.

### Remaining exemptions

The `[[exemptions.*]]` entries in
[supply-chain/config.toml](supply-chain/config.toml) are the complete list of
exact locked versions still lacking sufficient publisher or imported audit
evidence: 566 version entries across 528 crate names in this baseline.
**An exemption is not an audit.** A new or updated crate version
must have an applicable audit, a deliberately trusted imported audit, a
crate-specific trusted-publisher entry, or an explicit exemption for CI to pass.

When changing `supply-chain/`, explain each new audit, trusted import,
publisher-trust entry, or exemption in the pull request. Inspect the published
crate or version diff before claiming an audit. For an update, use
`cargo vet diff <crate> <old> <new>`. If an exemption is necessary, give the
reason and scope in its `notes` field and ask a reviewer to assess that
decision. Do not use `cargo vet regenerate exemptions` to silently approve a
changed dependency graph.

Cargo vet records these trust decisions. It does not scan for newly disclosed
advisories, and a passing check does not mean every baseline crate was audited.

## Advisory, license, and source checks

CI also runs `cargo deny --locked --all-features check advisories licenses
sources` with the policy in [deny.toml](deny.toml). It checks the full workspace
against RustSec advisories, permits only the listed dependency licenses, and
rejects packages from unapproved registries or Git sources. New source types
and license terms need explicit review before changing the policy. The two
CDLA license exceptions are limited to CA certificate data crates.

SPACE's own license is Apache-2.0, with a contributor CLA planned separately.
Cargo-deny's dependency license check does not enforce the CLA or replace legal
review of third-party terms.

`Cargo.lock` and the existing `--locked` CI checks make builds reproducible
with respect to resolved Cargo versions. They do not judge whether a new
package or release is trustworthy. Advisory scanners likewise detect known,
reported problems, not undisclosed malicious changes. Human review of the
actual dependency diff remains part of the approval for a dependency change.
