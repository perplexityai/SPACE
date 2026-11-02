# Fuzz targets

This standalone Cargo workspace exercises production libraries through thin
`cargo-fuzz` targets. It has its own lockfile and date-pinned nightly toolchain;
the main workspace keeps its stable compiler and normal build profiles.

## Run locally

Install a C++ compiler and `protoc`, then run from the repository root:

```sh
cd fuzz
cargo install cargo-fuzz --locked --version "$(cat cargo-fuzz.version)"
cargo fmt -- --check
cargo fetch --locked
mkdir -p corpus/slot_dhcp
cargo fuzz run slot_dhcp corpus/slot_dhcp seeds/slot_dhcp -- -max_total_time=60 -timeout=5 -rss_limit_mb=2048
```

The first invocation installs the nightly specified in `rust-toolchain.toml`
through rustup if needed. Fuzzing is supported on Linux and macOS. Change
`-max_total_time` for a longer local investigation.

`slot_dhcp` exercises the bounded guest-link DHCP codec in `net/common`.
The two checked-in seeds are synthetic DHCP DISCOVER and REQUEST packets,
using the same fictional MAC and fixed guest address as the parser tests.
They let mutations reach option parsing instead of only rejecting headers.
The mutable corpus, crash artifacts, coverage and instrumented build output
are ignored. `Cargo.lock` and the small, curated seed set are committed.

## CI and maintenance

The DHCP fuzz CI workflow runs only on pull requests that change
`crates/libs/net/common/src/slot_dhcp.rs`. It checks formatting and runs
`slot_dhcp` for 30 seconds with AddressSanitizer and bounded memory and
per-input time. Crash artifacts are uploaded on failure. Unrelated PRs,
merge-queue events and pushes do not run it; there is no scheduled run.
Manual local runs remain available using the commands above. This is a
short regression budget, not exhaustive fuzz qualification.

Add targets as `[[bin]]` entries in `Cargo.toml`, with a thin wrapper under
`fuzz_targets/` and at least one synthetic seed under `seeds/<target>/`.
Keep production parsing and validation in the owning library. Split harness
crates only when dependencies or incompatible features justify it.

Minimize any discovered failure with `cargo fuzz tmin`, then add a deterministic
regression test in the owning library. Review inputs for sensitive information
before committing them; never add captured tenant traffic or credentials.
Update the nightly pin, `cargo-fuzz.version` and lockfile deliberately and rerun
all targets. Build artifacts remain local to this workspace.
