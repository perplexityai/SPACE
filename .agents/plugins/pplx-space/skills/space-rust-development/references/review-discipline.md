# Rust review discipline (SPACE)

Use the sections relevant to the changed behavior for pre-PR self-review.
`docs/rust/space-review-lessons.md` supplies rationale and concrete examples;
read the corresponding lessons when needed, or review it broadly for a
cross-cutting refactor. Fix applicable findings before requesting review.
Report material findings and validation gaps; unrelated checklist items need
no N/A explanations in the PR.

## Conditional review

### Tests and smoke coverage

- [ ] Test or CI changes follow the [smoke reviewer stop rule](../../space-code-quality/references/test-economy.md#smoke-tests-reviewer-stop-rule): no new deployment smoke tests outside the typed `space_smoke` framework; maintenance of existing untyped ones is allowed.

### Boundaries and types

- [ ] No new `match action { "foo" => ..., _ => bad_request }` over a closed set of strings. Use an enum with `impl FromStr` + `Display` + instance-method predicates (e.g. `fn affects_lifecycle(self) -> bool`). See §1 in the SPACE lessons.
- [ ] No new `Option<String>` / `String` for status/state/action values at function boundaries. Lift to an enum, even if it's two variants.
- [ ] Any value that becomes a filesystem path (`format!("{chunks_dir}/{digest}")`, `Path::join(name)`) has its components validated at the entry boundary — not just on "the other path that also consumes it." See §9 in the SPACE lessons.

### Idempotency and side-effects after commit

- [ ] If a doc-comment claims "idempotent", the SQL has `ON CONFLICT (...) DO UPDATE/NOTHING` and the table has the matching unique index. Otherwise change the comment. See §2 in the SPACE lessons.
- [ ] Any non-transactional side-effect after `tx.commit()` (RPC, file I/O, second commit) has explicit rollback or a documented eventual-consistency reconciler. The current function is not allowed to return `Err` while leaving committed half-state with no cleanup. See §4 in the SPACE lessons.
- [ ] Searched the workspace for existing cleanup helpers (e.g. `finalize_sandbox_db_delete_tx`) before writing a new one. If the new path is intentionally different from the helper, the difference is explained in the PR description.

### Fail-loud discipline

- [ ] No new `unwrap_or(0)` / `unwrap_or_default()` on values that get persisted (DB row, file, manifest, metric, log). If the fallback isn't *correct*, propagate the error. See §3 in the SPACE lessons.
- [ ] No new `let _ = some_fallible_call()`. Replace with `if let Err(err) = ... { tracing::warn!(error = %err, "..."); }` at minimum.
- [ ] No new `Command::status().is_ok_and(|s| s.success())` that swallows stderr. Capture and log stderr on failure so the fallback path is diagnosable.
- [ ] Audited warn/error log strings touched by this change — they describe what *actually* happens now, not what used to happen.

### Async / blocking

- [ ] Any sha256 of a file > ~100 MB, file compression/decompression, large `walk_dir`, or other CPU-heavy work in an `async fn` is wrapped in `tokio::task::spawn_blocking`. See `docs/rust/async-and-tokio.md` and §6 in the SPACE lessons.
- [ ] gRPC handlers return their initial `Response` before any work that could exceed ~30s. If the work might exceed the deadline, the handler returns `FailedPrecondition`/`Unavailable` and a background task produces the artifact.
- [ ] Any new "N independent network/IO calls in a loop" pattern uses `futures::stream::iter(...).buffer_unordered(K)` (or `try_join_all`), not sequential `.await` in a `for`.

### Timeouts

- [ ] Client-side timeout, if any, is **longer** than the server's worst-case retry budget. (Otherwise the client times out while the server is still working.)
- [ ] The timeout wraps the *end-to-end* operation including connect/lookup/poll, not just one inner call. See §5 in the SPACE lessons.
- [ ] Function doc-comment states the latency contract so the next reader doesn't have to grep retry sleeps.

### Argument plumbing

- [ ] On every `a.or(b)`, the comment one line above states who wins. (Forces honesty about precedence.) See §7 in the SPACE lessons.
- [ ] Any refactor of "`None` means leave alone" into "`None` means use default" has a test exercising every caller that previously passed `None`.

### Classifiers

- [ ] String classification (`contains` / `starts_with`) is justified by the actual error string flow. If errors get wrapped (`format!("failed to ...: {e}")`), use `contains`, not `starts_with`. See §8 in the SPACE lessons.
- [ ] Tokens with numeric suffixes (`lookup 100`, `fwmark 1`) match on whitespace boundaries, not bare `contains`. Prefer `split_whitespace` + token equality.

### Caches and in-flight maps

- [ ] Any new cache has a bounded size + expired-entry eviction. See §10 in the SPACE lessons.
- [ ] Any in-flight dedup map (e.g. `HashMap<Key, Vec<oneshot::Sender>>`) is freed via RAII on `Drop`, not manual cleanup. Bare maps strand waiters on cancellation.
- [ ] Authentication caches consider negative-result caching (short TTL) to bound kube-apiserver / external-IDP load under bogus-token sprays.

### Tracing / observability

- [ ] Every `?` in an `#[tracing::instrument]`-annotated function has its outcome recorded on the span before propagating, or the function uses a helper that records on `Drop`. See §11 in the SPACE lessons.
- [ ] For gRPC clients/servers, used `tower_http::trace::TraceLayer` rather than hand-rolling a `Service` impl — only the layer knows about trailers.

### File I/O safety

- [ ] Any file polled by another process is written via temp + rename (`tempfile::NamedTempFile::new_in(dir).persist(p)`). `std::fs::write` is not atomic. See §13 in the SPACE lessons.
- [ ] If a comment claims "the receiver holds the FD across this unlink", the receiver actually holds an FD (i.e. open + pass `/proc/self/fd/N`), not a path it re-opens. See §14 in the SPACE lessons.

### Database

- [ ] No new CTE (`WITH x AS (...)`) unless the recursive/readability win is documented in a comment.
- [ ] Schema changes: column removals / type changes are *new* forward migrations (use the current version format in `db/README.md`), not edits to a previously-deployed baseline file. See §12 in the SPACE lessons.
- [ ] Data operations use the owning storage layer and repository database fixtures. Follow the `space-data-store-development` skill for store boundaries and guarded-transition exceptions; do not wait for a callsite-count threshold to move ordinary SQL into its owner.

### Network clients

- [ ] No new `Client::builder(...).build_http()` for an endpoint that could be `https://`. Use `reqwest` with `rustls-tls` (already in the workspace), or build a `HttpsConnector`-aware hyper client.
- [ ] No new `connect()` inside a poll loop. Lift the connection out, reuse the channel.

## Final validation

For a narrow Rust change, run formatting checks, affected-package compilation
and lint checks with the relevant targets/features, and tests that exercise the
changed behavior. Include affected consumers when changing a shared API and
doc tests when changing executable documentation. Targeted results are final
local evidence only for the scope they cover; report commands and limitations.

Cross-cutting changes to shared crates, workspace dependencies/features, build
configuration, or code generation need broad workspace validation. The owning
runner is `ci/scripts/run-rust-checks.sh` (`fmt`, `clippy`, `isolated-crates`,
`tests`, or `all`; GitHub CI splits `tests` into `tests-unprivileged` and
`tests-privileged`); use its current checks rather than maintaining a second
command matrix here. Its `clippy` mode does not lint crates alone: when a change
adds a crate or changes dependency features, also run its `isolated-crates`
mode (see `$space-validation`'s CI pipeline reference for running the check
alone). CI retains its complete workspace inventory, including all targets,
features, ignored tests, and doc tests. Do not narrow CI to local selectors.

On macOS, Rust changes behind `cfg(target_os = "linux")` also require targeted
compilation and lint checks for `aarch64-unknown-linux-musl`. Use
`$space-validation` when container-backed tests need Docker or the change needs
Linux/KVM, Kind, or other runtime evidence. Dependency skips are not final
proof when the required services can be started.
