# Logs integration validation

Install the ordinary Alloy/Loki chart using the
[Kind deployment helper](../../../../infra/space/kind/logs/README.md).
The test-only `values.yaml` extends collection to fixture pods and the synthetic
journal identifier, and enables diagnostic echo. Normal backend installations
contain neither fixture target. `run.sh` applies that overlay for the check and
restores the captured pre-test values afterward. It prints the saved values path
and retains it if restoration fails. Use the bench exclusively for
this bounded Alloy restart scenario.

## Reproduce the process and transport checks

Use a clean source checkout containing
`crates/l40_api/service/host/tests/logging_process.rs`. This chart can collect older JSON/text too; ingestion
alone does not establish which formatter an image contains. Record source and
image IDs before interpreting a run. The build needs the repo's pinned Rust
toolchain and native prerequisites, plus `jq`; validation needs Python 3.

```bash
SOURCE_CHECKOUT=/absolute/path/to/clean-logging-source
FIXTURE_IMAGE=space-oss-logs-fixture:YOUR_SOURCE_SHORT_SHA
# Choose new output paths and a unique short lowercase run name each time.
bash scripts/tests/observability/logs/build-fixture.sh \
  "$SOURCE_CHECKOUT" "$FIXTURE_IMAGE" target/logs-fixture-build
bash infra/space/kind/logs/local.sh status
kind load docker-image --name space-local "$FIXTURE_IMAGE"
bash scripts/tests/observability/logs/run.sh myrun1 "$FIXTURE_IMAGE" \
  target/logs-validation
```

The builder runs the existing process tests and packages their existing child
fixture, not another logging implementation. The runner creates isolated Jobs,
copies their exact JSON to the Kind worker's real journal with `systemd-cat`,
and compares application output, Alloy diagnostic output, and stored Loki
records. It checks 13 default/legacy/neutral/conflicting/blank/trimmed identity,
child-context, and stderr cases; typed fields; filtering; the testing-only
safe-request-detail gate; and ordinary-log positive controls in every case.
`SPACE_LOG_ENV=prod` with `DD_ENV=testing` must suppress request details.
It then rolls only this Alloy DaemonSet while 45 fixture processes emit through
both paths, saving observed missing/duplicate counts. Expect about two minutes.
The restart comparison is a local observation, not an exactly-once guarantee.

Raw output stays under the chosen local directory, not in git. `checks.json`
records assertions, `*-application.log` the source bytes, `*-alloy-*.log`
processed entries, and `*-loki.json` stored records. `run.json`, `jobs.json`,
and `restart.json` identify the run and task-owned resources. The wrapper removes only the Jobs recorded by this run after their evidence is
saved, and restores the exact pre-test Helm values on exit.
Diagnostic echo is off by default and may also be seen by an independently
running Datadog collect-all agent; turn it off after validation.


If the runner is force-killed, inspect `jobs.json` in its output directory and
delete only those Jobs after verifying their `logs-run` and
`space-local-owner` labels. Restore `backend-values-before.yaml` with the guarded
helper. Every kubectl command must name `--context kind-space-local`. Backend
retirement and persistent cursor handling belong to the deployment README.
