# Traces integration validation

Install the ordinary Collector/Tempo release with the
[Kind deployment helper](../../../../infra/space/kind/traces/README.md).
These checks own only their isolated fixture process, queries and evidence;
the backend chart has no dependency on this directory. Stop the two test
tunnels with Ctrl-C after validation. Fixture spans expire under the backend's
configured retention policy.

## Reproduce the stored-record contract

Use a clean source checkout containing the existing Rust
`trace_parity_fixture::emit` and `scripts/trace_fixture.py`. The selected fixture source commit and binary digest are recorded in every result.

```bash
# Leave these two tunnels running in separate terminals.
bash infra/space/kind/traces/local.sh ui
kubectl --context kind-space-local -n space-traces port-forward \
  --address 127.0.0.1 svc/collector 14317:4317 18888:8888

# Python 3.11+, the repo Rust toolchain and native build prerequisites required.
# Use a NEW output directory and run ID each time.
python3 scripts/tests/observability/traces/validate.py \
  --source /absolute/path/to/clean/tracing-stack-checkout \
  --output target/oss-traces-review01 --run oss-traces-review01
```

The operator check reuses `trace_fixture.build_fixture`, builds with
`cargo test --locked -p pplx-space-observability --features datadog-otel --lib
--no-run --message-format=json`, and runs the existing fixture with OTLP and
`always_on` **only in that isolated process**. It searches Tempo by a unique
synthetic version, fetches all four records (two requests and the job each
saved, which runs as its own root linked to its request's root) and verifies
exact span count, parent edges, links, names/kinds, statuses, typed attributes
and resource identity.
All assertions use Tempo's HTTP API; neither the chart nor this check needs Grafana.
The fixture synthesizes DB events; it does not call an actual database.
No new exporter or production bypass is introduced. Raw JSON, build output,
fixture log and `summary.json` remain under the chosen gitignored output path.

To repeat the drained restart check after a successful fixture run:

```bash
kubectl --context kind-space-local -n space-traces rollout restart deploy/collector
kubectl --context kind-space-local -n space-traces rollout status deploy/collector --timeout=180s
kubectl --context kind-space-local -n space-traces rollout restart deploy/tempo
kubectl --context kind-space-local -n space-traces rollout status deploy/tempo --timeout=180s
# Restart the tunnels, look up a previous trace ID, then rerun validate.py
# with a NEW run ID and output directory.
```

Do not restart while another agent is using this backend. This check proves
recovery of flushed local data, **not** lossless delivery through an outage.


[Historical validation and limits](validation.md) distinguish synthetic fixture
evidence from separately exercised public API journeys.
