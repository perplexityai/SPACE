# Feature flags

Runtime feature-flag evaluation for SPACE, over a vendor-neutral provider
interface. Every flag read has a fallback that travels with the flag rather
than with the call site — the value a scalar flag declares, or, for a JSON
flag, the caller's own local configuration. So a deployment with no flag
backend at all is a supported configuration; it just cannot change a flag
without a redeploy.

Three deployment shapes are supported:

| Shape | Cargo features | Runtime configuration | Who it is for |
| -- | -- | -- | -- |
| No provider | `--no-default-features` | none | a deployment whose flags are fixed at build time |
| flagd, via OpenFeature | `flagd` | `SPACE_FLAGD_ENABLED=true`, plus `FLAGD_*` | an OSS SPACE cluster |
| Eppo | `eppo` (default) | `EPPO_SDK_KEY` | deployments using Eppo |

The crate builds and passes its tests in each. Nothing in the evaluation path
names a vendor: `FeatureFlagProvider` carries plain scalars and one typed read
per flag type, and each backend converts to its own SDK inside its own module.

## Choosing a provider

Provider choice is made twice, and both have to agree.

**At build time**, by Cargo feature. A backend that is not compiled in cannot
be configured at runtime, which is the point: an OSS build need not link the
Eppo SDK, and a build that links neither is a legitimate configuration rather
than a broken one.

**At run time**, by environment. `FeatureFlagClient::from_env` checks flagd
first, then Eppo:

1. If `SPACE_FLAGD_ENABLED=true`, build the flagd provider. Failing to build it
   logs an error and falls through — flagd was asked for by name, so this is a
   misconfiguration rather than an absent optional backend. It does not fall
   through to *nothing*: with no other provider configured the slot gets a
   provider that reports `unready`, so a flag source that is broken stays
   distinguishable from a deployment that named no backend.
2. Otherwise, if `EPPO_SDK_KEY` is set, build the Eppo provider.
3. Otherwise the client is disabled and every flag resolves to its declared
   fallback. That case logs a warning naming the providers this binary was
   built with, so "rebuild with a provider feature" is distinguishable from
   "set the variable the provider you already have is waiting for".

flagd is checked first because it is the only one that has to be asked for.
flagd's own defaults point at `localhost:8015`, so "is it configured?" cannot
be inferred from the `FLAGD_*` variables the way it can from Eppo's SDK key —
without the explicit opt-in, every build with the feature would look configured
and quietly try to reach a daemon that is not there.

`FeatureFlagClient::provider_state` reports whether a provider is installed
and whether it can serve assignments — `absent`, `unready`, or `ready`. It
does not report that the provider supports a given flag type, or that any flag
has an assignment. `FeatureFlagClient::is_configured` is the coarser form,
answering only whether a provider is installed at all.

## Running flagd

[flagd](https://flagd.dev/) is configuration-file driven. Flag definitions are
JSON; an operator changes a flag by changing that JSON. There is no console —
see [What an operator gives up](#what-an-operator-gives-up).

SPACE compiles only flagd's two **in-process** resolvers, which evaluate
against a flag set held in memory and kept current in the background:

| Resolver | `FLAGD_RESOLVER` | Definitions come from | Needs a flagd daemon |
| -- | -- | -- | -- |
| In-process | `in-process` (default) | a daemon's sync stream, over gRPC | yes |
| File | `file` | a JSON file on disk, re-read when it changes | no |

The remote-evaluation resolvers (`rpc`, `rest`) are excluded at the Cargo level
rather than rejected at runtime, so a deployment cannot configure its way into
a flag read that makes a network round trip. `FLAGD_RESOLVER=rpc` on a SPACE
build falls back to in-process rather than taking effect. This matters because
SPACE reads flags on paths like sandbox creation, where a round trip would be
felt; `FeatureFlagProvider` promises reads served from local state.

### File resolver

The smallest working setup, and the one to start with in Kind or local
development. No daemon, no network:

```sh
export SPACE_FLAGD_ENABLED=true
export FLAGD_RESOLVER=file
export FLAGD_OFFLINE_FLAG_SOURCE_PATH=/etc/space/flags.json
```

Start from [`reference/flagd-space-flags.json`](reference/flagd-space-flags.json),
which defines every flag SPACE reads with defaults that reproduce an
unconfigured deployment — serving it unchanged changes no behavior. Mount it as
a ConfigMap and edit the ConfigMap to change a flag.

### In-process resolver

Point SPACE at a flagd daemon, which serves flag definitions over a sync
stream. SPACE still evaluates locally; the daemon only distributes definitions.

```sh
export SPACE_FLAGD_ENABLED=true
export FLAGD_RESOLVER=in-process
export FLAGD_HOST=flagd.space.svc.cluster.local
export FLAGD_PORT=8015
```

Everything else is flagd's own `FLAGD_*` surface — `FLAGD_TLS`,
`FLAGD_SERVER_CERT_PATH`, `FLAGD_SOURCE_SELECTOR`, `FLAGD_SOCKET_PATH`, the
retry and deadline settings — read by the flagd SDK itself, so flagd's
documentation stays accurate for SPACE.

### Startup and outage behavior

An unreachable flag source fails at **startup** rather than degrading quietly:
the provider gives up and `from_env` logs and falls through. That is
deliberate — a deployment learns its flag source is wrong instead of serving
every declared fallback while looking healthy.

Giving up takes about **40 seconds**, which matters for probe sizing. flagd's
in-process connector has a hard-coded 5 s connect timeout and retries
`FLAGD_RETRY_GRACE_PERIOD` (default 5) times with a doubling backoff from
`FLAGD_RETRY_BACKOFF_MS` (default 1 s) — five attempts plus 1+2+4+8 s of
waiting. The 5 s deadline on the initial flag set applies only after a
connection succeeds. A startup probe that fires before that budget kills the
pod mid-retry and turns a slow flag source into a crash loop; lower
`FLAGD_RETRY_GRACE_PERIOD` if you want it shorter.

Once started, a disconnect does **not** fail reads. The last-synced flag set
stays in memory and the sync stream reconnects, so flags keep resolving to
their last known values across a daemon restart.

## The flags SPACE reads

Six keys. The **subject** is what a percentage rollout hashes, so it decides
what a fractional rule splits on; the **attributes** are what a targeting rule
can match on beyond the subject.

| Flag key | Type | Subject | Attributes | Declared fallback |
| -- | -- | -- | -- | -- |
| `space-cluster-rate-limiter` | boolean | constant `space-cluster` | none | on |
| `space-api-client-concurrency-limiter` | boolean | constant `space-cluster` | none | on |
| `space-api-client-concurrency-quotas` | JSON object | constant `space-cluster` | none | env-configured caps |
| `space-net-gateway-known-egress-domains` | JSON object | sandbox id | none | the deployment's local list |
| `space-cluster-api-client-side-balancing` | boolean | `sandbox:`/`session:`/`org:` prefixed id | `cluster_id` | off |
| `space-native-create-pool-claim` | boolean | constant `space-cluster` | none | off |
| `space-snapshot-retention-config` | JSON object | cluster id | none | env-configured limits |

Each fallback is chosen so that losing the flag backend cannot turn a
guardrail off: the limiters fall back **on**, and the flags
that would enable new behavior fall back **off**. All three JSON keys fall back
to whatever their deployment configured locally rather than to a value chosen
here, because an override that resolves nothing should leave local
configuration standing — see `policy::override_allowed`.

Sandbox-fork admission is controlled solely by `SPACE_SANDBOX_FORK_ENABLED`,
which defaults to `true`. Set it to `false` on the API service and redeploy to
stop admitting new forks; existing operations and idempotent replays continue.
No feature-flag provider controls this switch.

Admission still validates the hard deadline and both snapshot cleanup graces.
Each grace must exceed the restore activation lease (at least 600 seconds).
The bare storage-sweeper default is 300 seconds, so deployments must configure
a longer grace for forking; the Kind chart already uses 3600 seconds for both.

### Subject shapes, and why they differ

There is no single SPACE subject identity, and the shape is a per-flag choice
rather than an accident:

- **A constant** (`space-cluster`) is a cluster-wide switch. Every
  caller hashes identically, so a fractional rule on one of these is all-or-
  nothing — use variants, not percentages.
- **A sandbox id** gives a deterministic percentage rollout: the same sandbox
  lands in the same bucket every time, so a ramp never re-rolls a sandbox it
  already admitted.
- **A cluster id** lets one cluster be retuned without touching the others.
- **`<cluster_id>/<node_name>`** qualifies the host, so identical private DNS
  names in different clusters do not collide into one bucket. No sandbox,
  guest, or manager-pod identity enters the decision.
- **A prefixed entity id** (`sandbox:`, `session:`, `org:`) lets one flag ramp
  along whichever axis a call site has at hand, while keeping the namespaces
  from colliding.

### Targeting examples

Percentage rollout, bucketed on whatever the subject is. flagd hashes the flag
key together with the targeting key, so two flags at 1% do not select the same
sandboxes:

```json
"targeting": { "fractional": [["on", 1], ["off", 99]] }
```

Match on an attribute, falling through to `defaultVariant` when the rule does
not match — `null` means "no opinion", not "off":

```json
"targeting": {
  "if": [{ "==": [{ "var": "cluster_id" }, "example-cluster"] }, "enforce", null]
}
```

Name individual hosts:

```json
"targeting": {
  "if": [
    { "in": [{ "var": "node_name" }, ["worker-192-0-2-10.example.com"]] },
    "enforce",
    null
  ]
}
```

## What OpenFeature cannot carry

Two shapes have no OpenFeature equivalent. Both fail quietly — the flag reads
as its declared fallback rather than erroring visibly — so they are worth
knowing before writing definitions. Both are properties of OpenFeature itself, so they apply
to every backend reached through the adapter, not just flagd.

**A JSON flag must be a JSON object.** OpenFeature resolves JSON as a
*structure* type, so a flag whose value is a bare number, a string, or a
top-level array cannot be resolved at all. This is why
`space-net-gateway-known-egress-domains` is defined as
`{"domains": [...]}` rather than as the bare array Eppo serves; its reader
accepts either.

**flagd 0.2.2 stringifies anything nested inside a JSON flag.** Its in-process
resolvers convert a nested array or object to that value's JSON *text*, so
`{"domains": ["a.com"]}` reaches a caller as `{"domains": "[\"a.com\"]"}`.
Scalar members are unaffected. This is an upstream defect, not a SPACE choice,
and it hits the two flags whose shapes need nesting — the known-egress domain
list and the per-client quota map.

Both readers decode their own field, so the flags work through flagd today.
The decoding lives in the readers rather than in the adapter for a reason: a
reader knows its field must be a list or a map, so decoding a string found
there is a typed decision, while the adapter cannot tell a stringified member
apart from a flag whose author meant a string. Writing flag definitions needs
no special care — write the natural nested JSON. The behaviour is pinned by
`tests/flagd_end_to_end.rs`, which reads through real flagd; when upstream
stops stringifying, that test fails and the decoding can be removed.

**`AttributeValue::Null` is dropped.** OpenFeature's evaluation context has no
null, so a null attribute arrives as an absent key and a rule testing for null
does not match. No SPACE flag relies on this today.

Integers and floats stay distinct, which is why `FeatureFlagProvider` has both
`integer_value` and `float_value`: OpenFeature's SPI resolves them through
separate methods, and flagd converts an integer read with `as_i64()`, so a
`0.25` variant read as an integer resolves to nothing rather than rounding.

## Observability

`FeatureFlagClient` emits these two itself, so a dashboard built on them reads
the same whichever backend a deployment runs — and reads at all on a
deployment running none:

- `feature_flags.evaluation{flag_key, outcome}` — on every read.
- `feature_flags.boolean_assignment{flag_key, value}` — on every resolved
  boolean.

`outcome` is the `Unresolved` reason a read carried back, or `assigned`:

| Outcome | What it means |
| --- | --- |
| `assigned` | The provider resolved a value of the requested type. |
| `no_provider` | No provider is installed, so nothing was asked. |
| `provider_unready` | A provider is installed and has no configuration to answer from yet. |
| `unassigned` | The provider answered: this flag has no assignment for this subject. |
| `error` | The provider tried to evaluate and failed. |

The split between `unassigned` and `error` is the one to watch. A flag that is
simply not defined is `unassigned`; `error` means the backend tried and failed,
including reading a flag as a type its definition does not have. Every
deployment has flags it has not defined, so only `error` is worth alerting on.
`provider_unready` is not startup-only either — a provider pointed at an
unreachable source stays there — so page on it persisting, not on seeing it.

The counters live on the client rather than on each provider precisely so
`no_provider` exists: a provider-free deployment emits no provider telemetry by
construction, so a tally owned by the providers would have shown the absence of
a series both for "no backend on purpose" and for "the backend broke".

The Eppo provider emits one more, `feature_flags.unassigned_diagnostic`, which
the OpenFeature adapter does not. It carries Eppo's own reason for an
unassignment, obtained from a second SDK call; OpenFeature reports a missing
flag as one undifferentiated `FlagNotFound`, so there is no equivalent detail
to report and inventing a mapping onto Eppo's vocabulary would misattribute it.
That one stays with the provider because only a backend can produce it.

`FeatureFlagClient::provider_state` answers the same question without a flag
key: `absent`, `unready`, or `ready`. Through the adapter it is the OpenFeature
provider's own `status`, narrowed — and never `absent`, since the adapter
cannot exist without a provider to wrap. flagd leaves `status` at the SPI
default of `Ready`, because an unreachable sync source fails its construction
instead.

## What an operator gives up

flagd has no management console. Compared with Eppo:

- Flag changes are configuration changes — a file or ConfigMap edit, reviewed
  and rolled out like any other, rather than a console toggle.
- There is no built-in audit trail of who changed a flag. Version control of
  the definitions is the substitute, and a better one, but it has to exist.
- There are no experiment or assignment analytics.

Document operational flag changes using the flag key and the configured
provider's update workflow. For example, instructions to adjust
`space-api-client-concurrency-quotas` should point to the deployment's flag
definitions rather than assume a particular provider console.

## Terminology

These get conflated, so, precisely:

- **OpenFeature** is a vendor-neutral flag-evaluation API with a provider SPI.
  It evaluates nothing itself.
- **flagd** is a flag backend with an OpenFeature provider. It is what actually
  resolves a flag.
- **The OpenTelemetry demo** happens to use OpenFeature with flagd. That is a
  demo's choice, not a dependency. OpenTelemetry is not a flag service and is
  not an Eppo equivalent.

None of them is required to run SPACE. A deployment with no flag provider is
supported; it simply cannot change a flag without a redeploy.
