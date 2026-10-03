# Gateway traffic gate

Run the production `space-netgtw-svc` against local network peers and editable
policy cases:

```sh
bash scripts/netgtw/traffic/run.sh
```

The command installs the pinned peer libraries with `uv`, builds the gateway and
its control fixture, then requests `sudo` on a devbox. It needs Linux, Python
3.11+, a preinstalled `uv`, OpenSSL, `iproute2`, `nftables`, `procps`, `mount`,
and `util-linux`, plus the repository's Rust/protobuf build tools. The SPACE devbox
and Buildkite command image supply these dependencies. The CI image pins and verifies its `uv`
binary. The launcher requires `uv` on `PATH` and fails if it is missing; tool
installation belongs to the CI image or developer setup.
No running SPACE services, database, AWS resources, or VM are needed. Reserved
DNS and domain-attribution cases require outbound TCP/853 to Cloudflare
(`1.1.1.1`) and Google (`8.8.8.8`). Domain-attribution cases also depend on
`sslip.io` answering synthetic names with the isolated peer's benchmark address;
application traffic stays local. Cargo and uv may download dependencies.
Peer libraries are isolated in `target/gateway-traffic/venv`. `requirements.txt`
pins the complete dependency set; regenerate it from `requirements.in` with the
`uv pip compile` command recorded at the top of that file.

```sh
# Validate/list without building or root; a typo or empty selection fails.
bash scripts/netgtw/traffic/run.sh --list

# Run one case (name or filename glob).
bash scripts/netgtw/traffic/run.sh --filter '05-*'

# Reuse binaries already built by the command above while editing cases.
NETGTW_TRAFFIC_SKIP_BUILD=1 bash scripts/netgtw/traffic/run.sh
```

## Editing cases

Every `cases/*.toml` file is discovered automatically. Add, edit, or delete a
file; no registry or pipeline list needs updating. Each case starts with closed
policy and cleared DNS attribution. Steps execute in order on the same gateway,
and a step's probes execute in order after its policy is installed through the
real control RPC. Use a preceding DNS probe when a domain permit needs evidence.

```toml
name = "Only the named service is reachable"

[[steps]]
policy = { default = "deny", allow = ["1.1.1.1", "api.198-18-1-2.sslip.io"] }
probes = [
  { kind = "dns-udp", target = "1.1.1.1", host = "api.198-18-1-2.sslip.io", expect = "answer" },
  { kind = "tls", host = "api.198-18-1-2.sslip.io", expect = "allow" },
  { kind = "tls", host = "other.example.test", expect = "deny" },
]
```

Policy `default` is `allow` or `deny`. Optional `allow` and `deny` arrays accept
IP addresses, CIDRs, exact domains, and `*.example.test` wildcards. Expectations
are written explicitly; the runner does not derive them from the policy evaluator.
Unknown fields, unsupported fixture destinations/ports, invalid rules, duplicate
case names, and empty inventories fail before building.

An inspected-TLS case may add one `credential` table to its policy. The runner
generates the value at runtime; the table names only its exact host, ports, path,
methods, and header. A probe with `expect_credential = "authorization"` requires
every origin request to contain that value. Origin receipts redact it and record
only which headers matched, so neither results nor logs contain the credential.

| Probe `kind` | Default destination/port | Expected result |
| --- | --- | --- |
| `tcp` | `198.18.1.2:9000` | `allow` / `deny`, byte echo |
| `http` | `198.18.1.2:80` | `allow` / `deny`, HTTP/1.1 `host`, keep-alive and chunked body echo |
| `tls` | `198.18.1.2:443` | `allow` / `deny`, verified TLS SNI `host` and HTTP/1.1 echo |
| `h2c` | `198.18.1.2:8080` | `allow` / `deny`, plaintext HTTP/2 with prior knowledge |
| `http2` | `198.18.1.2:8443` | `allow` / `deny`, verified TLS with required `h2` ALPN |
| `grpc`, `grpcs` | `198.18.1.2:50051`, `:50052` | `allow` / `deny`, plaintext / verified TLS gRPC unary and bidirectional streaming |
| `ws`, `wss` | `198.18.1.2:8081`, `:8444` | `allow` / `deny`, plaintext / verified TLS WebSocket upgrade, messages, ping/pong and close |
| `quic` | `198.18.1.2:443` (UDP) | `allow` / `deny`, verified QUIC handshake and concurrent bidirectional streams |
| `connect` | `198.18.1.2:8445` | `allow` / `deny`, authenticated HTTPS CONNECT with verified inner HTTPS |
| `tls-echo` | `198.18.1.2:9443` | `allow` / `deny`, TLS SNI `host` and byte echo |
| `udp` | `198.18.1.2:9000` | `allow` / `deny`; also accepts `port = 443` and a `sizes` array for multiple datagrams on one socket |
| `dns-udp`, `dns-tcp` | `198.18.1.2:53` | `answer` (A = `198.18.1.2` unless `addresses` is specified), `refused` (RCODE 5), or transport `deny` |
| `ingress` | guest `169.254.0.21:9000` | `allow` / `deny`, signed ServicePortIngress RPC and guest echo |

Egress `target` can be `198.18.1.2`, `198.18.2.2`, `198.18.53.53`, `1.1.1.1`,
`8.8.8.8`, or `169.254.169.254`. All are assigned inside the disposable network;
even the metadata address is a test peer. Ordinary DNS fixtures answer every A
query with `198.18.1.2`. Generic UDP/TCP DNS replies are relayed but never populate
the gateway's DNS attribution cache. Case `03` checks the production boundary:
a successful generic reply must leave HTTP/TLS denied; a subsequent reserved
DNS reply for the same name and address must permit them.

Domain cases resolve `*.198-18-1-2.sslip.io` through the reserved resolvers and
require the answer `198.18.1.2`. This preserves exact/wildcard domain admission,
hostname mismatch and policy-refresh coverage without trusting a guest-selected
fixture resolver or adding a test-only gateway trust configuration. Resolver
and authoritative-DNS failures fail these cases; they are not marked flaky.
Only resolver IPs are independently allowed, not the application peer address.

Guests using custom resolvers must use reserved DNS for domain-only egress and
hostname-based TLS interception exemptions. Resolver transport still needs
ordinary IP admission. Independent IP/default egress permissions continue to
work, but do not supply the hostname binding needed for an exemption. Without
trusted attribution, normal interception applies and certificate-pinned clients
may fail. IP exemptions are independent of DNS evidence. The gateway's existing
TLS-exemption unit table covers bound/unbound hosts with IP and default access.

Reserved DNS is resolved through the real public upstreams. Its positive probes
query `one.one.one.one` and assert `addresses = ["1.0.0.1", "1.1.1.1"]`, ignoring
answer order. HTTPS/opaque TLS peers use generated certificates verified by the
test clients. The default-off suite supplies no gateway fixture trust roots.
The opt-in [TLS authority suite](tls-authority/README.md) uses separate guest
and scoped upstream trust through the production configuration. All HTTP, gRPC,
WebSocket, and QUIC probes require `host`.

HTTP/2 opens three streams concurrently with binary bodies larger than the
initial stream and connection flow-control windows, checking full replies and
stream completion. The TLS client requires `h2` ALPN, with no HTTP/1 fallback.
TLS probes may set `alpn` plus `downstream_alpn` and `upstream_alpn` to assert
the independently negotiated protocols on an intercepted connection.
gRPC uses grpcio clients and a grpcio server with protobuf `BytesValue` messages,
checking unary replies, streaming message order/content, and final OK trailers.
WebSockets check text and fragmented binary messages, pong contents, and a clean
close handshake. WebSocket deny probes may set `http_status` to an integer from
400 through 599 to require that exact response; a missing HTTP status fails the
probe. Without it, transport denial or HTTP 403 is accepted, but other HTTP
statuses fail. Every denial still requires no upstream traffic.
QUIC uses aioquic with verified TLS and the `space-traffic-echo`
ALPN, three concurrent streams, FIN and ping completion; this is QUIC stream
coverage, not HTTP/3. Plain UDP/443 echo probes remain separate from QUIC.

Existing UDP probes send a single nonce. `sizes = [64, 1200, 8192]` sends three
binary datagrams on the same socket and requires an exact reply to each, including
one larger than the veth MTU. Sizes must be integers from 32 through 65507 bytes.
The default/address cases include this sequence in addition to the original
single-datagram probes.

For ingress, `token` accepts `valid` (default), `missing`, `wrong-port`,
`wrong-sandbox`, `stale-route`, or `invalid-signature`. An optional integer `code`
asserts the exact gRPC status. Normal bad-token denials require PermissionDenied,
so a transport outage cannot satisfy them. The runtime identity check is enabled.

Use `state = "no-identity"` or `state = "policy-error"` on a step to drive those
production transitions. The latter makes the host-control fixture unavailable
while requesting a new desired policy reference. `tcp` and `tls-echo` support
`session = "name", connection = "new"` followed by `connection = "existing"`
in a later probe. An existing-session check never silently reconnects. See
`05-policy-refresh.toml` and `06-fail-closed.toml` for revocation examples.

### Established connections

The UDP/QUIC allow probes require payload delivery after the first reply;
UDP/443 probes also check the packaged pre-TPROXY counter increments.
The [established-connection bypass](../../../crates/libs/node/mgr/server/src/firewall.rs)
is restricted to reply-direction traffic so later outbound datagrams still
reach the gateway proxy.

The worker image packages `docker/space/nftables/sandbox-pre-tproxy-guardrails.json` under `/etc/space/nftables`.
Its IPv4 UDP/443 counter is observation-only and applies on slot setup/reset, not live gateway restart.
The existing 15-second network metrics loop emits `node_mgr.guest_udp443_packets`, a gauge summing current kernel counters across active slots per `action:observe|reject` (from the rule verdict), without sandbox tags.
These count conntrack-new packets, including retries, rather than unique flows. Use Datadog `monotonic_diff()` or rate calculations for volume visibility; slot resets/removals lower the sum, so the difference misses that interval.

### UDP/443 policy rollback

Startup validation fences idle slots whose UDP/443 rule differs from the image's
packaged policy; the existing repair path rebuilds them before reuse. Slot setup
and reset restore the default `net.ipv4.icmp_ratemask=6168`; readiness validation
also repairs mask-only drift. Before enabling a rejecting policy, deploy and
record a #253-or-later observation image digest as its rollback target. Older
observation images do not repair stale rejecting namespaces.

Leased namespaces are not rebuilt automatically. Idle repair also does not run
while the worker is drained: its loop returns before `recover_startup_idle_slots`.
After rolling back to that recorded image digest, drain new allocations with
`space-nodemgr drain`, then remove the reject in **every affected idle and leased
namespace** from the node-manager's mount namespace:

```sh
set -euo pipefail
ns=sns-000001
ip netns exec "$ns" nft -j list chain ip sandbox pre_tproxy_guardrails |
  jq '{nftables: [.nftables[].rule? |
    select(.comment == "platform-egress-pre-tproxy-guardrail-quic-udp443") |
    .expr |= map(select(has("reject") | not)) | {replace: {rule: .}}]}' |
  ip netns exec "$ns" nft -j -f -
ip netns exec "$ns" sysctl -qw net.ipv4.icmp_ratemask=6168
ip netns exec "$ns" nft -a list chain ip sandbox pre_tproxy_guardrails
ip netns exec "$ns" sysctl -n net.ipv4.icmp_ratemask
```

Completion requires per-namespace readback of the counter-only rule and default
mask for every affected idle and leased namespace. The degraded count does not
prove rollback is complete. Verify before `space-nodemgr undrain` (leave
previously drained nodes drained). The replacement preserves matches, ordering,
and counters.

### Firewall rollout verification

The reply-direction restriction fixes a packet-level egress-policy bypass as well
as repeated-socket timeouts. A denied TCP SYN could previously reach upstream;
the recorded reproduction did not complete a TCP handshake or leak application
payload. Updating the node-manager binary alone does not refresh every running
namespace. Rules are reapplied during gateway launch or namespace setup/reset.

From each node-manager's mount namespace, with root access to its named network
namespaces, run this read-only audit from the repository checkout:

```sh
sudo python3 scripts/netgtw/audit-reply-rules.py
# Limit the audit to specific active sandbox namespaces:
sudo python3 scripts/netgtw/audit-reply-rules.py sns-000001 sns-000002
```

The audit checks for both deprecated broad exceptions, including duplicates.
Exit 0 means neither owned bypass is present; exit 1 reports retained or
unreadable rules; exit 2 means namespace discovery failed or found no sandboxes.
A namespace changing during the scan may require a second check. The audit does
not change rules or prove that an equivalent bypass exists under another name,
so packet checks remain the independent behavior oracle.

Use the normal rollout lifecycle to constrain affected namespaces, then rerun
the audit and repeated-socket traffic checks. Slot-ready validation does not
verify this boundary continuously. Audit success proves that the superseded
owned rules are absent; packet checks prove the routing behavior.

### Inbound reply boundary regression

Direct DNAT ingress is disabled while hosts need this containment. The
authenticated service-port ingress stream is the supported inbound path: its
TCP connection originates from the sandbox-local gateway address, and only the
established reply to that local address skips TPROXY. That packet routes to a
local socket and cannot take the forwarded fragmentation path. Other guest
TCP/UDP traffic enters TPROXY, and other forwarded guest traffic fails closed.

The following bounded Linux regression installs the production Rust firewall
rules in disposable network, mount, and PID namespaces. It checks TCP/UDP
outbound blocking with no gateway listening, direct inbound reply suppression,
local service-style TCP ingress, legacy/duplicate rule upgrades, the rollout
audit, and preserved ingress rules.
It also runs the real slot-pool startup worker and SQLite migrations, rebuilding
idle namespaces seeded with the opposite UDP/443 stage and with mask-only drift,
while leaving a current slot unchanged. Expected verdict and mask come from the
packaged stage: observation/6168 or reject/6160. Namespace inodes must stay the
same, proving in-place repair:

```sh
cargo build --locked --profile dev-release -p pplx-space-node-mgr --example firewall-reply-fixture
sudo python3 scripts/netgtw/tests/reply_boundary.py target/dev-release/examples/firewall-reply-fixture
```

It needs `ip`, `nft`, `unshare`, `mount`, `sysctl`, `timeout`, `/dev/net/tun`,
and the `/space` mountpoint provided by the SPACE CI image/devbox. All namespaces,
mounts, and processes are released on exit; it uses no VM, AWS, or cluster. This
is a firewall-boundary regression, not an end-to-end gateway policy test. The
traffic suite uses separate shell rules; Rust unit tests cover the production
proxy rule generator and real nft JSON round-tripping.

### Plaintext upgrades

Plaintext hostname-bound connections do not support protocol upgrades. The
WebSocket probe in `03-domain-rules.toml` therefore expects HTTP 403 with no
upstream connection or payload, even after trusted DNS attributes the allowed
name to the destination. A transport failure or HTTP 502 cannot satisfy this denial.
The adjacent WSS probe continues to require a successful WebSocket exchange.

## Evidence and isolation

The runner reuses `../bench/topology.sh`: guest, gateway, and upstream network
namespaces connected by veth interfaces, with real nftables TPROXY and outbound
socket marks. Forwarding is disabled; only the gateway's sockets reach upstream.
It launches `space-netgtw-svc` with `ip netns exec ngb-gw` and checks the running
process's `/proc/<pid>/ns/net` identity against that namespace. The client,
gateway and upstream namespaces must have distinct identities. `topology.json`
records these identities, the gateway PID and the installed nftables rules.
An outer mount, network, and PID namespace isolates fixed namespace names,
`/run/netns`, and the production host-control socket under `/space`. Concurrent
runs are independent. Namespace teardown kills descendants even on cancellation;
runtime sockets and generated keys are removed, and logs are returned to the
calling user's ownership. An unprivileged relay outside the network namespace
accepts only a run-owned Unix socket and forwards TCP/853 to the two fixed public
resolvers. The launcher stops it and removes its socket on completion or cancellation.

Only external peers and the host-control policy source are fixtures. Production
policy parsing, hydration, authorization, DNS attribution, proxying, revocation
hooks, and authenticated ingress execute in the real service binary. The gateway
is built separately with its normal Cargo command so fixture features cannot
affect it. Reserved DNS uses the production DNS-over-TLS client through the
transparent byte relay. TLS remains end to end between the gateway and the public
resolver, with the production endpoint names and compiled public trust roots.
The default suite uses the production trust settings; the TLS authority suite
uses the explicitly scoped private-origin CA configuration described above.

Each probe first checks the same peer directly; reserved DNS health checks use
verified DNS-over-TLS through the relay. Allowed probes must receive
their unique echo and show its receipt at the correct peer; TCP receipts are
reassembled by connection. Denied probes must show no new peer connection or
payload, even when the client times out. QUIC receipts include every incoming
datagram before handshake parsing, so a leaked Initial cannot pass a deny probe.
DNS refusals must not forward the denied question or send bytes/connect to a
DNS-over-TLS upstream. Missing cases and fatal setup failures always fail the
suite. Per-probe failures, including unavailable resolvers, broken peers,
malformed responses, and bad certificates, fail unless the probe is marked
`flaky = true`. Marked probes still run and retain their failure details, but
any per-probe failure is reported as `FLAKY` and a JUnit skip instead of
failing the suite. Remove the two current markers once their external health
check is reliable.

Results go to a unique `target/gateway-traffic/run.*` directory: `results.json`,
`junit.xml`, gateway/fixture logs, and `events.jsonl`. `manifest.json` records the
commit, binary hashes, and exact selected TOML files. Each probe prints its
result and elapsed time. The manifest also hashes the pinned peer dependencies.
These are correctness timings; use the adjacent [performance bench](../bench/README.md)
for throughput/CPU comparisons.

## CI and scope

To use this suite as a CI gate, run the command above on a Linux worker with
the documented prerequisites. Preserve the runner's nonzero exit status and
collect its logs and reports as build artifacts. Run workspace tests
separately: this suite checks gateway traffic behavior, not all application
behavior.

Coverage includes IPv4 address/default precedence, domain/DNS binding, wildcard
allows, hostname mismatches, HTTP/1.1, HTTP/2, gRPC, WebSockets, real QUIC,
TCP/UDP transport behavior, reserved DNS, policy updates, missing identity,
failed hydration, platform metadata denial, and ingress
token/route checks. Domain-dependent traffic needs matching DNS and protocol
evidence; independent address/default allows preserve transparent traffic.
TLS HTTP/2, gRPC and
WebSocket probes exercise SNI/DNS admission. Domain-only permits reject plaintext
HTTP/2/gRPC and QUIC. Plaintext WebSocket upgrades under domain-only admission
are refused before any upstream connection.

This gate does not exercise Firecracker, Kubernetes, deployed node rule
installation, IPv6, HTTP/3, TLS MITM, or future DNS-expiry lifetime guarantees.
As in the performance bench,
the pre-TPROXY platform drop chain is omitted so metadata-denial cases test the
gateway's own guardrail. Deployed smoke tests remain responsible for the full
sandbox path.
