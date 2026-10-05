# TLS authority regression gate

The shared-edge TLS authority bypass uses an allowed TLS SNI with a forbidden
encrypted HTTP authority on the same edge IP. This deterministic reproduction
exercises that boundary in the actual
`space-netgtw-svc`, using the [traffic harness](../README.md)'s real TPROXY
network namespaces. The upstream records both parsed authorities and exact
nonce-bearing payloads. No VM or database is required. The runner downloads a pinned public development
certificate before starting isolated peers; application traffic stays local.

## Reproduce and compare

```sh
# Build once and run the suite with inspection enabled through policy.
python3 scripts/netgtw/traffic/tls-authority/run.py

# Prove credential replacement through the production HTTP/1.1 and HTTP/2 data plane.
NETGTW_TRAFFIC_SKIP_BUILD=1 \
  python3 scripts/netgtw/traffic/tls-authority/run.py --filter '04-*'

# Compare a separately built, original production gateway with the same peers.
NETGTW_TRAFFIC_SKIP_BUILD=1 \
NETGTW_TRAFFIC_GATEWAY=/absolute/path/to/original/space-netgtw-svc \
  python3 scripts/netgtw/traffic/tls-authority/run.py
```

The original binary used during development is from SPACE `8b62d0ce`. Build it
in a detached worktree with `cargo build --locked -p pplx-space-net-gateway-svc
--bin space-netgtw-svc` and a separate `CARGO_TARGET_DIR` to preserve both binaries.
The fixture executable always comes from the current tree. `--filter '01-*'`
selects the shared-edge cases; `02-*` selects CONNECT; `03-*` selects revocation.
CI runs the existing suite plus this one.

These inspected-TLS cases independently allow the isolated origin IP. They do
not use generic DNS answers as attribution: the pinned public development
identity does not resolve to the isolated peer through trusted public DNS.
Independent address admission permits either initial SNI, but the encrypted
HTTP authority must still match that SNI. The suite retains first/later
authority mismatch denials, including switches to another allowed hostname,
and closes inspected connections when their IP permission is revoked.
The main traffic suite's `03-domain-rules.toml` and `05-policy-refresh.toml`
own domain-only SNI admission and DNS-attributed connection revocation using
real reserved DNS. No production trust or hostname check is bypassed here.

The suite requires normal traffic to work while forbidden
authority switches fail. A rejected first authority must not create an upstream
connection. Reused HTTP/1.1 and concurrent HTTP/2 checks require allowed requests
to finish and forbidden headers and bytes to be absent at the peer. Both names
are separately policy-allowed in the coalescing case, so that case cannot pass
merely through a domain allowlist check.
Positive HTTP receipts also require the original wire authority: adding `:443`
to an otherwise valid Host header can break signed requests.

| Behavior | Required positive evidence | Required enforcement evidence |
| --- | --- | --- |
| HTTP/1.1 | Fixed-length and 96 KiB chunked echoes on one TLS connection | Reject a forbidden first or later Host |
| HTTP/2 | Three concurrent 96 KiB streams, WINDOW_UPDATE and full replies | Reject a switched authority, including another allowed origin |
| WebSocket | Verified TLS upgrade, text, fragmented binary, ping/pong and close | Bind the opening request; close active streams on policy invalidation |
| gRPC | grpcio unary, client/server/bidirectional streams, binary trailers, trailers-only error, cancellation, deadline, subsequent RPC | Preserve HTTP/2 and use its per-stream authority gate |
| HTTPS CONNECT | curl proxy authentication, outer TLS, inner TLS and a 96 KiB inner HTTP echo | Authorize the outer proxy route; preserve authentication challenges and opaque tunnel forwarding |
| Revocation | Open HTTP/1.1, HTTP/2 and WebSocket sessions | Observe actual EOF within two seconds of policy change; timeout is a test failure |
| Credential injection | Guest sends an unrelated value; the exact origin receives the runtime-generated credential over HTTP/1.1 and concurrent HTTP/2 streams | Every origin receipt matches without recording the credential value |

Origin receipts require the exact application authentication header, including
`Fastly-Key`.

CONNECT clients offer both HTTP versions inside the tunnel. The authentication
probe requires every proxy challenge in order and authenticates with curl's
`--proxy-anyauth` when the supported scheme is the second challenge.

The fixture uses independent guest-interception and upstream PKIs. It implements
the real HostControl gateway-intermediate issuance RPC; the production gateway
generates its own keys and leaf certificates. Origin peers serve the public
`*.127-0-0-1.dev` development identity published by
[AppCove](https://github.com/appcove/127-0-0-1.dev) under BSD-2-Clause. The runner
pins the source revision and SHA-256 of the chain, key and license. The pinned
certificate expires **2026-11-18**; renewal requires updating the reviewed pins.
An expired or unverifiable identity fails the suite. Downloads and private keys
stay in temporary directories and are removed after the run.

The gateway verifies these peers with its existing public trust roots. The peer
certificate arguments only configure fixture servers and never reach the gateway.
The suite adds no production trust configuration. The unchanged default traffic
suite still generates its own test identity and runs with inspection disabled.

## Authenticated CONNECT compatibility

Clients can place authenticated HTTPS proxy URLs in `HTTPS_PROXY`. A proxy
can authenticate CONNECT, terminate the inner TLS session, and inject credentials.

SPACE checks the outer proxy with its usual TLS, policy and DNS-attribution
rules. It forwards CONNECT authentication and preserves all proxy challenges.
After a successful CONNECT, it relays bytes without terminating inner TLS or
checking the inner HTTP authority. The inner destination and the route selected
by the proxy are outside SPACE's inspection guarantee. An allowed proxy can
therefore provide access to destinations that SPACE would reject on a direct
connection. This preserves opaque CONNECT forwarding while inner inspection is
deferred; no trust flags or local explicit-proxy listener are added.

SPACE policy must permit the proxy. With policy `tls_interception: on`, guest
clients must trust SPACE's interception root for the outer HTTPS connection.
Some clients select `SSL_CERT_FILE` containing only the upstream proxy CA; any
override used to verify the outer connection must also include SPACE's root. Inside the tunnel, certificate trust
remains between the guest and upstream proxy. SPACE does not need the upstream
proxy's CA.

The protocol suite checks proxy authentication, complete inner HTTP payloads,
changed inner authorities and outer-proxy denials. A Rust regression additionally
exchanges real TLS signed by a separate private CA trusted only by the guest.
These are local fixtures, not a test of a live deployment or guest image.
Outer HTTP/2 CONNECT and IP-literal targets remain unsupported. Direct inspected
TLS still does not support upstream mTLS or certificate pinning. The policy's
`tls_interception` field selects inspection and defaults to `off`.

## Bounds, diagnosis and evidence

Both TLS legs have a ten-second setup budget and share eight nonqueued handshake
slots. Hyper bounds headers to 32 KiB, HTTP/1 headers to 128, HTTP/2 concurrent
streams to 32 and send buffers to 64 KiB. HTTP/1 headers have a ten-second
read deadline. Upgrade setup is bounded; WebSockets have a 60–70 second inactivity
limit. Certificate policy/execution/CA validity fences the complete lifetime,
including upstream drivers, CONNECT and WebSocket tasks. No inspected origin
pool spans guest connections. Bodies and trailers stream through Hyper.

For failure outcomes and trace correlation, use the sandbox,
connection ID and policy reference on `tls.server`; then distinguish request
refusals (`tls_http`) from TLS setup/validity failures (`tls_transport`) and
CONNECT relay failures (`tls_connect`). Stage, I/O kind and bounded TLS/HTTP reason
identify certificate, protocol, timeout and peer failures without exporting
headers, tokens, payloads or certificate secrets.

Each run emits a binary-hash manifest, topology identity, peer receipts,
`results.json`, JUnit and gateway logs under `target/gateway-traffic/run.*`.
The artifacts record the validated binaries and checks.
A mutation run disables the central authority comparison and requires the
coalescing and inner CONNECT cases to fail, then restores and rebuilds the binary.
