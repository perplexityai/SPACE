# Inspected HTTP upstream lifetime

Each guest session retains its admitted destination, hostname binding, certificate
lease and strict-TLS configuration. Its upstream sender is replaceable, not a
shared connection pool. Origin socket closure retires only that sender generation;
an older socket cannot drain a newer generation. Retirement does not cancel the
old driver or active HTTP/2 response streams. Successful upgrades retain their
relay ownership and drain the guest when the relay ends.

Ordinary requests remain untouched until sender readiness and the final current
hostname/DNS binding and credential/lease fences succeed. A failed readiness check
can replace the upstream before the request's first handoff. Concurrent
initialization is singleflight.
Each request can cross at most 32 failed-readiness retirements (the guest HTTP/2
concurrency limit), within one ten-second setup/readiness budget. Failed or
cancelled initialization drains the guest instead of allowing queued requests to
start a reconnect storm. The strict-TLS connector keeps its existing shared
TCP/handshake deadline and lease checks on every dial.

There is **no request replay** after entering Hyper's send operation, including
requests Hyper might return through its separate recovery API. Readiness can race
connection retirement, so the first request encountering an HTTP/2 GOAWAY or an
active abort can still fail. Errors after response headers remain body failures;
they do not restart the request. This removes queued pre-handoff failures from
lawful HTTP/1 retirement, not every upstream 502. CONNECT remains an opaque relay
with its existing request handling, not a retryable ordinary HTTP request.
