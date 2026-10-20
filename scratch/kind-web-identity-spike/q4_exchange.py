"""SPIKE: one ExchangeToken on a sandbox worker agent's volume socket, as
volume-manager asks, printing the status, the credmgr reason metadata and
the detail. Reuses the smoke harness's relay (space_smoke/credmgr_socket.py).
Usage: q4_exchange.py <node> <subject> <resource-arn>"""

import subprocess
import sys

import grpc

from space_smoke.credmgr_socket import EXCHANGE_TOKEN_METHOD, encode_exchange_request, relay_argv, relayed_channel

node, subject, resource = sys.argv[1:4]
kubectl = ["kubectl", "--context", "kind-space-local", "-n", "space"]
pod = subprocess.check_output(
    [*kubectl, "get", "pods", "-l", "app.kubernetes.io/name=space-node-debug",
     f"--field-selector=spec.nodeName={node}", "-o", "jsonpath={.items[0].metadata.name}"], text=True)
argv = relay_argv(kubectl, pod, "shell", "/space/run/volume-credentials/credmgr.sock",
                  user="space-volmgr", groups=("space-volcredclients",))
with relayed_channel(argv) as channel:
    call = channel.unary_unary(EXCHANGE_TOKEN_METHOD, request_serializer=lambda m: m, response_deserializer=lambda m: m)
    try:
        call(encode_exchange_request(resource=resource, scope="space.storage.read", subject=subject), timeout=90)
        print("status=OK")
    except grpc.RpcError as error:
        reason = dict(error.trailing_metadata() or ()).get("x-space-credmgr-error-reason", "-")
        print(f"status={error.code().name} reason={reason} detail={error.details()!r}")
