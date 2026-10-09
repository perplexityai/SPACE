#!/usr/bin/env python3
"""
Quick sandbox artifact DEK round-trip against a local credmgr authority UDS.

The authority needs database access, and SANDBOX_ID must name an existing
sandbox whose organization has BYOK enabled. credmgr resolves the KEK and AAD
from that sandbox; the caller names nothing else.

Must run as root (to seteuid into a volume-manager identity before connecting,
so the credmgr's UdsPeerAuthorizer accepts the caller):

  pip install grpcio grpcio-tools
  sudo python3 scripts/tests/test-credmgr-dek.py SANDBOX_ID [SOCKET] [--as space-volmgr]
"""
import argparse
import os
import pwd
import sys
import tempfile
import uuid
from pathlib import Path

import grpc
from grpc_tools import protoc

# Hardcoded fallbacks matching crates/libs/common/core/.../linux_identities.rs
# (canonical source: infra/space/identity/node-identities.sh).
FALLBACK_UIDS = {"space-sbxmgr": 950, "space-volmgr": 964}

ap = argparse.ArgumentParser()
ap.add_argument("sandbox_id", type=uuid.UUID)
ap.add_argument("socket", nargs="?", default="/space/run/credential-manager.sock")
ap.add_argument("--as", dest="as_user", default="space-sbxmgr",
                choices=["space-sbxmgr", "space-volmgr"])
args = ap.parse_args()

if os.geteuid() != 0:
    sys.exit("must run as root so we can seteuid into a credmgr-allowed service user")

try:
    pw = pwd.getpwnam(args.as_user)
    uid, gid = pw.pw_uid, pw.pw_gid
except KeyError:
    uid = FALLBACK_UIDS[args.as_user]
    gid = uid
    print(f"warn: user {args.as_user} not in /etc/passwd, using fallback uid={uid}")

# Drop GID first (can't change egid after seteuid drops root). Keep saved
# uid=0 via setresuid so the credmgr-allowed identity sticks around for
# every socket the gRPC channel opens.
os.setresgid(gid, gid, 0)
os.setresuid(uid, uid, 0)
print(f"running as {args.as_user} (uid={uid}, gid={gid})")

REPO = Path(__file__).resolve().parents[2]
PROTO_DIR = REPO / "proto"
PROTOS = [
    PROTO_DIR / "credential/manager/v1/credential_manager.proto",
    PROTO_DIR / "credential/manager/v1/network_credential.proto",
]

out = Path(tempfile.mkdtemp(prefix="credmgr-py-"))
if protoc.main(["protoc", f"-I{PROTO_DIR}", f"--python_out={out}",
                f"--grpc_python_out={out}", *map(str, PROTOS)]) != 0:
    sys.exit("protoc failed")
for d in ["credential", "credential/manager", "credential/manager/v1"]:
    (out / d / "__init__.py").touch()
sys.path.insert(0, str(out))

from credential.manager.v1 import credential_manager_pb2 as pb        # noqa: E402
from credential.manager.v1 import credential_manager_pb2_grpc as rpc  # noqa: E402

# Override :authority — gRPC otherwise URL-encodes the socket path into
# it (`space%2Frun%2F...`), which h2 rejects as a malformed authority.
channel = grpc.insecure_channel(
    f"unix:{args.socket}",
    options=[("grpc.default_authority", "localhost")],
)
client = rpc.CredentialManagerStub(channel)
sandbox = str(args.sandbox_id)

print(f"→ IssueSandboxArtifactDek  sandbox={sandbox}")
issued = client.IssueSandboxArtifactDek(pb.IssueSandboxArtifactDekRequest(sandbox_id=sandbox))
print(f"  plaintext_dek={len(issued.plaintext_dek)}B  wrapped_dek={len(issued.wrapped_dek)}B")

print("→ UnwrapSandboxArtifactDek")
unwrapped = client.UnwrapSandboxArtifactDek(pb.UnwrapSandboxArtifactDekRequest(
    sandbox_id=sandbox, wrapped_dek=issued.wrapped_dek,
))
assert unwrapped.plaintext_dek == issued.plaintext_dek, "round-trip mismatch"
print("✓ round-trip matches")

print("→ UnwrapSandboxArtifactDek for another sandbox (must fail)")
try:
    client.UnwrapSandboxArtifactDek(pb.UnwrapSandboxArtifactDekRequest(
        sandbox_id=str(uuid.uuid4()), wrapped_dek=issued.wrapped_dek,
    ))
    sys.exit("✗ cross-sandbox unwrap unexpectedly succeeded")
except grpc.RpcError as e:
    assert e.code() in (grpc.StatusCode.PERMISSION_DENIED, grpc.StatusCode.NOT_FOUND), e.code()
    print(f"  got {e.code().name}: {e.details()}")
print("✓ all good")
