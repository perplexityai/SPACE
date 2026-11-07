#!/usr/bin/env python3
"""
Quick IssueDek / UnwrapDek round-trip against a local credmgr UDS.

Must run as root (to seteuid into space-sbxmgr before connecting, so the
credmgr's UdsPeerAuthorizer accepts the caller as Service(space-sbxmgr)):

  pip install grpcio grpcio-tools
  sudo python3 scripts/tests/test-credmgr-dek.py [SOCKET] [--as space-tmplmgr]

Override a registered backend/key with the PROVIDER and KEY environment variables.
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

# Hardcoded fallbacks matching crates/l20_domain/host/layout/.../linux_identities.rs
# (canonical source: infra/space/identity/node-identities.sh).
FALLBACK_UIDS = {"space-sbxmgr": 950, "space-tmplmgr": 955}

ap = argparse.ArgumentParser()
ap.add_argument("socket", nargs="?", default="/space/run/credential-manager.sock")
ap.add_argument("--as", dest="as_user", default="space-sbxmgr",
                choices=["space-sbxmgr", "space-tmplmgr"])
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
PROVIDER = os.environ.get("PROVIDER", "in-memory")
KEY = os.environ.get("KEY", "test-key-1")

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
org = str(uuid.uuid4())
aad = b"sandbox=test/record=1"

print(f"→ IssueDek  org={org} provider={PROVIDER} key={KEY}")
issued = client.IssueDek(pb.IssueDekRequest(
    organization_id=org, provider=PROVIDER, master_key_identifier=KEY,
    spec=pb.DEK_SPEC_AES_256, aad=aad,
))
print(f"  plaintext_dek={len(issued.plaintext_dek)}B  wrapped_dek={len(issued.wrapped_dek)}B")

print("→ UnwrapDek")
unwrapped = client.UnwrapDek(pb.UnwrapDekRequest(
    organization_id=org, provider=PROVIDER, master_key_identifier=KEY,
    wrapped_dek=issued.wrapped_dek, aad=aad,
))
assert unwrapped.plaintext_dek == issued.plaintext_dek, "round-trip mismatch"
print("✓ round-trip matches")

print("→ UnwrapDek with wrong AAD (must fail PERMISSION_DENIED)")
try:
    client.UnwrapDek(pb.UnwrapDekRequest(
        organization_id=org, provider=PROVIDER, master_key_identifier=KEY,
        wrapped_dek=issued.wrapped_dek, aad=b"different",
    ))
    sys.exit("✗ wrong-AAD unwrap unexpectedly succeeded")
except grpc.RpcError as e:
    assert e.code() == grpc.StatusCode.PERMISSION_DENIED, e.code()
    print(f"  got {e.code().name}: {e.details()}")
print("✓ all good")
