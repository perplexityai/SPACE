"""Owned real-KMS fixture bootstrap; behavioral assertions live in typed scenarios.

Use the test dependency group for boto3. The journal contains resource identifiers,
never credentials, HMAC outputs or private keys. Caller supplies the account;
there is no production account or credential default.
"""

import argparse
import base64
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from datetime import datetime
import json
import hashlib
import hmac
import os
from pathlib import Path
import re
import secrets
import struct
import subprocess
import tempfile
import time
import uuid
from typing import Any

from space_smoke.anchor_recovery import Guard, Kubectl


RUN_TAG = "space-smoke-anchor-run"
OWNER_TAG = "space-smoke-fixture"
OWNER = "sandbox-anchor-hardware"
CATALOG = "space-anchor-epochs"
CREDENTIALS = "space-anchor-kms-credentials"


class HardwareFixture:
    def __init__(self, path: Path, profile: str, expected_account: str) -> None:
        import boto3
        from botocore.config import Config

        if not re.fullmatch(r"[0-9]{12}", expected_account):
            raise ValueError("an explicit 12-digit test account is required")
        self.path = path
        self.session = boto3.Session(profile_name=profile)
        self.client_config = Config(
            connect_timeout=2, read_timeout=5,
            retries={"mode": "standard", "total_max_attempts": 1},
        )
        self.identity = self.session.client("sts", config=self.client_config).get_caller_identity()
        if self.identity["Account"] != expected_account:
            raise RuntimeError("refusing AWS fixture in an unexpected account")
        self.account = expected_account
        self.iam = self.session.client("iam", config=self.client_config)
        self.state: dict[str, Any] = json.loads(path.read_text()) if path.exists() else {}
        if self.state and (
            self.state.get("account") != expected_account
            or self.state.get("version") != 1
            or not re.fullmatch(r"[0-9a-f]{16}", str(self.state.get("run", "")))
        ):
            raise RuntimeError("invalid or foreign hardware fixture journal")

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(mode="w", dir=self.path.parent, delete=False) as file:
            temporary = Path(file.name)
            os.chmod(temporary, 0o600)
            json.dump(self.state, file, sort_keys=True, indent=2)
            file.flush()
            os.fsync(file.fileno())
        temporary.replace(self.path)

    def kms(self, region: str) -> Any:
        return self.session.client("kms", region_name=region, config=self.client_config)

    def owned_key(self, arn: str) -> Any:
        parts = arn.split(":")
        if len(parts) != 6 or parts[4] != self.account:
            raise RuntimeError("refusing a key outside the configured account")
        client = self.kms(parts[3])
        tags = {
            item["TagKey"]: item["TagValue"]
            for item in client.list_resource_tags(KeyId=arn)["Tags"]
        }
        if tags.get(RUN_TAG) != self.state["run"] or tags.get(OWNER_TAG) != OWNER:
            raise RuntimeError("refusing a key not owned by this fixture")
        return client

    def wait_enabled(self, arn: str) -> None:
        client = self.owned_key(arn)
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            metadata = client.describe_key(KeyId=arn)["KeyMetadata"]
            if (
                metadata["KeyState"] == "Enabled"
                and metadata["KeySpec"] == "HMAC_384"
                and metadata["KeyUsage"] == "GENERATE_VERIFY_MAC"
                and metadata["Origin"] == "AWS_KMS"
                and metadata["MultiRegion"]
            ):
                return
            time.sleep(1)
        raise RuntimeError("owned HMAC key did not become ready")

    def provision(self, primary: str, replica: str) -> None:
        if self.state:
            raise RuntimeError("journal already exists; refresh or clean the existing fixture")
        if primary == replica:
            raise ValueError("two different regions are required")
        self.state = {
            "version": 1, "account": self.account, "run": secrets.token_hex(8),
            "primary_region": primary, "replica_region": replica, "keys": [],
        }
        self.save()
        tags = [
            {"TagKey": RUN_TAG, "TagValue": self.state["run"]},
            {"TagKey": OWNER_TAG, "TagValue": OWNER},
        ]
        for epoch in (1, 2):
            metadata = self.kms(primary).create_key(
                Description="Disposable SPACE sandbox-anchor hardware proof",
                KeySpec="HMAC_384", KeyUsage="GENERATE_VERIFY_MAC",
                Origin="AWS_KMS", MultiRegion=True, Tags=tags,
            )["KeyMetadata"]
            key = {"epoch": epoch, "primary": metadata["Arn"]}
            self.state["keys"].append(key)
            self.save()
            # The replica identity is known before the request, so an ambiguous
            # timeout is still recoverable from the journal.
            key["replica"] = metadata["Arn"].replace(f":{primary}:", f":{replica}:", 1)
            self.save()
            self.kms(primary).replicate_key(
                KeyId=metadata["Arn"], ReplicaRegion=replica,
                Description="Disposable SPACE sandbox-anchor hardware proof replica",
                Tags=tags,
            )
            self.wait_enabled(key["primary"])
            self.wait_enabled(key["replica"])

        caller = self.identity["Arn"]
        if ":assumed-role/" in caller:
            role_name = caller.split(":assumed-role/", 1)[1].split("/", 1)[0]
            caller = self.iam.get_role(RoleName=role_name)["Role"]["Arn"]
        name = f"space-anchor-proof-{self.state['run']}"
        self.state["role_name"] = name
        self.save()
        role = self.iam.create_role(
            RoleName=name,
            AssumeRolePolicyDocument=json.dumps({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow", "Principal": {"AWS": caller},
                    "Action": "sts:AssumeRole",
                }],
            }),
            Tags=[{"Key": item["TagKey"], "Value": item["TagValue"]} for item in tags],
        )["Role"]
        self.state["role_arn"] = role["Arn"]
        self.save()
        self.iam.put_role_policy(
            RoleName=name, PolicyName="owned-anchor-hmac",
            PolicyDocument=json.dumps({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow", "Action": ["kms:GenerateMac", "kms:DescribeKey"],
                    "Resource": [
                        key[side] for key in self.state["keys"] for side in ("primary", "replica")
                    ],
                }],
            }),
        )

    def runtime_credentials(self) -> Mapping[str, Any]:
        from botocore.exceptions import ClientError

        role = self.iam.get_role(RoleName=self.state["role_name"])["Role"]
        tags = {tag["Key"]: tag["Value"] for tag in role.get("Tags", [])}
        if (
            role["Arn"] != self.state["role_arn"]
            or tags.get(RUN_TAG) != self.state["run"]
            or tags.get(OWNER_TAG) != OWNER
        ):
            raise RuntimeError("refusing an unowned runtime role")
        deadline = time.monotonic() + 90
        sts = self.session.client("sts", config=self.client_config)
        while True:
            try:
                return sts.assume_role(
                    RoleArn=self.state["role_arn"],
                    RoleSessionName=f"anchor-proof-{self.state['run']}",
                    DurationSeconds=3600,
                )["Credentials"]
            except ClientError as error:
                if error.response["Error"]["Code"] != "AccessDenied" or time.monotonic() >= deadline:
                    raise
                time.sleep(2)

    def initial_catalog(self) -> dict[str, Any]:
        return {
            "scheme": 2, "environment": f"proof-{self.state['run']}",
            "epochs": [{"version": 1, "since_ms": 0, "key_id": self.state["keys"][0]["primary"]}],
        }

    def runtime_session(self) -> Any:
        import boto3

        credentials = self.runtime_credentials()
        return boto3.Session(
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=credentials["SessionToken"],
        )

    def benchmark(self, samples: int = 60) -> Mapping[str, Any]:
        """Measure real requests under the same restricted role as the authority."""
        session = self.runtime_session()
        clients = {
            side: session.client(
                "kms", region_name=self.state[f"{side}_region"], config=self.client_config,
            )
            for side in ("primary", "replica")
        }
        caller = session.client("sts", config=self.client_config).get_caller_identity()["Arn"]
        if f":assumed-role/{self.state['role_name']}/" not in caller:
            raise RuntimeError("hardware measurements are not using the restricted runtime role")
        metadata = []
        for key in self.state["keys"]:
            for side in ("primary", "replica"):
                observed = clients[side].describe_key(KeyId=key[side])["KeyMetadata"]
                metadata.append({
                    "epoch": key["epoch"], "region": self.state[f"{side}_region"],
                    "key_spec": observed["KeySpec"], "origin": observed["Origin"],
                    "key_usage": observed["KeyUsage"], "multi_region": observed["MultiRegion"],
                    "state": observed["KeyState"],
                })
        timings: dict[str, list[float]] = {"primary": [], "replica": []}
        request_ids: dict[str, list[str]] = {"primary": [], "replica": []}
        equivalent = True
        retries = 0
        key = self.state["keys"][0]
        for sample in range(samples):
            # These messages are not sandbox KDF inputs. No sandbox private
            # material is retained in the measurement report.
            message = f"space-anchor-benchmark:{self.state['run']}:{sample}".encode()
            values = []
            for side in ("primary", "replica"):
                start = time.perf_counter()
                response = clients[side].generate_mac(
                    KeyId=key[side], MacAlgorithm="HMAC_SHA_384", Message=message,
                )
                timings[side].append((time.perf_counter() - start) * 1000)
                retries += response["ResponseMetadata"].get("RetryAttempts", 0)
                if sample < 3:
                    request_ids[side].append(response["ResponseMetadata"]["RequestId"])
                values.append(response["Mac"])
            equivalent = equivalent and hmac.compare_digest(values[0], values[1])

        def summary(values: list[float]) -> Mapping[str, float | int]:
            ordered = sorted(values)
            return {
                "samples": len(values), "first_request_ms": values[0],
                "p50_ms": ordered[int((len(ordered) - 1) * 0.50)],
                "p95_ms": ordered[int((len(ordered) - 1) * 0.95)],
                "p99_ms": ordered[int((len(ordered) - 1) * 0.99)],
                "max_ms": max(values),
            }

        return {
            "backend": "real AWS KMS", "algorithm": "HMAC_SHA_384",
            "primary": summary(timings["primary"]), "replica": summary(timings["replica"]),
            "replica_outputs_identical": equivalent, "sdk_retries": retries,
            "hardware_keys": metadata, "runtime_role_is_scoped": True,
            "sample_aws_request_ids": request_ids,
        }

    def expected_public_key(self, sandbox_id: str, catalog: Mapping[str, Any]) -> str:
        """Independent OpenSSL-backed oracle; return only the public-key hash."""
        from cryptography.hazmat.primitives.asymmetric import ec
        from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

        identity = uuid.UUID(sandbox_id.removeprefix("sbx_"))
        if identity.version != 7:
            raise ValueError("sandbox KDF oracle requires UUIDv7")
        born = int.from_bytes(identity.bytes[:6], "big")
        epoch = next(
            epoch for epoch in reversed(catalog["epochs"]) if epoch["since_ms"] <= born
        )
        if epoch["key_id"] not in {key["primary"] for key in self.state["keys"]}:
            raise RuntimeError("refusing an oracle request for an unowned key")
        environment = catalog["environment"].encode()
        message = (
            struct.pack(">I", 1) + b"space-sandbox-anchor-v2\0"
            + struct.pack(">I", len(environment)) + environment
            + struct.pack(">Q", epoch["version"]) + identity.bytes
            + struct.pack(">I", 384)
        )
        client = self.runtime_session().client(
            "kms", region_name=self.state["primary_region"], config=self.client_config,
        )
        material = client.generate_mac(
            KeyId=epoch["key_id"], MacAlgorithm="HMAC_SHA_384", Message=message,
        )["Mac"]
        order = int("ffffffff00000000ffffffffffffffffbce6faada7179e84f3b9cac2fc632551", 16)
        scalar = int.from_bytes(material, "big") % (order - 1) + 1
        public = ec.derive_private_key(scalar, ec.SECP256R1()).public_key()
        encoded = public.public_bytes(Encoding.DER, PublicFormat.SubjectPublicKeyInfo)
        return hashlib.sha256(encoded).hexdigest()

    def set_primary_enabled(self, enabled: bool) -> None:
        """Fault only owned keys; journal intent before each reversible change."""
        from botocore.exceptions import ClientError

        self.state["primary_disable_intent"] = True
        self.save()
        for key in self.state["keys"]:
            arn = key["primary"]
            client = self.owned_key(arn)
            state = client.describe_key(KeyId=arn)["KeyMetadata"]["KeyState"]
            if enabled and state == "Disabled":
                client.enable_key(KeyId=arn)
            elif not enabled and state == "Enabled":
                client.disable_key(KeyId=arn)
            deadline = time.monotonic() + 90
            while time.monotonic() < deadline:
                try:
                    client.generate_mac(
                        KeyId=arn, MacAlgorithm="HMAC_SHA_384",
                        Message=b"space-anchor-owned-fault-readiness",
                    )
                    if enabled:
                        break
                except ClientError as error:
                    if error.response["Error"]["Code"] != "DisabledException":
                        raise
                    if not enabled:
                        break
                time.sleep(1)
            else:
                raise RuntimeError("owned KMS key did not reach the requested availability")
        if enabled:
            self.state["primary_disable_intent"] = False
            self.save()

    def install(self, context: str, cluster_uid: str, *, credentials_only: bool) -> None:
        if not re.fullmatch(r"kind-(?:space-local(?:-[a-z0-9-]+)?|space-oss-[0-9a-f]{10})", context):
            raise RuntimeError("refusing a non-Kind target")

        def kubectl(*args: str, document: Mapping[str, Any] | None = None) -> str:
            result = subprocess.run(
                ["kubectl", "--context", context, "-n", "space", *args],
                input=json.dumps(document) if document is not None else None,
                capture_output=True, text=True, check=True, timeout=30,
            )
            return result.stdout.strip()

        actual_uid = kubectl("get", "namespace", "kube-system", "-o", "jsonpath={.metadata.uid}")
        if not cluster_uid or actual_uid != cluster_uid:
            raise RuntimeError("refusing a changed Kind cluster identity")
        labels = {RUN_TAG: self.state["run"], OWNER_TAG: OWNER}
        resources: list[dict[str, Any]] = []
        if not credentials_only:
            resources.append({
                "apiVersion": "v1", "kind": "ConfigMap",
                "metadata": {"name": CATALOG, "namespace": "space", "labels": labels},
                "data": {"epochs.json": json.dumps(self.initial_catalog(), sort_keys=True)},
            })
        credentials = self.runtime_credentials()
        expiration = credentials["Expiration"]
        self.state["runtime_credentials_expire_at"] = (
            expiration.isoformat() if isinstance(expiration, datetime) else str(expiration)
        )
        self.save()
        resources.append({
            "apiVersion": "v1", "kind": "Secret",
            "metadata": {"name": CREDENTIALS, "namespace": "space", "labels": labels},
            "type": "Opaque",
            "data": {
                name: base64.b64encode(value.encode()).decode()
                for name, value in {
                    "AWS_ACCESS_KEY_ID": credentials["AccessKeyId"],
                    "AWS_SECRET_ACCESS_KEY": credentials["SecretAccessKey"],
                    "AWS_SESSION_TOKEN": credentials["SessionToken"],
                }.items()
            },
        })
        for resource in resources:
            name = resource["metadata"]["name"]
            existing = kubectl(
                "get", resource["kind"], name, "--ignore-not-found=true", "-o", "json",
            )
            if existing and json.loads(existing)["metadata"].get("labels", {}).get(RUN_TAG) != self.state["run"]:
                raise RuntimeError("refusing to overwrite a foreign fixture object")
            if existing and resource["kind"] == "ConfigMap":
                # Re-running bootstrap must never roll an advanced epoch
                # catalog back to epoch 1.
                continue
            # Secrets go only over stdin to kubectl, not argv or evidence files.
            # Server-side apply avoids a plaintext last-applied annotation.
            kubectl(
                "apply", "--server-side", "--field-manager=space-anchor-proof",
                "-f", "-", document=resource,
            )

    def cleanup(self) -> None:
        from botocore.exceptions import ClientError

        # Replica deletion must precede primary deletion. KMS retains its
        # mandatory deletion window; cleanup records the pending state.
        for side in ("replica", "primary"):
            for key in self.state.get("keys", []):
                arn = key.get(side)
                if not arn:
                    continue
                try:
                    client = self.owned_key(arn)
                    state = client.describe_key(KeyId=arn)["KeyMetadata"]["KeyState"]
                    if state not in ("PendingDeletion", "PendingReplicaDeletion"):
                        client.schedule_key_deletion(KeyId=arn, PendingWindowInDays=7)
                    key[f"{side}_cleanup"] = "deletion scheduled"
                    self.save()
                except ClientError as error:
                    if error.response["Error"]["Code"] != "NotFoundException":
                        raise
        name = self.state.get("role_name")
        if name:
            try:
                role = self.iam.get_role(RoleName=name)["Role"]
                tags = {tag["Key"]: tag["Value"] for tag in role.get("Tags", [])}
                if tags.get(RUN_TAG) != self.state["run"] or tags.get(OWNER_TAG) != OWNER:
                    raise RuntimeError("refusing cleanup of a foreign role")
                try:
                    self.iam.delete_role_policy(RoleName=name, PolicyName="owned-anchor-hmac")
                except ClientError as error:
                    if error.response["Error"]["Code"] != "NoSuchEntity":
                        raise
                self.iam.delete_role(RoleName=name)
            except ClientError as error:
                if error.response["Error"]["Code"] != "NoSuchEntity":
                    raise
        self.state["cleanup"] = "role removed; owned KMS keys pending scheduled deletion"
        self.save()


class HardwareControl:
    """Named Kind fixture operations; all mutations are Lease-fenced."""

    def __init__(self, fixture: HardwareFixture, kubectl: Kubectl, guard: Guard) -> None:
        self.fixture = fixture
        self.kubectl = kubectl
        self.guard = guard

    def catalog(self) -> Mapping[str, Any]:
        self.guard("read anchor epoch catalog", None)
        result = self.kubectl("get", f"configmap/{CATALOG}", "-o", "json", timeout=20)
        if result.returncode:
            raise RuntimeError(f"could not read epoch catalog: {result.detail()}")
        resource = json.loads(result.stdout)
        if resource["metadata"].get("labels", {}).get(RUN_TAG) != self.fixture.state["run"]:
            raise RuntimeError("epoch catalog belongs to a different fixture")
        return resource

    def benchmark(self) -> Mapping[str, Any]:
        self.guard("benchmark owned hardware derivation", None)
        return self.fixture.benchmark()

    def expected_public_key(self, sandbox_id: str) -> str:
        catalog = json.loads(self.catalog()["data"]["epochs.json"])
        return self.fixture.expected_public_key(sandbox_id, catalog)

    @contextmanager
    def primary_unavailable(self) -> Iterator[None]:
        self.guard("disable owned primary HMAC keys", None)
        try:
            self.fixture.set_primary_enabled(False)
            yield
        finally:
            self.guard("restore owned primary HMAC keys", None)
            self.fixture.set_primary_enabled(True)

    def rotate(self) -> Mapping[str, Any]:
        resource = self.catalog()
        catalog = json.loads(resource["data"]["epochs.json"])
        initial = self.fixture.initial_catalog()
        if (
            catalog["environment"] != initial["environment"]
            or catalog["scheme"] != 2
            or catalog["epochs"][0] != initial["epochs"][0]
        ):
            raise RuntimeError("refusing rotation of an unexpected epoch catalog")
        next_key = self.fixture.state["keys"][1]["primary"]
        if len(catalog["epochs"]) == 2 and catalog["epochs"][1]["key_id"] == next_key:
            return catalog["epochs"][1]
        if len(catalog["epochs"]) != 1:
            raise RuntimeError("refusing to remove or replace an existing epoch")
        self.fixture.wait_enabled(next_key)
        epoch = {
            "version": 2, "since_ms": time.time_ns() // 1_000_000 + 120_000,
            "key_id": next_key,
        }
        catalog["epochs"].append(epoch)
        patch = [
            {"op": "test", "path": "/metadata/resourceVersion",
             "value": resource["metadata"]["resourceVersion"]},
            {"op": "replace", "path": "/data/epochs.json",
             "value": json.dumps(catalog, sort_keys=True)},
        ]
        self.guard("append future hardware key epoch", None)
        result = self.kubectl(
            "patch", f"configmap/{CATALOG}", "--type=json",
            "-p", json.dumps(patch), timeout=20,
        )
        if result.returncode:
            raise RuntimeError(f"epoch publication failed: {result.detail()}")
        # A committed epoch is never rolled back by cleanup. Existing sandbox
        # identities remain tied to their original key; later tests see v2.
        return epoch


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("provision", "install", "refresh", "cleanup"))
    parser.add_argument("--journal", type=Path, required=True)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--expected-account", required=True)
    parser.add_argument("--primary-region", default="us-east-1")
    parser.add_argument("--replica-region", default="us-west-2")
    parser.add_argument("--context", default="")
    parser.add_argument("--cluster-uid", default="")
    args = parser.parse_args()
    fixture = HardwareFixture(args.journal, args.profile, args.expected_account)
    if args.action == "provision":
        fixture.provision(args.primary_region, args.replica_region)
    elif args.action in ("install", "refresh"):
        fixture.install(args.context, args.cluster_uid, credentials_only=args.action == "refresh")
    else:
        fixture.cleanup()
    print(f"{args.action} completed; resource journal: {args.journal}")


if __name__ == "__main__":
    main()
