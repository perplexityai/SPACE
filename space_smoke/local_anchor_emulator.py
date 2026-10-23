"""Disposable KMS/S3 boundary fixture, not a claim of real AWS validation.

Run explicitly with:
  uv tool run --from 'moto[server]==5.2.4' python space_smoke/local_anchor_emulator.py

The host address must be reachable from the task-owned Kind nodes. Credentials
and key material are synthetic; state is scoped to this process and disappears
on shutdown. Only the authority processes, not this fixture, are restarted in
the encrypted-backup recovery proof.
"""

import argparse
import signal
import threading
from xml.sax.saxutils import escape


def main() -> None:
    import boto3
    from moto.s3.responses import S3Response
    from moto.server import DomainDispatcherApplication, create_backend_app
    from werkzeug.serving import make_server

    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="172.19.0.1")
    parser.add_argument("--port", type=int, default=5017)
    args = parser.parse_args()

    # Moto's generic serializer emits GetBucketVersioningResponse, whereas
    # S3's REST-XML protocol specifies VersioningConfiguration. Keep the real
    # SDK's strict parser intact and correct this fixture response only.
    def get_bucket_versioning(response: S3Response) -> tuple[int, dict[str, str], str]:
        status = response.backend.get_bucket_versioning(response.bucket_name)
        value = f"<Status>{escape(status)}</Status>" if status else ""
        body = (
            '<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
            f"{value}</VersioningConfiguration>"
        )
        return 200, {"Content-Type": "application/xml"}, body

    S3Response.get_bucket_versioning = get_bucket_versioning
    server = make_server(
        args.host,
        args.port,
        DomainDispatcherApplication(create_backend_app),
        threaded=True,
    )
    worker = threading.Thread(target=server.serve_forever)
    worker.start()
    signal.signal(signal.SIGTERM, lambda *_: server.shutdown())
    signal.signal(signal.SIGINT, lambda *_: server.shutdown())
    try:
        session = boto3.Session(
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
            region_name="us-east-1",
        )
        endpoint = f"http://{args.host}:{args.port}"
        s3 = session.client("s3", endpoint_url=endpoint)
        s3.create_bucket(Bucket="space-split-trust-poc")
        s3.put_bucket_versioning(
            Bucket="space-split-trust-poc",
            VersioningConfiguration={"Status": "Enabled"},
        )
        kms = session.client("kms", endpoint_url=endpoint)
        key = kms.create_key(Description="Disposable split-trust architecture proof")[
            "KeyMetadata"
        ]["KeyId"]
        kms.create_alias(AliasName="alias/space-anchor-poc", TargetKeyId=key)
        print(
            "Synthetic KMS/S3 fixture ready; versioned bucket and KMS alias provisioned.",
            flush=True,
        )
        worker.join()
    finally:
        server.shutdown()
        server.server_close()


if __name__ == "__main__":
    main()
