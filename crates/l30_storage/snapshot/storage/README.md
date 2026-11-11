# Snapshot byte storage

`pplx-space-snapshot-storage` owns reusable operations over durable snapshot
bytes. It is separate from `pplx-space-snapshot-store`, which owns the
PostgreSQL catalog.

The current operation deletes every object under one parsed snapshot prefix.
An empty prefix is idempotent success. Backend implementations must surface
partial deletion as failure so callers never finalize catalog metadata while
bytes remain.

Vendor integrations are optional features. The S3 implementation is behind
the `s3` feature so catalog-only and substrate-neutral crates do not inherit
the AWS SDK.
