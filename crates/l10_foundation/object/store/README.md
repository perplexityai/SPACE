# Object storage transfers

`ObjectStore` retains its file-backed upload API. Call
`ObjectStore::streaming_upload()` before starting a streaming producer. A backend
that returns `None` needs the existing file path; streaming never silently falls
back to an unbounded buffer or temporary file.

The streaming capability adds no service rollout or configuration by itself.
Callers opt in and remain responsible for artifact identity, source ownership,
job retries, post-upload verification and publication.

## Streaming upload contract

Supply these independently:

- One `UploadBufferBudget` owned by the service and cloned across all concurrent
  uploads and store instances. Do not construct a fresh budget per artifact.
- Validated `StreamingUploadOptions`: part size, per-object part concurrency and
  an upper bound on encoded bytes. Parts are 5–256 whole MiB with concurrency
  1–64. The declared maximum must fit in 10,000 parts. An encoded size that cannot
  be bounded needs the file-backed path.
- `UploadSource`: an async reader, the producer's completion future and a
  transfer-scoped child cancellation token. Select capability and validate limits
  before admitting/starting a producer.

The reader supplies the exact object bytes. The producer runs independently;
its completion future joins that work and reports encoder finalization or panic
as an error. EOF alone never allows object completion. A blocking encoder must
run off Tokio workers, with bounded source buffering, and must own its admission
permit and immutable source references until it actually exits.

The uploader reserves memory before allocating each part. Full allocation size
remains charged for short tails. Hashing runs on the blocking pool. Immutable
part bytes carry their budget permit through every SDK body clone and retry;
there is no second full-part body copy. Uploads continue progressing while a
reader or allocation waits, including when the shared budget fits only one part.

The budget bounds part payload allocations, not total process RSS. Account
separately for source pipes, codecs, HTTP/TLS buffers, bounded part metadata,
other transfer directions and allocator overhead. Service admission must also
bound the number of producers. For example, evaluating 64 MiB parts with a
shared 1 GiB budget permits at most 16 simultaneously retained full part buffers;
this is a sizing example, not a production default.

Sources that reach EOF before filling the first part use a checksum-protected
single PUT after producer success, including empty sources. Larger sources use
multipart upload. Every body has Content-MD5 over its exact retained bytes; SDK
retries replay those bytes. A budgeted next part can fill while requests are in
flight, without increasing the configured HTTP request concurrency.
Oversized output fails rather than publishing a valid-looking truncated prefix.

## Failure, ownership and rollback

| Event | Outcome and cleanup owner |
|---|---|
| EOF and producer success | Await all parts, order the completion manifest, complete the object and return actual wire length. The caller still verifies and publishes its artifact. |
| Read/finalization failure, size violation, exhausted request retry | Drop pending part/read futures, cancel and close the source, attempt multipart abort, and join the producer before returning the original error. No completion request follows producer failure. |
| Cooperative cancellation | Same cleanup path; preserve producer-owned guards through actual producer exit. |
| Upload future dropped or caller timeout | Reader closure and token cancellation are synchronous. The existing abort guard schedules best-effort cleanup; producer exit cannot be awaited from Drop. Its task must retain source/admission ownership. |
| Blocking allocation/hash outlives cancellation | Its buffer permit remains charged until the worker and every body owner release the allocation. |
| Process death, ambiguous create response, failed abort | Bucket lifecycle cleanup must reclaim abandoned multipart uploads. No in-process guard is crash-safe. |
| Ambiguous completion | Return failure without deleting the destination. The caller owns verification and a new whole-upload attempt under its existing overwrite/identity policy. |
| Whole-upload retry | Create a fresh producer and upload attempt. Do not reuse part offsets or assume regenerated codec output is identical. |

The SDK supplies request-class timeouts and finite retries. The caller supplies
an end-to-end job deadline for budget waits, producer work and cleanup. The
producer must cooperate with cancellation and reader closure. A broken producer
can otherwise prevent ordinary failure cleanup from finishing.

Before enabling a service caller, verify incomplete-multipart lifecycle cleanup
on its bucket/backend. Retain the file-backed selection for rollout and rollback.
This API adds no persisted state, protobuf fields or database migration.

## Validation

`cargo test -p pplx-space-object-store` runs the real AWS SDK against local HTTP
fixtures. Streaming tests reconstruct bodies independently, enforce Content-MD5,
exercise retry replay and out-of-order acknowledgments, reject failed producer
finalization and oversized output, and verify shared-budget progress and cleanup.
These tests establish the client contract, not real-provider durability or
service-level performance; those require the caller's integration validation.
