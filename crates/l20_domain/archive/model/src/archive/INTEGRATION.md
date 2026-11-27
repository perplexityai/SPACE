# Archive envelope integration boundary

This `pplx-space-archive-model` library implements a byte envelope, not
production archival or a reconstruction schema. It retains AWS-LC hashing and
has no storage or database I/O. Consumers must define their reconstruction,
handoff, timer, and execution-generation contracts before using the envelope.
The obligations below describe the integration boundary; the library cannot
enforce application recovery semantics.

## Wire contract

V1 is a 52-byte header followed by exactly the declared payload bytes. Integers
are unsigned big endian; UUIDs use standard 16-byte order. There are no optional
header extensions or tolerated trailing bytes.

| Byte range (end exclusive) | Value |
| --- | --- |
| 0..8 | Magic `SPARCH` followed by two zero bytes |
| 8..10 | Archive format version, currently 1 |
| 10..12 | Nonzero reconstruction schema version, assigned by the business codec |
| 12..28 | Logical sandbox UUID |
| 28..44 | Immutable manifest revision UUID |
| 44..52 | Payload byte length |
| 52..end | Reconstruction payload |

The payload cap is 16 MiB; `MAX_MANIFEST_LEN` is 16 MiB + 52 bytes. Preflight the
complete serialized configuration before publication; refuse oversize rather
than truncate required state. Large disk/memory/SBOM artifacts belong outside
this envelope. There is no compression at this layer. Future layout or bound
changes require explicit format compatibility review; unknown versions fail closed.

`ManifestSha256` covers the **entire immutable object**, header and payload,
against the trusted catalog checksum. There is no embedded payload checksum.
Disk-artifact checksums are separate and remain required. Checksums establish
integrity relative to that trusted reference, not signatures or ownership authority.

`encode` returns `EncodedManifest`, an upload candidate with its reference.
`verify` takes a trusted `ManifestReference`, the codec's supported schema and
exact downloaded bytes. It rejects wrong identities, unsupported format/schema
(including schema zero), oversize, truncation, trailing bytes and wrong whole-object
checksum. Verify those bytes before decoding; reserialization is not readback proof.

The codec selects its supported version from code, never by echoing the received
header. Its decoder should accept `VerifiedManifest` and check its schema before
deserializing. This wrapper has private fields, no public constructor or
Deserialize, and borrows immutable input. It proves envelope identity, version
and byte integrity, not payload semantics, artifact availability, remote durability,
current authorization or catalog ownership. Encoded and verified Debug output
omits payloads. No production decoder or multi-version registry is supplied.

The schema-65535 fixture is synthetic wire evidence, not a production archive.
It is assembled independently using Python `struct.pack('>HH', 1, 65535)`, UUID
`.bytes`, `struct.pack('>Q', len(payload))`, and the exact JSON bytes, after the
magic. `fixtures/SHA256SUMS` pins the whole object and the separate JSON fixture;
that JSON digest is not embedded in the wire. Run `sha256sum -c SHA256SUMS` there.
Existing tests compare the encoder byte-for-byte with this independent fixture
and cover rejection, size edges, readback and representative codec conversion.

## Immediate storage and consumer obligations

These are first-consumer work, not capabilities implemented by this library:

1. Use configured, scoped immutable objects with manifest revision identity
   distinct from attempt identity. New contents receive a new UUIDv7 `ManifestId`.
   Never overwrite a catalog-referenced object. Conditional creation and retry
   readback must reject conflicting existing bytes. Storage/rollout owns archive
   encryption, IAM, and current-owner authorization.
2. Preserve exact disk checksums, formats, template/component dependencies and
   all required references. Copy artifacts first and verify their bytes, sizes,
   compatibility and ownership; publish the immutable manifest last.
3. Bound the actual GET before buffer growth by `MAX_MANIFEST_LEN`. HEAD size,
   ETag, successful PUT/copy and in-memory encoding are insufficient. Download
   the exact published object, verify against the encoded reference, then decode
   and validate reconstruction semantics. Async callers should put bounded
   hashing and codec work in `spawn_blocking`.
4. Only then perform the short fenced catalog commit through owning stores,
   rechecking attempt, lifecycle and ownership guards. A catalog row advertises
   a complete archive. Recovery must authorize the current owner and recheck
   existence and the exact current pointer at ownership transfer; an old verified
   reference cannot authorize recreation or transfer.
5. Deliver a behavioral recovery proof across a fresh
   runtime/cache boundary: same identity and preserved configuration, exact
   dependencies, timers and floors, cold disk recovery with fresh execution and
   credentials, and no memory/process continuation. Define and validate handoff,
   timer, and execution contracts rather than inventing reconstruction markers.
   Unknown safety-critical state blocks finalization. Failed attempts require
   fenced, crash-safe cleanup and rollback preserving ordinary recovery; this
   library cannot prove either.

Freeze historical producer fixtures only after a real integrated producer makes
and recovers catalog-backed archives. The synthetic fixture proves no deployment
compatibility or runtime recovery. Later manifest updates and old-revision GC
are separate consumer work; this note adds no runtime, policy, eligibility,
retention or recovery behavior.
