//! The size limits of Native file requests. A server that answers them uses
//! these, so a request accepted by one is accepted by every other.

use pplx_space_common_core::spaced::{MAX_MESSAGE_BYTES, MAX_WRITE_BATCH_ENTRIES};

pub const MAX_FILE_LIST_ENTRIES: usize = 10_000;
pub const MAX_FILE_WRITE_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_FILE_READ_BYTES: u64 = 16 * 1024 * 1024;
// Raw bytes per data message. A multiple of 3, so each message decodes from
// whole base64 quanta straight into its own buffer.
pub const FILE_WRITE_CHUNK_BYTES: usize = 1024 * 1024 - 1;
pub(crate) const FILE_WRITE_CHUNK_BASE64_CHARS: usize = FILE_WRITE_CHUNK_BYTES / 3 * 4;
/// The request body limit for `files:write`: the base64 of the largest payload
/// it accepts, plus 64 KiB for the path and the other fields, plus 1/32 more
/// for encoders that escape `/` as `\/`. Unescaped JSON of any accepted
/// payload fits. Escaped JSON fits while at most one base64 character in 32
/// is a `/`, twice the rate in uniformly distributed data; a payload near the
/// limit whose encoding is mostly `/` (long runs of 0xff bytes) can exceed it,
/// and such a client must send it unescaped or split it. Without this limit
/// the framework's 2 MiB default would cap a single write at about 1.5 MiB.
/// Writes stream downstream in `FILE_WRITE_CHUNK_BYTES` messages, so no hop
/// below this needs a larger limit. `files:write_batch` is sized separately
/// below: it is one gRPC message, which every hop caps at 4 MiB.
const MAX_FILE_WRITE_BASE64_CHARS: usize = MAX_FILE_WRITE_BYTES.div_ceil(3) * 4;
pub const WRITE_FILE_BODY_LIMIT_BYTES: usize =
    MAX_FILE_WRITE_BASE64_CHARS + MAX_FILE_WRITE_BASE64_CHARS / 32 + 64 * 1024;
/// Total decoded data plus path bytes accepted by one `files:write_batch`.
///
/// Unlike `files:write`, a batch is forwarded as a single unary gRPC message,
/// and the hops behind this API decode at most 4 MiB per message. Capping the
/// batch at 3 MiB leaves room for protobuf framing and request metadata, so an
/// accepted batch is not rejected downstream for size. It also bounds API
/// memory per batch request to a few copies of this cap.
pub const MAX_FILE_WRITE_BATCH_BYTES: usize = 3 * 1024 * 1024;
// A batch must fit one 4 MiB downstream message with room for framing.
const _: () = assert!(MAX_FILE_WRITE_BATCH_BYTES <= MAX_MESSAGE_BYTES - 512 * 1024);
/// Per-entry JSON allowance in a batch: one path escaped at worst two bytes per
/// input byte (quote and backslash) plus field names and punctuation. Paths
/// count toward `MAX_FILE_WRITE_BATCH_BYTES`, so heavier escaping only matters
/// for paths far shorter than the limit.
const FILE_WRITE_BATCH_ENTRY_JSON_OVERHEAD_BYTES: usize =
    2 * pplx_space_common_core::types::MAX_GUEST_PATH_BYTES + 256;
/// The request body limit for `files:write_batch`: the base64 of the batch
/// cap plus 1/32 for `\/` escaping, per-entry JSON overhead for the maximum
/// entry count, and 64 KiB for the remaining fields. Without it the
/// framework's 2 MiB default would cap a batch at about 1.5 MiB of data.
const MAX_FILE_WRITE_BATCH_BASE64_CHARS: usize = MAX_FILE_WRITE_BATCH_BYTES.div_ceil(3) * 4;
pub const WRITE_FILE_BATCH_BODY_LIMIT_BYTES: usize = MAX_FILE_WRITE_BATCH_BASE64_CHARS
    + MAX_FILE_WRITE_BATCH_BASE64_CHARS / 32
    + MAX_WRITE_BATCH_ENTRIES * FILE_WRITE_BATCH_ENTRY_JSON_OVERHEAD_BYTES
    + 64 * 1024;
pub const DEFAULT_FILE_LIST_DEPTH: u32 = 1;
