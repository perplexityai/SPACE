//! Server-side object copy between two buckets.
//!
//! Standing up a new cluster means moving an already-published template build
//! into that cluster's own artifact bucket. The bytes never need to travel
//! through the caller: S3 copies them internally, so one set of credentials
//! with `GetObject` on the source and `PutObject` on the destination moves a
//! 20 GiB rootfs without the process reading a byte of it.
//!
//! Two shapes, picked by source size, because `CopyObject` refuses a source
//! above 5 GiB:
//!
//! - at or below [`MAX_SINGLE_COPY_BYTES`], one `CopyObject`;
//! - above it, `CreateMultipartUpload` + one `UploadPartCopy` per byte range +
//!   `CompleteMultipartUpload`, with the same [`AbortGuard`] the upload path
//!   uses so a failure mid-way leaves no billable orphan upload behind.
//!
//! Part ranges come from [`multipart_part_ranges`], the same planner the
//! upload path uses, so parts stay above S3's 5 MiB per-part floor. At the
//! default 256 MiB part size the 10,000-part ceiling is 2.4 TiB — orders of
//! magnitude above any template artifact — and the tuning floor of 8 MiB still
//! covers 78 GiB.

use aws_sdk_s3::types::CompletedMultipartUpload;
use aws_smithy_types::error::display::DisplayErrorContext;
use percent_encoding::{AsciiSet, CONTROLS, utf8_percent_encode};
use tracing::{info, warn};

use crate::{
    AbortGuard, ObjectCredentials, ObjectStoreError, ObjectTarget, S3ObjectStore, S3Operation,
    byte_range_header, classify_terminal_s3_error, collect_completed_parts, multipart_part_ranges,
    observe_s3_error,
};

/// Largest source a single `CopyObject` may carry.
///
/// S3's hard limit is 5 GiB; the threshold sits below it so a
/// boundary-sized artifact takes the multipart path instead of the one call
/// that would be rejected.
const MAX_SINGLE_COPY_BYTES: u64 = 4 * 1024 * 1024 * 1024;

/// Characters `x-amz-copy-source` must carry percent-encoded.
///
/// The header holds `<bucket>/<key>`, so `/` stays literal — it is the
/// separator and the key's own path separators. Everything outside the URI
/// unreserved set is encoded, which is what makes the `sha256:` in a
/// content-addressed key safe to send.
const COPY_SOURCE_ENCODE_SET: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'#')
    .add(b'%')
    .add(b'<')
    .add(b'>')
    .add(b'?')
    .add(b'[')
    .add(b'\\')
    .add(b']')
    .add(b'^')
    .add(b'`')
    .add(b'{')
    .add(b'|')
    .add(b'}')
    .add(b':')
    .add(b'+');

/// Renders the `x-amz-copy-source` value addressing one source object.
fn copy_source_value(bucket: &str, key: &str) -> String {
    let path = format!("{bucket}/{key}");
    utf8_percent_encode(&path, COPY_SOURCE_ENCODE_SET).to_string()
}

impl S3ObjectStore {
    /// Server-side copy backing [`ObjectStore::copy_object`]. Lives here rather
    /// than in the trait impl because an impl block cannot span modules, and
    /// `lib.rs` is already long enough that this belongs in its own file.
    ///
    /// `creds` authorize both halves, so they must carry `GetObject` on `source`
    /// and `PutObject` on `dest`. Region, endpoint, and addressing mode are
    /// taken from `dest`: a cross-region copy is expressed by naming the source
    /// bucket, which S3 resolves itself.
    ///
    /// `source_size` is the source object's size, which every caller has
    /// already established (a copy is only issued once the source is known to
    /// exist). Taking it rather than re-`head`ing removes both a round-trip per
    /// object and the window in which two probes disagree about the size that
    /// selects the single-vs-multipart path.
    pub(crate) async fn copy_object_server_side(
        &self,
        creds: &ObjectCredentials,
        source: ObjectTarget<'_>,
        dest: ObjectTarget<'_>,
        source_size: u64,
    ) -> Result<(), ObjectStoreError> {
        let copy_source = copy_source_value(source.bucket, source.key);

        if source_size <= MAX_SINGLE_COPY_BYTES {
            info!(
                source_bucket = source.bucket,
                source_key = source.key,
                bucket = dest.bucket,
                key = dest.key,
                size_bytes = source_size,
                "copying object with a single server-side copy"
            );
            // S3 holds a single `CopyObject` open until the copy finishes, so
            // the server-side attempt timeout (30 s by default) covers copying
            // up to `MAX_SINGLE_COPY_BYTES`. Templates are published within
            // one bucket today, so this path does not run in prod. If a
            // separate source bucket is configured, especially in another
            // region, raise `SPACE_OBJECT_STORE_SERVER_SIDE_ATTEMPT_TIMEOUT_SECS`
            // or send copies larger than one part through the multipart copy.
            self.base
                .copy_object()
                .bucket(dest.bucket)
                .key(dest.key)
                .copy_source(&copy_source)
                .customize()
                .config_override(self.op_config(creds, &dest, S3Operation::CopyObject))
                .send()
                .await
                .map_err(|err| {
                    observe_s3_error(S3Operation::CopyObject, dest.bucket, dest.key, &err);
                    // Labelled with the *source*: a copy that fails because an
                    // object is gone is asking about the source, and the generic
                    // classifier would name the destination.
                    let source_label = format!("{}/{}", source.bucket, source.key);
                    if let Some(error) = classify_terminal_s3_error(&err, &source_label) {
                        return error;
                    }
                    ObjectStoreError::Copy(DisplayErrorContext(&err).to_string())
                })?;
            return Ok(());
        }

        self.multipart_copy(creds, source, dest, &copy_source, source_size)
            .await
    }

    /// `CreateMultipartUpload` → `UploadPartCopy` per range →
    /// `CompleteMultipartUpload`, aborting the upload on any failure.
    async fn multipart_copy(
        &self,
        creds: &ObjectCredentials,
        source: ObjectTarget<'_>,
        dest: ObjectTarget<'_>,
        copy_source: &str,
        size: u64,
    ) -> Result<(), ObjectStoreError> {
        // Clamp the part size so a source above the single-copy threshold can
        // never plan down to one part: `UploadPartCopy` carries the same 5 GiB
        // per-part limit `CopyObject` does. The min runs in `u64` and converts
        // once, because clamping in `usize` would need a fallback for the
        // conversion and every available fallback either defeats the clamp or
        // is itself a magic number.
        let part_size =
            usize::try_from((self.multipart_part_size as u64).min(MAX_SINGLE_COPY_BYTES))
                .unwrap_or(self.multipart_part_size);
        let ranges = multipart_part_ranges(size, part_size);
        let part_count = ranges.len();
        info!(
            source_bucket = source.bucket,
            source_key = source.key,
            bucket = dest.bucket,
            key = dest.key,
            size_bytes = size,
            part_count,
            part_size_bytes = part_size,
            "starting multipart server-side copy"
        );

        let create = self
            .base
            .create_multipart_upload()
            .bucket(dest.bucket)
            .key(dest.key)
            .customize()
            .config_override(self.op_config(creds, &dest, S3Operation::CreateMultipartUpload))
            .send()
            .await
            .map_err(|err| {
                observe_s3_error(
                    S3Operation::CreateMultipartUpload,
                    dest.bucket,
                    dest.key,
                    &err,
                );
                ObjectStoreError::Multipart(DisplayErrorContext(&err).to_string())
            })?;
        let upload_id = create
            .upload_id()
            .ok_or(ObjectStoreError::MissingField("upload_id"))?
            .to_string();

        let abort = AbortGuard::new(self, creds, &dest, &upload_id);

        let parts = collect_completed_parts(&ranges, self.multipart_concurrency, |range| {
            let client = self.base.clone();
            let bucket = dest.bucket.to_string();
            let key = dest.key.to_string();
            let upload_id = upload_id.clone();
            let copy_source = copy_source.to_string();
            let range_header = byte_range_header(range);
            let cfg = self.op_config(creds, &dest, S3Operation::UploadPartCopy);

            async move {
                let resp = client
                    .upload_part_copy()
                    .bucket(&bucket)
                    .key(&key)
                    .upload_id(&upload_id)
                    .part_number(range.part_number)
                    .copy_source(&copy_source)
                    .copy_source_range(&range_header)
                    .customize()
                    .config_override(cfg)
                    .send()
                    .await
                    .map_err(|err| {
                        observe_s3_error(S3Operation::UploadPartCopy, &bucket, &key, &err);
                        let error = DisplayErrorContext(&err).to_string();
                        warn!(
                            key,
                            upload_id,
                            part_number = range.part_number,
                            range = range_header,
                            error,
                            "multipart copy part failed"
                        );
                        ObjectStoreError::Multipart(error)
                    })?;

                resp.copy_part_result()
                    .and_then(|result| result.e_tag())
                    .ok_or(ObjectStoreError::MissingField("copy_part_result.e_tag"))
                    .map(str::to_string)
            }
        })
        .await?;

        self.base
            .complete_multipart_upload()
            .bucket(dest.bucket)
            .key(dest.key)
            .upload_id(&upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(parts))
                    .build(),
            )
            .customize()
            .config_override(self.op_config(creds, &dest, S3Operation::CompleteMultipartUpload))
            .send()
            .await
            .map_err(|err| {
                observe_s3_error(
                    S3Operation::CompleteMultipartUpload,
                    dest.bucket,
                    dest.key,
                    &err,
                );
                ObjectStoreError::Multipart(DisplayErrorContext(&err).to_string())
            })?;
        abort.disarm();
        info!(
            bucket = dest.bucket,
            key = dest.key,
            part_count,
            size_bytes = size,
            "completed multipart server-side copy"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests;
