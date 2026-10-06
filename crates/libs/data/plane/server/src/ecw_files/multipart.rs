//! Streaming `multipart/form-data` parsing for `POST /files`: each `file` part
//! is spooled to disk while multer's parse buffer stays bounded.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::Poll,
};

use axum::{
    body::{Body, Bytes},
    http::{HeaderMap, StatusCode, header},
};
use futures_util::{Stream, StreamExt, stream};
use pplx_space_common_core::types::{GuestUserName, MAX_GUEST_PATH_BYTES};

use super::{
    E2B_FILE_CONTENT_LIMIT_BYTES, EcwProxyError, ResponseKind, ResponseOrigin, SpoolFiles,
    StagedUpload, e2b_request_root, file_too_large_error,
};

/// Request bytes multer may hold in its parse buffer before feeding pauses.
/// Legitimate part headers need a few hundred bytes, so a body that fills the
/// buffer without forming a part is malformed. Multer rescans the buffer for a
/// header terminator on every poll, so this also caps the cost of each scan
/// when a client trickles header bytes in tiny frames.
pub(super) const E2B_MULTIPART_PARSE_BUFFER_LIMIT_BYTES: usize = 64 * 1024;
/// The CRLF that ends each part's data before the next boundary.
pub(super) const MULTIPART_PART_DATA_TERMINATOR_BYTES: usize = 2;
/// Parts per request, `file` or not. Each `file` part is staged in its own
/// spool file before any is written. The cap also bounds how far
/// `multipart_part_framing_lower_bound`'s undercount accumulates for typical
/// clients: two headers per part at two bytes each is 4,000 bytes over 1000
/// parts, well under the parse-buffer cap.
pub(super) const E2B_MULTIPART_PART_LIMIT: usize = 1000;

#[derive(Debug)]
pub(super) struct MultipartUploadPartE2b {
    pub(super) filename: Option<String>,
    pub(super) upload: StagedUpload,
}

fn too_many_multipart_parts_error() -> EcwProxyError {
    EcwProxyError::new(
        StatusCode::PAYLOAD_TOO_LARGE,
        format!("multipart uploads are limited to {E2B_MULTIPART_PART_LIMIT} parts per request"),
        ResponseOrigin::Client,
        ResponseKind::Validation,
    )
}

/// RFC 2046 limits a boundary to 70 characters. Multer formats and searches
/// for the boundary on every poll, so its length adds to the cost of each one.
const MULTIPART_BOUNDARY_MAX_LEN: usize = 70;

pub(super) fn multipart_boundary(content_type: &str) -> Option<&str> {
    content_type
        .split(';')
        .skip(1)
        .find_map(|part| part.trim().strip_prefix("boundary="))
        .map(|boundary| boundary.trim_matches('"'))
        .filter(|boundary| (1..=MULTIPART_BOUNDARY_MAX_LEN).contains(&boundary.len()))
}

/// `filenames_name_paths` is false when a `path` query names the file, in
/// which case part filenames are not kept or length-checked. Every part's
/// Content-Disposition is still parsed, so malformed quoting is rejected.
pub(super) async fn spool_multipart_uploads(
    body: Body,
    boundary: &str,
    filenames_name_paths: bool,
    spool: &mut SpoolFiles,
) -> Result<Vec<MultipartUploadPartE2b>, EcwProxyError> {
    spool_multipart_uploads_with_limit(
        body,
        boundary,
        E2B_FILE_CONTENT_LIMIT_BYTES,
        filenames_name_paths,
        spool,
    )
    .await
}

pub(super) async fn spool_multipart_uploads_with_limit(
    body: Body,
    boundary: &str,
    limit: usize,
    filenames_name_paths: bool,
    spool: &mut SpoolFiles,
) -> Result<Vec<MultipartUploadPartE2b>, EcwProxyError> {
    // Capping the whole body (here and in the epilogue drain below), not each
    // part, keeps the pre-streaming contract
    // and bounds the spool disk a single request can use across all its parts.
    let constraints =
        multer::Constraints::new().size_limit(multer::SizeLimit::new().whole_stream(limit as u64));
    let consumed_bytes = Arc::new(AtomicUsize::new(0));
    let fed_bytes = Arc::new(AtomicUsize::new(0));
    // Multer only borrows the body, so whatever follows the closing delimiter
    // can still be read and counted below.
    let mut body = Box::pin(body.into_data_stream());
    let mut multipart = multer::Multipart::with_constraints(
        throttled_multipart_body(
            &mut body,
            Arc::clone(&consumed_bytes),
            Arc::clone(&fed_bytes),
        ),
        boundary,
        constraints,
    );
    let mut uploads = Vec::new();
    let mut part_count = 0usize;
    while let Some(mut field) = multipart
        .next_field()
        .await
        .map_err(multipart_upload_error)?
    {
        part_count += 1;
        if part_count > E2B_MULTIPART_PART_LIMIT {
            return Err(too_many_multipart_parts_error());
        }
        consumed_bytes.fetch_add(
            multipart_part_framing_lower_bound(boundary, field.headers()),
            Ordering::Relaxed,
        );
        let disposition = ContentDispositionE2b::parse(field.headers())?;
        let is_file = disposition.name.as_deref() == Some("file");
        let filename = disposition
            .filename
            .filter(|filename| filenames_name_paths && is_file && !filename.is_empty());
        // The filename becomes a guest path later; reject an impossible one
        // before holding it and spooling its part.
        if let Some(filename) = &filename {
            if disposition.filename_has_escaped_backslash {
                return Err(EcwProxyError::bad_request(
                    "multipart filename contains \\\\, which clients encode as one backslash or two; send the destination in the path query or use application/octet-stream",
                ));
            }
            if filename.len() > MAX_GUEST_PATH_BYTES {
                return Err(EcwProxyError::bad_request(format!(
                    "multipart filename exceeds {MAX_GUEST_PATH_BYTES} bytes"
                )));
            }
            e2b_request_root(filename, None)?;
        }
        // Both are known from the part headers, so reject before spooling.
        if is_file && filenames_name_paths && filename.is_none() {
            return Err(EcwProxyError::bad_request(
                "multipart uploads require a filename or path query",
            ));
        }
        if is_file && !filenames_name_paths && !uploads.is_empty() {
            return Err(EcwProxyError::bad_request(
                "path query parameter only supports a single multipart upload",
            ));
        }
        let mut upload = if is_file {
            Some(spool.create().await?)
        } else {
            None
        };
        while let Some(chunk) = field.chunk().await.map_err(multipart_upload_error)? {
            consumed_bytes.fetch_add(chunk.len(), Ordering::Relaxed);
            // The whole-stream cap also bounds each part, so this check in
            // `write_chunk` never trips here; octet-stream uploads rely on it.
            if let Some(upload) = upload.as_mut() {
                upload.write_chunk(&chunk).await?;
            }
        }
        // The CRLF before the next boundary. Counting it here also means every
        // field start, non-empty chunk, and field end advances the total, which
        // `throttled_multipart_body` reads as parser progress.
        consumed_bytes.fetch_add(MULTIPART_PART_DATA_TERMINATOR_BYTES, Ordering::Relaxed);
        if let Some(upload) = upload {
            let upload = upload.finish().await?;
            uploads.push(MultipartUploadPartE2b { filename, upload });
        }
    }
    // Multer stops at the closing delimiter without reading to the end of the
    // body. Drain the epilogue under the same cap, so the whole request body,
    // not just the part multer parsed, stays within `limit`.
    drop(multipart);
    let mut body_len = fed_bytes.load(Ordering::Relaxed);
    while let Some(data) = body.next().await {
        let data = data.map_err(|_| EcwProxyError::bad_request("Invalid multipart body"))?;
        body_len = body_len.saturating_add(data.len());
        if body_len > limit {
            return Err(file_too_large_error());
        }
    }
    Ok(uploads)
}

/// The `name` and `filename` of a part's Content-Disposition, parsed from the
/// raw header bytes. Multer 3.1.0's accessors match `name` and `filename`
/// wherever they follow whitespace, including inside another parameter's
/// quoted value (`filename="a name=b.txt"; name="file"`), so they are not used.
#[derive(Debug, Default, PartialEq)]
pub(super) struct ContentDispositionE2b {
    pub(super) name: Option<String>,
    pub(super) filename: Option<String>,
    /// The quoted filename contained `\\`. httpx and Go send that for one
    /// backslash; raw-backslash clients send it for two. The two readings name
    /// different paths, so a filename used as a destination is rejected.
    pub(super) filename_has_escaped_backslash: bool,
}

impl ContentDispositionE2b {
    /// Splits parameters on `;` outside quoted values, where `\\` and `\"`
    /// are always escapes and any other backslash is literal. One rule
    /// everywhere means text inside a quoted value can't end the value or
    /// start another parameter, so `name=file` inside a filename stays part of
    /// the filename.
    ///
    /// httpx and Go escape this way. Browsers, fetch/undici, curl, and urllib3
    /// send `"` as `%22` and `\` raw. Both families' filenames read as sent,
    /// except for the one sequence they encode differently: `\\`, which
    /// `filename_has_escaped_backslash` flags so callers can reject it rather
    /// than guess. A raw filename ending in `\` is rejected here, because its
    /// closing quote reads as escaped.
    ///
    /// Only `name` and `filename` are read; other parameters are skipped
    /// without validation. A header with unbalanced quotes is rejected, and so
    /// is a `name` or `filename` that has a quote inside an unquoted value,
    /// text after its closing quote, more than one quoted segment, a second
    /// occurrence, or a non-UTF-8 value.
    pub(super) fn parse(headers: &HeaderMap) -> Result<Self, EcwProxyError> {
        let value = headers
            .get(header::CONTENT_DISPOSITION)
            .ok_or_else(|| EcwProxyError::bad_request("Missing multipart Content-Disposition"))?
            .as_bytes();
        let malformed = || EcwProxyError::bad_request("Invalid multipart Content-Disposition");
        let mut params = Vec::new();
        let (mut start, mut index, mut in_quotes) = (0, 0, false);
        while let Some(&byte) = value.get(index) {
            match byte {
                b'\\' if in_quotes && is_escape(value, index) => index += 1,
                b'"' => in_quotes = !in_quotes,
                b';' if !in_quotes => {
                    params.extend(value.get(start..index));
                    start = index + 1;
                }
                _ => {}
            }
            index += 1;
        }
        if in_quotes {
            return Err(malformed());
        }
        params.extend(value.get(start..));

        let mut disposition = Self::default();
        // The first segment is the disposition type, e.g. `form-data`.
        for param in params.into_iter().skip(1) {
            let Some(separator) = param.iter().position(|&byte| byte == b'=') else {
                continue;
            };
            let key = param.get(..separator).unwrap_or_default().trim_ascii();
            let is_filename = key.eq_ignore_ascii_case(b"filename");
            let slot = if key.eq_ignore_ascii_case(b"name") {
                &mut disposition.name
            } else if is_filename {
                &mut disposition.filename
            } else {
                continue;
            };
            let mut escaped_backslash = false;
            let raw = param.get(separator + 1..).unwrap_or_default().trim_ascii();
            let bytes = match raw.strip_prefix(b"\"") {
                Some(rest) => {
                    let inner = rest.strip_suffix(b"\"").ok_or_else(malformed)?;
                    let mut unescaped = Vec::with_capacity(inner.len());
                    let mut bytes = inner.iter().peekable();
                    while let Some(&byte) = bytes.next() {
                        match (byte, bytes.peek()) {
                            (b'\\', Some(&&escaped @ (b'\\' | b'"'))) => {
                                bytes.next();
                                escaped_backslash |= escaped == b'\\';
                                unescaped.push(escaped);
                            }
                            (b'"', _) => return Err(malformed()),
                            _ => unescaped.push(byte),
                        }
                    }
                    unescaped
                }
                None if raw.contains(&b'"') => return Err(malformed()),
                None => raw.to_vec(),
            };
            let text = String::from_utf8(bytes).map_err(|_| malformed())?;
            if slot.replace(text).is_some() {
                return Err(malformed());
            }
            disposition.filename_has_escaped_backslash |= is_filename && escaped_backslash;
        }
        Ok(disposition)
    }
}

/// Whether the backslash at `index`, inside a quoted value, escapes the next
/// byte (see `ContentDispositionE2b::parse`).
fn is_escape(value: &[u8], index: usize) -> bool {
    matches!(value.get(index + 1), Some(b'\\' | b'"'))
}

fn multipart_upload_error(error: multer::Error) -> EcwProxyError {
    if matches!(error, multer::Error::StreamSizeExceeded { .. }) {
        return file_too_large_error();
    }
    EcwProxyError::bad_request("Invalid multipart body")
}

/// Bytes multer consumes to open a part: `--{boundary}\r\n`, each header at
/// its shortest accepted form `name:value\n` (httparse also takes bare-LF
/// lines), and the blank line. Never more than the real count, so bytes fed
/// minus the running total never understates multer's buffer. A typical
/// `Name: value\r\n` header is undercounted by two bytes.
pub(super) fn multipart_part_framing_lower_bound(boundary: &str, headers: &HeaderMap) -> usize {
    headers
        .iter()
        .fold(boundary.len() + 6, |len, (name, value)| {
            len + name.as_str().len() + value.len() + 2
        })
}

/// Feeds the request body to multer while bytes fed minus `consumed_bytes`, an
/// upper bound on multer's parse buffer, stays within
/// `E2B_MULTIPART_PARSE_BUFFER_LIMIT_BYTES`; the buffer then holds at most the
/// cap plus one frame. Multer appends every ready frame, so past the cap this
/// yields once to let it parse what it holds. Without progress by the next poll,
/// the request fails: normally the buffered bytes cannot complete a part. The
/// bound leaves some consumed bytes uncounted (header whitespace, duplicate
/// headers multer drops, preamble, transport padding), so an encoding padded
/// until those exceed the cap is rejected too; typical clients stay far below
/// it (see `E2B_MULTIPART_PART_LIMIT`).
/// `fed_total` receives the running total of body bytes passed to multer.
pub(super) fn throttled_multipart_body<'a, S>(
    body: &'a mut S,
    consumed_bytes: Arc<AtomicUsize>,
    fed_total: Arc<AtomicUsize>,
) -> impl Stream<Item = Result<Bytes, axum::Error>> + Send + 'a
where
    S: Stream<Item = Result<Bytes, axum::Error>> + Unpin + Send,
{
    let mut fed_bytes = 0usize;
    let mut throttled_at = None;
    stream::poll_fn(move |context| {
        let consumed = consumed_bytes.load(Ordering::Relaxed);
        if fed_bytes.saturating_sub(consumed) > E2B_MULTIPART_PARSE_BUFFER_LIMIT_BYTES {
            if throttled_at == Some(consumed) {
                return Poll::Ready(Some(Err(axum::Error::new(
                    "multipart headers or preamble exceed the parse buffer limit",
                ))));
            }
            throttled_at = Some(consumed);
            context.waker().wake_by_ref();
            return Poll::Pending;
        }
        throttled_at = None;
        let next = std::pin::Pin::new(&mut *body).poll_next(context);
        if let Poll::Ready(Some(Ok(data))) = &next {
            fed_bytes = fed_bytes.saturating_add(data.len());
            fed_total.store(fed_bytes, Ordering::Relaxed);
        }
        next
    })
}

pub(super) fn resolve_multipart_uploads(
    uploads: Vec<MultipartUploadPartE2b>,
    query_path: Option<String>,
    user: Option<&GuestUserName>,
) -> Result<Vec<(String, StagedUpload)>, EcwProxyError> {
    uploads
        .into_iter()
        .map(|upload| {
            // The path query wins over the part's filename. Spooling has
            // already rejected parts with neither.
            let path = query_path.clone().or(upload.filename).ok_or_else(|| {
                EcwProxyError::bad_request("multipart uploads require a filename or path query")
            })?;
            Ok((e2b_request_root(&path, user)?, upload.upload))
        })
        .collect()
}
