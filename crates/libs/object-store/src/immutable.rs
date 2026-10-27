use super::*;
use pplx_space_common_core::types::{Digest, HashAlg};
use tokio::io::AsyncReadExt;

#[derive(Debug)]
pub struct ImmutablePut {
    pub bytes: u64,
    pub version_id: Option<String>,
}

fn version(value: Option<&str>) -> Result<Option<String>, ObjectStoreError> {
    match value {
        None | Some("null") => Ok(None),
        Some(value)
            if !value.is_empty() && value.len() <= 1024 && !value.chars().any(char::is_control) =>
        {
            Ok(Some(value.into()))
        }
        _ => Err(ObjectStoreError::Put(
            "invalid immutable object version receipt".into(),
        )),
    }
}

impl S3ObjectStore {
    pub(super) async fn create_immutable_file(
        &self,
        creds: &ObjectCredentials,
        target: ObjectTarget<'_>,
        source: &Path,
        expected: Digest,
    ) -> Result<ImmutablePut, ObjectStoreError> {
        let size = tokio::fs::metadata(source).await?.len();
        // A pack may contain many independently bounded frames. Uploads and
        // lost-receipt verification stream the object rather than buffering it.
        if size == 0 || size > 256 << 20 || expected.algorithm() != HashAlg::Blake3 {
            return Err(ObjectStoreError::Put(
                "immutable object exceeds bounds or has an invalid digest".into(),
            ));
        }
        let local = source.to_path_buf();
        let observed = tokio::task::spawn_blocking(move || -> std::io::Result<Digest> {
            let mut hasher = blake3::Hasher::new();
            hasher.update_reader(std::fs::File::open(local)?)?;
            Ok(Digest::blake3(*hasher.finalize().as_bytes()))
        })
        .await
        .map_err(|error| ObjectStoreError::Put(error.to_string()))??;
        if observed != expected {
            return Err(ObjectStoreError::Put(
                "immutable source digest disagrees with object identity".into(),
            ));
        }
        let content_md5 = file_range_content_md5(source, 0, size).await?;
        let body = ByteStream::read_from()
            .path(source)
            .length(Length::Exact(size))
            .build()
            .await
            .map_err(|error| ObjectStoreError::Put(error.to_string()))?;
        let response = self
            .base
            .put_object()
            .bucket(target.bucket)
            .key(target.key)
            .if_none_match("*")
            .content_length(size as i64)
            .content_md5(content_md5)
            .body(body)
            .customize()
            .config_override(self.op_config(creds, &target, S3Operation::PutObject))
            .send()
            .await;
        match response {
            Ok(response) => Ok(ImmutablePut {
                bytes: size,
                version_id: version(response.version_id())?,
            }),
            Err(SdkError::ServiceError(error)) if error.raw().status().as_u16() == 412 => {
                // Includes retry after a lost successful PUT response. A name
                // match or provider ETag alone does not prove content identity.
                let response = self
                    .base
                    .get_object()
                    .bucket(target.bucket)
                    .key(target.key)
                    .customize()
                    .config_override(self.op_config(creds, &target, S3Operation::GetObject))
                    .send()
                    .await
                    .map_err(|error| classify_get_error(error, target.bucket, target.key))?;
                if response.content_length() != Some(size as i64) {
                    return Err(ObjectStoreError::Put(
                        "existing immutable object has a conflicting length".into(),
                    ));
                }
                let version_id = version(response.version_id())?;
                let mut body = response.body.into_async_read().take(size + 1);
                let mut buffer = vec![0; 1 << 20];
                let mut bytes = 0;
                let mut hasher = blake3::Hasher::new();
                loop {
                    let count = body.read(&mut buffer).await?;
                    if count == 0 {
                        break;
                    }
                    bytes += count as u64;
                    let span = buffer.get(..count).ok_or_else(|| {
                        ObjectStoreError::Put("read exceeded its buffer capacity".into())
                    })?;
                    hasher.update(span);
                }
                if bytes != size || Digest::blake3(*hasher.finalize().as_bytes()) != expected {
                    return Err(ObjectStoreError::Put(
                        "existing immutable object has conflicting bytes".into(),
                    ));
                }
                Ok(ImmutablePut { bytes, version_id })
            }
            Err(error) => Err(ObjectStoreError::Put(format!(
                "conditional object create: {}",
                DisplayErrorContext(&error)
            ))),
        }
    }
}

#[cfg(test)]
mod tests;
