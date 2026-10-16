//! Filesystem calls: stat, list, read, write, mkdir, remove, rename and batched writes.

use super::*;

pub(super) fn idempotency_key_or_generated(
    key: Option<String>,
    required: bool,
) -> Result<Option<IdempotencyKey>, tonic::Status> {
    match key {
        Some(key) => IdempotencyKey::parse(key)
            .map(Some)
            .map_err(invalid_argument_status),
        None if required => IdempotencyKey::parse(uuid::Uuid::now_v7().to_string())
            .map(Some)
            .map_err(invalid_argument_status),
        None => Ok(None),
    }
}

impl SpacedHostClient {
    pub async fn stat(&self, path: String) -> Result<proto::StatResponse, tonic::Status> {
        self.stat_as_user(path, None).await
    }

    pub async fn stat_as_user(
        &self,
        path: String,
        user: Option<GuestUserName>,
    ) -> Result<proto::StatResponse, tonic::Status> {
        let path = GuestPath::parse(path).map_err(invalid_argument_status)?;
        self.call_filesystem("stat", |mut client| {
            let path = path.clone();
            let user = user.clone();
            async move {
                client
                    .stat(StatRequest {
                        path: path.expose().to_string(),
                        user: user.map(|user| user.as_str().to_string()),
                    })
                    .await
            }
        })
        .await
    }

    pub async fn list_stream_with_depth_as_user(
        &self,
        path: String,
        depth: Option<u32>,
        user: Option<GuestUserName>,
    ) -> Result<tonic::Streaming<proto::FileInfo>, tonic::Status> {
        let path = GuestPath::parse(path).map_err(invalid_argument_status)?;
        if depth == Some(0) {
            return Err(tonic::Status::invalid_argument(
                "depth must be greater than 0",
            ));
        }
        pplx_space_spaced_common::retry_status(
            "spaced host",
            "list",
            self.config.command_retry_policy,
            || {
                let mut client = self.filesystem.clone();
                let path = path.clone();
                let user = user.clone();
                async move {
                    client
                        .list(ListRequest {
                            path: path.expose().to_string(),
                            depth,
                            user: user.map(|user| user.as_str().to_string()),
                        })
                        .await
                        .map(Response::into_inner)
                }
            },
        )
        .await
    }

    pub async fn read_stream_as_user(
        &self,
        path: String,
        offset: u64,
        length: Option<u64>,
        user: Option<GuestUserName>,
    ) -> Result<tonic::Streaming<proto::FileChunk>, tonic::Status> {
        let path = GuestPath::parse(path).map_err(invalid_argument_status)?;
        pplx_space_spaced_common::retry_status(
            "spaced host",
            "read",
            self.config.command_retry_policy,
            || {
                let mut client = self.filesystem.clone();
                let path = path.clone();
                let user = user.clone();
                async move {
                    client
                        .read(proto::ReadRequest {
                            path: path.expose().to_string(),
                            offset,
                            length,
                            user: user.map(|user| user.as_str().to_string()),
                        })
                        .await
                        .map(Response::into_inner)
                }
            },
        )
        .await
    }

    pub async fn write(
        &self,
        path: String,
        data: Vec<u8>,
        append: bool,
    ) -> Result<proto::WriteResponse, tonic::Status> {
        self.write_with_idempotency(path, data, append, None, None)
            .await
    }

    pub async fn write_with_idempotency(
        &self,
        path: String,
        data: Vec<u8>,
        append: bool,
        idempotency_key: Option<String>,
        user: Option<GuestUserName>,
    ) -> Result<proto::WriteResponse, tonic::Status> {
        const CHUNK_SIZE: usize = 64 * 1024;
        let path = GuestPath::parse(path).map_err(invalid_argument_status)?;
        let idempotency_key = idempotency_key_or_generated(idempotency_key, append)?;
        let data = std::sync::Arc::new(data);

        pplx_space_spaced_common::retry_status(
            "spaced host",
            "write",
            self.config.command_retry_policy,
            || {
                let mut client = self.filesystem.clone();
                let path = path.clone();
                let idempotency_key = idempotency_key.clone();
                let user = user.clone();
                let data = data.clone();
                async move {
                    let open = WriteRequest {
                        message: Some(proto::write_request::Message::Open(proto::WriteOpen {
                            path: path.expose().to_string(),
                            append,
                            idempotency_key: proto_idempotency_key(idempotency_key),
                            user: user.map(|user| user.as_str().to_string()),
                        })),
                    };
                    // Retries share the supplied payload; only the next frame
                    // is copied, not the complete upload on every attempt.
                    let chunks =
                        futures_util::stream::unfold((data, 0), |(data, offset)| async move {
                            let chunk = data.get(offset..)?.chunks(CHUNK_SIZE).next()?.to_vec();
                            let next = offset + chunk.len();
                            Some((
                                WriteRequest {
                                    message: Some(proto::write_request::Message::Data(chunk)),
                                },
                                (data, next),
                            ))
                        });
                    let messages = futures_util::stream::once(async move { open }).chain(chunks);
                    client.write(messages).await.map(Response::into_inner)
                }
            },
        )
        .await
    }

    pub async fn write_stream_with_idempotency<S>(
        &self,
        path: String,
        append: bool,
        idempotency_key: Option<String>,
        user: Option<GuestUserName>,
        data: S,
    ) -> Result<proto::WriteResponse, tonic::Status>
    where
        S: futures_util::Stream<Item = Vec<u8>> + Send + 'static,
    {
        use futures_util::StreamExt;

        let path = GuestPath::parse(path).map_err(invalid_argument_status)?;
        let idempotency_key = idempotency_key_or_generated(idempotency_key, append)?;
        let open = WriteRequest {
            message: Some(proto::write_request::Message::Open(proto::WriteOpen {
                path: path.expose().to_string(),
                append,
                idempotency_key: proto_idempotency_key(idempotency_key),
                user: user.map(|user| user.as_str().to_string()),
            })),
        };
        let messages =
            futures_util::stream::once(async move { open }).chain(data.map(|chunk| WriteRequest {
                message: Some(proto::write_request::Message::Data(chunk)),
            }));
        self.filesystem
            .clone()
            .write(messages)
            .await
            .map(Response::into_inner)
    }

    pub async fn mkdir(
        &self,
        path: String,
        parents: bool,
        exist_ok: bool,
        user: Option<GuestUserName>,
    ) -> Result<proto::MkdirResponse, tonic::Status> {
        let path = GuestPath::parse(path).map_err(invalid_argument_status)?;
        self.call_filesystem_once("mkdir", |mut client| {
            let path = path.clone();
            let user = user.clone();
            async move {
                client
                    .mkdir(MkdirRequest {
                        path: path.expose().to_string(),
                        parents,
                        exist_ok,
                        user: user.map(|user| user.as_str().to_string()),
                    })
                    .await
            }
        })
        .await
    }

    pub async fn remove(
        &self,
        path: String,
        recursive: bool,
        missing_ok: bool,
    ) -> Result<proto::RemoveResponse, tonic::Status> {
        self.remove_as_user(path, recursive, missing_ok, None).await
    }

    pub async fn remove_as_user(
        &self,
        path: String,
        recursive: bool,
        missing_ok: bool,
        user: Option<GuestUserName>,
    ) -> Result<proto::RemoveResponse, tonic::Status> {
        let path = GuestPath::parse(path).map_err(invalid_argument_status)?;
        self.call_filesystem_once("remove", |mut client| {
            let path = path.clone();
            let user = user.clone();
            async move {
                client
                    .remove(RemoveRequest {
                        path: path.expose().to_string(),
                        recursive,
                        missing_ok,
                        user: user.map(|user| user.as_str().to_string()),
                    })
                    .await
            }
        })
        .await
    }

    pub async fn rename_as_user(
        &self,
        old_path: String,
        new_path: String,
        idempotency_key: Option<String>,
        user: Option<GuestUserName>,
    ) -> Result<proto::RenameResponse, tonic::Status> {
        let old_path = GuestPath::parse(old_path).map_err(invalid_argument_status)?;
        let new_path = GuestPath::parse(new_path).map_err(invalid_argument_status)?;
        let idempotency_key = idempotency_key_or_generated(idempotency_key, true)?;
        self.call_filesystem("rename", |mut client| {
            let old_path = old_path.clone();
            let new_path = new_path.clone();
            let idempotency_key = idempotency_key.clone();
            let user = user.clone();
            async move {
                client
                    .rename(RenameRequest {
                        old_path: old_path.expose().to_string(),
                        new_path: new_path.expose().to_string(),
                        idempotency_key: proto_idempotency_key(idempotency_key),
                        user: user.map(|user| user.as_str().to_string()),
                    })
                    .await
            }
        })
        .await
    }

    pub async fn write_batch_as_user(
        &self,
        entries: Vec<WriteBatchEntry>,
        idempotency_key: Option<String>,
        user: Option<GuestUserName>,
    ) -> Result<proto::WriteBatchResponse, tonic::Status> {
        let entries = entries
            .into_iter()
            .map(|entry| {
                let path = GuestPath::parse(entry.path).map_err(invalid_argument_status)?;
                Ok((path, entry.data, entry.append))
            })
            .collect::<Result<Vec<_>, tonic::Status>>()?;
        let idempotency_key = idempotency_key_or_generated(idempotency_key, true)?;
        self.call_filesystem("write_batch", |mut client| {
            let entries = entries.clone();
            let idempotency_key = idempotency_key.clone();
            let user = user.clone();
            async move {
                client
                    .write_batch(WriteBatchRequest {
                        entries: entries
                            .into_iter()
                            .map(|(path, data, append)| WriteBatchEntry {
                                path: path.expose().to_string(),
                                data,
                                append,
                            })
                            .collect(),
                        idempotency_key: proto_idempotency_key(idempotency_key),
                        user: user.map(|user| user.as_str().to_string()),
                    })
                    .await
            }
        })
        .await
    }
}
