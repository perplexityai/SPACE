//! File watching and change tracking.

use super::*;

impl SpacedHostClient {
    pub async fn watch_stream(
        &self,
        path: String,
        recursive: bool,
    ) -> Result<tonic::Streaming<proto::FileEvent>, tonic::Status> {
        self.watch_stream_with_options(
            path,
            WatchOptions {
                recursive,
                include_metadata_events: true,
                ..Default::default()
            },
        )
        .await
    }

    pub async fn watch_stream_with_options(
        &self,
        path: String,
        options: WatchOptions,
    ) -> Result<tonic::Streaming<proto::FileEvent>, tonic::Status> {
        let root = WatchRoot::new(GuestPath::parse(path).map_err(invalid_argument_status)?);
        let include_globs = options.include_globs;
        let exclude_globs = options.exclude_globs;
        let recursive = options.recursive;
        let include_metadata_events = options.include_metadata_events;
        let use_default_excludes = options.use_default_excludes;
        let user = options.user;
        pplx_space_spaced_common::retry_status(
            "spaced host",
            "watch",
            self.config.command_retry_policy,
            || {
                let mut client = self.filesystem.clone();
                let path = root.guest_path().expose().to_string();
                let include_globs = include_globs.clone();
                let exclude_globs = exclude_globs.clone();
                let user = user.clone();
                async move {
                    client
                        .watch(WatchRequest {
                            path,
                            recursive,
                            include_metadata_events,
                            include_globs: proto_globs(&include_globs),
                            exclude_globs: proto_globs(&exclude_globs),
                            use_default_excludes,
                            user: user.map(|user| user.as_str().to_string()),
                        })
                        .await
                        .map(Response::into_inner)
                }
            },
        )
        .await
    }

    pub async fn create_watcher_with_options(
        &self,
        root: String,
        options: WatchOptions,
    ) -> Result<proto::CreateWatcherResponse, tonic::Status> {
        let root = WatchRoot::new(GuestPath::parse(root).map_err(invalid_argument_status)?);
        let include_globs = options.include_globs;
        let exclude_globs = options.exclude_globs;
        let recursive = options.recursive;
        let include_metadata_events = options.include_metadata_events;
        let use_default_excludes = options.use_default_excludes;
        let user = options.user;
        self.call_filesystem_once("create_watcher", |mut client| {
            let root = root.guest_path().expose().to_string();
            let include_globs = include_globs.clone();
            let exclude_globs = exclude_globs.clone();
            let user = user.clone();
            async move {
                client
                    .create_watcher(CreateWatcherRequest {
                        root,
                        recursive,
                        include_metadata_events,
                        include_globs: proto_globs(&include_globs),
                        exclude_globs: proto_globs(&exclude_globs),
                        use_default_excludes,
                        user: user.map(|user| user.as_str().to_string()),
                    })
                    .await
            }
        })
        .await
    }

    pub async fn normalize_watch_root_as_user(
        &self,
        path: String,
        user: Option<GuestUserName>,
    ) -> Result<proto::NormalizeWatchRootResponse, tonic::Status> {
        let path = GuestPath::parse(path).map_err(invalid_argument_status)?;
        self.call_filesystem("normalize_watch_root", |mut client| {
            let path = path.clone();
            let user = user.clone();
            async move {
                client
                    .normalize_watch_root(NormalizeWatchRootRequest {
                        path: path.expose().to_string(),
                        user: user.map(|user| user.as_str().to_string()),
                    })
                    .await
            }
        })
        .await
    }

    pub async fn get_watcher_events(
        &self,
        watcher_id: String,
    ) -> Result<proto::GetWatcherEventsResponse, tonic::Status> {
        self.call_filesystem_once("get_watcher_events", |mut client| {
            let watcher_id = watcher_id.clone();
            async move {
                client
                    .get_watcher_events(GetWatcherEventsRequest { watcher_id })
                    .await
            }
        })
        .await
    }

    pub async fn remove_watcher(
        &self,
        watcher_id: String,
    ) -> Result<proto::RemoveWatcherResponse, tonic::Status> {
        self.call_filesystem_once("remove_watcher", |mut client| {
            let watcher_id = watcher_id.clone();
            async move {
                client
                    .remove_watcher(RemoveWatcherRequest { watcher_id })
                    .await
            }
        })
        .await
    }

    pub async fn enable_file_changes_with_options_as_user(
        &self,
        roots: Vec<String>,
        include_globs: Vec<RelativeGlobPattern>,
        exclude_globs: Vec<RelativeGlobPattern>,
        use_default_excludes: Option<bool>,
        user: Option<GuestUserName>,
    ) -> Result<proto::EnableFileChangesResponse, tonic::Status> {
        let roots = roots
            .into_iter()
            .map(GuestPath::parse)
            .collect::<Result<Vec<_>, _>>()
            .map_err(invalid_argument_status)?;
        self.call_filesystem_once("enable_file_changes", |mut client| {
            let roots = roots
                .iter()
                .map(|root| root.expose().to_string())
                .collect::<Vec<_>>();
            let include_globs = include_globs.clone();
            let exclude_globs = exclude_globs.clone();
            let user = user.clone();
            async move {
                client
                    .enable_file_changes(EnableFileChangesRequest {
                        roots,
                        include_globs: proto_globs(&include_globs),
                        exclude_globs: proto_globs(&exclude_globs),
                        use_default_excludes,
                        user: user.map(|user| user.as_str().to_string()),
                    })
                    .await
            }
        })
        .await
    }

    pub async fn disable_file_changes_as_user(
        &self,
        user: Option<GuestUserName>,
    ) -> Result<proto::DisableFileChangesResponse, tonic::Status> {
        self.call_filesystem_once("disable_file_changes", |mut client| {
            let user = user.clone();
            async move {
                client
                    .disable_file_changes(DisableFileChangesRequest {
                        user: user.map(|user| user.as_str().to_string()),
                    })
                    .await
            }
        })
        .await
    }

    pub async fn get_modified_files_as_user(
        &self,
        window: Option<prost_types::Duration>,
        since: Option<prost_types::Timestamp>,
        cursor: Option<String>,
        user: Option<GuestUserName>,
    ) -> Result<proto::GetModifiedFilesResponse, tonic::Status> {
        self.call_filesystem("get_modified_files", |mut client| {
            let cursor = cursor.clone();
            let user = user.clone();
            async move {
                client
                    .get_modified_files(GetModifiedFilesRequest {
                        window,
                        since,
                        cursor,
                        user: user.map(|user| user.as_str().to_string()),
                    })
                    .await
            }
        })
        .await
    }
}
