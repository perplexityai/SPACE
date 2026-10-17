use std::path::Path;
use std::sync::{Arc, OnceLock};

use globset::{GlobBuilder, GlobSet, GlobSetBuilder};
use pplx_space_common_core::types::RelativeGlobPattern;
use tonic::Status;

const MAX_GLOB_PATTERNS: usize = 128;

const DEFAULT_EXCLUDED_ABSOLUTE_PREFIXES: &[&str] = &[
    "/run",
    "/var/run",
    "/space",
    "/tmp/.X11-unix",
    "/tmp/.ICE-unix",
];

const DEFAULT_EXCLUDE_GLOBS: &[&str] = &[
    "**/__pycache__",
    "**/__pycache__/**",
    "**/.pytest_cache",
    "**/.pytest_cache/**",
    "**/.mypy_cache",
    "**/.mypy_cache/**",
    "**/.ruff_cache",
    "**/.ruff_cache/**",
    "**/.tox",
    "**/.tox/**",
    "**/.venv",
    "**/.venv/**",
    "**/.npm",
    "**/.npm/**",
    "**/.pnpm-store",
    "**/.pnpm-store/**",
    "**/.yarn",
    "**/.yarn/**",
    "**/.bun/cache",
    "**/.bun/cache/**",
    "**/pip/cache",
    "**/pip/cache/**",
    "**/pipx",
    "**/pipx/**",
    "**/.cache",
    "**/.cache/**",
];

const DEFAULT_TMP_BROWSER_DIRS: &[&str] = &[
    ".cache-ms-playwright",
    "puppeteer",
    "playwright",
    "chromium",
    ".org.chromium.Chromium",
];

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct WatchFilterConfig {
    pub(super) include_globs: Vec<RelativeGlobPattern>,
    pub(super) exclude_globs: Vec<RelativeGlobPattern>,
    pub(super) use_default_excludes: bool,
}

impl WatchFilterConfig {
    pub(super) fn new(
        include_globs: Vec<RelativeGlobPattern>,
        exclude_globs: Vec<RelativeGlobPattern>,
        use_default_excludes: bool,
    ) -> Result<Self, Status> {
        if include_globs.len().saturating_add(exclude_globs.len()) > MAX_GLOB_PATTERNS {
            return Err(Status::invalid_argument(format!(
                "at most {MAX_GLOB_PATTERNS} file watch glob patterns are allowed"
            )));
        }
        Ok(Self {
            include_globs,
            exclude_globs,
            use_default_excludes,
        })
    }

    pub(super) fn backend_config(&self) -> Self {
        Self {
            include_globs: Vec::new(),
            exclude_globs: self.exclude_globs.clone(),
            use_default_excludes: self.use_default_excludes,
        }
    }
}

pub(in crate::filesystem) fn parse_watch_globs(
    globs: Vec<String>,
    field: &'static str,
) -> Result<Vec<RelativeGlobPattern>, Status> {
    let mut globs = globs
        .into_iter()
        .map(|glob| {
            let glob = glob.trim().to_string();
            RelativeGlobPattern::parse(glob)
                .map_err(|error| Status::invalid_argument(format!("invalid {field}: {error}")))
        })
        .collect::<Result<Vec<_>, _>>()?;
    globs.sort();
    globs.dedup();
    Ok(globs)
}

#[derive(Clone)]
pub(super) struct WatchFilter {
    config: WatchFilterConfig,
    include: Arc<GlobSet>,
    exclude: Arc<GlobSet>,
}

impl WatchFilter {
    pub(super) fn compile(config: WatchFilterConfig) -> Result<Self, Status> {
        let include = compile_glob_set(&config.include_globs, "include_globs")?;
        let exclude = compile_glob_set(&config.exclude_globs, "exclude_globs")?;
        Ok(Self {
            config,
            include: Arc::new(include),
            exclude: Arc::new(exclude),
        })
    }

    pub(super) fn has_include_globs(&self) -> bool {
        !self.config.include_globs.is_empty()
    }

    pub(super) fn matches_path(&self, root: &Path, path: &Path) -> bool {
        let Ok(relative) = path.strip_prefix(root) else {
            return false;
        };
        self.matches_relative(path, relative)
    }

    pub(super) fn matches_relative(&self, path: &Path, relative: &Path) -> bool {
        if self.config.use_default_excludes
            && DEFAULT_EXCLUDED_ABSOLUTE_PREFIXES
                .iter()
                .any(|prefix| path.starts_with(prefix))
        {
            return false;
        }
        if self.config.use_default_excludes && is_tmp_browser_scratch_path(path) {
            return false;
        }
        let relative = if relative.as_os_str().is_empty() {
            Path::new(".")
        } else {
            relative
        };
        (self.config.include_globs.is_empty() || self.include.is_match(relative))
            && !self.exclude.is_match(relative)
            && (!self.config.use_default_excludes || !default_exclude_globset().is_match(relative))
    }
}

fn default_exclude_globset() -> &'static GlobSet {
    static DEFAULT_EXCLUDE_GLOBSET: OnceLock<GlobSet> = OnceLock::new();
    DEFAULT_EXCLUDE_GLOBSET.get_or_init(|| {
        let globs = parse_watch_globs(
            DEFAULT_EXCLUDE_GLOBS
                .iter()
                .map(|glob| (*glob).to_string())
                .collect(),
            "default exclude_globs",
        )
        .expect("default file-watch exclude globs are valid");
        compile_glob_set(&globs, "default exclude_globs")
            .expect("default file-watch exclude globs are valid")
    })
}

fn is_tmp_browser_scratch_path(path: &Path) -> bool {
    let Ok(relative) = path.strip_prefix("/tmp") else {
        return false;
    };
    relative
        .components()
        .next()
        .map(|component| component.as_os_str().to_string_lossy())
        .is_some_and(|name| DEFAULT_TMP_BROWSER_DIRS.contains(&name.as_ref()))
}

fn compile_glob_set(globs: &[RelativeGlobPattern], field: &str) -> Result<GlobSet, Status> {
    let mut builder = GlobSetBuilder::new();
    for glob in globs {
        let glob = GlobBuilder::new(glob.as_str())
            .literal_separator(true)
            .build()
            .map_err(|error| {
                Status::invalid_argument(format!(
                    "invalid {field} pattern {:?}: {error}",
                    glob.as_str()
                ))
            })?;
        builder.add(glob);
    }
    builder
        .build()
        .map_err(|error| Status::invalid_argument(format!("invalid {field}: {error}")))
}

#[cfg(test)]
mod tests;
