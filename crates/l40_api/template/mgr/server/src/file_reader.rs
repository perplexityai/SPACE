//! Template-owned read policy; the shared I/O crate has no rollout dependency.

use std::fs::File;
use std::io::{self, Read};

use pplx_space_common_io::sparse::SparseFileReader;
use pplx_space_feature_flags::{FeatureFlagClient, SPACE_TEMPLATE_SPARSE_AWARE_READS_FLAG};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TemplateReadMode {
    Ordinary,
    Sparse,
}

impl TemplateReadMode {
    pub(crate) fn from_flags(flags: &FeatureFlagClient, template: &str) -> Self {
        if flags.bool_value(SPACE_TEMPLATE_SPARSE_AWARE_READS_FLAG, template) {
            Self::Sparse
        } else {
            Self::Ordinary
        }
    }

    pub(crate) fn reader(self, file: File) -> io::Result<TemplateFileReader> {
        match self {
            Self::Ordinary => Ok(TemplateFileReader::Ordinary(file)),
            Self::Sparse => SparseFileReader::new(file).map(TemplateFileReader::Sparse),
        }
    }
}

pub(crate) enum TemplateFileReader {
    Ordinary(File),
    Sparse(SparseFileReader),
}

impl TemplateFileReader {
    pub(crate) fn file(&self) -> &File {
        match self {
            Self::Ordinary(file) => file,
            Self::Sparse(reader) => reader.file(),
        }
    }
}

impl Read for TemplateFileReader {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Ordinary(file) => file.read(buffer),
            Self::Sparse(reader) => reader.read(buffer),
        }
    }
}

#[cfg(test)]
mod tests;
