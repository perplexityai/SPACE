mod os;

pub use os::*;

#[derive(Clone, Copy, Debug)]
pub enum JailerExecutable<'a> {
    LegacyPath(&'a str),
    Sealed(&'a crate::verity::SealedFile),
}
