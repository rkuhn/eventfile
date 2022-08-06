use std::{
    num::TryFromIntError,
    path::{Path, PathBuf},
};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}: {1}")]
    Io(PathBuf, std::io::Error),
    #[error("{0}: {1}")]
    IoStr(&'static str, std::io::Error),
    #[error("wrong offset: expected {expected} found {found}")]
    WrongOffset { expected: u64, found: u64 },
    #[error("wrong stream version ({:#x})", .0)]
    WrongStreamVersion(u32),
    #[error("wrong user version: expected {:#x} found {:#x}", .expected, .found)]
    WrongUserVersion { expected: u32, found: u32 },
    #[error("numeric overflow when {0}")]
    NumericOverflow(&'static str),
    #[error("data corruption: {message} ({found} <> {expected})")]
    DataCorruption { message: &'static str, found: u64, expected: u64 },
    #[error("data not present: {message} (desired at {offset}, boundary at {boundary})")]
    DataNotPresent { message: &'static str, offset: u64, boundary: u64 },
    #[error("attempt to write beyond end of file")]
    WriteBeyondEnd,
}

impl Error {
    pub const fn wrong_offset(expected: u64, found: u64) -> Self {
        Self::WrongOffset { expected, found }
    }
    pub const fn wrong_stream_version(version: u32) -> Self {
        Self::WrongStreamVersion(version)
    }
    pub const fn wrong_user_version(expected: u32, found: u32) -> Self {
        Self::WrongUserVersion { expected, found }
    }
    pub const fn numeric_overflow(msg: &'static str) -> Self {
        Self::NumericOverflow(msg)
    }
    pub const fn data_corruption(message: &'static str, found: u64, expected: u64) -> Self {
        Self::DataCorruption { message, found, expected }
    }
    pub const fn data_not_present(message: &'static str, offset: u64, boundary: u64) -> Self {
        Self::DataNotPresent { message, offset, boundary }
    }
    pub const fn write_beyond_end() -> Self {
        Self::WriteBeyondEnd
    }
}
impl From<(PathBuf, std::io::Error)> for Error {
    fn from(pair: (PathBuf, std::io::Error)) -> Self {
        Error::Io(pair.0, pair.1)
    }
}
impl From<(&Path, std::io::Error)> for Error {
    fn from(pair: (&Path, std::io::Error)) -> Self {
        Error::Io(pair.0.to_owned(), pair.1)
    }
}
impl From<(&'static str, std::io::Error)> for Error {
    fn from(pair: (&'static str, std::io::Error)) -> Self {
        Error::IoStr(pair.0, pair.1)
    }
}
impl From<(&'static str, TryFromIntError)> for Error {
    fn from(pair: (&'static str, TryFromIntError)) -> Self {
        Error::NumericOverflow(pair.0)
    }
}

pub(crate) trait ErrCtx {
    type Output;
    type Error;
    fn ctx<T>(self, t: T) -> std::result::Result<Self::Output, (T, Self::Error)>;
}
impl<O, E> ErrCtx for std::result::Result<O, E> {
    type Output = O;
    type Error = E;

    fn ctx<T>(self, t: T) -> std::result::Result<Self::Output, (T, Self::Error)> {
        self.map_err(|e| (t, e))
    }
}
pub(crate) type Fallible<T> = std::result::Result<T, Error>;
