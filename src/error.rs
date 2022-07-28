use std::{
    num::TryFromIntError,
    path::{Path, PathBuf},
};

#[derive(Debug, thiserror::Error, derive_more::From)]
pub enum Error {
    #[error("{0}: {1}")]
    Io(PathBuf, std::io::Error),
    #[error("{0}: {1}")]
    IoStr(&'static str, std::io::Error),
    #[error("wrong offset: expected {expected} found {found}")]
    #[from(ignore)]
    WrongOffset { expected: u64, found: u64 },
    #[error("wrong stream version ({:#x})", .0)]
    #[from(ignore)]
    WrongStreamVersion(u32),
    #[error("wrong user version: expected {:#x} found {:#x}", .expected, .found)]
    #[from(ignore)]
    WrongUserVersion { expected: u32, found: u32 },
    #[error("numeric overflow when {0}")]
    #[from(ignore)]
    NumericOverflow(&'static str),
    #[error("data corruption: {message} ({found} <> {expected})")]
    #[from(ignore)]
    DataCorruption {
        message: &'static str,
        found: u64,
        expected: u64,
    },
}
impl From<(&Path, std::io::Error)> for Error {
    fn from(pair: (&Path, std::io::Error)) -> Self {
        Error::Io(pair.0.to_owned(), pair.1)
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
pub(crate) type Result<T> = std::result::Result<T, Error>;
