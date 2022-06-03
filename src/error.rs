use std::path::{Path, PathBuf};

#[derive(Debug, thiserror::Error, derive_more::From)]
pub enum Error {
    #[error("{0}: {1}")]
    Io(PathBuf, std::io::Error),
}
impl From<(&Path, std::io::Error)> for Error {
    fn from(pair: (&Path, std::io::Error)) -> Self {
        Error::Io(pair.0.to_owned(), pair.1)
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
