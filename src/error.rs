use std;

use std::error::Error;
use std::fmt;

use std::io;

#[derive(Debug)]
pub enum KvStoreError {
    IoErr(io::Error),
    SerdeErr(serde_json::Error),
    UnknownKeyError,
    UnexpectedCommandType,
}

impl Error for KvStoreError {
    fn cause(&self) -> Option<&dyn Error> {
        match self {
            Self::IoErr(err) => Some(err),
            Self::SerdeErr(err) => Some(err),
            _ => None,
        }
    }
}

impl From<io::Error> for KvStoreError {
    fn from(io_err: io::Error) -> Self {
        KvStoreError::IoErr(io_err)
    }
}

impl From<serde_json::Error> for KvStoreError {
    fn from(err: serde_json::Error) -> Self {
        KvStoreError::SerdeErr(err)
    }
}

impl fmt::Display for KvStoreError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::IoErr(ref err) => err.fmt(f),
            Self::SerdeErr(ref err) => err.fmt(f),
            Self::UnknownKeyError => write!(f, "Key not found"),
            Self::UnexpectedCommandType => write!(f, "Unexpected command"),
        }
    }
}

pub type Result<T> = std::result::Result<T, KvStoreError>;
