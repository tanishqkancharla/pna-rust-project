use std::path::PathBuf;

use crate::Result;
mod kvs;
mod sled;
pub use self::sled::SledKvsEngine;
pub use kvs::KvStore;

pub trait KvsEngine {
    fn open(path_buf: PathBuf) -> Result<Self>
    where
        Self: Sized;
    fn set(&mut self, key: String, value: String) -> Result<()>;
    fn get(&mut self, key: String) -> Result<Option<String>>;
    fn remove(&mut self, key: String) -> Result<()>;
}
