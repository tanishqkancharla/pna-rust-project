// #![deny(missing_docs)]
//! This is documentation for the `kv` crate.

mod client;
mod codec;
mod engines;
mod error;
mod logs;
mod server;
pub use client::KvsClient;
pub use engines::{KvStore, KvsEngine, SledKvsEngine};
pub use error::{KvStoreError, Result};
pub use server::KvsServer;
