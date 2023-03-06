use serde::{Deserialize, Serialize};

use crate::{KvStore, KvStoreError};

#[derive(Serialize, Deserialize, Debug)]
pub enum Message {
    Set { key: String, value: String },
    Get { key: String },
    Remove { key: String },
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Response {
    Get(Result<Option<String>, String>),
    Set(Result<(), String>),
    Remove(Result<(), String>),
}
