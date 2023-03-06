use crate::{KvStoreError, KvsEngine};
use std::path::PathBuf;

pub struct SledKvsEngine {
    db: sled::Db,
    dirty_count: u64,
}

impl From<sled::Error> for KvStoreError {
    fn from(err: sled::Error) -> Self {
        KvStoreError::StringError(err.to_string())
    }
}

const FLUSH_LIMIT: u64 = 1 << 12;

impl SledKvsEngine {
    fn maybe_flush(&mut self) -> crate::Result<()> {
        if self.dirty_count > FLUSH_LIMIT {
            self.db.flush()?;
        }

        Ok(())
    }
}

impl KvsEngine for SledKvsEngine {
    fn open(path: PathBuf) -> Result<SledKvsEngine, KvStoreError> {
        let db = sled::open(path)?;

        Ok(SledKvsEngine { db, dirty_count: 0 })
    }

    fn set(&mut self, key: String, value: String) -> crate::Result<()> {
        self.db.insert(key, value.as_bytes())?;
        // self.maybe_flush()?;

        Ok(())
    }

    fn get(&mut self, key: String) -> crate::Result<Option<String>> {
        let value = self.db.get(key)?;

        match value {
            Some(value) => {
                let value_as_string = String::from_utf8(value.to_vec())
                    .map_err(|err| KvStoreError::StringError(err.to_string()))?;

                Ok(Some(value_as_string))
            }
            None => Ok(None),
        }
    }

    fn remove(&mut self, key: String) -> crate::Result<()> {
        let contains_key = self.db.contains_key(key.clone())?;

        if !contains_key {
            return Err(KvStoreError::UnknownKeyError);
        }

        self.db.remove(key)?;
        // self.maybe_flush()?;

        Ok(())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.db.flush()?;
        Ok(())
    }
}
