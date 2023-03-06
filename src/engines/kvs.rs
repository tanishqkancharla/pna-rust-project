pub use crate::engines::KvsEngine;
use crate::logs::{log_path, Command, LogPointer, LogReader, LogWriter};
pub use crate::{KvStoreError, Result};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::PathBuf;

// Stale byte count size to trigger compaction
const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

#[derive(Debug)]
/** A simple key-value store */
pub struct KvStore {
    path: PathBuf,
    keydir: Keydir,
    readers: HashMap<u64, LogReader>,
    writer: LogWriter,
    log_gen: u64,
    stale_logs_size: u64,
}

type Keydir = HashMap<String, LogPointer>;

fn sorted_log_gens(path: &PathBuf) -> Result<Vec<u64>> {
    let mut log_entries: Vec<u64> = fs::read_dir(path)?
        .flat_map(|res| -> Result<_> { Ok(res?.path()) })
        .filter(|path| path.is_file() && path.extension() == Some("log".as_ref()))
        .flat_map(|path| {
            path.file_name()
                .and_then(OsStr::to_str)
                .map(|s| s.trim_end_matches(".log"))
                .map(str::parse::<u64>)
        })
        .flatten()
        .collect();

    log_entries.sort_unstable();
    Ok(log_entries)
}

fn index_logs(keydir: &mut Keydir, path: &PathBuf) -> Result<(HashMap<u64, LogReader>, u64, u64)> {
    let mut readers: HashMap<u64, LogReader> = HashMap::new();

    let log_gens = sorted_log_gens(&path)?;

    let mut stale_logs_size: u64 = 0;

    for &log_gen in &log_gens {
        let mut reader = LogReader::new(&path, log_gen)?;
        let mut commands = reader.iter();

        while let Some(Ok((cmd, log_pointer))) = commands.next() {
            match cmd {
                Command::Set { key, .. } => {
                    if let Some(existing_value) = keydir.get(&key) {
                        stale_logs_size += existing_value.len;
                    }
                    keydir.insert(key, log_pointer);
                }
                Command::Remove { key } => {
                    if let Some(existing_value) = keydir.get(&key) {
                        stale_logs_size += existing_value.len;
                    }
                    keydir.remove(&key);
                }
            };
        }

        readers.insert(log_gen, reader);
    }

    let current_log_gen = log_gens.last().unwrap_or(&0) + 1;

    Ok((readers, current_log_gen, stale_logs_size))
}

impl KvStore {
    fn maybe_compact(&mut self) -> Result<()> {
        if self.stale_logs_size > COMPACTION_THRESHOLD {
            // println!("Triggered compaction");
            self.compact()?;
        }
        Ok(())
    }

    fn compact(&mut self) -> Result<()> {
        // Write the current keydir into one new log file
        let old_log_gens = self.readers.keys().cloned().collect::<Vec<u64>>();
        let compact_log_gen = self.log_gen + 1;
        let mut new_keydir: Keydir = HashMap::new();

        let compact_log_path = log_path(&self.path, compact_log_gen);
        // println!("Compacting to path {:?}", &compact_log_path);
        let mut compact_log = BufWriter::new(File::create(&compact_log_path)?);

        let mut pos = 0;
        println!("{:#?}", self.readers);

        for (key, log_pointer) in self.keydir.iter() {
            let reader = self
                .readers
                .get_mut(&log_pointer.log_gen)
                .expect(&format!("Could not find reader {}", log_pointer.log_gen));

            if let Some(value) = reader.read_pointer(log_pointer)? {
                // Write to new file
                let cmd = Command::Set {
                    key: key.clone(),
                    value,
                };

                let len = compact_log.write(&serde_json::to_vec(&cmd)?)? as u64;

                if key == "key0" {
                    "hello";
                }
                let new_log_pointer = LogPointer {
                    len,
                    log_gen: compact_log_gen,
                    pos,
                };

                // Remake the keydir with the new log pointer
                new_keydir.insert(key.to_string(), new_log_pointer);
                pos += len;
            }
        }

        compact_log.flush()?;

        // Set up the reader to the compact log and the writer to the new log file
        self.readers = HashMap::new();
        let current_reader = LogReader::new(&self.path, compact_log_gen)?;
        self.readers.insert(compact_log_gen, current_reader);

        let new_log_gen = compact_log_gen + 1;
        self.writer = LogWriter::new(&self.path, new_log_gen)?;

        // Delete the old log files
        for old_log_gen in old_log_gens {
            fs::remove_file(log_path(&self.path, old_log_gen))?;
        }

        self.keydir = new_keydir;
        self.log_gen = new_log_gen;
        self.stale_logs_size = 0;

        // println!("Compacting finished: {:#?}", self);
        // println!("Compacting finished: new log gen: {}", new_log_gen);

        Ok(())
    }
}

impl KvsEngine for KvStore {
    /** Create a simple key-value store */
    fn open(path: PathBuf) -> Result<KvStore> {
        fs::create_dir_all(&path)?;

        let mut keydir: Keydir = HashMap::new();
        let (mut readers, current_log_gen, stale_logs_size) = index_logs(&mut keydir, &path)?;

        let writer = LogWriter::new(&path, current_log_gen)?;

        let current_reader = LogReader::new(&path, current_log_gen)?;
        readers.insert(current_log_gen, current_reader);

        return Ok(KvStore {
            path,
            readers,
            writer,
            keydir,
            log_gen: current_log_gen,
            stale_logs_size,
        });
    }
    /** Set a key to the given value */
    fn set(&mut self, key: String, value: String) -> Result<()> {
        // println!("Setting key: {} to value: {}", &key, &value);
        let log_pointer = self.writer.write_set_cmd(key.clone(), value)?;

        // println!("log pointer: {:#?}", log_pointer);

        if let Some(existing_value) = self.keydir.get(&key) {
            self.stale_logs_size += existing_value.len;
        }

        self.keydir.insert(key, log_pointer);
        self.maybe_compact()?;

        Ok(())
    }

    /** Remove the key from the store */
    fn remove(&mut self, key: String) -> Result<()> {
        // println!("Removing key: {}", &key);
        if !self.keydir.contains_key(&key) {
            return Err(KvStoreError::UnknownKeyError);
        }

        self.writer.write_rm_cmd(key.clone())?;

        if let Some(existing_value) = self.keydir.get(&key) {
            self.stale_logs_size += existing_value.len;
        }

        self.keydir.remove(&key);
        self.maybe_compact()?;

        Ok(())
    }

    /** Retrieve this key's value from the store */
    fn get(&mut self, key: String) -> Result<Option<String>> {
        // println!("Getting key: {}", &key);
        // println!("keydir: {:#?}", &self.keydir);

        if let Some(log_pointer) = self.keydir.get(&key) {
            // println!("log_pointer: {:#?}", log_pointer);
            self.readers
                .get_mut(&log_pointer.log_gen)
                .expect("Expected log reader")
                .read_pointer(log_pointer)
        } else {
            Ok(None)
        }
    }
}
