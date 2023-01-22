// #![deny(missing_docs)]
//! This is documentation for the `kv` crate.

use std::ffi::OsStr;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, SeekFrom, Write};
use std::io::{Read, Seek};
use std::path::PathBuf;
use std::{collections::HashMap, path::Path};
pub mod error;

use error::{KvStoreError, Result};

use serde::{Deserialize, Serialize};
use serde_json::de::IoRead;
use serde_json::{Deserializer, StreamDeserializer};

// Stale byte count size to trigger compaction
const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

#[derive(Debug)]
struct LogReader {
    log_gen: u64,
    reader: BufReader<File>,
}

impl LogReader {
    pub fn new(path: &Path, log_gen: u64) -> Result<LogReader> {
        let log_file_path = log_path(&path, log_gen);
        let file = File::open(log_file_path)?;

        return Ok(LogReader {
            log_gen,
            reader: BufReader::new(file),
        });
    }

    pub fn read_pointer(&mut self, log_pointer: &LogPointer) -> Result<Option<String>> {
        let pos = log_pointer.pos;
        let len = log_pointer.len;

        let reader = &mut self.reader;
        reader.seek(SeekFrom::Start(pos))?;

        let cmd_reader = reader.take(len);

        if let Command::Set { value, .. } = serde_json::from_reader(cmd_reader)? {
            Ok(Some(value))
        } else {
            Err(KvStoreError::UnexpectedCommandType)
        }
    }

    pub fn iter(&mut self) -> LogIterator {
        return LogIterator::from_reader(self.log_gen, &mut self.reader);
    }
}

struct LogIterator<'a> {
    log_gen: u64,
    deserializer: StreamDeserializer<'a, IoRead<&'a mut BufReader<File>>, Command>,
}

impl LogIterator<'_> {
    pub fn from_reader<'a>(log_gen: u64, reader: &'a mut BufReader<File>) -> LogIterator<'a> {
        let deserializer = Deserializer::from_reader(reader).into_iter::<Command>();
        return LogIterator {
            log_gen,
            deserializer,
        };
    }
}

impl Iterator for LogIterator<'_> {
    type Item = Result<(Command, LogPointer)>;

    fn next(&mut self) -> Option<Self::Item> {
        let pos = self.deserializer.byte_offset() as u64;
        let next = self.deserializer.next()?;
        let next_pos = self.deserializer.byte_offset() as u64;

        let len = next_pos - pos;

        let log_pointer = LogPointer {
            len,
            log_gen: self.log_gen,
            pos,
        };

        Some(
            next.map(|cmd| (cmd, log_pointer))
                .map_err(KvStoreError::SerdeErr),
        )
    }
}

#[derive(Debug)]
struct LogWriter {
    log_pos: u64,
    log_gen: u64,
    writer: BufWriter<File>,
}

impl LogWriter {
    pub fn new(path: &Path, log_gen: u64) -> Result<LogWriter> {
        let log_file_path = log_path(&path, log_gen);
        let file = File::create(log_file_path)?;

        return Ok(LogWriter {
            log_pos: 0,
            log_gen,
            writer: BufWriter::new(file),
        });
    }

    pub fn write_set_cmd(&mut self, key: String, value: String) -> Result<LogPointer> {
        let cmd = Command::Set { key, value };
        let pos = self.log_pos;

        let len = self.writer.write(&serde_json::to_vec(&cmd)?)? as u64;
        self.writer.flush()?;

        self.log_pos += len;

        Ok(LogPointer {
            log_gen: self.log_gen,
            pos,
            len,
        })
    }

    pub fn write_rm_cmd(&mut self, key: String) -> Result<()> {
        let cmd = Command::Remove { key };

        let len = self.writer.write(&serde_json::to_vec(&cmd)?)? as u64;
        self.writer.flush()?;

        self.log_pos += len;

        Ok(())
    }
}

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

#[derive(Debug, Serialize, Deserialize)]
enum Command {
    /// Set a key to a value
    Set {
        key: String,
        value: String,
    },
    Remove {
        key: String,
    },
}

#[derive(Debug)]
struct LogPointer {
    log_gen: u64,
    pos: u64,
    len: u64,
}

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

// fn compact_logs(keydir: &mut Keydir, path: &PathBuf) {}

fn log_path(dir: &Path, gen: u64) -> PathBuf {
    dir.join(format!("{}.log", gen))
}

impl KvStore {
    /** Create a simple key-value store */
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path: PathBuf = path.into();
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
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
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
    pub fn remove(&mut self, key: String) -> Result<()> {
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
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
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

        for (key, log_pointer) in self.keydir.iter() {
            let reader = self
                .readers
                .get_mut(&log_pointer.log_gen)
                .expect("Expected reader");

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
