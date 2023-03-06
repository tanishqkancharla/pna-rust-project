use serde_json::{de::IoRead, Deserializer, StreamDeserializer};

use crate::{KvStoreError, Result};
use serde::{Deserialize, Serialize};
use std::ffi::OsStr;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, SeekFrom, Write};
use std::io::{Read, Seek};
use std::path::PathBuf;
use std::{collections::HashMap, path::Path};

#[derive(Debug, Serialize, Deserialize)]
pub enum Command {
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
pub struct LogPointer {
    pub log_gen: u64,
    pub pos: u64,
    pub len: u64,
}

pub fn log_path(dir: &Path, gen: u64) -> PathBuf {
    dir.join(format!("{}.log", gen))
}

#[derive(Debug)]
pub struct LogReader {
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

pub struct LogIterator<'a> {
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
pub struct LogWriter {
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
