use crate::codec::*;
use crate::error::KvStoreError;
use serde::Deserialize;
use serde_json::StreamDeserializer;
use serde_json::{de::IoRead, Deserializer, Serializer};
use slog::{info, Logger, KV};
use std::result::Result;
use std::{
    io::{self, BufReader, BufWriter, Write},
    net::{SocketAddr, TcpStream, ToSocketAddrs},
};

pub struct KvsClient {
    logger: Logger,
    reader: Deserializer<IoRead<BufReader<TcpStream>>>,
    writer: BufWriter<TcpStream>,
}

impl KvsClient {
    pub fn new(logger: Logger, addr: SocketAddr) -> Result<KvsClient, io::Error> {
        info!(logger, "Connecting...");

        let reader_stream = TcpStream::connect(addr)?;
        let writer_stream = reader_stream.try_clone()?;

        info!(logger, "Connected.");

        let reader = Deserializer::from_reader(BufReader::new(reader_stream));
        let writer = BufWriter::new(writer_stream);

        return Ok(KvsClient {
            logger,
            reader,
            writer,
        });
    }

    fn send(&mut self, message: &Message) -> Result<Response, KvStoreError> {
        info!(self.logger, "Sending message...");
        self.writer.write(&serde_json::to_vec(message)?)?;
        self.writer.flush()?;
        info!(self.logger, "Sent.");

        info!(self.logger, "Waiting for response...");
        let response = Response::deserialize(&mut self.reader)?;
        info!(self.logger, "Received response: {:?}", response);

        return Ok(response);
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>, KvStoreError> {
        let message = Message::Get { key };
        let response = self.send(&message)?;

        match response {
            Response::Get(result) => return result.map_err(KvStoreError::StringError),
            _ => return Err(KvStoreError::StringError("Unexpected response".into())),
        }
    }

    pub fn set(&mut self, key: String, value: String) -> Result<(), KvStoreError> {
        let message = Message::Set { key, value };
        let response = self.send(&message)?;

        match response {
            Response::Set(result) => return result.map_err(KvStoreError::StringError),
            _ => return Err(KvStoreError::StringError("Unexpected response".into())),
        }
    }

    pub fn remove(&mut self, key: String) -> Result<(), KvStoreError> {
        let message = Message::Remove { key };
        let response = self.send(&message)?;

        match response {
            Response::Remove(result) => return result.map_err(KvStoreError::StringError),
            _ => return Err(KvStoreError::StringError("Unexpected response".into())),
        }
    }
}
