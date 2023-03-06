use std::{
    io::{self, BufReader, BufWriter, Write},
    net::{SocketAddr, TcpListener, TcpStream},
};

use serde_json::Deserializer;

use crate::{
    codec::{Message, Response},
    KvsEngine,
};

use slog::{error, info, Logger};

pub struct KvsServer<Engine: KvsEngine> {
    logger: Logger,
    engine: Box<Engine>,
}

impl<Engine: KvsEngine> KvsServer<Engine> {
    pub fn new(logger: Logger, engine: Box<Engine>) -> KvsServer<Engine> {
        return KvsServer { logger, engine };
    }

    pub fn listen(&mut self, addr: SocketAddr) -> Result<(), io::Error> {
        let listener = TcpListener::bind(addr)?;
        info!(self.logger, "Listening on {}", addr);

        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    if let Err(e) = self.handle_client(stream) {
                        error!(self.logger, "Error on serving client: {}", e);
                    }
                }
                Err(e) => error!(self.logger, "Connection failed: {}", e),
            }
        }

        Ok(())
    }

    fn handle_client(&mut self, stream: TcpStream) -> Result<(), io::Error> {
        info!(self.logger, "Connected to client.");
        let reader_stream = stream;
        let writer_stream = reader_stream.try_clone()?;

        let message_stream =
            Deserializer::from_reader(BufReader::new(reader_stream)).into_iter::<Message>();
        let mut writer = BufWriter::new(writer_stream);

        for message in message_stream {
            let message = message?;
            info!(self.logger, "Received message: {:?}", message);

            let response = self.handle_message(message);

            info!(self.logger, "Sending response: {:?}", response);
            serde_json::to_writer(&mut writer, &response)?;
            writer.flush()?;
        }

        Ok(())
    }

    fn handle_message(&mut self, message: Message) -> Response {
        match message {
            Message::Set { key, value } => {
                let result = self.engine.set(key, value).map_err(|err| err.to_string());
                Response::Set(result)
            }
            Message::Get { key } => {
                let result = self.engine.get(key).map_err(|err| err.to_string());
                Response::Get(result)
            }
            Message::Remove { key } => {
                let result = self.engine.remove(key).map_err(|err| err.to_string());
                Response::Remove(result)
            }
        }
    }
}
