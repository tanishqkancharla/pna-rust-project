use std::{
    env::current_dir,
    error::Error,
    net::{IpAddr, Ipv4Addr, SocketAddr},
};

use clap::{command, Parser, ValueEnum};
use kvs::{KvStore, KvsEngine, KvsServer, SledKvsEngine};
use slog::{o, Drain};

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum Engine {
    Kvs,
    Sled,
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Socket address to listen on
    #[arg(
		long,
		default_value_t=SocketAddr::new(
			IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
			8080,
		)
	)]
    addr: SocketAddr,

    /// What engine to use for the program. Default: kvs
    #[arg(value_enum, long, default_value_t=Engine::Kvs)]
    engine: Engine,
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Cli::parse();
    println!("{:#?}", args);

    let decorator = slog_term::PlainSyncDecorator::new(std::io::stderr());
    let drain = slog_term::FullFormat::new(decorator).build().fuse();

    let log = slog::Logger::root(
        drain,
        o!(
            "version" => env!("CARGO_PKG_VERSION"),
            "address" => args.addr,
            "engine" => match args.engine {
                Engine::Kvs => "kvs",
                Engine::Sled => "sled",
            }
        ),
    );

    match args.engine {
        Engine::Kvs => {
            let mut server = KvsServer::new(log, Box::new(KvStore::open(current_dir()?)?));
            server.listen(args.addr)?;
        }
        Engine::Sled => {
            let mut server = KvsServer::new(log, Box::new(SledKvsEngine::open(current_dir()?)?));
            server.listen(args.addr)?;
        }
    };

    Ok(())
}
