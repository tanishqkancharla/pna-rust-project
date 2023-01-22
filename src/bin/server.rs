use std::{
    error::Error,
    net::{IpAddr, Ipv4Addr, SocketAddr},
};

use clap::{command, Parser, ValueEnum};

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum Engine {
    Kvs,
    Sled,
}

fn socket_address(s: &str) -> Result<SocketAddr, String> {
    Ok(s.parse::<SocketAddr>().map_err(|err| err.to_string())?)
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[arg(value_parser = socket_address, long)]
    addr: Option<SocketAddr>,
    /// What engine to use for the program. Default: kvs
    #[arg(value_enum, long)]
    engine: Option<Engine>,
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Cli::parse();
    let addr = args.addr.unwrap_or(SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
        8080,
    ));

    let engine = args.engine.unwrap_or(Engine::Kvs);

    println!("Address is {:?}, engine is {:?}", addr, engine);
    Ok(())
}
