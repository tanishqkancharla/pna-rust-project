use std::net::{Ipv4Addr, SocketAddr};
use std::{error::Error, net::IpAddr};

use clap::{command, Parser, Subcommand};
use kvs::KvsClient;
use slog::{o, Drain};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Address on which to connect to server to
    #[arg(
		long,
		global=true,
		default_value_t=SocketAddr::new(
			IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
			8080,
		)
	)]
    addr: SocketAddr,

    /// Command to server
    #[command(subcommand)]
    command: CliCommand,
}

#[derive(Debug, Subcommand)]
enum CliCommand {
    /// Set a key to a value
    Set {
        key: String,
        value: String,
    },
    // Get the value to a key
    Get {
        key: String,
    },
    Rm {
        key: String,
    },
}

fn main() -> Result<(), Box<dyn Error>> {
    let Cli { addr, command, .. } = Cli::parse();

    let decorator = slog_term::PlainSyncDecorator::new(std::io::stderr());
    let drain = slog_term::FullFormat::new(decorator).build().fuse();

    let logger = slog::Logger::root(
        drain,
        o!("address" => addr, "command" => format!("{:?}", command)),
    );

    let mut client = KvsClient::new(logger, addr)?;

    match command {
        CliCommand::Set { key, value } => client.set(key, value)?,
        CliCommand::Get { key } => {
            let value = client.get(key)?;

            match value {
                None => println!("Key not found"),
                Some(value) => println!("{}", value),
            }
        }
        CliCommand::Rm { key } => client.remove(key)?,
    }

    Ok(())
}
