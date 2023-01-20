use std::{
    env::current_dir,
    error::Error,
    io::{self, stderr, Write},
    process::exit,
};

use clap::{Parser, Subcommand};
use kvs::{error::KvStoreError, KvStore};
use serde::{Deserialize, Serialize};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: CliCommand,
}

#[derive(Subcommand)]
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
    let cli = Cli::parse();
    let mut kvs = KvStore::open(&current_dir()?)?;

    // You can check for the existence of subcommands, and if found use their
    // matches just as you would the top level cmd
    match cli.command {
        CliCommand::Set { key, value } => {
            kvs.set(key, value)?;
        }
        CliCommand::Get { key } => match kvs.get(key).unwrap() {
            Some(value) => println!("{}", value),
            None => {
                print!("Key not found");
                return Ok(());
            }
        },
        CliCommand::Rm { key } => match kvs.remove(key) {
            Ok(()) => {}
            Err(KvStoreError::UnknownKeyError) => {
                print!("Key not found");
                exit(1);
            }
            Err(err) => return Err(Box::new(err)),
        },
    };

    Ok(())
}
