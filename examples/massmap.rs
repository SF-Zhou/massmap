use clap::{Parser, Subcommand};
use massmap::{MassMap, MassMapBuilder};
use serde_json::Value;
use std::fs::File;
use std::io::{BufReader, Error, ErrorKind, Result};
use std::path::{Path, PathBuf};

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Info(args) => run_info(args),
        Command::Convert(args) => run_convert(args),
    }
}

#[derive(Parser)]
#[command(
    author,
    version,
    about = "massmap utility for inspecting and creating massmap files",
    subcommand_required = true,
    arg_required_else_help = true
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Inspect a massmap file and print basic information
    Info(InfoArgs),
    /// Convert a JSON key-value file into a massmap binary file
    Convert(ConvertArgs),
}

#[derive(clap::Args)]
struct InfoArgs {
    /// Path to the massmap binary file
    #[arg(value_name = "FILE")]
    input: PathBuf,

    /// Optional key to look up in the massmap
    #[arg(short, long)]
    key: Option<String>,

    /// Optional bucket index to inspect
    #[arg(short, long)]
    bucket: Option<u64>,
}

#[derive(clap::Args)]
struct ConvertArgs {
    /// Path to the source JSON file containing key-value pairs
    #[arg(short, long, value_name = "FILE")]
    input: PathBuf,

    /// Path to the massmap binary file to produce
    #[arg(short, long, value_name = "FILE")]
    output: PathBuf,

    /// Optional override for the hash seed
    #[arg(long, value_name = "SEED", default_value_t = 0)]
    hash_seed: u64,

    /// Optional override for bucket count
    #[arg(long, value_name = "COUNT", default_value_t = 1 << 16)]
    bucket_count: u64,

    /// Optional override for writer buffer size in bytes
    #[arg(long, value_name = "BYTES", default_value_t = 16 << 20)]
    buffer_size: usize,
}

fn run_info(args: InfoArgs) -> Result<()> {
    let file = File::open(&args.input)?;

    let map = MassMap::<String, serde_json::Value, _>::load(file)?;

    let json = serde_json::to_string_pretty(&map.info())
        .map_err(|e| Error::other(format!("Failed to format JSON: {e}")))?;
    println!("{}", json);

    if let Some(key) = args.key {
        println!("{}: {:?}", key, map.get(&key)?);
    }

    if let Some(bucket_index) = args.bucket {
        if bucket_index >= map.meta.bucket_count {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "Bucket index {} out of range >= {}",
                    bucket_index, map.meta.bucket_count
                ),
            ));
        }
        let entries = map.get_bucket(bucket_index as usize)?;
        let json = serde_json::to_string_pretty(&entries)
            .map_err(|e| Error::other(format!("Failed to format JSON: {e}")))?;
        println!("Bucket {} entries:\n{}", bucket_index, json);
    }

    Ok(())
}

fn run_convert(args: ConvertArgs) -> Result<()> {
    let entries = load_entries_from_json(&args.input)?;
    let writer = File::create(&args.output)?;

    let info = MassMapBuilder::default()
        .with_hash_seed(args.hash_seed)
        .with_bucket_count(args.bucket_count)
        .with_writer_buffer_size(args.buffer_size)
        .build(&writer, entries.iter())?;

    let json = serde_json::to_string_pretty(&info)
        .map_err(|e| Error::other(format!("Failed to format JSON: {e}")))?;
    println!("{}", json);

    Ok(())
}

fn load_entries_from_json(path: &Path) -> Result<Vec<(String, Value)>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let value: Value = serde_json::from_reader(reader)
        .map_err(|e| invalid_json(format!("Failed to parse JSON input: {e}")))?;
    extract_entries(value)
}

fn extract_entries(value: Value) -> Result<Vec<(String, Value)>> {
    match value {
        Value::Object(map) => Ok(map.into_iter().collect::<Vec<_>>()),
        Value::Array(items) => {
            let mut entries = Vec::with_capacity(items.len());
            for (index, item) in items.into_iter().enumerate() {
                match item {
                    Value::Object(mut obj) => {
                        let key = obj.remove("key").ok_or_else(|| {
                            invalid_json(format!("entry {index} missing 'key' field"))
                        })?;
                        let value = obj.remove("value").ok_or_else(|| {
                            invalid_json(format!("entry {index} missing 'value' field"))
                        })?;
                        entries.push((expect_string(key, index)?, value));
                    }
                    Value::Array(mut pair) => {
                        if pair.len() != 2 {
                            return Err(invalid_json(format!(
                                "entry {index} expected array of length 2"
                            )));
                        }
                        let value = pair.pop().unwrap();
                        let key = pair.pop().unwrap();
                        entries.push((expect_string(key, index)?, value));
                    }
                    other => {
                        return Err(invalid_json(format!(
                            "unsupported entry format at index {index}: {other}"
                        )));
                    }
                }
            }
            Ok(entries)
        }
        other => Err(invalid_json(format!(
            "unsupported JSON top-level type: {other}"
        ))),
    }
}

fn expect_string(value: Value, index: usize) -> Result<String> {
    match value {
        Value::String(s) => Ok(s),
        other => Err(invalid_json(format!(
            "entry {index} expects string key, found {other}"
        ))),
    }
}

fn invalid_json(message: String) -> Error {
    Error::new(ErrorKind::InvalidData, message)
}
