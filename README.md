# massmap

[![Rust](https://github.com/SF-Zhou/massmap/actions/workflows/rust.yml/badge.svg)](https://github.com/SF-Zhou/massmap/actions/workflows/rust.yml)
[![codecov](https://codecov.io/gh/SF-Zhou/massmap/graph/badge.svg?token=AJ23ts9Fuz)](https://codecov.io/gh/SF-Zhou/massmap)
[![Crates.io](https://img.shields.io/crates/v/massmap.svg)](https://crates.io/crates/massmap)
[![Documentation](https://docs.rs/massmap/badge.svg)](https://docs.rs/massmap)

Static hash table that scales via disk-backed expansion to trim memory usage while ensuring each lookup needs exactly one I/O.

## Installation

Add `massmap` to your `Cargo.toml`:

```sh
cargo add massmap
```

or manually:

```toml
[dependencies]
massmap = "0.1"
```

## Quick Start

```rust
use massmap::{MassMap, MassMapBuilder};

fn main() -> std::io::Result<()> {
	let entries = [
		("apple".to_string(), 1u32),
		("banana".to_string(), 2u32),
		("cherry".to_string(), 3u32),
	];

	// Offline build step.
	let file = std::fs::File::create("fruits.massmap")?;
	MassMapBuilder::default()
		.with_hash_seed(42)
		.with_bucket_count(1024)
		.build(file, entries.iter())?;

	// Read-only lookup phase.
	let file = std::fs::File::open("fruits.massmap")?;
	let map = MassMap::<String, u32, _>::load(file)?;
	assert_eq!(map.get("banana")?, Some(2));
	Ok(())
}
```

## CLI tool

A command-line utility `massmap` is provided for inspecting and querying massmap files.

```sh
# 1. install the CLI tool
cargo install massmap --examples

# 2. convert a JSON file to massmap format
massmap convert -i examples/demo.json -o examples/demo.massmap --bucket-count 32
#> {
#>   "file_length": 656,
#>   "entry_count": 47,
#>   "bucket_count": 32,
#>   "empty_buckets": 5,
#>   "meta_offset": 486,
#>   "meta_length": 170,
#>   "hash_seed": 0
#> }

# 3. query a key from the massmap file
massmap info examples/demo.massmap -k 1999
#> {
#>   "file_length": 656,
#>   "entry_count": 47,
#>   "bucket_count": 32,
#>   "empty_buckets": 5,
#>   "meta_offset": 486,
#>   "meta_length": 170,
#>   "hash_seed": 0
#> }
#> 1999: Some(Number(7229))
```

## Readers and Writers

`MassMapReader` and `MassMapWriter` abstract over positional IO. The traits are implemented for `std::fs::File` out of the box, but they can also wrap network storage, memory-mapped regions, or custom paged backends. Override `MassMapReader::batch_read_at` to dispatch vectored reads when available.

## Format Layout

Every massmap file begins with a 24-byte header containing a magic number, metadata offset, and metadata length. Key/value buckets are stored contiguously after the header in MessagePack format, followed by a serialized [`MassMapMeta`](https://docs.rs/massmap/latest/massmap/struct.MassMapMeta.html) structure that records bucket locations and hashing parameters.

## Documentation

Comprehensive API documentation is available on [docs.rs](https://docs.rs/massmap/latest/massmap/).

## License

This project is dual-licensed under the [MIT License](LICENSE-MIT) and [Apache License 2.0](LICENSE-APACHE).
