# massmap

[![Rust](https://github.com/SF-Zhou/massmap/actions/workflows/rust.yml/badge.svg)](https://github.com/SF-Zhou/massmap/actions/workflows/rust.yml)
[![codecov](https://codecov.io/gh/SF-Zhou/massmap/graph/badge.svg?token=AJ23ts9Fuz)](https://codecov.io/gh/SF-Zhou/massmap)
[![Crates.io](https://img.shields.io/crates/v/massmap.svg)](https://crates.io/crates/massmap)
[![Documentation](https://docs.rs/massmap/badge.svg)](https://docs.rs/massmap)

Static hash table that scales via disk-backed expansion to trim memory usage while ensuring each lookup needs exactly one I/O.

1. During the build phase, split the raw data into enough buckets so each bucket stays small, which reduces disk I/O during lookups.
2. During lookups, touch only a single bucket and load all of its data into memory at once for comparisons.
3. Keep only the bucket index in memory; by tuning the number of buckets you can balance memory usage against lookup speed.

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
        .with_bucket_size_limit(16 << 10)
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

# 4. view the raw bytes of the massmap file
hexdump -C examples/demo.massmap
#> 00000000  4d 41 53 53 4d 41 50 21  00 00 00 00 00 00 01 e6  |MASSMAP!........|
#> 00000010  00 00 00 00 00 00 00 aa  92 92 a4 31 39 39 34 cd  |...........1994.|
#> 00000020  0f f1 92 a4 32 30 32 30  ce 00 01 18 94 91 92 a4  |....2020........|
#> 00000030  32 30 32 34 ce 00 01 76  05 92 92 a4 31 39 38 39  |2024...v....1989|
#> 00000040  cd 06 00 92 a4 32 30 30  39 cd 66 44 92 92 a4 31  |.....2009.fD...1|
#> 00000050  39 37 38 cd 01 81 92 a4  31 39 39 36 cd 17 0a 94  |978.....1996....|
#> 00000060  92 a4 31 39 39 31 cd 07  78 92 a4 31 39 39 38 cd  |..1991..x..1998.|
#> 00000070  1a cc 92 a4 32 30 30 31  cd 22 0d 92 a4 32 30 31  |....2001."...201|
#> 00000080  38 cd ff fe 91 92 a4 32  30 31 37 cd e8 c8 93 92  |8......2017.....|
#> 00000090  a4 31 39 37 39 cd 01 a7  92 a4 32 30 31 30 cd 78  |.1979.....2010.x|
#> 000000a0  58 92 a4 32 30 31 35 cd  c3 02 92 92 a4 31 39 39  |X..2015......199|
#> 000000b0  35 cd 13 e3 92 a4 31 39  39 39 cd 1c 3d 91 92 a4  |5.....1999..=...|
#> 000000c0  31 39 38 30 cd 01 d4 91  92 a4 32 30 30 38 cd 5e  |1980......2008.^|
#> 000000d0  24 92 92 a4 32 30 30 35  cd 38 20 92 a4 32 30 32  |$...2005.8 ..202|
#> 000000e0  31 ce 00 01 3c 50 92 92  a4 32 30 30 33 cd 29 aa  |1...<P...2003.).|
#> 000000f0  92 a4 32 30 31 32 cd 9b  5b 94 92 a4 31 39 39 33  |..2012..[...1993|
#> 00000100  cd 0b d3 92 a4 32 30 30  30 cd 1f 06 92 a4 32 30  |.....2000.....20|
#> 00000110  31 31 cd 8d b5 92 a4 32  30 31 36 cd d2 17 91 92  |11.....2016.....|
#> 00000120  a4 32 30 30 37 cd 50 0e  92 92 a4 31 39 38 37 cd  |.2007.P....1987.|
#> 00000130  04 63 92 a4 32 30 30 34  cd 30 c7 91 92 a4 31 39  |.c..2004.0....19|
#> 00000140  38 38 cd 05 62 92 92 a4  31 39 38 36 cd 03 cd 92  |88..b...1986....|
#> 00000150  a4 31 39 39 37 cd 19 51  92 92 a4 31 39 38 31 cd  |.1997..Q...1981.|
#> 00000160  01 f1 92 a4 31 39 38 34  cd 02 be 91 92 a4 32 30  |....1984......20|
#> 00000170  32 33 ce 00 01 66 62 91  92 a4 31 39 39 32 cd 09  |23...fb...1992..|
#> 00000180  1e 92 92 a4 31 39 38 35  cd 03 62 92 a4 32 30 31  |....1985..b..201|
#> 00000190  34 cd b7 40 92 92 a4 32  30 30 32 cd 25 22 92 a4  |4..@...2002.%"..|
#> 000001a0  32 30 30 36 cd 41 62 91  92 a4 32 30 31 33 cd a9  |2006.Ab...2013..|
#> 000001b0  e9 91 92 a4 31 39 38 33  cd 02 4c 91 92 a4 31 39  |....1983..L...19|
#> 000001c0  38 32 cd 02 15 92 92 a4  31 39 39 30 cd 06 7f 92  |82......1990....|
#> 000001d0  a4 32 30 31 39 ce 00 01  11 be 91 92 a4 32 30 32  |.2019........202|
#> 000001e0  32 ce 00 01 4e c2 93 2f  00 dc 00 20 93 18 15 02  |2...N../... ....|
#> 000001f0  93 2d 0c 01 93 39 13 02  93 4c 13 02 93 5f 25 04  |.-...9...L..._%.|
#> 00000200  93 cc 84 0a 01 93 cc 8e  1c 03 93 cc aa 13 02 93  |................|
#> 00000210  cc bd 0a 01 93 cc c7 0a  01 93 00 00 00 93 00 00  |................|
#> 00000220  00 93 cc d1 15 02 93 cc  e6 13 02 93 00 00 00 93  |................|
#> 00000230  cc f9 25 04 93 cd 01 1e  0a 01 93 cd 01 28 13 02  |..%..........(..|
#> 00000240  93 cd 01 3b 0a 01 93 cd  01 45 13 02 93 00 00 00  |...;.....E......|
#> 00000250  93 cd 01 58 13 02 93 00  00 00 93 cd 01 6b 0c 01  |...X.........k..|
#> 00000260  93 cd 01 77 0a 01 93 cd  01 81 13 02 93 cd 01 94  |...w............|
#> 00000270  13 02 93 cd 01 a7 0a 01  93 cd 01 b1 0a 01 93 cd  |................|
#> 00000280  01 bb 0a 01 93 cd 01 c5  15 02 93 cd 01 da 0c 01  |................|
#> 00000290
```

## Configuration

- `with_hash_seed(seed)`: choose deterministic sharding.
- `with_bucket_count(count)`: trade memory for faster lookups.
- `with_writer_buffer_size(bytes)`: tune streaming IO throughput.
- `with_field_names(true)`: emit MessagePack maps with named fields for easier debugging.
- `with_bucket_size_limit(bytes)`: guard against oversized buckets.
- Replace the default [`MassMapHashLoader`](https://docs.rs/massmap/latest/massmap/trait.MassMapHashLoader.html) to plug in custom hashers.

## Readers and Writers

`MassMapReader` and `MassMapWriter` abstract over positional IO. The traits are implemented for `std::fs::File` out of the box, but they can also wrap network storage, memory-mapped regions, or custom paged backends. Override `MassMapReader::batch_read_at` to dispatch vectored reads when available.

## Format Layout

Every massmap file begins with a 24-byte header containing a magic number, metadata offset, and metadata length. Key/value buckets are stored contiguously after the header in MessagePack format, followed by a serialized [`MassMapMeta`](https://docs.rs/massmap/latest/massmap/struct.MassMapMeta.html) structure that records bucket locations and hashing parameters.

## Documentation

Comprehensive API documentation is available on [docs.rs](https://docs.rs/massmap/latest/massmap/).

## License

This project is dual-licensed under the [MIT License](LICENSE-MIT) and [Apache License 2.0](LICENSE-APACHE).
