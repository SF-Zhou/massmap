//! High-performance, read-optimized key-value maps persisted to massmap files.
//!
//! The `massmap` crate provides utilities for building and querying immutable hash
//! maps that can be memory-mapped or streamed from disk with minimal overhead.
//! Builders take sorted or unsorted key-value iterators and produce a binary
//! representation that can be shared between processes, while readers offer
//! constant-time lookups backed by zero-copy IO traits.
//!
//! Typical usage involves serializing an iterator with [`MassMapBuilder`] and
//! opening the resulting file with [`MassMap`] to perform single or batched
//! lookups.
//!
//! ```
//! use massmap::{MassMap, MassMapBuilder};
//!
//! # fn main() -> std::io::Result<()> {
//! let entries = [
//!     ("apple".to_string(), 1u32),
//!     ("banana".to_string(), 2u32),
//! ];
//! let file = std::fs::File::create("examples/fruits.massmap")?;
//! MassMapBuilder::default().build(&file, entries.iter())?;
//!
//! let file = std::fs::File::open("examples/fruits.massmap")?;
//! let map = MassMap::<String, u32, _>::load(file)?;
//! assert_eq!(map.get("banana")?, Some(2));
//! # Ok(())
//! # }
//! ```
const MAGIC_NUMBER: u64 = u64::from_be_bytes(*b"MASSMAP!");

mod hasher;
pub use hasher::{MassMapDefaultHashLoader, MassMapHashConfig, MassMapHashLoader};

mod meta;
pub use meta::{MassMapBucketMeta, MassMapHeader, MassMapInfo, MassMapMeta};

mod reader;
pub use reader::MassMapReader;

mod writer;
pub use writer::MassMapWriter;

mod massmap;
pub use massmap::{MassMap, MassMapInner, MassMapIter};

mod builder;
pub use builder::{MassMapBuilder, MassMapMerger};
