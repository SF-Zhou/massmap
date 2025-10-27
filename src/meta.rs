use serde::{Deserialize, Serialize};

/// Metadata describing an individual hash bucket inside a massmap file.
#[derive(Serialize, Deserialize, Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct MassMapBucket {
    /// Offset of the serialized bucket payload within the massmap file.
    pub offset: u64,
    /// Length in bytes of the serialized bucket payload.
    pub length: u32,
    /// Number of entries stored in this bucket.
    pub count: u32,
}

/// Metadata serialized at the tail of every massmap file.
#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq, Eq)]
pub struct MassMapMeta {
    /// Total number of entries written into the map.
    pub length: u64,
    /// Seed used when hashing keys into buckets.
    pub hash_seed: u64,
    /// Descriptor for each bucket in the file.
    pub buckets: Vec<MassMapBucket>,
}

/// Summary returned by [`MassMapBuilder::build`](crate::MassMapBuilder::build).
#[derive(Debug, Serialize, Default)]
pub struct MassMapInfo {
    /// Final size of the generated massmap file in bytes.
    pub file_length: u64,
    /// Number of entries serialized.
    pub entry_count: u64,
    /// Number of buckets allocated.
    pub bucket_count: usize,
    /// Number of buckets that ended up empty.
    pub empty_buckets: usize,
    /// Offset of the serialized [`MassMapMeta`] structure.
    pub meta_offset: u64,
    /// Length in bytes of the serialized [`MassMapMeta`] structure.
    pub meta_length: u64,
    /// Hash seed used by the builder.
    pub hash_seed: u64,
}
