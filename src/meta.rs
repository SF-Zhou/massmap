use serde::{Deserialize, Serialize};
use std::{
    io::{Error, ErrorKind, Result},
    ops::Range,
};

use crate::{MAGIC_NUMBER, MassMapHashConfig};

/// Header serialized in raw bytes at the start of the massmap file.
#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq, Eq)]
pub struct MassMapHeader {
    /// Offset of the serialized [`MassMapMeta`] structure.
    pub meta_offset: u64,
    /// Length in bytes of the serialized [`MassMapMeta`] structure.
    pub meta_length: u64,
}

impl MassMapHeader {
    const S: usize = std::mem::size_of::<u64>();
    pub const SIZE: usize = Self::S * 3;

    pub fn serialize(&self) -> [u8; Self::SIZE] {
        let mut buf = [0u8; Self::SIZE];
        buf[..Self::S].copy_from_slice(&MAGIC_NUMBER.to_be_bytes());
        buf[Self::S..Self::S * 2].copy_from_slice(&self.meta_offset.to_be_bytes());
        buf[Self::S * 2..Self::S * 3].copy_from_slice(&self.meta_length.to_be_bytes());
        buf
    }

    pub fn deserialize(data: &[u8]) -> Result<Self> {
        let data: [u8; Self::SIZE] = if let Ok(data) = data.try_into() {
            data
        } else {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("Invalid header size: {}", data.len()),
            ));
        };

        let magic_number = u64::from_be_bytes(data[..Self::S].try_into().unwrap());
        let meta_offset = u64::from_be_bytes(data[Self::S..Self::S * 2].try_into().unwrap());
        let meta_length = u64::from_be_bytes(data[Self::S * 2..Self::S * 3].try_into().unwrap());

        if magic_number != MAGIC_NUMBER {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("Invalid magic number: {}", magic_number),
            ));
        }

        Ok(Self {
            meta_offset,
            meta_length,
        })
    }
}

/// Metadata describing an individual hash bucket inside a massmap file.
#[derive(Serialize, Deserialize, Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct MassMapBucketMeta {
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
    /// Number of entries.
    pub entry_count: u64,
    /// Number of buckets.
    pub bucket_count: u64,
    /// Number of non-empty buckets.
    pub occupied_bucket_count: u64,
    /// Range of non-empty bucket indices.
    pub occupied_bucket_range: Range<u64>,
    /// Hash configuration used to derive the [`BuildHasher`](std::hash::BuildHasher)
    /// when reopening the map.
    pub hash_config: MassMapHashConfig,
}

/// Summary returned by [`MassMapBuilder::build`](crate::MassMapBuilder::build).
#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq, Eq)]
pub struct MassMapInfo {
    /// Header of the massmap.
    pub header: MassMapHeader,
    /// Metadata of the massmap.
    pub meta: MassMapMeta,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_serialization() {
        let header = MassMapHeader {
            meta_offset: 12345,
            meta_length: 67890,
        };
        let serialized = header.serialize();
        let deserialized = MassMapHeader::deserialize(&serialized).unwrap();
        assert_eq!(header, deserialized);

        MassMapHeader::deserialize(&[]).unwrap_err();
    }
}
