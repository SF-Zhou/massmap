use foldhash::fast::FixedState;
use serde::Deserialize;
use std::borrow::Borrow;
use std::hash::{BuildHasher, Hash};
use std::io::{Error, ErrorKind, Result};
use std::marker::PhantomData;

use super::{MAGIC_NUMBER, MassMapMeta, MassMapReader};

/// Immutable hash map backed by a serialized massmap file.
///
/// A `MassMap` is created from a [`MassMapReader`] (typically a file) and
/// provides low-latency lookups without loading the whole dataset into memory.
/// Keys and values are deserialized on demand using `serde` and MessagePack.
///
/// # Type Parameters
/// - `K`: key type stored in the map; must implement `serde::Deserialize`.
/// - `V`: value type stored in the map; must implement `serde::Deserialize` and `Clone`.
/// - `R`: reader that satisfies [`MassMapReader`].
#[derive(Debug)]
pub struct MassMap<K, V, R: MassMapReader> {
    /// Metadata describing the layout and hashing strategy of the backing file.
    pub meta: MassMapMeta,
    /// Absolute offset within the reader at which the serialized metadata begins.
    pub meta_offset: u64,
    /// Length in bytes of the serialized metadata blob.
    pub meta_length: u64,
    hash_state: FixedState,
    reader: R,
    phantom_data: PhantomData<(K, V)>,
}

impl<K, V, R: MassMapReader> MassMap<K, V, R>
where
    K: for<'de> Deserialize<'de> + Eq + Hash,
    V: for<'de> Deserialize<'de> + Clone,
{
    /// Constructs a massmap from a [`MassMapReader`] implementation.
    ///
    /// The method validates the leading header (magic number, metadata offset and
    /// length) and deserializes [`MassMapMeta`]. Any IO or deserialization errors
    /// are forwarded to the caller.
    ///
    /// # Errors
    ///
    /// Returns an error when the magic number is invalid, the metadata cannot be
    /// read in full, or the MessagePack payload fails to deserialize.
    pub fn load(reader: R) -> Result<Self> {
        const S: usize = std::mem::size_of::<u64>();
        let (magic_number, meta_offset, meta_length) =
            reader.read_exact_at(0, S as u64 * 3, |data| {
                let magic_number = u64::from_be_bytes(data[..S].try_into().unwrap());
                let meta_offset = u64::from_be_bytes(data[S..S * 2].try_into().unwrap());
                let meta_length = u64::from_be_bytes(data[S * 2..S * 3].try_into().unwrap());
                Ok((magic_number, meta_offset, meta_length))
            })?;

        if magic_number != MAGIC_NUMBER {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("Invalid magic number: {}", magic_number),
            ));
        }
        let meta: MassMapMeta = reader.read_exact_at(meta_offset, meta_length, |data| {
            rmp_serde::from_slice(data).map_err(|e| {
                Error::new(
                    ErrorKind::InvalidData,
                    format!("Failed to deserialize MassMapMeta: {}", e),
                )
            })
        })?;

        let hash_state = FixedState::with_seed(meta.hash_seed);
        Ok(MassMap {
            meta,
            meta_offset,
            meta_length,
            hash_state,
            reader,
            phantom_data: PhantomData,
        })
    }

    /// Returns the number of entries written into this map.
    pub fn length(&self) -> u64 {
        self.meta.length
    }

    /// Attempts to deserialize the value associated with `k`.
    ///
    /// Keys are hashed using the stored seed and only the relevant bucket is
    /// deserialized, minimizing IO when the entry is missing.
    ///
    /// # Errors
    ///
    /// Returns an error if the reader fails to provide the bucket or if the
    /// serialized data cannot be deserialized into `(K, V)` pairs.
    pub fn get<Q>(&self, k: &Q) -> Result<Option<V>>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let index = self.bucket_index(k);
        let bucket = &self.meta.buckets[index];
        if bucket.count == 0 {
            return Ok(None);
        }

        self.reader
            .read_exact_at(bucket.offset, bucket.length as u64, |data| {
                let entries: Vec<(K, V)> = rmp_serde::from_slice(data).map_err(|e| {
                    Error::new(
                        ErrorKind::InvalidData,
                        format!("Failed to deserialize bucket entries: {}", e),
                    )
                })?;

                for (key, value) in entries.iter() {
                    if key.borrow() == k {
                        return Ok(Some(value.clone()));
                    }
                }
                Ok(None)
            })
    }

    /// Performs multiple lookups in a single pass.
    ///
    /// The reader is asked to fetch each bucket sequentially; implementations
    /// may override [`MassMapReader::batch_read_at`] to issue true scatter/gather
    /// reads where available. Results preserve the order of `keys`.
    ///
    /// # Errors
    ///
    /// Returns an error under the same conditions as [`get`](Self::get).
    pub fn batch_get<Q>(
        &self,
        keys: impl IntoIterator<Item = impl Borrow<Q>>,
    ) -> Result<Vec<Option<V>>>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let iov = keys.into_iter().map(|key| {
            let index = self.bucket_index(key.borrow());
            let bucket = &self.meta.buckets[index];
            (key, bucket.offset, bucket.length as u64)
        });

        self.reader.batch_read_at(iov, |expected, data| {
            if data.is_empty() {
                return Ok(None);
            }

            let entries: Vec<(K, V)> = rmp_serde::from_slice(data).map_err(|e| {
                Error::new(
                    ErrorKind::InvalidData,
                    format!("Failed to deserialize bucket entries: {}", e),
                )
            })?;

            for (key, value) in entries.iter() {
                if key.borrow() == expected.borrow() {
                    return Ok(Some(value.clone()));
                }
            }
            Ok(None)
        })
    }

    /// Creates an iterator that traverses all entries in the map by bucket order.
    ///
    /// The iterator reads each bucket sequentially from the backing storage,
    /// deserializes all entries in the bucket, and yields them one at a time.
    /// Each bucket is fully loaded into memory before any of its entries are yielded.
    ///
    /// # Examples
    ///
    /// ```
    /// use massmap::{MassMap, MassMapBuilder};
    ///
    /// # fn main() -> std::io::Result<()> {
    /// let entries = [("a", 1), ("b", 2), ("c", 3)];
    /// let file = std::fs::File::create("examples/iter_test.massmap")?;
    /// MassMapBuilder::default().build(&file, entries.iter())?;
    ///
    /// let file = std::fs::File::open("examples/iter_test.massmap")?;
    /// let map = MassMap::<String, i32, _>::load(file)?;
    /// let all_entries: Vec<_> = map.iter().collect::<std::io::Result<Vec<_>>>()?;
    /// assert_eq!(all_entries.len(), 3);
    /// # Ok(())
    /// # }
    /// ```
    pub fn iter(&self) -> MassMapIter<'_, K, V, R> {
        MassMapIter {
            map: self,
            bucket_index: 0,
            current_entries: Vec::new().into_iter(),
        }
    }

    fn bucket_index<Q>(&self, k: &Q) -> usize
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        (self.hash_state.hash_one(k) % (self.meta.buckets.len() as u64)) as usize
    }
}

/// Iterator over all entries in a [`MassMap`].
///
/// This iterator traverses buckets sequentially, loading each bucket fully into
/// memory before yielding its entries one by one.
pub struct MassMapIter<'a, K, V, R: MassMapReader> {
    map: &'a MassMap<K, V, R>,
    bucket_index: usize,
    current_entries: std::vec::IntoIter<(K, V)>,
}

impl<'a, K, V, R: MassMapReader> Iterator for MassMapIter<'a, K, V, R>
where
    K: for<'de> Deserialize<'de> + Eq + Hash,
    V: for<'de> Deserialize<'de>,
{
    type Item = Result<(K, V)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // If we have entries in the current bucket, yield the next one
            if let Some(entry) = self.current_entries.next() {
                return Some(Ok(entry));
            }

            // Move to the next bucket
            if self.bucket_index >= self.map.meta.buckets.len() {
                return None;
            }

            let bucket = &self.map.meta.buckets[self.bucket_index];
            self.bucket_index += 1;

            // Skip empty buckets
            if bucket.count == 0 {
                continue;
            }

            // Read and deserialize the bucket
            let result =
                self.map
                    .reader
                    .read_exact_at(bucket.offset, bucket.length as u64, |data| {
                        rmp_serde::from_slice(data).map_err(|e| {
                            Error::new(
                                ErrorKind::InvalidData,
                                format!("Failed to deserialize bucket entries: {}", e),
                            )
                        })
                    });

            match result {
                Ok(entries) => {
                    let vec: Vec<(K, V)> = entries;
                    self.current_entries = vec.into_iter();
                }
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::*;

    #[test]
    fn test_basic() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("massmap.bin");
        let writer = std::fs::File::create(&file).unwrap();
        let entries = vec![
            ("apple", 1),
            ("banana", 2),
            ("cherry", 3),
            ("date", 4),
            ("elderberry", 5),
        ];
        let builder = MassMapBuilder::default()
            .with_hash_seed(42)
            .with_bucket_count(8)
            .with_writer_buffer_size(8 << 20) // 8 MiB
            .with_field_names(true);
        let info = builder.build(&writer, entries.iter()).unwrap();
        assert_eq!(info.entry_count, 5);

        let file = std::fs::File::open(&file).unwrap();
        assert_eq!(info.file_length, file.metadata().unwrap().len());
        let map = MassMap::<String, i32, _>::load(file).unwrap();
        assert_eq!(map.length(), 5);
        assert_eq!(map.meta.hash_seed, 42);
        assert_eq!(map.meta.buckets.len(), 8);
        assert_eq!(map.meta.buckets.iter().map(|b| b.count).sum::<u32>(), 5);
        assert_eq!(map.get("apple").unwrap(), Some(1));
        assert_eq!(map.get("banana").unwrap(), Some(2));
        assert_eq!(map.get("steins").unwrap(), None);
        assert_eq!(map.get("gate").unwrap(), None);

        let keys = vec!["cherry", "date", "fig", "elderberry", "steins", "gate"];
        let results = map.batch_get::<str>(keys).unwrap();
        assert_eq!(results, vec![Some(3), Some(4), None, Some(5), None, None]);

        let keys = ["cherry", "date", "fig", "elderberry", "steins", "gate"].map(|s| s.to_string());
        let results = map.batch_get::<String>(&keys).unwrap();
        assert_eq!(results, vec![Some(3), Some(4), None, Some(5), None, None]);
    }

    #[test]
    fn test_1m() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("massmap.bin");
        let writer = std::fs::File::create(&file).unwrap();
        const N: u64 = 1_000_000;
        let entries = (0..N).map(|i| (i, i));

        let builder = MassMapBuilder::default()
            .with_bucket_count(N as u64)
            .with_writer_buffer_size(8 << 20); // 8 MiB
        builder.build(&writer, entries).unwrap();

        let file = std::fs::File::open(&file).unwrap();
        println!("massmap file size: {}", file.metadata().unwrap().len());

        let map = MassMap::<u64, u64, _>::load(file).unwrap();
        assert_eq!(map.length(), N as u64);
        assert_eq!(map.meta.buckets.len(), N as usize);
        assert_eq!(
            map.meta
                .buckets
                .iter()
                .map(|b| b.count as usize)
                .sum::<usize>(),
            N as usize
        );

        for _ in 0..10 {
            let k = rand::random::<u64>() % N as u64;
            assert_eq!(map.get(&k).unwrap(), Some(k));

            let k = k + N as u64;
            assert_eq!(map.get(&k).unwrap(), None);
        }
    }

    #[test]
    fn test_invalid_data() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("massmap_invalid.bin");
        let writer = std::fs::File::create(&path).unwrap();
        const N: u64 = 1000;
        let entries = (0..N).map(|i| (i, i));

        let builder = MassMapBuilder::default()
            .with_bucket_count(1)
            .with_writer_buffer_size(8 << 20); // 8 MiB
        let info = builder.build(&writer, entries).unwrap();

        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();

        {
            file.write_all_at(b"invalid data", 24).unwrap();
            let file = std::fs::File::open(&path).unwrap();
            let map = MassMap::<u64, u64, _>::load(file).unwrap();
            map.get(&0).unwrap_err();
            map.batch_get([0]).unwrap_err();
        }

        {
            file.write_all_at(b"invalid data", info.meta_offset)
                .unwrap();
            let file = std::fs::File::open(&path).unwrap();
            MassMap::<u64, u64, _>::load(file).unwrap_err();
        }

        {
            file.set_len(info.meta_offset + info.meta_length - 8)
                .unwrap();
            let file = std::fs::File::open(&path).unwrap();
            MassMap::<u64, u64, _>::load(file).unwrap_err();
        }

        {
            file.write_all_at(b"invalid data", 0).unwrap();
            let file = std::fs::File::open(&path).unwrap();
            MassMap::<u64, u64, _>::load(file).unwrap_err();
        }

        {
            let file = std::fs::File::create(&path).unwrap();
            MassMap::<u64, u64, _>::load(file).unwrap_err();
        }

        let writer = std::fs::File::create(&path).unwrap();
        let builder = MassMapBuilder::default()
            .with_bucket_count(1)
            .with_writer_buffer_size(8 << 20)
            .with_bucket_size_limit(16);
        let entries = (0..N).map(|i| (i, i));
        builder.build(&writer, entries).unwrap_err();
    }

    #[test]
    fn test_iterator_basic() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("massmap_iter.bin");
        let writer = std::fs::File::create(&file).unwrap();
        let entries = vec![
            ("apple", 1),
            ("banana", 2),
            ("cherry", 3),
            ("date", 4),
            ("elderberry", 5),
        ];
        let builder = MassMapBuilder::default()
            .with_hash_seed(42)
            .with_bucket_count(8);
        builder.build(&writer, entries.iter()).unwrap();

        let file = std::fs::File::open(&file).unwrap();
        let map = MassMap::<String, i32, _>::load(file).unwrap();

        // Collect all entries from the iterator
        let mut collected: Vec<_> = map.iter().collect::<std::io::Result<Vec<_>>>().unwrap();
        assert_eq!(collected.len(), 5);

        // Sort to compare with original entries
        collected.sort_by(|a, b| a.0.cmp(&b.0));
        let mut expected = entries
            .iter()
            .map(|(k, v)| (k.to_string(), *v))
            .collect::<Vec<_>>();
        expected.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(collected, expected);
    }

    #[test]
    fn test_iterator_empty() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("massmap_iter_empty.bin");
        let writer = std::fs::File::create(&file).unwrap();
        let entries: Vec<(String, i32)> = vec![];
        let builder = MassMapBuilder::default().with_bucket_count(8);
        builder.build(&writer, entries.iter()).unwrap();

        let file = std::fs::File::open(&file).unwrap();
        let map = MassMap::<String, i32, _>::load(file).unwrap();

        let collected: Vec<_> = map.iter().collect::<std::io::Result<Vec<_>>>().unwrap();
        assert_eq!(collected.len(), 0);
    }

    #[test]
    fn test_iterator_single_bucket() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("massmap_iter_single.bin");
        let writer = std::fs::File::create(&file).unwrap();
        let entries = vec![("a", 1), ("b", 2), ("c", 3), ("d", 4), ("e", 5)];
        // Use 1 bucket to ensure all entries are in the same bucket
        let builder = MassMapBuilder::default().with_bucket_count(1);
        builder.build(&writer, entries.iter()).unwrap();

        let file = std::fs::File::open(&file).unwrap();
        let map = MassMap::<String, i32, _>::load(file).unwrap();

        let collected: Vec<_> = map.iter().collect::<std::io::Result<Vec<_>>>().unwrap();
        assert_eq!(collected.len(), 5);

        // All entries should be present
        let mut collected_sorted = collected.clone();
        collected_sorted.sort_by(|a, b| a.0.cmp(&b.0));
        let mut expected = entries
            .iter()
            .map(|(k, v)| (k.to_string(), *v))
            .collect::<Vec<_>>();
        expected.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(collected_sorted, expected);
    }

    #[test]
    fn test_iterator_many_buckets() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("massmap_iter_many.bin");
        let writer = std::fs::File::create(&file).unwrap();
        const N: u64 = 1000;
        let entries = (0..N).map(|i| (i, i * 2));
        // Use many buckets
        let builder = MassMapBuilder::default().with_bucket_count(100);
        builder.build(&writer, entries).unwrap();

        let file = std::fs::File::open(&file).unwrap();
        let map = MassMap::<u64, u64, _>::load(file).unwrap();

        let collected: Vec<_> = map.iter().collect::<std::io::Result<Vec<_>>>().unwrap();
        assert_eq!(collected.len(), N as usize);

        // Verify all entries are present
        let mut collected_sorted = collected.clone();
        collected_sorted.sort_by(|a, b| a.0.cmp(&b.0));
        for i in 0..N {
            assert_eq!(collected_sorted[i as usize], (i, i * 2));
        }
    }

    #[test]
    fn test_iterator_multiple_iterations() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("massmap_iter_multiple.bin");
        let writer = std::fs::File::create(&file).unwrap();
        let entries = vec![("x", 10), ("y", 20), ("z", 30)];
        let builder = MassMapBuilder::default().with_bucket_count(4);
        builder.build(&writer, entries.iter()).unwrap();

        let file = std::fs::File::open(&file).unwrap();
        let map = MassMap::<String, i32, _>::load(file).unwrap();

        // First iteration
        let collected1: Vec<_> = map.iter().collect::<std::io::Result<Vec<_>>>().unwrap();
        assert_eq!(collected1.len(), 3);

        // Second iteration should yield the same results
        let collected2: Vec<_> = map.iter().collect::<std::io::Result<Vec<_>>>().unwrap();
        assert_eq!(collected2.len(), 3);
        assert_eq!(collected1, collected2);
    }

    #[test]
    fn test_iterator_partial_iteration() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("massmap_iter_partial.bin");
        let writer = std::fs::File::create(&file).unwrap();
        let entries = (0..100).map(|i| (i, i));
        let builder = MassMapBuilder::default().with_bucket_count(10);
        builder.build(&writer, entries).unwrap();

        let file = std::fs::File::open(&file).unwrap();
        let map = MassMap::<u64, u64, _>::load(file).unwrap();

        // Take only the first 10 entries
        let partial: Vec<_> = map
            .iter()
            .take(10)
            .collect::<std::io::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(partial.len(), 10);

        // Skip some and take more
        let skip_take: Vec<_> = map
            .iter()
            .skip(20)
            .take(5)
            .collect::<std::io::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(skip_take.len(), 5);
    }

    #[test]
    fn test_iterator_invalid_bucket() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("massmap_iter_invalid.bin");
        let writer = std::fs::File::create(&path).unwrap();
        let entries = (0..100).map(|i| (i, i));

        let builder = MassMapBuilder::default()
            .with_bucket_count(10)
            .with_writer_buffer_size(8 << 20);
        builder.build(&writer, entries).unwrap();

        // Corrupt bucket data by writing invalid data at offset 24 (start of bucket data)
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        file.write_all_at(b"invalid data", 24).unwrap();

        // Open the corrupted file and try to iterate
        let file = std::fs::File::open(&path).unwrap();
        let map = MassMap::<u64, u64, _>::load(file).unwrap();

        // The iterator should return an error when it tries to read the corrupted bucket
        let mut found_error = false;
        for result in map.iter() {
            if result.is_err() {
                found_error = true;
                break;
            }
        }
        assert!(
            found_error,
            "Iterator should return an error when reading a corrupted bucket"
        );
    }
}
