use std::io::{Error, ErrorKind, Result, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::{
    hash::{BuildHasher, Hash},
    io::BufWriter,
};

use serde::Deserialize;

use crate::{
    MassMap, MassMapBucketMeta, MassMapDefaultHashLoader, MassMapHashConfig, MassMapHashLoader,
    MassMapHeader, MassMapInfo, MassMapMeta, MassMapReader, MassMapWriter,
};

/// Builder type for emitting massmap files from key-value iterators.
///
/// The builder owns configuration such as the hash seed, bucket sizing, IO
/// buffering, field-name emission, and optional bucket size guards. Use
/// [`build`](Self::build) to stream MessagePack-encoded buckets to a
/// [`MassMapWriter`] sink (typically a file implementing `FileExt`).
///
/// The loader type parameter `H` allows swapping in custom
/// [`MassMapHashLoader`] implementations. Each builder instance is consumed by a
/// single call to [`build`](Self::build).
#[derive(Debug)]
pub struct MassMapBuilder<H: MassMapHashLoader = MassMapDefaultHashLoader> {
    hash_config: MassMapHashConfig,
    bucket_count: u64,
    writer_buffer_size: usize,
    field_names: bool,
    bucket_size_limit: u32,
    phantom: std::marker::PhantomData<H>,
}

impl<H: MassMapHashLoader> Default for MassMapBuilder<H> {
    fn default() -> Self {
        Self {
            hash_config: MassMapHashConfig::default(),
            bucket_count: 1024,
            writer_buffer_size: 16 << 20, // 16 MiB
            field_names: false,
            bucket_size_limit: u32::MAX,
            phantom: std::marker::PhantomData,
        }
    }
}

impl MassMapBuilder {
    /// Creates a new default massmap builder with default hash loader.
    #[allow(clippy::should_implement_trait)]
    pub fn default() -> Self {
        <Self as Default>::default()
    }
}

impl<H: MassMapHashLoader> MassMapBuilder<H> {
    /// Replaces the entire hash configuration used to distribute keys across buckets.
    ///
    /// This method allows advanced users to specify a custom [`MassMapHashConfig`]
    /// with arbitrary parameters. For most use cases, [`with_hash_seed`](Self::with_hash_seed)
    /// is sufficient to override just the hash seed parameter.
    ///
    /// # Parameters
    /// - `config`: The hash configuration to use for this builder.
    pub fn with_hash_config(mut self, config: MassMapHashConfig) -> Self {
        self.hash_config = config;
        self
    }

    /// Overrides the hash seed used to distribute keys across buckets.
    pub fn with_hash_seed(mut self, seed: u64) -> Self {
        self.hash_config.parameters["seed"] = serde_json::json!(seed);
        self
    }

    /// Sets the number of buckets to allocate prior to serialization.
    ///
    /// A larger bucket count reduces collisions at the cost of additional
    /// metadata and potentially sparser files.
    pub fn with_bucket_count(mut self, count: u64) -> Self {
        self.bucket_count = count;
        self
    }

    /// Adjusts the capacity of the buffered writer used while streaming data.
    pub fn with_writer_buffer_size(mut self, size: usize) -> Self {
        self.writer_buffer_size = size;
        self
    }

    /// Controls whether serialized MessagePack maps include field names.
    ///
    /// Enabling this makes the serialized buckets human readable at the cost
    /// of slightly larger files and additional encoding work.
    pub fn with_field_names(mut self, value: bool) -> Self {
        self.field_names = value;
        self
    }

    /// Sets a hard cap on the number of bytes allowed per bucket payload.
    ///
    /// Buckets that exceed this limit cause [`build`](Self::build) to abort
    /// with `ErrorKind::InvalidData`, which can be useful when targeting
    /// systems with strict per-request IO ceilings.
    pub fn with_bucket_size_limit(mut self, limit: u32) -> Self {
        self.bucket_size_limit = limit;
        self
    }

    /// Consumes the builder and writes a massmap to `writer` from `entries`.
    ///
    /// The iterator is hashed according to the configured parameters, buckets
    /// are serialized via `rmp-serde`, and a [`MassMapInfo`] summary is
    /// returned on success. Input ordering does not matter; keys are
    /// automatically distributed across buckets.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails, if any bucket exceeds the
    /// configured limit, or if the underlying writer reports an IO failure.
    ///
    /// # Examples
    ///
    /// ```
    /// use massmap::MassMapBuilder;
    ///
    /// # fn main() -> std::io::Result<()> {
    /// let data = [("it", 1u32), ("works", 2u32)];
    /// let file = std::fs::File::create("examples/example.massmap")?;
    /// let info = MassMapBuilder::default().build(&file, data.iter())?;
    /// assert_eq!(info.meta.entry_count, 2);
    /// # Ok(())
    /// # }
    /// ```
    pub fn build<W, K, V>(
        self,
        writer: &W,
        entries: impl Iterator<Item = impl std::borrow::Borrow<(K, V)>>,
    ) -> std::io::Result<MassMapInfo>
    where
        W: MassMapWriter,
        K: serde::Serialize + Clone + std::hash::Hash + Eq,
        V: serde::Serialize + Clone,
    {
        let build_hasher = H::load(&self.hash_config)?;

        let mut buckets: Vec<Vec<(K, V)>> = vec![Vec::new(); self.bucket_count as usize];
        let mut entry_count: u64 = 0;
        for entry in entries {
            let (key, value) = entry.borrow();
            let bucket_index = build_hasher.hash_one(key) % self.bucket_count;
            buckets[bucket_index as usize].push((key.clone(), value.clone()));
            entry_count += 1;
        }

        let mut bucket_metas: Vec<MassMapBucketMeta> =
            Vec::with_capacity(self.bucket_count as usize);

        let offset = AtomicU64::new(MassMapHeader::SIZE as u64);
        let mut buf_writer = BufWriter::with_capacity(
            self.writer_buffer_size,
            MassMapWriterWrapper {
                inner: writer,
                offset: &offset,
            },
        );
        let mut occupied_bucket_count = 0;
        let mut occupied_bucket_range = 0..0;
        for (i, bucket) in buckets.into_iter().enumerate() {
            if bucket.is_empty() {
                bucket_metas.push(MassMapBucketMeta {
                    offset: 0,
                    length: 0,
                    count: 0,
                });
                continue;
            }

            occupied_bucket_count += 1;
            if occupied_bucket_range.is_empty() {
                occupied_bucket_range.start = i as u64;
            }
            occupied_bucket_range.end = i as u64 + 1;

            let begin_offset = offset.load(Ordering::Relaxed) + buf_writer.buffer().len() as u64;

            let result = if self.field_names {
                rmp_serde::encode::write_named(&mut buf_writer, &bucket)
            } else {
                rmp_serde::encode::write(&mut buf_writer, &bucket)
            };
            result.map_err(|e| Error::other(format!("Fail to serialize bucket: {}", e)))?;

            let end_offset = offset.load(Ordering::Relaxed) + buf_writer.buffer().len() as u64;
            if end_offset - begin_offset > self.bucket_size_limit as u64 {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("bucket size exceeds {}", self.bucket_size_limit),
                ));
            }

            bucket_metas.push(MassMapBucketMeta {
                offset: begin_offset,
                length: (end_offset - begin_offset) as u32,
                count: bucket.len() as u32,
            });
        }

        let meta = MassMapMeta {
            hash_config: self.hash_config,
            entry_count,
            bucket_count: self.bucket_count,
            occupied_bucket_count,
            occupied_bucket_range,
            key_type: std::any::type_name::<K>().to_string(),
            value_type: std::any::type_name::<V>().to_string(),
        };

        let meta_offset = offset.load(Ordering::Relaxed) + buf_writer.buffer().len() as u64;
        rmp_serde::encode::write(&mut buf_writer, &(meta.clone(), bucket_metas))
            .map_err(|e| Error::other(format!("Fail to serialize meta: {}", e)))?;
        let finished_offset = offset.load(Ordering::Relaxed) + buf_writer.buffer().len() as u64;
        buf_writer.flush()?;

        let meta_length = finished_offset - meta_offset;
        let header = MassMapHeader {
            meta_offset,
            meta_length,
        };
        writer.write_all_at(&header.serialize(), 0)?;

        Ok(MassMapInfo { header, meta })
    }
}

/// Thin wrapper implementing [`std::io::Write`] in terms of [`MassMapWriter`].
///
/// This adapter streams each write into the underlying writer at consecutive
/// offsets tracked by an atomic counter. It is primarily used internally by the
/// builder.
pub struct MassMapWriterWrapper<'a, W: MassMapWriter> {
    inner: &'a W,
    offset: &'a AtomicU64,
}

impl<'a, W: MassMapWriter> std::io::Write for MassMapWriterWrapper<'a, W> {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        let offset = self.offset.fetch_add(buf.len() as u64, Ordering::Relaxed);
        self.inner.write_all_at(buf, offset)?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
pub struct MassMapMerger {
    writer_buffer_size: usize,
}

impl Default for MassMapMerger {
    fn default() -> Self {
        Self {
            writer_buffer_size: 16 << 20, // 16 MiB
        }
    }
}

impl MassMapMerger {
    /// Adjusts the capacity of the buffered writer used while streaming data.
    pub fn with_writer_buffer_size(mut self, size: usize) -> Self {
        self.writer_buffer_size = size;
        self
    }
}

impl MassMapMerger {
    pub fn merge<W, K, V, R: MassMapReader, H: MassMapHashLoader>(
        self,
        writer: &W,
        mut maps: Vec<MassMap<K, V, R, H>>,
    ) -> Result<MassMapInfo>
    where
        W: MassMapWriter,
        K: for<'de> Deserialize<'de> + Eq + Hash,
        V: for<'de> Deserialize<'de> + Clone,
    {
        if maps.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "No massmaps provided for merging",
            ));
        }

        maps.sort_by_key(|m| m.meta.occupied_bucket_range.start);

        let mut entry_count = 0;
        let mut bucket_metas =
            vec![MassMapBucketMeta::default(); maps[0].meta.bucket_count as usize];
        let hash_config = maps[0].meta.hash_config.clone();
        let mut occupied_bucket_count = 0;
        let mut occupied_bucket_range = 0..0;
        let mut global_offset = 0u64;

        for map in &maps {
            if map.meta.hash_config != hash_config {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "Incompatible hash configurations between massmaps",
                ));
            }
            if map.meta.bucket_count != bucket_metas.len() as u64 {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "Incompatible bucket counts between massmaps",
                ));
            }

            if map.meta.entry_count == 0 {
                continue;
            }

            occupied_bucket_count += map.meta.occupied_bucket_count;
            if occupied_bucket_range.is_empty() {
                occupied_bucket_range = map.meta.occupied_bucket_range.clone();
            } else if occupied_bucket_range.end <= map.meta.occupied_bucket_range.start {
                occupied_bucket_range.end = map.meta.occupied_bucket_range.end;
            } else {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "Overlapping occupied bucket ranges between massmaps",
                ));
            }

            // update bucket metas.
            for idx in map.meta.occupied_bucket_range.clone() {
                let bucket_meta = &mut bucket_metas[idx as usize];
                *bucket_meta = map.bucket_metas[idx as usize];
                if bucket_meta.count > 0 {
                    bucket_meta.offset += global_offset;
                }
            }
            entry_count += map.meta.entry_count;

            // copy buckets from reader to writer directly.
            let mut current_offset = MassMapHeader::SIZE as u64;
            let finished_offset = map.header.meta_offset;
            while current_offset < finished_offset {
                let chunk = std::cmp::min(
                    finished_offset - current_offset,
                    self.writer_buffer_size as u64,
                );
                map.reader.read_exact_at(current_offset, chunk, |data| {
                    writer.write_all_at(data, global_offset + MassMapHeader::SIZE as u64)?;
                    Ok(())
                })?;
                current_offset += chunk;
                global_offset += chunk;
            }
        }

        let meta = MassMapMeta {
            hash_config,
            entry_count,
            bucket_count: bucket_metas.len() as u64,
            occupied_bucket_count,
            occupied_bucket_range,
            key_type: std::any::type_name::<K>().to_string(),
            value_type: std::any::type_name::<V>().to_string(),
        };

        let meta_offset = global_offset + MassMapHeader::SIZE as u64;
        let offset = AtomicU64::new(meta_offset);
        let mut buf_writer = BufWriter::with_capacity(
            self.writer_buffer_size,
            MassMapWriterWrapper {
                inner: writer,
                offset: &offset,
            },
        );

        rmp_serde::encode::write(&mut buf_writer, &(meta.clone(), bucket_metas))
            .map_err(|e| Error::other(format!("Fail to serialize meta: {}", e)))?;
        buf_writer.flush()?;
        let finished_offset = offset.load(Ordering::Relaxed);

        let meta_length = finished_offset - meta_offset;
        let header = MassMapHeader {
            meta_offset,
            meta_length,
        };
        writer.write_all_at(&header.serialize(), 0)?;

        Ok(MassMapInfo { header, meta })
    }
}

#[cfg(test)]
mod tests {
    use std::{fs::File, hash::Hasher, sync::Arc};

    use crate::*;

    #[derive(Debug)]
    struct MemoryWriter {
        data: std::sync::Mutex<Vec<u8>>,
        limit: u64,
    }

    impl MemoryWriter {
        fn new(limit: u64) -> Self {
            Self {
                data: std::sync::Mutex::new(Vec::new()),
                limit,
            }
        }

        fn read_at(&self, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
            let data = self.data.lock().unwrap();
            let available = data.len() - std::cmp::min(offset as usize, data.len());
            let to_read = std::cmp::min(buf.len(), available);
            buf[..to_read].copy_from_slice(&data[offset as usize..offset as usize + to_read]);
            Ok(to_read)
        }
    }

    #[cfg(unix)]
    impl std::os::unix::fs::FileExt for MemoryWriter {
        fn read_at(&self, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
            self.read_at(buf, offset)
        }
        fn write_at(&self, mut buf: &[u8], offset: u64) -> std::io::Result<usize> {
            if offset > self.limit {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "Write exceeds limit",
                ));
            }
            if buf.len() as u64 + offset > self.limit {
                buf = &buf[..(self.limit - offset) as usize];
            }

            let mut data = self.data.lock().unwrap();
            if data.len() < (offset as usize + buf.len()) {
                data.resize(offset as usize + buf.len(), 0);
            }
            data[offset as usize..offset as usize + buf.len()].copy_from_slice(buf);
            Ok(buf.len())
        }
    }

    #[cfg(windows)]
    impl std::os::windows::fs::FileExt for MemoryWriter {
        fn seek_read(&self, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
            self.read_at(buf, offset)
        }
        fn seek_write(&self, mut buf: &[u8], offset: u64) -> std::io::Result<usize> {
            if offset > self.limit {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "Write exceeds limit",
                ));
            }
            if buf.len() as u64 + offset > self.limit {
                buf = &buf[..(self.limit - offset) as usize];
            }

            let mut data = self.data.lock().unwrap();
            if data.len() < (offset as usize + buf.len()) {
                data.resize(offset as usize + buf.len(), 0);
            }
            data[offset as usize..offset as usize + buf.len()].copy_from_slice(buf);
            Ok(buf.len())
        }
    }

    #[test]
    fn test_shorter_write() {
        // 6400 is sufficient to write all entries, 6000 is not.
        const SUFFICIENT_CAPACITY: u64 = 6400;
        const INSUFFICIENT_CAPACITY: u64 = 6000;
        const N: u64 = 1000;

        let entries = (0..N).map(|i| (i, i));
        let writer = MemoryWriter::new(SUFFICIENT_CAPACITY);
        let hash_config = MassMapHashConfig {
            name: "foldhash".to_string(),
            parameters: serde_json::json!({ "seed": 42 }),
        };
        let builder = MassMapBuilder::default()
            .with_bucket_count(1)
            .with_hash_config(hash_config);
        builder.build(&writer, entries).unwrap();

        let map = MassMap::<u64, u64, _>::load(writer).unwrap();
        for i in 0..N {
            let value = map.get(&i).unwrap().unwrap();
            assert_eq!(value, i);
        }

        let entries = (0..N).map(|i| (i, i));
        let writer = MemoryWriter::new(INSUFFICIENT_CAPACITY);
        let builder = MassMapBuilder::default().with_bucket_count(1);
        builder.build(&writer, entries).unwrap_err();
    }

    pub struct SimpleHasher {
        state: u64,
        modulo: u64,
    }

    impl SimpleHasher {
        pub fn new(modulo: u64) -> Self {
            SimpleHasher { state: 0, modulo }
        }
    }

    impl Hasher for SimpleHasher {
        fn finish(&self) -> u64 {
            self.state % self.modulo
        }

        fn write(&mut self, bytes: &[u8]) {
            for &byte in bytes.iter().rev() {
                self.state = self.state.wrapping_mul(256).wrapping_add(byte as u64);
            }
        }
    }

    struct SimpleBuildHasher {
        modulo: u64,
    }

    impl std::hash::BuildHasher for SimpleBuildHasher {
        type Hasher = SimpleHasher;

        fn build_hasher(&self) -> Self::Hasher {
            SimpleHasher::new(self.modulo)
        }
    }

    struct SimpleHashLoader;

    impl MassMapHashLoader for SimpleHashLoader {
        type BuildHasher = SimpleBuildHasher;

        fn load(config: &MassMapHashConfig) -> std::io::Result<Self::BuildHasher> {
            let modulo = config
                .parameters
                .get("modulo")
                .and_then(|v| v.as_u64())
                .unwrap_or(10000);
            Ok(SimpleBuildHasher { modulo })
        }
    }

    fn create_simple_map(
        entries: impl Iterator<Item = (u64, u64)>,
        bucket_count: u64,
        hash_modulo: u64,
    ) -> MassMap<u64, u64, MemoryWriter, SimpleHashLoader> {
        let writer = MemoryWriter::new(10 << 20); // 10 MiB
        let hash_config = MassMapHashConfig {
            name: "simplehash".to_string(),
            parameters: serde_json::json!({
                "modulo": hash_modulo
            }),
        };
        let builder = MassMapBuilder::<SimpleHashLoader>::default()
            .with_bucket_count(bucket_count)
            .with_hash_config(hash_config);
        builder.build(&writer, entries).unwrap();

        MassMap::<u64, u64, _, SimpleHashLoader>::load(writer).unwrap()
    }

    #[test]
    fn test_normal_merge() {
        let dir = tempfile::tempdir().unwrap();

        const M: u64 = 10000;
        const N: u64 = 100_000;
        const P: u64 = 10;

        let mut threads = Vec::with_capacity(P as usize);
        for i in 0..P {
            threads.push(std::thread::spawn(move || {
                let entries = (0..N).filter(|v| (v % M) / (M / P) == i).map(|v| (v, v));
                let map = create_simple_map(entries, M, M);
                assert_eq!(map.meta.occupied_bucket_count, M / P);
                assert_eq!(map.meta.entry_count, N / P);
                assert_eq!(map.meta.occupied_bucket_range.start, (M / P) * i);

                for item in map.iter() {
                    let (k, v) = item.unwrap();
                    assert_eq!(k, v);
                }
                map
            }));
        }

        let mut maps = threads
            .into_iter()
            .map(|t| t.join().unwrap())
            .collect::<Vec<_>>();
        maps.push(create_simple_map((0..0).map(|v| (v, v)), M, M));

        let path = dir.path().join("merge.massmap");
        let writer = std::fs::File::create(&path).unwrap();
        MassMapMerger::default().merge(&writer, maps).unwrap();

        let reader = std::fs::File::open(&path).unwrap();
        let map = MassMap::<u64, u64, _, SimpleHashLoader>::load(reader).unwrap();
        assert_eq!(map.len(), N);
        let map = Arc::new(map);

        let mut threads = Vec::with_capacity(P as usize);
        for i in 0..P {
            const CHUNK: u64 = N / P;
            let range = CHUNK * i..CHUNK * (i + 1);
            let map = map.clone();
            threads.push(std::thread::spawn(move || {
                for v in range {
                    assert_eq!(map.get(&v).unwrap().unwrap(), v);
                }
            }));
        }

        for thread in threads {
            thread.join().unwrap();
        }
    }

    #[test]
    fn test_invalid_merge() {
        // 1. different hash config.
        {
            let map1 = create_simple_map((0..1000).map(|i| (i, i)), 1024, 10000);
            let map2 = create_simple_map((1000..2000).map(|i| (i, i)), 1024, 20000);
            let writer = MemoryWriter::new(10 << 20); // 10 MiB
            MassMapMerger::default()
                .with_writer_buffer_size(1 << 20)
                .merge(&writer, vec![map1, map2])
                .unwrap_err();
        }

        // 2. different bucket count.
        {
            let map1 = create_simple_map((0..1000).map(|i| (i, i)), 1024, 10000);
            let map2 = create_simple_map((1000..2000).map(|i| (i, i)), 2048, 10000);
            let writer = MemoryWriter::new(10 << 20); // 10 MiB
            MassMapMerger::default()
                .merge(&writer, vec![map1, map2])
                .unwrap_err();
        }

        // 3. overlapping occupied bucket range.
        {
            let map1 = create_simple_map((0..1000).map(|i| (i, i)), 1024, 10000);
            let map2 = create_simple_map((500..1500).map(|i| (i, i)), 1024, 10000);
            let writer = MemoryWriter::new(10 << 20); // 10 MiB
            MassMapMerger::default()
                .merge(&writer, vec![map1, map2])
                .unwrap_err();
        }

        // 4. empty input.
        {
            let writer = MemoryWriter::new(10 << 20); // 10 MiB
            MassMapMerger::default()
                .merge::<_, u64, u64, File, SimpleHashLoader>(&writer, vec![])
                .unwrap_err();
        }
    }
}
