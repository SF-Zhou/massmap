use std::io::{Error, ErrorKind, Result, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::{hash::BuildHasher, io::BufWriter};

use crate::{
    MassMapBucketMeta, MassMapDefaultHashLoader, MassMapHashConfig, MassMapHashLoader,
    MassMapHeader, MassMapInfo, MassMapMeta, MassMapWriter,
};

/// Builder type for emitting massmap files from key-value iterators.
///
/// The builder owns configuration such as hash seed, bucket sizing and IO
/// buffering. Use [`build`](Self::build) to stream MessagePack-encoded buckets to
/// a [`MassMapWriter`] sink (typically a file implementing `FileExt`).
///
/// Cloning is not required; each builder instance is consumed by a single call
/// to [`build`](Self::build).
#[derive(Debug)]
pub struct MassMapBuilder<H: MassMapHashLoader = MassMapDefaultHashLoader> {
    hash_config: MassMapHashConfig,
    bucket_count: u64,
    writer_buffer_size: usize,
    field_names: bool,
    bucket_size_limit: u32,
    phantom: std::marker::PhantomData<H>,
}

impl Default for MassMapBuilder {
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
    /// Enabling this makes the output human readable at the cost of slightly
    /// larger files.
    pub fn with_field_names(mut self, value: bool) -> Self {
        self.field_names = value;
        self
    }

    /// Sets a hard cap on the number of bytes allowed per bucket payload.
    pub fn with_bucket_size_limit(mut self, limit: u32) -> Self {
        self.bucket_size_limit = limit;
        self
    }

    /// Consumes the builder and writes a massmap to `writer` from `entries`.
    ///
    /// The iterator is hashed according to the configured parameters, buckets
    /// are serialized via `rmp-serde`, and a [`MassMapInfo`] summary is returned
    /// on success.
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

        const S: usize = std::mem::size_of::<u64>();
        let mut bucket_metas: Vec<MassMapBucketMeta> =
            Vec::with_capacity(self.bucket_count as usize);

        let offset = AtomicU64::new(S as u64 * 3);
        let mut buf_writer = BufWriter::with_capacity(
            self.writer_buffer_size,
            MassMapWriterWrapper {
                inner: writer,
                offset: &offset,
            },
        );
        for bucket in buckets {
            if bucket.is_empty() {
                bucket_metas.push(MassMapBucketMeta {
                    offset: 0,
                    length: 0,
                    count: 0,
                });
                continue;
            }

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
            empty_buckets: bucket_metas.iter().filter(|b| b.count == 0).count() as u64,
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

#[cfg(test)]
mod tests {
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
}
