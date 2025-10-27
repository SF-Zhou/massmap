use foldhash::fast::FixedState;
use std::io::{Error, ErrorKind, Result, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::{hash::BuildHasher, io::BufWriter};

use super::{MAGIC_NUMBER, MassMapBucket, MassMapInfo, MassMapMeta, MassMapWriter};

/// Builder type for emitting massmap files from key-value iterators.
///
/// The builder owns configuration such as hash seed, bucket sizing and IO
/// buffering. Use [`build`](Self::build) to stream MessagePack-encoded buckets to
/// a [`MassMapWriter`] sink (typically a file implementing `FileExt`).
///
/// Cloning is not required; each builder instance is consumed by a single call
/// to [`build`](Self::build).
pub struct MassMapBuilder {
    hash_seed: u64,
    hash_state: FixedState,
    bucket_count: u64,
    writer_buffer_size: usize,
    field_names: bool,
    bucket_size_limit: u32,
}

impl Default for MassMapBuilder {
    fn default() -> Self {
        Self {
            hash_seed: 0,
            hash_state: FixedState::with_seed(0),
            bucket_count: 1024,
            writer_buffer_size: 16 << 20, // 16 MiB
            field_names: false,
            bucket_size_limit: u32::MAX,
        }
    }
}

impl MassMapBuilder {
    /// Overrides the hash seed used to distribute keys across buckets.
    pub fn with_hash_seed(mut self, seed: u64) -> Self {
        self.hash_seed = seed;
        self.hash_state = FixedState::with_seed(seed);
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
    /// let info = MassMapBuilder::default().build(file, data.iter())?;
    /// assert_eq!(info.entry_count, 2);
    /// # Ok(())
    /// # }
    /// ```
    pub fn build<W, K, V>(
        self,
        writer: W,
        entries: impl Iterator<Item = impl std::borrow::Borrow<(K, V)>>,
    ) -> std::io::Result<MassMapInfo>
    where
        W: MassMapWriter,
        K: serde::Serialize + Clone + std::hash::Hash + Eq,
        V: serde::Serialize + Clone,
    {
        let mut buckets: Vec<Vec<(K, V)>> = vec![Vec::new(); self.bucket_count as usize];
        let mut length: u64 = 0;
        for entry in entries {
            let (key, value) = entry.borrow();
            let bucket_index = self.hash_state.hash_one(key) % self.bucket_count;
            buckets[bucket_index as usize].push((key.clone(), value.clone()));
            length += 1;
        }

        const S: usize = std::mem::size_of::<u64>();
        let mut meta = MassMapMeta {
            hash_seed: self.hash_seed,
            buckets: Vec::with_capacity(self.bucket_count as usize),
            length,
        };

        let offset = AtomicU64::new(S as u64 * 3);
        let mut buf_writer = BufWriter::with_capacity(
            self.writer_buffer_size,
            MassMapWriterWrapper {
                inner: &writer,
                offset: &offset,
            },
        );
        for bucket in buckets {
            if bucket.is_empty() {
                meta.buckets.push(MassMapBucket {
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

            meta.buckets.push(MassMapBucket {
                offset: begin_offset,
                length: (end_offset - begin_offset) as u32,
                count: bucket.len() as u32,
            });
        }

        let meta_offset = offset.load(Ordering::Relaxed) + buf_writer.buffer().len() as u64;
        rmp_serde::encode::write(&mut buf_writer, &meta)
            .map_err(|e| Error::other(format!("Fail to serialize meta: {}", e)))?;
        let finished_offset = offset.load(Ordering::Relaxed) + buf_writer.buffer().len() as u64;
        buf_writer.flush()?;

        let meta_length = finished_offset - meta_offset;
        let mut data = [0u8; S * 3];
        data[..S].copy_from_slice(&MAGIC_NUMBER.to_be_bytes());
        data[S..S * 2].copy_from_slice(&meta_offset.to_be_bytes());
        data[S * 2..].copy_from_slice(&meta_length.to_be_bytes());
        writer.write_at(&data, 0)?;

        let empty_buckets = meta.buckets.iter().filter(|b| b.count == 0).count();
        Ok(MassMapInfo {
            file_length: finished_offset,
            entry_count: length,
            bucket_count: self.bucket_count as usize,
            empty_buckets,
            meta_offset,
            meta_length,
            hash_seed: self.hash_seed,
        })
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
        self.inner.write_at(buf, offset)?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}
