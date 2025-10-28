use std::{borrow::Borrow, io::Result};

/// Trait abstracting read access to massmap files.
///
/// Implementations must support positional reads without mutating shared state.
/// The trait is blanket-implemented for platform-specific `FileExt` handles.
pub trait MassMapReader {
    /// Reads `length` bytes starting at `offset` and forwards them to `f`.
    ///
    /// Implementations should return an error whenever the requested range
    /// cannot be satisfied in full.
    fn read_exact_at<F, R>(&self, offset: u64, length: u64, f: F) -> Result<R>
    where
        F: Fn(&[u8]) -> Result<R>;

    /// Reads multiple ranges in sequence, delegating to [`read_exact_at`](Self::read_exact_at).
    ///
    /// Override this method to take advantage of vectored IO when available.
    fn batch_read_at<Q: ?Sized, F, R>(
        &self,
        iov: impl IntoIterator<Item = (impl Borrow<Q>, u64, u64)>,
        f: F,
    ) -> Result<Vec<R>>
    where
        F: Fn(&Q, &[u8]) -> Result<R>,
    {
        let iter = iov.into_iter();
        let (lower, _) = iter.size_hint();
        let mut results = Vec::with_capacity(lower);
        for (key, offset, length) in iter {
            if length == 0 {
                results.push(f(key.borrow(), &[])?);
            } else {
                let result = self.read_exact_at(offset, length, |data| f(key.borrow(), data))?;
                results.push(result);
            }
        }
        Ok(results)
    }
}

#[cfg(unix)]
impl<T: std::os::unix::fs::FileExt> MassMapReader for T {
    fn read_exact_at<F, R>(&self, offset: u64, length: u64, f: F) -> Result<R>
    where
        F: Fn(&[u8]) -> Result<R>,
    {
        let mut buffer = vec![0u8; length as usize];
        self.read_exact_at(&mut buffer, offset)?;
        f(&buffer)
    }
}

#[cfg(windows)]
impl<T: std::os::windows::fs::FileExt> MassMapReader for T {
    fn read_exact_at<F, R>(&self, offset: u64, length: u64, f: F) -> Result<R>
    where
        F: Fn(&[u8]) -> Result<R>,
    {
        let mut buffer = vec![0u8; length as usize];
        let n = self.seek_read(&mut buffer, offset)?;
        if n != buffer.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!("Failed to read enough bytes: {} < {}", n, buffer.len()),
            ));
        }
        f(&buffer)
    }
}
