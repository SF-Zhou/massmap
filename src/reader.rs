use std::io::Result;

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
    fn batch_read_at<F, R>(&self, iov: &[(u64, u64)], f: F) -> Result<Vec<R>>
    where
        F: Fn(usize, &[u8]) -> Result<R>,
    {
        let mut results = Vec::with_capacity(iov.len());
        for (index, &(offset, length)) in iov.iter().enumerate() {
            if length == 0 {
                results.push(f(index, &[])?);
            } else {
                let result = self.read_exact_at(offset, length, |data| f(index, data))?;
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
        let bytes = self.seek_read(&mut buffer, offset)?;
        if bytes.len() != buffer.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Failed to read enough bytes",
            ));
        }
        f(&buffer)
    }
}
