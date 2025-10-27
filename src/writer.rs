use std::io::Result;

/// Trait representing positional writers suitable for massmap serialization.
///
/// Writers must support writing arbitrary byte slices at fixed offsets without
/// altering shared state; this is satisfied by `FileExt` handles on both Unix
/// and Windows.
pub trait MassMapWriter {
    /// Writes `data` at the given absolute `offset`.
    fn write_at(&self, data: &[u8], offset: u64) -> Result<()>;
}

#[cfg(unix)]
impl<T: std::os::unix::fs::FileExt> MassMapWriter for T {
    fn write_at(&self, data: &[u8], offset: u64) -> Result<()> {
        self.write_all_at(data, offset)
    }
}

#[cfg(windows)]
impl<T: std::os::windows::fs::FileExt> MassMapWriter for T {
    fn write_at(&self, data: &[u8], offset: u64) -> Result<()> {
        self.seek_write(data, offset)?;
        Ok(())
    }
}
